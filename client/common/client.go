package common

import (
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

// isConnectionError checks if the error indicates a broken/lost connection
func isConnectionError(err error) bool {
	if err == nil {
		return false
	}

	// Check for standard Go network/IO errors
	if errors.Is(err, net.ErrClosed) ||
		errors.Is(err, io.ErrClosedPipe) ||
		errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}

	// Check for network operation errors (connection refused, host unreachable, etc.)
	var netErr *net.OpError
	if errors.As(err, &netErr) {
		return true
	}

	// Check for syscall errors (EPIPE, ECONNRESET, etc.)
	var syscallErr *os.SyscallError
	return errors.As(err, &syscallErr)
}

// --- Interfaz para manejar cualquier tipo de fila de tabla ---
type TableRowHandler interface {
	// ProcessRecord convierte una línea de CSV en el mapa clave-valor del protocolo.
	ProcessRecord(record []string) (map[string]string, error)
	// GetExpectedFields devuelve el número de columnas que el CSV debe tener.
	GetExpectedFields() int
}

type MenuItemHandler struct{}

func (m MenuItemHandler) ProcessRecord(record []string) (map[string]string, error) {
	if len(record) != 7 {
		return nil, fmt.Errorf("expected 7 fields, got %d: %v", len(record), record)
	}
	return map[string]string{
		"product_id":     record[0],
		"name":           record[1],
		"category":       record[2],
		"price":          record[3],
		"is_seasonal":    record[4],
		"available_from": record[5],
		"available_to":   record[6],
	}, nil
}

func (m MenuItemHandler) GetExpectedFields() int { return 7 }

type StoreHandler struct{}

func (s StoreHandler) ProcessRecord(record []string) (map[string]string, error) {
	return map[string]string{
		"store_id":    record[0],
		"store_name":  record[1],
		"street":      record[2],
		"postal_code": record[3],
		"city":        record[4],
		"state":       record[5],
		"latitude":    record[6],
		"longitude":   record[7],
	}, nil
}

func (s StoreHandler) GetExpectedFields() int { return 8 }

type TransactionItemHandler struct{}

func (t TransactionItemHandler) ProcessRecord(record []string) (map[string]string, error) {
	return map[string]string{
		"transaction_id": record[0],
		"item_id":        record[1],
		"quantity":       record[2],
		"unit_price":     record[3],
		"subtotal":       record[4],
		"created_at":     record[5],
	}, nil
}

func (t TransactionItemHandler) GetExpectedFields() int { return 6 }

type TransactionHandler struct{}

func (t TransactionHandler) ProcessRecord(record []string) (map[string]string, error) {
	return map[string]string{
		"transaction_id":    record[0],
		"store_id":          record[1],
		"payment_method_id": record[2],
		"user_id":           record[3],
		"original_amount":   record[4],
		"discount_applied":  record[5],
		"final_amount":      record[6],
		"created_at":        record[7],
	}, nil
}

func (t TransactionHandler) GetExpectedFields() int { return 8 }

type UserHandler struct{}

func (u UserHandler) ProcessRecord(record []string) (map[string]string, error) {
	return map[string]string{
		"user_id":       record[0],
		"gender":        record[1],
		"birthdate":     record[2],
		"registered_at": record[3],
	}, nil
}

func (u UserHandler) GetExpectedFields() int { return 4 }

// ClientConfig holds the runtime configuration for a client instance.
// - ID: agency identifier as a string.
// - ServerAddress: TCP address of the server (host:port).
// - BetsFilePath: CSV path with the agency bets.
// - BatchLimit: maximum number of bets per batch (upper bound besides the 8 KiB framing limit).
type ClientConfig struct {
	ID            string
	ServerAddress string
	BetsFilePath  string
	BatchLimit    int32
}

// Client encapsulates the client behavior, including configuration and
// the currently open TCP connection (if any).
type Client struct {
	config  ClientConfig
	conn    net.Conn
	handler TableRowHandler // <-- Usa la interfaz
	opCode  byte            // <-- Sabe qué OpCode enviar
}

// NewClient constructs a Client with the provided configuration.
// The TCP connection is not opened here; see createClientSocket / SendBets.
// Handler and opCode will be set automatically during SendBets based on table types.
func NewClient(config ClientConfig) *Client {
	return &Client{
		config: config,
	}
}

func (c *Client) setClientTableType(handler TableRowHandler, opCode byte) error {
	c.handler = handler
	c.opCode = opCode
	return nil
}

// setHandlerForTableType automatically sets the appropriate handler and opcode based on table type directory name
func (c *Client) setHandlerForTableType(tableType string) error {
	switch tableType {
	case "transactions":
		return c.setClientTableType(TransactionHandler{}, OpCodeNewTransaction)
	case "transaction_items":
		return c.setClientTableType(TransactionItemHandler{}, OpCodeNewTransactionItems)
	case "menu_items":
		return c.setClientTableType(MenuItemHandler{}, OpCodeNewMenuItems)
	case "stores":
		return c.setClientTableType(StoreHandler{}, OpCodeNewStores)
	case "users":
		return c.setClientTableType(UserHandler{}, OpCodeNewUsers)
	default:
		return fmt.Errorf("unknown table type: %s", tableType)
	}
}

// processNextBet reads a single CSV record from betsReader, converts it
// to the protocol key/value map (including AGENCIA), and attempts to add
// it to the current batch buffer via AddBetWithFlush. If adding this bet
// would exceed either the 8 KiB framing limit or the configured BatchLimit,
// the function triggers a flush of the current batch to c.conn and then
// starts a new batch with this bet. The returned error is io.EOF when the
// CSV is exhausted, or any I/O/serialization error encountered.
func (c *Client) processNextRow(reader *csv.Reader, batchBuff *bytes.Buffer, counter *int32, batchNumber *int64) error {
	record, err := reader.Read()
	if err != nil {
		return err
	}

	// Llama al método de la interfaz para procesar el registro. El cliente no sabe qué tipo es.
	rowMap, err := c.handler.ProcessRecord(record)
	if err != nil {
		return err
	}

	// Llama a una función de bajo nivel también genérica
	if err := AddRowToBatch(rowMap, batchBuff, c.conn, counter, c.config.BatchLimit, c.opCode, batchNumber); err != nil {
		return err
	}
	return nil
}

// handleCancellation maneja la cancelación del contexto y envía batch pendiente si hay datos
func (c *Client) handleCancellation(batchBuff *bytes.Buffer, counter *int32, currentBatchNumber *int64) error {
	if *counter > 0 {
		(*currentBatchNumber)++ // Incrementar solo cuando realmente enviamos un batch
		if err := FlushBatch(batchBuff, c.conn, *counter, c.opCode, *currentBatchNumber, BatchCancel); err != nil {
			return err
		}
		*counter = 0
	}
	return context.Canceled
}

// handleEOF maneja el final del archivo y envía batch pendiente si hay datos
func (c *Client) handleEOF(batchBuff *bytes.Buffer, counter *int32, currentBatchNumber *int64) error {
	if *counter > 0 {
		(*currentBatchNumber)++ // Incrementar solo cuando realmente enviamos un batch
		if err := FlushBatch(batchBuff, c.conn, *counter, c.opCode, *currentBatchNumber, BatchEOF); err != nil {
			return err
		}
	}
	return nil // EOF exitoso
}

// processCSVLoop procesa el loop principal de lectura del CSV
func (c *Client) processCSVLoop(ctx context.Context, reader *csv.Reader, batchBuff *bytes.Buffer, counter *int32, currentBatchNumber *int64) error {
	for {
		// Verificar cancelación
		select {
		case <-ctx.Done():
			return c.handleCancellation(batchBuff, counter, currentBatchNumber)
		default:
			// Continuar procesamiento normal
		}

		// Leer y procesar la siguiente línea del CSV
		if err := c.processNextRow(reader, batchBuff, counter, currentBatchNumber); err != nil {
			if errors.Is(err, io.EOF) {
				return c.handleEOF(batchBuff, counter, currentBatchNumber)
			}
			// Cualquier otro error (CSV malformado, I/O, etc.)
			return err
		}
	}
}

// buildAndSendBatches streams the CSV, incrementally building table data
// bodies into batchBuff and flushing to c.conn as limits are reached.
// On context cancellation, it flushes any partial batch and returns the
// context error. On clean EOF, it flushes a final partial batch (if any)
// and returns nil. Any serialization or socket error is returned.
func (c *Client) buildAndSendBatches(ctx context.Context, reader *csv.Reader, batchNumber int64) error {
	var batchBuff bytes.Buffer
	var counter int32 = 0
	currentBatchNumber := batchNumber // Número del batch actual (no se incrementa por línea)

	return c.processCSVLoop(ctx, reader, &batchBuff, &counter, &currentBatchNumber)
}

// logConnectionAttempt registra el intento de conexión
func (c *Client) logConnectionAttempt(attempt, maxRetries int) {
	log.Infof(
		"action: connect | attempt: %d/%d | client_id: %v | server: %v",
		attempt,
		maxRetries,
		c.config.ID,
		c.config.ServerAddress,
	)
}

// handleConnectionSuccess maneja una conexión exitosa
func (c *Client) handleConnectionSuccess(conn net.Conn, attempt int) {
	c.conn = conn
	log.Infof(
		"action: connect | result: success | client_id: %v | attempt: %d",
		c.config.ID,
		attempt,
	)
}

// handleConnectionFailure maneja un fallo de conexión
func (c *Client) handleConnectionFailure(err error, attempt, maxRetries int) {
	log.Warningf(
		"action: connect | result: fail | client_id: %v | attempt: %d/%d | error: %v",
		c.config.ID,
		attempt,
		maxRetries,
		err,
	)
}

// waitBeforeRetry espera antes del siguiente intento si no es el último
func (c *Client) waitBeforeRetry(attempt, maxRetries int, retryInterval time.Duration) {
	if attempt < maxRetries {
		log.Infof(
			"action: connect_retry | waiting: %v | client_id: %v | next_attempt: %d",
			retryInterval,
			c.config.ID,
			attempt+1,
		)
		time.Sleep(retryInterval)
	}
}

// logAllRetriesFailed registra cuando todos los intentos fallan
func (c *Client) logAllRetriesFailed(maxRetries int, lastErr error) {
	log.Criticalf(
		"action: connect | result: fail_all_retries | client_id: %v | attempts: %d | error: %v",
		c.config.ID,
		maxRetries,
		lastErr,
	)
}

// attemptConnection intenta una sola conexión TCP
func (c *Client) attemptConnection() (net.Conn, error) {
	return net.Dial("tcp", c.config.ServerAddress)
}

// createClientSocket dials the configured ServerAddress and assigns the
// resulting connection to c.conn. It implements retry logic: 3 attempts
// with 2 second intervals between attempts. On failure it logs a critical message
// and returns the dial error; on success it returns nil.
func (c *Client) createClientSocket() error {
	const maxRetries = 3
	const retryInterval = 2 * time.Second

	var lastErr error

	for attempt := 1; attempt <= maxRetries; attempt++ {
		c.logConnectionAttempt(attempt, maxRetries)

		conn, err := c.attemptConnection()
		if err == nil {
			c.handleConnectionSuccess(conn, attempt)
			return nil
		}

		lastErr = err
		c.handleConnectionFailure(err, attempt, maxRetries)
		c.waitBeforeRetry(attempt, maxRetries, retryInterval)
	}

	c.logAllRetriesFailed(maxRetries, lastErr)
	return lastErr
}

// closeConnection safely closes the client connection and performs cleanup
func (c *Client) closeConnection() {
	if c.conn != nil {
		log.Infof("action: close_connection | client_id: %v", c.config.ID)
		c.conn.Close()
		c.conn = nil
	}
}

func (c *Client) SendBatch() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM)
	defer stop()

	if err := c.createClientSocket(); err != nil {
		return
	}
	defer c.conn.Close()

	readDone := make(chan struct{})
	readResponse(c.conn, readDone)

	lastErr := c.processTables(ctx)

	if lastErr == nil {
		c.sendFinished()
	}

	c.waitForResponses(ctx, readDone)
}

func (c *Client) processTables(ctx context.Context) error {
	dataDir := "./data"
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		log.Errorf("Error reading data directory: %v", err)
		return err
	}

	var lastErr error
	for _, entry := range entries {
		if entry.IsDir() {
			if err := c.processTableType(ctx, dataDir, entry.Name()); err != nil {
				lastErr = err
				if isConnectionError(err) {
					log.Criticalf("action: connection_lost | result: terminating | error: %v", err)
					c.closeConnection()
					return err
				}
			}
		}
	}
	return lastErr
}

func (c *Client) processTableType(ctx context.Context, dataDir, tableType string) error {
	subDirPath := filepath.Join(dataDir, tableType)
	log.Infof("action: processing_table_type | table_type: %s | client_id: %v", tableType, c.config.ID)

	if err := c.setHandlerForTableType(tableType); err != nil {
		log.Infof("action: skip_table_type | table_type: %s | reason: unsupported | client_id: %v | error: %v", tableType, c.config.ID, err)
		return nil
	}

	files, err := os.ReadDir(subDirPath)
	if err != nil {
		log.Errorf("Error reading subdirectory %s: %v", subDirPath, err)
		return err
	}

	for _, fileEntry := range files {
		if !fileEntry.IsDir() && filepath.Ext(fileEntry.Name()) == ".csv" {
			if err := c.processFileAndSendBatch(ctx, subDirPath, fileEntry.Name()); err != nil {
				return err
			}
			time.Sleep(1 * time.Second) // delay entre archivos
		}
	}
	return nil
}

// openAndPrepareFile abre el archivo y registra el inicio del procesamiento
func (c *Client) openAndPrepareFile(filePath string) (*os.File, error) {
	log.Infof("action: processing_file | file: %s", filePath)

	file, err := os.Open(filePath)
	if err != nil {
		log.Criticalf("action: read_file | result: fail | file: %s | error: %v", filePath, err)
		return nil, err
	}
	return file, nil
}

// setupCSVReader configura el lector CSV y salta el header
func (c *Client) setupCSVReader(file *os.File) (*csv.Reader, error) {
	reader := csv.NewReader(file)
	reader.Comma = ','
	reader.FieldsPerRecord = -1

	// Skip header
	if _, err := reader.Read(); err != nil {
		log.Criticalf("action: read_header | result: fail | error: %v", err)
		return nil, err
	}
	return reader, nil
}

// processFileAsync procesa el archivo de forma asíncrona y maneja los resultados
func (c *Client) processFileAsync(ctx context.Context, reader *csv.Reader, fileName string) error {
	writeDone := make(chan error, 1)
	go func() {
		writeDone <- c.buildAndSendBatches(ctx, reader, 0) // batchNumber reseteado en cada archivo
	}()

	if err := <-writeDone; err != nil && !errors.Is(err, context.Canceled) {
		log.Errorf("action: send_batches | result: fail | file: %s | error: %v", fileName, err)
		return err
	}

	log.Infof("action: completed_processing_file | file: %s | result: success", fileName)
	return nil
}

func (c *Client) processFileAndSendBatch(ctx context.Context, dir, fileName string) error {
	filePath := filepath.Join(dir, fileName)

	file, err := c.openAndPrepareFile(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	reader, err := c.setupCSVReader(file)
	if err != nil {
		return err
	}

	return c.processFileAsync(ctx, reader, fileName)
}

func (c *Client) waitForResponses(ctx context.Context, readDone chan struct{}) {
	select {
	case <-ctx.Done():
		_ = c.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		<-readDone
	case <-readDone:
		if tcp, ok := c.conn.(*net.TCPConn); ok {
			_ = tcp.CloseWrite()
		}
	}
}

// handleReadError maneja los diferentes tipos de errores de lectura
func handleReadError(err error) bool {
	if errors.Is(err, io.EOF) {
		log.Infof("action: leer_respuesta | result: server_closed_connection")
	} else if isConnectionError(err) {
		log.Criticalf("action: leer_respuesta | result: connection_lost | err: %v", err)
	} else {
		log.Errorf("action: leer_respuesta | result: fail | err: %v", err)
	}
	return true // Indica que se debe terminar el loop
}

// handleResponseMessage procesa un mensaje de respuesta recibido
func handleResponseMessage(msg interface{}) {
	// Usamos type assertion para acceder al método GetOpCode()
	if respMsg, ok := msg.(interface{ GetOpCode() byte }); ok {
		switch respMsg.GetOpCode() {
		case BetsRecvSuccessOpCode:
			log.Info("action: bets_enviadas | result: success")
		case BetsRecvFailOpCode:
			log.Error("action: bets_enviadas | result: fail")
		}
	}
}

// responseReaderLoop ejecuta el loop principal de lectura de respuestas
func responseReaderLoop(reader *bufio.Reader) {
	for {
		msg, err := ReadMessage(reader)
		if err != nil {
			if handleReadError(err) {
				break
			}
			continue
		}
		handleResponseMessage(msg)
	}
}

// readResponse consumes server responses from conn in a dedicated goroutine.
// It logs per-message results and terminates when:
//   - an I/O error occurs (EOF included), or
//
// The function closes readDone when the goroutine exits.
func readResponse(conn net.Conn, readDone chan struct{}) {
	reader := bufio.NewReader(conn)
	go func() {
		responseReaderLoop(reader)
		close(readDone)
	}()
}

// sendFinishedAndAskForWinners sends FINISHED (with the numeric agency ID).
// It logs success or failure for each write. On any serialization/I/O error it logs and returns.
func (c *Client) sendFinished() {
	agencyId, err := strconv.Atoi(c.config.ID)
	if err != nil {
		log.Errorf("action: send_finished | result: fail | error: %v", err)
		return
	}

	finishedMsg := Finished{int32(agencyId)}
	if _, err := finishedMsg.WriteTo(c.conn); err != nil {
		log.Errorf("action: send_finished | result: fail | error: %v", err)
		return
	}

	log.Infof("action: send_finished | result: success | agencyId: %d", int32(agencyId))
}
