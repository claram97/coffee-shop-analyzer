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

// --- NUEVO: Interfaz para manejar cualquier tipo de fila de tabla ---
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
		"product_id":     record[0], // item_id from CSV
		"name":           record[1], // item_name from CSV
		"category":       record[2], // category from CSV
		"price":          record[3], // price from CSV
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

// buildAndSendBatches streams the CSV, incrementally building table data
// bodies into batchBuff and flushing to c.conn as limits are reached.
// On context cancellation, it flushes any partial batch and returns the
// context error. On clean EOF, it flushes a final partial batch (if any)
// and returns nil. Any serialization or socket error is returned.
func (c *Client) buildAndSendBatches(ctx context.Context, reader *csv.Reader, batchNumber int64) error {
	var batchBuff bytes.Buffer
	var counter int32 = 0
	currentBatchNumber := batchNumber // Número del batch actual (no se incrementa por línea)

	// Loop principal que procesa línea por línea del archivo CSV actual
	for {

		// Verificar si se recibió señal de cancelación (Ctrl+C, SIGTERM, etc.)
		select {
		case <-ctx.Done():
			// Si hay datos pendientes en el buffer, enviarlos antes de terminar
			if counter > 0 {
				currentBatchNumber++ // Incrementar solo cuando realmente enviamos un batch
				if err := FlushBatch(&batchBuff, c.conn, counter, c.opCode, currentBatchNumber, BatchCancel); err != nil {
					return err
				}
				counter = 0
			}
			return ctx.Err() // Retornar el error de cancelación
		default:
			// Continuar procesamiento normal si no hay cancelación
		}

		// Leer y procesar la siguiente línea del CSV
		if err := c.processNextRow(reader, &batchBuff, &counter, &currentBatchNumber); err != nil {
			// Si llegamos al final del archivo (EOF = End Of File)
			if errors.Is(err, io.EOF) {
				// Enviar cualquier batch parcial que quede en el buffer
				if counter > 0 {
					currentBatchNumber++ // Incrementar solo cuando realmente enviamos un batch
					if err := FlushBatch(&batchBuff, c.conn, counter, c.opCode, currentBatchNumber, BatchEOF); err != nil {
						return err
					}
				}
				break // Salir del loop - archivo completamente procesado
			}
			// Si es cualquier otro error (CSV malformado, I/O, etc.), propagarlo
			return err
		}
	}
	return nil
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
		log.Infof(
			"action: connect | attempt: %d/%d | client_id: %v | server: %v",
			attempt,
			maxRetries,
			c.config.ID,
			c.config.ServerAddress,
		)

		conn, err := net.Dial("tcp", c.config.ServerAddress)
		if err == nil {
			c.conn = conn
			log.Infof(
				"action: connect | result: success | client_id: %v | attempt: %d",
				c.config.ID,
				attempt,
			)
			return nil
		}

		lastErr = err
		log.Warningf(
			"action: connect | result: fail | client_id: %v | attempt: %d/%d | error: %v",
			c.config.ID,
			attempt,
			maxRetries,
			err,
		)

		// Si no es el último intento, esperar antes del siguiente
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

	// Si llegamos aquí, todos los intentos fallaron
	log.Criticalf(
		"action: connect | result: fail_all_retries | client_id: %v | attempts: %d | error: %v",
		c.config.ID,
		maxRetries,
		lastErr,
	)
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

func (c *Client) SendBets() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM)
	defer stop()

	if err := c.createClientSocket(); err != nil {
		return
	}
	defer c.conn.Close()

	// Start the response reader ONCE, outside the loop
	readDone := make(chan struct{})
	readResponse(c.conn, readDone)

	// Loop configuration - now based on directory structure
	var lastErr error
	var batchNumber int64 = 1
	// Read .data directory to get all subdirectories/table types
	dataDir := "./data"
	entries, dirErr := os.ReadDir(dataDir)
	if dirErr != nil {
		log.Errorf("Error reading data directory: %v", dirErr)
		return
	}

	// Loop through each subdirectory (table type)
	for _, entry := range entries {
		if entry.IsDir() {
			subDirPath := filepath.Join(dataDir, entry.Name())
			tableType := entry.Name()

			log.Infof("action: processing_table_type | table_type: %s | client_id: %v", tableType, c.config.ID)

			// Set appropriate handler and opcode based on table type
			if err := c.setHandlerForTableType(tableType); err != nil {
				log.Infof("action: skip_table_type | table_type: %s | reason: unsupported | client_id: %v | error: %v", tableType, c.config.ID, err)
				continue
			}

			// Read files in this subdirectory
			files, err := os.ReadDir(subDirPath)
			if err != nil {
				log.Errorf("Error reading subdirectory %s: %v", subDirPath, err)
				continue
			}

			// Process each file in this table type directory
			for _, fileEntry := range files {
				batchNumber = 0 // Reset batch number for each new file
				if !fileEntry.IsDir() && filepath.Ext(fileEntry.Name()) == ".csv" {
					fileName := fileEntry.Name()
					filePath := filepath.Join(subDirPath, fileName)
					log.Infof("action: processing_file: %s | result: success", filePath)

					var file *os.File
					file, err = os.Open(filePath)
					if err != nil {
						log.Criticalf("action: read_file | result: fail | file: %s | error: %v", filePath, err)
						continue
					}

					reader := csv.NewReader(file)
					reader.Comma = ','
					// Skip header
					if _, err := reader.Read(); err != nil {
						log.Criticalf("action: read_file | result: fail | error: encabezado: %v", err)
						file.Close()
						continue
					}
					reader.FieldsPerRecord = -1

					writeDone := make(chan error, 1)
					go func() {
						writeDone <- c.buildAndSendBatches(ctx, reader, batchNumber)
					}()

					lastErr = <-writeDone
					if lastErr != nil && !errors.Is(lastErr, context.Canceled) {
						log.Errorf("action: send_bets | result: fail | file: %s | error: %v", filePath, lastErr)
						file.Close()

						// Si es un error de conexión, terminar el programa completamente
						if isConnectionError(lastErr) {
							log.Criticalf("action: connection_lost | result: terminating | error: %v", lastErr)
							c.closeConnection()
							return // Salir de SendBets completamente
						}
						continue
					}

					file.Close()
					log.Infof("action: completed_processing_file: %s | result: success", fileName)

					// Add delay between files
					time.Sleep(1 * time.Second)
				}
			}
		}
	}

	// Send FINISHED only after all iterations are complete
	if lastErr == nil {
		c.sendFinished()
	}

	// Wait for final responses and cleanup
	select {
	case <-ctx.Done():
		_ = c.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		<-readDone
		return
	case <-readDone:
		if tcp, ok := c.conn.(*net.TCPConn); ok {
			_ = tcp.CloseWrite()
		}
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
		for {
			msg, err := ReadMessage(reader)
			if err != nil {
				if errors.Is(err, io.EOF) {
					log.Infof("action: leer_respuesta | result: server_closed_connection")
				} else if isConnectionError(err) {
					log.Criticalf("action: leer_respuesta | result: connection_lost | err: %v", err)
				} else {
					log.Errorf("action: leer_respuesta | result: fail | err: %v", err)
				}
				break
			}
			switch msg.GetOpCode() {
			case BetsRecvSuccessOpCode:
				log.Info("action: bets_enviadas | result: success")
			case BetsRecvFailOpCode:
				log.Error("action: bets_enviadas | result: fail")
			}
		}
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
