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
	"strconv"
	"syscall"
	"time"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

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

type PaymentMethodHandler struct{}

func (p PaymentMethodHandler) ProcessRecord(record []string) (map[string]string, error) {
	return map[string]string{
		"method_id":   record[0],
		"method_name": record[1],
		"category":    record[2],
	}, nil
}

func (p PaymentMethodHandler) GetExpectedFields() int { return 3 }

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
		"voucher_id":        record[3],
		"user_id":           record[4],
		"original_amount":   record[5],
		"discount_applied":  record[6],
		"final_amount":      record[7],
		"created_at":        record[8],
	}, nil
}

func (t TransactionHandler) GetExpectedFields() int { return 9 }

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

type VoucherHandler struct{}

func (v VoucherHandler) ProcessRecord(record []string) (map[string]string, error) {
	return map[string]string{
		"voucher_id":     record[0],
		"voucher_code":   record[1],
		"discount_type":  record[2],
		"discount_value": record[3],
		"valid_from":     record[4],
		"valid_to":       record[5],
	}, nil
}

func (v VoucherHandler) GetExpectedFields() int { return 6 }

// --- NUEVO: Implementación específica para Apuestas ---
type BetHandler struct{}

func (b BetHandler) ProcessRecord(record []string) (map[string]string, error) {
	// Esta es la misma lógica que antes estaba en processNextBet
	return map[string]string{
		"NOMBRE":     record[0],
		"APELLIDO":   record[1],
		"DOCUMENTO":  record[2],
		"NACIMIENTO": record[3],
		"NUMERO":     record[4],
	}, nil
}

func (b BetHandler) GetExpectedFields() int { return 5 }

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
func NewClient(config ClientConfig, handler TableRowHandler, opCode byte) *Client {
	return &Client{
		config:  config,
		handler: handler,
		opCode:  opCode,
	}
}

func (c *Client) ChangeClientTableType(handler TableRowHandler, opCode byte) error {
	c.handler = handler
	c.opCode = opCode
	return nil
}

// processNextBet reads a single CSV record from betsReader, converts it
// to the protocol key/value map (including AGENCIA), and attempts to add
// it to the current batch buffer via AddBetWithFlush. If adding this bet
// would exceed either the 8 KiB framing limit or the configured BatchLimit,
// the function triggers a flush of the current batch to c.conn and then
// starts a new batch with this bet. The returned error is io.EOF when the
// CSV is exhausted, or any I/O/serialization error encountered.
func (c *Client) processNextRow(reader *csv.Reader, batchBuff *bytes.Buffer, counter *int32) error {
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
	if err := AddRowToBatch(rowMap, batchBuff, c.conn, counter, c.config.BatchLimit, c.opCode); err != nil {
		return err
	}
	return nil
}

// buildAndSendBatches streams the CSV, incrementally building NewBets
// bodies into batchBuff and flushing to c.conn as limits are reached.
// On context cancellation, it flushes any partial batch and returns the
// context error. On clean EOF, it flushes a final partial batch (if any)
// and returns nil. Any serialization or socket error is returned.
func (c *Client) buildAndSendBatches(ctx context.Context, reader *csv.Reader) error {
	var batchBuff bytes.Buffer
	var counter int32 = 0
	for {
		select {
		case <-ctx.Done():
			if counter > 0 {
				if err := FlushBatch(&batchBuff, c.conn, counter, c.opCode); err != nil { // <-- Usa c.opCode
					return err
				}
				counter = 0
			}
			return ctx.Err()
		default:
		}

		if err := c.processNextRow(reader, &batchBuff, &counter); err != nil {
			if errors.Is(err, io.EOF) {
				if counter > 0 {
					if err := FlushBatch(&batchBuff, c.conn, counter, c.opCode); err != nil { // <-- Usa c.opCode
						return err
					}
				}
				break
			}
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

// SendBets is the high-level entry point. It:
//  1. Opens the CSV and connects to the server.
//  2. Starts a reader goroutine (readResponse) to consume server replies.
//  3. Builds and streams batches (buildAndSendBatches) until EOF or cancellation.
//  4. On success, sends FINISHED.
//  5. Waits for either context cancellation or the reader goroutine to finish.
//
// It guarantees connection closure on exit and uses deadlines to unblock
// the reader goroutine on cancellation.
// func (c *Client) SendBets() {
// 	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM)
// 	defer stop()

// 	file, err := os.Open(c.config.BetsFilePath)
// 	if err != nil {
// 		log.Criticalf("action: read_file | result: fail | error: %v", err)
// 		return
// 	}
// 	defer file.Close()

// 	reader := csv.NewReader(file)
// 	reader.Comma = ','
// 	// Ignorar encabezado
// 	if _, err := reader.Read(); err != nil {
// 		log.Criticalf("action: read_file | result: fail | error: encabezado: %v", err)
// 		return
// 	}
// 	// La validación de campos ahora es dinámica, usando la interfaz.
// 	reader.FieldsPerRecord = -1 // Allow variable field counts, validate in handler

// 	if err := c.createClientSocket(); err != nil {
// 		return
// 	}
// 	defer c.conn.Close()

// 	writeDone := make(chan error, 1)
// 	go func() {
// 		writeDone <- c.buildAndSendBatches(ctx, reader)
// 	}()

// 	conn := c.conn
// 	readDone := make(chan struct{})
// 	readResponse(conn, readDone)

// 	if err = <-writeDone; err != nil && !errors.Is(err, context.Canceled) {
// 		log.Errorf("action: send_bets | result: fail | error: %v", err)
// 		return
// 	}

//		if err == nil {
//			c.sendFinished()
//		}
//		select {
//		case <-ctx.Done():
//			_ = c.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
//			<-readDone
//			return
//		case <-readDone:
//			if tcp, ok := c.conn.(*net.TCPConn); ok {
//				_ = tcp.CloseWrite()
//			}
//		}
//	}

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

	// Loop configuration
	iterations := 4
	var lastErr error

	for iteration := 1; iteration <= iterations; iteration++ {
		log.Infof("Starting iteration %d/%d", iteration, iterations)
		var file *os.File
		var err error
		// Open the CSV file for this iteration
		if iteration == 1 {
			file, err = os.Open(c.config.BetsFilePath)
		} else {
			c.ChangeClientTableType(MenuItemHandler{}, OpCodeNewMenuItems)
			file, err = os.Open("./menu_items.csv")
		}

		if err != nil {
			log.Criticalf("action: read_file | result: fail | iteration: %d | error: %v", iteration, err)
			return
		}

		reader := csv.NewReader(file)
		reader.Comma = ','
		// Skip header
		if _, err := reader.Read(); err != nil {
			log.Criticalf("action: read_file | result: fail | error: encabezado: %v", err)
			file.Close()
			return
		}
		reader.FieldsPerRecord = -1

		writeDone := make(chan error, 1)
		go func() {
			writeDone <- c.buildAndSendBatches(ctx, reader)
		}()

		lastErr = <-writeDone
		if lastErr != nil && !errors.Is(lastErr, context.Canceled) {
			log.Errorf("action: send_bets | result: fail | iteration: %d | error: %v", iteration, lastErr)
			file.Close()
			return
		}

		file.Close()
		log.Infof("Completed iteration %d/%d", iteration, iterations)

		// Add delay between iterations
		if iteration < iterations {
			time.Sleep(1 * time.Second)
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
				if !errors.Is(err, io.EOF) {
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
