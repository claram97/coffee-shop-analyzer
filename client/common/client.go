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
	config     ClientConfig
	conn       net.Conn
	csvHeaders []string
}

// prepareToProcess lee la primera fila del CSV (los encabezados)
// y los guarda en el cliente para su uso posterior.
func (c *Client) prepareToProcess(tableReader *csv.Reader) error {
	headers, err := tableReader.Read()
	if err != nil {
		// Podría ser io.EOF si el archivo está vacío, o un error de parseo
		return fmt.Errorf("error al leer los encabezados del CSV: %w", err)
	}
	c.csvHeaders = headers
	return nil
}

// NewClient constructs a Client with the provided configuration.
// The TCP connection is not opened here; see createClientSocket / SendBets.
func NewClient(config ClientConfig) *Client {
	client := &Client{
		config: config,
	}
	return client
}

// processNextBet lee un registro, lo convierte en un mapa usando los
// encabezados leídos previamente y lo añade al batch.
func (c *Client) processNextLine(tableReader *csv.Reader, batchBuff *bytes.Buffer, linesCounter *int32) error {
	lineFields, err := tableReader.Read()
	if err != nil {
		return err // Devuelve io.EOF al final del archivo, o un error de lectura.
	}

	// --- LÓGICA MODIFICADA ---

	// 1. Validar que la cantidad de campos coincida con los encabezados.
	if len(lineFields) != len(c.csvHeaders) {
		return fmt.Errorf("error de formato: la fila tiene %d campos, pero se esperaban %d (basado en los encabezados)", len(lineFields), len(c.csvHeaders))
	}

	// 2. Crear el mapa dinámicamente.
	bet := make(map[string]string)
	bet["AGENCIA"] = c.config.ID // Añadir el campo fijo.

	for i, field := range lineFields {
		// Asigna el valor del campo a la clave correspondiente del encabezado.
		key := c.csvHeaders[i]
		bet[key] = field
	}

	// --- FIN DE LA LÓGICA MODIFICADA ---

	// La lógica de envío se mantiene igual.
	if err := AddLineWithFlush(bet, batchBuff, c.conn, linesCounter, c.config.BatchLimit); err != nil {
		return err
	}

	return nil
}

// buildAndSendBatches streams the CSV, incrementally building NewBets
// bodies into batchBuff and flushing to c.conn as limits are reached.
// On context cancellation, it flushes any partial batch and returns the
// context error. On clean EOF, it flushes a final partial batch (if any)
// and returns nil. Any serialization or socket error is returned.
func (c *Client) buildAndSendBatches(ctx context.Context, tableReader *csv.Reader) error {
	var batchBuff bytes.Buffer
	var linesCounter int32 = 0
	for {
		select {
		case <-ctx.Done():
			if linesCounter > 0 {
				if err := FlushBatch(&batchBuff, c.conn, linesCounter); err != nil {
					return err
				}
				linesCounter = 0
			}
			return ctx.Err()
		default:
		}
		if err := c.processNextLine(tableReader, &batchBuff, &linesCounter); err != nil {
			if errors.Is(err, io.EOF) {
				if linesCounter > 0 {
					if err := FlushBatch(&batchBuff, c.conn, linesCounter); err != nil {
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
// resulting connection to c.conn. On failure it logs a critical message
// and returns the dial error; on success it returns nil.
func (c *Client) createClientSocket() error {
	const maxAttempts = 3
	var lastErr error

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		conn, err := net.Dial("tcp", c.config.ServerAddress)
		if err == nil {
			c.conn = conn
			if attempt > 1 {
				log.Infof("action: connect_retry | result: success | attempt: %d | client_id: %v", attempt, c.config.ID)
			}
			return nil
		}

		lastErr = err
		if attempt < maxAttempts {
			log.Warningf("action: connect | result: retrying | attempt: %d/%d | client_id: %v | error: %v", attempt, maxAttempts, c.config.ID, err)
			time.Sleep(2 * time.Second)
			continue
		}
	}

	log.Criticalf(
		"action: connect | result: fail | client_id: %v | error: %v",
		c.config.ID,
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
func (c *Client) SendTable() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM)
	defer stop()

	tableFile, err := os.Open(c.config.BetsFilePath)
	if err != nil {
		log.Criticalf("action: read_table | result: fail | error: %v", err)
		return
	}
	defer tableFile.Close()

	tableReader := csv.NewReader(tableFile)
	tableReader.Comma = ','

	if err := c.prepareToProcess(tableReader); err != nil {
		log.Criticalf("action: read_csv_headers | result: fail | error: %v", err)
		return
	}

	if err := c.createClientSocket(); err != nil {
		return
	}
	defer c.conn.Close()

	writeDone := make(chan error, 1)
	go func() {
		writeDone <- c.buildAndSendBatches(ctx, tableReader)
	}()

	conn := c.conn
	readDone := make(chan struct{})
	readResponse(conn, readDone)

	if err = <-writeDone; err != nil && !errors.Is(err, context.Canceled) {
		log.Errorf("action: send_table | result: fail | error: %v", err)
		return
	}

	if err == nil {
		c.sendFinished()
	}
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
//   - an I/O error occurs (EOF included)
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
			case LinesRecvSuccessOpCode:
				log.Info("action: tabla_enviada | result: success")
			case LinesRecvFailOpCode:
				log.Error("action: tabla_enviada | result: fail")
			}
		}
		close(readDone)
	}()
}

// sendFinished sends FINISHED (with the numeric agency ID).
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
