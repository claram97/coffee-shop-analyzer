package common

import (
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"errors"
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
	config ClientConfig
	conn   net.Conn
}

// NewClient constructs a Client with the provided configuration.
// The TCP connection is not opened here; see createClientSocket / SendBets.
func NewClient(config ClientConfig) *Client {
	client := &Client{
		config: config,
	}
	return client
}

// processNextBet reads a single CSV record from betsReader, converts it
// to the protocol key/value map (including AGENCIA), and attempts to add
// it to the current batch buffer via AddBetWithFlush. If adding this bet
// would exceed either the 8 KiB framing limit or the configured BatchLimit,
// the function triggers a flush of the current batch to c.conn and then
// starts a new batch with this bet. The returned error is io.EOF when the
// CSV is exhausted, or any I/O/serialization error encountered.
func (c *Client) processNextBet(betsReader *csv.Reader, batchBuff *bytes.Buffer, betsCounter *int32) error {
	betFields, err := betsReader.Read()
	if err != nil {
		return err
	}
	bet := map[string]string{
		"NOMBRE":     betFields[0],
		"APELLIDO":   betFields[1],
		"DOCUMENTO":  betFields[2],
		"NACIMIENTO": betFields[3],
		"NUMERO":     betFields[4],
	}
	if err := AddBetWithFlush(bet, batchBuff, c.conn, betsCounter, c.config.BatchLimit); err != nil {
		return err
	}
	return nil
}

// buildAndSendBatches streams the CSV, incrementally building NewBets
// bodies into batchBuff and flushing to c.conn as limits are reached.
// On context cancellation, it flushes any partial batch and returns the
// context error. On clean EOF, it flushes a final partial batch (if any)
// and returns nil. Any serialization or socket error is returned.
func (c *Client) buildAndSendBatches(ctx context.Context, betsReader *csv.Reader) error {
	var batchBuff bytes.Buffer
	var betsCounter int32 = 0
	for {
		select {
		case <-ctx.Done():
			if betsCounter > 0 {
				if err := FlushBatch(&batchBuff, c.conn, betsCounter); err != nil {
					return err
				}
				betsCounter = 0
			}
			return ctx.Err()
		default:
		}
		if err := c.processNextBet(betsReader, &batchBuff, &betsCounter); err != nil {
			if errors.Is(err, io.EOF) {
				if betsCounter > 0 {
					if err := FlushBatch(&batchBuff, c.conn, betsCounter); err != nil {
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
	conn, err := net.Dial("tcp", c.config.ServerAddress)
	if err != nil {
		log.Criticalf(
			"action: connect | result: fail | client_id: %v | error: %v",
			c.config.ID,
			err,
		)
		return err
	}
	c.conn = conn
	return nil
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
func (c *Client) SendBets() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM)
	defer stop()

	betsFile, err := os.Open(c.config.BetsFilePath)
	if err != nil {
		log.Criticalf("action: read_bets | result: fail | error: %v", err)
		return
	}
	defer betsFile.Close()

	betsReader := csv.NewReader(betsFile)
	betsReader.Comma = ','
	betsReader.FieldsPerRecord = 5

	if err := c.createClientSocket(); err != nil {
		return
	}
	defer c.conn.Close()

	writeDone := make(chan error, 1)
	go func() {
		writeDone <- c.buildAndSendBatches(ctx, betsReader)
	}()

	conn := c.conn
	readDone := make(chan struct{})
	readResponse(conn, readDone)

	if err = <-writeDone; err != nil && !errors.Is(err, context.Canceled) {
		log.Errorf("action: send_bets | result: fail | error: %v", err)
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
//   - an I/O error occurs (EOF included), or
//   
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