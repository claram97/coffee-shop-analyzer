package common

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"strconv"
	"time"

	"github.com/op/go-logging"
)

// ResponseHandler handles server response communication
type ResponseHandler struct {
	conn     net.Conn
	clientID string
	log      *logging.Logger
}

// NewResponseHandler creates a new response handler
func NewResponseHandler(conn net.Conn, clientID string, logger *logging.Logger) *ResponseHandler {
	return &ResponseHandler{
		conn:     conn,
		clientID: clientID,
		log:      logger,
	}
}

// handleReadError handles different types of reading errors
func (rh *ResponseHandler) handleReadError(err error) bool {
	if errors.Is(err, io.EOF) {
		rh.log.Infof("action: leer_respuesta | result: server_closed_connection")
	} else if isConnectionError(err) {
		rh.log.Criticalf("action: leer_respuesta | result: connection_lost | err: %v", err)
	} else {
		rh.log.Errorf("action: leer_respuesta | result: fail | err: %v", err)
	}
	return true // Indicates the loop should terminate
}

// handleResponseMessage processes a received response message
func (rh *ResponseHandler) handleResponseMessage(msg interface{}) {
	// Use type assertion to access the GetOpCode() method
	if respMsg, ok := msg.(interface{ GetOpCode() byte }); ok {
		switch respMsg.GetOpCode() {
		case BetsRecvSuccessOpCode:
			rh.log.Info("action: bets_enviadas | result: success")
		case BetsRecvFailOpCode:
			rh.log.Error("action: bets_enviadas | result: fail")
		}
	}
}

// responseReaderLoop executes the main response reading loop
func (rh *ResponseHandler) responseReaderLoop(reader *bufio.Reader) {
	for {
		msg, err := ReadMessage(reader)
		if err != nil {
			if rh.handleReadError(err) {
				break
			}
			continue
		}
		rh.handleResponseMessage(msg)
	}
}

// ReadResponses consumes server responses from connection in a dedicated goroutine.
// It logs per-message results and terminates when an I/O error occurs (EOF included).
// The function closes readDone when the goroutine exits.
func (rh *ResponseHandler) ReadResponses(readDone chan struct{}) {
	reader := bufio.NewReader(rh.conn)
	go func() {
		rh.responseReaderLoop(reader)
		close(readDone)
	}()
}

// WaitForResponses waits for response processing to complete with context support
func (rh *ResponseHandler) WaitForResponses(ctx context.Context, readDone chan struct{}) {
	select {
	case <-ctx.Done():
		_ = rh.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		<-readDone
	case <-readDone:
		if tcp, ok := rh.conn.(*net.TCPConn); ok {
			_ = tcp.CloseWrite()
		}
	}
}

// FinishedMessageSender handles sending the FINISHED message
type FinishedMessageSender struct {
	conn     net.Conn
	clientID string
	log      *logging.Logger
}

// NewFinishedMessageSender creates a new finished message sender
func NewFinishedMessageSender(conn net.Conn, clientID string, logger *logging.Logger) *FinishedMessageSender {
	return &FinishedMessageSender{
		conn:     conn,
		clientID: clientID,
		log:      logger,
	}
}

// SendFinished sends FINISHED message with the numeric agency ID.
// It logs success or failure for each write. On any serialization/I/O error it logs and returns.
func (fms *FinishedMessageSender) SendFinished() {
	agencyId, err := strconv.Atoi(fms.clientID)
	if err != nil {
		fms.log.Errorf("action: send_finished | result: fail | error: %v", err)
		return
	}

	finishedMsg := Finished{int32(agencyId)}
	if _, err := finishedMsg.WriteTo(fms.conn); err != nil {
		fms.log.Errorf("action: send_finished | result: fail | error: %v", err)
		return
	}

	fms.log.Infof("action: send_finished | result: success | agencyId: %d", int32(agencyId))
}
