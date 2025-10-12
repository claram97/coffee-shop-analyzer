package common

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/op/go-logging"

	// Import protocol definitions
	protocol "github.com/7574-sistemas-distribuidos/docker-compose-init/client/protocol"
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
	} else if IsConnectionError(err) {
		rh.log.Criticalf("action: leer_respuesta | result: connection_lost | err: %v", err)
	} else {
		rh.log.Errorf("action: leer_respuesta | result: fail | err: %v", err)
	}
	return true // Indicates the loop should terminate
}

// handleResponseMessage processes a received response message
func (rh *ResponseHandler) handleResponseMessage(msg interface{}) {
	respMsg, ok := msg.(interface{ GetOpCode() byte })
	if !ok {
		rh.log.Warningf("action: response_received | result: unknown_message | type: %T", msg)
		return
	}

	opcode := respMsg.GetOpCode()
	if opcode != protocol.BatchRecvSuccessOpCode && opcode != protocol.BatchRecvFailOpCode {
		rh.log.Infof("action: response_received | where: response_handling | opcode: %d", opcode)
	}

	switch typed := msg.(type) {
	case *protocol.BatchRecvSuccess:
		rh.log.Debug("action: batch_enviado | result: success")
	case *protocol.BatchRecvFail:
		rh.log.Error("action: batch_enviado | result: fail")
	case *protocol.DataBatch:
		rh.handleQueryResult(typed)
	case *protocol.QueryResultTable:
		rh.handleQueryResultTable("direct", "", typed)
	default:
		switch opcode {
		case protocol.OpCodeQueryResult1, protocol.OpCodeQueryResult2, protocol.OpCodeQueryResult3, protocol.OpCodeQueryResult4:
			rh.log.Infof("action: query_result_received | result: success | opcode: %d", opcode)
		case protocol.OpCodeQueryResultError:
			rh.log.Error("action: query_result_received | result: error")
		}
	}
}

// handleQueryResult processes a query result message
func (rh *ResponseHandler) handleQueryResult(dataBatch *protocol.DataBatch) {
	opcode := dataBatch.InnerOpcode
	queryID := ""
	if len(dataBatch.QueryIDs) > 0 {
		queryID = strconv.Itoa(int(dataBatch.QueryIDs[0]))
	}

	if !isKnownQueryOpcode(opcode) {
		rh.log.Warningf("action: query_result_received | result: unexpected_inner_message | opcode: %d", opcode)
		return
	}

	rh.handleQueryResultTable("databatch", queryID, dataBatch.ResultTable)
}

func (rh *ResponseHandler) handleQueryResultTable(source, queryID string, table *protocol.QueryResultTable) {
	if table == nil {
		rh.log.Warningf("action: query_result_received | source: %s | query_id: %s | result: missing_table_payload", source, queryID)
		return
	}

	opcodeName := queryOpcodeName(table.OpCode)
	statusText := batchStatusText(table.BatchStatus)
	rowCount := len(table.Rows)

	fields := []string{
		fmt.Sprintf("source: %s", source),
		fmt.Sprintf("opcode: %d (%s)", table.OpCode, opcodeName),
		fmt.Sprintf("status: %s", statusText),
		fmt.Sprintf("rows: %d", rowCount),
	}
	if queryID != "" {
		fields = append([]string{fmt.Sprintf("query_id: %s", queryID)}, fields...)
	}

	if table.OpCode == protocol.OpCodeQueryResultError {
		rh.log.Errorf("action: query_result_received | %s", strings.Join(fields, " | "))
	} else {
		rh.log.Infof("action: query_result_received | %s", strings.Join(fields, " | "))
	}

	for idx, row := range table.Rows {
		rh.log.Debugf("action: query_result_row | source: %s | query_id: %s | index: %d/%d | data: %+v", source, queryID, idx+1, rowCount, row)
	}

	if table.OpCode == protocol.OpCodeQueryResultError {
		for _, row := range table.Rows {
			rh.log.Errorf("action: query_result_error_detail | query_id: %s | code: %s | message: %s", queryID, row["error_code"], row["error_message"])
		}
	}
}

func queryOpcodeName(opcode byte) string {
	switch opcode {
	case protocol.OpCodeQueryResult1:
		return "Q1 filtered_transactions"
	case protocol.OpCodeQueryResult2:
		return "Q2 product_metrics"
	case protocol.OpCodeQueryResult3:
		return "Q3 tpv_analysis"
	case protocol.OpCodeQueryResult4:
		return "Q4 top_customers"
	case protocol.OpCodeQueryResultError:
		return "error"
	default:
		return fmt.Sprintf("unknown(%d)", opcode)
	}
}

func batchStatusText(status byte) string {
	switch status {
	case protocol.BatchContinue:
		return "CONTINUE"
	case protocol.BatchEOF:
		return "EOF"
	case protocol.BatchCancel:
		return "CANCEL"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", status)
	}
}

func isKnownQueryOpcode(opcode byte) bool {
	switch opcode {
	case protocol.OpCodeQueryResult1, protocol.OpCodeQueryResult2, protocol.OpCodeQueryResult3, protocol.OpCodeQueryResult4, protocol.OpCodeQueryResultError:
		return true
	default:
		return false
	}
}

// responseReaderLoop executes the main response reading loop
func (rh *ResponseHandler) responseReaderLoop(reader *bufio.Reader) {
	for {
		msg, err := protocol.ReadMessage(reader, rh.log)

		if err != nil {
			if rh.handleReadError(err) {
				break
			}
			continue
		}
		opcode := msg.(interface{ GetOpCode() byte }).GetOpCode()
		if opcode != protocol.BatchRecvSuccessOpCode && opcode != protocol.BatchRecvFailOpCode {
			rh.log.Infof("action: response_received | where: response_reading_loop| opcode: %d", opcode)
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

	finishedMsg := protocol.Finished{AgencyId: int32(agencyId)}
	if _, err := finishedMsg.WriteTo(fms.conn); err != nil {
		fms.log.Errorf("action: send_finished | result: fail | error: %v", err)
		return
	}

	fms.log.Infof("action: send_finished | result: success | agencyId: %d", int32(agencyId))
}
