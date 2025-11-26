package common

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/op/go-logging"

	protocol "github.com/7574-sistemas-distribuidos/docker-compose-init/client/protocol"
)

// ResponseHandler handles server response communication
type ResponseHandler struct {
	conn          net.Conn
	clientID      string
	outputBaseDir string
	log           *logging.Logger
}

// NewResponseHandler creates a new response handler
func NewResponseHandler(conn net.Conn, clientID string, outputBaseDir string, logger *logging.Logger) *ResponseHandler {
	return &ResponseHandler{
		conn:          conn,
		clientID:      clientID,
		outputBaseDir: outputBaseDir,
		log:           logger,
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
	// Use type assertion to access the GetOpCode() method
	if respMsg, ok := msg.(interface{ GetOpCode() byte }); ok {
		opcode := respMsg.GetOpCode()

		switch opcode {
		case protocol.BatchRecvSuccessOpCode:
			rh.log.Debug("action: batch_enviado | result: success")
		case protocol.BatchRecvFailOpCode:
			rh.log.Error("action: batch_enviado | result: fail")
		case protocol.OpCodeQueryResult1, protocol.OpCodeQueryResult2, protocol.OpCodeQueryResult3, protocol.OpCodeQueryResult4, protocol.OpCodeQueryResultError:
			// Handle query results
			if queryResult, ok := respMsg.(*protocol.QueryResultTable); ok {
				rh.handleQueryResultTable(queryResult)
			} else {
				rh.log.Warning("action: response_received | result: unexpected_format | type: query_result")
			}
		case protocol.OpCodeDataBatch:
			rh.log.Infof("action: query_result_received as OpCodeDataBatch")
			// Try to cast to DataBatch to get inner message type
			if dataBatch, ok := respMsg.(*protocol.DataBatch); ok {
				rh.handleQueryResult(dataBatch)
			} else {
				rh.log.Warning("action: response_received | result: unexpected_format | type: databatch")
			}
		case protocol.OpCodeFinished:
			rh.log.Info("action: finished_received | result: success")
			rh.conn.Close()
		}
	}
}

// handleQueryResult processes a query result message
func (rh *ResponseHandler) handleQueryResult(dataBatch *protocol.DataBatch) {
	// Log based on the inner message opcode
	switch dataBatch.OpCode {
	case protocol.OpCodeQueryResult1:
		rh.log.Info("action: query_result_received | Query: 1 |  Detail: Morning high-value transactions | result: success | type: filtered_transactions")
	case protocol.OpCodeQueryResult2:
		rh.log.Info("action: query_result_received | Query: 2 |  Detail: Product ranking by sales quantity and revenue | result: success | type: product_metrics")
	case protocol.OpCodeQueryResult3:
		rh.log.Info("action: query_result_received | Query: 3 |  Detail: Total Processing Volume by store and semester | result: success | type: tpv_analysis")
	case protocol.OpCodeQueryResult4:
		rh.log.Info("action: query_result_received | Query: 4 |  Detail: Top 3 customers by purchase count per store | result: success | type: top_customers")
	case protocol.OpCodeQueryResultError:
		rh.log.Error("action: query_result_received | result: error | type: query_error")
	default:
		rh.log.Warning("action: query_result_received | result: unknown_type | opcode: %d", dataBatch.OpCode)
	}

	// Write results to file if available
	if dataBatch.ResultTable != nil {
		typedRows, err := dataBatch.ResultTable.GetTypedRows()
		if err != nil {
			rh.log.Errorf("action: write_results | result: fail | error: %v", err)
			return
		}

		var filename string
		switch dataBatch.OpCode {
		case protocol.OpCodeQueryResult1:
			filename = "query1_results.json"
		case protocol.OpCodeQueryResult2:
			filename = "query2_results.json"
		case protocol.OpCodeQueryResult3:
			filename = "query3_results.json"
		case protocol.OpCodeQueryResult4:
			filename = "query4_results.json"
		case protocol.OpCodeQueryResultError:
			filename = "query_error_results.json"
		default:
			rh.log.Warning("action: write_results | result: unknown_type | opcode: %d", dataBatch.OpCode)
			return
		}

		filePath, err := rh.prepareOutputFilePath(filename)
		if err != nil {
			rh.log.Errorf("action: write_results | result: fail | error: %v", err)
			return
		}

		file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			rh.log.Errorf("action: write_results | result: fail | error: opening file %s: %v", filePath, err)
			return
		}
		defer file.Close()

		encoder := json.NewEncoder(file)
		if err := encoder.Encode(typedRows); err != nil {
			rh.log.Errorf("action: write_results | result: fail | error: encoding to %s: %v", filePath, err)
			return
		}

		rh.log.Infof("action: write_results | result: success | file: %s | rows: %d", filePath, len(dataBatch.ResultTable.Rows))
	}
}

// handleQueryResultTable processes a direct query result table message
func (rh *ResponseHandler) handleQueryResultTable(queryResult *protocol.QueryResultTable) {
	// Log based on the opcode
	switch queryResult.OpCode {
	case protocol.OpCodeQueryResult1:
		rh.log.Info("action: query_result_received | Query: 1 | detail: morning high-value transactions |result: success | type: filtered_transactions")
	case protocol.OpCodeQueryResult2:
		rh.log.Info("action: query_result_received | Query: 2 | detail: product ranking by sales quantity and revenue | result: success | type: product_metrics")
	case protocol.OpCodeQueryResult3:
		rh.log.Info("action: query_result_received | Query: 3 | detail: total processing volume by store and semester | result: success | type: tpv_analysis")
	case protocol.OpCodeQueryResult4:
		rh.log.Info("action: query_result_received | Query: 4 | detail: top 3 customers by purchase count per store | result: success | type: top_customers")
	case protocol.OpCodeQueryResultError:
		rh.log.Error("action: query_result_received | result: error | type: query_error")
	default:
		rh.log.Warning("action: query_result_received | result: unknown_type | opcode: %d", queryResult.OpCode)
	}

	// Write results to file
	typedRows, err := queryResult.GetTypedRows()
	if err != nil {
		rh.log.Errorf("action: write_results | result: fail | error: %v", err)
		return
	}

	var filename string
	switch queryResult.OpCode {
	case protocol.OpCodeQueryResult1:
		filename = "query1_results.json"
	case protocol.OpCodeQueryResult2:
		filename = "query2_results.json"
	case protocol.OpCodeQueryResult3:
		filename = "query3_results.json"
	case protocol.OpCodeQueryResult4:
		filename = "query4_results.json"
	case protocol.OpCodeQueryResultError:
		filename = "query_error_results.json"
	default:
		rh.log.Warning("action: write_results | result: unknown_type | opcode: %d", queryResult.OpCode)
		return
	}

	filePath, err := rh.prepareOutputFilePath(filename)
	if err != nil {
		rh.log.Errorf("action: write_results | result: fail | error: %v", err)
		return
	}

	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		rh.log.Errorf("action: write_results | result: fail | error: opening file %s: %v", filePath, err)
		return
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ") // Pretty-print with 2 spaces
	if err := encoder.Encode(typedRows); err != nil {
		rh.log.Errorf("action: write_results | result: fail | error: encoding to %s: %v", filePath, err)
		return
	}

	rh.log.Infof("action: write_results | result: success | file: %s | rows: %d", filePath, len(queryResult.Rows))
}

func (rh *ResponseHandler) prepareOutputFilePath(filename string) (string, error) {
	baseDir := rh.outputBaseDir
	if strings.TrimSpace(baseDir) == "" {
		baseDir = "./client_runs"
	}

	clientDir := filepath.Join(baseDir, rh.clientID)
	if err := os.MkdirAll(clientDir, 0o755); err != nil {
		return "", fmt.Errorf("creating client output directory %s: %w", clientDir, err)
	}

	return filepath.Join(clientDir, filename), nil
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

// SendFinished sends an empty FINISHED message.
// It logs success or failure for each write. On any serialization/I/O error it logs and returns.
func (fms *FinishedMessageSender) SendFinished() {
	finishedMsg := protocol.Finished{}
	if _, err := finishedMsg.WriteTo(fms.conn); err != nil {
		fms.log.Errorf("action: send_finished | result: fail | error: %v", err)
		return
	}

	fms.log.Infof("action: send_finished | result: success")
}
