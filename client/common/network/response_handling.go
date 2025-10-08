package common

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"strconv"
	"time"

	"github.com/op/go-logging"
	"google.golang.org/protobuf/proto"

	// Import protobuf definitions
	pb "github.com/7574-sistemas-distribuidos/docker-compose-init/client/protos"
)

// ReadEnvelope reads a protobuf Envelope from the connection
func ReadEnvelope(reader *bufio.Reader) (*pb.Envelope, error) {
	// Read message size (4 bytes, big-endian)
	var msgSize uint32
	if err := binary.Read(reader, binary.BigEndian, &msgSize); err != nil {
		return nil, err
	}

	// Read message bytes
	msgBytes := make([]byte, msgSize)
	if _, err := io.ReadFull(reader, msgBytes); err != nil {
		return nil, err
	}

	// Unmarshal protobuf
	envelope := &pb.Envelope{}
	if err := proto.Unmarshal(msgBytes, envelope); err != nil {
		return nil, err
	}

	return envelope, nil
}

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
func (rh *ResponseHandler) handleResponseMessage(envelope *pb.Envelope) {
	switch envelope.Type {
	case pb.Envelope_BATCH_RECV_SUrCESS:
		rh.log.Debug("action: batch_enviado | result: success")
	case pb.Envelope_BATCH_RECV_FAIL:
		rh.log.Error("action: batch_enviado | result: fail")
	case pb.Envelope_TABLE_DATA:
		if tableData := envelope.GetTableData(); tableData != nil {
			rh.handleTableData(tableData)
		}
	case pb.Envelope_EOF:
		if eofMsg := envelope.GetEof(); eofMsg != nil {
			rh.handleEOF(eofMsg)
		}
	case pb.Envelope_FINISHED:
		rh.log.Info("action: servidor_finished | result: all_queries_complete")
	default:
		rh.log.Warning("action: response_received | result: unknown_type | type: %v", envelope.Type)
	}
}

// handleEOF processes EOF message
func (rh *ResponseHandler) handleEOF(eofMsg *pb.EOFMessage) {
	tableName := eofMsg.Table.String()
	rh.log.Infof("action: eof_received | table: %s | result: completed", tableName)

	switch eofMsg.Table {
	case pb.TableName_QUERY_RESULTS_1:
		rh.log.Info("Query 1 transmission completed")
	case pb.TableName_QUERY_RESULTS_2:
		rh.log.Info("Query 2 transmission completed")
	case pb.TableName_QUERY_RESULTS_3:
		rh.log.Info("Query 3 transmission completed")
	case pb.TableName_QUERY_RESULTS_4:
		rh.log.Info("Query 4 transmission completed")
	}
}

// handleTableData processes a query result message
func (rh *ResponseHandler) handleTableData(tableData *pb.TableData) {
	// Log based on the table name
	switch tableData.Name {
	case pb.TableName_QUERY_RESULTS_1:
		rh.log.Infof("action: query_result_received | result: success | type: filtered_transactions | batch: %d | rows: %d",
			tableData.BatchNumber, len(tableData.Rows))
		rh.log.Debug("Query 1 result: Morning high-value transactions")
	case pb.TableName_QUERY_RESULTS_2:
		rh.log.Infof("action: query_result_received | result: success | type: product_metrics | batch: %d | rows: %d",
			tableData.BatchNumber, len(tableData.Rows))
		rh.log.Debug("Query 2 result: Product ranking by sales quantity and revenue")
	case pb.TableName_QUERY_RESULTS_3:
		rh.log.Infof("action: query_result_received | result: success | type: tpv_analysis | batch: %d | rows: %d",
			tableData.BatchNumber, len(tableData.Rows))
		rh.log.Debug("Query 3 result: Total Processing Volume by store and semester")
	case pb.TableName_QUERY_RESULTS_4:
		rh.log.Infof("action: query_result_received | result: success | type: top_customers | batch: %d | rows: %d",
			tableData.BatchNumber, len(tableData.Rows))
		rh.log.Debug("Query 4 result: Top 3 customers by purchase count per store")
	default:
		rh.log.Debugf("action: table_data_received | table: %v | batch: %d | rows: %d",
			tableData.Name, tableData.BatchNumber, len(tableData.Rows))
	}

	// Check batch status
	switch tableData.Status {
	case pb.BatchStatus_EOF:
		rh.log.Infof("action: batch_status | table: %s | status: eof | last_batch: %d",
			tableData.Name.String(), tableData.BatchNumber)
	case pb.BatchStatus_CANCEL:
		rh.log.Warningf("action: batch_status | table: %s | status: cancelled",
			tableData.Name.String())
	case pb.BatchStatus_CONTINUE:
		// Normal batch, more data expected
	}
}

// responseReaderLoop executes the main response reading loop
func (rh *ResponseHandler) responseReaderLoop(reader *bufio.Reader) {
	for {
		envelope, err := ReadEnvelope(reader)
		if err != nil {
			if rh.handleReadError(err) {
				break
			}
			continue
		}
		rh.handleResponseMessage(envelope)
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

	// Create FINISHED envelope
	envelope := &pb.Envelope{
		Type: pb.Envelope_FINISHED,
	}

	// Serialize
	msgBytes, err := proto.Marshal(envelope)
	if err != nil {
		fms.log.Errorf("action: send_finished | result: fail | error: marshal: %v", err)
		return
	}

	// Write size (4 bytes big-endian)
	msgSize := uint32(len(msgBytes))
	if err := binary.Write(fms.conn, binary.BigEndian, msgSize); err != nil {
		fms.log.Errorf("action: send_finished | result: fail | error: write_size: %v", err)
		return
	}

	// Write data
	if _, err := fms.conn.Write(msgBytes); err != nil {
		fms.log.Errorf("action: send_finished | result: fail | error: write_data: %v", err)
		return
	}

	fms.log.Infof("action: send_finished | result: success | agencyId: %d", int32(agencyId))
}
