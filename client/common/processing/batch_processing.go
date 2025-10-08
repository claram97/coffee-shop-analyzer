package common

import (
	"context"
	"encoding/binary"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"net"

	"github.com/op/go-logging"
	"google.golang.org/protobuf/proto"

	// Import protobuf definitions
	pb "github.com/7574-sistemas-distribuidos/docker-compose-init/client/protos"
)

// BatchProcessor handles batch processing logic
type BatchProcessor struct {
	conn       net.Conn
	handler    TableRowHandler
	tableName  pb.TableName
	batchLimit int32
	clientID   string
	log        *logging.Logger
}

// NewBatchProcessor creates a new batch processor
func NewBatchProcessor(conn net.Conn, handler TableRowHandler, tableName pb.TableName, batchLimit int32, clientID string, logger *logging.Logger) *BatchProcessor {
	return &BatchProcessor{
		conn:       conn,
		handler:    handler,
		tableName:  tableName,
		batchLimit: batchLimit,
		clientID:   clientID,
		log:        logger,
	}
}

// processNextRow reads a single CSV record from reader, converts it
// to a protobuf Row and adds it to the batch rows slice
func (bp *BatchProcessor) processNextRow(reader *csv.Reader, rows *[]*pb.Row, counter *int32, batchNumber *int64) error {
	record, err := reader.Read()
	if err != nil {
		return err
	}

	// Call the interface method to process the record
	rowMap, err := bp.handler.ProcessRecord(record)
	if err != nil {
		return err
	}

	// Convert map to Row with values in consistent order
	// The schema will define the column order
	values := make([]string, 0, len(rowMap))
	for _, v := range rowMap {
		values = append(values, v)
	}

	// Create protobuf Row
	row := &pb.Row{
		Values: values,
	}

	// Add row to batch
	*rows = append(*rows, row)
	*counter++

	// Check if we need to flush the batch
	if *counter >= bp.batchLimit {
		(*batchNumber)++
		if err := bp.flushBatch(*rows, *counter, *batchNumber, pb.BatchStatus_CONTINUE); err != nil {
			return err
		}
		// Reset for next batch
		*rows = make([]*pb.Row, 0, bp.batchLimit)
		*counter = 0
	}

	return nil
}

// flushBatch sends a batch of rows as a TableData message
func (bp *BatchProcessor) flushBatch(rows []*pb.Row, counter int32, batchNumber int64, status pb.BatchStatus) error {
	// Create TableData message
	tableData := &pb.TableData{
		Name:        bp.tableName,
		Schema:      bp.getSchema(),
		Rows:        rows,
		BatchNumber: uint64(batchNumber),
		Status:      status,
	}

	// Create Envelope
	envelope := &pb.Envelope{
		Type: pb.Envelope_TABLE_DATA,
		Payload: &pb.Envelope_TableData{
			TableData: tableData,
		},
	}

	// Serialize
	msgBytes, err := proto.Marshal(envelope)
	if err != nil {
		bp.log.Errorf("action: flush_batch | result: fail | error: marshal: %v", err)
		return err
	}

	// Write size (4 bytes big-endian)
	msgSize := uint32(len(msgBytes))
	if err := binary.Write(bp.conn, binary.BigEndian, msgSize); err != nil {
		bp.log.Errorf("action: flush_batch | result: fail | error: write_size: %v", err)
		return err
	}

	// Write data
	if _, err := bp.conn.Write(msgBytes); err != nil {
		bp.log.Errorf("action: flush_batch | result: fail | error: write_data: %v", err)
		return err
	}

	bp.log.Debugf("action: flush_batch | result: success | table: %s | batch: %d | rows: %d | status: %v",
		bp.tableName, batchNumber, counter, status)

	return nil
}

// getSchema returns the schema for this table type
func (bp *BatchProcessor) getSchema() *pb.TableSchema {
	// Get the column names from the handler's ProcessRecord method
	// For now, we'll use generic column names based on the expected fields
	columns := make([]string, bp.handler.GetExpectedFields())
	for i := range columns {
		columns[i] = fmt.Sprintf("column_%d", i)
	}
	return &pb.TableSchema{Columns: columns}
}

// handleCancellation handles context cancellation and sends pending batch if there's data
func (bp *BatchProcessor) handleCancellation(rows []*pb.Row, counter *int32, currentBatchNumber *int64) error {
	if *counter > 0 {
		(*currentBatchNumber)++ // Increment only when we actually send a batch
		if err := bp.flushBatch(rows, *counter, *currentBatchNumber, pb.BatchStatus_CANCEL); err != nil {
			return err
		}
		*counter = 0
	}
	return context.Canceled
}

// handleEOF handles end of file and sends pending batch if there's data
// isLastFile indicates whether this is the last file for this table type
func (bp *BatchProcessor) handleEOF(rows []*pb.Row, counter *int32, currentBatchNumber *int64, isLastFile bool) error {
	if *counter > 0 {
		(*currentBatchNumber)++ // Increment only when we actually send a batch

		// Choose the appropriate status - EOF only if this is the last file
		batchStatus := pb.BatchStatus_CONTINUE
		if isLastFile {
			batchStatus = pb.BatchStatus_EOF
			bp.log.Infof("action: send_last_batch | result: setting_eof_status | batch_number: %d", *currentBatchNumber)
		}

		if err := bp.flushBatch(rows, *counter, *currentBatchNumber, batchStatus); err != nil {
			return err
		}
	}
	return nil // Successful EOF
}

// processCSVLoop processes the main CSV reading loop
func (bp *BatchProcessor) processCSVLoop(ctx context.Context, reader *csv.Reader, rows *[]*pb.Row, counter *int32, currentBatchNumber *int64, isLastFile bool) error {
	for {
		// Check for cancellation
		select {
		case <-ctx.Done():
			return bp.handleCancellation(*rows, counter, currentBatchNumber)
		default:
			// Continue normal processing
		}

		// Read and process the next CSV line
		if err := bp.processNextRow(reader, rows, counter, currentBatchNumber); err != nil {
			if errors.Is(err, io.EOF) {
				return bp.handleEOF(*rows, counter, currentBatchNumber, isLastFile)
			}
			// Any other error (malformed CSV, I/O, etc.)
			return err
		}
	}
}

// BuildAndSendBatches streams the CSV, incrementally building table data
// rows into a slice and flushing to connection as limits are reached.
// On context cancellation, it flushes any partial batch and returns the
// context error. On clean EOF, it flushes a final partial batch (if any)
// and returns nil. Any serialization or socket error is returned.
//
// isLastFile indicates if this is the last file for this table type, which
// determines whether the final batch should have BatchStatus=EOF
//
// Returns the last batch number used and the error (if any)
func (bp *BatchProcessor) BuildAndSendBatches(ctx context.Context, reader *csv.Reader, batchNumber int64, isLastFile bool) (int64, error) {
	rows := make([]*pb.Row, 0, bp.batchLimit)
	var counter int32 = 0
	currentBatchNumber := batchNumber // Current batch number (not incremented per line)

	err := bp.processCSVLoop(ctx, reader, &rows, &counter, &currentBatchNumber, isLastFile)
	return currentBatchNumber, err
}

// GetConnection returns the network connection used by this processor
func (bp *BatchProcessor) GetConnection() net.Conn {
	return bp.conn
}
