package common

import (
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"io"
	"net"

	"github.com/op/go-logging"
)

// BatchProcessor handles batch processing logic
type BatchProcessor struct {
	conn       net.Conn
	handler    TableRowHandler
	opCode     byte
	batchLimit int32
	clientID   string
	log        *logging.Logger
}

// NewBatchProcessor creates a new batch processor
func NewBatchProcessor(conn net.Conn, handler TableRowHandler, opCode byte, batchLimit int32, clientID string, logger *logging.Logger) *BatchProcessor {
	return &BatchProcessor{
		conn:       conn,
		handler:    handler,
		opCode:     opCode,
		batchLimit: batchLimit,
		clientID:   clientID,
		log:        logger,
	}
}

// processNextRow reads a single CSV record from reader, converts it
// to the protocol key/value map and attempts to add it to the current batch buffer
func (bp *BatchProcessor) processNextRow(reader *csv.Reader, batchBuff *bytes.Buffer, counter *int32, batchNumber *int64) error {
	record, err := reader.Read()
	if err != nil {
		return err
	}

	// Call the interface method to process the record
	rowMap, err := bp.handler.ProcessRecord(record)
	if err != nil {
		return err
	}

	// Call the low-level generic function
	if err := AddRowToBatch(rowMap, batchBuff, bp.conn, counter, bp.batchLimit, bp.opCode, batchNumber); err != nil {
		return err
	}
	return nil
}

// handleCancellation handles context cancellation and sends pending batch if there's data
func (bp *BatchProcessor) handleCancellation(batchBuff *bytes.Buffer, counter *int32, currentBatchNumber *int64) error {
	if *counter > 0 {
		(*currentBatchNumber)++ // Increment only when we actually send a batch
		if err := FlushBatch(batchBuff, bp.conn, *counter, bp.opCode, *currentBatchNumber, BatchCancel); err != nil {
			return err
		}
		*counter = 0
	}
	return context.Canceled
}

// handleEOF handles end of file and sends pending batch if there's data
func (bp *BatchProcessor) handleEOF(batchBuff *bytes.Buffer, counter *int32, currentBatchNumber *int64) error {
	if *counter > 0 {
		(*currentBatchNumber)++ // Increment only when we actually send a batch
		if err := FlushBatch(batchBuff, bp.conn, *counter, bp.opCode, *currentBatchNumber, BatchEOF); err != nil {
			return err
		}
	}
	return nil // Successful EOF
}

// processCSVLoop processes the main CSV reading loop
func (bp *BatchProcessor) processCSVLoop(ctx context.Context, reader *csv.Reader, batchBuff *bytes.Buffer, counter *int32, currentBatchNumber *int64) error {
	for {
		// Check for cancellation
		select {
		case <-ctx.Done():
			return bp.handleCancellation(batchBuff, counter, currentBatchNumber)
		default:
			// Continue normal processing
		}

		// Read and process the next CSV line
		if err := bp.processNextRow(reader, batchBuff, counter, currentBatchNumber); err != nil {
			if errors.Is(err, io.EOF) {
				return bp.handleEOF(batchBuff, counter, currentBatchNumber)
			}
			// Any other error (malformed CSV, I/O, etc.)
			return err
		}
	}
}

// BuildAndSendBatches streams the CSV, incrementally building table data
// bodies into batchBuff and flushing to connection as limits are reached.
// On context cancellation, it flushes any partial batch and returns the
// context error. On clean EOF, it flushes a final partial batch (if any)
// and returns nil. Any serialization or socket error is returned.
func (bp *BatchProcessor) BuildAndSendBatches(ctx context.Context, reader *csv.Reader, batchNumber int64) error {
	var batchBuff bytes.Buffer
	var counter int32 = 0
	currentBatchNumber := batchNumber // Current batch number (not incremented per line)

	return bp.processCSVLoop(ctx, reader, &batchBuff, &counter, &currentBatchNumber)
}
