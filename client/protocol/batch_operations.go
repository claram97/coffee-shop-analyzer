package common

import (
	"bytes"
	"io"
)

// canFitInCurrentBatch checks if a new row can fit in the current batch
func canFitInCurrentBatch(currentBatchSize, newRowSize int, counter int32, batchLimit int32) bool {
	return currentBatchSize+newRowSize+HeaderOverhead <= MaxBatchSizeBytes && counter+1 <= batchLimit
}

// addRowToCurrentBatch adds a serialized row to the current batch buffer
func addRowToCurrentBatch(to *bytes.Buffer, rowBuffer *bytes.Buffer, counter *int32) error {
	_, err := io.Copy(to, rowBuffer)
	if err != nil {
		return err
	}
	*counter++
	return nil
}

// flushCurrentBatch flushes the current batch and increments the batch number
func flushCurrentBatch(to *bytes.Buffer, finalOutput io.Writer, counter int32, opCode byte, batchNumber *int64) error {
	*batchNumber++ // Incrementar el número de batch antes de enviarlo
	return FlushBatch(to, finalOutput, counter, opCode, *batchNumber, BatchContinue)
}

// startNewBatchWithRow starts a new batch with the given row
func startNewBatchWithRow(to *bytes.Buffer, row map[string]string, counter *int32) error {
	if err := writeStringMap(to, row); err != nil {
		return err
	}
	*counter = 1
	return nil
}

// AddRowToBatch serializes a single row as a [string map] and attempts to
// append it to the current batch buffer `to`. If appending would exceed the
// MaxBatchSizeBytes limit (including opcode+length+n headers) or the given
// batchLimit, this function first FlushBatch(to, finalOutput, *counter, batchNumber)
// and then starts a new batch with this row, setting *counter = 1.
// On success, it increments *counter and returns nil; any I/O/encoding
// error is returned. NOTE: batchNumber is used for the current batch being built.
func AddRowToBatch(row map[string]string, to *bytes.Buffer, finalOutput io.Writer, counter *int32, batchLimit int32, opCode byte, batchNumber *int64) error {
	// Serialize the row to calculate its size
	rowBuffer, err := serializeRowToBuffer(row)
	if err != nil {
		return err
	}

	// Check if the row fits in current batch
	if canFitInCurrentBatch(to.Len(), rowBuffer.Len(), *counter, batchLimit) {
		return addRowToCurrentBatch(to, rowBuffer, counter)
	}

	// Flush current batch and start new one
	if err := flushCurrentBatch(to, finalOutput, *counter, opCode, batchNumber); err != nil {
		return err
	}

	return startNewBatchWithRow(to, row, counter)
}
