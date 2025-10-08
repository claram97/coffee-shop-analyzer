package common

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/op/go-logging"

	// Import protobuf definitions
	pb "github.com/7574-sistemas-distribuidos/docker-compose-init/client/protos"
	// Import network utilities for connection error checking
	network "github.com/7574-sistemas-distribuidos/docker-compose-init/client/common/network"
)

// FileProcessor handles CSV file processing logic
type FileProcessor struct {
	clientID string
	log      *logging.Logger
}

// NewFileProcessor creates a new file processor
func NewFileProcessor(clientID string, logger *logging.Logger) *FileProcessor {
	return &FileProcessor{
		clientID: clientID,
		log:      logger,
	}
}

// TableTypeHandler manages table type configuration
type TableTypeHandler struct {
	clientID string
	log      *logging.Logger
}

// NewTableTypeHandler creates a new table type handler
func NewTableTypeHandler(clientID string, logger *logging.Logger) *TableTypeHandler {
	return &TableTypeHandler{
		clientID: clientID,
		log:      logger,
	}
}

// SetHandlerForTableType automatically sets the appropriate handler and table name based on table type directory name
func (tth *TableTypeHandler) GetHandlerAndOpCode(tableType string) (TableRowHandler, pb.TableName, error) {
	switch tableType {
	case "transactions":
		return TransactionHandler{}, pb.TableName_TRANSACTIONS, nil
	case "transaction_items":
		return TransactionItemHandler{}, pb.TableName_TRANSACTION_ITEMS, nil
	case "menu_items":
		return MenuItemHandler{}, pb.TableName_MENU_ITEMS, nil
	case "stores":
		return StoreHandler{}, pb.TableName_STORES, nil
	case "users":
		return UserHandler{}, pb.TableName_USERS, nil
	default:
		return nil, 0, fmt.Errorf("unknown table type: %s", tableType)
	}
}

// openAndPrepareFile opens the file and logs the start of processing
func (fp *FileProcessor) openAndPrepareFile(filePath string) (*os.File, error) {
	fp.log.Infof("action: processing_file | file: %s", filePath)

	file, err := os.Open(filePath)
	if err != nil {
		fp.log.Criticalf("action: read_file | result: fail | file: %s | error: %v", filePath, err)
		return nil, err
	}
	return file, nil
}

// setupCSVReader configures the CSV reader and skips the header
func (fp *FileProcessor) setupCSVReader(file *os.File) (*csv.Reader, error) {
	reader := csv.NewReader(file)
	reader.Comma = ','
	reader.FieldsPerRecord = -1

	// Skip header
	if _, err := reader.Read(); err != nil {
		fp.log.Criticalf("action: read_header | result: fail | error: %v", err)
		return nil, err
	}
	return reader, nil
}

// processFileAsync processes the file asynchronously and handles the results
// Returns the last batch number used and any error
func (fp *FileProcessor) processFileAsync(ctx context.Context, reader *csv.Reader, fileName string, processor *BatchProcessor, isLastFile bool, startingBatchNumber int64) (int64, error) {
	type batchResult struct {
		err         error
		lastBatchNo int64
	}

	writeDone := make(chan batchResult, 1)
	go func() {
		lastBatchNo, err := processor.BuildAndSendBatches(ctx, reader, startingBatchNumber, isLastFile)
		writeDone <- batchResult{err, lastBatchNo}
	}()

	result := <-writeDone
	if result.err != nil && !errors.Is(result.err, context.Canceled) {
		fp.log.Errorf("action: send_batches | result: fail | file: %s | error: %v", fileName, result.err)
		return result.lastBatchNo, result.err
	}

	fp.log.Infof("action: completed_processing_file | file: %s | result: success | last_batch_number: %d", fileName, result.lastBatchNo)
	return result.lastBatchNo, nil
}

// ProcessFile processes a single CSV file
// Returns the last batch number used and any error
func (fp *FileProcessor) ProcessFile(ctx context.Context, dir, fileName string, processor *BatchProcessor, isLastFile bool, startingBatchNumber int64) (int64, error) {
	filePath := filepath.Join(dir, fileName)

	file, err := fp.openAndPrepareFile(filePath)
	if err != nil {
		return startingBatchNumber, err
	}
	defer file.Close()

	reader, err := fp.setupCSVReader(file)
	if err != nil {
		return startingBatchNumber, err
	}

	return fp.processFileAsync(ctx, reader, fileName, processor, isLastFile, startingBatchNumber)
}

// ProcessTableType processes all CSV files in a table type directory
func (fp *FileProcessor) ProcessTableType(ctx context.Context, dataDir, tableType string, processorFactory func(TableRowHandler, pb.TableName) *BatchProcessor) error {
	subDirPath := filepath.Join(dataDir, tableType)
	fp.log.Infof("action: processing_table_type | table_type: %s | client_id: %v", tableType, fp.clientID)

	// Get handler and table name for this table type
	tth := NewTableTypeHandler(fp.clientID, fp.log)
	handler, tableName, err := tth.GetHandlerAndOpCode(tableType)
	if err != nil {
		fp.log.Infof("action: skip_table_type | table_type: %s | reason: unsupported | client_id: %v | error: %v", tableType, fp.clientID, err)
		return nil
	}

	// Create processor for this table type
	processor := processorFactory(handler, tableName)

	files, err := os.ReadDir(subDirPath)
	if err != nil {
		fp.log.Errorf("Error reading subdirectory %s: %v", subDirPath, err)
		return err
	}

	// Filter to only CSV files
	var csvFiles []os.DirEntry
	for _, entry := range files {
		if !entry.IsDir() && filepath.Ext(entry.Name()) == ".csv" {
			csvFiles = append(csvFiles, entry)
		}
	}

	// Initialize a table-wide batch counter
	var tableBatchCounter int64 = 0

	// Process each file
	for i, fileEntry := range csvFiles {
		// Check if this is the last CSV file for this table type
		isLastFile := (i == len(csvFiles)-1)

		if isLastFile {
			fp.log.Infof("action: processing_file | file: %s | is_last_file: true | starting_batch: %d", fileEntry.Name(), tableBatchCounter)
		} else {
			fp.log.Infof("action: processing_file | file: %s | starting_batch: %d", fileEntry.Name(), tableBatchCounter)
		}

		// Process the file with the current table batch counter
		lastBatchNo, err := fp.ProcessFile(ctx, subDirPath, fileEntry.Name(), processor, isLastFile, tableBatchCounter)
		if err != nil {
			return err
		}

		// Update the table batch counter for the next file
		tableBatchCounter = lastBatchNo
		fp.log.Infof("action: updated_batch_counter | table_type: %s | new_value: %d", tableType, tableBatchCounter)

		time.Sleep(1 * time.Second) // delay between files
	}

	// With protobuf, EOF is sent automatically with the last batch (BatchStatus=EOF)
	// when isLastFile=true in the last CSV file processing
	fp.log.Infof("action: completed_table_type | result: success | table_type: %s", tableType)
	return nil
}

// ProcessAllTables processes all table types in the data directory
func (fp *FileProcessor) ProcessAllTables(ctx context.Context, processorFactory func(TableRowHandler, pb.TableName) *BatchProcessor, dataDir string) error {
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		fp.log.Errorf("Error reading data directory: %v", err)
		return err
	}

	var lastErr error
	for _, entry := range entries {
		if entry.IsDir() {
			if err := fp.ProcessTableType(ctx, dataDir, entry.Name(), processorFactory); err != nil {
				lastErr = err
				if network.IsConnectionError(err) {
					fp.log.Criticalf("action: connection_lost | result: terminating | error: %v", err)
					return err
				}
			}
		}
	}
	return lastErr
}
