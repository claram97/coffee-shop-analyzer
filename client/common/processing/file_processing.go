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

	// Import protocol definitions
	protocol "github.com/7574-sistemas-distribuidos/docker-compose-init/client/protocol"
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

// SetHandlerForTableType automatically sets the appropriate handler and opcode based on table type directory name
func (tth *TableTypeHandler) GetHandlerAndOpCode(tableType string) (TableRowHandler, byte, error) {
	switch tableType {
	case "transactions":
		return TransactionHandler{}, protocol.OpCodeNewTransaction, nil
	case "transaction_items":
		return TransactionItemHandler{}, protocol.OpCodeNewTransactionItems, nil
	case "menu_items":
		return MenuItemHandler{}, protocol.OpCodeNewMenuItems, nil
	case "stores":
		return StoreHandler{}, protocol.OpCodeNewStores, nil
	case "users":
		return UserHandler{}, protocol.OpCodeNewUsers, nil
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
func (fp *FileProcessor) processFileAsync(ctx context.Context, reader *csv.Reader, fileName string, processor *BatchProcessor) error {
	writeDone := make(chan error, 1)
	go func() {
		writeDone <- processor.BuildAndSendBatches(ctx, reader, 0) // batchNumber reset on each file
	}()

	if err := <-writeDone; err != nil && !errors.Is(err, context.Canceled) {
		fp.log.Errorf("action: send_batches | result: fail | file: %s | error: %v", fileName, err)
		return err
	}

	fp.log.Infof("action: completed_processing_file | file: %s | result: success", fileName)
	return nil
}

// ProcessFile processes a single CSV file
func (fp *FileProcessor) ProcessFile(ctx context.Context, dir, fileName string, processor *BatchProcessor) error {
	filePath := filepath.Join(dir, fileName)

	file, err := fp.openAndPrepareFile(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	reader, err := fp.setupCSVReader(file)
	if err != nil {
		return err
	}

	return fp.processFileAsync(ctx, reader, fileName, processor)
}

// ProcessTableType processes all CSV files in a table type directory
func (fp *FileProcessor) ProcessTableType(ctx context.Context, dataDir, tableType string, processorFactory func(TableRowHandler, byte) *BatchProcessor) error {
	subDirPath := filepath.Join(dataDir, tableType)
	fp.log.Infof("action: processing_table_type | table_type: %s | client_id: %v", tableType, fp.clientID)

	// Get handler and opcode for this table type
	tth := NewTableTypeHandler(fp.clientID, fp.log)
	handler, opCode, err := tth.GetHandlerAndOpCode(tableType)
	if err != nil {
		fp.log.Infof("action: skip_table_type | table_type: %s | reason: unsupported | client_id: %v | error: %v", tableType, fp.clientID, err)
		return nil
	}

	// Create processor for this table type
	processor := processorFactory(handler, opCode)

	files, err := os.ReadDir(subDirPath)
	if err != nil {
		fp.log.Errorf("Error reading subdirectory %s: %v", subDirPath, err)
		return err
	}

	for _, fileEntry := range files {
		if !fileEntry.IsDir() && filepath.Ext(fileEntry.Name()) == ".csv" {
			if err := fp.ProcessFile(ctx, subDirPath, fileEntry.Name(), processor); err != nil {
				return err
			}
			time.Sleep(1 * time.Second) // delay between files
		}
	}
	return nil
}

// ProcessAllTables processes all table types in the data directory
func (fp *FileProcessor) ProcessAllTables(ctx context.Context, processorFactory func(TableRowHandler, byte) *BatchProcessor) error {
	dataDir := "./data"
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
