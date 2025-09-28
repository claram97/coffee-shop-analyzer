package common

import (
	"context"
	"encoding/binary"
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
	
	// Send EOF message after processing all files of this table type
	if err := fp.sendEOFMessage(processor, tableType); err != nil {
		fp.log.Errorf("action: send_eof | result: fail | table_type: %s | error: %v", tableType, err)
		return err
	}
	
	fp.log.Infof("action: sent_eof | result: success | table_type: %s", tableType)
	return nil
}

// ProcessAllTables processes all table types in the data directory
func (fp *FileProcessor) ProcessAllTables(ctx context.Context, processorFactory func(TableRowHandler, byte) *BatchProcessor, dataDir string) error {
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

// sendEOFMessage sends an EOF message for the specified table type
func (fp *FileProcessor) sendEOFMessage(processor *BatchProcessor, tableType string) error {
	conn := processor.GetConnection() // Assuming BatchProcessor has a method to get connection
	
	// Build EOF message body
	body := fp.buildEOFMessageBody(tableType)
	messageLength := int32(len(body))
	
	// Send opcode (u8)
	if err := binary.Write(conn, binary.LittleEndian, protocol.OpCodeEOF); err != nil {
		return fmt.Errorf("failed to send EOF opcode: %w", err)
	}
	
	// Send length (i32)
	if err := binary.Write(conn, binary.LittleEndian, messageLength); err != nil {
		return fmt.Errorf("failed to send EOF length: %w", err)
	}
	
	// Send body
	if _, err := conn.Write(body); err != nil {
		return fmt.Errorf("failed to send EOF body: %w", err)
	}
	
	fp.log.Infof("action: send_eof_message | result: success | table_type: %s | message_length: %d", tableType, messageLength)
	return nil
}

// buildEOFMessageBody builds the body of an EOF message following TableMessage format
func (fp *FileProcessor) buildEOFMessageBody(tableType string) []byte {
	var body []byte
	
	// TableMessage format: [nRows:i32][batchNumber:i64][status:u8][rows...]
	
	// nRows = 1 (one virtual row with table_type info)
	nRowsBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(nRowsBytes, 1)
	body = append(body, nRowsBytes...)
	
	// batchNumber = 0 (EOF doesn't have a specific batch number)
	batchNumberBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(batchNumberBytes, 0)
	body = append(body, batchNumberBytes...)
	
	// status = EOF (1)
	body = append(body, protocol.BatchEOF)
	
	// Row data: one pair ["table_type", tableType]
	// n_pairs (i32) = 1
	nPairsBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(nPairsBytes, 1)
	body = append(body, nPairsBytes...)
	
	// Key: "table_type"
	key := "table_type"
	keyBytes := []byte(key)
	keyLength := int32(len(keyBytes))
	
	keyLengthBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(keyLengthBytes, uint32(keyLength))
	body = append(body, keyLengthBytes...)
	body = append(body, keyBytes...)
	
	// Value: tableType (e.g., "menu_items")
	valueBytes := []byte(tableType)
	valueLength := int32(len(valueBytes))
	
	valueLengthBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(valueLengthBytes, uint32(valueLength))
	body = append(body, valueLengthBytes...)
	body = append(body, valueBytes...)
	
	return body
}
