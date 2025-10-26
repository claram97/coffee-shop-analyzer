package common

import "fmt"

// Protocol Operation Codes
const (
	BatchRecvSuccessOpCode    byte = 1
	BatchRecvFailOpCode       byte = 2
	OpCodeFinished            byte = 3
	OpCodeNewMenuItems        byte = 4
	OpCodeNewStores           byte = 5
	OpCodeNewTransactionItems byte = 6
	OpCodeNewTransaction      byte = 7
	OpCodeNewUsers            byte = 8
	OpCodeEOF                 byte = 9
	OpCodeClientHello         byte = 30

	// Query Result Message Opcodes
	OpCodeDataBatch        byte = 0  // Wrapper opcode
	OpCodeQueryResult1     byte = 20 // Q1: Filtered transactions
	OpCodeQueryResult2     byte = 21 // Q2: Product metrics
	OpCodeQueryResult3     byte = 22 // Q3: TPV analysis
	OpCodeQueryResult4     byte = 23 // Q4: Top customers
	OpCodeQueryResultError byte = 29 // Error result
)

// BatchStatus defines the status of a batch being sent
const (
	BatchContinue byte = 0 // Hay más batches en el archivo
	BatchEOF      byte = 1 // Último batch del archivo
	BatchCancel   byte = 2 // Batch enviado por cancelación
)

// Protocol limits
const (
	MaxBatchSizeBytes int = 1024 * 1024 // 1MB - Límite máximo del tamaño de batch en bytes
)

// Protocol frame overhead constants
const (
	HeaderOverhead = 18 // +1 (opcode) +4 (length) +4 (nLines) +8 (batchNumber) +1 (status)
)

var opCodeNames = map[byte]string{
	BatchRecvSuccessOpCode:    "BATCH_RECV_SUCCESS",
	BatchRecvFailOpCode:       "BATCH_RECV_FAIL",
	OpCodeFinished:            "FINISHED",
	OpCodeNewMenuItems:        "NEW_MENU_ITEMS",
	OpCodeNewStores:           "NEW_STORES",
	OpCodeNewTransactionItems: "NEW_TRANSACTION_ITEMS",
	OpCodeNewTransaction:      "NEW_TRANSACTION",
	OpCodeNewUsers:            "NEW_USERS",
	OpCodeEOF:                 "EOF",
	OpCodeClientHello:         "CLIENT_HELLO",
	OpCodeDataBatch:           "DATA_BATCH",
	OpCodeQueryResult1:        "QUERY_RESULT_1",
	OpCodeQueryResult2:        "QUERY_RESULT_2",
	OpCodeQueryResult3:        "QUERY_RESULT_3",
	OpCodeQueryResult4:        "QUERY_RESULT_4",
	OpCodeQueryResultError:    "QUERY_RESULT_ERROR",
}

// OpCodeName returns a readable name for the given opcode, or UNKNOWN(<value>) if it is not mapped.
func OpCodeName(op byte) string {
	if name, ok := opCodeNames[op]; ok {
		return name
	}
	return fmt.Sprintf("UNKNOWN(%d)", op)
}
