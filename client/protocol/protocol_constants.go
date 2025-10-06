package common

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
	
	// Query Result Message Opcodes
	OpCodeDataBatch           byte = 0   // Wrapper opcode
	OpCodeQueryResult1        byte = 20  // Q1: Filtered transactions
	OpCodeQueryResult2        byte = 21  // Q2: Product metrics
	OpCodeQueryResult3        byte = 22  // Q3: TPV analysis
	OpCodeQueryResult4        byte = 23  // Q4: Top customers
	OpCodeQueryResultError    byte = 29  // Error result
)

// BatchStatus defines the status of a batch being sent
const (
	BatchContinue byte = 0 // Hay más batches en el archivo
	BatchEOF      byte = 1 // Último batch del archivo
	BatchCancel   byte = 2 // Batch enviado por cancelación
)

// Protocol limits
const (
	MaxBatchSizeBytes int = 64 * 1024 // 64KB - Límite máximo del tamaño de batch en bytes
)

// Protocol frame overhead constants
const (
	HeaderOverhead = 18 // +1 (opcode) +4 (length) +4 (nLines) +8 (batchNumber) +1 (status)
)
