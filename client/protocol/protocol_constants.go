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
