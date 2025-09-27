# Protocol Documentation - Coffee Shop Analyzer

## Overview

This document describes the communication protocol used between the Go client and the Python orchestrator in the coffee shop analyzer system. The protocol is a binary, frame-based protocol that uses little-endian byte order for multi-byte values.

## Package Structure

The protocol implementation is located in the `client/protocol/` directory and consists of the following files:

- `protocol_constants.go` - Defines operation codes, batch status constants, and protocol limits
- `protocol_interfaces.go` - Defines core interfaces and error types
- `messages.go` - Implements protocol messages and message parsing
- `batch_operations.go` - Provides batch management and row handling functionality
- `batch_writer.go` - Handles batch serialization and framing
- `serialization.go` - Implements low-level data serialization functions

## Protocol Constants

### Operation Codes (OpCodes)

| OpCode | Value | Direction | Description |
|--------|-------|-----------|-------------|
| `BatchRecvSuccessOpCode` | 1 | Server→Client | Batch processed successfully |
| `BatchRecvFailOpCode` | 2 | Server→Client | Batch processing failed |
| `OpCodeFinished` | 3 | Client→Server | Client finished sending all batches |
| `OpCodeNewMenuItems` | 4 | Client→Server | Batch contains menu items data |
| `OpCodeNewStores` | 5 | Client→Server | Batch contains stores data |
| `OpCodeNewTransactionItems` | 6 | Client→Server | Batch contains transaction items data |
| `OpCodeNewTransaction` | 7 | Client→Server | Batch contains transaction data |
| `OpCodeNewUsers` | 8 | Client→Server | Batch contains users data |

### Batch Status

| Status | Value | Description |
|--------|-------|-------------|
| `BatchContinue` | 0 | More batches will follow for this file |
| `BatchEOF` | 1 | Last batch of the current file |
| `BatchCancel` | 2 | Batch sent due to cancellation |

### Protocol Limits

- **Maximum Batch Size**: 1MB (1,048,576 bytes)
- **Header Overhead**: 18 bytes per message

## Message Format

### General Frame Structure

All protocol messages follow this general structure:

```
[opcode:1][length:4][message_body:variable]
```

- `opcode`: 1 byte operation code
- `length`: 4 bytes (int32, little-endian) - length of message body
- `message_body`: Variable length message payload

### Batch Message Format

For batch messages (OpCodes 4-8), the message body has the following structure:

```
[opcode:1][length:4][nLines:4][batchNumber:8][status:1][data:variable]
```

- `opcode`: 1 byte operation code
- `length`: 4 bytes (int32, little-endian) - total length of: nLines + batchNumber + status + data
- `nLines`: 4 bytes (int32, little-endian) - number of data rows in this batch
- `batchNumber`: 8 bytes (int64, little-endian) - sequential batch identifier
- `status`: 1 byte batch status (see Batch Status table)
- `data`: Variable length serialized data rows

### Data Serialization Format

Data rows are serialized as string maps using the following format:

#### String Format
```
[length:4][utf8_bytes:variable]
```
- `length`: 4 bytes (int32, little-endian) - number of UTF-8 bytes
- `utf8_bytes`: UTF-8 encoded string data

#### String Map Format
```
[num_pairs:4][key1:string][value1:string][key2:string][value2:string]...
```
- `num_pairs`: 4 bytes (int32, little-endian) - number of key-value pairs
- Each pair consists of two consecutive strings (key and value)

## Core Interfaces

### Message Interface
```go
type Message interface {
    GetOpCode() byte
    GetLength() int32
}
```

### Writeable Interface
```go
type Writeable interface {
    WriteTo(out io.Writer) (int64, error)
}
```

### Readable Interface
```go
type Readable interface {
    readFrom(reader *bufio.Reader) error
    Message
}
```

## Protocol Messages

### 1. Finished Message (Client→Server)

Indicates that the client has finished sending all batch messages for its agency.

**Structure:**
- OpCode: `OpCodeFinished` (3)
- Length: 4 bytes
- Body: `[agencyId:4]`

**Usage:**
```go
msg := &Finished{AgencyId: 12345}
msg.WriteTo(writer)
```

### 2. BatchRecvSuccess (Server→Client)

Acknowledgment that a batch was processed successfully.

**Structure:**
- OpCode: `BatchRecvSuccessOpCode` (1)
- Length: 0 bytes
- Body: (empty)

### 3. BatchRecvFail (Server→Client)

Negative acknowledgment that a batch processing failed.

**Structure:**
- OpCode: `BatchRecvFailOpCode` (2)  
- Length: 0 bytes
- Body: (empty)

## Batch Operations

### Adding Rows to Batches

The `AddRowToBatch` function manages batch size limits and automatic flushing:

```go
func AddRowToBatch(
    row map[string]string,
    to *bytes.Buffer,
    finalOutput io.Writer,
    counter *int32,
    batchLimit int32,
    opCode byte,
    batchNumber *int64
) error
```

**Behavior:**
- Attempts to add a row to the current batch
- If adding the row would exceed `MaxBatchSizeBytes` or `batchLimit`, automatically flushes the current batch and starts a new one
- Manages batch numbering automatically
- Returns error on serialization or I/O failures

### Batch Flushing

The `FlushBatch` function serializes and sends a complete batch:

```go
func FlushBatch(
    batch *bytes.Buffer,
    out io.Writer,
    counter int32,
    opCode byte,
    batchNumber int64,
    batchStatus byte
) error
```

## Error Handling

### ProtocolError

Custom error type for protocol-related errors:

```go
type ProtocolError struct {
    Msg    string
    Opcode byte
}
```

**Common Error Scenarios:**
- Invalid opcode in incoming messages
- Body length mismatch
- Serialization failures
- Network I/O errors

## Usage Examples

### Sending a Batch of Menu Items

```go
var batch bytes.Buffer
var counter int32
var batchNumber int64 = 1

// Add rows to batch
for _, menuItem := range menuItems {
    row := map[string]string{
        "id":    menuItem.ID,
        "name":  menuItem.Name,
        "price": menuItem.Price,
    }
    
    err := AddRowToBatch(row, &batch, writer, &counter, 1000, OpCodeNewMenuItems, &batchNumber)
    if err != nil {
        // handle error
    }
}

// Flush final batch with EOF status
if counter > 0 {
    err := FlushBatch(&batch, writer, counter, OpCodeNewMenuItems, batchNumber, BatchEOF)
}
```

### Reading Server Responses

```go
reader := bufio.NewReader(connection)

for {
    msg, err := ReadMessage(reader)
    if err != nil {
        // handle error
        break
    }
    
    switch msg.GetOpCode() {
    case BatchRecvSuccessOpCode:
        // Batch processed successfully
        fmt.Println("Batch acknowledged")
    case BatchRecvFailOpCode:
        // Batch processing failed
        fmt.Println("Batch failed")
    }
}
```

## Protocol Flow

1. **Connection Establishment**: Client establishes TCP connection to server
2. **Data Transmission**: Client sends batches using appropriate OpCodes
3. **Acknowledgments**: Server responds with success/failure for each batch
4. **Completion**: Client sends `Finished` message when done
5. **Connection Termination**: Connection is closed

## Implementation Notes

- All multi-byte integers use little-endian byte order
- String data is UTF-8 encoded
- Batch size is limited to 1MB to prevent memory issues
- Batch numbering is sequential and managed automatically
- The protocol is designed for high-throughput data transfer with flow control through acknowledgments

## Thread Safety

The protocol implementation is **not thread-safe**. If multiple goroutines need to send data simultaneously, external synchronization is required.