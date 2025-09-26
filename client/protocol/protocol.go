package common

// protocol.go serves as the main entry point for the protocol implementation.
// The actual implementation is distributed across specialized files:
//
// - protocol_constants.go: Protocol opcodes, batch status, and limits
// - protocol_interfaces.go: Core interfaces (Message, Writeable, Readable) and ProtocolError
// - serialization.go: String and map serialization functions
// - batch_operations.go: Batch management logic (AddRowToBatch and helpers)
// - batch_writer.go: Batch writing and flushing logic (FlushBatch and helpers)
// - messages.go: Specific message types (Finished, BetsRecvSuccess, BetsRecvFail) and ReadMessage
//
// This architecture provides:
// - Clear separation of concerns
// - Better maintainability and testability
// - Modular code organization
// - Easier debugging and extension
