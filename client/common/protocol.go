package common

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

const (
	OpCodeNewBets             byte = 0
	BetsRecvSuccessOpCode     byte = 1
	BetsRecvFailOpCode        byte = 2
	OpCodeFinished            byte = 3
	OpCodeNewMenuItems        byte = 4
	OpCodeNewPaymentMethods   byte = 5
	OpCodeNewStores           byte = 6
	OpCodeNewTransactionItems byte = 7
	OpCodeNewTransaction      byte = 8
	OpCodeNewUsers            byte = 9
	OpCodeNewVouchers         byte = 10
)

// BatchStatus defines the status of a batch being sent
const (
	BatchContinue byte = 0 // Hay más batches en el archivo
	BatchEOF      byte = 1 // Último batch del archivo
	BatchCancel   byte = 2 // Batch enviado por cancelación
)

// ProtocolError models a framing/validation error while parsing or writing
// protocol messages. Opcode, when present, indicates the message context.
type ProtocolError struct {
	Msg    string
	Opcode byte
}

func (e *ProtocolError) Error() string {
	return fmt.Sprintf("protocol error: %s (opcode=%d)", e.Msg, e.Opcode)
}

// Message is implemented by all protocol messages and exposes the opcode
// and the computed body length (for outbound messages).
type Message interface {
	GetOpCode() byte
	GetLength() int32
}

// Writeable is implemented by outbound messages that can serialize themselves
// to the wire format: [opcode:1][length:i32 LE][body]. It returns the total
// number of bytes written (header + body) and any I/O error.
type Writeable interface {
	WriteTo(out io.Writer) (int32, error)
}

// Finished is a client→server message that indicates the agency finished
// sending all its bets. Body: [agencyId:i32].
type Finished struct {
	AgencyId int32
}

func (msg *Finished) GetOpCode() byte  { return OpCodeFinished }
func (msg *Finished) GetLength() int32 { return 4 }

// WriteTo writes the FINISHED frame with little-endian length and agencyId.
// It returns the total bytes written (1 + 4 + 4) or an error.
func (msg *Finished) WriteTo(out io.Writer) (int32, error) {
	if err := binary.Write(out, binary.LittleEndian, msg.GetOpCode()); err != nil {
		return 0, err
	}
	if err := binary.Write(out, binary.LittleEndian, msg.GetLength()); err != nil {
		return 0, err
	}
	if err := binary.Write(out, binary.LittleEndian, msg.AgencyId); err != nil {
		return 0, err
	}
	return 5 + msg.GetLength(), nil
}

// writeString writes a protocol [string]: length (i32 LE) + UTF-8 bytes.
func writeString(buff *bytes.Buffer, s string) error {
	if err := binary.Write(buff, binary.LittleEndian, int32(len(s))); err != nil {
		return err
	}
	_, err := buff.WriteString(s)
	return err
}

// writePair writes a protocol key/value pair as two [string]s in sequence.
func writePair(buff *bytes.Buffer, k string, v string) error {
	if err := writeString(buff, k); err != nil {
		return err
	}
	return writeString(buff, v)
}

// writeStringMap writes a protocol [string map]:
// first the number of pairs (i32 LE) and then each <k, v> as [string][string].
func writeStringMap(buff *bytes.Buffer, body map[string]string) error {
	if err := binary.Write(buff, binary.LittleEndian, int32(len(body))); err != nil {
		return err
	}
	for k, v := range body {
		if err := writePair(buff, k, v); err != nil {
			return err
		}
	}
	return nil
}

// AddRowToBatch serializes a single row as a [string map] and attempts to
// append it to the current batch buffer `to`. If appending would exceed the
// 8 KiB package limit (including opcode+length+n headers) or the given
// batchLimit, this function first FlushBatch(to, finalOutput, *counter, batchNumber)
// and then starts a new batch with this row, setting *counter = 1.
// On success, it increments *counter and returns nil; any I/O/encoding
// error is returned. NOTE: batchNumber is used for the current batch being built.
func AddRowToBatch(row map[string]string, to *bytes.Buffer, finalOutput io.Writer, counter *int32, batchLimit int32, opCode byte, batchNumber *int64) error {
	var buff bytes.Buffer
	if err := writeStringMap(&buff, row); err != nil {
		return err
	}

	// Verificar si la nueva fila cabe en el batch actual (8KB y límite de filas)
	if to.Len()+buff.Len()+1+4+8+4 <= 8*1024 && *counter+1 <= batchLimit { // Agregué +8 para batchNumber
		// Cabe en el batch actual, agregarlo
		_, err := io.Copy(to, &buff)
		if err != nil {
			return err
		}
		*counter++
		return nil
	}

	// No cabe en el batch actual, hacer flush del batch completo
	*batchNumber++ // Incrementar el número de batch antes de enviarlo
	if err := FlushBatch(to, finalOutput, *counter, opCode, *batchNumber, BatchContinue); err != nil {
		return err
	}

	// Empezar un nuevo batch con esta fila
	if err := writeStringMap(to, row); err != nil {
		return err
	}
	*counter = 1
	return nil
}

// FlushBatch frames and writes a message to `out` from the accumulated
// body in `batch`. The wire format is:
//
//	[opcode:1][length=i32 LE (4 + 8 + 1 + bodyLen)][nLines=i32 LE][batchNumber=i64 LE][status=u8][body]
//
// After a successful write it resets the batch buffer. Any write error is returned.
func FlushBatch(batch *bytes.Buffer, out io.Writer, counter int32, opCode byte, batchNumber int64, batchStatus byte) error {
	// [opcode:1]
	if err := binary.Write(out, binary.LittleEndian, opCode); err != nil {
		return err
	}

	// [length:i32] - Ahora incluye 4 (nLines) + 8 (batchNumber) + 1 (status) + bodyLen
	if err := binary.Write(out, binary.LittleEndian, int32(4+8+1+batch.Len())); err != nil {
		return err
	}

	// [nLines:i32]
	if err := binary.Write(out, binary.LittleEndian, counter); err != nil {
		return err
	}

	// [batchNumber:i64]
	if err := binary.Write(out, binary.LittleEndian, batchNumber); err != nil {
		return err
	}

	// [status:u8] ← NUEVO CAMPO - BatchStatus (Continue/EOF/Cancel)
	if err := binary.Write(out, binary.LittleEndian, batchStatus); err != nil {
		return err
	}

	// [body]
	if _, err := io.Copy(out, batch); err != nil {
		return err
	}

	batch.Reset()
	return nil
}

// Readable is implemented by inbound messages that can parse themselves
// from a bufio.Reader, consuming exactly their body according to framing.
type Readable interface {
	readFrom(reader *bufio.Reader) error
	Message
}

// BetsRecvSuccess is the server→client acknowledgment for a batch processed
// successfully. Its body length is always 0.
type BetsRecvSuccess struct{}

func (msg *BetsRecvSuccess) GetOpCode() byte  { return BetsRecvSuccessOpCode }
func (msg *BetsRecvSuccess) GetLength() int32 { return 0 }

// readFrom validates that the next i32 body length is exactly 0.
// It consumes the field and returns nil on success.
func (msg *BetsRecvSuccess) readFrom(reader *bufio.Reader) error {
	var length int32
	if err := binary.Read(reader, binary.LittleEndian, &length); err != nil {
		return err
	}
	if length != msg.GetLength() {
		return &ProtocolError{"invalid body length", BetsRecvSuccessOpCode}
	}
	return nil
}

// BetsRecvFail is the server→client negative acknowledgment for a batch.
// Its body length is always 0.
type BetsRecvFail struct{}

func (msg *BetsRecvFail) GetOpCode() byte  { return BetsRecvFailOpCode }
func (msg *BetsRecvFail) GetLength() int32 { return 0 }

// readFrom validates that the next i32 body length is exactly 0.
// It consumes the field and returns nil on success.
func (msg *BetsRecvFail) readFrom(reader *bufio.Reader) error {
	var length int32
	if err := binary.Read(reader, binary.LittleEndian, &length); err != nil {
		return err
	}
	if length != msg.GetLength() {
		return &ProtocolError{"invalid body length", BetsRecvFailOpCode}
	}
	return nil
}

// ReadMessage reads exactly one framed server response from reader.
// It consumes the opcode, dispatches to the message parser (which
// validates and consumes the body), and returns the parsed message.
// On invalid opcode or framing, a ProtocolError is returned; on I/O
// issues, the underlying error is returned.
func ReadMessage(reader *bufio.Reader) (Readable, error) {
	var opcode byte
	var err error
	if opcode, err = reader.ReadByte(); err != nil {
		return nil, err
	}
	switch opcode {
	case BetsRecvSuccessOpCode:
		{
			var msg BetsRecvSuccess
			err := msg.readFrom(reader)
			return &msg, err
		}
	case BetsRecvFailOpCode:
		{
			var msg BetsRecvFail
			err := msg.readFrom(reader)
			return &msg, err
		}
	default:
		return nil, &ProtocolError{"invalid opcode", opcode}
	}
}
