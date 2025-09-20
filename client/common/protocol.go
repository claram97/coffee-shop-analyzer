package common

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

const NewBatchOpCode byte = 0
const BatchRecvSuccessOpCode byte = 1
const BatchRecvFailOpCode byte = 2
const FinishedOpCode byte = 3

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

func (msg *Finished) GetOpCode() byte  { return FinishedOpCode }
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

// AddBetWithFlush serializes a single bet as a [string map] and attempts to
// append it to the current batch buffer `to`. If appending would exceed the
// 8 KiB package limit (including opcode+length+n headers) or the given
// batchLimit, this function first FlushBatch(to, finalOutput, *linesCounter)
// and then starts a new batch with this line, setting *linesCounter = 1.
// On success, it increments *linesCounter and returns nil; any I/O/encoding
// error is returned.
func AddLineWithFlush(line map[string]string, to *bytes.Buffer, finalOutput io.Writer, linesCounter *int32, batchLimit int32) error {
	var buff bytes.Buffer
	if err := writeStringMap(&buff, line); err != nil {
		return err
	}
	if to.Len()+buff.Len()+1+4+4 <= 8*1024 && *linesCounter+1 <= batchLimit {
		_, err := io.Copy(to, &buff)
		if err != nil {
			return err
		}
		*linesCounter++
		return nil
	}
	if err := FlushBatch(to, finalOutput, *linesCounter); err != nil {
		return err
	}
	if err := writeStringMap(to, line); err != nil {
		return err
	}
	*linesCounter = 1
	return nil
}

// FlushBatch frames and writes a NewBatch message to `out` from the accumulated
// body in `batch`. The wire format is:
//
//	[opcode=NewBatch:1][length=i32 LE (4 + bodyLen)][nBets=i32 LE][body]
//
// After a successful write it resets the batch buffer. Any write error is returned.
func FlushBatch(batch *bytes.Buffer, out io.Writer, linesCounter int32) error {
	if err := binary.Write(out, binary.LittleEndian, NewBatchOpCode); err != nil {
		return err
	}
	if err := binary.Write(out, binary.LittleEndian, int32(4+batch.Len())); err != nil {
		return err
	}
	if err := binary.Write(out, binary.LittleEndian, linesCounter); err != nil {
		return err
	}
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

// BatchRecvSuccess is the server→client acknowledgment for a batch processed
// successfully. Its body length is always 0.
type BatchRecvSuccess struct{}

func (msg *BatchRecvSuccess) GetOpCode() byte  { return BatchRecvSuccessOpCode }
func (msg *BatchRecvSuccess) GetLength() int32 { return 0 }

// readFrom validates that the next i32 body length is exactly 0.
// It consumes the field and returns nil on success.
func (msg *BatchRecvSuccess) readFrom(reader *bufio.Reader) error {
	var length int32
	if err := binary.Read(reader, binary.LittleEndian, &length); err != nil {
		return err
	}
	if length != msg.GetLength() {
		return &ProtocolError{"invalid body length", BatchRecvSuccessOpCode}
	}
	return nil
}

// BatchRecvFail is the server→client negative acknowledgment for a batch.
// Its body length is always 0.
type BatchRecvFail struct{}

func (msg *BatchRecvFail) GetOpCode() byte  { return BatchRecvFailOpCode }
func (msg *BatchRecvFail) GetLength() int32 { return 0 }

// readFrom validates that the next i32 body length is exactly 0.
// It consumes the field and returns nil on success.
func (msg *BatchRecvFail) readFrom(reader *bufio.Reader) error {
	var length int32
	if err := binary.Read(reader, binary.LittleEndian, &length); err != nil {
		return err
	}
	if length != msg.GetLength() {
		return &ProtocolError{"invalid body length", BatchRecvFailOpCode}
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
	case BatchRecvSuccessOpCode:
		{
			var msg BatchRecvSuccess
			err := msg.readFrom(reader)
			return &msg, err
		}
	case BatchRecvFailOpCode:
		{
			var msg BatchRecvFail
			err := msg.readFrom(reader)
			return &msg, err
		}
	default:
		return nil, &ProtocolError{"invalid opcode", opcode}
	}
}
