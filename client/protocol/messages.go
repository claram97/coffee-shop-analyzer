package common

import (
	"bufio"
	"encoding/binary"
	"io"
)

// Finished is a client→server message that indicates the agency finished
// sending all its batch messages. Body: [agencyId:i32].
type Finished struct {
	AgencyId int32
}

func (msg *Finished) GetOpCode() byte  { return OpCodeFinished }
func (msg *Finished) GetLength() int32 { return 4 }

// WriteTo writes the FINISHED frame with little-endian length and agencyId.
// It returns the total bytes written (1 + 4 + 4) or an error.
func (msg *Finished) WriteTo(out io.Writer) (int64, error) {
	var totalWritten int64

	if err := binary.Write(out, binary.LittleEndian, msg.GetOpCode()); err != nil {
		return totalWritten, err
	}
	totalWritten += 1

	if err := binary.Write(out, binary.LittleEndian, msg.GetLength()); err != nil {
		return totalWritten, err
	}
	totalWritten += 4

	if err := binary.Write(out, binary.LittleEndian, msg.AgencyId); err != nil {
		return totalWritten, err
	}
	totalWritten += 4

	return totalWritten, nil
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
