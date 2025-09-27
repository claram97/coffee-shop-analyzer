package common

import (
	"bufio"
	"encoding/binary"
	"io"
)

// Finished is a client→server message that indicates the agency finished
// sending all its bets. Body: [agencyId:i32].
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
