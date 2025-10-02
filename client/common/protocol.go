package common

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

const NewBetsOpCode byte = 0
const BetsRecvSuccessOpCode byte = 1
const BetsRecvFailOpCode byte = 2
const FinishedOpCode byte = 3
const WinnersOpCode byte = 4

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
// batchLimit, this function first FlushBatch(to, finalOutput, *betsCounter)
// and then starts a new batch with this bet, setting *betsCounter = 1.
// On success, it increments *betsCounter and returns nil; any I/O/encoding
// error is returned.
func AddBetWithFlush(bet map[string]string, to *bytes.Buffer, finalOutput io.Writer, betsCounter *int32, batchLimit int32) error {
	var buff bytes.Buffer
	if err := writeStringMap(&buff, bet); err != nil {
		return err
	}
	if to.Len()+buff.Len()+1+4+4 <= 8*1024 && *betsCounter+1 <= batchLimit {
		_, err := io.Copy(to, &buff)
		if err != nil {
			return err
		}
		*betsCounter++
		return nil
	}
	if err := FlushBatch(to, finalOutput, *betsCounter); err != nil {
		return err
	}
	if err := writeStringMap(to, bet); err != nil {
		return err
	}
	*betsCounter = 1
	return nil
}

// FlushBatch frames and writes a NewBets message to `out` from the accumulated
// body in `batch`. The wire format is:
//
//	[opcode=NewBets:1][length=i32 LE (4 + bodyLen)][nBets=i32 LE][body]
//
// After a successful write it resets the batch buffer. Any write error is returned.
func FlushBatch(batch *bytes.Buffer, out io.Writer, betsCounter int32) error {
	if err := binary.Write(out, binary.LittleEndian, NewBetsOpCode); err != nil {
		return err
	}
	if err := binary.Write(out, binary.LittleEndian, int32(4+batch.Len())); err != nil {
		return err
	}
	if err := binary.Write(out, binary.LittleEndian, betsCounter); err != nil {
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

// Winners is the server→client response listing winner documents for an agency.
// Body format: [n:i32 LE][n × [string]] where [string] is length-prefixed UTF-8.
type Winners struct {
	List []string
}

func (msg *Winners) GetOpCode() byte { return WinnersOpCode }

// GetLength computes the body length: 4 bytes for n plus each string's
// 4-byte length prefix and its bytes.
func (msg *Winners) GetLength() int32 {
	var totalLen int32 = 4
	for _, doc := range msg.List {
		totalLen += 4 + int32(len(doc))
	}
	return totalLen
}

// readFrom parses the Winners body defensively, validating remaining counters,
// string lengths, and consuming exactly the advertised number of bytes.
// It appends each winner ID to msg.List and returns nil on success.
func (msg *Winners) readFrom(reader *bufio.Reader) error {
	var remaining int32
	if err := binary.Read(reader, binary.LittleEndian, &remaining); err != nil {
		return err
	}
	if remaining < 4 {
		return &ProtocolError{"invalid body length", msg.GetOpCode()}
	}
	var nWinners int32
	if err := binary.Read(reader, binary.LittleEndian, &nWinners); err != nil {
		return err
	}
	if nWinners < 0 {
		return &ProtocolError{"invalid body", msg.GetOpCode()}
	}
	remaining -= 4
	for i := int32(0); i < nWinners; i++ {
		if remaining < 4 {
			return &ProtocolError{"invalid body length", msg.GetOpCode()}
		}
		var strLen int32
		if err := binary.Read(reader, binary.LittleEndian, &strLen); err != nil {
			return err
		}
		if strLen < 0 {
			return &ProtocolError{"invalid body", msg.GetOpCode()}
		}
		remaining -= 4
		if remaining < strLen {
			return &ProtocolError{"invalid body length", msg.GetOpCode()}
		}
		buf := make([]byte, int(strLen))
		if _, err := io.ReadFull(reader, buf); err != nil {
			return err
		}
		remaining -= strLen
		msg.List = append(msg.List, string(buf))
	}
	if remaining != 0 {
		return &ProtocolError{"invalid body length", msg.GetOpCode()}
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
	case WinnersOpCode:
		{
			var msg Winners
			err := msg.readFrom(reader)
			return &msg, err
		}
	default:
		return nil, &ProtocolError{"invalid opcode", opcode}
	}
}
