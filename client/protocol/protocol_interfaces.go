package common

import (
	"bufio"
	"fmt"
	"io"
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
//
// Note: This interface follows Go's standard io.WriterTo interface signature,
// returning int64 for the number of bytes written.
type Writeable interface {
	WriteTo(out io.Writer) (int64, error)
}

// Readable is implemented by inbound messages that can parse themselves
// from a bufio.Reader, consuming exactly their body according to framing.
type Readable interface {
	readFrom(reader *bufio.Reader) error
	Message
}
