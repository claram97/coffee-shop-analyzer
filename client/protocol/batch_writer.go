package common

import (
	"bytes"
	"encoding/binary"
	"io"
)

// writeOpCode writes the opcode field to the output
func writeOpCode(out io.Writer, opCode byte) error {
	return binary.Write(out, binary.LittleEndian, opCode)
}

// calculateMessageLength calculates the total message length including headers and body
func calculateMessageLength(bodyLen int) int32 {
	return int32(4 + 8 + 1 + bodyLen) // nLines + batchNumber + status + bodyLen
}

// writeMessageLength writes the message length field to the output
func writeMessageLength(out io.Writer, length int32) error {
	return binary.Write(out, binary.LittleEndian, length)
}

// writeLineCounter writes the number of lines field to the output
func writeLineCounter(out io.Writer, counter int32) error {
	return binary.Write(out, binary.LittleEndian, counter)
}

// writeBatchNumber writes the batch number field to the output
func writeBatchNumber(out io.Writer, batchNumber int64) error {
	return binary.Write(out, binary.LittleEndian, batchNumber)
}

// writeBatchStatus writes the batch status field to the output
func writeBatchStatus(out io.Writer, batchStatus byte) error {
	return binary.Write(out, binary.LittleEndian, batchStatus)
}

// writeMessageBody writes the message body and resets the batch buffer
func writeMessageBody(out io.Writer, batch *bytes.Buffer) error {
	if _, err := io.Copy(out, batch); err != nil {
		return err
	}
	batch.Reset()
	return nil
}

// FlushBatch frames and writes a message to `out` from the accumulated
// body in `batch`. The wire format is:
//
//	[opcode:1][length=i32 LE (4 + 8 + 1 + bodyLen)][nLines=i32 LE][batchNumber=i64 LE][status=u8][body]
//
// After a successful write it resets the batch buffer. Any write error is returned.
func FlushBatch(batch *bytes.Buffer, out io.Writer, counter int32, opCode byte, batchNumber int64, batchStatus byte) error {
	// Write opcode
	if err := writeOpCode(out, opCode); err != nil {
		return err
	}

	// Write message length
	length := calculateMessageLength(batch.Len())
	if err := writeMessageLength(out, length); err != nil {
		return err
	}

	// Write line counter
	if err := writeLineCounter(out, counter); err != nil {
		return err
	}

	// Write batch number
	if err := writeBatchNumber(out, batchNumber); err != nil {
		return err
	}

	// Write batch status
	if err := writeBatchStatus(out, batchStatus); err != nil {
		return err
	}

	// Write message body and reset buffer
	return writeMessageBody(out, batch)
}
