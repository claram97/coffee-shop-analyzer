package common

import (
	"bytes"
	"encoding/binary"
)

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

// serializeRowToBuffer serializes a row to a temporary buffer for size calculation
func serializeRowToBuffer(row map[string]string) (*bytes.Buffer, error) {
	var buff bytes.Buffer
	if err := writeStringMap(&buff, row); err != nil {
		return nil, err
	}
	return &buff, nil
}
