package common

import (
	"bufio"
	"encoding/binary"
	"io"
	"github.com/op/go-logging"
	"fmt"
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

// DataBatch represents a batch of data containing another message inside.
type DataBatch struct {
	OpCode      byte
	QueryIDs    []int32
	TableIDs    []byte
	InnerMsg    Readable
	BatchNum    int64
	TotalShards int32
	ShardNum    int32
	MetaData    map[int32]int32
}

func (msg *DataBatch) GetOpCode() byte  { return OpCodeDataBatch }
func (msg *DataBatch) GetLength() int32 { return 0 } // Not directly used

// readFrom consumes the DataBatch body including its inner message
func (msg *DataBatch) readFrom(reader *bufio.Reader) error {
	// Read length as int32
	var length int32
	if err := binary.Read(reader, binary.LittleEndian, &length); err != nil {
		return err
	}

	// Read initial part of body to determine structure
	// For now, just skip to the inner message by finding its opcode
	innerOpcode, err := reader.ReadByte()
	if err != nil {
		return err
	}
	
	// Store the inner opcode for use in logging/handling
	msg.OpCode = innerOpcode

	// Skip the rest of the DataBatch structure, just read the result type for logging
	// In a full implementation, we'd parse all fields properly
	return nil
}

// QueryResult represents a generic query result
type QueryResult struct {
	OpCode     byte
	ResultData interface{}
}

func (msg *QueryResult) GetOpCode() byte  { return msg.OpCode }
func (msg *QueryResult) GetLength() int32 { return 0 } // Not directly used

func (msg *QueryResult) readFrom(reader *bufio.Reader) error {
	// Read length as int32
	var length int32
	if err := binary.Read(reader, binary.LittleEndian, &length); err != nil {
		return err
	}

	// For debugging, just skip the content
	// In production, we would parse the specific result structure based on opcode
	return nil
}

type Query2Result struct {
	Month    string
	Name     string
	Quantity int32
	Revenue  float64
	OpCode   byte
	Results  []Query2Result
}

func (msg *Query2Result) GetOpCode() byte  { return msg.OpCode }
func (msg *Query2Result) GetLength() int32 { return 0 } // Not directly used

func (msg *Query2Result) readFrom(reader *bufio.Reader) error {
	// Leer cantidad de filas
	var nRows int32
	if err := binary.Read(reader, binary.LittleEndian, &nRows); err != nil {
		return err
	}

	var results []Query2Result
	for rowIdx := int32(0); rowIdx < nRows; rowIdx++ {
		var batchNumber int64
		if err := binary.Read(reader, binary.LittleEndian, &batchNumber); err != nil {
			return err
		}
		var status byte
		if err := binary.Read(reader, binary.LittleEndian, &status); err != nil {
			return err
		}

		// Leer cantidad de pares clave-valor
		var nPairs int32
		if err := binary.Read(reader, binary.LittleEndian, &nPairs); err != nil {
			return err
		}

		var month, name, quantityStr, revenueStr string
		for i := int32(0); i < nPairs; i++ {
			// Leer clave
			var keyLen int32
			if err := binary.Read(reader, binary.LittleEndian, &keyLen); err != nil {
				return err
			}
			keyBytes := make([]byte, keyLen)
			if _, err := io.ReadFull(reader, keyBytes); err != nil {
				return err
			}
			key := string(keyBytes)

			// Leer valor
			var valLen int32
			if err := binary.Read(reader, binary.LittleEndian, &valLen); err != nil {
				return err
			}
			valBytes := make([]byte, valLen)
			if _, err := io.ReadFull(reader, valBytes); err != nil {
				return err
			}
			val := string(valBytes)

			switch key {
			case "month":
				month = val
			case "name":
				name = val
			case "quantity":
				quantityStr = val
			case "revenue":
				revenueStr = val
			}
		}

		var quantity int32
		if len(quantityStr) > 0 {
			fmt.Sscanf(quantityStr, "%d", &quantity)
		}
		var revenue float64
		if len(revenueStr) > 0 {
			fmt.Sscanf(revenueStr, "%f", &revenue)
		}

		result := Query2Result{
			Month:    month,
			Name:     name,
			Quantity: quantity,
			Revenue:  revenue,
			OpCode:   msg.OpCode,
		}
		results = append(results, result)
	}

	// Guardar los resultados en el struct
	msg.Results = results
	msg.OpCode = 21 // Asignar el opcode correspondiente
	return nil
}

// ReadMessage reads exactly one framed server response from reader.
// It consumes the opcode, dispatches to the message parser (which
// validates and consumes the body), and returns the parsed message.
// On invalid opcode or framing, a ProtocolError is returned; on I/O
// issues, the underlying error is returned.
func ReadMessage(reader *bufio.Reader, logger *logging.Logger) (Readable, error) {
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
	case OpCodeDataBatch:
		{
			var msg DataBatch
			err := msg.readFrom(reader)
			return &msg, err
		}
	case 21:
		{
			// var length int32
			// if err := binary.Read(reader, binary.LittleEndian, &length); err != nil {
			// 	return nil, err
			// }
			var msg Query2Result
			err := msg.readFrom(reader)
			logger.Infof("action: query_result_received | result: query_2 | opcode: %d | rows: %d", msg.GetOpCode(), len(msg.Results))
			return &msg, err
			// return nil, &ProtocolError{"opcode is 21: query 2. length: " + fmt.Sprintf("%d", length), opcode}
		}
	case 22:
		{
			var length int32
			if err := binary.Read(reader, binary.LittleEndian, &length); err != nil {
				return nil, err
			}
			// Opcional: validar que length == esperado
			// var msg QueryResult1
			// err := msg.readFrom(reader)
			return nil, &ProtocolError{"opcode is 22: query 3. length: " + fmt.Sprintf("%d", length), opcode}

		}
	case 23:
		{
			var length int32
			if err := binary.Read(reader, binary.LittleEndian, &length); err != nil {
				return nil, err
			}
			// Opcional: validar que length == esperado
			// var msg QueryResult1
			// err := msg.readFrom(reader)
			return nil, &ProtocolError{"opcode is 23: query 4. length: " + fmt.Sprintf("%d", length), opcode}
		}
	case 20:
		{
			var length int32
			if err := binary.Read(reader, binary.LittleEndian, &length); err != nil {
				return nil, err
			}
			// Opcional: validar que length == esperado
			// var msg QueryResult1
			// err := msg.readFrom(reader)
			return nil, &ProtocolError{"opcode is 20: query 1. length: " + fmt.Sprintf("%d", length), opcode}
		}
	default:
		return nil, &ProtocolError{"invalid opcode", opcode}
	}
}
