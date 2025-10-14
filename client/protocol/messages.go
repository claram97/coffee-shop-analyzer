package common

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"strconv"

	"github.com/op/go-logging"
)

// parseFloat parses a string to float64, returns 0 on error
func parseFloat(s string) float64 {
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0
	}
	return f
}

// Finished is a client→server message that indicates the agency finished
// sending all its batch messages. Body: [] (empty).
type Finished struct{}

func (msg *Finished) GetOpCode() byte  { return OpCodeFinished }
func (msg *Finished) GetLength() int32 { return 0 }

// WriteTo writes the FINISHED frame with little-endian length.
// It returns the total bytes written (1 + 4) or an error.
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

	return totalWritten, nil
}

// readFrom validates that the next i32 body length is exactly 0.
// It consumes the field and returns nil on success.
func (msg *Finished) readFrom(reader *bufio.Reader) error {
	var length int32
	if err := binary.Read(reader, binary.LittleEndian, &length); err != nil {
		return err
	}
	if length != msg.GetLength() {
		return &ProtocolError{"invalid body length", OpCodeFinished}
	}
	return nil
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
type ShardInfo struct {
	Total byte
	Index byte
}

type QueryResultTable struct {
	OpCode      byte
	BatchNumber int64
	BatchStatus byte
	Rows        []map[string]string
}

type Query1Result struct {
	FinalAmount   float64 `json:"final_amount"`
	TransactionID string  `json:"transaction_id"`
}

type Query2Result struct {
	Month    string  `json:"month"`
	Name     string  `json:"name"`
	Quantity int     `json:"quantity"`
	Revenue  float64 `json:"revenue"`
}

type Query3Result struct {
	Amount    float64 `json:"amount"`
	Period    string  `json:"period"`
	StoreName string  `json:"store_name"`
}

type Query4Result struct {
	Birthdate     string `json:"birthdate"`
	PurchaseCount int    `json:"purchase_count"`
	StoreName     string `json:"store_name"`
}

func (t *QueryResultTable) GetOpCode() byte  { return t.OpCode }
func (t *QueryResultTable) GetLength() int32 { return 0 }

func (t *QueryResultTable) readFrom(reader *bufio.Reader) error {
	var length int32
	if err := binary.Read(reader, binary.LittleEndian, &length); err != nil {
		return err
	}
	if length < 0 {
		return &ProtocolError{"negative body length", t.OpCode}
	}

	body := make([]byte, length)
	if _, err := io.ReadFull(reader, body); err != nil {
		return err
	}

	return t.populateFromBody(body)
}

func (t *QueryResultTable) populateFromBody(body []byte) error {
	batchNumber, status, rows, err := parseTableMessageBody(body, t.OpCode)
	if err != nil {
		return err
	}

	t.BatchNumber = batchNumber
	t.BatchStatus = status
	t.Rows = rows
	return nil
}

func (t *QueryResultTable) GetTypedRows() (interface{}, error) {
	switch t.OpCode {
	case OpCodeQueryResult1:
		results := make([]Query1Result, len(t.Rows))
		for i, row := range t.Rows {
			finalAmount, _ := strconv.ParseFloat(row["final_amount"], 64)
			results[i] = Query1Result{
				FinalAmount:   finalAmount,
				TransactionID: row["transaction_id"],
			}
		}
		return results, nil
	case OpCodeQueryResult2:
		results := make([]Query2Result, len(t.Rows))
		for i, row := range t.Rows {
			quantity, _ := strconv.Atoi(row["quantity"])
			revenue, _ := strconv.ParseFloat(row["revenue"], 64)
			results[i] = Query2Result{
				Month:    row["month"],
				Name:     row["name"],
				Quantity: quantity,
				Revenue:  revenue,
			}
		}
		return results, nil
	case OpCodeQueryResult3:
		results := make([]Query3Result, len(t.Rows))
		for i, row := range t.Rows {
			amount, _ := strconv.ParseFloat(row["amount"], 64)
			results[i] = Query3Result{
				Amount:    amount,
				Period:    row["period"],
				StoreName: row["store_name"],
			}
		}
		return results, nil
	case OpCodeQueryResult4:
		results := make([]Query4Result, len(t.Rows))
		for i, row := range t.Rows {
			purchaseCount, _ := strconv.Atoi(row["purchase_count"])
			results[i] = Query4Result{
				Birthdate:     row["birthdate"],
				PurchaseCount: purchaseCount,
				StoreName:     row["store_name"],
			}
		}
		return results, nil
	case OpCodeQueryResultError:
		// Mantener genérico o agregar un struct si hay campos específicos
		return t.Rows, nil
	default:
		return nil, fmt.Errorf("unsupported opcode for typed rows: %d", t.OpCode)
	}
}

type DataBatch struct {
	OpCode      byte
	QueryIDs    []int32
	TableIDs    []byte
	BatchNum    int64
	Reserved    uint16
	MetaData    map[int32]int32
	ShardsInfo  []ShardInfo
	TotalShards int32
	ShardNum    int32
	InnerOpcode byte
	InnerLength int32
	InnerRaw    []byte
	ResultTable *QueryResultTable
}

func (msg *DataBatch) GetOpCode() byte  { return OpCodeDataBatch }
func (msg *DataBatch) GetLength() int32 { return 0 } // Not directly used

// readFrom consumes the DataBatch body including its inner message
func (msg *DataBatch) readFrom(reader *bufio.Reader) error {
	var length int32
	if err := binary.Read(reader, binary.LittleEndian, &length); err != nil {
		return err
	}
	if length < 0 {
		return &ProtocolError{"negative body length", OpCodeDataBatch}
	}

	body := make([]byte, length)
	if _, err := io.ReadFull(reader, body); err != nil {
		return err
	}

	msg.OpCode = OpCodeDataBatch
	msg.MetaData = make(map[int32]int32)

	offset := 0
	if length == 0 {
		return &ProtocolError{"empty DataBatch body", OpCodeDataBatch}
	}

	// table_ids
	if offset >= len(body) {
		return &ProtocolError{"missing table ids length", OpCodeDataBatch}
	}
	tableCount := int(body[offset])
	offset++
	if len(body) < offset+tableCount {
		return &ProtocolError{"truncated table ids", OpCodeDataBatch}
	}
	msg.TableIDs = append([]byte(nil), body[offset:offset+tableCount]...)
	offset += tableCount

	// query_ids
	if offset >= len(body) {
		return &ProtocolError{"missing query ids length", OpCodeDataBatch}
	}
	queryCount := int(body[offset])
	offset++
	if len(body) < offset+queryCount {
		return &ProtocolError{"truncated query ids", OpCodeDataBatch}
	}
	msg.QueryIDs = make([]int32, queryCount)
	for i := 0; i < queryCount; i++ {
		msg.QueryIDs[i] = int32(body[offset])
		offset++
	}

	// reserved
	if len(body) < offset+2 {
		return &ProtocolError{"missing reserved field", OpCodeDataBatch}
	}
	msg.Reserved = binary.LittleEndian.Uint16(body[offset : offset+2])
	offset += 2

	// batch number
	if len(body) < offset+8 {
		return &ProtocolError{"missing batch number", OpCodeDataBatch}
	}
	msg.BatchNum = int64(binary.LittleEndian.Uint64(body[offset : offset+8]))
	offset += 8

	// meta dictionary
	if len(body) < offset+1 {
		return &ProtocolError{"missing meta length", OpCodeDataBatch}
	}
	metaCount := int(body[offset])
	offset++
	for i := 0; i < metaCount; i++ {
		if len(body) < offset+2 {
			return &ProtocolError{"truncated meta entry", OpCodeDataBatch}
		}
		key := int32(body[offset])
		val := int32(body[offset+1])
		msg.MetaData[key] = val
		offset += 2
	}

	// shards info
	if len(body) < offset+1 {
		return &ProtocolError{"missing shards length", OpCodeDataBatch}
	}
	shardCount := int(body[offset])
	offset++
	msg.ShardsInfo = make([]ShardInfo, shardCount)
	for i := 0; i < shardCount; i++ {
		if len(body) < offset+2 {
			return &ProtocolError{"truncated shard entry", OpCodeDataBatch}
		}
		total := body[offset]
		index := body[offset+1]
		msg.ShardsInfo[i] = ShardInfo{Total: total, Index: index}
		offset += 2
	}
	if shardCount > 0 {
		msg.TotalShards = int32(msg.ShardsInfo[0].Total)
		msg.ShardNum = int32(msg.ShardsInfo[0].Index)
	} else {
		msg.TotalShards = 1
		msg.ShardNum = 0
	}

	if len(body) < offset+5 {
		return &ProtocolError{"missing embedded message header", OpCodeDataBatch}
	}

	msg.InnerOpcode = body[offset]
	offset++
	innerLen := int32(binary.LittleEndian.Uint32(body[offset : offset+4]))
	msg.InnerLength = innerLen
	offset += 4
	if innerLen < 0 {
		return &ProtocolError{"negative embedded length", OpCodeDataBatch}
	}

	if len(body) < offset+int(innerLen) {
		return &ProtocolError{"truncated embedded message", OpCodeDataBatch}
	}

	msg.InnerRaw = append([]byte(nil), body[offset:offset+int(innerLen)]...)
	offset += int(innerLen)

	if offset != len(body) {
		return &ProtocolError{"extra bytes after embedded message", OpCodeDataBatch}
	}

	if isQueryResultOpcode(msg.InnerOpcode) {
		table := &QueryResultTable{OpCode: msg.InnerOpcode}
		if err := table.populateFromBody(msg.InnerRaw); err != nil {
			return err
		}
		msg.ResultTable = table
	}

	return nil
}

func isQueryResultOpcode(opcode byte) bool {
	switch opcode {
	case OpCodeQueryResult1, OpCodeQueryResult2, OpCodeQueryResult3, OpCodeQueryResult4, OpCodeQueryResultError:
		return true
	default:
		return false
	}
}

func parseTableMessageBody(body []byte, opcode byte) (int64, byte, []map[string]string, error) {
	offset := 0
	if len(body) < 4 {
		return 0, 0, nil, &ProtocolError{"missing row count", opcode}
	}

	nRows := int32(binary.LittleEndian.Uint32(body[offset : offset+4]))
	if nRows < 0 {
		return 0, 0, nil, &ProtocolError{"negative row count", opcode}
	}
	offset += 4

	if len(body) < offset+8 {
		return 0, 0, nil, &ProtocolError{"missing batch number", opcode}
	}
	batchNumber := int64(binary.LittleEndian.Uint64(body[offset : offset+8]))
	offset += 8

	if len(body) < offset+1 {
		return 0, 0, nil, &ProtocolError{"missing batch status", opcode}
	}
	status := body[offset]
	offset++

	rows := make([]map[string]string, 0, nRows)
	for i := int32(0); i < nRows; i++ {
		if len(body) < offset+4 {
			return 0, 0, nil, &ProtocolError{"missing pair count", opcode}
		}
		nPairs := int32(binary.LittleEndian.Uint32(body[offset : offset+4]))
		if nPairs < 0 {
			return 0, 0, nil, &ProtocolError{"negative pair count", opcode}
		}
		offset += 4

		row := make(map[string]string, nPairs)
		for j := int32(0); j < nPairs; j++ {
			if len(body) < offset+4 {
				return 0, 0, nil, &ProtocolError{"missing key length", opcode}
			}
			keyLen := int32(binary.LittleEndian.Uint32(body[offset : offset+4]))
			if keyLen < 0 {
				return 0, 0, nil, &ProtocolError{"negative key length", opcode}
			}
			offset += 4

			if len(body) < offset+int(keyLen) {
				return 0, 0, nil, &ProtocolError{"truncated key", opcode}
			}
			key := string(body[offset : offset+int(keyLen)])
			offset += int(keyLen)

			if len(body) < offset+4 {
				return 0, 0, nil, &ProtocolError{"missing value length", opcode}
			}
			valLen := int32(binary.LittleEndian.Uint32(body[offset : offset+4]))
			if valLen < 0 {
				return 0, 0, nil, &ProtocolError{"negative value length", opcode}
			}
			offset += 4

			if len(body) < offset+int(valLen) {
				return 0, 0, nil, &ProtocolError{"truncated value", opcode}
			}
			value := string(body[offset : offset+int(valLen)])
			offset += int(valLen)

			row[key] = value
		}

		rows = append(rows, row)
	}

	if offset != len(body) {
		return 0, 0, nil, &ProtocolError{fmt.Sprintf("table message length mismatch: consumed=%d total=%d", offset, len(body)), opcode}
	}

	return batchNumber, status, rows, nil
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
	case OpCodeQueryResult1, OpCodeQueryResult2, OpCodeQueryResult3, OpCodeQueryResult4, OpCodeQueryResultError:
		{
			msg := &QueryResultTable{OpCode: opcode}
			err := msg.readFrom(reader)
			if err == nil && logger != nil {
				logger.Infof("action: query_result_received | opcode: %d | status: %d | rows: %d", msg.OpCode, msg.BatchStatus, len(msg.Rows))
			}
			for _, row := range msg.Rows {
				switch msg.OpCode {
				case OpCodeQueryResult1:
					logger.Infof("Transaction ID: %s | Final Amount: $%.2f", row["transaction_id"], parseFloat(row["final_amount"]))
				case OpCodeQueryResult2:
					logger.Infof("Product: %s | Month: %s | Quantity: %s | Revenue: $%.2f", row["name"], row["month"], row["quantity"], parseFloat(row["revenue"]))
				case OpCodeQueryResult3:
					logger.Infof("Store: %s | Period: %s | Amount: $%.2f", row["store_name"], row["period"], parseFloat(row["amount"]))
				case OpCodeQueryResult4:
					logger.Infof("Customer Birthdate: %s | Purchase Count: %s | Store: %s", row["birthdate"], row["purchase_count"], row["store_name"])
				case OpCodeQueryResultError:
					rowJson, _ := json.Marshal(row)
					logger.Infof("Error Row: %s", string(rowJson))
				}
				logger.Infof("") // Add blank line between rows
			}
			return msg, err
		}
	case OpCodeFinished:
		{
			var msg Finished
			err := msg.readFrom(reader)
			logger.Infof("action: finished_received | result: success")
			return &msg, err
		}
	default:
		return nil, &ProtocolError{"invalid opcode", opcode}
	}
}

// Query 2
// 2025-10-13 00:04:28 INFO     row: map[month:2024-01 name:Hot Chocolate quantity:310703 revenue:2796327.0]

// Query 4
// 2025-10-13 00:06:42 INFO     row: map[birthdate:1965-04-01 purchase_count:3 store_name:G Coffee @ Seksyen 21]

// Query 1
// 2025-10-13 00:06:22 INFO     row: map[final_amount:75.0 transaction_id:e3d6673f-5dca-4878-bdf2-137affa75725]

// Query 3
// 2025-10-13 00:06:21 INFO     row: map[amount:2053737.0 period:2024-S1 store_name:G Coffee @ Alam Tun Hussein Onn]
