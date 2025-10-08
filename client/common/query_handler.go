package common

import (
	"encoding/binary"
	"fmt"
	"net"
	"time"

	"github.com/op/go-logging"
	"google.golang.org/protobuf/proto"

	pb "github.com/7574-sistemas-distribuidos/docker-compose-init/client/protos"
)

// QueryHandler handles query requests and receiving results
type QueryHandler struct {
	clientID string
	conn     net.Conn
	log      *logging.Logger
}

// NewQueryHandler creates a new query handler for requesting and receiving results
func NewQueryHandler(conn net.Conn, clientID string, logger *logging.Logger) *QueryHandler {
	return &QueryHandler{
		clientID: clientID,
		conn:     conn,
		log:      logger,
	}
}

// RequestQueryResult sends a query request to the server using protobuf
// Query types are 1-4, corresponding to Q1-Q4
func (qh *QueryHandler) RequestQueryResult(queryType int) error {
	if queryType < 1 || queryType > 4 {
		return fmt.Errorf("invalid query type: %d (must be 1-4)", queryType)
	}

	qh.log.Infof("action: request_query | result: sending | query_type: %d", queryType)

	// Map query type to protobuf QueryType enum
	// Note: QueryType enum starts at 0, so we subtract 1
	var pbQueryType pb.QueryType
	switch queryType {
	case 1:
		pbQueryType = pb.QueryType_QUERY_1
	case 2:
		pbQueryType = pb.QueryType_QUERY_2
	case 3:
		pbQueryType = pb.QueryType_QUERY_3
	case 4:
		pbQueryType = pb.QueryType_QUERY_4
	default:
		return fmt.Errorf("invalid query type: %d", queryType)
	}

	// Create QueryRequest message
	queryRequest := &pb.QueryRequest{
		QueryType: pbQueryType,
	}

	// Create Envelope
	envelope := &pb.Envelope{
		Type: pb.Envelope_QUERY_REQUEST,
		Payload: &pb.Envelope_QueryRequest{
			QueryRequest: queryRequest,
		},
	}

	// Serialize
	msgBytes, err := proto.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("failed to marshal query request: %v", err)
	}

	// Write size (4 bytes big-endian)
	msgSize := uint32(len(msgBytes))
	if err := binary.Write(qh.conn, binary.BigEndian, msgSize); err != nil {
		return fmt.Errorf("failed to send query size: %v", err)
	}

	// Write data
	if _, err := qh.conn.Write(msgBytes); err != nil {
		return fmt.Errorf("failed to send query request: %v", err)
	}

	qh.log.Infof("action: request_query | result: sent | query_type: %d", queryType)
	return nil
}

// RequestAllQueries requests all query types (1-4)
func (qh *QueryHandler) RequestAllQueries() {
	for queryType := 1; queryType <= 4; queryType++ {
		err := qh.RequestQueryResult(queryType)
		if err != nil {
			qh.log.Errorf("action: request_query | result: fail | query_type: %d | error: %v",
				queryType, err)
		}

		// Small delay between queries
		time.Sleep(500 * time.Millisecond)
	}
}
