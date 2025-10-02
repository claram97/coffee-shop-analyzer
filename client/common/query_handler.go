package common

import (
	"fmt"
	"net"
	"time"

	"github.com/op/go-logging"
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

// RequestQueryResult sends a query request to the server
// Query types are 1-4, corresponding to Q1-Q4
func (qh *QueryHandler) RequestQueryResult(queryType int) error {
	if queryType < 1 || queryType > 4 {
		return fmt.Errorf("invalid query type: %d (must be 1-4)", queryType)
	}

	qh.log.Infof("action: request_query | result: sending | query_type: %d", queryType)

	// Create a simple message with just the opcode matching the query type
	// The opcode itself serves as both message type and query ID
	opcode := byte(queryType)

	// Write the opcode byte
	if _, err := qh.conn.Write([]byte{opcode}); err != nil {
		return fmt.Errorf("failed to send query request: %v", err)
	}

	// Write a zero-length body
	lengthBytes := []byte{0, 0, 0, 0} // 4 bytes for int32 = 0
	if _, err := qh.conn.Write(lengthBytes); err != nil {
		return fmt.Errorf("failed to send query body length: %v", err)
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
