package common

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"time"

	"github.com/op/go-logging"

	protocol "github.com/7574-sistemas-distribuidos/docker-compose-init/client/protocol"
)

// IsConnectionError checks if the error indicates a broken/lost connection
func IsConnectionError(err error) bool {
	if err == nil {
		return false
	}

	// Check for standard Go network/IO errors
	if errors.Is(err, net.ErrClosed) ||
		errors.Is(err, io.ErrClosedPipe) ||
		errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}

	// Check for network operation errors (connection refused, host unreachable, etc.)
	var netErr *net.OpError
	if errors.As(err, &netErr) {
		return true
	}

	// Check for syscall errors (EPIPE, ECONNRESET, etc.)
	var syscallErr *os.SyscallError
	return errors.As(err, &syscallErr)
}

// ConnectionManager handles TCP connection logic for the client
type ConnectionManager struct {
	serverAddress string
	clientID      string
	conn          net.Conn
	log           *logging.Logger
}

// NewConnectionManager creates a new connection manager
func NewConnectionManager(serverAddress, clientID string, logger *logging.Logger) *ConnectionManager {
	return &ConnectionManager{
		serverAddress: serverAddress,
		clientID:      clientID,
		log:           logger,
	}
}

// logConnectionAttempt registers the connection attempt
func (cm *ConnectionManager) logConnectionAttempt(attempt, maxRetries int) {
	cm.log.Infof(
		"action: connect | attempt: %d/%d | client_id: %v | server: %v",
		attempt,
		maxRetries,
		cm.clientID,
		cm.serverAddress,
	)
}

// handleConnectionSuccess handles a successful connection
func (cm *ConnectionManager) handleConnectionSuccess(conn net.Conn, attempt int) {
	cm.conn = conn
	cm.log.Infof(
		"action: connect | result: success | client_id: %v | attempt: %d",
		cm.clientID,
		attempt,
	)
}

// handleConnectionFailure handles a connection failure
func (cm *ConnectionManager) handleConnectionFailure(err error, attempt, maxRetries int) {
	cm.log.Warningf(
		"action: connect | result: fail | client_id: %v | attempt: %d/%d | error: %v",
		cm.clientID,
		attempt,
		maxRetries,
		err,
	)
}

// waitBeforeRetry waits before the next attempt if it's not the last one
func (cm *ConnectionManager) waitBeforeRetry(attempt, maxRetries int, retryInterval time.Duration) {
	if attempt < maxRetries {
		cm.log.Infof(
			"action: connect_retry | waiting: %v | client_id: %v | next_attempt: %d",
			retryInterval,
			cm.clientID,
			attempt+1,
		)
		time.Sleep(retryInterval)
	}
}

// logAllRetriesFailed logs when all attempts fail
func (cm *ConnectionManager) logAllRetriesFailed(maxRetries int, lastErr error) {
	cm.log.Criticalf(
		"action: connect | result: fail_all_retries | client_id: %v | attempts: %d | error: %v",
		cm.clientID,
		maxRetries,
		lastErr,
	)
}

// attemptConnection attempts a single TCP connection
func (cm *ConnectionManager) attemptConnection() (net.Conn, error) {
	return net.Dial("tcp", cm.serverAddress)
}

// Connect establishes the TCP connection with retry logic
func (cm *ConnectionManager) Connect(attempts int, retryInterval time.Duration) error {

	var lastErr error

	for attempt := 1; attempt <= attempts; attempt++ {
		cm.logConnectionAttempt(attempt, attempts)

		conn, err := cm.attemptConnection()
		if err == nil {
			cm.handleConnectionSuccess(conn, attempt)
			if err := cm.sendClientHello(conn); err != nil {
				cm.log.Errorf(
					"action: client_hello | result: fail | client_id: %s | error: %v",
					cm.clientID,
					err,
				)
				_ = conn.Close()
				cm.conn = nil
				lastErr = err
				break
			}
			return nil
		}

		lastErr = err
		cm.handleConnectionFailure(err, attempt, attempts)
		cm.waitBeforeRetry(attempt, attempts, retryInterval)
	}

	cm.logAllRetriesFailed(attempts, lastErr)
	return lastErr
}

// GetConnection returns the current connection
func (cm *ConnectionManager) GetConnection() net.Conn {
	return cm.conn
}

// Close safely closes the client connection and performs cleanup
func (cm *ConnectionManager) Close() {
	if cm.conn != nil {
		cm.log.Infof("action: close_connection | client_id: %v", cm.clientID)
		cm.conn.Close()
		cm.conn = nil
	}
}

func (cm *ConnectionManager) sendClientHello(conn net.Conn) error {
	if strings.TrimSpace(cm.clientID) == "" {
		return fmt.Errorf("missing client ID for handshake")
	}

	uuidBytes, err := uuidStringToBytes(cm.clientID)
	if err != nil {
		return err
	}

	frame := make([]byte, 1+4+16)
	frame[0] = protocol.OpCodeClientHello
	binary.LittleEndian.PutUint32(frame[1:], 16)
	copy(frame[5:], uuidBytes[:])

	if _, err := conn.Write(frame); err != nil {
		return fmt.Errorf("failed to send client hello: %w", err)
	}

	cm.log.Infof("action: client_hello | result: sent | client_id: %s", cm.clientID)
	return nil
}

func uuidStringToBytes(id string) ([16]byte, error) {
	var out [16]byte
	cleaned := strings.ReplaceAll(strings.TrimSpace(id), "-", "")
	if len(cleaned) != 32 {
		return out, fmt.Errorf("client id must be a UUID (32 hex chars), got length %d", len(cleaned))
	}

	decoded, err := hex.DecodeString(cleaned)
	if err != nil {
		return out, fmt.Errorf("invalid client id format: %w", err)
	}
	copy(out[:], decoded)
	return out, nil
}
