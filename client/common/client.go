package common

import (
	"context"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/op/go-logging"

	// Import the refactored modules with aliases to avoid name conflicts
	network "github.com/7574-sistemas-distribuidos/docker-compose-init/client/common/network"
	processing "github.com/7574-sistemas-distribuidos/docker-compose-init/client/common/processing"
)

var log = logging.MustGetLogger("log")

// ClientConfig holds the runtime configuration for a client instance.
type ClientConfig struct {
	ID              string
	ServerAddress   string
	TablesDirectory string
	BatchLimit      int32
	Attempts        int
	RetryInterval   time.Duration
}

// Client encapsulates the client behavior and orchestrates all components
type Client struct {
	config            ClientConfig
	connectionManager *network.ConnectionManager
	fileProcessor     *processing.FileProcessor
}

// NewClient constructs a Client with the provided configuration
func NewClient(config ClientConfig) *Client {
	cfg := config

	sanitizedID := strings.TrimSpace(cfg.ID)
	if sanitizedID == "" {
		generated := uuid.New().String()
		log.Infof("action: client_id | result: generated | value: %s", generated)
		sanitizedID = generated
	} else {
		if parsed, err := uuid.Parse(sanitizedID); err != nil {
			generated := uuid.New().String()
			log.Warningf(
				"action: client_id | result: invalid_config | provided: %s | fallback: %s | error: %v",
				cfg.ID,
				generated,
				err,
			)
			sanitizedID = generated
		} else {
			sanitizedID = parsed.String()
		}
	}

	cfg.ID = sanitizedID

	connectionManager := network.NewConnectionManager(cfg.ServerAddress, cfg.ID, log)
	fileProcessor := processing.NewFileProcessor(cfg.ID, log)

	return &Client{
		config:            cfg,
		connectionManager: connectionManager,
		fileProcessor:     fileProcessor,
		// queryHandler will be initialized after connection is established
	}
}

// SendBatch orchestrates the entire client process using the composed components
func (c *Client) SendBatch() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM)
	defer stop()

	// Establish connection
	if err := c.connectionManager.Connect(c.config.Attempts, c.config.RetryInterval*time.Second); err != nil {
		return
	}
	defer c.connectionManager.Close()

	conn := c.connectionManager.GetConnection()

	// Set up response handling
	readDone := make(chan struct{})
	responseHandler := network.NewResponseHandler(conn, c.config.ID, log)
	responseHandler.ReadResponses(readDone)

	// Process all tables with a factory function to create batch processors
	processorFactory := func(handler processing.TableRowHandler, opCode byte) *processing.BatchProcessor {
		return processing.NewBatchProcessor(conn, handler, opCode, c.config.BatchLimit, c.config.ID, log)
	}

	lastErr := c.fileProcessor.ProcessAllTables(ctx, processorFactory, c.config.TablesDirectory)

	// Send finished message if no errors
	if lastErr == nil {
		finishedSender := network.NewFinishedMessageSender(conn, c.config.ID, log)
		finishedSender.SendFinished()
	}

	// Wait for responses
	responseHandler.WaitForResponses(ctx, readDone)
}
