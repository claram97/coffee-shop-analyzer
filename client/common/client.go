package common

import (
	"context"
	"os/signal"
	"syscall"

	"github.com/op/go-logging"

	// Import the refactored modules with aliases to avoid name conflicts
	network "github.com/7574-sistemas-distribuidos/docker-compose-init/client/common/network"
	processing "github.com/7574-sistemas-distribuidos/docker-compose-init/client/common/processing"
)

var log = logging.MustGetLogger("log")

// ClientConfig holds the runtime configuration for a client instance.
type ClientConfig struct {
	ID            string
	ServerAddress string
	BetsFilePath  string
	BatchLimit    int32
}

// Client encapsulates the client behavior and orchestrates all components
type Client struct {
	config            ClientConfig
	connectionManager *network.ConnectionManager
	fileProcessor     *processing.FileProcessor
}

// NewClient constructs a Client with the provided configuration
func NewClient(config ClientConfig) *Client {
	connectionManager := network.NewConnectionManager(config.ServerAddress, config.ID, log)
	fileProcessor := processing.NewFileProcessor(config.ID, log)

	return &Client{
		config:            config,
		connectionManager: connectionManager,
		fileProcessor:     fileProcessor,
	}
}

// SendBatch orchestrates the entire client process using the composed components
func (c *Client) SendBatch() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM)
	defer stop()

	// Establish connection
	if err := c.connectionManager.Connect(); err != nil {
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

	lastErr := c.fileProcessor.ProcessAllTables(ctx, processorFactory)

	// Send finished message if no errors
	if lastErr == nil {
		finishedSender := network.NewFinishedMessageSender(conn, c.config.ID, log)
		finishedSender.SendFinished()
	}

	// Wait for responses
	responseHandler.WaitForResponses(ctx, readDone)
}
