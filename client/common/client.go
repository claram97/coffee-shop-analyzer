package common

import (
	"context"
	"os/signal"
	"syscall"
	"time"

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
	queryHandler      *QueryHandler
}

// NewClient constructs a Client with the provided configuration
func NewClient(config ClientConfig) *Client {
	connectionManager := network.NewConnectionManager(config.ServerAddress, config.ID, log)
	fileProcessor := processing.NewFileProcessor(config.ID, log)

	return &Client{
		config:            config,
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

	// Initialize the query handler now that we have a connection
	c.queryHandler = NewQueryHandler(conn, c.config.ID, log)

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
		
		// Wait a bit for the server to process finished message
		time.Sleep(1 * time.Second)
		
		// Request query results
		log.Info("action: query_phase | result: start | message: Requesting query results")
		c.queryHandler.RequestAllQueries()
		
		// Wait longer to receive query results
		time.Sleep(5 * time.Second)
	}

	// Wait for responses
	responseHandler.WaitForResponses(ctx, readDone)
}
