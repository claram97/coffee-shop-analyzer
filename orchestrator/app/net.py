"""
Refactored Orchestrator using modular architecture.
"""

import logging
import os
from common.network import ServerManager, MessageHandler, ResponseHandler
from common.processing import create_filtered_data_batch, message_logger
from protocol.constants import Opcodes
from middleware.middleware_client import MessageMiddlewareQueue
from app.results_consumer import ResultsConsumer

class Orchestrator:
    """Orchestrator using modular network and processing components."""
    
    def __init__(self, port: int, listen_backlog: int):
        """Initialize modular orchestrator.
        
        Args:
            port: Port to listen on
            listen_backlog: Maximum pending connections
        """
        self.message_handler = MessageHandler()
        
        # Setup middleware connections
        rabbitmq_host = os.getenv("RABBITMQ_HOST", "rabbitmq")
        self._filter_router_queue = MessageMiddlewareQueue(rabbitmq_host, "filter_router_queue")
        
        # Setup the results consumer
        results_queue = os.getenv("RESULTS_QUEUE", "orchestrator_results_queue")
        self.results_consumer = ResultsConsumer(results_queue, rabbitmq_host)
        
        # Setup network server
        self.server_manager = ServerManager(port, listen_backlog, self._handle_message)
        
        # Register message processors
        self._setup_message_processors()
        
    def _setup_message_processors(self):
        """Setup message processors for different message types."""
        # Register processor for data messages
        for opcode in [Opcodes.NEW_MENU_ITEMS, Opcodes.NEW_STORES, 
                      Opcodes.NEW_TRANSACTION_ITEMS, Opcodes.NEW_TRANSACTION, 
                      Opcodes.NEW_USERS]:
            self.message_handler.register_processor(opcode, self._process_data_message)
            
        # Register processor for EOF messages
        self.message_handler.register_processor(Opcodes.EOF, self._process_eof_message)
        
        # Register processor for query messages
        for opcode in range(1, 5):  # Query types 1-4
            self.message_handler.register_processor(opcode, self._process_query_request)
            
        # FINISHED message is handled by default (returns False to close connection)
        
    def _handle_message(self, msg, client_sock) -> bool:
        """Main message handler that delegates to specific processors.
        
        Args:
            msg: Received message object
            client_sock: Client socket for responses
            
        Returns:
            True to continue connection, False to close
        """
        return self.message_handler.handle_message(msg, client_sock)
        
    def _process_data_message(self, msg, client_sock) -> bool:
        """Process data messages with filtering and logging.
        
        Args:
            msg: Data message to process
            client_sock: Client socket for responses
            
        Returns:
            True to continue connection
        """
        try:
            status_text = self.message_handler.get_status_text(getattr(msg, 'batch_status', 0))
            
            # 1. Write original received message
            # Not performant at all, but useful for debugging purposes
            # message_logger.write_original_message(msg, status_text)
            
            # 2. Process filtered batch
            self._process_filtered_batch(msg, status_text)
            
            # 3. Success logging
            message_logger.log_batch_processing_success(msg, status_text)
            
            # 4. Additional logging with data preview
            self.message_handler.log_batch_preview(msg, status_text)
            
            ResponseHandler.send_success(client_sock)
            return True
            
        except Exception as e:
            return ResponseHandler.handle_processing_error(msg, client_sock, e)
    
    def register_client_for_query(self, query_id, client_sock):
        """Register a client to receive results for a specific query.
        
        Args:
            query_id: The ID of the query to get results for
            client_sock: The client socket to send results to
        """
        self.results_consumer.register_client_for_query(query_id, client_sock)
        logging.info(f"action: client_registered_for_query | query_id: {query_id}")
        
    def _process_query_request(self, msg, client_sock) -> bool:
        """Process a query request message from a client.
        
        This registers the client to receive the results of the specified query.
        
        Args:
            msg: Query request message to process
            client_sock: Client socket for responses
            
        Returns:
            True to continue connection
        """
        try:
            # Extract query ID from the opcode (1-4 map to QueryType.Q1-Q4)
            query_id = str(msg.opcode)
            
            # Register the client to receive results for this query
            self.register_client_for_query(query_id, client_sock)
            
            # Send a success response to the client
            ResponseHandler.send_success(client_sock)
            
            logging.info(f"action: query_request_received | query_id: {query_id}")
            return True
            
        except Exception as e:
            logging.error(f"action: query_request_processing | result: fail | error: {e}")
            return ResponseHandler.handle_processing_error(msg, client_sock, e)
        
    def _process_filtered_batch(self, msg, status_text: str):
        """Process and log filtered batch."""
        try:
            filtered_batch = create_filtered_data_batch(msg)
            
            # Generate bytes and log information
            batch_bytes = filtered_batch.to_bytes()
           
            self._filter_router_queue.send(batch_bytes)
            
            # Write filtered message to file
            # Useful for debugging large batches, but not performant at all
            # message_logger.write_filtered_message(filtered_batch, status_text)
            
            # Log filtered batch information
            message_logger.log_batch_filtered(filtered_batch, status_text)
            
        except Exception as filter_error:
            logging.warning(
                "action: batch_filter | result: fail | batch_number: %d | opcode: %d | error: %s",
                getattr(msg, 'batch_number', 0), msg.opcode, str(filter_error)
            )
    
    def _process_eof_message(self, msg, client_sock) -> bool:
        """Process EOF messages by sending them to the filter router queue.
        
        Args:
            msg: EOF message to process
            client_sock: Client socket for responses
            
        Returns:
            True to continue connection
        """
        try:
            table_type = msg.get_table_type()
            
            # Log EOF reception
            logging.info(
                "action: eof_received | result: success | table_type: %s | batch_number: %d",
                table_type, getattr(msg, 'batch_number', 0)
            )
            
            # Serialize EOF message to bytes
            message_bytes = msg.to_bytes()
            
            # Send to filter router queue
            self._filter_router_queue.send(message_bytes)
            
            # Log successful forwarding
            logging.info(
                "action: eof_forwarded | result: success | table_type: %s | bytes_length: %d",
                table_type, len(message_bytes)
            )
            
            ResponseHandler.send_success(client_sock)
            return True
            
        except Exception as e:
            logging.error(
                "action: eof_processing | result: fail | table_type: %s | batch_number: %d | error: %s",
                getattr(msg, 'table_type', 'unknown'), getattr(msg, 'batch_number', 0), str(e)
            )
            return ResponseHandler.handle_processing_error(msg, client_sock, e)
            
    def run(self):
        """Start the orchestrator server and results consumer."""
        # Start the results consumer first
        self.results_consumer.start()
        
        # Then start the server
        try:
            self.server_manager.run()
        finally:
            # Ensure we shut down the results consumer
            self.results_consumer.stop()


class MockFilterRouterQueue:
    """Mock implementation of filter router queue for development/testing."""
    
    def send(self, message_bytes: bytes):
        """Simulate sending message to queue.
        
        Args:
            message_bytes: Serialized message bytes to send
        """
        logging.info(
            "action: mock_queue_send | result: success | message_size: %d bytes",
            len(message_bytes)
        )
        # In real implementation, this would send to RabbitMQ, Kafka, etc.
        # For now, just log that we received the message