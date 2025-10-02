"""
Refactored orchestrator using modular architecture.
"""

import logging
from common.network import ServerManager, MessageHandler, ResponseHandler
from common.processing import create_filtered_data_batch, message_logger
from protocol.constants import Opcodes
from middleware import MessageMiddlewareQueue

class Orchestrator:
    """Orchestrator using modular network and processing components."""
    
    def __init__(self, port: int, listen_backlog: int):
        """Initialize modular orchestrator.
        
        Args:
            port: Port to listen on
            listen_backlog: Maximum pending connections
        """
        self.message_handler = MessageHandler()
        self._filter_router_queue = MessageMiddlewareQueue("rabbitmq", "filter.router.in")
        self.server_manager = ServerManager(port, listen_backlog, self._handle_message)
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
        """Start the orchestrator server."""
        self.server_manager.run()


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