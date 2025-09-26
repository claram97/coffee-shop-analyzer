"""
Refactored orchestrator using modular architecture.
"""

import logging
from common.network import ServerManager, MessageHandler, ResponseHandler
from common.processing import create_filtered_data_batch, message_logger
from common.protocol import Opcodes


class ModularOrchestrator:
    """Orchestrator using modular network and processing components."""
    
    def __init__(self, port: int, listen_backlog: int):
        """Initialize modular orchestrator.
        
        Args:
            port: Port to listen on
            listen_backlog: Maximum pending connections
        """
        self.message_handler = MessageHandler()
        self.server_manager = ServerManager(port, listen_backlog, self._handle_message)
        self._setup_message_processors()
        
    def _setup_message_processors(self):
        """Setup message processors for different message types."""
        # Register processor for data messages
        for opcode in [Opcodes.NEW_MENU_ITEMS, Opcodes.NEW_STORES, 
                      Opcodes.NEW_TRANSACTION_ITEMS, Opcodes.NEW_TRANSACTION, 
                      Opcodes.NEW_USERS]:
            self.message_handler.register_processor(opcode, self._process_data_message)
            
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
            message_logger.write_original_message(msg, status_text)
            
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
           
            # Future: Send to message queue
            # self._filter_router_queue.send(batch_bytes)
            
            # Write filtered message to file
            message_logger.write_filtered_message(filtered_batch, status_text)
            
            # Log filtered batch information
            message_logger.log_batch_filtered(filtered_batch, status_text)
            
        except Exception as filter_error:
            logging.warning(
                "action: batch_filter | result: fail | batch_number: %d | opcode: %d | error: %s",
                getattr(msg, 'batch_number', 0), msg.opcode, str(filter_error)
            )
            
    def run(self):
        """Start the orchestrator server."""
        self.server_manager.run()


# Backward compatibility - keep the old Orchestrator class working
class Orchestrator(ModularOrchestrator):
    """Backward compatible orchestrator class."""
    pass