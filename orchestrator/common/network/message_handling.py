"""
Message handling and response utilities for network communication.
"""

import logging
from typing import Dict, Any

from ..protocol import Opcodes, BetsRecvSuccess, BetsRecvFail


class MessageHandler:
    """Handles different types of protocol messages."""
    
    def __init__(self):
        """Initialize message handler."""
        self.message_processors = {}
        
    def register_processor(self, opcode: int, processor_func):
        """Register a processor function for a specific opcode.
        
        Args:
            opcode: Message opcode to handle
            processor_func: Function to process messages with this opcode
        """
        self.message_processors[opcode] = processor_func
        
    def get_status_text(self, batch_status: int) -> str:
        """Convert batch status to readable text."""
        status_names = {0: "Continue", 1: "EOF", 2: "Cancel"}
        return status_names.get(batch_status, f"Unknown({batch_status})")
        
    def is_data_message(self, msg) -> bool:
        """Determine if message contains table data."""
        return (msg.opcode != Opcodes.FINISHED and 
                msg.opcode != Opcodes.BETS_RECV_SUCCESS and 
                msg.opcode != Opcodes.BETS_RECV_FAIL)
                
    def handle_message(self, msg, client_sock) -> bool:
        """Handle received message using registered processors.
        
        Args:
            msg: Received message object
            client_sock: Client socket for responses
            
        Returns:
            True to continue connection, False to close
        """
        # Check if we have a specific processor for this opcode
        if msg.opcode in self.message_processors:
            return self.message_processors[msg.opcode](msg, client_sock)
            
        # Default handling for common message types
        if self.is_data_message(msg):
            return self._handle_data_message(msg, client_sock)
        elif msg.opcode == Opcodes.FINISHED:
            return False  # Close connection
            
        # Unknown message type - continue connection but log warning
        logging.warning(f"action: handle_message | result: unknown_opcode | opcode: {msg.opcode}")
        return True
        
    def _handle_data_message(self, msg, client_sock) -> bool:
        """Default handler for data messages.
        
        This is a fallback - normally you'd register specific processors.
        """
        try:
            status_text = self.get_status_text(getattr(msg, 'batch_status', 0))
            logging.info(
                "action: data_message_received | opcode: %d | amount: %d | status: %s",
                msg.opcode, getattr(msg, 'amount', 0), status_text
            )
            self.send_success_response(client_sock)
            return True
        except Exception as e:
            logging.error(f"action: handle_data_message | result: fail | error: {e}")
            self.send_failure_response(client_sock)
            return True
            
    def send_success_response(self, client_sock):
        """Send success response to client."""
        BetsRecvSuccess().write_to(client_sock)
        
    def send_failure_response(self, client_sock):
        """Send failure response to client."""
        BetsRecvFail().write_to(client_sock)
        
    def log_batch_preview(self, msg, status_text: str):
        """Log preview of batch data for debugging."""
        try:
            if hasattr(msg, 'rows') and msg.rows and len(msg.rows) > 0:
                sample_rows = msg.rows[:2]  # First 2 rows as sample
                all_keys = set()
                for row in sample_rows:
                    all_keys.update(row.__dict__.keys())
                
                sample_data = [row.__dict__ for row in sample_rows]
                
                logging.debug(
                    "action: batch_preview | batch_number: %d | status: %s | opcode: %d | keys: %s | sample_count: %d | sample: %s",
                    getattr(msg, 'batch_number', 0), status_text, msg.opcode, sorted(list(all_keys)), len(sample_rows), sample_data
                )
        except Exception:
            logging.debug("action: batch_preview | batch_number: %d | result: skip", getattr(msg, 'batch_number', 0))


class ResponseHandler:
    """Handles sending responses back to clients."""
    
    @staticmethod
    def send_success(client_sock):
        """Send BETS_RECV_SUCCESS response."""
        BetsRecvSuccess().write_to(client_sock)
        
    @staticmethod
    def send_failure(client_sock):
        """Send BETS_RECV_FAIL response."""
        BetsRecvFail().write_to(client_sock)
        
    @staticmethod  
    def handle_processing_error(msg, client_sock, error: Exception) -> bool:
        """Handle errors during message processing.
        
        Args:
            msg: Message that caused the error
            client_sock: Client socket
            error: Exception that occurred
            
        Returns:
            True to continue connection
        """
        ResponseHandler.send_failure(client_sock)
        logging.error(
            "action: message_processing | result: fail | batch_number: %d | amount: %d | error: %s", 
            getattr(msg, 'batch_number', 0), getattr(msg, 'amount', 0), str(error)
        )
        return True