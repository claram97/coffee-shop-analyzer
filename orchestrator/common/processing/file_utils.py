"""
File utilities for logging and debugging message processing.
"""

import logging
from typing import Any


class MessageLogger:
    """Handles logging of messages to files for debugging purposes."""
    
    def __init__(self, original_file: str = "received_messages.txt", filtered_file: str = "filtered_messages.txt"):
        """Initialize message logger with file paths.
        
        Args:
            original_file: Path to file for original messages
            filtered_file: Path to file for filtered messages
        """
        self.original_file = original_file
        self.filtered_file = filtered_file
        
    def write_original_message(self, msg: Any, status_text: str):
        """Write original message to file for debugging."""
        try:
            with open(self.original_file, "a", encoding="utf-8") as f:
                f.write(f"=== Original Message - Opcode: {msg.opcode} - Amount: {msg.amount} - Batch: {msg.batch_number} - Status: {status_text} ===\n")
                for i, row in enumerate(msg.rows):
                    f.write(f"Row {i+1}: {row.__dict__}\n")
                f.write("\n")
        except Exception as e:
            logging.warning(f"action: write_original_message | result: fail | error: {e}")
            
    def write_filtered_message(self, filtered_batch: Any, status_text: str):
        """Write filtered message to file for debugging."""
        try:
            with open(self.filtered_file, "a", encoding="utf-8") as f:
                f.write(f"=== Filtered Message - Table: {filtered_batch.filtered_data['table_name']} - Original: {filtered_batch.filtered_data['original_row_count']} - Filtered: {filtered_batch.filtered_data['filtered_row_count']} - Batch: {filtered_batch.filtered_data['batch_number']} - Status: {status_text} ===\n")
                f.write(f"Query IDs: {filtered_batch.query_ids}\n")
                f.write(f"Table IDs: {filtered_batch.table_ids}\n")
                f.write(f"Total Shards: {filtered_batch.total_shards}, Shard Num: {filtered_batch.shard_num}\n")
                for i, row in enumerate(filtered_batch.filtered_data['rows']):
                    f.write(f"Row {i+1}: {row}\n")
                f.write("\n")
        except Exception as e:
            logging.warning(f"action: write_filtered_message | result: fail | error: {e}")
            
    def log_batch_processing_success(self, msg: Any, status_text: str):
        """Log successful batch processing."""
        logging.info(
            "action: batch_received | result: success | opcode: %d | amount: %d | batch_number: %d | status: %s",
            msg.opcode, msg.amount, msg.batch_number, status_text
        )
        
    def log_batch_filtered(self, filtered_batch: Any, status_text: str):
        """Log information about filtered batch."""
        logging.info(
            "action: batch_filtered | table: %s | original_count: %d | filtered_count: %d | batch_number: %d | status: %s",
            filtered_batch.filtered_data['table_name'],
            filtered_batch.filtered_data['original_row_count'],
            filtered_batch.filtered_data['filtered_row_count'],
            filtered_batch.filtered_data['batch_number'],
            status_text
        )


# Global logger instance for convenience
message_logger = MessageLogger()