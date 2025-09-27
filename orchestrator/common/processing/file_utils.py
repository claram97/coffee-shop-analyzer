"""
Provides file and logging utilities for debugging the message processing pipeline
by writing the contents of messages to disk.
"""

import logging
from typing import Any


class MessageLogger:
    """
    Provides a utility for writing the detailed contents of network messages
    to disk, which is useful for debugging data processing and transformation
    pipelines.
    """

    def __init__(self, original_file: str = "received_messages.txt", filtered_file: str = "filtered_messages.txt"):
        """
        Initializes the MessageLogger with specified output file paths.

        Args:
            original_file: The file path for logging raw, unprocessed messages.
            filtered_file: The file path for logging processed, filtered messages.
        """
        self.original_file = original_file
        self.filtered_file = filtered_file

    def write_original_message(self, msg: Any, status_text: str):
        """
        Appends the contents of a raw, unprocessed message to a log file.

        Each row of the message's data is written out for detailed inspection.
        This method is safe and will log a warning on failure without crashing.

        Args:
            msg: The original message object, expected to have attributes like
                 `opcode`, `amount`, `batch_number`, and `rows`.
            status_text: A human-readable string describing the batch status.
        """
        try:
            with open(self.original_file, "a", encoding="utf-8") as f:
                f.write(f"=== Original Message - Opcode: {msg.opcode} - Amount: {msg.amount} - Batch: {msg.batch_number} - Status: {status_text} ===\n")
                for i, row in enumerate(msg.rows):
                    f.write(f"Row {i+1}: {row.__dict__}\n")
                f.write("\n")
        except Exception as e:
            logging.warning(f"action: write_original_message | result: fail | error: {e}")

    def write_filtered_message(self, filtered_batch: Any, status_text: str):
        """
        Appends the contents of a processed and filtered `DataBatch` to a log file.

        This includes both the filtered data rows and the wrapper's metadata.
        This method is safe and will log a warning on failure without crashing.

        Args:
            filtered_batch: The processed `DataBatch` object. It is expected to
                            have a `filtered_data` attribute.
            status_text: A human-readable string describing the batch status.
        """
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
        """
        Logs a high-level summary to the standard logger indicating that a
        raw batch has been successfully received.

        Args:
            msg: The original message object.
            status_text: A human-readable string describing the batch status.
        """
        logging.info(
            "action: batch_received | result: success | opcode: %d | amount: %d | batch_number: %d | status: %s",
            msg.opcode, msg.amount, msg.batch_number, status_text
        )

    def log_batch_filtered(self, filtered_batch: Any, status_text: str):
        """
        Logs a summary to the standard logger detailing the results of the
        filtering process, including original and filtered row counts.

        Args:
            filtered_batch: The processed `DataBatch` object.
            status_text: A human-readable string describing the batch status.
        """
        logging.info(
            "action: batch_filtered | table: %s | original_count: %d | filtered_count: %d | batch_number: %d | status: %s",
            filtered_batch.filtered_data['table_name'],
            filtered_batch.filtered_data['original_row_count'],
            filtered_batch.filtered_data['filtered_row_count'],
            filtered_batch.filtered_data['batch_number'],
            status_text
        )


# A global singleton instance of the logger for convenient access throughout the application.
message_logger = MessageLogger()