"""
Provides utility classes for handling incoming messages and sending
standardized responses within a network communication protocol.
"""

import logging
from typing import Dict, Any

from protocol import Opcodes, BatchRecvSuccess, BatchRecvFail


class MessageHandler:
    """
    Handles the dispatching of incoming messages to registered processor functions
    based on their opcode.
    """

    def __init__(self):
        """Initializes the MessageHandler, setting up a registry for message processors."""
        self.message_processors: Dict[int, Any] = {}

    def register_processor(self, opcode: int, processor_func: Any):
        """
        Registers a processor function to handle a specific message opcode.

        Args:
            opcode: The integer opcode of the message to handle.
            processor_func: The function that will be called to process messages
                            with the given opcode.
        """
        self.message_processors[opcode] = processor_func

    def get_status_text(self, batch_status: int) -> str:
        """
        Converts a numeric batch status code into a human-readable string.

        Args:
            batch_status: The numeric status code (e.g., 0 for Continue, 1 for EOF).

        Returns:
            A string representation of the status (e.g., "Continue", "EOF").
        """
        status_names = {0: "Continue", 1: "EOF", 2: "Cancel"}
        return status_names.get(batch_status, f"Unknown({batch_status})")

    def is_data_message(self, msg: Any) -> bool:
        """
        Determines if a given message is a data-carrying message.

        This is identified by checking that the opcode is not a control signal
        (like FINISHED) or a response message.

        Args:
            msg: The message object to check.

        Returns:
            True if the message is considered a data message, False otherwise.
        """
        return (msg.opcode != Opcodes.FINISHED and
                msg.opcode != Opcodes.BATCH_RECV_SUCCESS and
                msg.opcode != Opcodes.BATCH_RECV_FAIL)

    def handle_message(self, msg: Any, client_sock: Any) -> bool:
        """
        Dispatches a received message to the appropriate processor.

        It first checks for a specifically registered processor for the message's
        opcode. If none is found, it uses default handlers for common message types.

        Args:
            msg: The received message object.
            client_sock: The client's socket object, used for sending responses.

        Returns:
            True to keep the connection open, or False to close it.
        """
        # Check if a specific processor is registered for this opcode
        if msg.opcode in self.message_processors:
            return self.message_processors[msg.opcode](msg, client_sock)

        # Apply default handling for common message types
        if self.is_data_message(msg):
            return self._handle_data_message(msg, client_sock)
        elif msg.opcode == Opcodes.FINISHED:
            return False  # Signal to close the connection

        # Log a warning for unhandled message types but keep the connection open
        logging.warning(f"action: handle_message | result: unknown_opcode | opcode: {msg.opcode}")
        return True

    def _handle_data_message(self, msg: Any, client_sock: Any) -> bool:
        """
        Provides a default handling mechanism for data messages.

        This method acts as a fallback if no specific processor is registered. It logs
        the reception of the data and sends a success response.

        Args:
            msg: The data message object.
            client_sock: The client's socket for responding.

        Returns:
            Always returns True to keep the connection open.
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

    def send_success_response(self, client_sock: Any):
        """Sends a predefined success (BATCH_RECV_SUCCESS) response to the client."""
        BatchRecvSuccess().write_to(client_sock)

    def send_failure_response(self, client_sock: Any):
        """Sends a predefined failure (BATCH_RECV_FAIL) response to the client."""
        BatchRecvFail().write_to(client_sock)

    def log_batch_preview(self, msg: Any, status_text: str):
        """
        Logs a summarized preview of a data batch for debugging purposes.

        It includes metadata like batch number, status, and a sample of the
        first two rows to aid in troubleshooting.

        Args:
            msg: The message containing the batch data.
            status_text: The human-readable status of the batch.
        """
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
    """Provides a collection of static utility methods for sending standardized responses."""

    @staticmethod
    def send_success(client_sock: Any):
        """Sends a BATCH_RECV_SUCCESS response to the specified client."""
        BatchRecvSuccess().write_to(client_sock)

    @staticmethod
    def send_failure(client_sock: Any):
        """Sends a BATCH_RECV_FAIL response to the specified client."""
        BatchRecvFail().write_to(client_sock)

    @staticmethod
    def handle_processing_error(msg: Any, client_sock: Any, error: Exception) -> bool:
        """
        Centralized handler for logging an error and notifying the client.

        This ensures a failure response is sent and the error is logged with
        relevant details from the message that caused the issue.

        Args:
            msg: The message that caused the processing error.
            client_sock: The client's socket to send the failure response to.
            error: The exception that was caught.

        Returns:
            Always returns True to indicate the connection should remain open.
        """
        ResponseHandler.send_failure(client_sock)
        logging.error(
            "action: message_processing | result: fail | batch_number: %d | amount: %d | error: %s",
            getattr(msg, 'batch_number', 0), getattr(msg, 'amount', 0), str(error)
        )
        return True