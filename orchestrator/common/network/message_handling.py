"""
Provides utility classes for handling incoming messages and sending
standardized responses within a network communication protocol.
"""

import logging
import os
from typing import Any, Dict, Optional

from common.processing import create_filtered_data_batch, create_filtered_data_batch_protocol2

from middleware.middleware_client import MessageMiddlewareExchange
from protocol.constants import Opcodes
from protocol.messages import BatchRecvFail, BatchRecvSuccess


class MessageHandler:
    """
    Handles the dispatching of incoming messages to registered processor functions
    based on their opcode.
    """

    def __init__(self):
        """Initializes the MessageHandler, setting up publishers and registry."""
        self.message_processors: Dict[int, Any] = {}

        # ---- Publisher hacia el FILTER ROUTER (exchange) ----
        rabbitmq_host = os.getenv("RABBITMQ_HOST", "rabbitmq")
        fr_exchange = os.getenv("ORCH_TO_FR_EXCHANGE", "fr.ex")
        rk_fmt = os.getenv("ORCH_TO_FR_RK_FMT", "fr.{pid:02d}")
        # Forzamos PID 0 para livianas
        rk_fr0 = rk_fmt.format(pid=0)

        # Publisher al exchange del Filter Router, con rk del router 0
        self._fr_pub_ex = MessageMiddlewareExchange(
            host=rabbitmq_host,
            exchange_name=fr_exchange,
            route_keys=[rk_fr0],
        )
        self._rk_fr0 = rk_fr0  # útil para logs

    def register_processor(self, opcode: int, processor_func: Any):
        self.message_processors[opcode] = processor_func

    def get_status_text(self, batch_status: int) -> str:
        status_names = {0: "Continue", 1: "EOF", 2: "Cancel"}
        return status_names.get(batch_status, f"Unknown({batch_status})")

    def is_data_message(self, msg: Any) -> bool:
        return (
            msg.opcode != Opcodes.FINISHED
            and msg.opcode != Opcodes.BATCH_RECV_SUCCESS
            and msg.opcode != Opcodes.BATCH_RECV_FAIL
        )

    def old_handle_message(self, msg: Any, client_sock: Any, client_id: Optional[str] = None) -> bool:
        # 1) Fast-path: tablas livianas → Filter Router 0 por exchange
        effective_client_id = client_id or getattr(msg, "client_id", None)
        if msg.opcode in (Opcodes.NEW_MENU_ITEMS, Opcodes.NEW_STORES):
            try:
                if not effective_client_id:
                    raise RuntimeError("client_id required for light data path")

                db = create_filtered_data_batch(msg, effective_client_id)
                raw = db.to_bytes()
                tname = (
                    "menu_items" if msg.opcode == Opcodes.NEW_MENU_ITEMS else "stores"
                )
                logging.info(
                    "action=orch_forward_light table=%s bn=%d bytes=%d rk=%s",
                    tname,
                    getattr(db, "batch_number", 0),
                    len(raw),
                    self._rk_fr0,
                )
                self._fr_pub_ex.send(raw)
                self.send_success_response(client_sock)
            except Exception:
                logging.exception("forward_light_failed")
                self.send_failure_response(client_sock)
            return True

        # 2) Camino normal (procesadores registrados)
        if msg.opcode in self.message_processors:
            logging.debug("mensaje con opcode %d está en message_processors", msg.opcode)
            return self.message_processors[msg.opcode](msg, client_sock)

        # 3) Fallback por defecto (logs + ACK)
        if self.is_data_message(msg):
            return self._handle_data_message(msg, client_sock)
        elif msg.opcode == Opcodes.FINISHED:
            logging.info("action: client_finished | result: received")
            return True

        logging.warning(
            "action=handle_message result=unknown_opcode opcode=%d", msg.opcode
        )
        return True

    def handle_message(self, msg: Any, client_sock: Any, client_id: Optional[str] = None) -> bool:
        """
        Protocol2-aware variant of handle_message.

        Behaves like `handle_message` but when forwarding light tables it will
        call `create_filtered_data_batch_protocol2` (which returns a protobuf
        `Envelope`), serialize it with `SerializeToString()` and send the
        resulting bytes to the Filter Router exchange. For other message paths
        the behavior mirrors the legacy `handle_message` (processors and
        fallback to `_handle_data_message`).
        """
        effective_client_id = client_id or getattr(msg, "client_id", None)

        # 1) Fast-path: tablas livianas → Filter Router 0 por exchange (protocol2)
        if msg.opcode in (Opcodes.NEW_MENU_ITEMS, Opcodes.NEW_STORES):
            try:
                if not effective_client_id:
                    raise RuntimeError("client_id required for light data path")

                env = create_filtered_data_batch_protocol2(msg, effective_client_id)
                # env is a protobuf Envelope; serialize to bytes for transport
                raw = env.SerializeToString()
                tname = (
                    "menu_items" if msg.opcode == Opcodes.NEW_MENU_ITEMS else "stores"
                )
                # Attempt to log a batch_number if present in the payload
                bn = 0
                try:
                    bn = getattr(env, "data_batch", env).payload.batch_number
                except Exception:
                    # keep bn as 0 if structure differs
                    bn = 0

                logging.info(
                    "action=orch_forward_light_proto table=%s bn=%d bytes=%d rk=%s",
                    tname,
                    bn,
                    len(raw),
                    self._rk_fr0,
                )
                self._fr_pub_ex.send(raw)
                self.send_success_response(client_sock)
            except Exception:
                logging.exception("forward_light_proto_failed")
                self.send_failure_response(client_sock)
            return True

        # 2) Camino normal (procesadores registrados)
        if msg.opcode in self.message_processors:
            logging.debug("mensaje con opcode %d está en message_processors", msg.opcode)
            return self.message_processors[msg.opcode](msg, client_sock)

        # 3) Fallback por defecto (logs + ACK)
        if self.is_data_message(msg):
            return self._handle_data_message(msg, client_sock)
        elif msg.opcode == Opcodes.FINISHED:
            logging.info("action: client_finished | result: received")
            return True

        logging.warning(
            "action=handle_message_protocol2 result=unknown_opcode opcode=%d", msg.opcode
        )
        return True

    def _handle_data_message(self, msg: Any, client_sock: Any) -> bool:
        try:
            status_text = self.get_status_text(getattr(msg, "batch_status", 0))
            logging.info(
                "action=data_message_received opcode=%d amount=%d status=%s",
                msg.opcode,
                getattr(msg, "amount", 0),
                status_text,
            )
            self.send_success_response(client_sock)
            return True
        except Exception as e:
            logging.error("action=handle_data_message result=fail error=%s", e)
            self.send_failure_response(client_sock)
            return True

    def send_success_response(self, client_sock: Any):
        BatchRecvSuccess().write_to(client_sock)

    def send_failure_response(self, client_sock: Any):
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
            if hasattr(msg, "rows") and msg.rows and len(msg.rows) > 0:
                sample_rows = msg.rows[:2]  # First 2 rows as sample
                all_keys = set()
                for row in sample_rows:
                    all_keys.update(row.__dict__.keys())

                sample_data = [row.__dict__ for row in sample_rows]

                logging.debug(
                    "action: batch_preview | batch_number: %d | status: %s | opcode: %d | keys: %s | sample_count: %d | sample: %s",
                    getattr(msg, "batch_number", 0),
                    status_text,
                    msg.opcode,
                    sorted(list(all_keys)),
                    len(sample_rows),
                    sample_data,
                )
        except Exception:
            logging.debug(
                "action: batch_preview | batch_number: %d | result: skip",
                getattr(msg, "batch_number", 0),
            )


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
            getattr(msg, "batch_number", 0),
            getattr(msg, "amount", 0),
            str(error),
        )
        return True
