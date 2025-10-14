# orchestrator/app/worker.py
import logging
import os
from typing import Optional

from common.processing import create_filtered_data_batch, message_logger
from middleware.middleware_client import MessageMiddlewareExchange
from protocol.constants import Opcodes
from protocol.dispatcher import deserialize_message_from_bytes


class ProcessingWorker:
    """
    A worker process that consumes raw messages from a queue, processes them,
    and forwards them to the filter router exchange.
    """

    def __init__(self):
        """Initializes the worker, setting up RabbitMQ configuration."""
        rabbitmq_host = os.getenv("RABBITMQ_HOST", "rabbitmq")
        self._fr_exchange = os.getenv("ORCH_TO_FR_EXCHANGE", "orch.to.fr")
        self._fr_rk_fmt = os.getenv("ORCH_TO_FR_RK_FMT", "fr.{pid:02d}")
        self._num_routers = max(1, int(os.getenv("FILTER_ROUTER_COUNT", "1")))
        self._host = rabbitmq_host
        self._publishers: dict[str, MessageMiddlewareExchange] = {}

    def _publisher_for_rk(self, rk: str) -> MessageMiddlewareExchange:
        """Creates or retrieves a publisher for a specific routing key."""
        pub = self._publishers.get(rk)
        if pub is None:
            logging.info(
                "action: worker_create_publisher | exchange: %s | rk: %s | host: %s",
                self._fr_exchange,
                rk,
                self._host,
            )
            pub = MessageMiddlewareExchange(
                host=self._host,
                exchange_name=self._fr_exchange,
                route_keys=[rk],
            )
            self._publishers[rk] = pub
        return pub

    def _send_to_filter_router_exchange(self, raw: bytes, batch_number: Optional[int]):
        """Sends a message to a specific filter router instance."""
        pid = 0 if batch_number is None else int(batch_number) % self._num_routers
        rk = self._fr_rk_fmt.format(pid=pid)
        self._publisher_for_rk(rk).send(raw)

    def _broadcast_eof_to_all(self, raw: bytes):
        """Broadcasts an EOF message to all filter router instances."""
        for pid in range(self._num_routers):
            rk = self._fr_rk_fmt.format(pid=pid)
            self._publisher_for_rk(rk).send(raw)

    def run(self, task_queue):
        """The main loop for the worker process."""
        logging.info("action: worker_start | result: success | pid: %s", os.getpid())
        while True:
            try:
                task = task_queue.get()

                if task is None:
                    logging.info("action: worker_shutdown | pid: %s", os.getpid())
                    break

                raw_msg_bytes, client_id = task
                
                msg = deserialize_message_from_bytes(raw_msg_bytes)
                if not msg:
                    continue
                
                setattr(msg, "client_id", client_id)
                
                if msg.opcode == Opcodes.EOF:
                    self._process_eof_message(msg)
                else:
                    self._process_data_message(msg)

            except Exception as e:
                logging.error("action: worker_processing | result: fail | error: %s", e, exc_info=True)

    def _process_data_message(self, msg):
        """Processes a standard data message."""
        try:
            # This logic is moved directly from the old Orchestrator
            filtered_batch = create_filtered_data_batch(msg, msg.client_id)
            batch_bytes = filtered_batch.to_bytes()
            bn = int(getattr(filtered_batch, "batch_number", 0) or 0)
            self._send_to_filter_router_exchange(batch_bytes, bn)

            # Optional: Add logging if needed
            status_text = {0: "Continue", 1: "EOF", 2: "Cancel"}.get(
                getattr(msg, "batch_status", 0), "Unknown"
            )
            message_logger.log_batch_processing_success(msg, status_text)

        except Exception as e:
            logging.error(
                "action: worker_data_processing | result: fail | batch: %s | error: %s",
                getattr(msg, "batch_number", "N/A"),
                e,
            )

    def _process_eof_message(self, msg):
        """Processes an EOF message by broadcasting it."""
        try:
            # Your EOFMessage class requires the client_id to be set before serialization
            # to include it in the payload.
            msg.client_id = getattr(msg, "client_id", None)

            message_bytes = msg.to_bytes()
            self._broadcast_eof_to_all(message_bytes)
            logging.info(
                "action: worker_eof_forwarded | result: success | table_type: %s",
                msg.get_table_type(),
            )
        except Exception as e:
            logging.error(
                "action: worker_eof_processing | result: fail | error: %s", e
            )