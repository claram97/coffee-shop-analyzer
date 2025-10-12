# orchestrator/app/net.py
"""
Refactored Orchestrator using modular architecture.
"""

import logging
import os
from typing import Optional

from app.results_consumer import ResultsConsumer
from common.network import MessageHandler, ResponseHandler, ServerManager
from common.processing import create_filtered_data_batch, message_logger

from middleware.middleware_client import MessageMiddlewareExchange
from protocol.constants import Opcodes


class Orchestrator:
    """Orchestrator using modular network and processing components."""

    def __init__(self, port: int, listen_backlog: int):
        """Initialize modular orchestrator.

        Args:
            port: Port to listen on
            listen_backlog: Maximum pending connections
        """
        rabbitmq_host = os.getenv("RABBITMQ_HOST", "rabbitmq")
        self._fr_exchange = os.getenv("ORCH_TO_FR_EXCHANGE", "orch.to.fr")
        self._fr_rk_fmt = os.getenv("ORCH_TO_FR_RK_FMT", "fr.{pid:02d}")
        self._num_routers = max(1, int(os.getenv("FILTER_ROUTER_COUNT", "1")))

        self._publishers: dict[str, MessageMiddlewareExchange] = {}

        self.message_handler = MessageHandler()
        self._client_ids: dict[object, str] = {}

        results_queue = os.getenv("RESULTS_QUEUE", "orchestrator_results_queue")
        self.results_consumer = ResultsConsumer(results_queue, rabbitmq_host)

        self.server_manager = ServerManager(
            port,
            listen_backlog,
            self._handle_message,
            self._cleanup_client,
        )

        self._host = rabbitmq_host

        self._setup_message_processors()

    def _get_client_id(self, client_sock) -> Optional[str]:
        return self._client_ids.get(client_sock)

    def _register_client(self, client_sock, client_id: str):
        self._client_ids[client_sock] = client_id

    def _cleanup_client(self, client_sock):
        self._client_ids.pop(client_sock, None)

    def _publisher_for_rk(self, rk: str) -> MessageMiddlewareExchange:
        pub = self._publishers.get(rk)
        if pub is None:
            logging.info(
                "action: create_publisher | exchange: %s | rk: %s | host: %s",
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
        pid = 0 if batch_number is None else int(batch_number) % self._num_routers
        rk = self._fr_rk_fmt.format(pid=pid)
        self._publisher_for_rk(rk).send(raw)

    def _broadcast_eof_to_all(self, raw: bytes):
        for pid in range(self._num_routers):
            rk = self._fr_rk_fmt.format(pid=pid)
            self._publisher_for_rk(rk).send(raw)

    def _setup_message_processors(self):
        """Setup message processors for different message types."""
        for opcode in [
            Opcodes.NEW_MENU_ITEMS,
            Opcodes.NEW_STORES,
            Opcodes.NEW_TRANSACTION_ITEMS,
            Opcodes.NEW_TRANSACTION,
            Opcodes.NEW_USERS,
        ]:
            self.message_handler.register_processor(opcode, self._process_data_message)

        self.message_handler.register_processor(Opcodes.EOF, self._process_eof_message)

        for opcode in range(1, 5):
            self.message_handler.register_processor(opcode, self._process_query_request)

    def _handle_message(self, msg, client_sock) -> bool:
        if msg.opcode == Opcodes.CLIENT_HELLO:
            client_id = getattr(msg, "client_id", None)
            if not client_id:
                logging.error("action: client_hello | result: fail | reason: missing_client_id")
                ResponseHandler.send_failure(client_sock)
                return False

            self._register_client(client_sock, client_id)
            logging.info(
                "action: client_hello | result: success | client_id: %s | fileno: %s",
                client_id,
                client_sock.fileno(),
            )
            ResponseHandler.send_success(client_sock)
            return True

        client_id = self._get_client_id(client_sock)
        if not client_id:
            logging.error(
                "action: message_received | result: fail | reason: missing_handshake | opcode: %s",
                msg.opcode,
            )
            ResponseHandler.send_failure(client_sock)
            return False

        setattr(msg, "client_id", client_id)
        return self.message_handler.handle_message(msg, client_sock, client_id=client_id)

    def _process_data_message(self, msg, client_sock) -> bool:
        try:
            status_text = self.message_handler.get_status_text(
                getattr(msg, "batch_status", 0)
            )

            message_logger.write_original_message(msg, status_text)

            self._process_filtered_batch(msg, status_text)

            message_logger.log_batch_processing_success(msg, status_text)
            self.message_handler.log_batch_preview(msg, status_text)

            ResponseHandler.send_success(client_sock)
            return True
        except Exception as e:
            return ResponseHandler.handle_processing_error(msg, client_sock, e)

    def register_client_for_query(self, query_id, client_sock, client_id: str):
        self.results_consumer.register_client_for_query(query_id, client_sock, client_id)
        logging.info(
            "action: client_registered_for_query | query_id: %s | client_id: %s",
            query_id,
            client_id,
        )

    def _process_query_request(self, msg, client_sock) -> bool:
        try:
            query_id = str(msg.opcode)
            client_id = self._get_client_id(client_sock)
            if not client_id:
                logging.error(
                    "action: query_request_received | result: fail | reason: missing_client_id | query_id: %s",
                    query_id,
                )
                ResponseHandler.send_failure(client_sock)
                return False

            self.register_client_for_query(query_id, client_sock, client_id)
            ResponseHandler.send_success(client_sock)
            logging.info("action: query_request_received | query_id: %s", query_id)
            return True
        except Exception as e:
            logging.error(
                "action: query_request_processing | result: fail | error: %s", e
            )
            return ResponseHandler.handle_processing_error(msg, client_sock, e)

    def _process_filtered_batch(self, msg, status_text: str):
        """Filtra, empaqueta y publica el DataBatch al exchange del Filter Router."""
        try:
            client_id = getattr(msg, "client_id", None)
            if not client_id:
                raise RuntimeError("missing client_id for filtered batch")

            filtered_batch = create_filtered_data_batch(msg, client_id)
            batch_bytes = filtered_batch.to_bytes()

            bn = int(getattr(filtered_batch, "batch_number", 0) or 0)
            self._send_to_filter_router_exchange(batch_bytes, bn)

        except Exception as filter_error:
            logging.error(
                "action: batch_filter | result: fail | batch_number: %d | opcode: %d | error: %s",
                getattr(msg, "batch_number", 0),
                msg.opcode,
                str(filter_error),
            )

    def _process_eof_message(self, msg, client_sock) -> bool:
        """Reenvía EOFs al exchange del Filter Router (broadcast a todas las réplicas)."""
        try:
            table_type = msg.get_table_type()
            logging.info(
                "action: eof_received | result: success | table_type: %s | batch_number: %d",
                table_type,
                getattr(msg, "batch_number", 0),
            )

            message_bytes = msg.to_bytes()

            self._broadcast_eof_to_all(message_bytes)

            logging.info(
                "action: eof_forwarded | result: success | table_type: %s | bytes_length: %d | replicas: %d",
                table_type,
                len(message_bytes),
                self._num_routers,
            )

            ResponseHandler.send_success(client_sock)
            return True

        except Exception as e:
            logging.error(
                "action: eof_processing | result: fail | table_type: %s | batch_number: %d | error: %s",
                getattr(msg, "table_type", "unknown"),
                getattr(msg, "batch_number", 0),
                str(e),
            )
            return ResponseHandler.handle_processing_error(msg, client_sock, e)

    def run(self):
        """Start the orchestrator server and results consumer."""
        self.results_consumer.start()
        try:
            self.server_manager.run()
        finally:
            self.results_consumer.stop()


class MockFilterRouterQueue:
    """(Sigue ahí por compat de tests que lo importan en otro lado)"""

    def send(self, message_bytes: bytes):
        logging.info(
            "action: mock_queue_send | result: success | message_size: %d bytes",
            len(message_bytes),
        )
