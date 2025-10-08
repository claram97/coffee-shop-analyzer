# orchestrator/app/net.py
"""
Refactored Orchestrator using protobuf messages.
"""

import logging
import os
from typing import Optional

from app.results_consumer import ResultsConsumer
from common.protobuf_handler import (
    ProtobufMessageReader,
    ProtobufMessageWriter,
    ProtobufMessageHandler
)

from middleware.middleware_client import MessageMiddlewareExchange


class Orchestrator:
    """Orchestrator using protobuf messages."""

    def __init__(self, port: int, listen_backlog: int):
        """Initialize protobuf-based orchestrator.

        Args:
            port: Port to listen on
            listen_backlog: Maximum pending connections
        """
        rabbitmq_host = os.getenv("RABBITMQ_HOST", "rabbitmq")
        self._fr_exchange = os.getenv("ORCH_TO_FR_EXCHANGE", "orch.to.fr")
        self._fr_rk_fmt = os.getenv("ORCH_TO_FR_RK_FMT", "fr.{pid:02d}")
        self._num_routers = max(1, int(os.getenv("FILTER_ROUTER_COUNT", "1")))

        self._publishers: dict[str, MessageMiddlewareExchange] = {}

        # Use protobuf message handler
        self.message_handler = ProtobufMessageHandler()

        results_queue = os.getenv("RESULTS_QUEUE", "orchestrator_results_queue")
        self.results_consumer = ResultsConsumer(results_queue, rabbitmq_host)

        # Server manager with protobuf
        from common.network import ServerManager
        self.server_manager = ServerManager(
            port, 
            listen_backlog, 
            self._handle_client
        )

        self._host = rabbitmq_host
        self._port = port

        self._setup_message_processors()

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
        """Setup protobuf message processors."""
        from protos import envelope_pb2
        
        # Register TABLE_DATA processor
        self.message_handler.register_processor(
            envelope_pb2.Envelope.TABLE_DATA,
            self._process_table_data_message
        )
        
        # Register EOF processor
        self.message_handler.register_processor(
            envelope_pb2.Envelope.EOF,
            self._process_eof_message
        )
        
        # Register QUERY_REQUEST processor
        self.message_handler.register_processor(
            envelope_pb2.Envelope.QUERY_REQUEST,
            self._process_query_request
        )
        
        # Register FINISHED processor
        self.message_handler.register_processor(
            envelope_pb2.Envelope.FINISHED,
            self._process_finished_message
        )

    def _handle_client(self, client_sock):
        """Handle client connection with protobuf messages."""
        try:
            logging.info("action: client_connected | result: start")
            while True:
                # Read protobuf envelope
                envelope = ProtobufMessageReader.read_envelope(client_sock)
                if envelope is None:
                    logging.info("action: client_disconnected | result: eof")
                    break
                
                # Process envelope
                success = self.message_handler.handle_envelope(envelope, client_sock)
                if not success:
                    logging.warning("action: message_processing | result: failed")
                    
        except Exception as e:
            logging.error(f"action: handle_client | result: fail | error: {e}")
        finally:
            client_sock.close()

    def _process_table_data_message(self, envelope, client_sock) -> bool:
        """Process TABLE_DATA envelope."""
        from protos import envelope_pb2, table_data_pb2
        
        try:
            if not envelope.HasField('table_data'):
                logging.error("action: process_table_data | result: fail | error: missing table_data")
                ProtobufMessageWriter.send_batch_fail(client_sock)
                return False
            
            table_data = envelope.table_data
            
            table_name = table_data_pb2.TableName.Name(table_data.name)
            batch_status = table_data_pb2.BatchStatus.Name(table_data.status)
            
            logging.info(
                "action: table_data_received | table: %s | batch: %d | rows: %d | status: %s",
                table_name,
                table_data.batch_number,
                len(table_data.rows),
                batch_status
            )
            
            # Serialize and forward to RabbitMQ
            table_bytes = table_data.SerializeToString()
            self._send_to_filter_router_exchange(table_bytes, table_data.batch_number)
            
            logging.info(
                "action: table_data_forwarded | table: %s | batch: %d | bytes: %d",
                table_name,
                table_data.batch_number,
                len(table_bytes)
            )
            
            # Send success response
            ProtobufMessageWriter.send_batch_success(client_sock)
            
            return True
            
        except Exception as e:
            logging.error(f"action: process_table_data | result: fail | error: {e}")
            ProtobufMessageWriter.send_batch_fail(client_sock)
            return False

    def _process_query_request(self, envelope, client_sock) -> bool:
        """Process QUERY_REQUEST envelope."""
        from protos import envelope_pb2, query_request_pb2
        
        try:
            if not envelope.HasField('query_request'):
                logging.error("action: process_query_request | result: fail | error: missing query_request")
                ProtobufMessageWriter.send_batch_fail(client_sock)
                return False
            
            query_request = envelope.query_request
            query_type = query_request.query_type
            
            # Map QueryType enum to query ID (0->1, 1->2, etc.)
            query_id = str(query_type + 1)
            
            query_type_name = query_request_pb2.QueryType.Name(query_type)
            
            logging.info(
                "action: query_request_received | query_type: %s | query_id: %s",
                query_type_name,
                query_id
            )
            
            # Register client for results
            self.results_consumer.register_client_for_query(query_id, client_sock)
            
            # Send success response
            ProtobufMessageWriter.send_batch_success(client_sock)
            
            logging.info("action: client_registered_for_query | query_id: %s", query_id)
            return True
            
        except Exception as e:
            logging.error(f"action: process_query_request | result: fail | error: {e}")
            ProtobufMessageWriter.send_batch_fail(client_sock)
            return False

    def _process_eof_message(self, envelope, client_sock) -> bool:
        """Process EOF envelope."""
        from protos import envelope_pb2, table_data_pb2, eof_message_pb2
        
        try:
            if not envelope.HasField('eof'):
                logging.error("action: process_eof | result: fail | error: missing eof")
                ProtobufMessageWriter.send_batch_fail(client_sock)
                return False
            
            eof_msg = envelope.eof
            table_name = table_data_pb2.TableName.Name(eof_msg.table)
            
            logging.info(
                "action: eof_received | table: %s",
                table_name
            )
            
            # Serialize and broadcast EOF to all filter routers
            envelope_bytes = envelope.SerializeToString()
            self._broadcast_eof_to_all(envelope_bytes)
            
            logging.info(
                "action: eof_forwarded | table: %s | bytes: %d | replicas: %d",
                table_name,
                len(envelope_bytes),
                self._num_routers
            )
            
            # Send success response
            ProtobufMessageWriter.send_batch_success(client_sock)
            
            return True
            
        except Exception as e:
            logging.error(f"action: process_eof | result: fail | error: {e}")
            ProtobufMessageWriter.send_batch_fail(client_sock)
            return False

    def _process_finished_message(self, envelope, client_sock) -> bool:
        """Process FINISHED envelope."""
        try:
            logging.info("action: finished_received | client: finishing")
            ProtobufMessageWriter.send_batch_success(client_sock)
            return True
        except Exception as e:
            logging.error(f"action: process_finished | result: fail | error: {e}")
            return False

    def run(self):
        """Start the orchestrator server and results consumer."""
        logging.info("action: orchestrator_starting | port: %d", self._port)
        self.results_consumer.start()
        try:
            self.server_manager.run()
        finally:
            self.results_consumer.stop()


class MockFilterRouterQueue:
    """Mock queue for testing."""

    def send(self, message_bytes: bytes):
        logging.info(
            "action: mock_queue_send | result: success | message_size: %d bytes",
            len(message_bytes),
        )
