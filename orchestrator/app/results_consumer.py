"""
Results consumer that handles query results from the results-finisher component.
"""

import logging
import threading
from typing import Dict, Optional

from google.protobuf.message import DecodeError

from middleware.middleware_client import (
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareQueue,
)
from protocol import BatchStatus, DataBatch, Opcodes
from protocol.messages import (
    QueryResult1,
    QueryResult2,
    QueryResult3,
    QueryResult4,
    QueryResultError,
)
from protocol2.envelope_pb2 import Envelope, MessageType
from protocol2.table_data_pb2 import TableName, TableStatus
from protocol2.table_data_utils import iterate_rows_as_dicts


class ResultsConsumer:
    """Consumes result messages from the results-finisher component and forwards them to clients."""

    def __init__(self, queue_name: str = "orchestrator_results_queue", host: str = "rabbitmq"):
        """Initialize the results consumer.

        Args:
            queue_name: The name of the queue to consume from
            host: RabbitMQ host address
        """
        self.queue_name = queue_name
        self.host = host
        self.client_connections: Dict[str, Dict[str, object]] = {}
        self.middleware: Optional[MessageMiddlewareQueue] = None
        self.stopping = False
        self.client_lock = threading.Lock()
        self.orchestrator = None

    def set_orchestrator(self, orchestrator):
        """Set the orchestrator instance for the consumer."""
        self.orchestrator = orchestrator
        logging.info("action: set_orchestrator | result: success")

    def start(self):
        """Start consuming results from the queue."""
        try:
            self.middleware = MessageMiddlewareQueue(host=self.host, queue_name=self.queue_name)
            self.middleware.start_consuming(self._process_result)
            logging.info(
                "action: results_consumer_start | result: success | queue: %s",
                self.queue_name,
            )
        except MessageMiddlewareDisconnectedError as e:
            logging.error(
                "action: results_consumer_start | result: connection_fail | error: %s",
                e,
            )
        except Exception as e:
            logging.error(
                "action: results_consumer_start | result: fail | error: %s",
                e,
            )

    def stop(self):
        """Stop the consumer."""
        self.stopping = True
        if not self.middleware:
            return
        try:
            self.middleware.stop_consuming()
            self.middleware.close()
            logging.info("action: results_consumer_stop | result: success")
        except MessageMiddlewareDisconnectedError as e:
            logging.warning(
                "action: results_consumer_stop | result: already_disconnected | error: %s",
                e,
            )
        except Exception as e:
            logging.error(
                "action: results_consumer_stop | result: fail | error: %s",
                e,
            )

    def register_client(self, client_connection, client_id: str, lock: Optional[object] = None):
        """Register a client connection (and optional lock) for delivering results."""
        with self.client_lock:
            self.client_connections[client_id] = {"conn": client_connection, "lock": lock}
        logging.info(
            "action: client_registered | result: success | client_id: %s",
            client_id,
        )

    def unregister_client(self, client_identifier):
        """Unregister the client from receiving results."""
        with self.client_lock:
            if isinstance(client_identifier, str):
                key = client_identifier
                removed = self.client_connections.pop(key, None) is not None
            else:
                # Lookup by connection object (fallback for legacy callers).
                key = None
                removed = False
                for cid, entry in list(self.client_connections.items()):
                    conn = entry.get("conn")
                    if conn is client_identifier:
                        key = cid
                        removed = self.client_connections.pop(cid, None) is not None
                        break
        logging.info(
            "action: client_unregistered | result: %s | client_id: %s",
            "success" if removed else "not_found",
            key if isinstance(key, str) else "unknown",
        )

    def _process_result(self, body: bytes):
        """Process a result message from the queue."""
        if not body:
            logging.warning(
                "action: process_result | result: empty_message | error: payload_is_empty"
            )
            return

        # Try to parse the new protobuf envelope first.
        try:
            env = Envelope()
            env.ParseFromString(body)
            if env.type == MessageType.DATA_BATCH:
                self._new_process_result(env)
                return
            logging.warning(
                "action: process_result | result: unsupported_envelope_type | type: %s",
                env.type,
            )
            return
        except DecodeError:
            pass
        except Exception as e:
            logging.warning(
                "action: process_result | result: envelope_parse_fail | error: %s",
                e,
            )

        # Fallback to the legacy opcode-framed format.
        self._process_result_legacy(body)

    def _new_process_result(self, env: Envelope):
        """Handle results encoded using the protobuf Envelope/DataBatch format."""
        try:
            data_batch = env.data_batch

            if not data_batch.query_ids:
                logging.warning(
                    "action: process_result | result: missing_query_id | error: No query_id in batch"
                )
                return

            query_enum = int(data_batch.query_ids[0])
            query_id = str(query_enum + 1)

            client_id = getattr(data_batch, "client_id", None)
            if not client_id:
                logging.warning(
                    "action: process_result | result: missing_client_id | query_id: %s",
                    query_id,
                )
                return

            table_msg = data_batch.payload
            batch_status = getattr(table_msg, "status", None)
            status_text = self._status_to_text(batch_status)
            row_count = len(table_msg.rows) if hasattr(table_msg, "rows") else 0

            try:
                table_name = TableName.Name(table_msg.name)
            except (ValueError, AttributeError):
                table_name = str(getattr(table_msg, "name", "unknown"))

            logging.info(
                "action: result_received | result: success | query_id: %s | table: %s | batch_status: %s | rows: %d",
                query_id,
                table_name,
                status_text,
                row_count,
            )

            if row_count > 0 and hasattr(table_msg, "rows"):
                try:
                    max_rows_to_log = 30
                    for idx, row_dict in enumerate(iterate_rows_as_dicts(table_msg), start=1):
                        logging.info(
                            "action: result_row | query_id: %s | row: %d/%d | data: %s",
                            query_id,
                            idx,
                            row_count,
                            row_dict,
                        )
                        if idx >= max_rows_to_log:
                            if row_count > max_rows_to_log:
                                logging.info(
                                    "action: result_row | query_id: %s | message: 'Only logged first %d rows out of %d'",
                                    query_id,
                                    max_rows_to_log,
                                    row_count,
                                )
                            break
                except Exception as e:
                    logging.warning(
                        "action: log_row_data | result: fail | error: %s", e
                    )

            legacy_bytes = self._build_legacy_result_bytes(
                query_enum=query_enum,
                table_msg=table_msg,
                query_id=query_id,
            )
            if legacy_bytes is None:
                logging.error(
                    "action: process_result | result: fail | error: legacy_conversion_failed | query_id: %s",
                    query_id,
                )
                return

            self._forward_result_to_client(query_id, legacy_bytes, client_id)

        except Exception as e:
            logging.error(
                "action: process_result | result: fail | error: %s",
                e,
            )

    def _process_result_legacy(self, body: bytes):
        """Legacy handler for opcode-framed DataBatch messages."""
        try:
            if body[0] != Opcodes.DATA_BATCH:
                logging.warning(
                    "action: process_result | result: unknown_format | error: Not a legacy DataBatch"
                )
                return

            data_batch = DataBatch.deserialize_from_bytes(body)

            if not data_batch.query_ids:
                logging.warning(
                    "action: process_result | result: missing_query_id | error: No query_id in batch"
                )
                return

            query_id = str(data_batch.query_ids[0])
            client_id = getattr(data_batch, "client_id", None)
            if not client_id:
                logging.warning(
                    "action: process_result | result: missing_client_id | query_id: %s",
                    query_id,
                )
                return

            inner_opcode = getattr(data_batch.batch_msg, "opcode", "unknown")
            batch_status = getattr(data_batch.batch_msg, "batch_status", "unknown")
            row_count = (
                len(getattr(data_batch.batch_msg, "rows", []))
                if hasattr(data_batch.batch_msg, "rows")
                else 0
            )

            status_text = "UNKNOWN"
            if batch_status == BatchStatus.CONTINUE:
                status_text = "CONTINUE"
            elif batch_status == BatchStatus.EOF:
                status_text = "EOF"
            elif batch_status == BatchStatus.CANCEL:
                status_text = "CANCEL"

            opcode_name = self._get_opcode_name(inner_opcode)

            logging.info(
                "action: result_received | result: success | query_id: %s | opcode: %s (%s) | batch_status: %s (%s) | rows: %d",
                query_id,
                inner_opcode,
                opcode_name,
                batch_status,
                status_text,
                row_count,
            )

            if row_count > 0 and hasattr(data_batch.batch_msg, "rows"):
                try:
                    if inner_opcode == Opcodes.QUERY_RESULT_1:
                        max_rows_to_log = 30
                        rows_to_log = data_batch.batch_msg.rows[:max_rows_to_log]
                        logging.info(
                            "action: query_1_amount_rows | query_id: %s | total_rows: %d | logging_first: %d",
                            query_id,
                            row_count,
                            max_rows_to_log,
                        )
                        for idx, row in enumerate(rows_to_log, start=1):
                            row_data = self._row_to_dict(row)
                            logging.info(
                                "action: result_row | query_id: %s | row: %d/%d | data: %s",
                                query_id,
                                idx,
                                row_count,
                                row_data,
                            )
                        if row_count > max_rows_to_log:
                            logging.info(
                                "action: result_row | query_id: %s | message: 'Only logged first %d rows out of %d'",
                                query_id,
                                max_rows_to_log,
                                row_count,
                            )
                    else:
                        max_rows_to_log = 30
                        for idx, row in enumerate(data_batch.batch_msg.rows, start=1):
                            row_data = self._row_to_dict(row)
                            logging.info(
                                "action: result_row | query_id: %s | row: %d/%d | data: %s",
                                query_id,
                                idx,
                                row_count,
                                row_data,
                            )
                            if idx >= max_rows_to_log:
                                if row_count > max_rows_to_log:
                                    logging.info(
                                        "action: result_row | query_id: %s | message: 'Only logged first %d rows out of %d'",
                                        query_id,
                                        max_rows_to_log,
                                        row_count,
                                    )
                                break
                except Exception as e:
                    logging.warning(
                        "action: log_row_data | result: fail | error: %s",
                        e,
                    )

            self._forward_result_to_client(
                query_id,
                data_batch.batch_msg.to_bytes(),
                client_id,
            )

        except Exception as e:
            logging.error(
                "action: process_result | result: fail | error: %s",
                e,
            )

    @staticmethod
    def _row_to_dict(row) -> Dict[str, str]:
        if isinstance(row, dict):
            return row
        attrs = [
            attr
            for attr in dir(row)
            if not attr.startswith("_") and not callable(getattr(row, attr))
        ]
        return {attr: getattr(row, attr) for attr in attrs}

    def _get_opcode_name(self, opcode: int) -> str:
        """Get a human-readable name for an opcode."""
        opcode_names = {
            Opcodes.QUERY_RESULT_1: "QUERY_RESULT_1",
            Opcodes.QUERY_RESULT_2: "QUERY_RESULT_2",
            Opcodes.QUERY_RESULT_3: "QUERY_RESULT_3",
            Opcodes.QUERY_RESULT_4: "QUERY_RESULT_4",
            Opcodes.QUERY_RESULT_ERROR: "QUERY_RESULT_ERROR",
        }
        return opcode_names.get(opcode, "UNKNOWN")

    def _forward_result_to_client(self, query_id: str, result_bytes: bytes, client_id: str):
        """Forward the result to the appropriate client."""
        with self.client_lock:
            entry = self.client_connections.get(client_id)
        if not entry:
            logging.warning(
                "action: forward_result | result: no_client | query_id: %s",
                query_id,
            )
            return
        client_conn = entry.get("conn")
        lock = entry.get("lock")
        try:
            if lock is not None:
                with lock:
                    client_conn.sendall(result_bytes)
            else:
                client_conn.sendall(result_bytes)
            logging.info(
                "action: forward_result | result: success | query_id: %s | bytes_sent: %d",
                query_id,
                len(result_bytes),
            )
            if self.orchestrator:
                self.orchestrator.increment_queries_sent(client_id)
        except Exception as e:
            logging.error(
                "action: forward_result | result: fail | query_id: %s | error: %s",
                query_id,
                e,
            )
            self.unregister_client(client_id)

    @staticmethod
    def _status_to_text(status) -> str:
        if status == TableStatus.CONTINUE:
            return "CONTINUE"
        if status == TableStatus.EOF:
            return "EOF"
        if status == TableStatus.CANCEL:
            return "CANCEL"
        return "UNKNOWN"

    @staticmethod
    def _map_table_status_to_batch_status(status) -> int:
        if status == TableStatus.EOF:
            return BatchStatus.EOF
        if status == TableStatus.CANCEL:
            return BatchStatus.CANCEL
        return BatchStatus.CONTINUE

    def _build_legacy_result_bytes(
        self, query_enum: int, table_msg, query_id: str
    ) -> Optional[bytes]:
        """Convert a protobuf TableData payload into the legacy TableMessage representation."""
        status = getattr(table_msg, "status", TableStatus.CONTINUE)
        batch_status = self._map_table_status_to_batch_status(status)
        batch_number = int(getattr(table_msg, "batch_number", 0) or 0)
        rows = list(iterate_rows_as_dicts(table_msg))

        # Error path
        if status == TableStatus.CANCEL:
            msg = QueryResultError()
            if not rows:
                rows = [
                    {
                        "query_id": query_id,
                        "error_code": "EXECUTION_ERROR",
                        "error_message": "Unknown error",
                    }
                ]
            normalized = []
            for row in rows:
                normalized.append(
                    {
                        "query_id": row.get("query_id", query_id),
                        "error_code": row.get("error_code", "EXECUTION_ERROR"),
                        "error_message": row.get("error_message", ""),
                    }
                )
            msg.rows = normalized
            msg.batch_number = batch_number
            msg.batch_status = batch_status
            return msg.to_bytes()

        if query_enum == 0:
            msg = QueryResult1()
            normalized = []
            for row in rows:
                normalized.append(
                    {
                        "transaction_id": row.get("transaction_id", ""),
                        "final_amount": row.get("final_amount", ""),
                    }
                )
        elif query_enum == 1:
            msg = QueryResult2()
            normalized = []
            for row in rows:
                normalized.append(
                    {
                        "month": row.get("month", ""),
                        "name": row.get("item_name") or row.get("name", ""),
                        "quantity": row.get("quantity", ""),
                        "revenue": row.get("revenue", ""),
                    }
                )
        elif query_enum == 2:
            msg = QueryResult3()
            normalized = []
            for row in rows:
                normalized.append(
                    {
                        "store_name": row.get("store_name", ""),
                        "period": row.get("period", ""),
                        "amount": row.get("amount", ""),
                    }
                )
        elif query_enum == 3:
            msg = QueryResult4()
            normalized = []
            for row in rows:
                normalized.append(
                    {
                        "store_name": row.get("store_name", ""),
                        "birthdate": row.get("birthdate", ""),
                        "purchase_count": row.get("purchase_count", ""),
                    }
                )
        else:
            logging.error(
                "action: build_legacy_result | result: unsupported_query | query_enum: %s",
                query_enum,
            )
            return None

        msg.rows = normalized
        msg.batch_number = batch_number
        msg.batch_status = batch_status
        return msg.to_bytes()
