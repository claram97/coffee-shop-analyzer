#!/usr/bin/env python3
"""
Coffee Shop Data Aggregator

This module implements the main aggregator functionality for collecting,
processing and analyzing coffee shop data from multiple clients.
"""
import logging
import os
from collections import defaultdict
from typing import Dict, Optional, Tuple

from processing import (
    process_query_2,
    process_query_3,
    process_query_4_transactions,
    serialize_query2_results,
    serialize_query3_results,
    serialize_query4_transaction_results,
)

from app_config.config_loader import Config
from middleware.middleware_client import (
    MessageMiddlewareExchange,
    MessageMiddlewareQueue,
)
from protocol2.databatch_pb2 import DataBatch
from protocol2.envelope_pb2 import Envelope, MessageType
from protocol2.table_data_pb2 import TableName
from protocol2.table_data_utils import build_table_data, iterate_rows_as_dicts

# Para transactions y query 1: re-enviar
# Para transaction_items y query 2: procesamiento en esta instancia
# Para transactions y query 3: procesamiento en esta instancia
# Para transactions y query 4: procesamiento en esta instancia

TID_TO_NAME = {
    TableName.MENU_ITEMS: "menu_items",
    TableName.STORES: "stores",
    TableName.TRANSACTION_ITEMS: "transaction_items",
    TableName.TRANSACTIONS: "transactions",
    TableName.USERS: "users",
}
LIGHT_TABLES = {"menu_items", "stores"}


class ExchangePublisherPool:
    def __init__(self, host):
        self._host = host
        self._pool = {}
        logging.info("Created exchange publisher pool for host: %s", host)

    def get_exchange(self, exchange_name, route_keys, queue_name=None):
        key = (
            exchange_name,
            tuple(route_keys) if isinstance(route_keys, list) else (route_keys,),
            queue_name,  # Include queue_name in cache key
        )
        if key not in self._pool:
            logging.info(
                "Creating new exchange in pool: %s with keys %s",
                exchange_name,
                route_keys,
            )
            self._pool[key] = MessageMiddlewareExchange(
                host=self._host,
                exchange_name=exchange_name,
                route_keys=route_keys,
                queue_name=queue_name,
            )
        else:
            logging.debug("Reusing existing exchange from pool: %s", exchange_name)
        return self._pool[key]


class Aggregator:
    """Main aggregator class for coffee shop data analysis."""

    def __init__(self, id: str):
        """
        Initialize the aggregator.

        Args:
          id (str): The unique identifier for the aggregator instance.
        """
        self.id = id
        self.running = False
        self.config_path = os.getenv("CONFIG_PATH", "/config/config.ini")
        self.config = Config(self.config_path)
        self.host = self.config.broker.host
        self._jr_replicas = self.config.routers.joiner

        self._exchange_pool = ExchangePublisherPool(host=self.host)
        logging.info("Initialized exchange pool for aggregator %s", id)

        tables = ["menu_items", "stores", "transactions", "transaction_items", "users"]
        self._exchanges = {}

        for table in tables:
            exchange_name = self.config.filter_router_exchange(table)
            routing_key = self.config.filter_router_rk(table, self.id)
            queue_name = self.config.aggregator_queue(table, self.id)

            self._exchanges[table] = self._exchange_pool.get_exchange(
                exchange_name=exchange_name,
                route_keys=[routing_key],
                queue_name=queue_name,
            )
            logging.info(
                "Created exchange connection for %s: %s -> %s with queue %s",
                table,
                exchange_name,
                routing_key,
                queue_name,
            )

        self._out_queues = {}
        for table in tables:
            for replica in range(0, self._jr_replicas):
                out_q = self.config.aggregator_to_joiner_router_queue(
                    table, self.id, replica
                )
                self._out_queues[(table, replica)] = MessageMiddlewareQueue(
                    host=self.host, queue_name=out_q
                )

        # In-memory batch deduplication: track received batches per (table, client_id, query_ids_tuple)
        self._received_batches = defaultdict(set)
        # Track joiner replicas used per (table, client_id)
        self._client_table_replicas: Dict[Tuple[str, str], set[int]] = defaultdict(set)

    def _is_duplicate_batch(self, data_batch: DataBatch) -> bool:
        """
        Check if a batch is a duplicate. Returns True if duplicate, False otherwise.
        Tracks batches by (table, client_id, query_ids_tuple, batch_number).
        """
        table = data_batch.payload.name
        client_id = data_batch.client_id
        batch_number = data_batch.payload.batch_number
        query_ids_tuple = tuple(sorted(data_batch.query_ids))

        dedup_key = (table, client_id, query_ids_tuple)

        # if batch_number in self._received_batches[dedup_key]:
        #     logging.warning(
        #         "DUPLICATE batch detected and discarded in aggregator: table=%s bn=%s client=%s queries=%s",
        #         table, batch_number, client_id, data_batch.query_ids
        #     )
        #     return True

        # Mark batch as received
        self._received_batches[dedup_key].add(batch_number)
        logging.debug(
            "Batch marked as received: table=%s bn=%s client=%s queries=%s",
            table,
            batch_number,
            client_id,
            data_batch.query_ids,
        )
        return False

    def _target_replicas(
        self, table: str, client_id: Optional[str], batch_number: int
    ) -> list[int]:
        if table in LIGHT_TABLES:
            replicas = list(range(self._jr_replicas))
        else:
            shard = batch_number if batch_number is not None else 0
            replicas = [shard % self._jr_replicas]
        if client_id:
            self._client_table_replicas[(table, client_id)].update(replicas)
        return replicas

    def _send_to_joiner_by_table(
        self, table: str, client_id: Optional[str], batch_number: int, raw_bytes: bytes
    ):
        logging.info("Sending to joiner by table=%s", table)
        replicas = self._target_replicas(table, client_id, batch_number)
        for replica in replicas:
            q = self._out_queues.get((table, replica))
            if not q:
                logging.error(
                    "No out queue configured for table=%s replica=%s", table, replica
                )
                raise Exception(f"No out queue configured for table={table}")
            q.send(raw_bytes)

    def _forward_databatch_by_table(
        self, data_batch: DataBatch, raw: bytes, table_name: str
    ):
        logging.info("Forwarding DataBatch message")
        """Reenvía DataBatch a la cola correcta usando el table_name provisto."""
        try:
            table = table_name
            logging.debug("Forwarding DataBatch to table=%s", table)
            if not table:
                logging.error("Unknown table_name=%s", table_name)
                return

            batch_number = int(getattr(data_batch.payload, "batch_number", 0) or 0)
            self._send_to_joiner_by_table(
                table, data_batch.client_id, batch_number, raw
            )
        except Exception:
            logging.exception(
                "Failed to forward databatch by table. Table: %s", table_name
            )
            raise  # Re-raise to cause NACK and redelivery

    def _forward_eof(self, raw: bytes, table_name: str):
        logging.debug("Forwarding EOF message")
        """Parse EOF, update trace with aggregator ID, and forward to joiner routers."""
        try:
            envelope = Envelope()
            envelope.ParseFromString(raw)
            eof = envelope.eof

            # Update trace: append aggregator_id to existing trace
            # Expected format: "filter_router_id:aggregator_id"
            # The filter already set this, so we keep it as-is
            original_trace = eof.trace if eof.trace else ""
            logging.info(
                "Forwarding EOF for table=%s with trace=%s", table_name, original_trace
            )
            client_id = eof.client_id if eof.client_id else None
            replicas: list[int]
            if table_name in LIGHT_TABLES or not client_id:
                replicas = list(range(self._jr_replicas))
            else:
                replicas = list(
                    self._client_table_replicas.pop((table_name, client_id), [])
                )
                if not replicas:
                    replicas = list(range(self._jr_replicas))

            for replica in replicas:
                q = self._out_queues.get((table_name, replica))
                if not q:
                    raise Exception(
                        f"No out queue configured for table={table_name}, replica={replica}"
                    )
                q.send(raw)
                logging.debug(
                    "Forwarded EOF to joiner_router replica=%s table=%s trace=%s",
                    replica,
                    table_name,
                    original_trace,
                )
            if client_id:
                self._client_table_replicas.pop((table_name, client_id), None)
        except Exception:
            logging.exception("Failed to forward EOF")
            raise

    def _broadcast_cleanup_to_joiners(self, raw: bytes, input_table: str):
        """
        Broadcast client cleanup message to all joiner router replicas.
        Send to all queues (all tables) for all joiner router replicas.
        
        Args:
            raw: Raw message bytes
            input_table: The table name of the input queue that received this cleanup message
        """
        try:
            envelope = Envelope()
            envelope.ParseFromString(raw)
            cleanup_msg = envelope.clean_up
            client_id = cleanup_msg.client_id if cleanup_msg.client_id else ""

            # Base trace: append aggregator_id and input_table to existing trace
            # Format: {filter_router_id}:{aggregator_id}:{input_table}
            original_trace = cleanup_msg.trace if cleanup_msg.trace else ""
            if original_trace:
                base_trace = f"{original_trace}:{self.id}:{input_table}"
            else:
                base_trace = f"{self.id}:{input_table}"

            tables = ["menu_items", "stores", "transactions", "transaction_items", "users"]
            total_queues = len(tables) * self._jr_replicas

            logging.info(
                "action: broadcast_cleanup_to_joiners | result: broadcasting | client_id: %s | input_table: %s | tables: %d | replicas: %d | total_queues: %d | base_trace: %s",
                client_id,
                input_table,
                len(tables),
                self._jr_replicas,
                total_queues,
                base_trace,
            )

            # Send to all queues (all tables) for all joiner router replicas
            # Make trace unique per (input_table, output_table) combination
            # Format: {filter_router_id}:{aggregator_id}:{input_table}:{output_table}
            for table in tables:
                # Create unique trace per output table by appending output table name
                cleanup_msg.trace = f"{base_trace}:{table}"
                envelope.clean_up.CopyFrom(cleanup_msg)
                updated_raw = envelope.SerializeToString()
                
                for replica in range(self._jr_replicas):
                    queue = self._out_queues.get((table, replica))
                    if not queue:
                        logging.error(
                            "No out queue configured for table=%s, replica=%d",
                            table,
                            replica,
                        )
                        continue
                    queue.send(updated_raw)
                    logging.debug(
                        "Forwarded CLEAN_UP_MESSAGE to joiner_router input_table=%s output_table=%s replica=%d client_id=%s trace=%s",
                        input_table,
                        table,
                        replica,
                        client_id,
                        cleanup_msg.trace,
                    )
        except Exception:
            logging.exception("Failed to broadcast cleanup to joiners")
            raise

    def run(self):
        """Start the aggregator server."""
        self.running = True
        logging.info("Starting aggregator server with ID %s", self.id)

        self._exchanges["menu_items"].start_consuming(self._handle_menu_item)
        self._exchanges["stores"].start_consuming(self._handle_store)
        self._exchanges["transactions"].start_consuming(self._handle_transaction)
        self._exchanges["transaction_items"].start_consuming(
            self._handle_transaction_item
        )
        self._exchanges["users"].start_consuming(self._handle_user)

        logging.info("Started aggregator server")

    def stop(self):
        """Stop the aggregator server."""
        self.running = False
        logging.info("Stopping aggregator server")

        for exchange in self._exchanges.values():
            exchange.stop_consuming()

        for queue in self._out_queues.values():
            queue.close()

        logging.info("Aggregator server stopped")

    def _handle_menu_item(
        self, message: bytes, channel=None, delivery_tag=None, redelivered=None
    ) -> bool:
        logging.debug("Handling menu item message")
        try:
            if not message:
                if channel and delivery_tag is not None:
                    channel.basic_nack(delivery_tag=delivery_tag, requeue=True)
                return False
            envelope = Envelope()
            envelope.ParseFromString(message)
            if envelope.type == MessageType.EOF_MESSAGE:
                eof_msg = envelope.eof
                table_name = TID_TO_NAME[eof_msg.table]
                self._forward_eof(message, table_name)
            elif envelope.type == MessageType.DATA_BATCH:
                data_batch = envelope.data_batch
                if self._is_duplicate_batch(data_batch):
                    if channel and delivery_tag is not None:
                        channel.basic_ack(delivery_tag=delivery_tag)
                    return False
                table_name = TID_TO_NAME[data_batch.payload.name]
                self._forward_databatch_by_table(data_batch, message, table_name)
            elif envelope.type == MessageType.CLEAN_UP_MESSAGE:
                self._broadcast_cleanup_to_joiners(message, "menu_items")
            else:
                logging.warning("Unknown message type: %s", envelope.type)

            if channel and delivery_tag is not None:
                channel.basic_ack(delivery_tag=delivery_tag)
                logging.debug(
                    f"Manually ACKed menu item message, delivery_tag: {delivery_tag}"
                )
            return False
        except Exception:
            logging.exception("Failed to handle menu item")
            if channel and delivery_tag is not None:
                try:
                    channel.basic_nack(delivery_tag=delivery_tag, requeue=True)
                except Exception:
                    logging.exception("Failed to NACK menu item message")
            return False

    def _handle_store(
        self, message: bytes, channel=None, delivery_tag=None, redelivered=None
    ) -> bool:
        logging.debug("Handling store message")
        try:
            if not message:
                if channel and delivery_tag is not None:
                    channel.basic_nack(delivery_tag=delivery_tag, requeue=True)
                return False
            envelope = Envelope()
            envelope.ParseFromString(message)
            if envelope.type == MessageType.EOF_MESSAGE:
                eof_msg = envelope.eof
                table_name = TID_TO_NAME[eof_msg.table]
                self._forward_eof(message, table_name)
            elif envelope.type == MessageType.DATA_BATCH:
                data_batch = envelope.data_batch
                if self._is_duplicate_batch(data_batch):
                    if channel and delivery_tag is not None:
                        channel.basic_ack(delivery_tag=delivery_tag)
                    return False
                table_name = TID_TO_NAME[data_batch.payload.name]
                self._forward_databatch_by_table(data_batch, message, table_name)
            elif envelope.type == MessageType.CLEAN_UP_MESSAGE:
                self._broadcast_cleanup_to_joiners(message, "stores")
            else:
                logging.warning("Unknown message type: %s", envelope.type)

            if channel and delivery_tag is not None:
                channel.basic_ack(delivery_tag=delivery_tag)
                logging.debug(
                    f"Manually ACKed store message, delivery_tag: {delivery_tag}"
                )
            return False
        except Exception:
            logging.exception("Failed to handle store")
            if channel and delivery_tag is not None:
                try:
                    channel.basic_nack(delivery_tag=delivery_tag, requeue=True)
                except Exception:
                    logging.exception("Failed to NACK store message")
            return False

    def _handle_transaction(
        self, message: bytes, channel=None, delivery_tag=None, redelivered=None
    ):
        logging.debug("Handling transaction message")
        try:
            if not message:
                if channel and delivery_tag is not None:
                    channel.basic_nack(delivery_tag=delivery_tag, requeue=True)
                return False

            envelope = Envelope()
            envelope.ParseFromString(message)

            if envelope.type == MessageType.EOF_MESSAGE:
                eof_msg = envelope.eof
                table_name = TID_TO_NAME[eof_msg.table]
                self._forward_eof(message, table_name)
            elif envelope.type == MessageType.DATA_BATCH:
                data_batch = envelope.data_batch
                if self._is_duplicate_batch(data_batch):
                    if channel and delivery_tag is not None:
                        channel.basic_ack(delivery_tag=delivery_tag)
                    return False
                table_data = data_batch.payload
                rows = list(iterate_rows_as_dicts(table_data))
                query_id = data_batch.query_ids[0] if data_batch.query_ids else None

                if query_id == 0 or query_id is None:
                    self._forward_databatch_by_table(
                        data_batch, message, "transactions"
                    )
                elif query_id == 2:
                    processed = process_query_3(rows)
                    out = serialize_query3_results(processed)
                    columns = [
                        "transaction_id",
                        "store_id",
                        "payment_method_id",
                        "user_id",
                        "original_amount",
                        "discount_applied",
                        "final_amount",
                        "created_at",
                    ]
                    row_values = [
                        [str(row.get(col, "")) for col in columns] for row in out
                    ]
                    new_table_data = build_table_data(
                        table_name=TableName.TRANSACTIONS,
                        columns=columns,
                        rows=row_values,
                        batch_number=table_data.batch_number,
                        status=table_data.status,
                    )
                    new_data_batch = DataBatch()
                    new_data_batch.CopyFrom(data_batch)
                    new_data_batch.payload.CopyFrom(new_table_data)
                    new_envelope = Envelope(
                        type=MessageType.DATA_BATCH, data_batch=new_data_batch
                    )
                    new_message = new_envelope.SerializeToString()
                    self._forward_databatch_by_table(
                        new_data_batch, new_message, "transactions"
                    )
                elif query_id == 3:
                    processed = process_query_4_transactions(rows)
                    out = serialize_query4_transaction_results(processed)
                    columns = [
                        "transaction_id",
                        "store_id",
                        "payment_method_id",
                        "voucher_id",
                        "user_id",
                        "original_amount",
                        "discount_applied",
                        "final_amount",
                        "created_at",
                    ]
                    row_values = [
                        [str(row.get(col, "")) for col in columns] for row in out
                    ]
                    new_table_data = build_table_data(
                        table_name=TableName.TRANSACTIONS,
                        columns=columns,
                        rows=row_values,
                        batch_number=table_data.batch_number,
                        status=table_data.status,
                    )
                    new_data_batch = DataBatch()
                    new_data_batch.CopyFrom(data_batch)
                    new_data_batch.payload.CopyFrom(new_table_data)
                    new_envelope = Envelope(
                        type=MessageType.DATA_BATCH, data_batch=new_data_batch
                    )
                    new_message = new_envelope.SerializeToString()
                    self._forward_databatch_by_table(
                        new_data_batch, new_message, "transactions"
                    )
                else:
                    logging.error(
                        "Unexpected query_id=%s for transaction table", query_id
                    )
                    return False
            elif envelope.type == MessageType.CLEAN_UP_MESSAGE:
                self._broadcast_cleanup_to_joiners(message, "transactions")
            else:
                logging.warning("Unknown message type: %s", envelope.type)

            if channel and delivery_tag is not None:
                channel.basic_ack(delivery_tag=delivery_tag)
                logging.debug(
                    f"Manually ACKed transaction message, delivery_tag: {delivery_tag}"
                )
            return False
        except Exception:
            logging.exception("Failed to handle transaction")
            if channel and delivery_tag is not None:
                try:
                    channel.basic_nack(delivery_tag=delivery_tag, requeue=True)
                except Exception:
                    logging.exception("Failed to NACK transaction message")
            return False

    def _handle_transaction_item(
        self, message: bytes, channel=None, delivery_tag=None, redelivered=None
    ):
        logging.debug("Handling transaction item message")
        try:
            if not message:
                return False

            envelope = Envelope()
            envelope.ParseFromString(message)

            if envelope.type == MessageType.EOF_MESSAGE:
                eof_msg = envelope.eof
                table_name = TID_TO_NAME[eof_msg.table]
                self._forward_eof(message, table_name)

            elif envelope.type == MessageType.DATA_BATCH:
                data_batch = envelope.data_batch
                if self._is_duplicate_batch(data_batch):
                    if channel and delivery_tag is not None:
                        channel.basic_ack(delivery_tag=delivery_tag)
                    return False
                table_data = data_batch.payload
                rows = list(iterate_rows_as_dicts(table_data))
                query_id = data_batch.query_ids[0] if data_batch.query_ids else None

                if int(query_id) == 1:
                    logging.debug("Processing transaction item with query 2")
                    processed = process_query_2(rows)
                    out = serialize_query2_results(processed)
                    # TODO: Convert out to protobuf
                    columns = [
                        "transaction_item_id",
                        "transaction_id",
                        "item_id",
                        "quantity",
                        "subtotal",
                        "created_at",
                    ]
                    row_values = [
                        [str(row.get(col, "")) for col in columns] for row in out
                    ]
                    status = table_data.status
                    new_table_data = build_table_data(
                        table_name=TableName.TRANSACTION_ITEMS,
                        columns=columns,
                        rows=row_values,
                        batch_number=table_data.batch_number,
                        status=status,
                    )
                    new_data_batch = DataBatch(
                        query_ids=data_batch.query_ids,
                        filter_steps=data_batch.filter_steps,
                        shards_info=data_batch.shards_info,
                        client_id=data_batch.client_id,
                        payload=new_table_data,
                    )
                    new_envelope = Envelope(
                        type=MessageType.DATA_BATCH, data_batch=new_data_batch
                    )
                    new_message = new_envelope.SerializeToString()
                    self._forward_databatch_by_table(
                        new_data_batch, new_message, "transaction_items"
                    )
                else:
                    logging.error(
                        "Transaction item with query distinct from 1: %s", query_id
                    )
                    return False
            elif envelope.type == MessageType.CLEAN_UP_MESSAGE:
                self._broadcast_cleanup_to_joiners(message, "transaction_items")
            else:
                logging.warning("Unknown message type: %s", envelope.type)

            if channel and delivery_tag is not None:
                channel.basic_ack(delivery_tag=delivery_tag)
                logging.debug(
                    f"Manually ACKed transaction item message, delivery_tag: {delivery_tag}"
                )
            return False
        except Exception:
            logging.exception("Failed to handle transaction item")
            if channel and delivery_tag is not None:
                try:
                    channel.basic_nack(delivery_tag=delivery_tag, requeue=True)
                except Exception:
                    logging.exception("Failed to NACK transaction item message")
            return False

    def _handle_user(
        self, message: bytes, channel=None, delivery_tag=None, redelivered=None
    ) -> bool:
        logging.debug("Handling user message")
        try:
            if not message:
                logging.error("Empty message received in user handler")
                if channel and delivery_tag is not None:
                    channel.basic_nack(delivery_tag=delivery_tag, requeue=True)
                return False
            envelope = Envelope()
            envelope.ParseFromString(message)
            if envelope.type == MessageType.EOF_MESSAGE:
                eof_msg = envelope.eof
                table_name = TID_TO_NAME[eof_msg.table]
                self._forward_eof(message, table_name)
            elif envelope.type == MessageType.DATA_BATCH:
                data_batch = envelope.data_batch
                if self._is_duplicate_batch(data_batch):
                    if channel and delivery_tag is not None:
                        channel.basic_ack(delivery_tag=delivery_tag)
                    return False
                table_name = TID_TO_NAME[data_batch.payload.name]
                self._forward_databatch_by_table(data_batch, message, table_name)
            elif envelope.type == MessageType.CLEAN_UP_MESSAGE:
                self._broadcast_cleanup_to_joiners(message, "users")
            else:
                logging.warning("Unknown message type: %s", envelope.type)

            if channel and delivery_tag is not None:
                channel.basic_ack(delivery_tag=delivery_tag)
                logging.debug(
                    f"Manually ACKed user message, delivery_tag: {delivery_tag}"
                )
            return False
        except Exception:
            logging.exception("Failed to handle user")
            if channel and delivery_tag is not None:
                try:
                    channel.basic_nack(delivery_tag=delivery_tag, requeue=True)
                except Exception:
                    logging.exception("Failed to NACK user message")
            return False
