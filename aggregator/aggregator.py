#!/usr/bin/env python3
"""
Coffee Shop Data Aggregator

This module implements the main aggregator functionality for collecting,
processing and analyzing coffee shop data from multiple clients.
"""
import logging
import os
import random

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
from protocol2.envelope_pb2 import Envelope, MessageType
from protocol2.databatch_pb2 import DataBatch
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


class ExchangePublisherPool:
    def __init__(self, host):
        self._host = host
        self._pool = {}
        logging.info("Created exchange publisher pool for host: %s", host)

    def get_exchange(self, exchange_name, route_keys):
        key = (
            exchange_name,
            tuple(route_keys) if isinstance(route_keys, list) else (route_keys,),
        )
        if key not in self._pool:
            logging.info(
                "Creating new exchange in pool: %s with keys %s",
                exchange_name,
                route_keys,
            )
            self._pool[key] = MessageMiddlewareExchange(
                host=self._host, exchange_name=exchange_name, route_keys=route_keys
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
                exchange_name=exchange_name, route_keys=[routing_key]
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

    def _send_to_joiner_by_table(self, table: str, raw_bytes: bytes):
        logging.debug("Sending to joiner by table=%s", table)
        replica = random.randint(0, self._jr_replicas - 1)
        q = self._out_queues.get((table, replica))
        if not q:
            logging.error("No out queue configured for table=%s", table)
            return
        q.send(raw_bytes)

    def _forward_databatch_by_table(self, raw: bytes, table_name: str):
        logging.debug("Forwarding DataBatch message")
        """Reenvía DataBatch a la cola correcta usando el table_name provisto."""
        try:
            table = table_name
            logging.debug(
                "Forwarding DataBatch to table=%s", table
            )
            if not table:
                logging.error("Unknown table_name=%s", table_name)
                return

            self._send_to_joiner_by_table(table, raw)
        except Exception:
            logging.exception(
                "Failed to forward databatch by table. Table: %s", table_name
            )
            return

    def _forward_eof(self, raw: bytes, table_name: str):
        logging.debug("Forwarding EOF message")
        """Detecta tabla desde EOFMessage y reenvía a la cola correcta."""
        table = table_name
        logging.debug("Forwarding EOF for table=%s", table)
        for replica in range(0, self._jr_replicas):
            q = self._out_queues.get((table, replica))
            q.send(raw)

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

    def _handle_menu_item(self, message: bytes) -> bool:
        logging.debug("Handling menu item message")
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
                table_name = TID_TO_NAME[data_batch.payload.name]
                self._forward_databatch_by_table(message, table_name)
            else:
                logging.warning("Unknown message type: %s", envelope.type)
            return True
        except Exception:
            logging.exception("Failed to handle menu item")
            return False

    def _handle_store(self, message: bytes) -> bool:
        logging.debug("Handling store message")
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
                table_name = TID_TO_NAME[data_batch.payload.name]
                self._forward_databatch_by_table(message, table_name)
            else:
                logging.warning("Unknown message type: %s", envelope.type)
            return True
        except Exception:
            logging.exception("Failed to handle store")
            return False

    def _handle_transaction(self, message: bytes):
        logging.debug("Handling transaction message")
        try:
            if not message:
                return False
    
            envelope = Envelope()
            envelope.ParseFromString(message)

            if envelope.type == MessageType.EOF_MESSAGE:
                eof_msg = envelope.eof
                table_name = TID_TO_NAME[eof_msg.table]
                self._forward_eof(message, table_name)
                return True

            elif envelope.type == MessageType.DATA_BATCH:
                data_batch = envelope.data_batch
                table_data = data_batch.payload
                rows = list(iterate_rows_as_dicts(table_data))
                query_id = data_batch.query_ids[0] if data_batch.query_ids else None

                if query_id == 0 or query_id is None:
                    self._forward_databatch_by_table(message, "transactions")
                elif query_id == 2:
                    processed = process_query_3(rows)
                    out = serialize_query3_results(processed)
                    # TODO: Convert out to new protobuf format
                    # For now, assume out is list of dicts
                    columns = ['transaction_id', 'store_id', 'payment_method_id', 'user_id', 'original_amount', 'discount_applied', 'final_amount', 'created_at']
                    row_values = [[str(row.get(col, '')) for col in columns] for row in out]
                    new_table_data = build_table_data(
                        table_name=TableName.TRANSACTIONS,
                        columns=columns,
                        rows=row_values,
                        batch_number=table_data.batch_number,
                        status=table_data.status
                    )
                    new_data_batch = DataBatch()
                    new_data_batch.CopyFrom(data_batch)
                    new_data_batch.payload.CopyFrom(new_table_data)
                    new_envelope = Envelope(type=MessageType.DATA_BATCH, data_batch=new_data_batch)
                    new_message = new_envelope.SerializeToString()
                    self._forward_databatch_by_table(new_message, "transactions")
                elif query_id == 3:
                    processed = process_query_4_transactions(rows)
                    out = serialize_query4_transaction_results(processed)
                    columns = ['transaction_id', 'store_id', 'payment_method_id', 'voucher_id', 'user_id', 'original_amount', 'discount_applied', 'final_amount', 'created_at']
                    row_values = [[str(row.get(col, '')) for col in columns] for row in out]
                    new_table_data = build_table_data(
                        table_name=TableName.TRANSACTIONS,
                        columns=columns,
                        rows=row_values,
                        batch_number=table_data.batch_number,
                        status=table_data.status
                    )
                    new_data_batch = DataBatch()
                    new_data_batch.CopyFrom(data_batch)
                    new_data_batch.payload.CopyFrom(new_table_data)
                    new_envelope = Envelope(type=MessageType.DATA_BATCH, data_batch=new_data_batch)
                    new_message = new_envelope.SerializeToString()
                    self._forward_databatch_by_table(new_message, "transactions")
                else:
                    logging.error("Unexpected query_id=%s for transaction table", query_id)
                    return False
            else:
                logging.warning("Unknown message type: %s", envelope.type)
            return True
        except Exception:
            logging.exception("Failed to handle transaction")
            return False

    def _handle_transaction_item(self, message: bytes):
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
                return True

            elif envelope.type == MessageType.DATA_BATCH:
                data_batch = envelope.data_batch
                table_data = data_batch.payload
                rows = list(iterate_rows_as_dicts(table_data))
                query_id = data_batch.query_ids[0] if data_batch.query_ids else None

                if int(query_id) == 1:
                    logging.debug("Processing transaction item with query 2")
                    processed = process_query_2(rows)
                    out = serialize_query2_results(processed)
                    # TODO: Convert out to protobuf
                    columns = ['transaction_item_id', 'transaction_id', 'item_id', 'quantity', 'subtotal', 'created_at']
                    row_values = [[str(row.get(col, '')) for col in columns] for row in out]
                    status = table_data.status
                    new_table_data = build_table_data(
                        table_name=TableName.TRANSACTION_ITEMS,
                        columns=columns,
                        rows=row_values,
                        batch_number=table_data.batch_number,
                        status=status
                    )
                    new_data_batch = DataBatch(
                        query_ids=data_batch.query_ids,
                        filter_steps=data_batch.filter_steps,
                        shards_info=data_batch.shards_info,
                        client_id=data_batch.client_id,
                        payload=new_table_data
                    )
                    new_envelope = Envelope(type=MessageType.DATA_BATCH, data_batch=new_data_batch)
                    new_message = new_envelope.SerializeToString()
                    self._forward_databatch_by_table(new_message, "transaction_items")
                else:
                    logging.error(
                        "Transaction item with query distinct from 1: %s", query_id
                    )
                    return False
            else:
                logging.warning("Unknown message type: %s", envelope.type)
            return True
        except Exception:
            logging.exception("Failed to handle transaction item")
            return False

    def _handle_user(self, message: bytes) -> bool:
        logging.debug("Handling user message")
        try:
            if not message:
                logging.error("Empty message received in user handler")
                return False
            envelope = Envelope()
            envelope.ParseFromString(message)
            if envelope.type == MessageType.EOF_MESSAGE:
                eof_msg = envelope.eof
                table_name = TID_TO_NAME[eof_msg.table]
                self._forward_eof(message, table_name)
            elif envelope.type == MessageType.DATA_BATCH:
                data_batch = envelope.data_batch
                table_name = TID_TO_NAME[data_batch.payload.name]
                self._forward_databatch_by_table(message, table_name)
            else:
                logging.warning("Unknown message type: %s", envelope.type)
            return True
        except Exception:
            logging.exception("Failed to handle user")
            return False
