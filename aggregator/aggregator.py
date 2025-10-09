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
from queryid import QueryId

from app_config.config_loader import Config
from middleware.middleware_client import (
    MessageMiddlewareExchange,
    MessageMiddlewareQueue,
)
from protocol.constants import BatchStatus, Opcodes
from protocol.databatch import DataBatch
from protocol.messages import EOFMessage

# Para transactions y query 1: re-enviar
# Para transaction_items y query 2: procesamiento en esta instancia
# Para transactions y query 3: procesamiento en esta instancia
# Para transactions y query 4: procesamiento en esta instancia

TID_TO_NAME = {
    Opcodes.NEW_MENU_ITEMS: "menu_items",
    Opcodes.NEW_STORES: "stores",
    Opcodes.NEW_TRANSACTION: "transactions",
    Opcodes.NEW_TRANSACTION_ITEMS: "transaction_items",
    Opcodes.NEW_USERS: "users",
}


class ExchangePublisherPool:
    def __init__(self, host):
        self._host = host
        self._pool = {}
        logging.info(f"Created exchange publisher pool for host: {host}")

    def get_exchange(self, exchange_name, route_keys):
        key = (
            exchange_name,
            tuple(route_keys) if isinstance(route_keys, list) else (route_keys,),
        )
        if key not in self._pool:
            logging.info(
                f"Creating new exchange in pool: {exchange_name} with keys {route_keys}"
            )
            self._pool[key] = MessageMiddlewareExchange(
                host=self._host, exchange_name=exchange_name, route_keys=route_keys
            )
        else:
            logging.debug(f"Reusing existing exchange from pool: {exchange_name}")
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
        logging.info(f"Initialized exchange pool for aggregator {id}")

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
                f"Created exchange connection for {table}: {exchange_name} -> {routing_key} with queue {queue_name}"
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
        replica = random.randint(0, self._jr_replicas - 1)
        q = self._out_queues.get((table, replica))
        if not q:
            logging.error("No out queue configured for table=%s", table)
            return
        q.send(raw_bytes)

    def _forward_databatch_by_opcode(self, raw: bytes, opcode: int):
        """Reenvía DataBatch a la cola correcta usando el opcode provisto."""
        try:
            table = TID_TO_NAME.get(opcode)
            logging.info(
                "Forwarding DataBatch with opcode=%s to table=%s", opcode, table
            )
            if not table:
                logging.error("Unknown opcode=%s", opcode)
                return

            self._send_to_joiner_by_table(table, raw)
        except Exception:
            logging.exception(
                "Failed to forward databatch by opcode. Opcode: %s", opcode
            )
            return

    def _forward_eof(self, raw: bytes, opcode: int):
        """Detecta tabla desde EOFMessage y reenvía a la cola correcta."""
        table = TID_TO_NAME.get(opcode)
        if not table:
            logging.error("Unknown opcode in EOF message: %s", opcode)
            return
        logging.info("Forwarding EOF for table=%s", table)
        for replica in range(0, self._jr_replicas):
            q = self._out_queues.get((table, replica))
            q.send(raw)

    def run(self):
        """Start the aggregator server."""
        self.running = True
        logging.info(f"Starting aggregator server with ID {self.id}")

        self._exchanges["menu_items"].start_consuming(self._handle_menu_item)
        self._exchanges["stores"].start_consuming(self._handle_store)
        self._exchanges["transactions"].start_consuming(self._handle_transaction)
        self._exchanges["transaction_items"].start_consuming(
            self._handle_transaction_item
        )
        self._exchanges["users"].start_consuming(self._handle_user)

        logging.debug("Started aggregator server")

    def stop(self):
        """Stop the aggregator server."""
        self.running = False
        logging.debug("Stopping aggregator server")

        for exchange in self._exchanges.values():
            exchange.stop_consuming()

        for queue in self._out_queues:
            queue.close()

        logging.info("Aggregator server stopped")

    def _handle_menu_item(self, message: bytes) -> bool:
        try:
            if not message:
                return False
            if message[0] == Opcodes.EOF:
                self._forward_eof(message, Opcodes.NEW_MENU_ITEMS)
            else:
                self._forward_databatch_by_opcode(message, Opcodes.NEW_MENU_ITEMS)
            return True
        except Exception:
            logging.exception("Failed to handle menu item")
            return False

    def _handle_store(self, message: bytes) -> bool:
        try:
            if not message:
                return False
            if message[0] == Opcodes.EOF:
                self._forward_eof(message, Opcodes.NEW_STORES)
            else:
                self._forward_databatch_by_opcode(message, Opcodes.NEW_STORES)
            return True
        except Exception:
            logging.exception("Failed to handle store")
            return False

    def _handle_transaction(self, message: bytes):
        try:
            if not message:
                return False
            if message[0] == Opcodes.EOF:
                self._forward_eof(message, Opcodes.NEW_TRANSACTION)
                return True

            db = DataBatch.deserialize_from_bytes(message)
            rows = db.batch_msg.rows or []
            query_id = db.query_ids[0] if db.query_ids else None

            if query_id == 1 or query_id is None:
                self._forward_databatch_by_opcode(message, Opcodes.NEW_TRANSACTION)
            elif query_id == 3:
                processed = process_query_3(rows)
                out = serialize_query3_results(processed)
                db.batch_msg.rows = out
                db.batch_bytes = db.batch_msg.to_bytes()
                db.batch_msg.amount = len(out)
                databatch = db.to_bytes()
                self._forward_databatch_by_opcode(databatch, Opcodes.NEW_TRANSACTION)
            elif query_id == 4:
                processed = process_query_4_transactions(rows)
                out = serialize_query4_transaction_results(processed)
                db.batch_msg.rows = out
                db.batch_bytes = db.batch_msg.to_bytes()
                db.batch_msg.amount = len(out)
                databatch = db.to_bytes()
                self._forward_databatch_by_opcode(databatch, Opcodes.NEW_TRANSACTION)
            else:
                logging.error("Unexpected query_id=%s for transaction table", query_id)
                return False
            return True
        except Exception:
            logging.exception("Failed to handle transaction")
            return False

    def _handle_transaction_item(self, message: bytes):
        try:
            if not message:
                return False
            if message[0] == Opcodes.EOF:
                self._forward_eof(message, Opcodes.NEW_TRANSACTION_ITEMS)
                return True

            db = DataBatch.deserialize_from_bytes(message)
            rows = db.batch_msg.rows or []
            query_id = db.query_ids[0] if db.query_ids else None

            if int(query_id) == QueryId.SECOND_QUERY:
                logging.info("Processing transaction item with query 2")
                processed = process_query_2(rows)
                out = serialize_query2_results(processed)
                db.batch_msg.rows = out
                db.batch_bytes = db.batch_msg.to_bytes()
                db.batch_msg.amount = len(out)
                databatch = db.to_bytes()
                self._forward_databatch_by_opcode(
                    databatch, Opcodes.NEW_TRANSACTION_ITEMS
                )
            else:
                logging.error(
                    "Transaction item with query distinct from 2: %s", query_id
                )
                return False
            return True
        except Exception:
            logging.exception("Failed to handle transaction item")
            return False

    def _handle_user(self, message: bytes):
        try:
            if not message:
                logging.error("Empty message received in user handler")
                return False
            if message[0] == Opcodes.EOF:
                self._forward_eof(message, Opcodes.NEW_USERS)
            else:
                self._forward_databatch_by_opcode(message, Opcodes.NEW_USERS)
            return True
        except Exception:
            logging.exception("Failed to handle user")
            return False
