#!/usr/bin/env python3
"""
Coffee Shop Data Aggregator

This module implements the main aggregator functionality for collecting,
processing and analyzing coffee shop data from multiple clients.
"""
import logging
import os

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
from protocol.constants import Opcodes
from protocol.databatch import DataBatch

# Para transactions y query 1: re-enviar
# Para transaction_items y query 2: procesamiento en esta instancia
# Para transactions y query 3: procesamiento en esta instancia
# Para transactions y query 4: procesamiento en esta instancia

<<<<<<< HEAD
=======
# Connection pool for sharing connections
class ExchangePublisherPool:
    def __init__(self, host):
        self._host = host
        self._pool = {}
        logging.info(f"Created exchange publisher pool for host: {host}")
        
    def get_exchange(self, exchange_name, route_keys):
        key = (exchange_name, tuple(route_keys) if isinstance(route_keys, list) else (route_keys,))
        if key not in self._pool:
            logging.info(f"Creating new exchange in pool: {exchange_name} with keys {route_keys}")
            self._pool[key] = MessageMiddlewareExchange(
                host=self._host, 
                exchange_name=exchange_name, 
                route_keys=route_keys
            )
        else:
            logging.debug(f"Reusing existing exchange from pool: {exchange_name}")
        return self._pool[key]
>>>>>>> 8238692250d316b554ae8908ced177c3865be5e9

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
<<<<<<< HEAD

=======
        
        # Create a shared connection pool
        self._exchange_pool = ExchangePublisherPool(host=self.host)
        logging.info(f"Initialized exchange pool for aggregator {id}")
        
>>>>>>> 8238692250d316b554ae8908ced177c3865be5e9
        # Setup input exchanges for different data types
        tables = ["menu_items", "stores", "transactions", "transaction_items", "users"]
        self._exchanges = {}

        # Create exchange connections for each table based on the aggregator ID
        for table in tables:
            exchange_name = self.config.filter_router_exchange(table)
            routing_key = self.config.filter_router_rk(table, self.id)
<<<<<<< HEAD
            # Create a stable queue name that identifies this aggregator
            queue_name = self.config.aggregator_queue(table, self.id)
            self._exchanges[table] = MessageMiddlewareExchange(
                host=self.host,
                exchange_name=exchange_name,
                route_keys=[routing_key],
                is_consumer=True,
                queue_name=queue_name,
            )
            logging.info(
                f"Created exchange connection for {table}: {exchange_name} -> {routing_key} with queue {queue_name}"
            )

        # Setup the joiner output queue
=======
            queue_name = self.config.aggregator_queue(table, self.id)

            # Get exchange from the pool instead of creating a new one each time
            self._exchanges[table] = self._exchange_pool.get_exchange(
                exchange_name=exchange_name,
                route_keys=[routing_key]
            )
            logging.info(f"Created exchange connection for {table}: {exchange_name} -> {routing_key} with queue {queue_name}")
            
>>>>>>> 8238692250d316b554ae8908ced177c3865be5e9
        self.joiner_queue = MessageMiddlewareQueue(
            host=self.host,
            queue_name=self.config.aggregator_to_joiner_router_queue(
                tables[0], self.id
            ),
        )

    def run(self):
        """Start the aggregator server."""
        self.running = True
        logging.info(f"Starting aggregator server with ID {self.id}")

        # Start consuming from each input queue
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

        # Stop consuming from all queues
        for exchange in self._exchanges.values():
            exchange.stop_consuming()
<<<<<<< HEAD

        # Close all connections
        for exchange in self._exchanges.values():
            exchange.close()

        self.joiner_queue.close()

=======
        
        # Since we're using a pool, we don't close individual exchanges
        # as they might share connections
        
        # Close the joiner queue
        self.joiner_queue.close()
        
        logging.info("Aggregator server stopped")
    
>>>>>>> 8238692250d316b554ae8908ced177c3865be5e9
    def _handle_menu_item(self, message: bytes) -> bool:
        """Process incoming menu item messages."""
        try:
            logging.info(f"Received menu item.  Passing to joiner.")
            self.joiner_queue.send(message)
            return True
        except:
            logging.error(f"Failed to decode menu item message")
            return False

    def _handle_store(self, message: bytes) -> bool:
        """Process incoming store messages."""
        try:
            logging.info(f"Received store. Passing to joiner.")
            self.joiner_queue.send(message)
            return True
        except:
            logging.error(f"Failed to decode store message")
            return False

    def _handle_transaction(self, message: bytes):
        """Process incoming transaction messages."""
        try:
            logging.info(f"Received transaction")
            transactions_databatch = DataBatch.deserialize_from_bytes(message)
            transactions = transactions_databatch.batch_msg.rows
            query_id = transactions_databatch.query_ids[0]
            table_id = transactions_databatch.table_ids[0]
            logging.debug(
                f"Transaction belongs to table {table_id} and query {query_id}"
            )

            if table_id == Opcodes.NEW_TRANSACTION:
                if query_id == QueryId.FIRST_QUERY or query_id == QueryId.EOF:
                    self.joiner_queue.send(message)
                elif query_id == QueryId.THIRD_QUERY:
                    processed_data = process_query_3(transactions)
                    serialized_data = serialize_query3_results(processed_data)
                    self.joiner_queue.send(serialized_data)
                elif query_id == QueryId.FOURTH_QUERY:
                    processed_data = process_query_4_transactions(transactions)
                    serialized_data = serialize_query4_transaction_results(
                        processed_data
                    )
                    self.joiner_queue.send(serialized_data)
                else:
                    logging.error(
                        f"Transaction message with unexpected query_id {query_id}"
                    )
            else:
                logging.error(
                    f"Transaction message with unexpected table_id {table_id}"
                )
        except:
            logging.error(f"Failed to decode transaction item message")

    def _handle_transaction_item(self, message: bytes):
        """Process incoming transaction item messages."""
        try:
            logging.info(f"Received transaction item")
            transaction_items_databatch = DataBatch.deserialize_from_bytes(message)
            transaction_items = transaction_items_databatch.batch_msg.rows
            query_id = transaction_items_databatch.query_ids[0]
            table_ids = transaction_items_databatch.table_ids

            if (
                Opcodes.NEW_TRANSACTION_ITEMS in table_ids
                and query_id == QueryId.SECOND_QUERY
            ):
                logging.debug(
                    f"Transaction belongs to table {table_id} and query {query_id}"
                )
                processed_data = process_query_2(transaction_items)
                serialized_data = serialize_query2_results(processed_data)
                self.joiner_queue.send(serialized_data)
            elif table_id == Opcodes.EOF:
                logging.debug(f"EOF message received for transaction items.")
                self.joiner_queue.send(message)
            else:
                logging.error(
                    f"Transaction item message with unexpected table_id {table_id} or query_id {query_id}"
                )
        except Exception as e:
            logging.error("Failed to decode transaction item message: %s", e)

    def _handle_user(self, message: bytes):
        """Process incoming user messages."""
        try:
            logging.info(f"Received user.  Passing to joiner.")
            self.joiner_queue.send(message)
            return True
        except:
            logging.error(f"Failed to decode user message")
            return False
