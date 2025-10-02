#!/usr/bin/env python3
"""
Coffee Shop Data Aggregator

This module implements the main aggregator functionality for collecting,
processing and analyzing coffee shop data from multiple clients.
"""
import logging
import os

from aggregator.mock import mock_process
from aggregator.processing import process_query_2, process_query_3, process_query_4_transactions, serialize_query2_results, serialize_query3_results, serialize_query4_transaction_results
from aggregator.queryid import QueryId
from protocol.constants import Opcodes
from protocol.databatch import DataBatch
from protocol.messages import EOFMessage
from app_config.config_loader import Config
from middleware.middleware_client import MessageMiddlewareExchange, MessageMiddlewareQueue

# Para transactions y query 1: re-enviar
# Para transaction_items y query 2: procesamiento en esta instancia
# Para transactions y query 3: procesamiento en esta instancia
# Para transactions y query 4: procesamiento en esta instancia

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
        
        # Setup input exchanges for different data types
        tables = ["menu_items", "stores", "transactions", "transaction_items", "users"]
        self._exchanges = {}
        
        # Create exchange connections for each table based on the aggregator ID
        for table in tables:
            exchange_name = self.config.filter_router_exchange(table)
            routing_key = self.config.filter_router_rk(table, self.id)
            self._exchanges[table] = MessageMiddlewareExchange(
                host=self.host,
                exchange_name=exchange_name,
                route_keys=[routing_key]
            )
            logging.debug(f"Created exchange connection for {table}: {exchange_name} -> {routing_key}")
            
        # Setup the joiner output queue
        self.joiner_queue = MessageMiddlewareQueue(
            host=self.host,
            queue_name=self.config.aggregator_to_joiner_router_queue(tables[0], self.id)
        )
    
    def run(self):
        """Start the aggregator server."""
        self.running = True
        logging.info(f"Starting aggregator server with ID {self.id}")
        
        # Start consuming from each input queue
        self._exchanges["menu_items"].start_consuming(self._handle_menu_item)
        self._exchanges["stores"].start_consuming(self._handle_store)
        self._exchanges["transactions"].start_consuming(self._handle_transaction)
        self._exchanges["transaction_items"].start_consuming(self._handle_transaction_item)
        self._exchanges["users"].start_consuming(self._handle_user)
        
        logging.debug("Started aggregator server")

    def stop(self):
        """Stop the aggregator server."""
        self.running = False
        logging.debug("Stopping aggregator server")
        
        # Stop consuming from all queues
        for exchange in self._exchanges.values():
            exchange.stop_consuming()
        
        # Close all connections
        for exchange in self._exchanges.values():
            exchange.close()
        
        self.joiner_queue.close()
    
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
            logging.debug(f"Transaction belongs to table {table_id} and query {query_id}")

            if table_id == Opcodes.NEW_TRANSACTION:
                if query_id == QueryId.FIRST_QUERY or query_id == QueryId.EOF:
                    self.joiner_queue.send(message)
                elif query_id == QueryId.THIRD_QUERY:
                    processed_data = process_query_3(transactions)
                    serialized_data = serialize_query3_results(processed_data)
                    self.joiner_queue.send(serialized_data)
                elif query_id == QueryId.FOURTH_QUERY:
                    processed_data = process_query_4_transactions(transactions)
                    serialized_data = serialize_query4_transaction_results(processed_data)
                    self.joiner_queue.send(serialized_data)
                else:
                    logging.error(f"Transaction message with unexpected query_id {query_id}")
            else:
                logging.error(f"Transaction message with unexpected table_id {table_id}")        
        except:
            logging.error(f"Failed to decode transaction item message")
    
    def _handle_transaction_item(self, message: bytes):
        """Process incoming transaction item messages."""
        try:
            logging.info(f"Received transaction item")
            transaction_items_databatch = DataBatch.deserialize_from_bytes(message)
            transaction_items = transaction_items_databatch.batch_msg.rows
            query_id = transaction_items_databatch.query_ids[0]
            table_id = transaction_items_databatch.table_ids[0]
            
            if table_id == Opcodes.NEW_TRANSACTION_ITEMS and query_id == QueryId.SECOND_QUERY:
                logging.debug(f"Transaction belongs to table {table_id} and query {query_id}")
                processed_data = process_query_2(transaction_items)
                serialized_data = serialize_query2_results(processed_data)
                self.joiner_queue.send(serialized_data)
            elif table_id == Opcodes.EOF:
                logging.debug(f"EOF message received for transaction items.")
                self.joiner_queue.send(message)
            else:
                logging.error(f"Transaction item message with unexpected table_id {table_id} or query_id {query_id}")            
        except:
            logging.error(f"Failed to decode transaction item message")
    
    def _handle_user(self, message: bytes):
        """Process incoming user messages."""
        try:
            logging.info(f"Received user.  Passing to joiner.")
            self.joiner_queue.send(message)
            return True
        except:
            logging.error(f"Failed to decode user message")
            return False
    