#!/usr/bin/env python3
"""
Coffee Shop Data Aggregator

This module implements the main aggregator functionality for collecting,
processing and analyzing coffee shop data from multiple clients.
"""
import logging

from aggregator.mock import mock_process
from aggregator.processing import process_query_2, process_query_3, process_query_4_transactions, serialize_query2_results, serialize_query3_results, serialize_query4_transaction_results
from aggregator.queryid import QueryId
from protocol.constants import Opcodes
from protocol.databatch import DataBatch


# from middleware.middleware_client import MessageMiddlewareExchange, MessageMiddlewareQueue

# Para transactions y query 1: re-enviar
# Para transaction_items y query 2:
# Para transactions y query 3:
# Para transactions y query 4:

class Aggregator:
    """Main aggregator class for coffee shop data analysis."""
    
    def __init__(self, id: str):
        """
        Initialize the aggregator.
        
        Args:
          id (int): The unique identifier for the aggregator instance.
        """
        self.id = id
        self.running = False
        
    #     # Input exchanges for different data types
    #     if self.id == "00":
    #         self._menu_items_exchange = MessageMiddlewareExchange(
    #             host="localhost",
    #             exchange_name="ex.menu_items",
    #             route_keys=[f"tx.shard.{self.id}"])
    #         self._stores_exchange = MessageMiddlewareExchange(
    #             host="localhost",
    #             exchange_name="ex.stores", 
    #             route_keys=[f"tx.shard.{self.id}"])
    #     self._transactions_exchange = MessageMiddlewareExchange(
    #         host="localhost",
    #         exchange_name="ex.transactions", 
    #         route_keys=[f"tx.shard.{self.id}"])
    #     self._transaction_items_exchange = MessageMiddlewareExchange(
    #         host="localhost",
    #         exchange_name="ex.transaction_items", 
    #         route_keys=[f"tx.shard.{self.id}"])
    #     self._users_exchange = MessageMiddlewareExchange(
    #         host="localhost",
    #         exchange_name="ex.users", 
    #         route_keys=[f"tx.shard.{self.id}"])

    #     self.joiner_queue = MessageMiddlewareQueue(
    #         host="localhost",
    #         queue_name="aggregator_to_joiner_queue"
    #     )
    
    def run(self):
        """Start the aggregator server."""
        self.running = True
        # self._menu_items_exchange.start_consuming(self._handle_menu_item)
        # self._stores_exchange.start_consuming(self._handle_store)
        # self._transactions_exchange.start_consuming(self._handle_transaction)
        # self._transaction_items_exchange.start_consuming(self._handle_transaction_item)
        # self._users_exchange.start_consuming(self._handle_user)
        # logging.debug("Started aggregator server")
        
        # mock_process(self.id)

    def stop(self):
        """Stop the aggregator server."""
        self.running = False
        logging.debug("Stopping aggregator server")
    
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
    