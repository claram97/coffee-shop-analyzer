#!/usr/bin/env python3
"""
Coffee Shop Data Aggregator

This module implements the main aggregator functionality for collecting,
processing and analyzing coffee shop data from multiple clients.
"""

import socket
import threading
import json
import time
import logging
from typing import Dict, List, Any, Optional
from collections import defaultdict
from datetime import datetime
import configparser
from middleware.middleware_client import MessageMiddlewareExchange, MessageMiddlewareQueue


class Aggregator:
    """Main aggregator class for coffee shop data analysis."""
    
    def __init__(self, id: int):
        """
        Initialize the aggregator.
        
        Args:
          id (int): The unique identifier for the aggregator instance.
        """
        self.id = id
        self.running = False
        
        # Input exchanges for different data types
        self._menu_items_exchange = MessageMiddlewareExchange(
            host="localhost",
            exchange_name="ex.menu_items",
            route_keys=[f"tx.shard.{self.id}"])
        self._stores_exchange = MessageMiddlewareExchange(
            host="localhost",
            exchange_name="ex.stores", 
            route_keys=[f"tx.shard.{self.id}"])
        self._transactions_exchange = MessageMiddlewareExchange(
            host="localhost",
            exchange_name="ex.transactions", 
            route_keys=[f"tx.shard.{self.id}"])
        self._transaction_items_exchange = MessageMiddlewareExchange(
            host="localhost",
            exchange_name="ex.transaction_items", 
            route_keys=[f"tx.shard.{self.id}"])
        self._users_exchange = MessageMiddlewareExchange(
            host="localhost",
            exchange_name="ex.users", 
            route_keys=[f"tx.shard.{self.id}"])

        self.joiner_queue = MessageMiddlewareQueue(
            host="localhost",
            queue_name="aggregator_to_joiner_queue"
        )
    
    def run(self):
        """Start the aggregator server."""
        self.running = True
        self._menu_items_exchange.start_consuming(self._handle_menu_item)
        self._stores_exchange.start_consuming(self._handle_store)
        self._transactions_exchange.start_consuming(self._handle_transaction)
        self._transaction_items_exchange.start_consuming(self._handle_transaction_item)
        self._users_exchange.start_consuming(self._handle_user)
        logging.debug("Starting aggregator server")
    
    def stop(self):
        """Stop the aggregator server."""
        self.running = False
        logging.debug("Stopping aggregator server")
    
    def _handle_menu_item(self, message: bytes) -> bool:
        """Process incoming menu item messages."""
        try:
            data = json.loads(message.decode('utf-8'))
            logging.info(f"Received menu item: {data}")
            # Process menu item data as needed
            self.joiner_queue.send(message)
            return True
        except json.JSONDecodeError as e:
            logging.error(f"Failed to decode menu item message: {e}")
            return False
    
    def _handle_store(self, message: bytes) -> bool:
        """Process incoming store messages."""
        try:
            data = json.loads(message.decode('utf-8'))
            logging.info(f"Received store: {data}")
            # Process store data as needed
            self.joiner_queue.send(message)
            return True
        except json.JSONDecodeError as e:
            logging.error(f"Failed to decode store message: {e}")
            return False
    
    def _handle_transaction(self, message: bytes) -> bool:
        """Process incoming transaction messages."""
        try:
            data = json.loads(message.decode('utf-8'))
            logging.info(f"Received transaction: {data}")
            # Process transaction data as needed
            self.joiner_queue.send(message)
            return True
        except json.JSONDecodeError as e:
            logging.error(f"Failed to decode transaction message: {e}")
            return False
    
    def _handle_transaction_item(self, message: bytes) -> bool:
        """Process incoming transaction item messages."""
        try:
            data = json.loads(message.decode('utf-8'))
            logging.info(f"Received transaction item: {data}")
            # Process transaction item data as needed
            self.joiner_queue.send(message)
            return True
        except json.JSONDecodeError as e:
            logging.error(f"Failed to decode transaction item message: {e}")
            return False
    
    def _handle_user(self, message: bytes) -> bool:
        """Process incoming user messages."""
        try:
            data = json.loads(message.decode('utf-8'))
            logging.info(f"Received user: {data}")
            # Process user data as needed
            self.joiner_queue.send(message)
            return True
        except json.JSONDecodeError as e:
            logging.error(f"Failed to decode user message: {e}")
            return False
        
