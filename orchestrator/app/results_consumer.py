"""
Results consumer that handles query results from the results-finisher component.
"""

import logging
import threading
import time
from middleware.middleware_client import MessageMiddlewareQueue, MessageMiddlewareDisconnectedError
from protocol import DataBatch, Opcodes
from protocol.messages import QueryResult1, QueryResult2, QueryResult3, QueryResult4, QueryResultError


class ResultsConsumer:
    """Consumes result messages from the results-finisher component and forwards them to clients."""

    
    def __init__(self, queue_name="orchestrator_results_queue", host="rabbitmq"):
        """Initialize the results consumer.
        
        Args:
            queue_name: The name of the queue to consume from
            host: RabbitMQ host address
        """
        self.queue_name = queue_name
        self.host = host
        self.client_connections = {}  # Map of query_id -> client_connection
        self.middleware = None
        self.stopping = False
        self.client_lock = threading.Lock()
        
    def start(self):
        """Start consuming results from the queue."""
        try:
            self.middleware = MessageMiddlewareQueue(host=self.host, queue_name=self.queue_name)
            self.middleware.start_consuming(self._process_result)
            logging.info(f"action: results_consumer_start | result: success | queue: {self.queue_name}")
        except MessageMiddlewareDisconnectedError as e:
            logging.error(f"action: results_consumer_start | result: connection_fail | error: {e}")
        except Exception as e:
            logging.error(f"action: results_consumer_start | result: fail | error: {e}")
    
    def stop(self):
        """Stop the consumer."""
        self.stopping = True
        if self.middleware:
            try:
                self.middleware.stop_consuming()
                self.middleware.close()
                logging.info("action: results_consumer_stop | result: success")
            except MessageMiddlewareDisconnectedError as e:
                logging.warning(f"action: results_consumer_stop | result: already_disconnected | error: {e}")
            except Exception as e:
                logging.error(f"action: results_consumer_stop | result: fail | error: {e}")
    
    def register_client_for_query(self, query_id, client_connection):
        """Register a client connection to receive results for a specific query.
        
        Args:
            query_id: The query ID to associate with this client
            client_connection: The client socket or connection object
        """
        with self.client_lock:
            self.client_connections[query_id] = client_connection
            logging.info(f"action: client_registered | result: success | query_id: {query_id}")
    
    def unregister_client_for_query(self, query_id):
        """Unregister a client from receiving results for a query.
        
        Args:
            query_id: The query ID to unregister
        """
        with self.client_lock:
            if query_id in self.client_connections:
                del self.client_connections[query_id]
                logging.info(f"action: client_unregistered | result: success | query_id: {query_id}")
    
    def _process_result(self, body):
        """Process a result message from the queue.
        
        Args:
            body: Raw message bytes from the queue
        """
        try:
            # Parse the DataBatch wrapper
            if not body or body[0] != Opcodes.DATA_BATCH:
                logging.warning("action: process_result | result: unknown_format | error: Not a DataBatch")
                return
                
            data_batch = DataBatch.deserialize_from_bytes(body)
            
            if not data_batch.query_ids:
                logging.warning("action: process_result | result: missing_query_id | error: No query_id in batch")
                return
                
            query_id = str(data_batch.query_ids[0])
            
            # Log the receipt of the result
            result_type = "unknown"
            if data_batch.table_ids:
                result_type = f"opcode_{data_batch.table_ids[0]}"
                
            logging.info(f"action: result_received | result: success | query_id: {query_id} | type: {result_type}")
            
            # Forward the result to the client
            self._forward_result_to_client(query_id, body)
            
        except Exception as e:
            logging.error(f"action: process_result | result: fail | error: {e}")
    
    def _forward_result_to_client(self, query_id, result_bytes):
        """Forward the result to the appropriate client.
        
        Args:
            query_id: The query ID to find the associated client
            result_bytes: The raw result bytes to forward
        """
        client_conn = None
        with self.client_lock:
            client_conn = self.client_connections.get(query_id)
            
        if not client_conn:
            logging.warning(f"action: forward_result | result: no_client | query_id: {query_id}")
            return
            
        try:
            # Send the raw bytes to the client
            client_conn.sendall(result_bytes)
            logging.info(f"action: forward_result | result: success | query_id: {query_id}")
            
            # After forwarding the result, unregister the client
            self.unregister_client_for_query(query_id)
            
        except Exception as e:
            logging.error(f"action: forward_result | result: fail | query_id: {query_id} | error: {e}")
            # Clean up the connection on error
            self.unregister_client_for_query(query_id)
