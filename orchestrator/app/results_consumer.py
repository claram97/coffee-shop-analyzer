"""
Results consumer that handles query results from the results-finisher component.
"""

import logging
import threading
import time
from middleware.middleware_client import MessageMiddlewareQueue, MessageMiddlewareDisconnectedError
from protocol import DataBatch, Opcodes, BatchStatus
from protocol.messages import QueryResult1, QueryResult2, QueryResult3, QueryResult4, QueryResultError
from protocol.messages import Finished

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
        self.client_connections = {}
        self.middleware = None
        self.stopping = False
        self.queries_sent = {}  # Dict: client_id -> set of query_ids
        self.client_lock = threading.Lock()

    def set_orchestrator(self, orchestrator):
        """Set the orchestrator instance for the consumer.
        
        Args:
            orchestrator: The orchestrator instance
        """
        self.orchestrator = orchestrator
        logging.info("action: set_orchestrator | result: success")

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

    def register_client(self, client_connection, client_id: str):

        """Register a client connection to receive results for a specific query.
        
        Args:
            client_connection: The client socket or connection object
        """
        with self.client_lock:
            self.client_connections[client_id] = client_connection
            self.queries_sent[client_id] = set()  # Inicializar set por cliente
            logging.info(
                "action: client_registered | result: success | client_id: %s",
                client_id,
            )

    def unregister_client(self, client_id: str):
        """Unregister the client from receiving results.
        """
        with self.client_lock:
            key = client_id
            if key in self.client_connections:
                del self.client_connections[key]
            if key in self.queries_sent:
                del self.queries_sent[key]  # Limpiar queries_sent por cliente
            logging.info(
                "action: client_unregistered | result: success | client_id: %s",
                client_id,
            )
                
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
            
            client_id = getattr(data_batch, "client_id", None)
            if not client_id:
                logging.warning(
                    "action: process_result | result: missing_client_id | query_id: %s",
                    query_id,
                )
                return
            
            # Log detailed information about the result
            inner_opcode = getattr(data_batch.batch_msg, "opcode", "unknown")
            batch_status = getattr(data_batch.batch_msg, "batch_status", "unknown")
            row_count = len(getattr(data_batch.batch_msg, "rows", [])) if hasattr(data_batch.batch_msg, "rows") else 0
            
            # Get status text
            status_text = "UNKNOWN"
            if batch_status == BatchStatus.CONTINUE:
                status_text = "CONTINUE"
            elif batch_status == BatchStatus.EOF:
                status_text = "EOF"
            elif batch_status == BatchStatus.CANCEL:
                status_text = "CANCEL"
            
            # Get opcode name
            opcode_name = self._get_opcode_name(inner_opcode)
            
            # Log detailed information about the result
            logging.info(
                f"action: result_received | result: success | query_id: {query_id} | " 
                f"opcode: {inner_opcode} ({opcode_name}) | batch_status: {batch_status} ({status_text}) | rows: {row_count}"
            )
            
            # Log ALL rows from the result
            if row_count > 0 and hasattr(data_batch.batch_msg, "rows"):
                try:
                    for i, row in enumerate(data_batch.batch_msg.rows):
                        row_data = {}
                        if isinstance(row, dict):
                            # For dictionary rows
                            row_data = row
                        else:
                            # For object rows, convert to dict for logging
                            attrs = [attr for attr in dir(row) if not attr.startswith('_') and not callable(getattr(row, attr))]
                            row_data = {attr: getattr(row, attr) for attr in attrs}
                        
                        logging.info(f"action: result_row | query_id: {query_id} | row: {i+1}/{row_count} | data: {row_data}")
                except Exception as e:
                    logging.warning(f"action: log_row_data | result: fail | error: {str(e)}")
            
            # Mine
            self._forward_result_to_client(query_id, data_batch.batch_msg.to_bytes(), client_id)

        except Exception as e:
            logging.error(f"action: process_result | result: fail | error: {str(e)}")
    
    def _get_opcode_name(self, opcode):
        """Get a human-readable name for an opcode.
        
        Args:
            opcode: The opcode to get the name for
            
        Returns:
            A string name for the opcode
        """
        opcode_names = {
            Opcodes.QUERY_RESULT_1: "QUERY_RESULT_1",
            Opcodes.QUERY_RESULT_2: "QUERY_RESULT_2",
            Opcodes.QUERY_RESULT_3: "QUERY_RESULT_3",
            Opcodes.QUERY_RESULT_4: "QUERY_RESULT_4",
            Opcodes.QUERY_RESULT_ERROR: "QUERY_RESULT_ERROR"
        }
        return opcode_names.get(opcode, "UNKNOWN")
        

            
    def _forward_result_to_client(self, query_id, result_bytes, client_id: str):
        """Forward the result to the appropriate client.
        
        Args:
            query_id: The query ID to find the associated client
            result_bytes: The raw result bytes to forward
        """
        client_conn = None
        with self.client_lock:
            client_conn = self.client_connections.get(client_id)
            
        if not client_conn:
            logging.warning(f"action: forward_result | result: no_client | query_id: {query_id}")
            return
            
        try:
            # Send the raw bytes to the client
            client_conn.sendall(result_bytes)
            self.queries_sent[client_id].add(query_id)  # Agregar a set por cliente
            bytes_sent = len(result_bytes)
            logging.info(f"action: forward_result | result: success | query_id: {query_id} | bytes_sent: {bytes_sent}")
            if len(self.queries_sent[client_id]) == 4:  # Chequear por cliente
                logging.info(f"action: all_queries_sent | result: success | client_id: {client_id}")
                finished_msg = Finished()
                client_conn.sendall(finished_msg.to_bytes())
                # client_conn.close()  # Cerrar la conexión después de enviar FINISHED
                # self.unregister_client(client_id)
            self.orchestrator.increment_queries_sent(client_id)  # Incrementar en el orchestrator

        except Exception as e:
            logging.error(f"action: forward_result | result: fail | query_id: {query_id} | error: {str(e)}")
            # Clean up the connection on error
            self.unregister_client(client_id)