"""
Results consumer that handles query results from the results-finisher component.
Converts custom protocol results to protobuf before forwarding to clients.
"""

import logging
import threading
import time
from middleware.middleware_client import MessageMiddlewareQueue, MessageMiddlewareDisconnectedError
from protocol import DataBatch, Opcodes, BatchStatus
from protocol.messages import QueryResult1, QueryResult2, QueryResult3, QueryResult4, QueryResultError
from common.protobuf_handler import ProtobufMessageWriter


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
            
            # Forward the result to the client
            self._forward_result_to_client(query_id, body)
            
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
    
    def _convert_to_protobuf_table_data(self, data_batch):
        """Convert custom protocol DataBatch to protobuf TableData.
        
        Args:
            data_batch: DataBatch object from custom protocol
            
        Returns:
            protobuf TableData message
        """
        from protos import table_data_pb2
        
        # Create TableData
        table_data = table_data_pb2.TableData()
        
        # Map opcode to TableName (query results)
        opcode = getattr(data_batch.batch_msg, "opcode", None)
        if opcode == Opcodes.QUERY_RESULT_1:
            table_data.name = table_data_pb2.TableName.QUERY_RESULTS_1
        elif opcode == Opcodes.QUERY_RESULT_2:
            table_data.name = table_data_pb2.TableName.QUERY_RESULTS_2
        elif opcode == Opcodes.QUERY_RESULT_3:
            table_data.name = table_data_pb2.TableName.QUERY_RESULTS_3
        elif opcode == Opcodes.QUERY_RESULT_4:
            table_data.name = table_data_pb2.TableName.QUERY_RESULTS_4
        else:
            logging.warning(f"action: convert_to_protobuf | result: unknown_opcode | opcode: {opcode}")
            table_data.name = table_data_pb2.TableName.MENU_ITEMS  # Default
        
        # Set batch number
        table_data.batch_number = getattr(data_batch.batch_msg, "batch_number", 0)
        
        # Map batch status
        batch_status = getattr(data_batch.batch_msg, "batch_status", BatchStatus.CONTINUE)
        if batch_status == BatchStatus.EOF:
            table_data.status = table_data_pb2.BatchStatus.EOF
        elif batch_status == BatchStatus.CANCEL:
            table_data.status = table_data_pb2.BatchStatus.CANCEL
        else:
            table_data.status = table_data_pb2.BatchStatus.CONTINUE
        
        # Convert rows
        if hasattr(data_batch.batch_msg, "rows"):
            for row in data_batch.batch_msg.rows:
                pb_row = table_data.rows.add()
                
                # Extract values from row (handle both dict and object)
                if isinstance(row, dict):
                    values = [str(v) for v in row.values()]
                else:
                    # Get all non-private attributes as values
                    attrs = [attr for attr in dir(row) 
                            if not attr.startswith('_') and not callable(getattr(row, attr))]
                    values = [str(getattr(row, attr)) for attr in attrs]
                
                pb_row.values.extend(values)
        
        # Create schema (extract column names from first row if available)
        if hasattr(data_batch.batch_msg, "rows") and len(data_batch.batch_msg.rows) > 0:
            first_row = data_batch.batch_msg.rows[0]
            if isinstance(first_row, dict):
                columns = list(first_row.keys())
            else:
                attrs = [attr for attr in dir(first_row) 
                        if not attr.startswith('_') and not callable(getattr(first_row, attr))]
                columns = attrs
            
            table_data.schema.columns.extend(columns)
        
        return table_data
        

            
    def _forward_result_to_client(self, query_id, result_bytes):
        """Forward the result to the appropriate client using protobuf.
        
        Args:
            query_id: The query ID to find the associated client
            result_bytes: The raw result bytes (custom protocol from RabbitMQ)
        """
        from protos import envelope_pb2
        
        client_conn = None
        with self.client_lock:
            client_conn = self.client_connections.get(query_id)
            
        if not client_conn:
            logging.warning(f"action: forward_result | result: no_client | query_id: {query_id}")
            return
            
        try:
            # 1. Deserialize custom protocol from RabbitMQ
            data_batch = DataBatch.deserialize_from_bytes(result_bytes)
            
            # 2. Convert to protobuf TableData
            table_data = self._convert_to_protobuf_table_data(data_batch)
            
            # 3. Create Envelope with TableData
            envelope = envelope_pb2.Envelope()
            envelope.type = envelope_pb2.Envelope.TABLE_DATA
            envelope.table_data.CopyFrom(table_data)
            
            # 4. Send using ProtobufMessageWriter
            ProtobufMessageWriter.write_envelope(client_conn, envelope)
            
            logging.info(
                f"action: forward_result | result: success | query_id: {query_id} | "
                f"table: {table_data.name} | batch: {table_data.batch_number} | "
                f"rows: {len(table_data.rows)} | status: {table_data.status}"
            )
            
            # Check if this is an EOF batch - if so, unregister the client
            batch_status = getattr(data_batch.batch_msg, "batch_status", None)
            if batch_status == BatchStatus.EOF:
                logging.info(f"action: eof_result_forwarded | query_id: {query_id} | unregistering_client: true")
                self.unregister_client_for_query(query_id)
            
        except Exception as e:
            logging.error(f"action: forward_result | result: fail | query_id: {query_id} | error: {str(e)}")
            # Clean up the connection on error
            self.unregister_client_for_query(query_id)
