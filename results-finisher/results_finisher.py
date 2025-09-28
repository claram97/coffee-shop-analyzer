import json
import logging
import os
import pickle
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Dict, Any, List

from middleware_client import MessageMiddlewareQueue
from protocol import (
    ProtocolError, Opcodes, BatchStatus, DataBatch
)
from protocol.messages import EOFMessage
from query_strategy import get_strategy
from constants import QueryType

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [ResultsFinisher] - %(message)s'
)

@dataclass
class QueryState:
    """State for tracking a specific query's data and completion."""
    query_id: str
    query_type: QueryType
    status: str = "processing"
    consolidated_data: Dict[str, Any] = field(default_factory=dict)
    completion_tracker: Dict[str, Any] = field(default_factory=dict)
    last_update_time: float = field(default_factory=time.time)

class ResultsFinisher:
    """
    The ResultsFinisher processes DataBatch messages containing query data and EOFMessage signals.
    
    Flow:
    1. Receives DataBatch messages from ResultsRouter containing data for specific queries
    2. Consolidates data per query using query-specific strategies  
    3. When EOFMessage arrives, finalizes the query and sends results to output queue
    4. Maintains checkpoints for fault tolerance
    """
    
    def __init__(self,
                 input_client: MessageMiddlewareQueue,
                 output_client: MessageMiddlewareQueue,
                 checkpoint_dir: str):
        
        self.input_client = input_client
        self.output_client = output_client
        self.checkpoint_dir = checkpoint_dir    
        
        self.active_queries: Dict[str, QueryState] = {}
        self.lock = threading.Lock()
        self.executor = ThreadPoolExecutor(max_workers=os.cpu_count() or 1)
        
        os.makedirs(self.checkpoint_dir, exist_ok=True)
        self._load_state_from_checkpoints()

    def _load_state_from_checkpoints(self):
        """Load saved query states from checkpoint files."""
        logging.info("Loading state from checkpoints...")
        for filename in os.listdir(self.checkpoint_dir):
            if filename.endswith(".state"):
                filepath = os.path.join(self.checkpoint_dir, filename)
                try:
                    with open(filepath, 'rb') as f:
                        state: QueryState = pickle.load(f)
                        self.active_queries[state.query_id] = state
                        logging.info(f"Recovered state for query: {state.query_id}")
                except Exception as e:
                    logging.error(f"Failed to load checkpoint {filename}: {e}")

    def _save_checkpoint(self, query_state: QueryState):
        """Save query state to checkpoint file."""
        filepath = os.path.join(self.checkpoint_dir, f"{query_state.query_id}.state")
        try:
            with open(filepath, 'wb') as f:
                pickle.dump(query_state, f)
        except IOError as e:
            logging.error(f"Failed to save checkpoint for query {query_state.query_id}: {e}")

    def _message_handler(self, body: bytes):
        """Main message handler that processes incoming DataBatch and EOFMessage."""
        logging.info(f"DEBUG: Received message of {len(body)} bytes")
        
        try:
            if len(body) < 5:  # Minimum: 1 byte opcode + 4 bytes length
                logging.error("Message too short for valid protocol frame")
                return

            # Read opcode to determine message type
            opcode = body[0]
            
            if opcode == Opcodes.DATA_BATCH:
                # Use DataBatch deserialization
                message = DataBatch.deserialize_from_bytes(body)
                self._handle_data_batch(message)
                
            elif opcode == Opcodes.EOF:
                # Parse EOFMessage directly
                length = int.from_bytes(body[1:5], 'little', signed=True)
                if len(body) != 5 + length:
                    logging.error(f"EOF message length mismatch: expected {5 + length}, got {len(body)}")
                    return
                
                eof_message = EOFMessage()
                eof_message.read_from(body[5:])  # Skip opcode and length
                self._handle_eof_message(eof_message)
                
            else:
                logging.error(f"Unexpected message opcode: {opcode}")
                return

            logging.info("DEBUG: Successfully processed message")
        
        except ProtocolError as e:
            logging.error(f"Message is malformed, discarding. Error: {e}")
            return
        
        except Exception as e:
            logging.error(f"Error in message handler: {e}", exc_info=True)
            raise

    def _handle_data_batch(self, batch: DataBatch):
        """Process DataBatch message containing query data."""
        logging.info(f"DEBUG: Processing DataBatch for queries: {batch.query_ids}")
        
        if not batch.query_ids:
            logging.warning("DataBatch has no query_ids, skipping")
            return

        # Process data for each query ID mentioned in the batch
        for query_id_int in batch.query_ids:
            query_id = str(query_id_int)
            
            with self.lock:
                # Create query state if doesn't exist
                if query_id not in self.active_queries:
                    try:
                        query_type = QueryType(f"Q{query_id}")
                    except ValueError:
                        query_type = QueryType.UNKNOWN
                    
                    self.active_queries[query_id] = QueryState(query_id, query_type)
                    logging.info(f"New query started: {query_id} (Type: {query_type.value})")
                
                state = self.active_queries[query_id]
                self._process_data_batch_rows(state, batch)
                self._save_checkpoint(state)

    def _handle_eof_message(self, eof_msg: EOFMessage):
        """Process EOFMessage indicating a query group is complete."""
        table_type = eof_msg.get_table_type()
        logging.info(f"DEBUG: Processing EOF for table '{table_type}'")

        with self.lock:
            # Find all queries that are waiting for this table type
            queries_to_finalize = []
            
            for query_id, state in list(self.active_queries.items()):
                if state.status == "processing":
                    expected_tables = self._get_expected_tables_for_query(state.query_type)
                    
                    if table_type in expected_tables:
                        self._process_eof_for_query(state, table_type)
                        self._save_checkpoint(state)

                        if self._is_query_complete(state):
                            state.status = "finalizing"
                            queries_to_finalize.append(state)
                            logging.info(f"Query {query_id} is complete after EOF for '{table_type}'")
            
            # Submit complete queries for finalization
            for state in queries_to_finalize:
                self.executor.submit(self._finalize_query, state)

    def _process_data_batch_rows(self, state: QueryState, batch: DataBatch):
        """Consolidate data from batch into query state using query-specific strategy."""
        logging.info(f"Processing DataBatch #{getattr(batch, 'batch_number', 0)} for query {state.query_id}")
        
        if hasattr(batch.batch_msg, 'rows') and batch.batch_msg.rows:
            strategy = get_strategy(state.query_type)
            strategy.consolidate(state.consolidated_data, batch.batch_msg.rows)
            logging.debug(f"Consolidated {len(batch.batch_msg.rows)} rows for query {state.query_id}")

        state.last_update_time = time.time()

    def _process_eof_for_query(self, state: QueryState, table_type: str):
        """Mark EOF signal as received for a specific table in query."""
        logging.info(f"Processing EOF for table '{table_type}' in query {state.query_id}")
        
        state.completion_tracker.setdefault("eof_signals", {})[table_type] = True
        state.last_update_time = time.time()

    def _is_query_complete(self, state: QueryState) -> bool:
        """Check if query has received all expected EOF signals."""
        eof_signals = state.completion_tracker.get("eof_signals", {})
        expected_tables = self._get_expected_tables_for_query(state.query_type)
        
        if not expected_tables:
            logging.debug(f"Query {state.query_id}: No expected tables for type {state.query_type}")
            return False
        
        # Query is complete when all expected EOF signals have arrived
        missing_eof_signals = expected_tables - set(eof_signals.keys())
        if missing_eof_signals:
            logging.debug(f"Query {state.query_id}: Still waiting for EOF signals from: {missing_eof_signals}")
            return False

        logging.info(f"Query {state.query_id} is complete! All EOFs received.")
        return True
    
    def _get_expected_tables_for_query(self, query_type: QueryType) -> set[str]:
        """
        Get the set of expected table types based on the query type.
        This determines which EOF signals we need to wait for.
        
        Table names should match the message types:
        - NewMenuItems -> "menu_items" 
        - NewStores -> "stores"
        - NewTransactions -> "transactions"
        - NewTransactionItems -> "transaction_items"
        - NewUsers -> "users"
        """
        # Define which tables each query type expects based on query requirements
        expected_tables_map = {
            QueryType.Q1: {"transactions"},  # Q1 gets pre-joined transaction+store data in one stream
            QueryType.Q2: {"transaction_items"},  # Q2 gets pre-joined transaction_items+users data  
            QueryType.Q3: {"transactions"},  # Q3 gets pre-joined transaction+store data
            QueryType.Q4: {"transaction_items"},  # Q4 gets pre-joined transaction_items+users data
        }
        
        return expected_tables_map.get(query_type, set())

    def _finalize_query(self, state: QueryState):
        """Finalize query using query-specific strategy and send result."""
        try:
            logging.info(f"Finalizing query {state.query_id} (Type: {state.query_type.value})...")
            
            strategy = get_strategy(state.query_type)
            final_result = strategy.finalize(state.consolidated_data)
            
            self._send_final_result(state.query_id, final_result)
        
        except Exception as e:
            logging.error(f"Error during finalization of query {state.query_id}: {e}", exc_info=True)
            self._send_error_result(state.query_id, str(e))
        
        finally:
            self._cleanup_query(state.query_id)

    def _send_final_result(self, query_id: str, result: Any, status: str = "success", error_msg: str = ""):
        """Send final query result to output queue."""
        message = {
            "query_id": query_id, 
            "status": status, 
            "result": result, 
            "error": error_msg
        }
        try:
            self.output_client.send(json.dumps(message, indent=2).encode('utf-8'))
            logging.info(f"Sent final result for query {query_id}")
        except Exception as e:
            logging.error(f"Failed to send final result for query {query_id}: {e}")

    def _send_error_result(self, query_id: str, error_msg: str):
        """Send error result for failed query."""
        self._send_final_result(query_id, None, "error", error_msg)

    def _cleanup_query(self, query_id: str):
        """Remove query from active state and clean up checkpoint file."""
        with self.lock:
            if query_id in self.active_queries:
                del self.active_queries[query_id]
        
        checkpoint_file = os.path.join(self.checkpoint_dir, f"{query_id}.state")
        if os.path.exists(checkpoint_file):
            os.remove(checkpoint_file)
        logging.info(f"Cleanup complete for query {query_id}.")

    def start(self):
        """Start consuming messages from input queue."""
        logging.info("ResultsFinisher starting consumer...")
        self.input_client.start_consuming(self._message_handler)

    def stop(self):
        """Stop consuming and shutdown gracefully."""
        logging.info("ResultsFinisher stopping...")
        self.input_client.stop_consuming()
        self.executor.shutdown(wait=True)
        self.input_client.close()
        self.output_client.close()
        logging.info("ResultsFinisher stopped.")