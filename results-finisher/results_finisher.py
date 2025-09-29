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
    batch_counters: Dict[str, Dict[str, int]] = field(default_factory=dict)  # table_type -> {received: N, expected: M}
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
        """Main message handler that processes incoming DataBatch messages."""
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
        logging.info(f"DEBUG: Processing DataBatch #{getattr(batch, 'batch_number', 0)} for queries: {batch.query_ids}")
        
        if not batch.query_ids:
            logging.warning("DataBatch has no query_ids, skipping")
            return

        # Get table type from embedded message
        table_type = self._get_table_type_from_batch(batch)
        if not table_type:
            logging.warning("Could not determine table type from DataBatch, skipping")
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
                
                # Process batch data (if not EOF-only batch)
                if hasattr(batch.batch_msg, 'rows') and batch.batch_msg.rows:
                    self._process_data_batch_rows(state, batch)
                
                # Handle batch counting and EOF detection
                self._process_batch_completion(state, batch, table_type)
                
                self._save_checkpoint(state)

    def _get_table_type_from_batch(self, batch: DataBatch) -> str:
        """Extract table type from DataBatch embedded message."""
        if not hasattr(batch, 'batch_msg') or not batch.batch_msg:
            return ""
        
        # Map message opcodes to table types
        table_type_map = {
            Opcodes.NEW_TRANSACTION: "transactions",
            Opcodes.NEW_STORES: "stores", 
            Opcodes.NEW_MENU_ITEMS: "menu_items",
            Opcodes.NEW_TRANSACTION_ITEMS: "transaction_items",
            Opcodes.NEW_USERS: "users"
        }
        
        return table_type_map.get(batch.batch_msg.opcode, "")

    def _process_batch_completion(self, state: QueryState, batch: DataBatch, table_type: str):
        """Handle batch counting and check for table completion."""
        batch_number = getattr(batch, 'batch_number', 0)
        batch_status = getattr(batch.batch_msg, 'batch_status', BatchStatus.CONTINUE)
        
        # Initialize batch counter for this table if needed
        if table_type not in state.batch_counters:
            state.batch_counters[table_type] = {"received": 0, "expected": None}
        
        # Increment received batch count
        state.batch_counters[table_type]["received"] += 1
        
        logging.debug(f"Query {state.query_id}: Received batch #{batch_number} for table '{table_type}' (status: {batch_status})")
        
        # Check if this is the final batch (EOF status)
        if batch_status == BatchStatus.EOF:
            # Set expected total based on batch number
            state.batch_counters[table_type]["expected"] = batch_number
            logging.info(f"Query {state.query_id}: Table '{table_type}' marked complete at batch #{batch_number}")
            
            # Check if all batches for this table have been received
            if self._is_table_complete(state, table_type):
                logging.info(f"Query {state.query_id}: All batches received for table '{table_type}'")
                state.completion_tracker.setdefault("completed_tables", set()).add(table_type)
                
                # Check if query is fully complete
                if self._is_query_complete(state):
                    state.status = "finalizing"
                    self.executor.submit(self._finalize_query, state)

    def _is_table_complete(self, state: QueryState, table_type: str) -> bool:
        """Check if all expected batches have been received for a table."""
        counter = state.batch_counters.get(table_type, {})
        expected = counter.get("expected")
        received = counter.get("received", 0)
        
        if expected is None:
            return False  # Haven't received EOF batch yet
            
        return received >= expected

    def _process_data_batch_rows(self, state: QueryState, batch: DataBatch):
        """Consolidate data from batch into query state using query-specific strategy."""
        logging.info(f"Processing DataBatch #{getattr(batch, 'batch_number', 0)} for query {state.query_id}")
        
        if hasattr(batch.batch_msg, 'rows') and batch.batch_msg.rows:
            strategy = get_strategy(state.query_type)
            strategy.consolidate(state.consolidated_data, batch.batch_msg.rows)
            logging.debug(f"Consolidated {len(batch.batch_msg.rows)} rows for query {state.query_id}")

        state.last_update_time = time.time()

    def _is_query_complete(self, state: QueryState) -> bool:
        """Check if query has received all expected tables completely."""
        expected_tables = self._get_expected_tables_for_query(state.query_type)
        completed_tables = state.completion_tracker.get("completed_tables", set())
        
        if not expected_tables:
            logging.debug(f"Query {state.query_id}: No expected tables for type {state.query_type}")
            return False
        
        # Query is complete when all expected tables are completed
        missing_tables = expected_tables - completed_tables
        if missing_tables:
            logging.debug(f"Query {state.query_id}: Still waiting for tables: {missing_tables}")
            return False

        logging.info(f"Query {state.query_id} is complete! All tables received: {completed_tables}")
        return True
    
    def _get_expected_tables_for_query(self, query_type: int) -> set:
        """Get the set of expected table types for a specific query type."""
        if query_type == 1:  # Q1
            return {"NewTransactions"}
        elif query_type == 2:  # Q2  
            return {"NewStores"}
        elif query_type == 3:  # Q3
            return {"NewTransactions", "NewStores"}
        elif query_type == 4:  # Q4
            return {"NewTransactions", "NewStores"}
        else:
            logging.warning(f"Unknown query type: {query_type}")
            return set()

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