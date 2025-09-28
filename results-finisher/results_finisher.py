import json
import logging
import os
import pickle
import threading
import time
import io
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
    query_id: str
    query_type: QueryType
    status: str = "processing"
    consolidated_data: Dict[str, Any] = field(default_factory=dict)
    completion_tracker: Dict[str, Any] = field(default_factory=dict)
    last_update_time: float = field(default_factory=time.time)

    def __post_init__(self):
        self.completion_tracker = {
            "received_batches": {},
            "eof_signals": {},  # Track EOF signals per table type (table_name -> True)
        }

class ResultsFinisher:
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
        logging.info("Loading state from checkpoints...")
        for filename in os.listdir(self.checkpoint_dir):
            if filename.endswith(".state"):
                filepath = os.path.join(self.checkpoint_dir, filename)
                try:
                    with open(filepath, 'rb') as f:
                        state = pickle.load(f)
                        self.active_queries[state.query_id] = state
                        logging.info(f"Recovered state for query: {state.query_id}")
                except Exception as e:
                    logging.error(f"Failed to load checkpoint {filename}: {e}")

    def _save_checkpoint(self, query_state: QueryState):
        filepath = os.path.join(self.checkpoint_dir, f"{query_state.query_id}.state")
        try:
            with open(filepath, 'wb') as f:
                pickle.dump(query_state, f)
        except IOError as e:
            logging.error(f"Failed to save checkpoint for query {query_state.query_id}: {e}")

    def _message_handler(self, body: bytes):
        logging.info(f"DEBUG: Received message of {len(body)} bytes")
        
        try:
            message = DataBatch.deserialize_from_bytes(body)
            
            if isinstance(message, DataBatch):
                logging.info(f"DEBUG: Successfully parsed DataBatch - query_ids: {message.query_ids}, batch_number: {message.batch_number}")
                
                if not message.query_ids:
                    logging.warning("DataBatch has no query_ids, skipping")
                    return

                logging.info(f"DEBUG: Processing DataBatch for queries: {message.query_ids}")
                
                for query_id_int in message.query_ids:
                    query_id = str(query_id_int)
                    logging.info(f"DEBUG: Processing query_id: {query_id}")
                    
                    with self.lock:
                        if query_id not in self.active_queries:
                            try:
                                query_type = QueryType(f"Q{query_id}")
                            except ValueError:
                                query_type = QueryType.UNKNOWN
                            self.active_queries[query_id] = QueryState(query_id, query_type)
                            logging.info(f"New query started: {query_id} (Type: {query_type.value})")
                        
                        state = self.active_queries[query_id]
                        self._process_data_batch(state, message)
                        self._save_checkpoint(state)

                        if self._is_query_complete(state) and state.status != "finalizing":
                            state.status = "finalizing"
                            logging.info(f"Query {query_id} is complete. Submitting for finalization.")
                            self.executor.submit(self._finalize_query, state)
            
            elif isinstance(message, EOFMessage):
                table_type = message.get_table_type()
                logging.info(f"DEBUG: Successfully parsed EOFMessage for table '{table_type}'")
                
                # EOF messages affect all active queries - they signal end of a table type
                with self.lock:
                    for query_id, state in self.active_queries.items():
                        self._process_eof_message(state, message)
                        self._save_checkpoint(state)
                        
                        if self._is_query_complete(state) and state.status != "finalizing":
                            state.status = "finalizing"
                            logging.info(f"Query {query_id} is complete after EOF. Submitting for finalization.")
                            self.executor.submit(self._finalize_query, state)
            
            else:
                logging.error(f"Unexpected message type: {type(message)}")
                return
                
            logging.info(f"DEBUG: Successfully processed message")
        
        except ProtocolError as e:
            logging.error(f"Message is malformed, discarding. Error: {e}")
            logging.error(f"DEBUG: Message body prefix (first 100 bytes): {body[:100].hex()}")
            # Don't raise an exception - this allows the message to be ACK'd and discarded
            # since it's permanently malformed and shouldn't be reprocessed
            return
        
        except Exception as e:
            logging.error(f"Error in message handler: {e}", exc_info=True)
            logging.error(f"DEBUG: Exception occurred while processing message")
            # Re-raise the exception so the message gets NACK'd and requeued
            raise

    def _process_data_batch(self, state: QueryState, batch: DataBatch):
        logging.info(f"Processing DataBatch #{batch.batch_number} for query {state.query_id}, embedded type: {type(batch.batch_msg).__name__}")
        
        # Process the embedded message data if it has rows
        if hasattr(batch.batch_msg, 'rows') and batch.batch_msg.rows:
            strategy = get_strategy(state.query_type)
            strategy.consolidate(state.consolidated_data, batch.batch_msg.rows)
            logging.debug(f"Consolidated {len(batch.batch_msg.rows)} rows for query {state.query_id}")

        # Track regular data batches
        source_index = str(batch.batch_number)
        received = state.completion_tracker.setdefault("received_batches", {})
        batch_info = received.setdefault(source_index, {})
        
        # Track copy and shard information from DataBatch metadata
        copy_info = batch.meta
        if copy_info:
            part, total = next(iter(copy_info.items()))
            batch_info["total_copies"] = total
            batch_info.setdefault("received_copies", set()).add(part)
        
        if batch.total_shards > 1:
            batch_info["total_shards"] = batch.total_shards
            batch_info.setdefault("received_shards", set()).add(batch.shard_num)

        state.last_update_time = time.time()
        logging.debug(f"Query {state.query_id} - Received batches: {len(received)}")

    def _process_eof_message(self, state: QueryState, eof_msg: EOFMessage):
        table_type = eof_msg.get_table_type()
        logging.info(f"Processing EOF for table '{table_type}' in query {state.query_id}")
        
        # Track EOF signals per table type
        if "eof_signals" not in state.completion_tracker:
            state.completion_tracker["eof_signals"] = {}
        state.completion_tracker["eof_signals"][table_type] = True
        
        state.last_update_time = time.time()

    def _is_query_complete(self, state: QueryState) -> bool:
        tracker = state.completion_tracker
        eof_signals = tracker.get("eof_signals", {})
        received_batches = tracker.get("received_batches", {})
        
        # A query is complete if we have received all expected EOFs.
        # The additional checks for batches only apply if batches were received.
        
        logging.info(f"Checking completion for query {state.query_id}: "
                     f"received_batches={len(received_batches)}, "
                     f"eof_signals={list(eof_signals.keys())}")
        
        expected_tables = self._get_expected_tables_for_query(state.query_type)
        
        if not expected_tables:
            logging.debug(f"Query {state.query_id}: No expected tables for type {state.query_type}, cannot determine completion.")
            return False
        
        # 1. Check if all expected EOF signals have arrived. This is the main condition.
        if not expected_tables.issubset(eof_signals.keys()):
            missing_eof_signals = expected_tables - set(eof_signals.keys())
            logging.debug(f"Query {state.query_id}: Still waiting for EOF signals from: {missing_eof_signals}")
            return False

        # 2. If data batches were received, ensure they are all complete.
        for batch_num, batch_info in received_batches.items():
            # Check for copy completeness
            if "total_copies" in batch_info:
                received_copies = batch_info.get("received_copies", set())
                if len(received_copies) != batch_info["total_copies"]:
                    logging.debug(f"Query {state.query_id}: Batch {batch_num} has incomplete copies. "
                                  f"Got {len(received_copies)} of {batch_info['total_copies']}.")
                    return False
            
            # Check for shard completeness
            if "total_shards" in batch_info:
                received_shards = batch_info.get("received_shards", set())
                if len(received_shards) != batch_info["total_shards"]:
                    logging.debug(f"Query {state.query_id}: Batch {batch_num} has incomplete shards. "
                                  f"Got {len(received_shards)} of {batch_info['total_shards']}.")
                    return False
        
        logging.info(f"Query {state.query_id} is complete! All EOFs received and all batches are complete.")
        return True
    
    def _get_expected_tables_for_query(self, query_type: QueryType) -> set[str]:
        """
        Get the set of expected table types based on the query type.
        This determines which EOF signals we need to wait for.
        """
        # Define which tables each query type expects
        # This should be configured based on your specific query requirements
        expected_tables_map = {
            QueryType.Q1: {"menu_items", "transactions", "stores"},
            QueryType.Q2: {"transactions", "transaction_items", "stores", "users"},
            QueryType.Q3: {"transactions", "stores"},
            QueryType.Q4: {"menu_items", "transaction_items"},
        }
        
        return expected_tables_map.get(query_type, set())

    def _finalize_query(self, state: QueryState):
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
        message = {"query_id": query_id, "status": status, "result": result, "error": error_msg}
        try:
            self.output_client.send(json.dumps(message, indent=2).encode('utf-8'))
            logging.info(f"Sent final result for query {query_id}")
        except Exception as e:
            logging.error(f"Failed to send final result for query {query_id}: {e}")

    def _send_error_result(self, query_id: str, error_msg: str):
        self._send_final_result(query_id, None, "error", error_msg)

    def _cleanup_query(self, query_id: str):
        with self.lock:
            if query_id in self.active_queries:
                del self.active_queries[query_id]
        
        checkpoint_file = os.path.join(self.checkpoint_dir, f"{query_id}.state")
        if os.path.exists(checkpoint_file):
            os.remove(checkpoint_file)
        logging.info(f"Cleanup complete for query {query_id}.")

    def start(self):
        logging.info("ResultsFinisher starting consumer...")
        self.input_client.start_consuming(self._message_handler)

    def stop(self):
        logging.info("ResultsFinisher stopping...")
        self.input_client.stop_consuming()
        self.executor.shutdown(wait=True)
        self.input_client.close()
        self.output_client.close()
        logging.info("ResultsFinisher stopped.")