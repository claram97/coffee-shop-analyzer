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
from protocol import deserialize_message, Finished, DataBatch, ProtocolError
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
            "eof_signal_received": False,
            "total_source_batches": -1,
            "received_batches": {},
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
            batch = deserialize_message(body)
            logging.info(f"DEBUG: Successfully parsed message - query_ids: {batch.query_ids}, batch_number: {batch.batch_number}")
        except ProtocolError as e:
            logging.error(f"Message is malformed, discarding. Error: {e}")
            logging.error(f"DEBUG: Message body prefix (first 100 bytes): {body[:100].hex()}")
            # Don't raise an exception - this allows the message to be ACK'd and discarded
            # since it's permanently malformed and shouldn't be reprocessed
            return

        try:
            if not batch.query_ids:
                logging.warning("Message has no query_ids, skipping")
                return

            logging.info(f"DEBUG: Processing message for queries: {batch.query_ids}")
            
            for query_id_int in batch.query_ids:
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
                    self._process_batch(state, batch)
                    self._save_checkpoint(state)

                    if self._is_query_complete(state) and state.status != "finalizing":
                        state.status = "finalizing"
                        logging.info(f"Query {query_id} is complete. Submitting for finalization.")
                        self.executor.submit(self._finalize_query, state)
            
            logging.info(f"DEBUG: Successfully processed message")
        
        except Exception as e:
            logging.error(f"Error in message handler: {e}", exc_info=True)
            logging.error(f"DEBUG: Exception occurred while processing message with query_ids: {getattr(batch, 'query_ids', 'unknown')}")
            # Re-raise the exception so the message gets NACK'd and requeued
            raise

    def _process_batch(self, state: QueryState, batch: DataBatch):
        logging.info(f"Processing batch #{batch.batch_number} for query {state.query_id}, type: {type(batch.batch_msg).__name__}")
        
        if isinstance(batch.batch_msg, Finished):
            state.completion_tracker["eof_signal_received"] = True
            state.completion_tracker["total_source_batches"] = batch.batch_number
            logging.info(f"EOF signal received for query {state.query_id}. Total source batches: {batch.batch_number}")
            # EOF signal should not be counted as a regular batch - return early
            state.last_update_time = time.time()
            return
        
        if hasattr(batch.batch_msg, 'rows') and batch.batch_msg.rows:
            strategy = get_strategy(state.query_type)
            strategy.consolidate(state.consolidated_data, batch.batch_msg.rows)
            logging.debug(f"Consolidated {len(batch.batch_msg.rows)} rows for query {state.query_id}")

        # Only track non-EOF batches in received_batches
        source_index = str(batch.batch_number)
        received = state.completion_tracker.setdefault("received_batches", {})
        batch_info = received.setdefault(source_index, {})
        
        copy_info = batch.meta
        if copy_info:
            part, total = next(iter(copy_info.items()))
            batch_info["total_copies"] = total
            batch_info.setdefault("received_copies", set()).add(part)
        
        if batch.total_shards > 1:
            batch_info["total_shards"] = batch.total_shards
            batch_info.setdefault("received_shards", set()).add(batch.shard_num)

        state.last_update_time = time.time()
        logging.debug(f"Query {state.query_id} - Received batches: {len(received)}, Total expected: {state.completion_tracker.get('total_source_batches', -1)}")

    def _is_query_complete(self, state: QueryState) -> bool:
        tracker = state.completion_tracker
        logging.info(f"Checking completion for query {state.query_id}: EOF={tracker.get('eof_signal_received')}, "
                    f"total_batches={tracker.get('total_source_batches', -1)}, "
                    f"received_batches={len(tracker.get('received_batches', {}))}")
        
        if not tracker.get("eof_signal_received") or tracker.get("total_source_batches", -1) == -1:
            return False
        
        if len(tracker.get("received_batches", {})) != tracker["total_source_batches"]:
            return False

        for batch_info in tracker["received_batches"].values():
            if "total_copies" in batch_info and len(batch_info.get("received_copies", set())) != batch_info["total_copies"]:
                return False
            if "total_shards" in batch_info and len(batch_info.get("received_shards", set())) != batch_info["total_shards"]:
                return False
        
        logging.info(f"Query {state.query_id} is complete!")
        return True

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