from __future__ import annotations

import json
import logging
import os
import random
import threading
import time
from collections import defaultdict
from random import randint
from typing import Dict, Optional
import functools

from middleware.middleware_client import (MessageMiddlewareExchange,
                                          MessageMiddlewareQueue)
from protocol2.databatch_pb2 import DataBatch, Query
from protocol2.envelope_pb2 import Envelope, MessageType
from protocol2.eof_message_pb2 import EOFMessage
from protocol2.table_data_pb2 import (Row, TableData, TableName, TableSchema,
                                      TableStatus)
from protocol2.table_data_utils import iterate_rows_as_dicts

TABLE_NAME_TO_STR = {
    TableName.MENU_ITEMS: "menu_items",
    TableName.STORES: "stores",
    TableName.TRANSACTION_ITEMS: "transaction_items",
    TableName.TRANSACTIONS: "transactions",
    TableName.USERS: "users",
}
LIGHT_TABLES = {TableName.MENU_ITEMS, TableName.STORES}


def is_bit_set(mask: int, idx: int) -> bool:
    return ((mask >> idx) & 1) == 1


def set_bit(mask: int, idx: int) -> int:
    return mask | (1 << idx)


def first_zero_bit(mask: int, total_bits: int) -> Optional[int]:
    for i in range(total_bits):
        if not is_bit_set(mask, i):
            return i
    return None


class TableConfig:
    def __init__(self, aggregators: int):
        self.aggregators = aggregators


class QueryPolicyResolver:
    def steps_remaining(
        self, batch_table_name: TableName, batch_queries: list[Query], steps_done: int
    ) -> bool:
        if batch_table_name == TableName.TRANSACTIONS:
            if len(batch_queries) == 3:
                return steps_done == 0
            if len(batch_queries) == 1 and batch_queries[0] == Query.Q4:
                return False
            if len(batch_queries) == 2:
                return steps_done == 1
            if len(batch_queries) == 1 and batch_queries[0] == Query.Q3:
                return False
            if len(batch_queries) == 1 and batch_queries[0] == Query.Q1:
                return steps_done == 2
        if batch_table_name == TableName.TRANSACTION_ITEMS:
            return steps_done == 0
        return False

    def get_duplication_count(self, batch_queries: list[Query]) -> int:
        return 2 if len(batch_queries) > 1 else 1

    def get_new_batch_queries(
        self, batch_table_name: TableName, batch_queries: list[Query], copy_number: int
    ) -> list[Query]:
        if batch_table_name == TableName.TRANSACTIONS:
            if len(batch_queries) == 3:
                return [Query.Q4] if copy_number == 1 else [Query.Q1, Query.Q3]
            if len(batch_queries) == 2:
                return [Query.Q3] if copy_number == 1 else [Query.Q1]
        return list(batch_queries)

    def total_steps(
        self, batch_table_name: TableName, batch_queries: list[Query]
    ) -> int:
        if batch_table_name == TableName.TRANSACTIONS:
            if len(batch_queries) == 3:
                return 3
            if len(batch_queries) == 2:
                return 2
            if batch_queries == [Query.Q1]:
                return 3
            if batch_queries in [[Query.Q3], [Query.Q4]]:
                return 1
        return 1


class FilterRouter:
    """
    Filter router that routes batches to filters_pool or aggregators, and forwards light table EOFs.
    
    Message ACKing Strategy:
    - DATA_BATCH: Delayed ack - manually acked after timeout (prevents loss during crashes)
    - EOF (light tables): Delayed ack - manually acked after all orch_workers EOFs received
    - EOF (non-light tables): Acked immediately (via middleware return True)
    - Errors: Exception raised causes middleware to NACK and redeliver
    """
    def __init__(
        self,
        producer: "ExchangeBusProducer",
        policy: QueryPolicyResolver,
        table_cfg: TableConfig,
        orch_workers: int,
        persistence_dir: str = "/tmp/filter_router_state",
        router_id: int = 0,
        batch_ack_timeout: int = 30,
    ):
        self._p = producer
        self._pol = policy
        self._cfg = table_cfg
        self._log = logging.getLogger("filter-router")
        self._orch_workers = orch_workers
        
        # Track light table EOFs: key=(table, client_id) -> (count, EOFMessage)
        # Only forward EOFs for light tables (MENU_ITEMS, STORES)
        self._pending_eof: Dict[tuple[TableName, str], tuple[int, EOFMessage]] = {}
        # Track received EOF traces to prevent double-counting on restart (only for light tables)
        self._received_eof_traces: set[tuple[TableName, str, str]] = set()
        # Store unacked EOF messages: key=(table, client_id) -> list of (channel, delivery_tag)
        self._unacked_eofs: Dict[tuple[TableName, str], list] = {}
        self._eof_ack_lock = threading.Lock()
        self._state_lock = threading.Lock()
        
        # Track unacked DATA_BATCH messages: key=delivery_tag -> (channel, client_id, batch_number, timestamp)
        # Use delivery_tag as key to prevent collisions when batches loop through router multiple times
        self._unacked_batches: Dict[int, tuple] = {}
        self._batch_ack_lock = threading.Lock()
        self._batch_ack_timeout = batch_ack_timeout
        self._batch_ack_thread = None
        self._stop_event = threading.Event()
        
        # Persistence (only for EOF state)
        self._persistence_dir = persistence_dir
        self._router_id = router_id
        self._persistence_lock = threading.Lock()
        os.makedirs(self._persistence_dir, exist_ok=True)
        
        # Restore state from disk
        self._restore_state()
        
        # Start background thread for timeout-based batch ACKs
        self._start_batch_ack_thread()

    def _get_pending_eof_path(self) -> str:
        """Get file path for pending EOF state."""
        return os.path.join(self._persistence_dir, f"router_{self._router_id}_pending_eof.json")
    
    def _get_received_eof_traces_path(self) -> str:
        """Get file path for received EOF traces."""
        return os.path.join(self._persistence_dir, f"router_{self._router_id}_received_eof_traces.json")
    
    def _ensure_persistence_dir(self) -> None:
        """Ensure the persistence directory exists, creating it if necessary."""
        os.makedirs(self._persistence_dir, exist_ok=True)
    
    def _persist_pending_eof(self) -> None:
        """Persist pending EOF state to disk (only for light tables)."""
        with self._persistence_lock:
            self._ensure_persistence_dir()
            path = self._get_pending_eof_path()
            temp_path = path + ".tmp"
            try:
                # Convert to serializable format
                data = {}
                for (table, client), (count, eof) in self._pending_eof.items():
                    key = f"{table}:{client}"
                    data[key] = {
                        "count": count,
                        "table": eof.table,
                        "client_id": eof.client_id,
                    }
                with open(temp_path, "w") as f:
                    json.dump(data, f)
                os.rename(temp_path, path)
                self._log.debug("Persisted pending EOF: %d entries", len(data))
            except Exception as e:
                self._log.error("Failed to persist pending EOF: %s", e)
                if os.path.exists(temp_path):
                    os.remove(temp_path)
                raise
    
    def _persist_received_eof_traces(self) -> None:
        """Persist received EOF traces to disk."""
        with self._persistence_lock:
            self._ensure_persistence_dir()
            path = self._get_received_eof_traces_path()
            temp_path = path + ".tmp"
            try:
                data = [list(trace) for trace in self._received_eof_traces]
                with open(temp_path, "w") as f:
                    json.dump(data, f)
                os.rename(temp_path, path)
                self._log.debug("Persisted received EOF traces: %d entries", len(data))
            except Exception as e:
                self._log.error("Failed to persist received EOF traces: %s", e)
                if os.path.exists(temp_path):
                    os.remove(temp_path)
                raise
    
    def _persist_eof_state_atomic(self) -> None:
        """
        Atomically persist both pending EOF and received EOF traces.
        This prevents inconsistent state if the router crashes between the two operations.
        """
        with self._persistence_lock:
            self._ensure_persistence_dir()
            eof_path = self._get_pending_eof_path()
            received_traces_path = self._get_received_eof_traces_path()
            eof_temp = eof_path + ".tmp"
            received_traces_temp = received_traces_path + ".tmp"
            
            try:
                # Prepare both data structures
                eof_data = {}
                for (table, client), (count, eof) in self._pending_eof.items():
                    key = f"{table}:{client}"
                    eof_data[key] = {
                        "count": count,
                        "table": eof.table,
                        "client_id": eof.client_id,
                    }
                
                received_traces_data = [list(trace) for trace in self._received_eof_traces]
                
                # Write received traces first (so if EOF succeeds, received definitely succeeded)
                with open(received_traces_temp, "w") as f:
                    json.dump(received_traces_data, f)
                
                # Write EOF data
                with open(eof_temp, "w") as f:
                    json.dump(eof_data, f)
                
                # Atomically commit both (received traces first, then EOF)
                # If EOF rename succeeds, received traces rename must have succeeded
                os.rename(received_traces_temp, received_traces_path)
                os.rename(eof_temp, eof_path)
                
                self._log.debug(
                    "Atomically persisted EOF state: pending EOF (%d entries), received traces (%d entries)",
                    len(eof_data), len(received_traces_data)
                )
            except Exception as e:
                self._log.error("Failed to atomically persist EOF state: %s", e)
                # Clean up temp files
                for temp_path in [eof_temp, received_traces_temp]:
                    if os.path.exists(temp_path):
                        try:
                            os.remove(temp_path)
                        except Exception as cleanup_err:
                            self._log.warning("Failed to clean up temp file %s: %s", temp_path, cleanup_err)
                raise
    
    def _restore_state(self) -> None:
        """Restore EOF state from disk on startup (only for light tables)."""
        if not os.path.exists(self._persistence_dir):
            return
        
        self._log.info("Restoring filter router EOF state from disk: %s", self._persistence_dir)
        
        # Restore pending EOFs (only for light tables)
        eof_path = self._get_pending_eof_path()
        if os.path.exists(eof_path):
            try:
                with open(eof_path, "r") as f:
                    data = json.load(f)
                for key_str, eof_data in data.items():
                    table_str, client = key_str.split(":", 1)
                    table = int(table_str)
                    # Only restore light table EOFs
                    if table in LIGHT_TABLES:
                        eof = EOFMessage(table=eof_data["table"], client_id=eof_data["client_id"])
                        self._pending_eof[(table, client)] = (eof_data["count"], eof)
                self._log.info("Restored %d pending EOF entries", len(self._pending_eof))
            except Exception as e:
                self._log.warning("Failed to restore pending EOFs: %s", e)
        
        # Restore received EOF traces (to prevent double-counting on restart)
        received_traces_path = self._get_received_eof_traces_path()
        if os.path.exists(received_traces_path):
            try:
                with open(received_traces_path, "r") as f:
                    data = json.load(f)
                # Only restore traces for light tables
                for trace in data:
                    table = trace[0] if isinstance(trace[0], int) else int(trace[0])
                    if table in LIGHT_TABLES:
                        self._received_eof_traces.add(tuple(trace))
                self._log.info("Restored %d received EOF traces", len(self._received_eof_traces))
            except Exception as e:
                self._log.warning("Failed to restore received EOF traces: %s", e)
        
        self._log.info("Filter router state restoration complete")

    def process_message(self, msg: Envelope, channel=None, delivery_tag=None, redelivered=False) -> tuple[bool, bool]:
        """
        Process a message and return (should_ack, ack_now).
        - should_ack: True if message should eventually be acked
        - ack_now: True if should ack immediately, False if should delay ack (manual ack required)
        
        Returns:
            (True, True): Ack immediately via middleware
            (True, False): Delay ack - will manually ack later
            Exception raised: Middleware will NACK and redeliver
        """
        self._log.debug("Processing message: %r, message type: %r, redelivered: %r", msg, msg.type, redelivered)
        if msg.type == MessageType.DATA_BATCH:
            batch = msg.data_batch
            # For invalid batches (no table), ack immediately to avoid infinite redelivery
            if batch.payload.name is None:
                self._log.warning("Batch sin table_id válido. bn=%s", batch.payload.batch_number)
                return (True, True)  # Ack immediately for invalid batches
            
            # Process batch - use delayed ACK to prevent loss during crashes
            try:
                self._handle_data(batch)
                # Store batch info for delayed ACK (will be acked after timeout)
                # Use delivery_tag as key to prevent collisions when batches loop through router multiple times
                with self._batch_ack_lock:
                    self._unacked_batches[delivery_tag] = (
                        channel, 
                        batch.client_id, 
                        int(batch.payload.batch_number), 
                        time.time()
                    )
                self._log.debug("DATA_BATCH processed successfully, delayed ACK: table=%s bn=%s client=%s tag=%s", 
                              batch.payload.name, batch.payload.batch_number, batch.client_id, delivery_tag)
                return (True, False)  # Delay ACK - will manually ack after timeout
            except Exception as e:
                self._log.error("Failed to process DATA_BATCH: %s", e, exc_info=True)
                # On error, let middleware NACK (by raising exception)
                raise
        elif msg.type == MessageType.EOF_MESSAGE:
            # EOF messages: delay ack until fully processed (only for light tables)
            return self._handle_table_eof(msg.eof, channel, delivery_tag, redelivered)
        else:
            self._log.warning("Unknown message type: %r", type(msg))
            return (True, True)  # Ack unknown message types to avoid infinite redelivery

    def _handle_data(self, batch: DataBatch) -> None:
        """
        Handle a data batch: clone when necessary (fanout) and route to filters_pool or aggregators.
        No state tracking or deduplication - just routing logic.
        
        Raises exception if send fails, which will cause the message to be NACKed and redelivered.
        """
        table = batch.payload.name
        queries = batch.query_ids
        mask = batch.filter_steps
        bn = batch.payload.batch_number
        cid = batch.client_id

        # Note: Invalid batches (table is None) are handled in process_message and acked immediately
        if table is None:
            self._log.warning("Batch sin table_id válido. bn=%s", bn)
            return

        self._log.debug(
            "recv DataBatch table=%s queries=%s mask=%s bn=%s cid=%s",
            table, queries, bin(mask), bn, cid
        )

        # Check if batch needs to go through filters
        total_steps = self._pol.total_steps(table, queries)
        next_step = first_zero_bit(mask, total_steps)
        if next_step is not None and self._pol.steps_remaining(table, queries, steps_done=next_step):
            # Send to filters_pool
            # CRITICAL: Create a copy before modifying to avoid corrupting the original batch
            # If the router crashes after modification but before ACK, the batch will be redelivered
            # with the wrong filter_steps mask, causing incorrect routing
            new_mask = set_bit(mask, next_step)
            batch_copy = DataBatch()
            batch_copy.CopyFrom(batch)
            batch_copy.filter_steps = new_mask
            self._log.debug("→ filters step=%d table=%s new_mask=%s", next_step, table, bin(new_mask))
            try:
                self._p.send_to_filters_pool(batch_copy)
                self._log.debug("Successfully sent batch to filters_pool: table=%s bn=%s", table, bn)
            except Exception as e:
                self._log.error("Failed to send batch to filters_pool: table=%s bn=%s: %s", table, bn, e)
                raise
            return

        # Check if batch needs fanout (cloning with different queries)
        dup_count = int(self._pol.get_duplication_count(queries) or 1)
        if dup_count > 1:
            self._log.debug("Fan-out x%d table=%s queries=%s", dup_count, table, queries)
            
            fanout_batches = []
            for i in range(dup_count):
                new_queries = self._pol.get_new_batch_queries(table, queries, copy_number=i) or list(queries)
                b = DataBatch()
                b.CopyFrom(batch)
                b.query_ids.clear()
                b.query_ids.extend(new_queries)
                fanout_batches.append(b)
            
            # Process all fanout batches recursively
            # If any fails, exception will propagate and message will be NACKed
            for b in fanout_batches:
                self._handle_data(b)
            self._log.debug("Successfully processed all fanout batches: table=%s bn=%s", table, bn)
            return

        # Send directly to aggregator
        try:
            self._send_to_aggregator(batch)
            self._log.debug("Successfully sent batch to aggregator: table=%s bn=%s", table, bn)
        except Exception as e:
            self._log.error("Failed to send batch to aggregator: table=%s bn=%s: %s", table, bn, e)
            raise

    def _send_to_aggregator(self, batch: DataBatch) -> None:
        if batch.payload.name in [TableName.MENU_ITEMS, TableName.STORES]:
            self._log.debug(
                "_send_to_some_aggregator table=%s bn=%s",
                batch.payload.name,
                batch.payload.batch_number,
            )
        self._p.send_to_aggregator_partition(int(batch.payload.batch_number) % max(1, int(self._cfg.aggregators)), batch)

    def _handle_table_eof(self, eof: EOFMessage, channel=None, delivery_tag=None, redelivered=False) -> tuple[bool, bool]:
        """
        Handle EOF message. Only forward EOFs for light tables (MENU_ITEMS, STORES).
        Forward when all orch_workers have sent EOF for that table+client.
        """
        key = (eof.table, eof.client_id)
        trace_key = (eof.table, eof.client_id, eof.trace)

        # Only process EOFs for light tables
        if eof.table not in LIGHT_TABLES:
            self._log.debug("Ignoring EOF for non-light table: table=%s client=%s", eof.table, eof.client_id)
            return (True, True)  # Ack immediately for non-light tables

        with self._state_lock:
            # Check if already received (to prevent double-counting on restart)
            if trace_key in self._received_eof_traces:
                self._log.warning("TABLE_EOF duplicate (already received): key=%s, trace=%s. ACK.", key, eof.trace)
                return (True, True)
            
        if redelivered:
            self._log.info("TABLE_EOF REDELIVERED (recovering state): key=%s trace=%s", key, eof.trace)
        else:
            self._log.info("TABLE_EOF received: key=%s trace=%s", key, eof.trace)

        with self._eof_ack_lock:
            if channel is not None and delivery_tag is not None:
                if key not in self._unacked_eofs:
                    self._unacked_eofs[key] = []
                self._unacked_eofs[key].append((channel, delivery_tag))

        flush_info = None
        try:
            with self._state_lock:
                # Mark as received to prevent double-counting on restart
                self._received_eof_traces.add(trace_key)
                try:
                    self._persist_received_eof_traces()
                except Exception as e:
                    self._log.error("Failed to persist received EOF traces: %s", e)
                    # Continue anyway - state is in memory
                
                (recvd, _eof) = self._pending_eof.get(key, (0, eof))
                self._pending_eof[key] = (recvd + 1, eof)
                try:
                    self._persist_pending_eof()
                except Exception as e:
                    self._log.error("Failed to persist pending EOF: %s", e)
                    # Don't return here - we still want to try to flush if possible
                    # The state is in memory even if persistence failed
                
                flush_info = self._check_and_flush_eof(key)

            if flush_info:
                try:
                    self._flush_eof(*flush_info)
                except Exception as e:
                    self._log.error("Failed to flush EOF: %s", e, exc_info=True)
                    # Rollback received trace on flush failure - EOF will be redelivered
                    with self._state_lock:
                        self._received_eof_traces.discard(trace_key)
                    # Don't ack - let it be redelivered
                    raise
                # Clean up received traces for this key after successful flush
                with self._state_lock:
                    traces_to_remove = {t for t in self._received_eof_traces if t[0] == key[0] and t[1] == key[1]}
                    for t in traces_to_remove:
                        self._received_eof_traces.discard(t)
                    try:
                        self._persist_eof_state_atomic()
                    except Exception as e:
                        self._log.error("Failed to persist EOF state after flush: %s", e)
        except Exception as e:
            self._log.error("Error processing EOF: %s", e, exc_info=True)
            # Rollback received trace on error
            with self._state_lock:
                self._received_eof_traces.discard(trace_key)
            # Don't ack on error - let it be redelivered
            raise

        return (True, False)

    def _check_and_flush_eof(self, key: tuple[TableName, str]) -> Optional[tuple]:
        """
        Checks if EOF for `key` can be flushed. Only for light tables.
        Flush when all orch_workers have sent EOF.
        This method MUST be called with _state_lock held.
        """
        (recvd, eof) = self._pending_eof.get(key, (0, None))

        if eof is None or recvd < self._orch_workers:
            if eof is not None:
                self._log.info("TABLE_EOF deferred: key=%s recvd=%d orch_workers=%d", 
                             key, recvd, self._orch_workers)
            return None

        self._log.info("TABLE_EOF ready to be flushed for key: %s", key)
        
        self._pending_eof.pop(key, None)
        
        total_parts = max(1, int(self._cfg.aggregators))
        
        return (key, total_parts)

    def _flush_eof(self, key: tuple[TableName, str], total_parts: int):
        """
        Sends EOF messages to aggregators and cleans up state.
        This method is called outside of the state lock.
        Only ACKs and cleans up state if ALL partitions successfully receive EOF.
        Raises exception if any partition fails to trigger redelivery.
        """
        self._log.info("TABLE_EOF -> aggregators: key=%s parts=%d", key, total_parts)
        failed_parts = []
        successful_parts = []
        
        for part in range(total_parts):
            try:
                self._p.send_table_eof_to_aggregator_partition(key, part)
                successful_parts.append(part)
            except Exception as e:
                failed_parts.append(part)
                self._log.error(
                    "send_table_eof_to_aggregator_partition failed part=%d key=%s: %s",
                    part, key, e
                )
        
        # Only proceed if ALL partitions succeeded
        if failed_parts:
            self._log.error(
                "EOF flush partially failed: key=%s successful_parts=%s failed_parts=%s total_parts=%d. "
                "Will not ACK or clean up state - EOF will be redelivered.",
                key, successful_parts, failed_parts, total_parts
            )
            raise RuntimeError(
                f"Failed to send EOF to {len(failed_parts)}/{total_parts} aggregator partitions "
                f"for key={key}: failed_parts={failed_parts}"
            )
        
        # All partitions succeeded - ACK and clean up state
        self._log.info("EOF successfully sent to all %d aggregator partitions: key=%s", total_parts, key)
        # Manually ACK all EOF messages for this key
        self._ack_eof(key)
        
        with self._state_lock:
            # Clean up received EOF traces for this key
            traces_to_remove = {t for t in self._received_eof_traces if t[0] == key[0] and t[1] == key[1]}
            for t in traces_to_remove:
                self._received_eof_traces.discard(t)
            
            try:
                self._persist_eof_state_atomic()
            except Exception as e:
                self._log.warning("Failed to clean up persisted EOF state after flush: %s", e)
    
    def _ack_eof(self, key: tuple[TableName, str]) -> None:
        """Acknowledge all EOF messages for this key after they have been fully processed."""
        with self._eof_ack_lock:
            ack_list = self._unacked_eofs.get(key, [])
            if ack_list:
                self._log.info("ACKing %d TABLE_EOF messages: key=%s", len(ack_list), key)
                acked_count = 0
                failed_count = 0
                for channel, delivery_tag in ack_list:
                    try:
                        if channel is None:
                            self._log.warning("Channel is None for EOF key=%s delivery_tag=%s - skipping ACK", key, delivery_tag)
                            failed_count += 1
                            continue
                        if not hasattr(channel, 'is_open') or not channel.is_open:
                            self._log.warning("Channel is closed for EOF key=%s delivery_tag=%s - message will be redelivered", key, delivery_tag)
                            failed_count += 1
                            continue
                        channel.basic_ack(delivery_tag=delivery_tag)
                        acked_count += 1
                    except Exception as e:
                        self._log.error(
                            "Failed to ACK EOF key=%s delivery_tag=%s (will be redelivered): %s",
                            key, delivery_tag, e
                        )
                        failed_count += 1
                
                if failed_count > 0:
                    self._log.warning("Failed to ACK %d/%d EOF messages for key=%s - they will be redelivered", 
                                    failed_count, len(ack_list), key)
                
                del self._unacked_eofs[key]
    
    def _start_batch_ack_thread(self) -> None:
        """Start background thread for timeout-based batch acknowledgment."""
        self._batch_ack_thread = threading.Thread(
            target=self._ack_old_batches,
            daemon=True,
            name="batch-ack-thread"
        )
        self._batch_ack_thread.start()
        self._log.info("Batch ACK thread started (timeout=%ds)", self._batch_ack_timeout)
    
    def _ack_old_batches(self) -> None:
        """
        Background thread that periodically ACKs batches older than timeout.
        This prevents unbounded redelivery while still protecting against loss during crashes.
        Optimized to use multiple=True to reduce callback overhead.
        """
        check_interval = 5  # Check every 5 seconds
        while not self._stop_event.is_set():
            try:
                time.sleep(check_interval)
                now = time.time()
                
                with self._batch_ack_lock:
                    # 1. Identify max_tags per channel for timed-out batches and invalid entries
                    acks_by_channel = {} # channel -> max_tag
                    to_remove = set()
                    
                    for delivery_tag, (channel, client_id, batch_number, timestamp) in self._unacked_batches.items():
                        # Check for invalid channel first
                        if channel is None:
                             self._log.warning("Channel is None for batch client=%s bn=%s tag=%s - skipping ACK", client_id, batch_number, delivery_tag)
                             to_remove.add(delivery_tag)
                             continue
                        if not hasattr(channel, 'is_open') or not channel.is_open:
                             self._log.warning("Channel is closed for batch client=%s bn=%s tag=%s - message will be redelivered", client_id, batch_number, delivery_tag)
                             to_remove.add(delivery_tag)
                             continue

                        age = now - timestamp
                        if age > self._batch_ack_timeout:
                            current_max = acks_by_channel.get(channel, 0)
                            if delivery_tag > current_max:
                                acks_by_channel[channel] = delivery_tag
                    
                    # 2. Schedule batched ACKs
                    for channel, max_tag in acks_by_channel.items():
                        try:
                            # Use add_callback_threadsafe to ACK from background thread
                            # multiple=True ACKs this tag and all prior unacked tags on this channel
                            cb = functools.partial(channel.basic_ack, delivery_tag=max_tag, multiple=True)
                            channel.connection.add_callback_threadsafe(cb)
                            self._log.debug("Timeout batched ACK: max_tag=%s", max_tag)
                        except Exception as e:
                            self._log.error("Failed to schedule batched ACK max_tag=%s: %s", max_tag, e)
                            # If scheduling fails, we still remove them to avoid retry loop (consistent with previous logic)
                            # and because the channel might be in a bad state anyway.
                    
                    # 3. Remove ACKed entries (<= max_tag) and invalid entries
                    for delivery_tag, (channel, _, _, _) in self._unacked_batches.items():
                        if channel in acks_by_channel:
                            if delivery_tag <= acks_by_channel[channel]:
                                to_remove.add(delivery_tag)
                    
                    for tag in to_remove:
                        self._unacked_batches.pop(tag, None)
                        
            except Exception as e:
                if not self._stop_event.is_set():
                    self._log.error("Error in batch ACK thread: %s", e, exc_info=True)
        
        self._log.info("Batch ACK thread stopped")
    
    def stop_batch_ack_thread(self) -> None:
        """Stop the batch ACK thread and ACK all pending batches."""
        self._log.info("Stopping batch ACK thread...")
        self._stop_event.set()
        
        if self._batch_ack_thread and self._batch_ack_thread.is_alive():
            self._batch_ack_thread.join(timeout=10)
        
        # ACK all remaining batches on shutdown
        with self._batch_ack_lock:
            if self._unacked_batches:
                self._log.info("ACKing %d remaining batches on shutdown", len(self._unacked_batches))
                for delivery_tag, (channel, client_id, batch_number, _) in list(self._unacked_batches.items()):
                    try:
                        if channel and hasattr(channel, 'is_open') and channel.is_open:
                            cb = functools.partial(channel.basic_ack, delivery_tag=delivery_tag)
                            channel.connection.add_callback_threadsafe(cb)
                            self._log.debug("Shutdown ACK batch: client=%s bn=%s tag=%s", client_id, batch_number, delivery_tag)
                    except Exception as e:
                        self._log.error("Failed to schedule ACK batch on shutdown client=%s bn=%s tag=%s: %s", client_id, batch_number, delivery_tag, e)
                self._unacked_batches.clear()
        
        self._log.info("Batch ACK thread stopped and all batches ACKed")
    
    


class ExchangeBusProducer:
    def __init__(
        self,
        host: str,
        filters_pool_queue: str,
        in_mw: MessageMiddlewareExchange,
        exchange_fmt: str = "ex.{table}",
        rk_fmt: str = "agg.{table}.{pid:02d}",
        *,
        router_id: int = 0,
        max_retries: int = 5,
        base_backoff_ms: int = 100,
        backoff_multiplier: float = 2.0,
    ):
        self._log = logging.getLogger("filter-router.bus")
        self._host = host
        self._filters_pub = MessageMiddlewareQueue(host, filters_pool_queue)
        self._exchange_fmt = exchange_fmt
        self._rk_fmt = rk_fmt
        self._pub_cache: dict[tuple[TableName, str], MessageMiddlewareExchange] = {}
        self._pub_locks: dict[tuple[TableName, str], threading.Lock] = {}
        self._in_mw = in_mw
        self._router_id = router_id

        self._max_retries = int(max_retries)
        self._base_backoff_ms = int(base_backoff_ms)
        self._backoff_multiplier = float(backoff_multiplier)

    def shutdown(self):
        """Closes all active publisher connections."""
        self._log.info("Shutting down ExchangeBusProducer...")

        try:
            self._filters_pub.close()
        except Exception as e:
            self._log.warning(f"Error closing filters_pool publisher: {e}")

        for pub in self._pub_cache.values():
            try:
                pub.close()
            except Exception as e:
                self._log.warning(f"Error closing cached publisher for key {pub}: {e}")

        self._pub_cache.clear()
        self._log.info("ExchangeBusProducer shutdown complete.")

    def _key_for(self, table_name: TableName, pid: int) -> tuple[TableName, str]:
        table = TABLE_NAME_TO_STR[table_name]
        ex = self._exchange_fmt.format(table=table)
        rk = self._rk_fmt.format(table=table, pid=pid)
        return (ex, rk)

    def _get_pub(self, table_name: str, pid: int) -> MessageMiddlewareExchange:
        table = TABLE_NAME_TO_STR[table_name]
        key = self._key_for(table, pid)
        pub = self._pub_cache.get(key)

        if pub is not None and getattr(pub, "is_closed", None):
            try:
                if pub.is_closed():
                    self._log.debug("publisher cached but closed: %s → recreate", key)
                    self._drop_pub(key)
                    pub = None
            except Exception:
                pass

        if pub is None:
            self._log.debug(
                "create publisher exchange=%s rk=%s host=%s", key[0], key[1], self._host
            )
            pub = MessageMiddlewareExchange(
                host=self._host, exchange_name=key[0], route_keys=[key[1]]
            )
            self._pub_cache[key] = pub
            if key not in self._pub_locks:
                self._pub_locks[key] = threading.Lock()
        return pub

    def _drop_pub(self, key: tuple[TableName, str]) -> None:
        pub = self._pub_cache.pop(key, None)
        if pub is not None:
            try:
                if hasattr(pub, "close"):
                    pub.close()
            except Exception:
                pass

    def _send_with_retry(self, key: tuple[TableName, str], payload: bytes) -> None:
        """
        Envía `payload` al exchange/rk indicado por `key`, con reintentos y recreación del publisher.
        Bloquea por key para serializar accesos concurrentes al mismo canal.
        """
        self._log.debug("send_with_retry key=%s payload_size=%d", key, len(payload))
        lock = self._pub_locks.setdefault(key, threading.Lock())
        with lock:
            attempt = 0
            backoff = self._base_backoff_ms / 1000.0
            last_error = None

            while attempt <= self._max_retries:
                try:
                    pub = self._pub_cache.get(key)
                    if pub is None:
                        self._log.debug("pub cache miss -> create: %s", key)
                        pub = MessageMiddlewareExchange(
                            host=self._host, exchange_name=key[0], route_keys=[key[1]]
                        )
                        self._pub_cache[key] = pub
                    pub.send(payload)
                    return
                except Exception as e:
                    last_error = e
                    self._log.warning(
                        "send failed (attempt %d/%d) key=%s: %s",
                        attempt + 1,
                        self._max_retries,
                        key,
                        e,
                    )
                    self._drop_pub(key)

                    attempt += 1
                    if attempt > self._max_retries:
                        break

                    jitter = random.uniform(0, backoff * 0.2)
                    sleep_s = backoff + jitter
                    time.sleep(sleep_s)
                    backoff *= self._backoff_multiplier

            raise RuntimeError(
                f"Error sending message to {key}: retries exhausted"
            ) from last_error

    def send_to_filters_pool(self, batch: DataBatch) -> None:
        try:
            self._log.debug("publish → filters_pool")
            envelope = Envelope(type=MessageType.DATA_BATCH, data_batch=batch)
            raw = envelope.SerializeToString()
            self._filters_pub.send(raw)
        except Exception as e:
            self._log.error("filters_pool send failed: %s", e)
            raise

    def send_to_aggregator_partition(self, partition_id: int, batch: DataBatch) -> None:
        table = batch.payload.name
        key = self._key_for(table, int(partition_id))
        try:
            self._log.debug(
                "publish → aggregator table=%s part=%d", table, int(partition_id)
            )
            envelope = Envelope(type=MessageType.DATA_BATCH, data_batch=batch)
            raw = envelope.SerializeToString()
            self._send_with_retry(key, raw)
        except Exception as e:
            self._log.error(
                "aggregator send failed table=%s part=%d: %s",
                table,
                int(partition_id),
                e,
            )
            # CRITICAL: Re-raise exception so caller knows send failed
            # If we don't re-raise, the batch will be ACKed even though it wasn't sent,
            # causing batch loss
            raise

    def requeue_to_router(self, batch: DataBatch) -> None:
        env = Envelope(type=MessageType.DATA_BATCH, data_batch=batch)
        try:
            self._log.info(
                "requeue_to_router: reinjecting batch table=%s queries=%s",
                batch.payload.name,
                batch.query_ids,
            )
            raw = env.SerializeToString()
            self._in_mw.send(raw)
        except Exception as e:
            self._log.error("requeue_to_router failed: %s", e)

    def send_table_eof_to_aggregator_partition(
        self, key: tuple[TableName, str], partition_id: int
    ) -> None:
        try:
            # Add trace: "filter_router_id:aggregator_id"
            trace = f"{self._router_id}:{partition_id}"
            eof = EOFMessage(table=key[0], client_id=key[1], trace=trace)
            env = Envelope(type=MessageType.EOF_MESSAGE, eof=eof)
            payload = env.SerializeToString()
            k = self._key_for(key[0], int(partition_id))
            self._log.info(
                "publish TABLE_EOF → aggregator key=%s part=%d trace=%s", key, int(partition_id), trace
            )
            self._send_with_retry(k, payload)
        except Exception as e:
            self._log.error(
                "aggregator TABLE_EOF send failed key=%s part=%d: %s",
                key,
                int(partition_id),
                e,
            )


class RouterServer:
    def __init__(
        self,
        host: str,
        router_in: MessageMiddlewareExchange,
        producer: ExchangeBusProducer,
        policy: QueryPolicyResolver,
        table_cfg: TableConfig,
        stop_event: threading.Event,
        orch_workers: int,
        persistence_dir: str = "/tmp/filter_router_state",
        router_id: int = 0,
    ):
        self._producer = producer
        self._mw_in = router_in
        self._router = FilterRouter(
            producer=producer,
            policy=policy,
            table_cfg=table_cfg,
            orch_workers=orch_workers,
            persistence_dir=persistence_dir,
            router_id=router_id,
        )
        self._log = logging.getLogger("filter-router-server")
        self._stop_event = stop_event

    def run(self) -> None:
        self._log.debug("RouterServer starting consume")

        def _cb(body: bytes, channel=None, delivery_tag=None, redelivered=False):
            if self._stop_event.is_set():
                self._log.warning("Shutdown in progress, skipping incoming message.")
                return True  # Auto-ack to avoid redelivery during shutdown
            try:
                if len(body) < 1:
                    self._log.error("Received empty message")
                    return True  # Ack empty messages
                msg = Envelope()
                msg.ParseFromString(body)
                should_ack, ack_now = self._router.process_message(msg, channel, delivery_tag, redelivered)
                return ack_now  # Return whether to ack immediately
            except Exception as e:
                self._log.exception("Error in router callback: %s", e)
                return False  # NACK to trigger redelivery on transient errors

        try:
            self._mw_in.start_consuming(_cb)
            self._log.info("RouterServer consuming (thread started)")
        except Exception as e:
            self._log.exception("start_consuming failed: %s", e)

    def stop(self) -> None:
        """Stops the consumer and shuts down the producer."""
        self._log.info("Stopping Filter Router Server...")

        try:
            # Stop batch ACK thread first to ensure all pending batches are ACKed
            self._router.stop_batch_ack_thread()
        except Exception as e:
            self._log.warning(f"Error stopping batch ACK thread: {e}")

        try:
            self._mw_in.stop_consuming()
            self._log.info("Input consumer stopped.")
        except Exception as e:
            self._log.warning(f"Error stopping input consumer: {e}")

        try:
            self._producer.shutdown()
        except Exception as e:
            self._log.warning(f"Error during producer shutdown: {e}")

        self._log.info("Filter Router Server stopped.")
