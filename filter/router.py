from __future__ import annotations

import json
import logging
import os
import random
import shutil
import threading
import time
from collections import defaultdict
from random import randint
from typing import Dict, Optional

from middleware.middleware_client import (
    MessageMiddlewareExchange,
    MessageMiddlewareQueue,
)
from protocol2.clean_up_message_pb2 import CleanUpMessage
from protocol2.databatch_pb2 import DataBatch, Query
from protocol2.envelope_pb2 import Envelope, MessageType
from protocol2.eof_message_pb2 import EOFMessage
from protocol2.table_data_pb2 import Row, TableData, TableName, TableSchema, TableStatus
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
    def __init__(
        self,
        producer: "ExchangeBusProducer",
        policy: QueryPolicyResolver,
        table_cfg: TableConfig,
        orch_workers: int,
        router_id: int = 0,
    ):
        self._p = producer
        self._pol = policy
        self._cfg = table_cfg
        self._log = logging.getLogger("filter-router")
        self._orch_workers = orch_workers
        self._router_id = router_id

        # Batch deduplication: track received batch numbers per (table, client_id, query_ids_tuple)
        # In-memory only (no persistence)
        # Use regular dict instead of defaultdict to ensure each key gets its own set
        self._received_batches: Dict[tuple[int, str, tuple], set[int]] = {}

        # Track pending EOFs for LIGHT_TABLES only: key=(table, client_id) -> (set of worker_ids, EOFMessage)
        # Only menu_items and stores tables need EOF broadcasting
        # Worker IDs are extracted from the trace field (format: "orch_worker_id")
        # EOFs remain in _pending_eof until client cleanup removes them
        self._pending_eof: Dict[tuple[TableName, str], tuple[set[str], EOFMessage]] = {}

        # Track pending cleanup messages: key=client_id -> set of worker_ids
        # Worker IDs are extracted from the trace field (format: "orch_worker_id")
        self._pending_cleanups: Dict[str, set[str]] = {}
        self._state_lock = threading.Lock()

        # Blacklist: client_ids that should have their batches discarded
        # Format: {client_id: timestamp} - timestamp is when the client was blacklisted
        self._blacklist: Dict[str, float] = {}
        self._blacklist_lock = threading.Lock()

        # State directory and file paths
        self._state_dir = os.getenv(
            "FILTER_ROUTER_STATE_DIR", "/tmp/filter_router_state"
        )
        os.makedirs(self._state_dir, exist_ok=True)
        self._blacklist_file = os.path.join(
            self._state_dir, f"blacklist_{router_id}.json"
        )
        self._eof_state_file = os.path.join(
            self._state_dir, f"eof_state_{router_id}.json"
        )
        self._cleanup_state_file = os.path.join(
            self._state_dir, f"cleanup_state_{router_id}.json"
        )

        # Load state at bootstrap
        self._load_and_clean_blacklist()
        self._load_eof_state()
        self._load_cleanup_state()

    def _load_and_clean_blacklist(self) -> None:
        """
        Load blacklist from file and remove entries older than 10 minutes.
        Called at bootstrap.
        """
        current_time = time.time()
        cutoff_time = current_time - 600  # 10 minutes ago

        # Load from file if it exists
        if os.path.exists(self._blacklist_file):
            try:
                with open(self._blacklist_file, "r") as f:
                    data = json.load(f)
                    # Filter out entries older than 10 minutes
                    self._blacklist = {
                        client_id: timestamp
                        for client_id, timestamp in data.items()
                        if timestamp > cutoff_time
                    }
                    self._log.info(
                        "Loaded blacklist: %d entries (removed %d old entries)",
                        len(self._blacklist),
                        len(data) - len(self._blacklist),
                    )
            except Exception as e:
                self._log.warning("Failed to load blacklist file: %s", e)
                self._blacklist = {}
        else:
            self._blacklist = {}
            self._log.info("Blacklist file not found, starting with empty blacklist")

        # Save cleaned blacklist back to file
        self._save_blacklist()

    def _save_blacklist(self) -> None:
        """Save blacklist to file."""
        try:
            with open(self._blacklist_file, "w") as f:
                json.dump(self._blacklist, f)
        except Exception as e:
            self._log.error("Failed to save blacklist file: %s", e)

    def _add_to_blacklist(self, client_id: str) -> None:
        """Add a client_id to the blacklist (both in memory and file)."""
        if not client_id:
            return

        current_time = time.time()
        with self._blacklist_lock:
            self._blacklist[client_id] = current_time
            self._save_blacklist()
            self._log.info("Added client_id to blacklist: %s", client_id)

    # ─────────────────────────────────────────────────────────────────────────────
    # EOF State Persistence
    # ─────────────────────────────────────────────────────────────────────────────

    def _load_eof_state(self) -> None:
        """Load EOF state from disk at startup."""
        if os.path.exists(self._eof_state_file):
            try:
                with open(self._eof_state_file, "r") as f:
                    data = json.load(f)
                    # Reconstruct pending_eof: {"table_client": ["worker1", "worker2"]}
                    for key_str, worker_list in data.items():
                        parts = key_str.split("_", 1)
                        if len(parts) == 2:
                            table = int(parts[0])
                            client_id = parts[1]
                            key = (table, client_id)
                            # Create placeholder EOFMessage (will be replaced on first real EOF)
                            eof_placeholder = EOFMessage(
                                table=table, client_id=client_id
                            )
                            self._pending_eof[key] = (set(worker_list), eof_placeholder)
                    self._log.info(
                        "Loaded EOF state: %d pending EOFs", len(self._pending_eof)
                    )
            except Exception as e:
                self._log.warning("Failed to load EOF state file: %s", e)

    def _save_eof_state(self) -> None:
        """Save EOF state to disk."""
        try:
            data = {}
            for (table, client_id), (workers, _eof) in self._pending_eof.items():
                key_str = f"{table}_{client_id}"
                data[key_str] = list(workers)
            with open(self._eof_state_file, "w") as f:
                json.dump(data, f)
        except Exception as e:
            self._log.error("Failed to save EOF state file: %s", e)

    # ─────────────────────────────────────────────────────────────────────────────
    # Cleanup State Persistence
    # ─────────────────────────────────────────────────────────────────────────────

    def _load_cleanup_state(self) -> None:
        """Load cleanup state from disk at startup."""
        if os.path.exists(self._cleanup_state_file):
            try:
                with open(self._cleanup_state_file, "r") as f:
                    data = json.load(f)
                    # Reconstruct pending_cleanups: {"client_id": ["worker1", "worker2"]}
                    for client_id, worker_list in data.items():
                        self._pending_cleanups[client_id] = set(worker_list)
                    self._log.info(
                        "Loaded cleanup state: %d pending cleanups",
                        len(self._pending_cleanups),
                    )
            except Exception as e:
                self._log.warning("Failed to load cleanup state file: %s", e)

    def _save_cleanup_state(self) -> None:
        """Save cleanup state to disk."""
        try:
            data = {
                client_id: list(workers)
                for client_id, workers in self._pending_cleanups.items()
            }
            with open(self._cleanup_state_file, "w") as f:
                json.dump(data, f)
        except Exception as e:
            self._log.error("Failed to save cleanup state file: %s", e)

    def _is_blacklisted(self, client_id: str) -> bool:
        """Check if a client_id is in the blacklist."""
        if not client_id:
            return False

        with self._blacklist_lock:
            return client_id in self._blacklist

    def process_message(
        self, msg: Envelope, channel=None, delivery_tag=None, redelivered=False
    ) -> tuple[bool, bool]:
        """
        Process a message and return (should_ack, ack_now).
        - should_ack: True if message should eventually be acked
        - ack_now: True if should ack immediately, False if should delay ack
        """
        self._log.debug(
            "Processing message: %r, message type: %r, redelivered: %r",
            msg,
            msg.type,
            redelivered,
        )
        if msg.type == MessageType.DATA_BATCH:
            self._handle_data(msg.data_batch)
            return (True, True)  # Regular messages: ack immediately
        elif msg.type == MessageType.EOF_MESSAGE:
            # EOF messages: delay ack until fully processed
            return self._handle_table_eof(msg.eof, channel, delivery_tag, redelivered)
        elif msg.type == MessageType.CLEAN_UP_MESSAGE:
            # Client cleanup: delay ack until fully processed
            return self._handle_client_cleanup_like(
                msg.clean_up, channel, delivery_tag, redelivered
            )
        else:
            self._log.warning("Unknown message type: %r", type(msg))
            return (True, True)

    def _is_duplicate_batch(self, batch: DataBatch) -> bool:
        """
        Check if a batch is a duplicate. Returns True if duplicate, False otherwise.
        Tracks batches by (table, client_id, query_ids_tuple, batch_number).
        Only deduplicates batches that are finished processing (no more filter steps).
        In-memory only - no persistence.
        """
        if batch.payload is None:
            self._log.warning("Cannot check duplicate: batch has no payload")
            return False

        # Convert protobuf enum to int for consistent hashing in tuple key
        table = int(batch.payload.name)
        # Ensure client_id is a string and not None
        client_id = str(batch.client_id) if batch.client_id else ""
        batch_number = int(batch.payload.batch_number)

        # Convert protobuf repeated field to list of integers for proper comparison
        queries = [int(q) for q in batch.query_ids]
        query_ids_tuple = tuple(sorted(queries))

        # Create immutable key tuple - ensure all elements are hashable
        dedup_key = (table, client_id, query_ids_tuple)

        # Validate key is hashable
        try:
            hash(dedup_key)
        except TypeError as e:
            self._log.error("Dedup key is not hashable: %s error=%s", dedup_key, e)
            return False

        with self._state_lock:
            # Get or create a set for this key - ensure each key gets its own independent set
            if dedup_key not in self._received_batches:
                self._received_batches[dedup_key] = set()
            received_set = self._received_batches[dedup_key]

            # if batch_number in received_set:
            #     self._log.warning(
            #         "DUPLICATE batch detected and discarded: table=%s bn=%s client=%s queries=%s",
            #         table, batch_number, client_id, queries
            #     )
            #     return True

            # Mark batch as received
            received_set.add(batch_number)
            self._log.debug(
                "Batch marked as received: table=%s bn=%s client=%s queries=%s",
                table,
                batch_number,
                client_id,
                queries,
            )
            return False

    def _handle_data(self, batch: DataBatch) -> None:
        table = batch.payload.name
        queries = batch.query_ids
        mask = batch.filter_steps
        bn = batch.payload.batch_number
        cid = batch.client_id

        if table is None:
            self._log.warning("Batch sin table_id válido. bn=%s", bn)
            return

        # Check blacklist for batches from filter workers (mask != 0)
        if mask != 0:
            if self._is_blacklisted(str(cid) if cid else ""):
                self._log.info(
                    "Discarding batch from blacklisted client: table=%s bn=%s cid=%s mask=%s",
                    table,
                    bn,
                    cid,
                    bin(mask),
                )
                return

        self._log.debug(
            "recv DataBatch table=%s queries=%s mask=%s bn=%s cid=%s",
            table,
            queries,
            bin(mask),
            bn,
            cid,
        )

        try:
            total_steps = self._pol.total_steps(table, queries)
            next_step = first_zero_bit(mask, total_steps)
            if next_step is not None and self._pol.steps_remaining(
                table, queries, steps_done=next_step
            ):
                new_mask = set_bit(mask, next_step)
                batch.filter_steps = new_mask
                self._log.debug(
                    "→ filters step=%d table=%s new_mask=%s",
                    next_step,
                    table,
                    bin(new_mask),
                )
                self._p.send_to_filters_pool(batch)
                return

            dup_count = int(self._pol.get_duplication_count(queries) or 1)
            if dup_count > 1:
                self._log.debug(
                    "Fan-out x%d table=%s queries=%s", dup_count, table, queries
                )
                fanout_batches = []
                for i in range(dup_count):
                    new_queries = self._pol.get_new_batch_queries(
                        table, queries, copy_number=i
                    ) or list(queries)
                    b = DataBatch()
                    b.CopyFrom(batch)
                    b.query_ids.clear()
                    b.query_ids.extend(new_queries)
                    fanout_batches.append(b)

                for b in fanout_batches:
                    self._handle_data(b)
                return

            # Check for duplicates only when batch is finished processing (ready to send to aggregator)
            # This prevents marking batches as duplicates when they're still going through filter steps
            if self._is_duplicate_batch(batch):
                return

            self._send_to_some_aggregator(batch)

        except Exception:
            self._log.exception("Error processing batch: table=%s bn=%s", table, bn)
            raise

    def _send_to_some_aggregator(self, batch: DataBatch) -> None:
        if batch.payload.name in [TableName.MENU_ITEMS, TableName.STORES]:
            self._log.debug(
                "_send_to_some_aggregator table=%s bn=%s",
                batch.payload.name,
                batch.payload.batch_number,
            )
        num_parts = max(1, int(self._cfg.aggregators))
        self._p.send_to_aggregator_partition(randint(0, num_parts - 1), batch)

    def _handle_table_eof(
        self, eof: EOFMessage, channel=None, delivery_tag=None, redelivered=False
    ) -> tuple[bool, bool]:
        key = (eof.table, eof.client_id)

        if redelivered:
            self._log.info("TABLE_EOF REDELIVERED: key=%s trace=%s", key, eof.trace)
        else:
            self._log.debug("TABLE_EOF received: key=%s trace=%s", key, eof.trace)

        # Only broadcast EOFs for LIGHT_TABLES (menu_items and stores)
        # These tables don't go through filters, so FIFO is assured
        if eof.table not in LIGHT_TABLES:
            self._log.debug("Ignoring EOF for non-light table: key=%s", key)
            return (True, True)  # Ack immediately for non-light tables

        flush_info = None
        with self._state_lock:
            # Extract worker ID from trace field (format: "orch_worker_id")
            worker_id = None
            if eof.trace:
                # Trace format from orchestrator worker: "orch_worker_id"
                # Extract the worker ID part
                if eof.trace.startswith("orch_"):
                    worker_id = eof.trace
                else:
                    # Handle cases where trace might have additional parts (e.g., "orch_0:filter_router_id:aggregator_id")
                    # Extract just the orchestrator worker part
                    parts = eof.trace.split(":")
                    for part in parts:
                        if part.startswith("orch_"):
                            worker_id = part
                            break
                    if not worker_id:
                        # Fallback: use the entire trace as worker_id if no "orch_" prefix found
                        worker_id = eof.trace

            if not worker_id:
                # Fallback: if no trace, use a default identifier (shouldn't happen with fix, but handle gracefully)
                self._log.warning(
                    "EOF without trace field, using fallback deduplication: key=%s", key
                )
                worker_id = f"unknown_{hash(eof.SerializeToString()) % 10000}"

            # Get or create the set of received worker IDs for this key
            (recvd_workers, _eof) = self._pending_eof.get(key, (set(), eof))

            # Check if this worker ID was already received (deduplication)
            if worker_id in recvd_workers:
                self._log.warning(
                    "Duplicate EOF ignored: key=%s worker_id=%s (already received from this worker)",
                    key,
                    worker_id,
                )
                return (True, True)  # ACK immediately - already processed this EOF

            # Check if EOF is already complete (all workers received) before adding this worker
            # This prevents re-broadcasting on duplicate messages after completion
            already_complete = len(recvd_workers) >= self._orch_workers
            if already_complete:
                self._log.info(
                    "EOF already complete for key=%s (received from %d workers), ignoring duplicate",
                    key,
                    len(recvd_workers),
                )
                return (True, True)

            # Add this worker ID to the set
            recvd_workers.add(worker_id)
            self._pending_eof[key] = (recvd_workers, eof)

            # Persist EOF state before ACKing
            self._save_eof_state()

            # Check if we've received EOF from all orchestrator workers
            # Keep entry in _pending_eof until cleanup removes it
            if len(recvd_workers) >= self._orch_workers:
                flush_info = (key, max(1, int(self._cfg.aggregators)))
                self._log.info(
                    "TABLE_EOF complete: key=%s received from %d/%d workers",
                    key,
                    len(recvd_workers),
                    self._orch_workers,
                )
            else:
                self._log.debug(
                    "TABLE_EOF deferred: key=%s received from %d/%d workers (worker_id=%s)",
                    key,
                    len(recvd_workers),
                    self._orch_workers,
                    worker_id,
                )

        if flush_info:
            self._flush_eof(*flush_info)

        return (True, True)  # ACK immediately - state is persisted to disk

    def _flush_eof(self, key: tuple[TableName, str], total_parts: int):
        """
        Sends EOF messages to aggregators and cleans up state.
        This method is called outside of the state lock.
        """
        self._log.info("TABLE_EOF -> aggregators: key=%s parts=%d", key, total_parts)
        for part in range(total_parts):
            try:
                self._p.send_table_eof_to_aggregator_partition(key, part)
            except Exception as e:
                self._log.error(
                    "send_table_eof_to_aggregator_partition failed part=%d key=%s: %s",
                    part,
                    key,
                    e,
                )

        # Save EOF state after flush
        # Note: entry remains in _pending_eof until cleanup removes it
        with self._state_lock:
            self._save_eof_state()

        # Clean up deduplication state for this table/client
        with self._state_lock:
            keys_to_remove = [
                k
                for k in self._received_batches.keys()
                if k[0] == key[0] and k[1] == key[1]
            ]
            for k in keys_to_remove:
                self._received_batches.pop(k, None)

    def _cleanup_client_state(self, client_id: str) -> None:
        """
        Clean all in-memory and persisted state for a given client_id.
        This includes:
        - Unacked EOF messages
        - Received batch deduplication state
        - Pending EOF state
        - Any persisted state on disk
        """
        if not client_id:
            self._log.warning("_cleanup_client_state called with empty client_id")
            return

        self._log.info(
            "action: cleanup_client_state | result: starting | client_id: %s", client_id
        )

        # Clean received batches deduplication state
        with self._state_lock:
            # Remove all entries where client_id matches (second element of tuple key)
            keys_to_remove = [
                key for key in self._received_batches.keys() if key[1] == client_id
            ]
            for key in keys_to_remove:
                del self._received_batches[key]

            # Clean pending EOF state
            keys_to_remove = [
                key for key in self._pending_eof.keys() if key[1] == client_id
            ]
            for key in keys_to_remove:
                del self._pending_eof[key]

            # Clean pending cleanups
            self._pending_cleanups.pop(client_id, None)

            # Save updated state to disk
            self._save_eof_state()
            self._save_cleanup_state()

        # Clean persisted state if it exists
        # Note: Currently filter router doesn't persist state, but this is here for future-proofing
        # If persistence is added later, this is where it should be cleaned
        state_dir = os.getenv("FILTER_ROUTER_STATE_DIR")
        if state_dir and os.path.exists(state_dir):
            try:
                # Look for any files/directories related to this client_id
                for item in os.listdir(state_dir):
                    if client_id in item:
                        item_path = os.path.join(state_dir, item)
                        try:
                            if os.path.isfile(item_path):
                                os.remove(item_path)
                                self._log.debug("Removed persisted file: %s", item_path)
                            elif os.path.isdir(item_path):
                                shutil.rmtree(item_path)
                                self._log.debug(
                                    "Removed persisted directory: %s", item_path
                                )
                        except Exception as e:
                            self._log.warning(
                                "Failed to remove persisted state %s: %s", item_path, e
                            )
            except Exception as e:
                self._log.warning("Failed to clean persisted state directory: %s", e)

        self._log.info(
            "action: cleanup_client_state | result: success | client_id: %s", client_id
        )

    def _handle_client_cleanup_like(
        self, cleanup_msg, channel=None, delivery_tag=None, redelivered=False
    ) -> tuple[bool, bool]:
        """
        Handle cleanup message similar to EOF. Returns (should_ack, ack_now).
        - should_ack: True if message should eventually be acked
        - ack_now: True if should ack immediately, False if should delay ack
        """
        client_id = cleanup_msg.client_id if cleanup_msg.client_id else ""
        if not client_id:
            self._log.warning("CLEANUP without valid client_id; ignoring")
            return (True, True)

        if redelivered:
            self._log.info(
                "CLEANUP REDELIVERED: client_id=%s trace=%s",
                client_id,
                cleanup_msg.trace,
            )
        else:
            self._log.debug(
                "CLEANUP received: client_id=%s trace=%s", client_id, cleanup_msg.trace
            )

        # Check blacklist - if client is already blacklisted, just ACK
        if self._is_blacklisted(client_id):
            self._log.info(
                "CLEANUP for already blacklisted client_id=%s, ACKing immediately",
                client_id,
            )
            return (True, True)

        flush_info = None
        with self._state_lock:
            worker_id = None
            if cleanup_msg.trace:
                if cleanup_msg.trace.startswith("orch_"):
                    worker_id = cleanup_msg.trace
                else:
                    worker_id = cleanup_msg.trace
                    self._log.warning(
                        "CLEANUP trace format unexpected: client_id=%s trace=%s",
                        client_id,
                        cleanup_msg.trace,
                    )
            else:
                self._log.warning(
                    "CLEANUP received without trace: client_id=%s", client_id
                )
                worker_id = (
                    f"unknown_{len(self._pending_cleanups.get(client_id, set()))}"
                )

            recvd_workers = self._pending_cleanups.get(client_id, set())

            if worker_id in recvd_workers:
                self._log.warning(
                    "Duplicate CLEANUP ignored: client_id=%s worker_id=%s (already received from this worker)",
                    client_id,
                    worker_id,
                )
                return (True, True)  # ACK immediately - already processed this cleanup

            recvd_workers.add(worker_id)
            self._pending_cleanups[client_id] = recvd_workers

            # Persist cleanup state before ACKing
            self._save_cleanup_state()

            if len(recvd_workers) >= self._orch_workers:
                self._add_to_blacklist(client_id)
                flush_info = (client_id, max(1, int(self._cfg.aggregators)))
                self._pending_cleanups.pop(client_id, None)
                self._log.info(
                    "CLEANUP complete: client_id=%s received from %d/%d workers",
                    client_id,
                    len(recvd_workers),
                    self._orch_workers,
                )
            else:
                self._log.debug(
                    "CLEANUP deferred: client_id=%s received from %d/%d workers (worker_id=%s)",
                    client_id,
                    len(recvd_workers),
                    self._orch_workers,
                    worker_id,
                )

        if flush_info:
            self._flush_cleanup(*flush_info)

        return (True, True)  # ACK immediately - state is persisted to disk

    def _flush_cleanup(self, client_id: str, total_parts: int):
        """
        Sends cleanup messages to aggregators and cleans up state.
        This method is called outside of the state lock.
        """
        self._log.info(
            "CLEANUP -> aggregators: client_id=%s parts=%d", client_id, total_parts
        )

        self._cleanup_client_state(client_id)

        try:
            self._p.send_cleanup_to_aggregator_partition(
                randint(0, total_parts - 1), client_id
            )
        except Exception as e:
            self._log.error(
                "Failed to send CLEANUP to aggregator client_id=%s: %s",
                client_id,
                e,
            )
            raise

        with self._state_lock:
            self._save_cleanup_state()


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
                "publish TABLE_EOF → aggregator key=%s part=%d trace=%s",
                key,
                int(partition_id),
                trace,
            )
            self._send_with_retry(k, payload)
        except Exception as e:
            self._log.error(
                "aggregator TABLE_EOF send failed key=%s part=%d: %s",
                key,
                int(partition_id),
                e,
            )

    def send_cleanup_to_aggregator_partition(
        self, partition_id: int, client_id: str
    ) -> None:
        """
        Send client cleanup message to a random aggregator partition.
        The aggregator will broadcast this to joiner routers.
        """
        cleanup_msg = CleanUpMessage()
        cleanup_msg.client_id = client_id
        cleanup_msg.trace = f"{self._router_id}:{partition_id}"
        key = self._key_for(TableName.MENU_ITEMS, int(partition_id))
        try:
            env = Envelope(type=MessageType.CLEAN_UP_MESSAGE, clean_up=cleanup_msg)
            payload = env.SerializeToString()
            self._log.info(
                "publish CLEAN_UP_MESSAGE → aggregator part=%d client_id=%s",
                int(partition_id),
                cleanup_msg.client_id,
            )
            self._send_with_retry(key, payload)
        except Exception as e:
            self._log.error(
                "aggregator CLEAN_UP_MESSAGE send failed part=%d client_id=%s: %s",
                int(partition_id),
                cleanup_msg.client_id,
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
        router_id: int = 0,
    ):
        self._producer = producer
        self._mw_in = router_in
        self._router = FilterRouter(
            producer=producer,
            policy=policy,
            table_cfg=table_cfg,
            orch_workers=orch_workers,
            router_id=router_id,
        )
        self._log = logging.getLogger("filter-router-server")
        self._stop_event = stop_event

    def run(self) -> None:
        self._log.debug("RouterServer starting consume")

        def _cb(body: bytes, channel=None, delivery_tag=None, redelivered=False):
            if self._stop_event.is_set():
                self._log.warning(
                    "Shutdown in progress, NACKing message for redelivery."
                )
                if channel is not None and delivery_tag is not None:
                    try:
                        channel.basic_nack(delivery_tag=delivery_tag, requeue=True)
                    except Exception as e:
                        self._log.warning("NACK failed during shutdown: %s", e)
                return False
            try:
                if len(body) < 1:
                    self._log.error("Received empty message")
                    return True  # Ack empty messages
                msg = Envelope()
                msg.ParseFromString(body)
                should_ack, ack_now = self._router.process_message(
                    msg, channel, delivery_tag, redelivered
                )
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
            self._mw_in.stop_consuming()
            self._log.info("Input consumer stopped.")
        except Exception as e:
            self._log.warning(f"Error stopping input consumer: {e}")

        try:
            self._producer.shutdown()
        except Exception as e:
            self._log.warning(f"Error during producer shutdown: {e}")

        self._log.info("Filter Router Server stopped.")
