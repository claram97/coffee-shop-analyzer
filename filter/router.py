from __future__ import annotations

import json
import logging
import os
import random
import shutil
import threading
import time
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
from protocol2.table_data_pb2 import TableName

TABLE_NAME_TO_STR = {
    TableName.MENU_ITEMS: "menu_items",
    TableName.STORES: "stores",
    TableName.TRANSACTION_ITEMS: "transaction_items",
    TableName.TRANSACTIONS: "transactions",
    TableName.USERS: "users",
}
LIGHT_TABLES = {TableName.MENU_ITEMS, TableName.STORES}

BLACKLIST_RETENTION_SECONDS = 10 * 60
BACKOFF_JITTER_FACTOR = 0.2
UNKNOWN_WORKER_HASH_MOD = 10000
MIN_AGGREGATOR_PARTS = 1
ORCH_PREFIX = "orch_"


def is_bit_set(mask: int, idx: int) -> bool:
    """Check if a specific bit is set in a bitmask.

    Args:
        mask: The bitmask to check.
        idx: The zero-based index of the bit to check.

    Returns:
        True if the bit at index `idx` is set, False otherwise.
    """
    return ((mask >> idx) & 1) == 1


def set_bit(mask: int, idx: int) -> int:
    """Set a specific bit in a bitmask.

    Args:
        mask: The original bitmask.
        idx: The zero-based index of the bit to set.

    Returns:
        A new bitmask with the bit at index `idx` set.
    """
    return mask | (1 << idx)


def first_zero_bit(mask: int, total_bits: int) -> Optional[int]:
    """Find the index of the first unset (zero) bit in a bitmask.

    Args:
        mask: The bitmask to search.
        total_bits: The total number of bits to check.

    Returns:
        The index of the first zero bit, or None if all bits are set.
    """
    for i in range(total_bits):
        if not is_bit_set(mask, i):
            return i
    return None


class TableConfig:
    """Configuration for table-specific routing parameters.

    Attributes:
        aggregators: Number of aggregator partitions to route batches to.
    """

    def __init__(self, aggregators: int):
        """Initialize table configuration.

        Args:
            aggregators: Number of aggregator partitions.
        """
        self.aggregators = aggregators


class QueryPolicyResolver:
    """Resolves query routing policies for different table and query combinations.

    Determines how batches should be routed, duplicated, and processed based on
    the table type and queries involved.
    """

    def steps_remaining(
        self, batch_table_name: TableName, batch_queries: list[Query], steps_done: int
    ) -> bool:
        """Check if there are remaining filter steps for a batch.

        Args:
            batch_table_name: The table name for the batch.
            batch_queries: List of queries associated with the batch.
            steps_done: Number of filter steps already completed.

        Returns:
            True if there are remaining filter steps, False otherwise.
        """
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
        """Get the number of times a batch should be duplicated for fan-out.

        Args:
            batch_queries: List of queries associated with the batch.

        Returns:
            Number of copies to create (2 if multiple queries, 1 otherwise).
        """
        return 2 if len(batch_queries) > 1 else 1

    def get_new_batch_queries(
        self, batch_table_name: TableName, batch_queries: list[Query], copy_number: int
    ) -> list[Query]:
        """Get the queries for a specific copy when fanning out batches.

        Args:
            batch_table_name: The table name for the batch.
            batch_queries: Original list of queries.
            copy_number: Zero-based index of the copy being created.

        Returns:
            List of queries for this specific copy.
        """
        if batch_table_name == TableName.TRANSACTIONS:
            if len(batch_queries) == 3:
                return [Query.Q4] if copy_number == 1 else [Query.Q1, Query.Q3]
            if len(batch_queries) == 2:
                return [Query.Q3] if copy_number == 1 else [Query.Q1]
        return list(batch_queries)

    def total_steps(
        self, batch_table_name: TableName, batch_queries: list[Query]
    ) -> int:
        """Get the total number of filter steps required for a batch.

        Args:
            batch_table_name: The table name for the batch.
            batch_queries: List of queries associated with the batch.

        Returns:
            Total number of filter steps required.
        """
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
    """Routes data batches through filter and aggregator stages.

    Handles message routing, EOF coordination, client cleanup, and blacklist
    management. Maintains persistent state for recovery and deduplication.
    """

    def __init__(
        self,
        producer: "ExchangeBusProducer",
        policy: QueryPolicyResolver,
        table_cfg: TableConfig,
        orch_workers: int,
        router_id: int = 0,
    ):
        """Initialize the filter router.

        Args:
            producer: Producer for sending messages to filters and aggregators.
            policy: Policy resolver for query routing decisions.
            table_cfg: Configuration for table-specific parameters.
            orch_workers: Number of orchestrator workers to wait for EOF/cleanup.
            router_id: Unique identifier for this router instance.
        """
        self._p = producer
        self._pol = policy
        self._cfg = table_cfg
        self._log = logging.getLogger("filter-router")
        self._orch_workers = orch_workers
        self._router_id = router_id

        self._init_state_containers()
        self._configure_state_paths(router_id)
        self._load_initial_state()

    def _init_state_containers(self) -> None:
        """Initialize in-memory state containers for EOF tracking and blacklist."""
        self._pending_eof: Dict[tuple[TableName, str], tuple[set[str], EOFMessage]] = {}
        self._pending_cleanups: Dict[str, set[str]] = {}
        self._state_lock = threading.Lock()

        self._blacklist: Dict[str, float] = {}
        self._blacklist_lock = threading.Lock()

    def _configure_state_paths(self, router_id: int) -> None:
        """Configure file paths for persistent state storage.

        Args:
            router_id: Router ID used to create unique state file names.
        """
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

    def _load_initial_state(self) -> None:
        """Load all persistent state from disk at startup."""
        self._load_and_clean_blacklist()
        self._load_eof_state()
        self._load_cleanup_state()

    def _load_and_clean_blacklist(self) -> None:
        """Load blacklist from file and remove entries older than retention period.

        Called at bootstrap to restore blacklist state and clean up expired entries.
        Entries older than BLACKLIST_RETENTION_SECONDS are automatically removed.
        """
        current_time = time.time()
        cutoff_time = current_time - BLACKLIST_RETENTION_SECONDS

        if os.path.exists(self._blacklist_file):
            try:
                with open(self._blacklist_file, "r") as f:
                    data = json.load(f)
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

        self._save_blacklist()

    def _save_blacklist(self) -> None:
        """Persist the current blacklist state to disk.

        Logs errors but does not raise exceptions to avoid disrupting message processing.
        """
        try:
            with open(self._blacklist_file, "w") as f:
                json.dump(self._blacklist, f)
        except Exception as e:
            self._log.error("Failed to save blacklist file: %s", e)

    def _add_to_blacklist(self, client_id: str) -> None:
        """Add a client_id to the blacklist with current timestamp.

        The client will be blocked from sending further batches. The entry is
        persisted to disk and will expire after BLACKLIST_RETENTION_SECONDS.

        Args:
            client_id: The client identifier to blacklist. Empty strings are ignored.
        """
        if not client_id:
            return

        current_time = time.time()
        with self._blacklist_lock:
            self._blacklist[client_id] = current_time
            self._save_blacklist()
            self._log.info("Added client_id to blacklist: %s", client_id)

    def _load_eof_state(self) -> None:
        """Load pending EOF state from disk at startup.

        Restores the set of workers that have already sent EOF messages for
        each (table, client_id) pair, allowing the router to resume EOF
        coordination after a restart.
        """
        if os.path.exists(self._eof_state_file):
            try:
                with open(self._eof_state_file, "r") as f:
                    data = json.load(f)
                    for key_str, worker_list in data.items():
                        parts = key_str.split("_", 1)
                        if len(parts) != 2:
                            continue
                        table = int(parts[0])
                        client_id = parts[1]
                        key = (table, client_id)
                        eof_placeholder = EOFMessage(table=table, client_id=client_id)
                        self._pending_eof[key] = (set(worker_list), eof_placeholder)
                    self._log.info(
                        "Loaded EOF state: %d pending EOFs", len(self._pending_eof)
                    )
            except Exception as e:
                self._log.warning("Failed to load EOF state file: %s", e)

    def _save_eof_state(self) -> None:
        """Persist pending EOF state to disk.

        Saves the set of workers that have sent EOF for each (table, client_id)
        pair. Logs errors but does not raise exceptions.
        """
        try:
            data = {}
            for (table, client_id), (workers, _eof) in self._pending_eof.items():
                key_str = f"{table}_{client_id}"
                data[key_str] = list(workers)
            with open(self._eof_state_file, "w") as f:
                json.dump(data, f)
        except Exception as e:
            self._log.error("Failed to save EOF state file: %s", e)

    def _load_cleanup_state(self) -> None:
        """Load pending cleanup state from disk at startup.

        Restores the set of workers that have already sent cleanup messages
        for each client_id, allowing the router to resume cleanup coordination
        after a restart.
        """
        if os.path.exists(self._cleanup_state_file):
            try:
                with open(self._cleanup_state_file, "r") as f:
                    data = json.load(f)
                    for client_id, worker_list in data.items():
                        self._pending_cleanups[client_id] = set(worker_list)
                    self._log.info(
                        "Loaded cleanup state: %d pending cleanups",
                        len(self._pending_cleanups),
                    )
            except Exception as e:
                self._log.warning("Failed to load cleanup state file: %s", e)

    def _save_cleanup_state(self) -> None:
        """Persist pending cleanup state to disk.

        Saves the set of workers that have sent cleanup messages for each
        client_id. Logs errors but does not raise exceptions.
        """
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
        """Check if a client_id is currently blacklisted.

        Args:
            client_id: The client identifier to check.

        Returns:
            True if the client is blacklisted, False otherwise.
            Empty or None client_ids are never blacklisted.
        """
        if not client_id:
            return False

        with self._blacklist_lock:
            return client_id in self._blacklist

    def process_message(self, msg: Envelope) -> bool:
        """Process an incoming message and determine if it should be acknowledged.

        Routes messages to appropriate handlers based on message type:
        - DATA_BATCH: Routes batches through filter/aggregator pipeline
        - EOF_MESSAGE: Coordinates EOF across orchestrator workers
        - CLEAN_UP_MESSAGE: Coordinates client cleanup across workers

        Args:
            msg: The envelope message to process.

        Returns:
            True if the message should be acknowledged immediately, False otherwise.
            All messages should eventually be acknowledged.
        """
        self._log.debug(
            "Processing message: %r, message type: %r",
            msg,
            msg.type,
        )
        if msg.type == MessageType.DATA_BATCH:
            self._handle_data(msg.data_batch)
            return True
        elif msg.type == MessageType.EOF_MESSAGE:
            return self._handle_table_eof(msg.eof)
        elif msg.type == MessageType.CLEAN_UP_MESSAGE:
            return self._handle_client_cleanup_like(msg.clean_up)
        else:
            self._log.warning("Unknown message type: %r", type(msg))
            return True

    def _should_drop_for_blacklist(self, mask: int, cid) -> bool:
        """Check if a batch should be dropped due to blacklist.

        Args:
            mask: Filter steps mask for the batch.
            cid: Client ID associated with the batch.

        Returns:
            True if the batch should be dropped, False otherwise.
            Batches with mask=0 are never dropped (initial state).
        """
        if mask == 0:
            return False
        if self._is_blacklisted(str(cid) if cid else ""):
            self._log.info(
                "Discarding batch from blacklisted client: cid=%s mask=%s",
                cid,
                bin(mask),
            )
            return True
        return False

    def _route_back_to_filters(
        self, batch: DataBatch, table: TableName, queries: list[Query], mask: int
    ) -> bool:
        """Route a batch back to the filter pool if more filter steps are needed.

        Args:
            batch: The data batch to route.
            table: Table name for the batch.
            queries: List of queries associated with the batch.
            mask: Current filter steps mask.

        Returns:
            True if the batch was routed back to filters, False if no more
            filter steps are needed.
        """
        total_steps = self._pol.total_steps(table, queries)
        next_step = first_zero_bit(mask, total_steps)
        if next_step is None:
            return False
        if not self._pol.steps_remaining(table, queries, steps_done=next_step):
            return False
        new_mask = set_bit(mask, next_step)
        batch.filter_steps = new_mask
        self._log.debug(
            "→ filters step=%d table=%s new_mask=%s",
            next_step,
            table,
            bin(new_mask),
        )
        self._p.send_to_filters_pool(batch)
        return True

    def _fan_out_batches(
        self, batch: DataBatch, table: TableName, queries: list[Query]
    ) -> bool:
        """Duplicate and fan out a batch when multiple queries require separate copies.

        Creates multiple copies of the batch with different query sets as
        determined by the policy resolver.

        Args:
            batch: The original data batch.
            table: Table name for the batch.
            queries: List of queries associated with the batch.

        Returns:
            True if fan-out occurred, False if no duplication is needed.
        """
        dup_count = int(self._pol.get_duplication_count(queries) or 1)
        if dup_count <= 1:
            return False
        self._log.debug("Fan-out x%d table=%s queries=%s", dup_count, table, queries)
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
        return True

    def _handle_data(self, batch: DataBatch) -> None:
        """Handle an incoming data batch message.

        Validates the batch, checks blacklist, and routes it through the
        appropriate processing pipeline (filters or aggregators).

        Args:
            batch: The data batch to process.
        """
        table = batch.payload.name
        queries = batch.query_ids
        mask = batch.filter_steps
        bn = batch.payload.batch_number
        cid = batch.client_id

        if table is None:
            self._log.warning("Batch sin table_id válido. bn=%s", bn)
            return

        if self._should_drop_for_blacklist(mask, cid):
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
            self._process_batch_flow(batch, table, queries, mask)
        except Exception:
            self._log.exception("Error processing batch: table=%s bn=%s", table, bn)
            raise

    def _process_batch_flow(
        self,
        batch: DataBatch,
        table: TableName,
        queries: list[Query],
        mask: int,
    ) -> None:
        """Execute the core processing flow for a received batch.

        Determines the next routing step: route back to filters, fan out for
        multiple queries, or send to aggregators.

        Args:
            batch: The data batch to process.
            table: Table name for the batch.
            queries: List of queries associated with the batch.
            mask: Current filter steps mask.
        """
        if self._route_back_to_filters(batch, table, queries, mask):
            return

        if self._fan_out_batches(batch, table, queries):
            return

        self._send_to_some_aggregator(batch)

    def _send_to_some_aggregator(self, batch: DataBatch) -> None:
        """Send a batch to a randomly selected aggregator partition.

        Args:
            batch: The data batch to send to aggregators.
        """
        if batch.payload.name in [TableName.MENU_ITEMS, TableName.STORES]:
            self._log.debug(
                "_send_to_some_aggregator table=%s bn=%s",
                batch.payload.name,
                batch.payload.batch_number,
            )
        num_parts = max(MIN_AGGREGATOR_PARTS, int(self._cfg.aggregators))
        self._p.send_to_aggregator_partition(randint(0, num_parts - 1), batch)

    def _handle_table_eof(self, eof: EOFMessage) -> bool:
        """Handle a table EOF message from an orchestrator worker.

        Coordinates EOF messages across multiple orchestrator workers. Only
        processes EOFs for "light" tables (MENU_ITEMS, STORES). When EOFs
        are received from all orchestrator workers, forwards EOF to aggregators.

        Args:
            eof: The EOF message to process.

        Returns:
            True if the message should be acknowledged immediately.
        """
        key = (eof.table, eof.client_id)

        if eof.table not in LIGHT_TABLES:
            self._log.debug("Ignoring EOF for non-light table: key=%s", key)
            return True

        flush_info = None
        with self._state_lock:
            worker_id = self._get_eof_worker_id(eof, key)

            (recvd_workers, _eof) = self._pending_eof.get(key, (set(), eof))

            if worker_id in recvd_workers:
                self._log.warning(
                    "Duplicate EOF ignored: key=%s worker_id=%s (already received from this worker)",
                    key,
                    worker_id,
                )
                return True

            already_complete = len(recvd_workers) >= self._orch_workers
            if already_complete:
                self._log.info(
                    "EOF already complete for key=%s (received from %d workers), ignoring duplicate",
                    key,
                    len(recvd_workers),
                )
                return True

            recvd_workers.add(worker_id)
            self._pending_eof[key] = (recvd_workers, eof)

            self._save_eof_state()

            if len(recvd_workers) >= self._orch_workers:
                flush_info = (
                    key,
                    max(MIN_AGGREGATOR_PARTS, int(self._cfg.aggregators)),
                )
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

        return True

    def _flush_eof(self, key: tuple[TableName, str], total_parts: int) -> None:
        """Send EOF messages to all aggregator partitions and update state.

        Called when EOF coordination is complete (all orchestrator workers
        have sent EOF). Sends EOF to each aggregator partition and saves
        the updated state.

        Args:
            key: Tuple of (table_name, client_id) identifying the EOF.
            total_parts: Number of aggregator partitions to send EOF to.

        Note:
            This method is called outside of the state lock to avoid blocking
            message processing during I/O operations.
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

        with self._state_lock:
            self._save_eof_state()

    def _get_eof_worker_id(self, eof: EOFMessage, key: tuple[TableName, str]) -> str:
        """Extract worker identifier from EOF message trace.

        Parses the trace field to identify which orchestrator worker sent
        the EOF. Falls back to a hash-based identifier if trace is missing.

        Args:
            eof: The EOF message containing the trace.
            key: Tuple of (table_name, client_id) for logging.

        Returns:
            String identifier for the worker that sent the EOF.
        """
        if eof.trace:
            if eof.trace.startswith("orch_"):
                return eof.trace
            parts = eof.trace.split(":")
            for part in parts:
                if part.startswith("orch_"):
                    return part
            return eof.trace

        self._log.warning(
            "EOF without trace field, using fallback deduplication: key=%s", key
        )
        return f"unknown_{hash(eof.SerializeToString()) % UNKNOWN_WORKER_HASH_MOD}"

    def _record_eof_worker_under_lock(
        self, key: tuple[TableName, str], worker_id: str, eof: EOFMessage
    ) -> tuple[bool, Optional[tuple]]:
        """Record an EOF worker ID into pending EOF state.

        Must be called while holding _state_lock. Handles duplicate detection
        and determines when EOF coordination is complete.

        Args:
            key: Tuple of (table_name, client_id) identifying the EOF.
            worker_id: Identifier of the worker that sent the EOF.
            eof: The EOF message being recorded.

        Returns:
            Tuple of (should_return_immediately, flush_info):
            - should_return_immediately: True if message should be ACKed immediately
            - flush_info: (key, total_parts) when EOF is complete, None otherwise
        """
        (recvd_workers, _eof) = self._pending_eof.get(key, (set(), eof))

        if worker_id in recvd_workers:
            self._log.warning(
                "Duplicate EOF ignored: key=%s worker_id=%s (already received from this worker)",
                key,
                worker_id,
            )
            return True, None

        already_complete = len(recvd_workers) >= self._orch_workers
        if already_complete:
            self._log.info(
                "EOF already complete for key=%s (received from %d workers), ignoring duplicate",
                key,
                len(recvd_workers),
            )
            return True, None

        recvd_workers.add(worker_id)
        self._pending_eof[key] = (recvd_workers, eof)

        self._save_eof_state()

        if len(recvd_workers) >= self._orch_workers:
            flush_info = (key, max(MIN_AGGREGATOR_PARTS, int(self._cfg.aggregators)))
            self._log.info(
                "TABLE_EOF complete: key=%s received from %d/%d workers",
                key,
                len(recvd_workers),
                self._orch_workers,
            )
            return False, flush_info

        self._log.debug(
            "TABLE_EOF deferred: key=%s received from %d/%d workers (worker_id=%s)",
            key,
            len(recvd_workers),
            self._orch_workers,
            worker_id,
        )
        return False, None

    def _cleanup_client_state(self, client_id: str) -> None:
        """Remove all state associated with a client_id.

        Cleans up both in-memory and persisted state including:
        - Pending EOF messages for the client
        - Pending cleanup tracking
        - Any persisted files/directories containing the client_id

        Args:
            client_id: The client identifier to clean up.
        """
        if not client_id:
            self._log.warning("_cleanup_client_state called with empty client_id")
            return

        self._log.info(
            "action: cleanup_client_state | result: starting | client_id: %s", client_id
        )

        self._clear_in_memory_state_for_client(client_id)
        self._remove_persisted_state_for_client(client_id)

        self._log.info(
            "action: cleanup_client_state | result: success | client_id: %s", client_id
        )

    def _clear_in_memory_state_for_client(self, client_id: str) -> None:
        """Remove in-memory state for a client and persist changes.

        Removes pending EOF and cleanup tracking for the client, then saves
        the updated state to disk. Must be called while holding _state_lock.

        Args:
            client_id: The client identifier to clean up.
        """
        with self._state_lock:
            keys_to_remove = [
                key for key in self._pending_eof.keys() if key[1] == client_id
            ]
            for key in keys_to_remove:
                del self._pending_eof[key]

            self._pending_cleanups.pop(client_id, None)

            self._save_eof_state()
            self._save_cleanup_state()

    def _remove_persisted_state_for_client(self, client_id: str) -> None:
        """Remove persisted files/directories related to a client_id.

        Scans the state directory for files/directories containing the client_id
        in their name and removes them. Logs warnings for failures but does
        not raise exceptions.

        Args:
            client_id: The client identifier to clean up.
        """
        state_dir = os.getenv("FILTER_ROUTER_STATE_DIR")
        if not state_dir or not os.path.exists(state_dir):
            return

        try:
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

    def _get_cleanup_worker_id(self, cleanup_msg, client_id: str) -> str:
        """Extract worker identifier from cleanup message trace.

        Parses the trace field to identify which orchestrator worker sent
        the cleanup message. Falls back to a counter-based identifier if
        trace is missing.

        Args:
            cleanup_msg: The cleanup message containing the trace.
            client_id: Client ID for logging purposes.

        Returns:
            String identifier for the worker that sent the cleanup message.
        """
        if cleanup_msg.trace:
            if cleanup_msg.trace.startswith("orch_"):
                return cleanup_msg.trace
            self._log.warning(
                "CLEANUP trace format unexpected: client_id=%s trace=%s",
                client_id,
                cleanup_msg.trace,
            )
            return cleanup_msg.trace

        self._log.warning("CLEANUP received without trace: client_id=%s", client_id)
        return f"unknown_{len(self._pending_cleanups.get(client_id, set()))}"

    def _record_cleanup_worker_under_lock(
        self, client_id: str, worker_id: str
    ) -> tuple[bool, Optional[tuple]]:
        """Record a cleanup worker ID into pending cleanup state.

        Must be called while holding _state_lock. Handles duplicate detection
        and determines when cleanup coordination is complete.

        Args:
            client_id: The client identifier being cleaned up.
            worker_id: Identifier of the worker that sent the cleanup message.

        Returns:
            Tuple of (should_return_immediately, flush_info):
            - should_return_immediately: True if message should be ACKed immediately
            - flush_info: (client_id, total_parts) when cleanup is complete, None otherwise
        """
        recvd_workers = self._pending_cleanups.get(client_id, set())

        if worker_id in recvd_workers:
            self._log.warning(
                "Duplicate CLEANUP ignored: client_id=%s worker_id=%s (already received from this worker)",
                client_id,
                worker_id,
            )
            return True, None

        recvd_workers.add(worker_id)
        self._pending_cleanups[client_id] = recvd_workers

        self._save_cleanup_state()

        if len(recvd_workers) >= self._orch_workers:
            self._add_to_blacklist(client_id)
            flush_info = (
                client_id,
                max(MIN_AGGREGATOR_PARTS, int(self._cfg.aggregators)),
            )
            self._pending_cleanups.pop(client_id, None)
            self._log.info(
                "CLEANUP complete: client_id=%s received from %d/%d workers",
                client_id,
                len(recvd_workers),
                self._orch_workers,
            )
            return False, flush_info

        self._log.debug(
            "CLEANUP deferred: client_id=%s received from %d/%d workers (worker_id=%s)",
            client_id,
            len(recvd_workers),
            self._orch_workers,
            worker_id,
        )
        return False, None

    def _handle_client_cleanup_like(self, cleanup_msg) -> bool:
        """Handle a client cleanup message from an orchestrator worker.

        Coordinates cleanup messages across multiple orchestrator workers.
        When cleanups are received from all workers, adds the client to the
        blacklist and forwards cleanup to aggregators.

        Args:
            cleanup_msg: The cleanup message to process.

        Returns:
            True if the message should be acknowledged immediately.
        """
        client_id = cleanup_msg.client_id if cleanup_msg.client_id else ""
        if not client_id:
            self._log.warning("CLEANUP without valid client_id; ignoring")
            return True

        self._log.debug(
            "CLEANUP received: client_id=%s trace=%s", client_id, cleanup_msg.trace
        )

        if self._is_blacklisted(client_id):
            self._log.info(
                "CLEANUP for already blacklisted client_id=%s, ACKing immediately",
                client_id,
            )
            return True

        flush_info = None
        with self._state_lock:
            worker_id = self._get_cleanup_worker_id(cleanup_msg, client_id)

            recvd_workers = self._pending_cleanups.get(client_id, set())

            if worker_id in recvd_workers:
                self._log.warning(
                    "Duplicate CLEANUP ignored: client_id=%s worker_id=%s (already received from this worker)",
                    client_id,
                    worker_id,
                )
                return True

            recvd_workers.add(worker_id)
            self._pending_cleanups[client_id] = recvd_workers

            self._save_cleanup_state()

            if len(recvd_workers) >= self._orch_workers:
                self._add_to_blacklist(client_id)
                flush_info = (
                    client_id,
                    max(MIN_AGGREGATOR_PARTS, int(self._cfg.aggregators)),
                )
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

        return True

    def _flush_cleanup(self, client_id: str, total_parts: int) -> None:
        """Send cleanup message to aggregators and clean up client state.

        Called when cleanup coordination is complete (all orchestrator workers
        have sent cleanup). Cleans up all state for the client and sends
        cleanup to a random aggregator partition.

        Args:
            client_id: The client identifier being cleaned up.
            total_parts: Number of aggregator partitions available.

        Note:
            This method is called outside of the state lock to avoid blocking
            message processing during I/O operations.
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
    """Produces messages to filters pool and aggregator exchanges.

    Manages publisher connections with caching, retry logic, and automatic
    reconnection. Thread-safe per exchange/routing key combination.
    """

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
        """Initialize the exchange bus producer.

        Args:
            host: Message broker host address.
            filters_pool_queue: Queue name for sending batches to filter pool.
            in_mw: Middleware exchange for requeuing messages back to router.
            exchange_fmt: Format string for exchange names (default: "ex.{table}").
            rk_fmt: Format string for routing keys (default: "agg.{table}.{pid:02d}").
            router_id: Unique identifier for this router instance.
            max_retries: Maximum number of retry attempts for failed sends.
            base_backoff_ms: Initial backoff delay in milliseconds.
            backoff_multiplier: Multiplier for exponential backoff.
        """
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

    def shutdown(self) -> None:
        """Close all active publisher connections and clean up resources.

        Closes the filters pool publisher and all cached exchange publishers.
        Logs warnings for errors but does not raise exceptions.
        """
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

    def _key_for(self, table_name: TableName, pid: int) -> tuple[str, str]:
        """Generate exchange and routing key tuple for a table and partition.

        Args:
            table_name: The table name enum value.
            pid: Partition ID for the routing key.

        Returns:
            Tuple of (exchange_name, routing_key) formatted according to
            the configured format strings.
        """
        table = TABLE_NAME_TO_STR[table_name]
        ex = self._exchange_fmt.format(table=table)
        rk = self._rk_fmt.format(table=table, pid=pid)
        return (ex, rk)

    def _get_pub(self, table_name: TableName, pid: int) -> MessageMiddlewareExchange:
        """Get or create a publisher for a table and partition.

        Checks cache first, recreates if closed. Creates new publisher if
        not cached. Thread-safe per (exchange, routing_key) combination.

        Args:
            table_name: The table name enum value.
            pid: Partition ID.

        Returns:
            A MessageMiddlewareExchange publisher for the specified exchange/routing key.
        """
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

    def _drop_pub(self, key: tuple[str, str]) -> None:
        """Remove a publisher from cache and close it.

        Args:
            key: Tuple of (exchange_name, routing_key) identifying the publisher.
        """
        pub = self._pub_cache.pop(key, None)
        if pub is not None:
            try:
                if hasattr(pub, "close"):
                    pub.close()
            except Exception:
                pass

    def _is_pub_closed(self, pub) -> bool:
        """Safely check whether a cached publisher is closed.

        Handles missing attributes and exceptions gracefully, defaulting to
        True (closed) if the check cannot be performed.

        Args:
            pub: The publisher to check.

        Returns:
            True if the publisher is closed or cannot be checked, False otherwise.
        """
        if pub is None:
            return True
        try:
            is_closed = getattr(pub, "is_closed", None)
            if callable(is_closed):
                return is_closed()
            if is_closed is not None:
                return bool(is_closed)
        except Exception:
            return True
        return False

    def _ensure_pub_for_key(
        self, key: tuple[str, str]
    ) -> MessageMiddlewareExchange:
        """Ensure a live publisher exists for the given key.

        Checks cache and recreates publisher if missing or closed. Creates
        new locks as needed for thread-safe access.

        Args:
            key: Tuple of (exchange_name, routing_key) identifying the publisher.

        Returns:
            A live MessageMiddlewareExchange publisher for the key.
        """
        pub = self._pub_cache.get(key)
        if pub is not None and self._is_pub_closed(pub):
            try:
                self._log.debug("publisher cached but closed: %s → recreate", key)
            except Exception:
                pass
            self._drop_pub(key)
            pub = None

        if pub is None:
            self._log.debug("pub cache miss -> create: %s", key)
            pub = MessageMiddlewareExchange(
                host=self._host, exchange_name=key[0], route_keys=[key[1]]
            )
            self._pub_cache[key] = pub
            if key not in self._pub_locks:
                self._pub_locks[key] = threading.Lock()
        return pub

    def _compute_sleep_with_jitter(self, base_backoff_s: float) -> float:
        """Compute sleep interval with random jitter to avoid thundering herd.

        Adds a random jitter up to BACKOFF_JITTER_FACTOR * base_backoff_s to
        prevent multiple retries from synchronizing.

        Args:
            base_backoff_s: Base backoff delay in seconds.

        Returns:
            Sleep interval in seconds with jitter added.
        """
        jitter = random.uniform(0, base_backoff_s * BACKOFF_JITTER_FACTOR)
        return base_backoff_s + jitter

    def _send_with_retry(self, key: tuple[str, str], payload: bytes) -> None:
        """Send payload to exchange/routing key with retry logic and publisher recreation.

        Implements exponential backoff with jitter. Automatically recreates
        publishers on failure. Thread-safe per key using per-key locks.

        Args:
            key: Tuple of (exchange_name, routing_key) identifying the destination.
            payload: Serialized message bytes to send.

        Raises:
            RuntimeError: If all retry attempts are exhausted.
        """
        self._log.debug("send_with_retry key=%s payload_size=%d", key, len(payload))
        lock = self._pub_locks.setdefault(key, threading.Lock())
        with lock:
            attempt = 0
            backoff = self._base_backoff_ms / 1000.0
            last_error = None

            while attempt <= self._max_retries:
                try:
                    pub = self._ensure_pub_for_key(key)
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

                    sleep_s = self._compute_sleep_with_jitter(backoff)
                    time.sleep(sleep_s)
                    backoff *= self._backoff_multiplier

            raise RuntimeError(
                f"Error sending message to {key}: retries exhausted"
            ) from last_error

    def send_to_filters_pool(self, batch: DataBatch) -> None:
        """Send a data batch to the filters pool queue.

        Args:
            batch: The data batch to send.

        Raises:
            Exception: If the send operation fails.
        """
        try:
            self._log.debug("publish → filters_pool")
            envelope = Envelope(type=MessageType.DATA_BATCH, data_batch=batch)
            raw = envelope.SerializeToString()
            self._filters_pub.send(raw)
        except Exception as e:
            self._log.error("filters_pool send failed: %s", e)
            raise

    def send_to_aggregator_partition(self, partition_id: int, batch: DataBatch) -> None:
        """Send a data batch to a specific aggregator partition.

        Args:
            partition_id: The aggregator partition ID to send to.
            batch: The data batch to send.

        Raises:
            Exception: If the send operation fails after retries.
        """
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
        """Requeue a batch back to the router input queue.

        Used for reinjecting batches that need to be reprocessed.

        Args:
            batch: The data batch to requeue.

        Raises:
            Exception: If the requeue operation fails.
        """
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
        """Send a table EOF message to a specific aggregator partition.

        Args:
            key: Tuple of (table_name, client_id) identifying the EOF.
            partition_id: The aggregator partition ID to send to.

        Raises:
            Exception: If the send operation fails after retries.
        """
        try:
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
        """Send a client cleanup message to a specific aggregator partition.

        The aggregator will broadcast this cleanup to joiner routers to
        coordinate state cleanup across the system.

        Args:
            partition_id: The aggregator partition ID to send to.
            client_id: The client identifier to clean up.

        Raises:
            Exception: If the send operation fails after retries.
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
    """Server that consumes messages and routes them through FilterRouter.

    Manages the message consumption loop and coordinates graceful shutdown
    of the router and producer components.
    """

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
        """Initialize the router server.

        Args:
            host: Message broker host address (for logging/debugging).
            router_in: Middleware exchange for consuming incoming messages.
            producer: Producer for sending messages to filters and aggregators.
            policy: Policy resolver for query routing decisions.
            table_cfg: Configuration for table-specific parameters.
            stop_event: Event to signal shutdown requests.
            orch_workers: Number of orchestrator workers to wait for EOF/cleanup.
            router_id: Unique identifier for this router instance.
        """
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
        """Start consuming messages and processing them through the router.

        Blocks until stop() is called or an error occurs. Messages are
        processed through FilterRouter.process_message(). Returns False
        from callback to NACK messages during shutdown or on errors.
        """
        self._log.debug("RouterServer starting consume")

        def _cb(body: bytes):
            if self._stop_event.is_set():
                self._log.warning(
                    "Shutdown in progress, NACKing message for redelivery."
                )
                return False
            try:
                if len(body) < 1:
                    self._log.error("Received empty message")
                    return True
                msg = Envelope()
                msg.ParseFromString(body)
                return self._router.process_message(msg)
            except Exception as e:
                self._log.exception("Error in router callback: %s", e)
                return False

        try:
            self._mw_in.start_consuming(_cb)
            self._log.info("RouterServer consuming (thread started)")
        except Exception as e:
            self._log.exception("start_consuming failed: %s", e)

    def stop(self) -> None:
        """Stop the consumer and gracefully shut down all components.

        Stops message consumption, flushes final state, closes producer connections,
        and cleans up resources. Logs warnings for errors but does not raise exceptions.
        
        Order of operations:
        1. Stop consuming and wait for in-flight messages to complete
        2. Flush final state to disk (EOF, cleanup, blacklist)
        3. Shutdown producer (closes connections after all sends complete)
        """
        self._log.info("Stopping Filter Router Server...")

        try:
            # Step 1: Stop consuming - this blocks until all in-flight callbacks complete
            self._mw_in.stop_consuming()
            self._log.info("Input consumer stopped.")
        except Exception as e:
            self._log.warning(f"Error stopping input consumer: {e}")

        try:
            # Step 2: Flush final state before closing connections
            self._log.info("Flushing final state to disk...")
            with self._state_lock:
                self._save_eof_state()
                self._save_cleanup_state()
            self._save_blacklist()
            self._log.info("Final state flushed.")
        except Exception as e:
            self._log.warning(f"Error flushing final state: {e}")

        try:
            # Step 3: Shutdown producer (closes connections)
            self._producer.shutdown()
        except Exception as e:
            self._log.warning(f"Error during producer shutdown: {e}")

        self._log.info("Filter Router Server stopped.")
