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
    def __init__(
        self,
        producer: "ExchangeBusProducer",
        policy: QueryPolicyResolver,
        table_cfg: TableConfig,
        orch_workers: int,
        persistence_dir: str = "/tmp/filter_router_state",
        router_id: int = 0,
    ):
        self._p = producer
        self._pol = policy
        self._cfg = table_cfg
        self._log = logging.getLogger("filter-router")
        self._pending_batches: Dict[tuple[TableName, str], int] = defaultdict(int)
        self._pending_eof: Dict[tuple[str, str], tuple[int, EOFMessage]] = {}
        self._orch_workers = orch_workers
        # Store unacked EOF messages: key=(table, client_id) -> list of (channel, delivery_tag)
        self._unacked_eofs: Dict[tuple[TableName, str], list] = {}
        self._eof_ack_lock = threading.Lock()
        
        # Batch deduplication: track received batch numbers per (table, client_id, query_ids_tuple)
        self._received_batches: Dict[tuple[TableName, str, tuple], set[int]] = defaultdict(set)
        
        # Track pending increments: (table, client_id) -> set of batch_numbers that COMPLETED increment
        # This makes pending increments idempotent on redelivery
        # A batch is in this set ONLY if both mark and increment+persist succeeded
        self._pending_increments: Dict[tuple[TableName, str], set[int]] = defaultdict(set)
        
        # Persistence
        self._persistence_dir = persistence_dir
        self._router_id = router_id
        self._persistence_lock = threading.Lock()
        self._state_lock = threading.Lock()
        os.makedirs(self._persistence_dir, exist_ok=True)
        
        # Restore state from disk
        self._restore_state()

    def _get_pending_batches_path(self) -> str:
        """Get file path for pending batches state."""
        return os.path.join(self._persistence_dir, f"router_{self._router_id}_pending_batches.json")
    
    def _get_pending_eof_path(self) -> str:
        """Get file path for pending EOF state."""
        return os.path.join(self._persistence_dir, f"router_{self._router_id}_pending_eof.json")
    
    def _get_received_batches_path(self) -> str:
        """Get file path for received batches deduplication state."""
        return os.path.join(self._persistence_dir, f"router_{self._router_id}_received_batches.json")
    
    def _get_pending_increments_path(self) -> str:
        """Get file path for pending increments tracking."""
        return os.path.join(self._persistence_dir, f"router_{self._router_id}_pending_increments.json")
    
    def _persist_pending_batches(self) -> None:
        """Persist pending batches counter to disk."""
        with self._persistence_lock:
            path = self._get_pending_batches_path()
            temp_path = path + ".tmp"
            try:
                # Convert tuple keys to strings for JSON serialization
                # Key format is (client_id, table)
                data = {f"{client}:{table}": count for (client, table), count in self._pending_batches.items()}
                with open(temp_path, "w") as f:
                    json.dump(data, f)
                os.rename(temp_path, path)
                self._log.debug("Persisted pending batches: %d entries", len(data))
            except Exception as e:
                self._log.error("Failed to persist pending batches: %s", e)
                if os.path.exists(temp_path):
                    os.remove(temp_path)
                raise
    
    def _persist_pending_eof(self) -> None:
        """Persist pending EOF state to disk."""
        with self._persistence_lock:
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
    
    def _persist_received_batches(self) -> None:
        """Persist received batches for deduplication."""
        with self._persistence_lock:
            path = self._get_received_batches_path()
            temp_path = path + ".tmp"
            try:
                # Convert to serializable format
                data = {f"{table}:{client}:{','.join(map(str, query_ids))}": list(batches) 
                        for (table, client, query_ids), batches in self._received_batches.items()}
                with open(temp_path, "w") as f:
                    json.dump(data, f)
                os.rename(temp_path, path)
                self._log.debug("Persisted received batches: %d entries", len(data))
            except Exception as e:
                self._log.error("Failed to persist received batches: %s", e)
                if os.path.exists(temp_path):
                    os.remove(temp_path)
                raise
    
    def _persist_pending_increments(self) -> None:
        """Persist pending increments tracking."""
        with self._persistence_lock:
            path = self._get_pending_increments_path()
            temp_path = path + ".tmp"
            try:
                # Convert to serializable format
                # Key format is (table, client_id)
                data = {f"{table}:{client}": list(batches)
                        for (table, client), batches in self._pending_increments.items()}
                with open(temp_path, "w") as f:
                    json.dump(data, f)
                os.rename(temp_path, path)
                self._log.debug("Persisted pending increments: %d entries", len(data))
            except Exception as e:
                self._log.error("Failed to persist pending increments: %s", e)
                if os.path.exists(temp_path):
                    os.remove(temp_path)
                raise
    
    def _restore_state(self) -> None:
        """Restore state from disk on startup."""
        if not os.path.exists(self._persistence_dir):
            return
        
        self._log.info("Restoring filter router state from disk: %s", self._persistence_dir)
        
        # Restore pending batches
        batches_path = self._get_pending_batches_path()
        if os.path.exists(batches_path):
            try:
                with open(batches_path, "r") as f:
                    data = json.load(f)
                for key_str, count in data.items():
                    # Key format is "client_id:table"
                    client, table_str = key_str.split(":", 1)
                    table = int(table_str)
                    self._pending_batches[(client, table)] = count
                self._log.info("Restored %d pending batch counters", len(self._pending_batches))
            except Exception as e:
                self._log.warning("Failed to restore pending batches: %s", e)
        
        # Restore pending EOFs
        eof_path = self._get_pending_eof_path()
        if os.path.exists(eof_path):
            try:
                with open(eof_path, "r") as f:
                    data = json.load(f)
                for key_str, eof_data in data.items():
                    table_str, client = key_str.split(":", 1)
                    table = int(table_str)
                    eof = EOFMessage(table=eof_data["table"], client_id=eof_data["client_id"])
                    self._pending_eof[(table, client)] = (eof_data["count"], eof)
                self._log.info("Restored %d pending EOF entries", len(self._pending_eof))
            except Exception as e:
                self._log.warning("Failed to restore pending EOFs: %s", e)
        
        # Restore received batches for deduplication
        received_path = self._get_received_batches_path()
        if os.path.exists(received_path):
            try:
                with open(received_path, "r") as f:
                    data = json.load(f)
                for key_str, batch_list in data.items():
                    parts = key_str.split(":", 2)
                    table = int(parts[0])
                    client = parts[1]
                    query_ids = tuple(map(int, parts[2].split(","))) if len(parts) > 2 and parts[2] else tuple()
                    self._received_batches[(table, client, query_ids)] = set(batch_list)
                self._log.info("Restored %d received batch sets for deduplication", len(self._received_batches))
            except Exception as e:
                self._log.warning("Failed to restore received batches: %s", e)
        
        # Restore pending increments tracking
        increments_path = self._get_pending_increments_path()
        if os.path.exists(increments_path):
            try:
                with open(increments_path, "r") as f:
                    data = json.load(f)
                for key_str, batch_list in data.items():
                    table_str, client = key_str.split(":", 1)
                    table = int(table_str)
                    self._pending_increments[(table, client)] = set(batch_list)
                self._log.info("Restored %d pending increment sets", len(self._pending_increments))
            except Exception as e:
                self._log.warning("Failed to restore pending increments: %s", e)
        
        self._log.info("Filter router state restoration complete")

    def process_message(self, msg: Envelope, channel=None, delivery_tag=None, redelivered=False) -> tuple[bool, bool]:
        """
        Process a message and return (should_ack, ack_now).
        - should_ack: True if message should eventually be acked
        - ack_now: True if should ack immediately, False if should delay ack
        """
        self._log.debug("Processing message: %r, message type: %r, redelivered: %r", msg, msg.type, redelivered)
        if msg.type == MessageType.DATA_BATCH:
            self._handle_data(msg.data_batch)
            return (True, True)  # Regular messages: ack immediately
        elif msg.type == MessageType.EOF_MESSAGE:
            # EOF messages: delay ack until fully processed
            return self._handle_table_eof(msg.eof, channel, delivery_tag, redelivered)
        else:
            self._log.warning("Unknown message type: %r", type(msg))
            return (True, True)

    def _handle_data(self, batch: DataBatch) -> None:
        table = batch.payload.name
        queries = batch.query_ids
        mask = batch.filter_steps
        bn = batch.payload.batch_number
        cid = batch.client_id

        if table is None:
            self._log.warning("Batch sin table_id válido. bn=%s", bn)
            return

        self._log.debug(
            "recv DataBatch table=%s queries=%s mask=%s bn=%s cid=%s",
            table, queries, bin(mask), bn, cid
        )
        key = (cid, table)
        
        batch_is_duplicate = False
        dedup_key = None
        if mask != 0:
            query_ids_tuple = tuple(sorted(queries))
            dedup_key = (table, cid, query_ids_tuple)
            with self._state_lock:
                if bn in self._received_batches.get(dedup_key, set()):
                    batch_is_duplicate = True

        if batch_is_duplicate:
            self._log.warning(
                "DUPLICATE batch detected: table=%s bn=%s cid=%s queries=%s mask=%s",
                table, bn, cid, queries, bin(mask)
            )
            with self._state_lock:
                increment_key = (table, cid)
                if bn in self._pending_increments.get(increment_key, set()):
                    self._log.info("Duplicate batch needs pending decrement (crash recovery): bn=%d", bn)
                    self._pending_batches[key] = max(0, self._pending_batches.get(key, 1) - 1)
                    self._pending_increments[increment_key].discard(bn)
                    try:
                        self._persist_pending_batches()
                        self._persist_pending_increments()
                    except Exception as e:
                        self._log.error("Failed to persist after duplicate decrement: %s", e)
            self._maybe_flush_pending_eof(key)
            return

        increment_done = False
        if mask == 0:
            with self._state_lock:
                increment_key = (table, cid)
                if bn not in self._pending_increments.get(increment_key, set()):
                    self._pending_batches[key] = self._pending_batches.get(key, 0) + 1
                    self._pending_increments.setdefault(increment_key, set()).add(bn)
                    self._log.debug("pending++ %s bn=%d -> %d", key, bn, self._pending_batches[key])
                    try:
                        self._persist_pending_batches()
                        self._persist_pending_increments()
                        increment_done = True
                    except Exception as e:
                        self._log.error("Failed to persist after increment: %s", e)
                        self._pending_batches[key] -= 1
                        self._pending_increments[increment_key].discard(bn)
                        raise
                else:
                    self._log.info("Batch already incremented pending (redelivery): table=%s bn=%d cid=%s", 
                                  table, bn, cid)
                    increment_done = True

        try:
            total_steps = self._pol.total_steps(table, queries)
            next_step = first_zero_bit(mask, total_steps)
            if next_step is not None and self._pol.steps_remaining(table, queries, steps_done=next_step):
                new_mask = set_bit(mask, next_step)
                batch.filter_steps = new_mask
                self._log.debug("→ filters step=%d table=%s new_mask=%s", next_step, table, bin(new_mask))
                self._p.send_to_filters_pool(batch)
                return

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
                
                for b in fanout_batches:
                    self._handle_data(b)
                
                with self._state_lock:
                    increment_key = (table, cid)
                    if increment_done or bn in self._pending_increments.get(increment_key, set()):
                        self._pending_batches[key] = max(0, self._pending_batches.get(key, 1) - 1)
                        self._pending_increments.get(increment_key, set()).discard(bn)
                        self._log.debug("pending-- (fanout parent) %s bn=%d -> %d", key, bn, self._pending_batches[key])
                        try:
                            self._persist_pending_batches()
                            self._persist_pending_increments()
                        except Exception as e:
                            self._log.error("Failed to persist after fanout decrement: %s", e)
                            raise
                self._maybe_flush_pending_eof(key)
                return

            self._send_to_some_aggregator(batch)
            
            with self._state_lock:
                if mask != 0 and dedup_key:
                    self._received_batches.setdefault(dedup_key, set()).add(bn)
                    try:
                        self._persist_received_batches()
                    except Exception as e:
                        self._log.error("Failed to persist received batches: %s", e)

                increment_key = (table, cid)
                if increment_done or bn in self._pending_increments.get(increment_key, set()):
                    self._pending_batches[key] = max(0, self._pending_batches.get(key, 1) - 1)
                    self._pending_increments.get(increment_key, set()).discard(bn)
                    self._log.debug("pending-- %s bn=%d -> %d", key, bn, self._pending_batches[key])
                    try:
                        self._persist_pending_batches()
                        self._persist_pending_increments()
                    except Exception as e:
                        self._log.error("Failed to persist after final decrement: %s", e)
                        raise
            self._maybe_flush_pending_eof(key)

        except Exception:
            if increment_done:
                with self._state_lock:
                    self._pending_batches[key] = max(0, self._pending_batches.get(key, 1) - 1)
                    try:
                        self._persist_pending_batches()
                    except Exception as persist_err:
                        self._log.error("Failed to rollback pending batches: %s", persist_err)
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

    def _handle_table_eof(self, eof: EOFMessage, channel=None, delivery_tag=None, redelivered=False) -> tuple[bool, bool]:
        key = (eof.table, eof.client_id)
        
        if redelivered:
            self._log.info("TABLE_EOF REDELIVERED (recovering state): key=%s", key)
        else:
            self._log.debug("TABLE_EOF received: key=%s", key)

        with self._eof_ack_lock:
            if channel is not None and delivery_tag is not None:
                if key not in self._unacked_eofs:
                    self._unacked_eofs[key] = []
                self._unacked_eofs[key].append((channel, delivery_tag))

        flush_info = None
        with self._state_lock:
            (recvd, _eof) = self._pending_eof.get(key, (0, eof))
            self._pending_eof[key] = (recvd + 1, eof)
            try:
                self._persist_pending_eof()
            except Exception as e:
                self._log.error("Failed to persist pending EOF: %s", e)
                return (True, False)
            
            flush_info = self._check_and_flush_eof(key)

        if flush_info:
            self._flush_eof(*flush_info)

        return (True, False)

    def _maybe_flush_pending_eof(self, key: tuple[TableName, str]) -> None:
        flush_info = None
        with self._state_lock:
            flush_info = self._check_and_flush_eof(key)

        if flush_info:
            self._flush_eof(*flush_info)

    def _check_and_flush_eof(self, key: tuple[TableName, str]) -> Optional[tuple]:
        """
        Checks if EOF for `key` can be flushed. If so, it updates the state
        to mark it as "flushing" and returns the necessary info for the caller
        to perform the I/O. This method MUST be called with _state_lock held.
        """
        pending = self._pending_batches.get(key, 0)
        (recvd, eof) = self._pending_eof.get(key, (0, None))

        if eof is None or recvd < self._orch_workers or pending > 0:
            if eof is not None:
                self._log.info("TABLE_EOF deferred: key=%s pending=%d recvd=%d orch_workers=%d", 
                             key, pending, recvd, self._orch_workers)
            return None

        self._log.info("TABLE_EOF ready to be flushed for key: %s", key)
        
        self._pending_eof.pop(key, None)
        
        total_parts = max(1, int(self._cfg.aggregators))
        
        return (key, total_parts)

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
                    part, key, e
                )
        
        self._ack_eof(key)
        
        with self._state_lock:
            self._pending_batches.pop(key, None)
            keys_to_remove = [k for k in self._received_batches.keys() if k[0] == key[0] and k[1] == key[1]]
            for k in keys_to_remove:
                self._received_batches.pop(k, None)
            self._pending_increments.pop(key, None)
            
            try:
                self._persist_pending_batches()
                self._persist_pending_eof()
                self._persist_received_batches()
                self._persist_pending_increments()
            except Exception as e:
                self._log.warning("Failed to clean up persisted state after EOF: %s", e)
    
    def _ack_eof(self, key: tuple[TableName, str]) -> None:
        """Acknowledge all EOF messages for this key after they have been fully processed."""
        with self._eof_ack_lock:
            ack_list = self._unacked_eofs.get(key, [])
            if ack_list:
                self._log.info("ACKing %d TABLE_EOF messages: key=%s", len(ack_list), key)
                for channel, delivery_tag in ack_list:
                    try:
                        channel.basic_ack(delivery_tag=delivery_tag)
                    except Exception as e:
                        self._log.error("Failed to ACK EOF key=%s delivery_tag=%s: %s", key, delivery_tag, e)
                del self._unacked_eofs[key]


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
            self._mw_in.stop_consuming()
            self._log.info("Input consumer stopped.")
        except Exception as e:
            self._log.warning(f"Error stopping input consumer: {e}")

        try:
            self._producer.shutdown()
        except Exception as e:
            self._log.warning(f"Error during producer shutdown: {e}")

        self._log.info("Filter Router Server stopped.")
