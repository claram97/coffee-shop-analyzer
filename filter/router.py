from __future__ import annotations

import copy
import hashlib
import logging
import random
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from queue import Queue, Empty
from random import randint
from typing import Any, Dict, List, Optional, Union

from middleware.middleware_client import (
    MessageMiddlewareExchange,
    MessageMiddlewareQueue,
)
from protocol.constants import Opcodes
from protocol.databatch import DataBatch
from protocol.messages import EOFMessage

TID_TO_NAME = {
    Opcodes.NEW_TRANSACTION: "transactions",
    Opcodes.NEW_USERS: "users",
    Opcodes.NEW_TRANSACTION_ITEMS: "transaction_items",
    Opcodes.NEW_MENU_ITEMS: "menu_items",
    Opcodes.NEW_STORES: "stores",
}
LIGHT_TABLES = {"menu_items", "stores"}


def table_name_of(db: DataBatch) -> str:
    if not getattr(db, "batch_msg", None):
        return ""
    return TID_TO_NAME.get(int(db.batch_msg.opcode))


def queries_of(db: DataBatch) -> List[int]:
    return list(getattr(db, "query_ids", []) or [])


def rows_of(db: DataBatch) -> List[Union[dict, object]]:
    inner = getattr(db, "batch_msg", None)
    if inner is None or not hasattr(inner, "rows"):
        return []
    return inner.rows or []


def get_field(row: Union[dict, object], key: str):
    if isinstance(row, dict):
        return row.get(key)
    return getattr(row, key, None)


@dataclass
class MetadataCompat:
    table_name: str
    queries: List[int]
    total_filter_steps: int
    reserved_u16: int = 0


def table_eof_to_bytes(key: tuple[str, str]) -> bytes:
    eof_msg = EOFMessage()
    eof_msg.create_eof_message(batch_number=0, table_type=key[0], client_id=key[1])
    return eof_msg.to_bytes()


def is_bit_set(mask: int, idx: int) -> bool:
    return ((mask >> idx) & 1) == 1


def set_bit(mask: int, idx: int) -> int:
    return mask | (1 << idx)


def first_zero_bit(mask: int, total_bits: int) -> Optional[int]:
    for i in range(total_bits):
        if not is_bit_set(mask, i):
            return i
    return None


def _hash_u64(s: str) -> int:
    return int.from_bytes(
        hashlib.blake2b(s.encode("utf-8"), digest_size=8).digest(),
        "little",
        signed=False,
    )


def _pick_key_field(table_name: str, queries: List[int]) -> Optional[str]:
    t = table_name
    qs = set(queries or [])
    if t == "transactions":
        return "user_id" if 4 in qs else "transaction_id"
    if t == "transaction_items":
        return "transaction_id"
    if t == "users":
        return "user_id"
    return None


def _clone_with_rows(
    batch: DataBatch, subrows: list, parts_info: tuple[int, int] | None = None
) -> DataBatch:
    b = copy.copy(batch)
    inner = copy.copy(batch.batch_msg)
    inner.rows = subrows
    b.batch_msg = inner
    if parts_info:
        parts, pid = parts_info
        b.shards_info = getattr(batch, "shards_info", []) + [(parts, pid)]
    b.batch_bytes = b.batch_msg.to_bytes()
    return b


def _group_rows_by_partition(
    table_name: str,
    queries: List[int],
    rows: List[Union[dict, object]],
    num_parts: int,
) -> Dict[int, List[Union[dict, object]]]:
    parts: Dict[int, List[Union[dict, object]]] = defaultdict(list)
    if num_parts <= 1:
        parts[0] = list(rows)
        return parts
    key = _pick_key_field(table_name, queries)
    if not key:
        parts[0] = list(rows)
        return parts
    for r in rows:
        val = get_field(r, key)
        if val is None:
            parts[0].append(r)
            continue
        pid = _hash_u64(str(val)) % num_parts
        parts[int(pid)].append(r)
    return parts


class TableConfig:
    def __init__(self, aggregators: int):
        self.aggregators = aggregators


class QueryPolicyResolver:
    def steps_remaining(
        self, batch_table_name: str, batch_queries: list[int], steps_done: int
    ) -> bool:
        if batch_table_name == "transactions":
            if len(batch_queries) == 3:
                return steps_done == 0
            if len(batch_queries) == 1 and batch_queries[0] == 4:
                return False
            if len(batch_queries) == 2:
                return steps_done == 1
            if len(batch_queries) == 1 and batch_queries[0] == 3:
                return False
            if len(batch_queries) == 1 and batch_queries[0] == 1:
                return steps_done == 2
        if batch_table_name == "transaction_items":
            return steps_done == 0
        return False

    def get_duplication_count(self, batch_queries: list[int]) -> int:
        return 2 if len(batch_queries) > 1 else 1

    def get_new_batch_queries(
        self, batch_table_name: str, batch_queries: list[int], copy_number: int
    ) -> list[int]:
        if batch_table_name == "transactions":
            if len(batch_queries) == 3:
                return [4] if copy_number == 1 else [1, 3]
            if len(batch_queries) == 2:
                return [3] if copy_number == 1 else [1]
        return list(batch_queries)

    def total_steps(self, batch_table_name: str, batch_queries: list[int]) -> int:
        if batch_table_name == "transactions":
            if len(batch_queries) == 3:
                return 3
            if len(batch_queries) == 2:
                return 2
            if batch_queries == [1]:
                return 3
            if batch_queries == [3] or batch_queries == [4]:
                return 1
        return 1


class FilterRouter:
    def __init__(
        self,
        producer: "ExchangeBusProducer",
        policy: QueryPolicyResolver,
        table_cfg: TableConfig,
    ):
        self._p = producer
        self._pol = policy
        self._cfg = table_cfg
        self._log = logging.getLogger("filter-router")
        self._pending_batches: Dict[tuple[str, str], int] = defaultdict(int)
        self._pending_eof: Dict[tuple[str, str], EOFMessage] = {}
        self._state_lock = threading.Lock()

    def process_message(self, msg: Any) -> None:
        try:
            if isinstance(msg, DataBatch):
                self._handle_data(msg)
            elif isinstance(msg, EOFMessage):
                self._handle_table_eof(msg)
            else:
                self._log.warning("Unknown message type: %r", type(msg))
        except Exception as e:
            self._log.exception("Unhandled error in process_message: %s", e)

    def _handle_data(self, batch: DataBatch) -> None:
        table = table_name_of(batch)
        queries = queries_of(batch)
        mask = int(getattr(batch, "reserved_u16", 0))
        bn = int(getattr(batch, "batch_number", 0))
        cid = getattr(batch, "client_id", "")
        if not table:
            self._log.warning("Batch sin table_id válido. bn=%s", bn)
            return

        self._log.debug(
            "recv DataBatch table=%s queries=%s mask=%s shards_info=%s bn=%s cid=%s",
            table,
            queries,
            bin(mask),
            getattr(batch, "shards_info", []),
            bn,
            cid,
        )
        key = (cid, table)

        if mask == 0:
            with self._state_lock:
                self._pending_batches[table] += 1
            self._log.debug("pending++ %s -> %d", table, self._pending_batches[key])

        total_steps = self._pol.total_steps(table, queries)
        next_step = first_zero_bit(mask, total_steps)
        if next_step is not None and self._pol.steps_remaining(
            table, queries, steps_done=next_step
        ):
            batch.reserved_u16 = set_bit(mask, next_step)
            self._log.debug(
                "→ filters step=%d table=%s new_mask=%s",
                next_step,
                table,
                bin(batch.reserved_u16),
            )
            try:
                self._p.send_to_filters_pool(batch)
            except Exception as e:
                self._log.error("send_to_filters_pool failed: %s", e)
            return

        dup_count = int(self._pol.get_duplication_count(queries) or 1)
        if dup_count > 1:
            self._log.debug(
                "Fan-out x%d table=%s queries=%s", dup_count, table, queries
            )

            try:
                with self._state_lock:
                    self._pending_batches[table] += dup_count
                for i in range(dup_count):
                    new_queries = self._pol.get_new_batch_queries(
                        table, queries, copy_number=i
                    ) or list(queries)
                    b = copy.copy(batch)
                    inner = copy.copy(batch.batch_msg)
                    b.batch_msg = inner
                    b.query_ids = list(new_queries)
                    b.batch_bytes = b.batch_bytes
                    self._handle_data(b)
            except Exception as e:
                self._log.error("requeue_to_router failed: %s", e)

            with self._state_lock:
                self._pending_batches[table] = max(0, self._pending_batches[key] - 1)
            self._log.debug(
                "pending-- (fanout parent) %s -> %d",
                key,
                self._pending_batches[key],
            )
            return

        try:
            self._send_sharded_to_aggregators(batch, table, queries)
        except Exception as e:
            self._log.error("send_sharded failed: %s", e)

        with self._state_lock:
            self._pending_batches[table] = max(0, self._pending_batches[table] - 1)
        self._log.debug("pending-- %s -> %d", table, self._pending_batches[table])
        self._maybe_flush_pending_eof(key)

    def _send_sharded_to_aggregators(
        self, batch: DataBatch, table: str, queries: List[int]
    ) -> None:
        num_parts = max(1, int(self._cfg.aggregators))
        self._p.send_to_aggregator_partition(randint(0, num_parts - 1), batch)

    def _pick_part_for_empty_payload(
        self, table: str, queries: List[int], reserved_u16: int
    ) -> int:
        n = max(1, int(self._cfg.num_aggregator_partitions(table)))
        seed = hashlib.blake2b(
            f"{table}|{tuple(sorted(queries))}|{reserved_u16}".encode(),
            digest_size=8,
        ).digest()
        return int.from_bytes(seed, "little") % n

    def _handle_table_eof(self, eof: EOFMessage) -> None:
        key = (eof.table_type, eof.client_id)
        self._log.info("TABLE_EOF received: key=%s", key)
        with self._state_lock:
            self._pending_eof[key] = eof
        self._maybe_flush_pending_eof(key)

    def _maybe_flush_pending_eof(self, key: tuple[str, str]) -> None:
        with self._state_lock:
            pending = self._pending_batches.get(key, 0)
            eof = self._pending_eof.get(key)
            if eof is None or pending > 0:
                if eof is not None:
                    self._log.info("TABLE_EOF deferred: key=%s pending=%d", key, pending)
                return
            total_parts = max(1, int(self._cfg.aggregators))
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
            self._pending_eof.pop(key, None)
            self._pending_batches.pop(key, None)


class ExchangeBusProducer:
    """Thread-safe producer using thread-local connections"""
    
    def __init__(
        self,
        host: str,
        filters_pool_queue: str,
        in_mw: MessageMiddlewareExchange,
        exchange_fmt: str = "ex.{table}",
        rk_fmt: str = "agg.{table}.{pid:02d}",
        *,
        max_retries: int = 5,
        base_backoff_ms: int = 100,
        backoff_multiplier: float = 2.0,
    ):
        self._log = logging.getLogger("filter-router.bus")
        self._host = host
        self._filters_pool_queue = filters_pool_queue
        self._exchange_fmt = exchange_fmt
        self._rk_fmt = rk_fmt
        self._in_mw = in_mw

        self._max_retries = int(max_retries)
        self._base_backoff_ms = int(base_backoff_ms)
        self._backoff_multiplier = float(backoff_multiplier)
        
        # Thread-local storage for connections (Pika is NOT thread-safe)
        self._thread_local = threading.local()

    def _get_filters_pub(self):
        """Get or create thread-local filters publisher"""
        if not hasattr(self._thread_local, 'filters_pub') or self._thread_local.filters_pub is None:
            self._thread_local.filters_pub = MessageMiddlewareQueue(
                self._host, self._filters_pool_queue
            )
        return self._thread_local.filters_pub

    def _get_exchange_pub(self, exchange: str, rk: str):
        """Get or create thread-local exchange publisher"""
        if not hasattr(self._thread_local, 'exchange_pubs'):
            self._thread_local.exchange_pubs = {}
        
        key = (exchange, rk)
        if key not in self._thread_local.exchange_pubs:
            self._thread_local.exchange_pubs[key] = MessageMiddlewareExchange(
                host=self._host, exchange_name=exchange, route_keys=[rk]
            )
        return self._thread_local.exchange_pubs[key]

    def shutdown(self):
        """Closes all active publisher connections."""
        self._log.info("Shutting down ExchangeBusProducer...")
        # Note: thread-local connections will be garbage collected
        self._log.info("ExchangeBusProducer shutdown complete.")

    def _key_for(self, table: str, pid: int) -> tuple[str, str]:
        ex = self._exchange_fmt.format(table=table)
        rk = self._rk_fmt.format(table=table, pid=pid)
        return (ex, rk)

    def _send_with_retry(self, exchange: str, rk: str, payload: bytes) -> None:
        """Send with retries using thread-local connection"""
        attempt = 0
        backoff = self._base_backoff_ms / 1000.0
        last_error = None

        while attempt <= self._max_retries:
            try:
                pub = self._get_exchange_pub(exchange, rk)
                pub.send(payload)
                return
            except Exception as e:
                last_error = e
                self._log.warning(
                    "send failed (attempt %d/%d) ex=%s rk=%s: %s",
                    attempt + 1,
                    self._max_retries,
                    exchange,
                    rk,
                    e,
                )
                # Recreate connection on failure
                if hasattr(self._thread_local, 'exchange_pubs'):
                    self._thread_local.exchange_pubs.pop((exchange, rk), None)

                attempt += 1
                if attempt > self._max_retries:
                    break

                jitter = random.uniform(0, backoff * 0.2)
                sleep_s = backoff + jitter
                time.sleep(sleep_s)
                backoff *= self._backoff_multiplier

        self._log.error(
            f"Error sending message to {exchange}/{rk}: retries exhausted - {last_error}"
        )

    def send_to_filters_pool(self, batch: DataBatch) -> None:
        try:
            self._log.debug("publish → filters_pool")
            batch.batch_bytes = batch.batch_msg.to_bytes()
            pub = self._get_filters_pub()
            pub.send(batch.to_bytes())
        except Exception as e:
            self._log.error("filters_pool send failed: %s", e)

    def send_to_aggregator_partition(self, partition_id: int, batch: DataBatch) -> None:
        table = table_name_of(batch)
        ex, rk = self._key_for(table, int(partition_id))
        try:
            self._log.debug(
                "publish → aggregator table=%s part=%d", table, int(partition_id)
            )
            if getattr(batch, "batch_bytes", None) is None:
                batch.batch_bytes = batch.batch_msg.to_bytes()
            payload = batch.to_bytes()
            self._send_with_retry(ex, rk, payload)
        except Exception as e:
            self._log.error(
                "aggregator send failed table=%s part=%d: %s",
                table,
                int(partition_id),
                e,
            )

    def requeue_to_router(self, batch: DataBatch) -> None:
        try:
            self._log.debug(
                "requeue_to_router: reinyectando batch table=%s queries=%s",
                table_name_of(batch),
                queries_of(batch),
            )
            batch.batch_bytes = batch.batch_msg.to_bytes()
            raw = batch.to_bytes()
            self._in_mw.send(raw)
        except Exception as e:
            self._log.error("requeue_to_router failed: %s", e)

    def send_table_eof_to_aggregator_partition(
        self, key: tuple[str, str], partition_id: int
    ) -> None:
        try:
            payload = table_eof_to_bytes(key)
            ex, rk = self._key_for(key[0], int(partition_id))
            self._log.debug(
                "publish TABLE_EOF → aggregator key=%s part=%d", key, int(partition_id)
            )
            self._send_with_retry(ex, rk, payload)
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
        worker_threads: int = 10,
    ):
        self._producer = producer
        self._mw_in = router_in
        self._router = FilterRouter(
            producer=producer, policy=policy, table_cfg=table_cfg
        )
        self._log = logging.getLogger("filter-router-server")
        self._stop_event = stop_event
        self._executor = ThreadPoolExecutor(
            max_workers=worker_threads,
            thread_name_prefix="router-worker"
        )

    def run(self) -> None:
        self._log.info(f"RouterServer starting with {self._executor._max_workers} workers")

        def _cb(body: bytes):
            if self._stop_event.is_set():
                self._log.warning("Shutdown in progress, skipping incoming message.")
                return
            # Submit to thread pool - each thread gets its own Pika connection
            self._executor.submit(self._process_single_message, body)

        try:
            self._mw_in.start_consuming(_cb)
            self._log.info("RouterServer consuming started")
        except Exception as e:
            self._log.exception("start_consuming failed: %s", e)

    def _process_single_message(self, body: bytes) -> None:
        """Process a single message - called from thread pool"""
        try:
            if len(body) < 1:
                self._log.error("Received empty message")
                return

            opcode = body[0]
            if opcode == Opcodes.EOF:
                eof_msg = EOFMessage.deserialize_from_bytes(body)
                self._router.process_message(eof_msg)
            elif opcode == Opcodes.DATA_BATCH:
                db = DataBatch.deserialize_from_bytes(body)
                self._router.process_message(db)
            else:
                self._log.warning(f"Unwanted message opcode: {opcode}")
        except Exception as e:
            self._log.exception("Error in router callback: %s", e)

    def stop(self) -> None:
        """Stops the consumer and shuts down the producer."""
        self._log.info("Stopping Filter Router Server...")

        try:
            self._mw_in.stop_consuming()
            self._log.info("Input consumer stopped.")
        except Exception as e:
            self._log.warning(f"Error stopping input consumer: {e}")

        self._log.info("Waiting for worker threads to complete...")
        self._executor.shutdown(wait=True, timeout=30)

        try:
            self._producer.shutdown()
        except Exception as e:
            self._log.warning(f"Error during producer shutdown: {e}")

        self._log.info("Filter Router Server stopped.")