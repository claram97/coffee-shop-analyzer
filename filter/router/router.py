from __future__ import annotations

import copy
import hashlib
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, List, NamedTuple, Optional


class CopyInfo(NamedTuple):
    index: int
    total: int


@dataclass
class Metadata:
    table_name: str
    queries: List[int]
    total_filter_steps: int
    reserved_u16: int = 0
    copy_info: List[CopyInfo] = field(default_factory=list)


@dataclass
class DataBatch:
    payload: Any
    metadata: Metadata


@dataclass
class TableEOF:
    table_name: str


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


def _pick_key_field(table: str, queries: List[int]) -> Optional[str]:
    t = table.lower()
    qs = set(queries or [])
    if t == "transactions":
        return "user_id" if 4 in qs else "transaction_id"
    if t == "transaction_items":
        return "transaction_id"
    if t == "users":
        return "user_id"
    return None


def _group_rows_by_partition(
    table: str,
    queries: List[int],
    rows: List[Dict[str, Any]],
    num_parts: int,
) -> Dict[int, List[Dict[str, Any]]]:
    parts: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
    if num_parts <= 1:
        parts[0] = list(rows)
        return parts
    key = _pick_key_field(table, queries)
    if not key:
        parts[0] = list(rows)
        return parts
    for r in rows:
        val = r.get(key)
        if val is None:
            parts[0].append(r)
            continue
        pid = _hash_u64(str(val)) % num_parts
        parts[int(pid)].append(r)
    return parts


class BusProducer:
    def send_to_filters_pool(self, batch: DataBatch, step: int) -> None:
        raise NotImplementedError

    def send_to_aggregator_partition(self, partition_id: int, batch: DataBatch) -> None:
        raise NotImplementedError

    def send_table_eof_to_aggregator_partition(
        self, table_name: str, partition_id: int
    ) -> None:
        raise NotImplementedError

    def requeue_to_router(self, batch: DataBatch) -> None:
        raise NotImplementedError


class TableConfig:
    def num_aggregator_partitions(self, table_name: str) -> int:
        raise NotImplementedError


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
        if batch_table_name == "users":
            return steps_done == 0
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


class FilterRouter:
    def __init__(
        self, producer: BusProducer, policy: QueryPolicyResolver, table_cfg: TableConfig
    ):
        self._p = producer
        self._pol = policy
        self._cfg = table_cfg
        self._log = logging.getLogger("filter-router")
        self._pending_batches: Dict[str, int] = defaultdict(int)
        self._pending_eof: Dict[str, TableEOF] = {}

    def process_message(self, msg: Any) -> None:
        if isinstance(msg, DataBatch):
            self._handle_data(msg)
        elif isinstance(msg, TableEOF):
            self._handle_table_eof(msg)
        else:
            self._log.warning("Unknown message type: %r", type(msg))

    def _handle_data(self, batch: DataBatch) -> None:
        m = batch.metadata
        table = m.table_name

        if int(m.reserved_u16) == 0:
            self._pending_batches[table] += 1
            self._log.debug(
                "pending++ table=%s → %d", table, self._pending_batches[table]
            )

        next_step = first_zero_bit(m.reserved_u16, m.total_filter_steps)
        if next_step is not None and self._pol.steps_remaining(
            table, m.queries, steps_done=next_step
        ):
            m.reserved_u16 = set_bit(m.reserved_u16, next_step)
            self._log.debug(
                "→ filters step=%d table=%s mask=%s",
                next_step,
                table,
                bin(m.reserved_u16),
            )
            self._p.send_to_filters_pool(batch, step=next_step)
            return

        dup_count = int(self._pol.get_duplication_count(m.queries) or 1)
        if dup_count > 1:
            self._log.debug(
                "Fan-out x%d table=%s queries=%s", dup_count, table, m.queries
            )
            for i in range(dup_count):
                new_queries = self._pol.get_new_batch_queries(
                    table, m.queries, copy_number=i
                ) or list(m.queries)
                b = copy.deepcopy(batch)
                b.metadata.copy_info = m.copy_info + [
                    CopyInfo(index=i, total=dup_count)
                ]
                b.metadata.queries = new_queries
                self._p.requeue_to_router(b)
            return

        self._send_sharded_to_aggregators(batch)
        self._pending_batches[table] = max(0, self._pending_batches[table] - 1)
        self._log.debug("pending-- table=%s → %d", table, self._pending_batches[table])
        self._maybe_flush_pending_eof(table)

    def _send_sharded_to_aggregators(self, batch: DataBatch) -> None:
        m = batch.metadata
        table = m.table_name
        rows = getattr(batch.payload, "rows", None)
        num_parts = max(1, int(self._cfg.num_aggregator_partitions(table)))

        if not isinstance(rows, list):
            pid = self._pick_part_for_empty_payload(table, m)
            self._log.debug("→ aggregator (no-rows) part=%d table=%s", pid, table)
            self._p.send_to_aggregator_partition(pid, batch)
            return

        by_part = _group_rows_by_partition(table, m.queries, rows, num_parts)
        for pid, subrows in by_part.items():
            if not subrows:
                continue
            b = copy.deepcopy(batch)
            if hasattr(b.payload, "rows"):
                b.payload.rows = subrows
            elif isinstance(b.payload, dict):
                b.payload = dict(b.payload)
                b.payload["rows"] = subrows
            else:
                b.payload = type("RowsPayload", (), {"rows": subrows})()
            self._log.debug(
                "→ aggregator part=%d table=%s rows=%d", pid, table, len(subrows)
            )
            self._p.send_to_aggregator_partition(int(pid), b)

    def _pick_part_for_empty_payload(self, table: str, m: Metadata) -> int:
        n = max(1, int(self._cfg.num_aggregator_partitions(table)))
        seed = hashlib.blake2b(
            f"{table}|{tuple(sorted(m.queries))}|{m.reserved_u16}".encode(),
            digest_size=8,
        ).digest()
        return int.from_bytes(seed, "little") % n

    def _handle_table_eof(self, eof: TableEOF) -> None:
        table = eof.table_name
        self._log.debug(
            "TABLE_EOF received: table=%s pending=%d",
            table,
            self._pending_batches[table],
        )
        self._pending_eof[table] = eof
        self._maybe_flush_pending_eof(table)

    def _maybe_flush_pending_eof(self, table: str) -> None:
        pending = self._pending_batches.get(table, 0)
        eof = self._pending_eof.get(table)
        if eof is None or pending > 0:
            return
        total_parts = max(1, int(self._cfg.num_aggregator_partitions(table)))
        for part in range(total_parts):
            self._log.debug("TABLE_EOF → aggregator part=%d table=%s", part, table)
            self._p.send_table_eof_to_aggregator_partition(table, part)
        self._pending_eof.pop(table, None)
        self._pending_batches.pop(table, None)
        self._log.debug("TABLE_EOF flushed (all parts): table=%s", table)
