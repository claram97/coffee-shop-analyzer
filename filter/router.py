# filter_router.py
from __future__ import annotations

import copy
import hashlib
import logging
from collections import defaultdict
from dataclasses import dataclass
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


def table_eof_to_bytes(table_name: str) -> bytes:
    eof_msg = EOFMessage()
    eof_msg.create_eof_message(batch_number=0, table_type=table_name)
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
    # copia shallow del batch
    b = copy.copy(batch)
    # copia shallow del mensaje interno (sin duplicar filas)
    inner = copy.copy(batch.batch_msg)
    inner.rows = subrows
    b.batch_msg = inner
    # shards_info + serialización una sola vez
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
            if batch_queries == [1] or batch_queries == [3] or batch_queries == [4]:
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
        self._pending_batches: Dict[str, int] = defaultdict(int)
        self._pending_eof: Dict[str, EOFMessage] = {}

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
        rows = rows_of(batch)
        mask = int(getattr(batch, "reserved_u16", 0))
        bn = int(getattr(batch, "batch_number", 0))
        if not table:
            self._log.warning("Batch sin table_id válido. bn=%s", bn)
            return

        self._log.debug(
            "recv DataBatch table=%s queries=%s rows=%d mask=%s shards_info=%s bn=%s",
            table,
            queries,
            len(rows),
            bin(mask),
            getattr(batch, "shards_info", []),
            bn,
        )
        if self._log.isEnabledFor(logging.DEBUG) and rows:
            sample = rows[0]
            if isinstance(sample, dict):
                sk = list(sample.keys())[:8]
            else:
                sk = [k for k in dir(sample) if not k.startswith("_")][:8]
            self._log.debug("sample_row_keys=%s", sk)

        if mask == 0:
            self._pending_batches[table] += 1
            self._log.debug("pending++ %s -> %d", table, self._pending_batches[table])

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

            # NO hacer pending-- acá antes de crear hijos; lo hacemos al final del bloque.
            try:
                self._pending_batches[table] += dup_count
                for i in range(dup_count):
                    new_queries = self._pol.get_new_batch_queries(
                        table, queries, copy_number=i
                    ) or list(queries)
                    b = copy.copy(batch)
                    inner = copy.copy(batch.batch_msg)
                    inner.rows = getattr(batch.batch_msg, "rows", [])
                    b.batch_msg = inner
                    b.query_ids = list(new_queries)
                    b.batch_bytes = b.batch_msg.to_bytes()
                    self._p.requeue_to_router(b)
            except Exception as e:
                self._log.error("requeue_to_router failed: %s", e)

            # El padre ya “se dividió”, lo damos por terminado en este router
            self._pending_batches[table] = max(0, self._pending_batches[table] - 1)
            self._log.debug(
                "pending-- (fanout parent) %s -> %d",
                table,
                self._pending_batches[table],
            )
            self._maybe_flush_pending_eof(table)
            return

        try:
            self._send_sharded_to_aggregators(batch, table, queries)
        except Exception as e:
            self._log.error("send_sharded failed: %s", e)

        self._pending_batches[table] = max(0, self._pending_batches[table] - 1)
        self._log.debug("pending-- %s -> %d", table, self._pending_batches[table])
        self._maybe_flush_pending_eof(table)

    def _send_sharded_to_aggregators(
        self, batch: DataBatch, table: str, queries: List[int]
    ) -> None:
        if table in ["stores", "menu_items"]:
            self._p.send_to_aggregator_partition(0, batch)
            return
        rows = rows_of(batch)
        num_parts = max(1, int(self._cfg.aggregators))
        self._log.debug(
            "shard plan table=%s parts=%d rows=%d", table, num_parts, len(rows)
        )

        if not isinstance(rows, list) or len(rows) == 0:
            pid = self._pick_part_for_empty_payload(
                table, queries, int(getattr(batch, "reserved_u16", 0))
            )
            self._log.debug("→ aggregator (no-rows) part=%d table=%s", pid, table)
            self._p.send_to_aggregator_partition(pid, batch)
            return

        by_part = _group_rows_by_partition(table, queries, rows, num_parts)
        for pid, subrows in by_part.items():
            if not subrows:
                continue
            b = _clone_with_rows(batch, subrows, (num_parts, int(pid)))
            self._log.debug(
                "→ aggregator part=%d table=%s rows=%d", int(pid), table, len(subrows)
            )
            self._p.send_to_aggregator_partition(int(pid), b)

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
        table = eof.table_type
        self._log.info("TABLE_EOF received: table=%s", table)
        self._pending_eof[table] = eof
        self._maybe_flush_pending_eof(table)

    def _maybe_flush_pending_eof(self, table: str) -> None:
        pending = self._pending_batches.get(table, 0)
        eof = self._pending_eof.get(table)
        if eof is None or pending > 0:
            if eof is not None:
                self._log.info(
                    "TABLE_EOF deferred: table=%s pending=%d", table, pending
                )
            return
        total_parts = max(1, int(self._cfg.aggregators))
        self._log.info(
            "TABLE_EOF -> aggregators: table=%s parts=%d", table, total_parts
        )
        for part in range(total_parts):
            try:
                self._p.send_table_eof_to_aggregator_partition(table, part)
            except Exception as e:
                self._log.error(
                    "send_table_eof_to_aggregator_partition failed part=%d table=%s: %s",
                    part,
                    table,
                    e,
                )
        self._pending_eof.pop(table, None)
        self._pending_batches.pop(table, None)


class ExchangeBusProducer:
    def __init__(
        self,
        host: str,
        filters_pool_queue: str,
        in_mw: MessageMiddlewareExchange,
        exchange_fmt: str = "ex.{table}",
        rk_fmt: str = "agg.{table}.{pid:02d}",
    ):
        self._log = logging.getLogger("filter-router.bus")
        self._host = host
        self._filters_pub = MessageMiddlewareQueue(host, filters_pool_queue)
        self._exchange_fmt = exchange_fmt
        self._rk_fmt = rk_fmt
        self._pub_cache: dict[tuple[str, str], MessageMiddlewareExchange] = {}
        self._in_mw = in_mw

    def _get_pub(self, table: str, pid: int) -> MessageMiddlewareExchange:
        ex = self._exchange_fmt.format(table=table)
        rk = self._rk_fmt.format(table=table, pid=pid)
        key = (ex, rk)
        pub = self._pub_cache.get(key)
        if pub is None:
            self._log.debug(
                "create publisher exchange=%s rk=%s host=%s", ex, rk, self._host
            )
            pub = MessageMiddlewareExchange(
                host=self._host, exchange_name=ex, route_keys=[rk]
            )
            self._pub_cache[key] = pub
        return pub

    def send_to_filters_pool(self, batch: DataBatch) -> None:
        try:
            self._log.debug("publish → filters_pool")
            batch.batch_bytes = batch.batch_msg.to_bytes()
            self._filters_pub.send(batch.to_bytes())
        except Exception as e:
            self._log.error("filters_pool send failed: %s", e)

    def send_to_aggregator_partition(self, partition_id: int, batch: DataBatch) -> None:
        table = table_name_of(batch)
        try:
            self._log.debug(
                "publish → aggregator table=%s part=%d",
                table,
                int(partition_id),
            )
            batch.batch_bytes = batch.batch_msg.to_bytes()
            self._get_pub(table, partition_id).send(batch.to_bytes())
        except Exception as e:
            self._log.error(
                "aggregator send failed table=%s part=%d: %s",
                table,
                int(partition_id),
                e,
            )

    def requeue_to_router(self, batch: DataBatch) -> None:
        """
        Reenvía un DataBatch de vuelta al router de filtros.
        Se usa cuando se hace fan-out (duplicación de batches para queries múltiples).

        Args:
            batch: Instancia de DataBatch a reenviar.
        """
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
        self, table_name: str, partition_id: int
    ) -> None:
        try:
            raw = table_eof_to_bytes(table_name)
            self._log.debug(
                "publish TABLE_EOF → aggregator table=%s part=%d",
                table_name,
                int(partition_id),
            )
            self._get_pub(table_name, partition_id).send(raw)
        except Exception as e:
            self._log.error(
                "aggregator TABLE_EOF send failed table=%s part=%d: %s",
                table_name,
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
    ):
        self._mw_in = router_in
        self._router = FilterRouter(
            producer=producer, policy=policy, table_cfg=table_cfg
        )
        self._log = logging.getLogger("filter-router-server")

    def run(self) -> None:
        self._log.debug("RouterServer starting consume")

        def _cb(body: bytes):
            try:
                if len(body) < 1:
                    self._log.error("Received empty message")
                    return

                opcode = body[0]
                if opcode == Opcodes.EOF:
                    eof_msg = EOFMessage()
                    eof_msg.read_from(body[5:])
                    self._log.debug("recv EOF table=%s", eof_msg.table_type)
                    self._router.process_message(eof_msg)
                elif opcode == Opcodes.DATA_BATCH:
                    db = DataBatch.deserialize_from_bytes(body)
                    self._router.process_message(db)
                else:
                    self._log.warning(f"Unwanted message opcode: {opcode}")
            except Exception as e:
                self._log.exception("Error in router callback: %s", e)

        try:
            self._mw_in.start_consuming(_cb)
            self._log.debug("RouterServer consuming (thread started)")
        except Exception as e:
            self._log.exception("start_consuming failed: %s", e)

    def stop(self) -> None:
        try:
            self._mw_in.stop_consuming()
        except Exception:
            pass
