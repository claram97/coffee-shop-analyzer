from __future__ import annotations

import copy
import hashlib
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

# Se asume que existen helpers de protocolo:
# - databatch_from_bytes(raw) -> DataBatch
# - databatch_to_bytes(db) -> bytes
# - partition_eof_from_bytes(raw) -> PartitionEOF | None
# - table_eof_to_bytes(table_id: int) -> bytes
# Y tipos:
# - DataBatch con: table_ids: List[int], query_ids: List[int], batch_msg.rows: List[dict]
# - PartitionEOF con: table_id: int, partition_id: int

T_TRANSACTIONS = 0
T_USERS = 1
T_TRANSACTION_ITEMS = 2
T_MENU_ITEMS = 10
T_STORES = 11


class TableRouteCfg:
    def __init__(
        self,
        exchange_name: str,
        agg_shards: int,
        joiner_shards: int,
        key_pattern: str,
    ):
        self.exchange_name = exchange_name
        self.agg_shards = agg_shards
        self.joiner_shards = joiner_shards
        self.key_pattern = key_pattern


ROUTE_CFG: Dict[int, TableRouteCfg] = {
    T_TRANSACTION_ITEMS: TableRouteCfg(
        "ex.transaction_items",
        agg_shards=16,
        joiner_shards=32,
        key_pattern="ti.shard.{shard:02d}",
    ),
    T_TRANSACTIONS: TableRouteCfg(
        "ex.transactions",
        agg_shards=16,
        joiner_shards=32,
        key_pattern="tx.shard.{shard:02d}",
    ),
    T_USERS: TableRouteCfg(
        "ex.users",
        agg_shards=16,
        joiner_shards=32,
        key_pattern="users.shard.{shard:02d}",
    ),
    T_MENU_ITEMS: TableRouteCfg(
        "ex.menu_items",
        agg_shards=1,
        joiner_shards=32,
        key_pattern="mi.broadcast.{shard:02d}",
    ),
    T_STORES: TableRouteCfg(
        "ex.stores",
        agg_shards=1,
        joiner_shards=32,
        key_pattern="stores.broadcast.{shard:02d}",
    ),
}


def _hash_to_shard(s: str, num_shards: int) -> int:
    h = hashlib.blake2b(s.encode("utf-8"), digest_size=4).digest()
    return int.from_bytes(h, "little") % num_shards


def _shard_key_for_row(
    table_id: int, row: Dict[str, Any], queries: List[int]
) -> Optional[str]:
    q = set(queries)
    if 4 in q:
        key = row.get("user_id")
        return str(key) if key is not None else None
    if 2 in q and table_id == T_TRANSACTION_ITEMS:
        key = row.get("transaction_id")
        return str(key) if key is not None else None
    if 3 in q and table_id == T_TRANSACTIONS:
        key = row.get("transaction_id")
        return str(key) if key is not None else None
    return None


def is_broadcast_table(table_id: int, queries: List[int]) -> bool:
    q = set(queries)
    if 2 in q and table_id == T_MENU_ITEMS:
        return True
    if (3 in q or 4 in q) and table_id == T_STORES:
        return True
    return False


class ExchangePublisherPool:
    def __init__(self, factory: Callable[[str, str], "MessageMiddleware"]):
        self._factory = factory
        self._pool: Dict[Tuple[str, str], "MessageMiddleware"] = {}

    def get_pub(self, exchange_name: str, routing_key: str) -> "MessageMiddleware":
        k = (exchange_name, routing_key)
        pub = self._pool.get(k)
        if pub is None:
            pub = self._factory(exchange_name, routing_key)
            self._pool[k] = pub
        return pub


class JoinerRouter:
    def __init__(
        self, in_mw, publisher_pool, route_cfg: Dict[int, TableRouteCfg] = ROUTE_CFG
    ):
        self._in = in_mw
        self._pool = publisher_pool
        self._cfg = route_cfg
        self._pending_eofs: Dict[int, set[int]] = {}

    def run(self):
        self._in.start_consuming(self._on_raw)

    def _on_raw(self, raw: bytes):
        pe = partition_eof_from_bytes(raw)
        if pe is not None:
            self._handle_partition_eof(pe)
            return

        db = databatch_from_bytes(raw)
        if not db.table_ids:
            return
        table_id = int(db.table_ids[0])
        queries: List[int] = getattr(db, "query_ids", []) or []
        cfg = self._cfg.get(table_id)
        if cfg is None:
            return

        if is_broadcast_table(table_id, queries):
            self._broadcast(cfg, raw)
            return

        inner = db.batch_msg
        if inner is None or not hasattr(inner, "rows"):
            self._publish(cfg, shard=0, raw=raw)
            return

        rows: List[Dict[str, Any]] = inner.rows or []
        if not rows:
            self._publish(cfg, shard=0, raw=raw)
            return

        buckets: Dict[int, List[Dict[str, Any]]] = {}
        for r in rows:
            k = _shard_key_for_row(table_id, r, queries)
            shard = 0 if k is None else _hash_to_shard(k, cfg.shards)
            buckets.setdefault(shard, []).append(r)

        for shard, shard_rows in buckets.items():
            if not shard_rows:
                continue
            db_sh = copy.deepcopy(db)
            db_sh.payload.rows = shard_rows
            raw_sh = databatch_to_bytes(db_sh)
            self._publish(cfg, shard, raw_sh)

    def _handle_partition_eof(self, table_id: int, partition_id: int):
        cfg = self._cfg[table_id]
        recvd = self._pending_eofs.setdefault(table_id, set())
        recvd.add(partition_id)

        if len(recvd) >= cfg.agg_shards:
            # ya recibimos todos los PARTITION_EOF de los aggregators → emitir TABLE_EOF a joiners
            self._broadcast(
                cfg, self._make_table_eof(table_id), shards=cfg.joiner_shards
            )
            self._pending_eofs[table_id] = set()

    def _broadcast_table_eof(self, table_id: int, cfg: TableRouteCfg):
        raw_eof = table_eof_to_bytes(table_id)
        for shard in range(cfg.shards):
            rk = self._rk(cfg, shard)
            pub = self._pool.get_pub(cfg.exchange_name, rk)
            pub.send(raw_eof)

    def _rk(self, cfg: TableRouteCfg, shard: int) -> str:
        return cfg.key_pattern.format(shard=shard)

    def _publish(self, cfg: TableRouteCfg, shard: int, raw: bytes):
        rk = self._rk(cfg, shard)
        pub = self._pool.get_pub(cfg.exchange_name, rk)
        pub.send(raw)

    def _broadcast(self, cfg: TableRouteCfg, raw: bytes, shards: Optional[int] = None):
        if shards is None:
            shards = cfg.joiner_shards
        for shard in range(shards):
            rk = cfg.key_pattern.format(shard=shard)
            pub = self._pool.get_pub(cfg.exchange_name, rk)
            pub.send(raw)


def rabbit_exchange_factory(
    exchange_name: str, routing_key: str
) -> "MessageMiddleware":
    return MessageMiddlewareExchange(
        host="localhost", exchange_name=exchange_name, route_keys=[routing_key]
    )
