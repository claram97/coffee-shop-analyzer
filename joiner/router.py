# joiner/router.py
from __future__ import annotations

import copy
import hashlib
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

# ==== Helpers de protocolo (ajustá el import a tu módulo real) ====
from protocol_utils import partition_eof_from_bytes  # -> PartitionEOF | None
from protocol_utils import table_eof_to_bytes  # -> bytes
from protocol_utils import databatch_from_bytes, databatch_to_bytes

# ==== Middleware ====
from middleware.middleware_client import (
    MessageMiddleware,
    MessageMiddlewareExchange,
    MessageMiddlewareQueue,
)

# ==== Tipos de tablas ====
T_TRANSACTIONS = 0
T_USERS = 1
T_TRANSACTION_ITEMS = 2
T_MENU_ITEMS = 10
T_STORES = 11


# ==============================
# Config de ruteo por tabla
# ==============================
class TableRouteCfg:
    def __init__(
        self,
        exchange_name: str,
        agg_shards: int,
        joiner_shards: int,
        key_pattern: str,
    ):
        self.exchange_name = exchange_name
        self.agg_shards = int(agg_shards)
        self.joiner_shards = int(joiner_shards)
        self.key_pattern = key_pattern  # ej: "tx.shard.{shard:02d}"


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


# ==============================
# Sharding helpers
# ==============================
def _hash_to_shard(s: str, num_shards: int) -> int:
    h = hashlib.blake2b(s.encode("utf-8"), digest_size=4).digest()
    return int.from_bytes(h, "little") % num_shards


def _shard_key_for_row(
    table_id: int, row: Dict[str, Any], queries: List[int]
) -> Optional[str]:
    q = set(queries)

    # Q4: shard por user_id tanto en transactions como en users
    if 4 in q:
        key = row.get("user_id")
        return str(key) if key is not None else None

    # Q2: transaction_items por transaction_id (menu_items → broadcast)
    if 2 in q and table_id == T_TRANSACTION_ITEMS:
        key = row.get("transaction_id")
        return str(key) if key is not None else None

    # Q3: transactions por transaction_id (stores → broadcast)
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


# ==============================
# Pool de publishers por (exchange, routing_key)
# ==============================
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


# ==============================
# Router de Joiners (con split por shard y manejo de EOFs)
# ==============================
class JoinerRouter:
    """
    Recibe batches del Aggregator:
      - Broadcast de tablas livianas (stores/menu_items) a todos los shards de joiners.
      - Particiona tablas pesadas por clave de sharding (según query) y publica por shard.
    Además:
      - Cuenta PARTITION_EOFs por tabla (de aggregators).
      - Cuando recibe todos los PARTITION_EOF de una tabla, emite TABLE_EOF a todos los shards de joiners.
    """

    def __init__(
        self,
        in_mw: "MessageMiddleware",  # cola desde Aggregators→JoinerRouter
        publisher_pool: ExchangePublisherPool,  # pool (exchange, rk) -> publisher
        route_cfg: Dict[int, TableRouteCfg] = ROUTE_CFG,
    ):
        self._in = in_mw
        self._pool = publisher_pool
        self._cfg = route_cfg
        self._pending_eofs: Dict[int, Set[int]] = (
            {}
        )  # table_id -> {partition_ids recibidos}

    # ---- loop ----
    def run(self):
        self._in.start_consuming(self._on_raw)

    # ---- callback de consumo ----
    def _on_raw(self, raw: bytes):
        pe = partition_eof_from_bytes(raw)
        if pe is not None:
            self._handle_partition_eof(pe)
            return

        db = databatch_from_bytes(raw)
        if not db.table_ids:
            return

        table_id = int(db.table_ids[0])
        queries: List[int] = list(getattr(db, "query_ids", []) or [])
        cfg = self._cfg.get(table_id)
        if cfg is None:
            return

        # Broadcast de tablas livianas
        if is_broadcast_table(table_id, queries):
            self._broadcast(cfg, raw)  # a todos los joiner_shards
            return

        inner = getattr(db, "batch_msg", None)
        if inner is None or not hasattr(inner, "rows"):
            # Sin filas parseadas: mandar a shard 0
            self._publish(cfg, shard=0, raw=raw)
            return

        rows: List[Dict[str, Any]] = inner.rows or []
        if not rows:
            # Batch vacío: mandar a shard 0 (para que RC cuente el batch)
            self._publish(cfg, shard=0, raw=raw)
            return

        # Bucket por shard (usa joiner_shards)
        buckets: Dict[int, List[Dict[str, Any]]] = {}
        for r in rows:
            k = _shard_key_for_row(table_id, r, queries)
            shard = 0 if k is None else _hash_to_shard(k, cfg.joiner_shards)
            buckets.setdefault(shard, []).append(r)

        # Publicar sub-batches por shard
        for shard, shard_rows in buckets.items():
            if not shard_rows:
                continue
            db_sh = copy.deepcopy(db)
            if getattr(db_sh, "batch_msg", None) and hasattr(db_sh.batch_msg, "rows"):
                db_sh.batch_msg.rows = shard_rows
            raw_sh = databatch_to_bytes(db_sh)
            self._publish(cfg, shard, raw_sh)

    # ---- manejo de PARTITION_EOF ----
    def _handle_partition_eof(self, pe) -> None:
        # pe: objeto con table_id y partition_id (de aggregators)
        table_id = int(pe.table_id)
        part_id = int(pe.partition_id)

        cfg = self._cfg.get(table_id)
        if cfg is None:
            return

        recvd = self._pending_eofs.setdefault(table_id, set())
        recvd.add(part_id)

        if len(recvd) >= cfg.agg_shards:
            # Recibimos EOF de todas las particiones de aggregators -> broadcast TABLE_EOF a joiners
            raw_eof = table_eof_to_bytes(table_id)
            self._broadcast(cfg, raw_eof, shards=cfg.joiner_shards)
            # reset
            self._pending_eofs[table_id] = set()

    # ---- publicación ----
    def _rk(self, cfg: TableRouteCfg, shard: int) -> str:
        return cfg.key_pattern.format(shard=int(shard))

    def _publish(self, cfg: TableRouteCfg, shard: int, raw: bytes):
        rk = self._rk(cfg, shard)
        pub = self._pool.get_pub(cfg.exchange_name, rk)
        pub.send(raw)

    def _broadcast(self, cfg: TableRouteCfg, raw: bytes, shards: Optional[int] = None):
        if shards is None:
            shards = cfg.joiner_shards
        for shard in range(int(shards)):
            rk = self._rk(cfg, shard)
            pub = self._pool.get_pub(cfg.exchange_name, rk)
            pub.send(raw)


# ==============================
# Factory de publishers (RabbitMQ)
# ==============================
def rabbit_exchange_factory(
    exchange_name: str, routing_key: str
) -> "MessageMiddleware":
    return MessageMiddlewareExchange(
        host="localhost", exchange_name=exchange_name, route_keys=[routing_key]
    )


# ==============================
# Servidor de router (consumidor)
# ==============================
class JoinerRouterServer:
    """
    Consume de una cola (Aggregators→JoinerRouter) y despacha al JoinerRouter.
    """

    def __init__(self, host: str, in_queue: str, router: JoinerRouter):
        self._mw_in = MessageMiddlewareQueue(host, in_queue)
        self._router = router

    def run(self):
        def _cb(body: bytes):
            # El JoinerRouter ya parsea PARTITION_EOF/DataBatch adentro de _on_raw
            self._router._on_raw(body)

        self._mw_in.start_consuming(_cb)
