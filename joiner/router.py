from __future__ import annotations

import copy
import hashlib
from typing import Any, Callable, Dict, List, Optional, Tuple

# Se asume que existen:
# - MessageMiddleware (interfaz de la cátedra)
# - MessageMiddlewareExchange (tu impl. RabbitMQ)
# - databatch_from_bytes(raw) -> DataBatch
# - databatch_to_bytes(db) -> bytes
# - DataBatch con: table_ids: List[int], query_ids: List[int], batch_msg.rows: List[dict]

# ==============================
# IDs de tablas (ajusta a tus valores reales)
# ==============================
T_TRANSACTIONS = 0
T_USERS = 1
T_TRANSACTION_ITEMS = 2
T_MENU_ITEMS = 10
T_STORES = 11

# ==============================
# Config de ruteo por tabla
# ==============================


class TableRouteCfg:
    def __init__(self, exchange_name: str, shards: int, key_pattern: str):
        self.exchange_name = exchange_name
        self.shards = shards
        self.key_pattern = key_pattern  # ej: "tx.shard.{shard:02d}"


ROUTE_CFG: Dict[int, TableRouteCfg] = {
    T_TRANSACTION_ITEMS: TableRouteCfg(
        "ex.transaction_items", 32, "ti.shard.{shard:02d}"
    ),
    T_TRANSACTIONS: TableRouteCfg("ex.transactions", 32, "tx.shard.{shard:02d}"),
    T_USERS: TableRouteCfg("ex.users", 32, "users.shard.{shard:02d}"),
    T_MENU_ITEMS: TableRouteCfg("ex.menu_items", 32, "mi.broadcast.{shard:02d}"),
    T_STORES: TableRouteCfg("ex.stores", 32, "stores.broadcast.{shard:02d}"),
}

# ==============================
# Sharding helpers
# ==============================


def _hash_to_shard(s: str, num_shards: int) -> int:
    """Hash estable → shard [0, num_shards)."""
    h = hashlib.blake2b(s.encode("utf-8"), digest_size=4).digest()
    return int.from_bytes(h, "little") % num_shards


def _shard_key_for_row(
    table_id: int, row: Dict[str, Any], queries: List[int]
) -> Optional[str]:
    """
    Devuelve la *clave particionadora* (string) para esta fila según el anexo (Q2/Q3/Q4).
    Luego el shard = hash(clave) % B.
    """
    q = set(queries)

    # Q4: shard por user_id tanto en transactions como en users
    if 4 in q:
        key = row.get("user_id")
        return str(key) if key is not None else None

    # Q2: shard por transaction_id en transaction_items (menu_items → broadcast)
    if 2 in q and table_id == T_TRANSACTION_ITEMS:
        key = row.get("transaction_id")
        return str(key) if key is not None else None

    # Q3: shard por transaction_id en transactions (stores → broadcast)
    if 3 in q and table_id == T_TRANSACTIONS:
        key = row.get("transaction_id")
        return str(key) if key is not None else None

    # Si para esta combinación no corresponde sharding (o es tabla liviana → broadcast), None
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
# Router de Joiners (con split por shard)
# ==============================


class JoinerRouter:
    """
    Recibe batches del Aggregator y:
      - si la tabla es liviana y requiere broadcast → publica el mismo batch a todos los shards.
      - si la tabla se particiona → agrupa filas por shard y publica un batch por shard.
    """

    def __init__(
        self,
        in_mw: "MessageMiddleware",  # cola desde Aggregator
        publisher_pool: ExchangePublisherPool,  # pool (exchange, rk) -> publisher
        route_cfg: Dict[int, TableRouteCfg] = ROUTE_CFG,
    ):
        self._in = in_mw
        self._pool = publisher_pool
        self._cfg = route_cfg

    def run(self):
        self._in.start_consuming(self._on_raw)

    def _on_raw(self, raw: bytes):
        db = databatch_from_bytes(raw)

        if not db.table_ids:
            return
        table_id = int(db.table_ids[0])
        queries: List[int] = getattr(db, "query_ids", []) or []

        cfg = self._cfg.get(table_id)
        if cfg is None:
            # tabla no ruteable → opcional: DLQ/log
            return

        # Broadcast de tablas livianas
        if is_broadcast_table(table_id, queries):
            self._broadcast(cfg, raw)
            return

        inner = db.batch_msg
        if inner is None or not hasattr(inner, "rows"):
            # Sin filas parseadas: manda a shard 0 (o DLQ)
            self._publish(cfg, shard=0, raw=raw)
            return

        rows: List[Dict[str, Any]] = inner.rows or []
        if not rows:
            # Batch vacío: a shard 0 para que le cierre la cuenta al controller
            self._publish(cfg, shard=0, raw=raw)
            return

        # 1) Bucket por shard
        buckets: Dict[int, List[Dict[str, Any]]] = {}
        for r in rows:
            k = _shard_key_for_row(table_id, r, queries)
            if k is None:
                # Si la fila no tiene clave particionadora para esta query,
                # elegimos shard 0.
                shard = 0
            else:
                shard = _hash_to_shard(k, cfg.shards)
            buckets.setdefault(shard, []).append(r)

        # 2) Publicar un sub-batch por shard
        for shard, shard_rows in buckets.items():
            if shard_rows is None or len(shard_rows) == 0:
                continue

            # Copia liviana del DataBatch + reemplazo de filas
            db_sh = copy.deepcopy(
                db
            )  # asegura no tocar el original ni compartir referencias
            db_sh.payload.rows = (
                shard_rows  # payload ahora sólo con las filas del shard
            )
            raw_sh = databatch_to_bytes(db_sh)
            self._publish(cfg, shard, raw_sh)

    # ========== publicación ==========
    def _rk(self, cfg: TableRouteCfg, shard: int) -> str:
        return cfg.key_pattern.format(shard=shard)

    def _publish(self, cfg: TableRouteCfg, shard: int, raw: bytes):
        rk = self._rk(cfg, shard)
        pub = self._pool.get_pub(cfg.exchange_name, rk)
        pub.send(raw)

    def _broadcast(self, cfg: TableRouteCfg, raw: bytes):
        for shard in range(cfg.shards):
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
# Wiring (ejemplo)
# ==============================

# in_mw = MessageMiddlewareQueue(host="localhost", queue_name="Aggregator→JoinerRouter_q")
# pool = ExchangePublisherPool(factory=rabbit_exchange_factory)
# router = JoinerRouter(in_mw=in_mw, publisher_pool=pool, route_cfg=ROUTE_CFG)
# router.run()
