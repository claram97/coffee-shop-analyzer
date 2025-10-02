from __future__ import annotations

import copy
import hashlib
import logging
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from app_config.config_loader import Config
from middleware.middleware_client import (
    MessageMiddleware,
    MessageMiddlewareExchange,
    MessageMiddlewareQueue,
)
from protocol.constants import Opcodes
from protocol.databatch import DataBatch
from protocol.messages import EOFMessage

NAME_TO_ID = {
    "transactions": Opcodes.NEW_TRANSACTION,
    "users": Opcodes.NEW_USERS,
    "transaction_items": Opcodes.NEW_TRANSACTION_ITEMS,
    "menu_items": Opcodes.NEW_MENU_ITEMS,
    "stores": Opcodes.NEW_STORES,
}
ID_TO_NAME = {v: k for (k, v) in NAME_TO_ID.items()}
LIGHT_TABLES = {"menu_items", "stores"}

log = logging.getLogger("joiner-router")


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
        self.key_pattern = key_pattern


def _hash_to_shard(s: str, num_shards: int) -> int:
    h = hashlib.blake2b(s.encode("utf-8"), digest_size=4).digest()
    return int.from_bytes(h, "little") % num_shards


def _shard_key_for_row(table_id: int, row, queries: List[int]) -> Optional[str]:
    q = set(queries)

    if 4 in q:
        key = getattr(row, "user_id", None)
        return str(key) if key is not None else None

    if 2 in q and table_id == Opcodes.NEW_TRANSACTION_ITEMS:
        key = getattr(row, "item_id", None)
        return str(key) if key is not None else None

    if 3 in q and table_id == Opcodes.NEW_TRANSACTION:
        key = getattr(row, "store_id", None)
        return str(key) if key is not None else None

    return None


def is_broadcast_table(table_id: int, queries: List[int]) -> bool:
    q = set(queries)
    if 2 in q and table_id == Opcodes.NEW_MENU_ITEMS:
        return True
    if (3 in q or 4 in q) and table_id == Opcodes.NEW_STORES:
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
    """
    - Recibe DataBatch y TABLE_EOF (desde Aggregators por cola).
    - DataBatch: broadcast (livianas) o sharding por clave (Q2/Q3/Q4) y publish por shard.
    - TABLE_EOF: cuenta por tabla como si vinieran de particiones de aggregator; al completar `agg_shards`
      re-emite TABLE_EOF a TODOS los `joiner_shards`.
    """

    def __init__(
        self,
        in_mw: "MessageMiddleware",
        publisher_pool: ExchangePublisherPool,
        route_cfg: Dict[int, TableRouteCfg],
    ):
        self._in = in_mw
        self._pool = publisher_pool
        self._cfg = route_cfg
        self._pending_eofs: Dict[int, Set[int]] = {}
        self._part_counter: Dict[int, int] = {}

    def run(self):
        self._in.start_consuming(self._on_raw)

    def _try_parse_eof(self, body: bytes) -> Optional[EOFMessage]:
        if not body or len(body) < 1 or body[0] != Opcodes.EOF:
            return None
        try:
            return EOFMessage.deserialize_from_bytes(body)
        except Exception:
            return None

    def _try_parse_databatch(self, body: bytes) -> Optional[DataBatch]:
        if not body or len(body) < 1 or body[0] != Opcodes.DATA_BATCH:
            return None
        try:
            return DataBatch.deserialize_from_bytes(body)
        except Exception:
            return None

    @staticmethod
    def _eof_table_id(eof: EOFMessage) -> Optional[int]:
        # Intentar ambos: atributo y método helper
        raw = getattr(eof, "table_type", None)
        if raw in (None, "") and hasattr(eof, "get_table_type"):
            try:
                raw = eof.get_table_type()
            except Exception:
                raw = None

        if raw is None:
            return None

        s = str(raw).strip().lower()

        # 1) nombre → id
        if s in NAME_TO_ID:
            return NAME_TO_ID[s]

        # 2) numérico → id
        try:
            num = int(s)
            # validar que sea un opcode conocido
            return num if num in ID_TO_NAME else None
        except ValueError:
            return None

    def _on_raw(self, raw: bytes):
        eof = self._try_parse_eof(raw)
        if eof is not None:
            self._handle_partition_eof_like(eof, raw)
            return

        db = self._try_parse_databatch(raw)
        if db is None or not db.table_ids:
            return

        table_id = int(getattr(db.batch_msg, "opcode", db.table_ids[0]))
        queries: List[int] = list(getattr(db, "query_ids", []) or [])
        cfg = self._cfg.get(table_id)
        if cfg is None:
            return

        if is_broadcast_table(table_id, queries):
            self._broadcast(cfg, raw)
            return

        inner = getattr(db, "batch_msg", None)
        if inner is None or not hasattr(inner, "rows"):
            self._publish(cfg, shard=0, raw=raw)
            return

        rows = inner.rows or []
        if not rows:
            self._publish(cfg, shard=0, raw=raw)
            return

        buckets: Dict[int, List[Any]] = {}
        for r in rows:
            k = _shard_key_for_row(table_id, r, queries)
            shard = 0 if k is None else _hash_to_shard(k, cfg.joiner_shards)
            buckets.setdefault(shard, []).append(r)

        for shard, shard_rows in buckets.items():
            if not shard_rows:
                continue
            db_sh = copy.deepcopy(db)
            if getattr(db_sh, "batch_msg", None) and hasattr(db_sh.batch_msg, "rows"):
                db_sh.batch_msg.rows = shard_rows
            db_sh.batch_bytes = db_sh.batch_msg.to_bytes()
            raw_sh = db_sh.to_bytes()
            self._publish(cfg, shard, raw_sh)

    def _handle_partition_eof_like(self, eof: EOFMessage, raw_eof: bytes) -> None:
        table_id = self._eof_table_id(eof)
        if table_id is None:
            return
        cfg = self._cfg.get(table_id)
        if cfg is None:
            return

        recvd = self._pending_eofs.setdefault(table_id, set())
        next_idx = self._part_counter.get(table_id, 0) + 1
        self._part_counter[table_id] = next_idx
        recvd.add(next_idx)

        if len(recvd) >= cfg.agg_shards:
            self._broadcast(cfg, raw_eof, shards=cfg.joiner_shards)
            self._pending_eofs[table_id] = set()
            self._part_counter[table_id] = 0

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


def build_route_cfg_from_config(cfg: "Config") -> Dict[int, TableRouteCfg]:
    """
    Construye el mapa {table_id: TableRouteCfg} usando:
      - names.joiner_router_exchange_fmt (ej: "jx.{table}")
      - names.joiner_router_rk_fmt      (ej: "join.{table}.shard.{shard:02d}")
      - agg_shards[table]               (particiones de salida del aggregator)
      - joiner_shards[table]            (shards de joiners por tabla)
      - workers.joiners como default para livianas si no hay entrada en joiner_shards
    """
    ex_fmt = cfg.names.joiner_router_exchange_fmt
    rk_fmt = cfg.names.joiner_router_rk_fmt

    def _ex(table: str) -> str:
        return ex_fmt.format(table=table)

    def _rk_pattern(table: str) -> str:
        return rk_fmt.replace("{table}", table)

    route: Dict[int, TableRouteCfg] = {}

    for tname, tid in NAME_TO_ID.items():
        agg_parts = cfg.agg_partitions(tname)
        if tname in LIGHT_TABLES:
            j_parts = cfg.joiner_partitions(tname)
            if j_parts <= 1:
                j_parts = max(1, int(cfg.workers.joiners))
        else:
            j_parts = cfg.joiner_partitions(tname)

        route[tid] = TableRouteCfg(
            exchange_name=_ex(tname),
            agg_shards=agg_parts,
            joiner_shards=j_parts,
            key_pattern=_rk_pattern(tname),
        )

    return route


def build_publisher_pool_from_config(cfg: "Config") -> ExchangePublisherPool:
    def factory(exchange_name: str, routing_key: str) -> "MessageMiddleware":
        return MessageMiddlewareExchange(
            host=cfg.broker.host, exchange_name=exchange_name, route_keys=[routing_key]
        )

    return ExchangePublisherPool(factory)


def build_joiner_router_from_config(cfg: "Config", in_queue: str) -> JoinerRouter:
    in_mw = MessageMiddlewareQueue(cfg.broker.host, in_queue)
    pool = build_publisher_pool_from_config(cfg)
    route_cfg = build_route_cfg_from_config(cfg)
    return JoinerRouter(in_mw=in_mw, publisher_pool=pool, route_cfg=route_cfg)


class JoinerRouterServer:
    """
    Consume de una cola (Aggregators→JoinerRouter) y despacha al JoinerRouter.
    """

    def __init__(self, host: str, in_queue: str, router: JoinerRouter):
        self._mw_in = MessageMiddlewareQueue(host, in_queue)
        self._router = router
        self._log = logging.getLogger("joiner-router-server")

    def run(self):
        self._log.info(
            "JoinerRouterServer consuming from '%s'",
            getattr(self._mw_in, "_queue", "?"),
        )

        def _cb(body: bytes):
            try:
                self._router._on_raw(body)
            except Exception as e:
                self._log.exception("Error in joiner-router callback: %s", e)

        self._mw_in.start_consuming(_cb)
