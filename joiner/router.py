from __future__ import annotations

import copy
import hashlib
import logging
import threading
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
    return table_id in (Opcodes.NEW_MENU_ITEMS, Opcodes.NEW_STORES)


class ExchangePublisherPool:
    def __init__(self, factory: Callable[[str, str], "MessageMiddleware"]):
        self._factory = factory
        self._pool: Dict[Tuple[str, str, int], "MessageMiddleware"] = {}
        self._lock = threading.Lock()

    def get_pub(self, exchange_name: str, routing_key: str) -> "MessageMiddleware":
        # clave incluye el id del hilo
        k = (exchange_name, routing_key, threading.get_ident())
        with self._lock:
            pub = self._pool.get(k)
            # Si el canal se cerró, recrearlo
            if pub is None or getattr(pub, "is_closed", lambda: False)():
                log.debug(
                    "create publisher exchange=%s rk=%s (thread=%s)",
                    exchange_name,
                    routing_key,
                    k[2],
                )
                pub = self._factory(exchange_name, routing_key)
                self._pool[k] = pub
        return pub


class JoinerRouter:
    """
    - Recibe DataBatch y TABLE_EOF (desde Aggregators por cola).
    - DataBatch: broadcast (livianas) o sharding por clave (Q2/Q3/Q4) y publish por shard.
    - TABLE_EOF: cuenta por tabla; al completar `agg_shards` re-emite TABLE_EOF a TODOS los `joiner_shards`.
    """

    def __init__(
        self,
        in_mw: "MessageMiddleware",
        publisher_pool: ExchangePublisherPool,
        route_cfg: Dict[int, TableRouteCfg],
        fr_replicas: int,
    ):
        self._in = in_mw
        self._pool = publisher_pool
        self._cfg = route_cfg
        self._pending_eofs: Dict[int, Set[int]] = {}
        self._part_counter: Dict[int, int] = {}
        self._fr_replicas = fr_replicas
        log.info(
            "JoinerRouter init: tables=%s",
            {
                ID_TO_NAME[k]: {
                    "ex": v.exchange_name,
                    "agg": v.agg_shards,
                    "join": v.joiner_shards,
                }
                for k, v in route_cfg.items()
            },
        )

    def run(self):
        log.info("JoinerRouter consuming…")
        self._in.start_consuming(self._on_raw)

    def _try_parse_eof(self, body: bytes) -> Optional[EOFMessage]:
        if not body or len(body) < 1 or body[0] != Opcodes.EOF:
            return None
        try:
            return EOFMessage.deserialize_from_bytes(body)
        except Exception as e:
            log.error("EOF parse error: %s", e)
            return None

    def _try_parse_databatch(self, body: bytes) -> Optional[DataBatch]:
        if not body or len(body) < 1 or body[0] != Opcodes.DATA_BATCH:
            return None
        try:
            return DataBatch.deserialize_from_bytes(body)
        except Exception as e:
            log.error("DataBatch parse error: %s", e)
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

        # nombre → id
        if s in NAME_TO_ID:
            return NAME_TO_ID[s]

        # numérico → id
        try:
            num = int(s)
            return num if num in ID_TO_NAME else None
        except ValueError:
            return None

    def _on_raw(self, raw: bytes):
        eof = self._try_parse_eof(raw)
        if eof is not None:
            self._handle_partition_eof_like(eof, raw)
            return

        db = self._try_parse_databatch(raw)
        if db is None:
            log.warning("skip message: not DataBatch")
            return

        table_id = int(db.batch_msg.opcode)
        tname = ID_TO_NAME.get(table_id, f"#{table_id}")
        queries: List[int] = list(getattr(db, "query_ids", []) or [])
        cfg = self._cfg.get(table_id)
        if cfg is None:
            log.warning("no route cfg for table=%s (%s)", tname, table_id)
            return

        inner = getattr(db, "batch_msg", None)
        rows = (inner.rows or []) if (inner and hasattr(inner, "rows")) else []
        log.info(
            "recv DataBatch table=%s queries=%s rows=%d", tname, queries, len(rows)
        )
        if log.isEnabledFor(logging.DEBUG) and rows:
            sample = rows[0]
            keys = (
                [k for k in dir(sample) if not k.startswith("_")]
                if not isinstance(sample, dict)
                else list(sample.keys())
            )
            log.debug("sample keys: %s", keys[:8])

        if is_broadcast_table(table_id, queries):
            log.info("broadcast table=%s shards=%d", tname, cfg.joiner_shards)
            self._broadcast(cfg, raw)
            return

        if not rows:
            log.debug("empty rows → shard=0 (metadata-only) table=%s", tname)
            self._publish(cfg, shard=0, raw=raw)
            return

        # Bucket por shard
        buckets: Dict[int, List[Any]] = {}
        for r in rows:
            k = _shard_key_for_row(table_id, r, queries)
            shard = 0 if k is None else _hash_to_shard(k, cfg.joiner_shards)
            buckets.setdefault(shard, []).append(r)

        if log.isEnabledFor(logging.INFO):
            sizes = {sh: len(rs) for sh, rs in buckets.items()}
            log.info("shard plan table=%s -> %s", tname, sizes)

        # Emitir por shard
        for shard, shard_rows in buckets.items():
            if not shard_rows:
                continue
            db_sh = copy.deepcopy(db)
            db_sh.shards_info.append((cfg.joiner_shards, shard))
            if getattr(db_sh, "batch_msg", None) and hasattr(db_sh.batch_msg, "rows"):
                db_sh.batch_msg.rows = shard_rows
            db_sh.batch_bytes = db_sh.batch_msg.to_bytes()
            raw_sh = db_sh.to_bytes()
            self._publish(cfg, shard, raw_sh)

    def _handle_partition_eof_like(self, eof: EOFMessage, raw_eof: bytes) -> None:
        table_id = self._eof_table_id(eof)
        if table_id is None:
            log.warning("EOF without valid table_type; ignoring")
            return
        cfg = self._cfg.get(table_id)
        if cfg is None:
            log.warning("EOF for unknown table_id=%s; ignoring", table_id)
            return

        tname = ID_TO_NAME.get(table_id, f"#{table_id}")
        recvd = self._pending_eofs.setdefault(table_id, set())
        next_idx = self._part_counter.get(table_id, 0) + 1
        self._part_counter[table_id] = next_idx
        recvd.add(next_idx)

        log.info("EOF recv table=%s progress=%d/%d", tname, len(recvd), cfg.agg_shards)

        if len(recvd) >= cfg.agg_shards * self._fr_replicas:
            log.info(
                "EOF threshold reached for table=%s → broadcast to %d shards",
                tname,
                cfg.joiner_shards,
            )
            self._broadcast(cfg, raw_eof, shards=cfg.joiner_shards)
            self._pending_eofs[table_id] = set()
            self._part_counter[table_id] = 0

    def _rk(self, cfg: TableRouteCfg, shard: int) -> str:
        return cfg.key_pattern.format(shard=int(shard))

    def _safe_send(self, pub, raw, ex, rk):
        try:
            pub.send(raw)
        except Exception as e:
            log.warning(
                "send failed once ex=%s rk=%s: %s; recreating pub and retrying",
                ex,
                rk,
                e,
            )
            # recreate per-thread publisher and retry 1 vez
            pub2 = self._pool.get_pub(ex, rk)
            pub2.send(raw)

    def _publish(self, cfg: TableRouteCfg, shard: int, raw: bytes):
        rk = self._rk(cfg, shard)
        ex = cfg.exchange_name
        pub = self._pool.get_pub(ex, rk)
        log.debug("publish ex=%s rk=%s size=%d", ex, rk, len(raw))
        self._safe_send(pub, raw, ex, rk)

    def _broadcast(self, cfg: TableRouteCfg, raw: bytes, shards: Optional[int] = None):
        if shards is None:
            shards = cfg.joiner_shards
        for shard in range(int(shards)):
            rk = self._rk(cfg, shard)
            ex = cfg.exchange_name
            pub = self._pool.get_pub(ex, rk)
            log.debug("broadcast ex=%s rk=%s size=%d", ex, rk, len(raw))
            self._safe_send(pub, raw, ex, rk)


def build_route_cfg_from_config(cfg: "Config") -> Dict[int, TableRouteCfg]:
    ex_fmt = cfg.names.joiner_router_exchange_fmt
    rk_fmt = cfg.names.joiner_router_rk_fmt

    def _ex(table: str) -> str:
        return ex_fmt.format(table=table)

    def _rk_pattern(table: str) -> str:
        # dejamos {shard:02d} para .format(shard=..)
        return rk_fmt.replace("{table}", table)

    route: Dict[int, TableRouteCfg] = {}

    for tname, tid in NAME_TO_ID.items():
        agg_parts = cfg.workers.aggregators
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

    log.info(
        "Route cfg: %s",
        {
            ID_TO_NAME[k]: {
                "ex": v.exchange_name,
                "agg": v.agg_shards,
                "join": v.joiner_shards,
                "rk_pat": v.key_pattern,
            }
            for k, v in route.items()
        },
    )

    return route


def build_publisher_pool_from_config(cfg: "Config") -> ExchangePublisherPool:
    def factory(exchange_name: str, routing_key: str) -> "MessageMiddleware":
        return MessageMiddlewareExchange(
            host=cfg.broker.host,
            exchange_name=exchange_name,
            route_keys=[routing_key],
        )

    log.info("Publisher pool factory using host=%s", cfg.broker.host)
    return ExchangePublisherPool(factory)


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
