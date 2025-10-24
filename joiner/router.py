from __future__ import annotations

import hashlib
import logging
import threading
from random import randint
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from app_config.config_loader import Config
from middleware.middleware_client import (
    MessageMiddleware,
    MessageMiddlewareExchange,
    MessageMiddlewareQueue,
)
from protocol2.databatch_pb2 import DataBatch, Query, ShardInfo
from protocol2.envelope_pb2 import Envelope, MessageType
from protocol2.eof_message_pb2 import EOFMessage
from protocol2.table_data_pb2 import Row, TableName
from protocol2.table_data_utils import iterate_rows_as_dicts

STR_TO_NAME = {
    "transactions": TableName.TRANSACTIONS,
    "users": TableName.USERS,
    "transaction_items": TableName.TRANSACTION_ITEMS,
    "menu_items": TableName.MENU_ITEMS,
    "stores": TableName.STORES,
}
NAME_TO_STR = {v: k for (k, v) in STR_TO_NAME.items()}
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


def _norm_user_id(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v)
    return s.split(".", maxsplit=1)[0] if s.endswith(".0") else s


def _shard_key_for_row(
    table_name: TableName, row: dict[str, str], queries: List[Query]
) -> Optional[str]:
    q = set(queries)

    if table_name == TableName.USERS:
        uid = row["user_id"]
        return _norm_user_id(uid)

    if Query.Q4 in q:
        uid = row["user_id"]
        return _norm_user_id(uid)

    if Query.Q2 in q and table_name == TableName.TRANSACTION_ITEMS:
        key = row["item_id"]
        return str(key) if key is not None else None

    if Query.Q3 in q and table_name == TableName.TRANSACTIONS:
        key = row["store_id"]
        return str(key) if key is not None else None

    return None


def is_broadcast_table(table_name: TableName) -> bool:
    return table_name in [TableName.MENU_ITEMS, TableName.STORES]


class ExchangePublisherPool:
    def __init__(self, factory: Callable[[str, str], "MessageMiddleware"]):
        self._factory = factory
        self._pool: Dict[Tuple[str, str, int], "MessageMiddleware"] = {}
        self._lock = threading.Lock()

    def get_pub(self, exchange_name: str, routing_key: str) -> "MessageMiddleware":
        k = (exchange_name, routing_key, threading.get_ident())
        with self._lock:
            pub = self._pool.get(k)
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

    def shutdown(self):
        """Closes all active publisher connections in the pool."""
        log.info("Shutting down ExchangePublisherPool, closing all publishers...")
        with self._lock:
            for pub in self._pool.values():
                try:
                    pub.close()
                except Exception as e:
                    log.warning(f"Error closing publisher: %s", e)
            self._pool.clear()
        log.info("ExchangePublisherPool shutdown complete.")


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
        stop_event: threading.Event,
    ):
        self._in = in_mw
        self._pool = publisher_pool
        self._cfg = route_cfg
        self._pending_eofs: Dict[tuple[TableName, str], Set[int]] = {}
        self._part_counter: Dict[tuple[TableName, str], int] = {}
        self._fr_replicas = fr_replicas
        self._stop_event = stop_event
        self._is_shutting_down = False
        log.info(
            "JoinerRouter init: tables=%s",
            {
                NAME_TO_STR[k]: {
                    "ex": v.exchange_name,
                    "agg": v.agg_shards,
                    "join": v.joiner_shards,
                }
                for k, v in route_cfg.items()
            },
        )

    def shutdown(self):
        log.info("Shutting down JoinerRouter...")
        self._is_shutting_down = True
        self._pool.shutdown()
        log.info("JoinerRouter shutdown complete.")

    def run(self):
        log.info("JoinerRouter consuming…")
        self._in.start_consuming(self._on_raw)

    def _on_raw(self, raw: bytes):
        if self._is_shutting_down or self._stop_event.is_set():
            log.warning("Router is shutting down, skipping new message.")
            return

        envelope = Envelope()
        envelope.ParseFromString(raw)

        if envelope.type == MessageType.EOF_MESSAGE:
            return self._handle_partition_eof_like(envelope.eof, raw)

        if envelope.type == MessageType.DATA_BATCH:
            db = envelope.data_batch
        else:
            log.warning("Skipping message: unknown type")
            return

        table = db.payload.name
        queries = db.query_ids

        cfg = self._cfg.get(table)

        if Query.Q1 in queries:
            self._publish(cfg, randint(0, cfg.joiner_shards - 1), raw)

        if cfg is None:
            log.warning("no route cfg for table=%s", table)
            return

        log.debug("recv DataBatch table=%s queries=%s", table, queries)

        if is_broadcast_table(table):
            log.info("broadcast table=%s shards=%d", table, cfg.joiner_shards)
            self._broadcast(cfg, raw)
            return

        if len(db.payload.rows) == 0:
            log.debug("empty rows → shard=0 (metadata-only) table=%s", table)
            self._publish(cfg, shard=randint(0, cfg.agg_shards - 1), raw=raw)
            return

        buckets: Dict[int, List[Row]] = {}
        for shard in range(0, cfg.joiner_shards):
            buckets[shard] = []

        for row in iterate_rows_as_dicts(db.payload):
            k = _shard_key_for_row(table, row, queries)
            if k is None or not k.strip():
                continue
            shard = _hash_to_shard(k, cfg.joiner_shards)
            buckets[shard].append(row)

        if log.isEnabledFor(logging.DEBUG):
            sizes = {sh: len(rs) for sh, rs in buckets.items()}
            log.debug("shard plan table=%s -> %s", table, sizes)

        for shard, shard_rows in buckets.items():
            if not shard_rows:
                continue
            db_sh = DataBatch()
            db_sh.CopyFrom(db)
            shard_info = db_sh.shards_info.add()
            shard_info.total_shards = cfg.joiner_shards
            shard_info.shard_number = shard
            db_sh.payload.ClearField("rows")
            columns = db_sh.payload.schema.columns
            for row in shard_rows:
                new_row = db_sh.payload.rows.add()
                new_row.values.extend(str(row.get(col, "")) for col in columns)
            raw_sh = Envelope(
                type=MessageType.DATA_BATCH, data_batch=db_sh
            ).SerializeToString()
            self._publish(cfg, shard, raw_sh)

    def _handle_partition_eof_like(self, eof: EOFMessage, raw_env: bytes) -> None:
        table = eof.table
        if table is None:
            log.warning("EOF without valid table_type; ignoring")
            return
        cid = eof.client_id
        key = (table, cid)
        cfg = self._cfg.get(table)
        if cfg is None:
            log.warning("EOF for unknown table_id=%s; ignoring", table)
            return

        recvd = self._pending_eofs.setdefault(key, set())
        next_idx = self._part_counter.get(key, 0) + 1
        self._part_counter[key] = next_idx
        recvd.add(next_idx)

        log.info(
            "EOF recv key=%s progress=%d/%d",
            key,
            len(recvd),
            cfg.agg_shards * self._fr_replicas,
        )

        if len(recvd) >= cfg.agg_shards * self._fr_replicas:
            log.info(
                "EOF threshold reached for key=%s → broadcast to %d shards",
                key,
                cfg.joiner_shards,
            )
            self._broadcast(cfg, raw_env, shards=cfg.joiner_shards)
            # Use safe pops in case concurrent callbacks already cleared these.
            self._pending_eofs.pop(key, None)
            self._part_counter.pop(key, None)

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
        return rk_fmt.replace("{table}", table)

    route: Dict[TableName, TableRouteCfg] = {}

    for tnamestr, tname in STR_TO_NAME.items():
        agg_parts = cfg.workers.aggregators
        if tnamestr in LIGHT_TABLES:
            j_parts = cfg.joiner_partitions(tnamestr)
            if j_parts <= 1:
                j_parts = max(1, int(cfg.workers.joiners))
        else:
            j_parts = cfg.joiner_partitions(tnamestr)

        route[tname] = TableRouteCfg(
            exchange_name=_ex(tnamestr),
            agg_shards=agg_parts,
            joiner_shards=j_parts,
            key_pattern=_rk_pattern(tnamestr),
        )

    log.info(
        "Route cfg: %s",
        {
            NAME_TO_STR[k]: {
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
