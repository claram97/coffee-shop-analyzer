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
        logging.debug("key: %s", key if key is not None else "?")
        return str(key) if key is not None else None

    logging.warning("returning None key for queries %s and table %d", q, table_name)
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
        router_id: int = 0,
    ):
        self._in = in_mw
        self._pool = publisher_pool
        self._cfg = route_cfg
        self._pending_eofs: Dict[tuple[TableName, str], Set[str]] = (
            {}
        )  # Changed to Set[str] for trace strings
        self._part_counter: Dict[tuple[TableName, str], int] = {}
        self._fr_replicas = fr_replicas
        self._stop_event = stop_event
        self._is_shutting_down = False
        self._router_id = router_id
        # Store unacked EOF messages: key=(table, client_id) -> list of (channel, delivery_tag)
        self._unacked_eofs: Dict[tuple[TableName, str], list] = {}
        self._eof_ack_lock = threading.Lock()
        
        # In-memory batch deduplication: track received batches per (table, client_id, query_ids_tuple)
        # This mitigates duplicate batch propagation from aggregators (no persistence needed)
        from collections import defaultdict
        self._received_batches: Dict[tuple[TableName, str, tuple], set[int]] = defaultdict(set)
        
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

    def _is_duplicate_batch(self, data_batch: DataBatch) -> bool:
        """
        Check if a batch is a duplicate. Returns True if duplicate, False otherwise.
        Tracks batches by (table, client_id, query_ids_tuple, batch_number).
        In-memory only - no persistence (loss on restart is acceptable).
        """
        table = data_batch.payload.name
        client_id = data_batch.client_id
        batch_number = data_batch.payload.batch_number
        query_ids_tuple = tuple(sorted(data_batch.query_ids))
        
        dedup_key = (table, client_id, query_ids_tuple)
        
        if batch_number in self._received_batches[dedup_key]:
            log.warning(
                "DUPLICATE batch detected and discarded in joiner router: table=%s bn=%s client=%s queries=%s",
                table, batch_number, client_id, data_batch.query_ids
            )
            return True
        
        # Mark batch as received
        self._received_batches[dedup_key].add(batch_number)
        log.debug(
            "Batch marked as received: table=%s bn=%s client=%s queries=%s",
            table, batch_number, client_id, data_batch.query_ids
        )
        return False

    def shutdown(self):
        log.info("Shutting down JoinerRouter...")
        self._is_shutting_down = True
        self._pool.shutdown()
        log.info("JoinerRouter shutdown complete.")

    def run(self):
        log.info("JoinerRouter consuming…")
        self._in.start_consuming(self._on_raw_with_ack)

    def _on_raw_with_ack(self, raw: bytes, channel=None, delivery_tag=None, redelivered=False):
        """
        Process a message and return whether to ACK immediately.
        - Returns True: ACK immediately
        - Returns False: Delay ACK (will be done manually later)
        """
        if self._is_shutting_down or self._stop_event.is_set():
            log.warning("Router is shutting down, skipping new message.")
            return True  # Auto-ack to avoid redelivery during shutdown

        envelope = Envelope()
        envelope.ParseFromString(raw)

        if envelope.type == MessageType.EOF_MESSAGE:
            return self._handle_partition_eof_like(envelope.eof, raw, channel, delivery_tag, redelivered)

        if envelope.type == MessageType.DATA_BATCH:
            db = envelope.data_batch
            
            # Check for duplicate batch and discard if already processed
            if self._is_duplicate_batch(db):
                return True  # ACK duplicate batch
        else:
            log.warning("Skipping message: unknown type")
            return True

        table = db.payload.name
        queries = db.query_ids

        cfg = self._cfg.get(table)

        if Query.Q1 in queries:
            self._publish(cfg, randint(0, cfg.joiner_shards - 1), raw)
            return True

        if cfg is None:
            log.warning("no route cfg for table=%s", table)
            return True

        log.debug("recv DataBatch table=%s queries=%s", table, queries)

        if is_broadcast_table(table):
            log.info("broadcast table=%s shards=%d", table, cfg.joiner_shards)
            self._broadcast(cfg, raw)
            return True

        if len(db.payload.rows) == 0:
            log.debug("empty rows → shard=0 (metadata-only) table=%s", table)
            self._publish(cfg, shard=randint(0, cfg.agg_shards - 1), raw=raw)
            return True

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
        
        return True

    def _on_raw(self, raw: bytes):
        """Legacy method for backward compatibility."""
        self._on_raw_with_ack(raw)

    def _handle_partition_eof_like(self, eof: EOFMessage, raw_env: bytes, channel=None, delivery_tag=None, redelivered=False) -> bool:
        """
        Handle EOF message. Returns whether to ACK immediately.
        - Returns True: ACK immediately  
        - Returns False: Delay ACK (will be done manually after forwarding to workers)
        """
        table = eof.table
        if table is None:
            log.warning("EOF without valid table_type; ignoring")
            return True
        cid = eof.client_id
        key = (table, cid)
        cfg = self._cfg.get(table)
        if cfg is None:
            log.warning("EOF for unknown table_id=%s; ignoring", table)
            return True

        if redelivered:
            log.info("TABLE_EOF REDELIVERED (recovering state): key=%s", key)
        else:
            log.debug("TABLE_EOF received: key=%s", key)

        # Store the channel and delivery_tag for later acking (append to list)
        with self._eof_ack_lock:
            if channel is not None and delivery_tag is not None:
                if key not in self._unacked_eofs:
                    self._unacked_eofs[key] = []
                self._unacked_eofs[key].append((channel, delivery_tag))

        # Use trace field to detect duplicates
        trace = eof.trace if eof.trace else None
        if trace:
            recvd = self._pending_eofs.setdefault(key, set())
            if trace in recvd:
                log.warning("Duplicate EOF ignored: key=%s trace=%s", key, trace)
                return False  # Delay ACK even for duplicates
            recvd.add(trace)
        else:
            # Fallback to old behavior if no trace (backward compatibility)
            recvd = self._pending_eofs.setdefault(key, set())
            next_idx = self._part_counter.get(key, 0) + 1
            self._part_counter[key] = next_idx
            recvd.add(next_idx)

        log.info(
            "EOF recv key=%s trace=%s progress=%d/%d",
            key,
            trace or "no-trace",
            len(recvd),
            cfg.agg_shards * self._fr_replicas,
        )

        if len(recvd) >= cfg.agg_shards * self._fr_replicas:
            log.info(
                "EOF threshold reached for key=%s → broadcast to %d shards",
                key,
                cfg.joiner_shards,
            )
            # Update trace to include joiner_router_id before broadcasting
            eof_updated = EOFMessage(
                table=eof.table, client_id=eof.client_id, trace=str(self._router_id)
            )
            env_updated = Envelope(type=MessageType.EOF_MESSAGE, eof=eof_updated)
            raw_updated = env_updated.SerializeToString()
            self._broadcast(cfg, raw_updated, shards=cfg.joiner_shards)
            
            # Now ACK the EOF message since we've successfully forwarded it
            self._ack_eof(key)
            
            # Use safe pops in case concurrent callbacks already cleared these.
            self._pending_eofs.pop(key, None)
            self._part_counter.pop(key, None)
        
        # Return False to delay ACK until fully processed
        return False
    
    def _ack_eof(self, key: tuple[TableName, str]) -> None:
        """Acknowledge all EOF messages for this key after they have been fully processed."""
        with self._eof_ack_lock:
            ack_list = self._unacked_eofs.get(key, [])
            if ack_list:
                log.info("ACKing %d TABLE_EOF messages: key=%s", len(ack_list), key)
                acked_count = 0
                failed_count = 0
                for channel, delivery_tag in ack_list:
                    try:
                        if not channel:
                            log.warning("Channel is None for EOF key=%s delivery_tag=%s - skipping ACK", key, delivery_tag)
                            failed_count += 1
                            continue
                        channel.basic_ack(delivery_tag=delivery_tag)
                        acked_count += 1
                    except Exception as e:
                        log.warning("Failed to ACK EOF key=%s delivery_tag=%s (will be redelivered): %s", key, delivery_tag, e)
                        failed_count += 1
                
                if failed_count > 0:
                    log.warning("Failed to ACK %d/%d EOF messages for key=%s - they will be redelivered", failed_count, len(ack_list), key)
                
                del self._unacked_eofs[key]

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
