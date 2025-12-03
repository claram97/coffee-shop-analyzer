from __future__ import annotations

import hashlib
import json
import logging
import os
import shutil
import threading
import time
from collections import defaultdict
from random import randint
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from app_config.config_loader import Config
from middleware.middleware_client import (MessageMiddleware,
                                          MessageMiddlewareExchange,
                                          MessageMiddlewareQueue)
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
BLACKLIST_TTL_SECONDS = 600

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
    - TABLE_EOF: cuenta por tabla; al completar `agg_shards * fr_replicas` (filter_routers * aggregators) re-emite TABLE_EOF a TODOS los `joiner_shards`.
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
        # Track pending EOFs: key=(table, client_id) -> set of traces
        # EOFs remain in _pending_eofs until client cleanup removes them
        self._pending_eofs: Dict[tuple[TableName, str], Set[str]] = {}
        self._part_counter: Dict[tuple[TableName, str], int] = {}
        self._fr_replicas = fr_replicas
        self._stop_event = stop_event
        self._is_shutting_down = False
        self._router_id = router_id

        # In-memory batch deduplication: track received batches per (table, client_id, query_ids_tuple)
        # This mitigates duplicate batch propagation from aggregators (no persistence needed)
        self._received_batches: Dict[tuple[TableName, str, tuple], set[int]] = (
            defaultdict(set)
        )

        # Blacklist: client_ids that should have their batches discarded
        # Format: {client_id: timestamp} - timestamp is when the client was blacklisted
        self._blacklist: Dict[str, float] = {}
        self._blacklist_lock = threading.Lock()
        
        # Blacklist file path
        state_dir = os.getenv("JOINER_ROUTER_STATE_DIR", "/tmp/joiner_router_state")
        os.makedirs(state_dir, exist_ok=True)
        self._blacklist_file = os.path.join(state_dir, f"blacklist_{router_id}.json")
        self._eof_state_file = os.path.join(state_dir, f"eof_state_{router_id}.json")
        
        # Load and clean blacklist at bootstrap
        self._load_and_clean_blacklist()
        
        # Load EOF state at bootstrap (for crash recovery)
        self._load_eof_state()

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

        # if batch_number in self._received_batches[dedup_key]:
        #     log.warning(
        #         "DUPLICATE batch detected and discarded in joiner router: table=%s bn=%s client=%s queries=%s",
        #         table, batch_number, client_id, data_batch.query_ids
        #     )
        #     return True

        # Mark batch as received
        self._received_batches[dedup_key].add(batch_number)
        log.debug(
            "Batch marked as received: table=%s bn=%s client=%s queries=%s",
            table,
            batch_number,
            client_id,
            data_batch.query_ids,
        )
        return False

    def _load_and_clean_blacklist(self) -> None:
        """
        Load blacklist from file and remove entries older than 10 minutes.
        Called at bootstrap.
        """
        current_time = time.time()
        cutoff_time = current_time - BLACKLIST_TTL_SECONDS  # 10 minutes ago
        
        # Load from file if it exists
        if os.path.exists(self._blacklist_file):
            try:
                with open(self._blacklist_file, 'r') as f:
                    data = json.load(f)
                    # Filter out entries older than 10 minutes
                    self._blacklist = {
                        client_id: timestamp
                        for client_id, timestamp in data.items()
                        if timestamp > cutoff_time
                    }
                    log.info(
                        "Loaded blacklist: %d entries (removed %d old entries)",
                        len(self._blacklist),
                        len(data) - len(self._blacklist)
                    )
            except Exception as e:
                log.warning("Failed to load blacklist file: %s", e)
                self._blacklist = {}
        else:
            self._blacklist = {}
            log.info("Blacklist file not found, starting with empty blacklist")
        
        # Save cleaned blacklist back to file
        self._save_blacklist()

    def _save_blacklist(self) -> None:
        """Save blacklist to file."""
        try:
            with open(self._blacklist_file, 'w') as f:
                json.dump(self._blacklist, f)
        except Exception as e:
            log.error("Failed to save blacklist file: %s", e)

    def _add_to_blacklist(self, client_id: str) -> None:
        """Add a client_id to the blacklist (both in memory and file)."""
        if not client_id:
            return
        
        current_time = time.time()
        with self._blacklist_lock:
            self._blacklist[client_id] = current_time
            self._save_blacklist()
            log.info("Added client_id to blacklist: %s", client_id)

    def _is_blacklisted(self, client_id: str) -> bool:
        """Check if a client_id is in the blacklist."""
        if not client_id:
            return False
        
        with self._blacklist_lock:
            return client_id in self._blacklist

    def _load_eof_state(self) -> None:
        """
        Load EOF progress state from file for crash recovery.
        This allows us to ACK messages immediately while still being able
        to recover progress if we crash before broadcasting.
        """
        if os.path.exists(self._eof_state_file):
            try:
                with open(self._eof_state_file, 'r') as f:
                    data = json.load(f)
                    # Reconstruct _pending_eofs from persisted data
                    # Format: {"table_id:client_id": ["trace1", "trace2", ...], ...}
                    for key_str, traces in data.items():
                        parts = key_str.split(":", 1)
                        if len(parts) == 2:
                            table_id = int(parts[0])
                            client_id = parts[1]
                            key = (table_id, client_id)
                            self._pending_eofs[key] = set(traces)
                    log.info(
                        "Loaded EOF state: %d keys recovered",
                        len(self._pending_eofs)
                    )
            except Exception as e:
                log.warning("Failed to load EOF state file: %s", e)
        else:
            log.info("EOF state file not found, starting fresh")

    def _save_eof_state(self) -> None:
        """
        Persist EOF progress state to file.
        Called after updating _pending_eofs to ensure crash recovery.
        """
        try:
            # Convert keys to string format for JSON serialization
            data = {}
            for (table_id, client_id), traces in self._pending_eofs.items():
                key_str = f"{table_id}:{client_id}"
                # Convert traces to list (they might be ints or strings)
                data[key_str] = [str(t) for t in traces]
            
            with open(self._eof_state_file, 'w') as f:
                json.dump(data, f)
        except Exception as e:
            log.error("Failed to save EOF state file: %s", e)


    def shutdown(self):
        log.info("Shutting down JoinerRouter...")
        self._is_shutting_down = True
        self._pool.shutdown()
        log.info("JoinerRouter shutdown complete.")

    def run(self):
        log.info("JoinerRouter consuming…")
        self._in.start_consuming(self._on_raw_with_ack)

    def _on_raw_with_ack(
        self,
        raw: bytes,
        channel=None,
        delivery_tag=None,
        redelivered=False,
        queue_name: Optional[str] = None,
    ):
        """
        Process a message and return whether to ACK immediately.
        - Returns True: ACK immediately
        - Returns False: Delay ACK (will be done manually later)
        """
        if self._is_shutting_down or self._stop_event.is_set():
            log.warning("Router is shutting down, NACKing message for redelivery.")
            if channel is not None and delivery_tag is not None:
                try:
                    channel.basic_nack(delivery_tag=delivery_tag, requeue=True)
                except Exception as e:
                    log.warning("NACK failed during shutdown: %s", e)
            return False

        envelope = Envelope()
        envelope.ParseFromString(raw)

        if envelope.type == MessageType.EOF_MESSAGE:
            return self._handle_partition_eof_like(
                envelope.eof, raw, channel, delivery_tag, redelivered
            )

        if envelope.type == MessageType.CLEAN_UP_MESSAGE:
            return self._handle_client_cleanup_like(
                envelope.clean_up,
                raw,
                channel,
                delivery_tag,
                redelivered,
                queue_name=queue_name,
            )

        if envelope.type == MessageType.DATA_BATCH:
            db = envelope.data_batch

            # Check blacklist - discard batches from clients being cleaned up
            client_id = db.client_id if db.client_id else ""
            if self._is_blacklisted(client_id):
                log.info(
                    "Discarding batch from blacklisted client: table=%s bn=%s cid=%s",
                    db.payload.name,
                    db.payload.batch_number,
                    client_id,
                )
                return True  # ACK and discard

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

    def _handle_partition_eof_like(
        self,
        eof: EOFMessage,
        raw_env: bytes,
        channel=None,
        delivery_tag=None,
        redelivered=False,
    ) -> bool:
        """
        Handle EOF message. Returns whether to ACK immediately.
        We persist EOF progress to disk before ACKing, so we can recover
        if we crash. When threshold is reached, we broadcast the consolidated
        EOF to workers.
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

        # Use trace field to detect duplicates
        trace = eof.trace if eof.trace else None
        threshold = cfg.agg_shards * self._fr_replicas
        
        if trace:
            recvd = self._pending_eofs.setdefault(key, set())
            
            # Check if this trace was already received (deduplication)
            if trace in recvd:
                log.warning("Duplicate EOF ignored: key=%s trace=%s", key, trace)
                return True  # ACK duplicate immediately
            
            # Check if EOF is already complete (all workers received) before adding this trace
            # This prevents re-broadcasting on duplicate messages after completion
            already_complete = len(recvd) >= threshold
            if already_complete:
                log.info(
                    "EOF already complete for key=%s (received from %d workers), ignoring duplicate",
                    key,
                    len(recvd),
                )
                return True
            
            recvd.add(trace)
        else:
            # Fallback to old behavior if no trace (backward compatibility)
            recvd = self._pending_eofs.setdefault(key, set())
            
            # Check if already complete before adding
            already_complete = len(recvd) >= threshold
            if already_complete:
                log.info(
                    "EOF already complete for key=%s (received from %d workers), ignoring duplicate",
                    key,
                    len(recvd),
                )
                return True
            
            next_idx = self._part_counter.get(key, 0) + 1
            self._part_counter[key] = next_idx
            recvd.add(str(next_idx))  # Convert to string for consistency

        # Persist state BEFORE acknowledging - this is crucial for crash recovery
        self._save_eof_state()

        log.info(
            "EOF recv key=%s trace=%s progress=%d/%d",
            key,
            trace or "no-trace",
            len(recvd),
            threshold,
        )

        # Check if we've received EOF from all workers
        # Keep entry in _pending_eofs until cleanup removes it
        if len(recvd) >= threshold:
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
            
            # Note: entry remains in _pending_eofs until cleanup removes it

        # ACK immediately - state is persisted, so we can recover if we crash
        return True

    def _cleanup_client_state(self, client_id: str) -> None:
        """
        Clean all in-memory and persisted state for a given client_id.
        This includes:
        - Pending EOF state
        - Partition counter state
        - Received batch deduplication state
        - Any persisted state on disk
        """
        if not client_id:
            log.warning("_cleanup_client_state called with empty client_id")
            return

        log.info(
            "action: cleanup_client_state | result: starting | client_id: %s", client_id
        )

        # Clean pending EOFs and partition counters
        keys_to_remove = [
            key for key in self._pending_eofs.keys() if key[1] == client_id
        ]
        for key in keys_to_remove:
            self._pending_eofs.pop(key, None)
            self._part_counter.pop(key, None)
        
        # Persist the cleaned state
        if keys_to_remove:
            self._save_eof_state()

        # Clean received batches deduplication state
        # Remove all entries where client_id matches (second element of tuple key)
        keys_to_remove = [
            key for key in self._received_batches.keys() if key[1] == client_id
        ]
        for key in keys_to_remove:
            del self._received_batches[key]

        # Clean persisted state if it exists
        # Note: Joiner router doesn't typically persist state, but this is here for future-proofing
        state_dir = os.getenv("JOINER_ROUTER_STATE_DIR")
        if state_dir and os.path.exists(state_dir):
            try:
                # Look for any files/directories related to this client_id
                for item in os.listdir(state_dir):
                    if client_id in item:
                        item_path = os.path.join(state_dir, item)
                        try:
                            if os.path.isfile(item_path):
                                os.remove(item_path)
                                log.debug("Removed persisted file: %s", item_path)
                            elif os.path.isdir(item_path):
                                shutil.rmtree(item_path)
                                log.debug("Removed persisted directory: %s", item_path)
                        except Exception as e:
                            log.warning(
                                "Failed to remove persisted state %s: %s", item_path, e
                            )
            except Exception as e:
                log.warning("Failed to clean persisted state directory: %s", e)

        log.info(
            "action: cleanup_client_state | result: success | client_id: %s", client_id
        )

    def _handle_client_cleanup_like(
        self,
        cleanup_msg,
        raw_env: bytes,
        channel=None,
        delivery_tag=None,
        redelivered=False,
        queue_name: Optional[str] = None,
    ) -> bool:
        """
        Handle cleanup message. Returns whether to ACK immediately.
        On the first cleanup message for a client, we immediately:
        1. Blacklist the client
        2. Clean local state
        3. Broadcast cleanup to all workers
        Subsequent cleanup messages for the same client are just ACKed immediately.
        """
        client_id = cleanup_msg.client_id if cleanup_msg.client_id else ""
        if not client_id:
            log.warning("CLEANUP without valid client_id; ignoring")
            return True

        # Check if this client is already blacklisted (cleanup already processed)
        if self._is_blacklisted(client_id):
            log.info(
                "CLEANUP for already-blacklisted client_id=%s; ACKing immediately",
                client_id,
            )
            return True

        log.info(
            "CLEANUP received for client_id=%s → processing immediately",
            client_id,
        )

        # Blacklist client first to reject any further batches and subsequent cleanups
        self._add_to_blacklist(client_id)

        # Clean all local state for this client
        self._cleanup_client_state(client_id)

        # Stamp this joiner router id into the trace before forwarding
        cleanup_msg.trace = str(self._router_id)

        # Broadcast CLEANUP to all worker exchanges/shards
        for table_name, table_cfg in self._cfg.items():
            env = Envelope(type=MessageType.CLEAN_UP_MESSAGE, clean_up=cleanup_msg)
            raw = env.SerializeToString()
            log.info(
                "action: broadcast_cleanup_to_workers | result: broadcasting | client_id: %s | table: %s | shards: %d",
                client_id,
                NAME_TO_STR.get(table_name, table_name),
                table_cfg.joiner_shards,
            )
            self._broadcast(table_cfg, raw, shards=table_cfg.joiner_shards)

        log.info(
            "CLEANUP processing complete for client_id=%s; ACKing",
            client_id,
        )

        # ACK immediately after processing
        return True

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
            try:
                pub2 = self._pool.get_pub(ex, rk)
                pub2.send(raw)
            except Exception as e2:
                log.error(
                    "send failed after retry ex=%s rk=%s: %s",
                    ex,
                    rk,
                    e2,
                )
                raise  # Re-raise to propagate failure

    def _publish(self, cfg: TableRouteCfg, shard: int, raw: bytes):
        rk = self._rk(cfg, shard)
        ex = cfg.exchange_name
        pub = self._pool.get_pub(ex, rk)
        log.debug("publish ex=%s rk=%s size=%d", ex, rk, len(raw))
        self._safe_send(pub, raw, ex, rk)

    def _broadcast(self, cfg: TableRouteCfg, raw: bytes, shards: Optional[int] = None):
        if shards is None:
            shards = cfg.joiner_shards
        # Track successful sends - if any fail, we'll raise to trigger redelivery
        for shard in range(int(shards)):
            rk = self._rk(cfg, shard)
            ex = cfg.exchange_name
            pub = self._pool.get_pub(ex, rk)
            log.debug("broadcast ex=%s rk=%s size=%d", ex, rk, len(raw))
            self._safe_send(pub, raw, ex, rk)  # Will raise if send fails


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

        def _cb(body: bytes, channel=None, delivery_tag=None, redelivered=False):
            try:
                if len(body) < 1:
                    self._log.error("Received empty message")
                    return True  # Ack empty messages
                should_ack = self._router._on_raw_with_ack(
                    body, channel, delivery_tag, redelivered
                )
                return should_ack  # Return whether to ack immediately
            except Exception as e:
                self._log.exception("Error in joiner-router callback: %s", e)
                return False  # NACK to trigger redelivery on errors

        self._mw_in.start_consuming(_cb)
