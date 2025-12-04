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
    """
    Determine the sharding key for a row based on table type and active queries.
    
    The sharding strategy varies by query type:
    - USERS table: always uses normalized user_id
    - Q4 queries: uses normalized user_id from any table
    - Q2 queries on TRANSACTION_ITEMS: uses item_id
    - Q3 queries on TRANSACTIONS: uses store_id
    
    Returns None if no valid sharding key can be determined for the given
    table/query combination.
    """
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
        """
        Get or create a publisher for the given exchange and routing key.
        
        Uses thread-local pooling: each thread gets its own publisher instance
        for a given (exchange, routing_key) pair. If a publisher is closed or
        doesn't exist, a new one is created using the factory.
        
        Thread-safe: uses locks to prevent race conditions during pool access.
        """
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
    Routes DataBatch and EOF messages from Aggregators to Joiner workers.
    
    Message routing strategies:
    - DataBatch: Routes based on query type and table characteristics:
      * Q1 queries: Random shard selection
      * Light tables (menu_items, stores): Broadcast to all joiner shards
      * Other queries: Sharded by key (user_id, item_id, or store_id depending on query)
    - EOF messages: Tracks EOFs per (table, client_id) until threshold is reached
      (agg_shards * fr_replicas), then broadcasts consolidated EOF to all joiner shards
    - Cleanup messages: Blacklists clients and broadcasts cleanup to all workers
    
    Maintains state for crash recovery (blacklist and EOF progress persisted to disk)
    and handles client blacklisting to prevent processing messages from failed clients.
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
        """
        Initialize the JoinerRouter with message middleware and routing configuration.
        
        Sets up state tracking for EOF messages, blacklist management, and crash
        recovery. Loads persisted state from disk (blacklist and EOF progress) if
        available, cleaning up expired entries.
        
        Args:
            in_mw: Input message middleware to consume from
            publisher_pool: Pool of publishers for routing messages
            route_cfg: Table-specific routing configuration (TableName -> TableRouteCfg)
            fr_replicas: Number of filter router replicas (used for EOF threshold calculation)
            stop_event: Event to signal shutdown
            router_id: Unique identifier for this router instance
        """
        self._in = in_mw
        self._pool = publisher_pool
        self._cfg = route_cfg

        self._pending_eofs: Dict[tuple[TableName, str], Set[str]] = {}
        self._part_counter: Dict[tuple[TableName, str], int] = {}
        self._fr_replicas = fr_replicas
        self._stop_event = stop_event
        self._is_shutting_down = False
        self._router_id = router_id

        self._blacklist: Dict[str, float] = {}
        self._blacklist_lock = threading.Lock()

        state_dir = os.getenv("JOINER_ROUTER_STATE_DIR", "/tmp/joiner_router_state")
        os.makedirs(state_dir, exist_ok=True)
        self._blacklist_file = os.path.join(state_dir, f"blacklist_{router_id}.json")
        self._eof_state_file = os.path.join(state_dir, f"eof_state_{router_id}.json")

        self._load_and_clean_blacklist()

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

    def _load_and_clean_blacklist(self) -> None:
        """
        Load blacklist from file and remove entries older than 10 minutes.
        Called at bootstrap.
        """
        current_time = time.time()
        cutoff_time = current_time - BLACKLIST_TTL_SECONDS

        if os.path.exists(self._blacklist_file):
            try:
                with open(self._blacklist_file, "r") as f:
                    data = json.load(f)
                    self._blacklist = {
                        client_id: timestamp
                        for client_id, timestamp in data.items()
                        if timestamp > cutoff_time
                    }
                    log.info(
                        "Loaded blacklist: %d entries (removed %d old entries)",
                        len(self._blacklist),
                        len(data) - len(self._blacklist),
                    )
            except Exception as e:
                log.warning("Failed to load blacklist file: %s", e)
                self._blacklist = {}
        else:
            self._blacklist = {}
            log.info("Blacklist file not found, starting with empty blacklist")

        self._save_blacklist()

    def _save_blacklist(self) -> None:
        """Save blacklist to file."""
        try:
            with open(self._blacklist_file, "w") as f:
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
                with open(self._eof_state_file, "r") as f:
                    data = json.load(f)
                    for key_str, traces in data.items():
                        parts = key_str.split(":", 1)
                        if len(parts) == 2:
                            table_id = int(parts[0])
                            client_id = parts[1]
                            key = (table_id, client_id)
                            self._pending_eofs[key] = set(traces)
                    log.info(
                        "Loaded EOF state: %d keys recovered", len(self._pending_eofs)
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
            data = {}
            for (table_id, client_id), traces in self._pending_eofs.items():
                key_str = f"{table_id}:{client_id}"
                data[key_str] = [str(t) for t in traces]

            with open(self._eof_state_file, "w") as f:
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
        self._in.start_consuming(self._on_raw)

    def _handle_eof_or_cleanup(self, envelope: Envelope, raw: bytes):
        """
        Handle EOF or cleanup messages from the envelope.

        For EOF messages: delegates to _handle_partition_eof_like to track EOF
        progress and broadcast when threshold is reached. Blacklisted clients'
        EOFs are handled but may be ignored during processing.

        For cleanup messages: delegates to _handle_client_cleanup_like to blacklist
        the client and clean up all associated state.

        Returns True if the message was handled successfully and should be ACKed,
        False otherwise (which will cause the message to be requeued).
        """
        if envelope.type == MessageType.EOF_MESSAGE:
            return self._handle_partition_eof_like(envelope.eof, raw)

        if envelope.type == MessageType.CLEAN_UP_MESSAGE:
            return self._handle_client_cleanup_like(
                envelope.clean_up,
                raw,
            )

    def _shard_databatch(self, db, table, queries, cfg):
        """
        Shard a DataBatch by distributing rows across joiner shards based on shard keys.
        
        For each row, computes a shard key using _shard_key_for_row, hashes it to
        determine the target shard, and groups rows by shard. Then creates separate
        DataBatch messages for each shard with appropriate ShardInfo metadata and
        publishes them to the corresponding routing keys.
        
        Rows without valid shard keys are skipped.
        """
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

    def _on_raw(self, raw: bytes):
        """
        Process incoming messages and determine ACK strategy.
        
        Handles three message types:
        - DATA_BATCH: Routes based on query type (Q1=random, broadcast tables=broadcast,
          others=sharded by key). Discards batches from blacklisted clients.
        - EOF_MESSAGE: Delegates to EOF handler for threshold tracking
        - CLEAN_UP_MESSAGE: Delegates to cleanup handler for client blacklisting
        
        Returns True to ACK immediately, False to delay ACK (not currently used).
        """
        envelope = Envelope()
        envelope.ParseFromString(raw)

        if envelope.type not in [
            MessageType.DATA_BATCH,
            MessageType.EOF_MESSAGE,
            MessageType.CLEAN_UP_MESSAGE,
        ]:
            log.warning("Skipping message: unknown type")
            return True

        if envelope.type != MessageType.DATA_BATCH:
            self._handle_eof_or_cleanup(envelope, raw)

        db = envelope.data_batch

        client_id = db.client_id if db.client_id else ""
        if self._is_blacklisted(client_id):
            log.info(
                "Discarding batch from blacklisted client: table=%s bn=%s cid=%s",
                db.payload.name,
                db.payload.batch_number,
                client_id,
            )
            return True

        table = db.payload.name
        queries = db.query_ids
        log.debug("recv DataBatch table=%s queries=%s", table, queries)

        cfg = self._cfg.get(table)
        if cfg is None:
            log.warning("no route cfg for table=%s", table)
            return True

        if Query.Q1 in queries:
            self._publish(cfg, randint(0, cfg.joiner_shards - 1), raw)
            return True

        if is_broadcast_table(table):
            log.info("broadcast table=%s shards=%d", table, cfg.joiner_shards)
            self._broadcast(cfg, raw)
            return True

        if len(db.payload.rows) == 0:
            log.debug("empty rows → shard=0 (metadata-only) table=%s", table)
            self._publish(cfg, shard=randint(0, cfg.agg_shards - 1), raw=raw)
            return True

        self._shard_databatch(db, table, queries, cfg)

        return True

    def _mark_eof_as_received(self, trace, recvd, threshold, key):
        """
        Record an EOF message as received, handling deduplication and progress tracking.
        
        If trace is provided, uses it for deduplication. If not, generates a sequential
        partition counter. Handles duplicate EOFs and already-completed EOFs gracefully.
        Persists state to disk after updating to enable crash recovery.
        
        Returns True if EOF was duplicate/already complete (should be ignored),
        False if EOF was successfully recorded.
        """
        if trace:
            if trace in recvd:
                log.warning("Duplicate EOF ignored: key=%s trace=%s", key, trace)
                return True

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
            recvd.add(str(next_idx))

        log.info(
            "EOF recv key=%s trace=%s progress=%d/%d",
            key,
            trace or "no-trace",
            len(recvd),
            threshold,
        )

        self._save_eof_state()

        return False

    def _check_eof_completion(self, recvd, threshold, eof, key, cfg):
        """
        Check if EOF threshold is reached and broadcast consolidated EOF if so.
        
        When the number of received EOFs reaches the threshold (agg_shards * fr_replicas),
        creates a new EOF message with this router's trace and broadcasts it to all
        joiner shards for the table.
        """
        if len(recvd) >= threshold:
            log.info(
                "EOF threshold reached for key=%s → broadcast to %d shards",
                key,
                cfg.joiner_shards,
            )
            eof_updated = EOFMessage(
                table=eof.table, client_id=eof.client_id, trace=str(self._router_id)
            )
            env_updated = Envelope(type=MessageType.EOF_MESSAGE, eof=eof_updated)
            raw_updated = env_updated.SerializeToString()
            self._broadcast(cfg, raw_updated, shards=cfg.joiner_shards)

    def _handle_partition_eof_like(
        self,
        eof: EOFMessage,
        raw_env: bytes,
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

        threshold = cfg.agg_shards * self._fr_replicas
        trace = eof.trace if eof.trace else None
        recvd = self._pending_eofs.setdefault(key, set())

        if self._mark_eof_as_received(trace, recvd, threshold, key):
            return True

        self._check_eof_completion(recvd, threshold, eof, key, cfg)

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

        keys_to_remove = [
            key for key in self._pending_eofs.keys() if key[1] == client_id
        ]
        for key in keys_to_remove:
            self._pending_eofs.pop(key, None)
            self._part_counter.pop(key, None)

        if keys_to_remove:
            self._save_eof_state()

        state_dir = os.getenv("JOINER_ROUTER_STATE_DIR")
        if state_dir and os.path.exists(state_dir):
            try:
                for item in os.listdir(state_dir):
                    if client_id not in item:
                        continue
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

        self._add_to_blacklist(client_id)

        self._cleanup_client_state(client_id)

        cleanup_msg.trace = str(self._router_id)

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

        return True

    def _rk(self, cfg: TableRouteCfg, shard: int) -> str:
        return cfg.key_pattern.format(shard=int(shard))

    def _safe_send(self, pub, raw, ex, rk):
        """
        Send a message with automatic retry on failure.
        
        Attempts to send using the provided publisher. If it fails (e.g., connection
        closed), retrieves a new publisher from the pool and retries once. Raises
        exception if retry also fails.
        """
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
                raise

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
    """
    Build routing configuration for all tables from application config.
    
    Creates TableRouteCfg for each table with:
    - Exchange names formatted from config template
    - Routing key patterns from config template
    - Aggregator shard count (from workers.aggregators)
    - Joiner shard count (from joiner_partitions, with special handling for
      light tables that fall back to workers.joiners if partitions <= 1)
    
    Returns a dictionary mapping TableName enum values to their route configs.
    """
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
    Server wrapper that consumes from a queue and dispatches messages to JoinerRouter.
    
    Consumes messages from the input queue (Aggregators → JoinerRouter) and forwards
    them to the JoinerRouter for processing. Handles message validation and error
    handling in the consumption callback.
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
                if len(body) < 1:
                    self._log.error("Received empty message")
                    return True
                should_ack = self._router._on_raw(body)
                return should_ack
            except Exception as e:
                self._log.exception("Error in joiner-router callback: %s", e)
                return False

        self._mw_in.start_consuming(_cb)
