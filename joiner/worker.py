from __future__ import annotations

import json
import logging
import os
import threading
import time
from collections import defaultdict
from typing import Callable, Dict, List, Optional, Set, Tuple

from middleware.middleware_client import MessageMiddleware
from protocol2.databatch_pb2 import DataBatch, Query
from protocol2.envelope_pb2 import Envelope, MessageType
from protocol2.eof_message_pb2 import EOFMessage
from protocol2.table_data_pb2 import Row, TableData, TableName, TableSchema
from protocol2.table_data_utils import iterate_rows_as_dicts

log = logging.getLogger("joiner-worker")

STR_TO_NAME = {
    "transactions": TableName.TRANSACTIONS,
    "users": TableName.USERS,
    "transaction_items": TableName.TRANSACTION_ITEMS,
    "menu_items": TableName.MENU_ITEMS,
    "stores": TableName.STORES,
}
NAME_TO_STR = {v: k for (k, v) in STR_TO_NAME.items()}
BLACKLIST_TTL_SECONDS = 600


def norm(v) -> str:
    return "" if v is None else str(v)


def index_by_attr(table_data: TableData, attr: str) -> Dict[str, dict[str, str]]:
    """
    Create an index dictionary from table data using the specified attribute as the key.

    Returns a dictionary mapping attribute values to row dictionaries.
    Rows with None or empty attribute values are skipped.
    """
    idx: Dict[str, dict[str, str]] = {}
    for r in iterate_rows_as_dicts(table_data):
        logging.info("table: %s, row: %s", table_data.name, r)
        k = norm(r[attr])
        if k:
            idx[k] = r
    return idx


class JoinerWorker:
    def __init__(
        self,
        in_mw: Dict[TableName, MessageMiddleware],
        out_results_mw: MessageMiddleware,
        router_replicas: int,
        stop_event: threading.Event,
        logger=None,
        shard_index: int = 0,
        out_factory: Optional[Callable[[], MessageMiddleware]] = None,
        persistence_dir: str = "/tmp/joiner_state",
        write_buffer_size: int = 100,
    ):
        """
        Initialize the JoinerWorker.

        Args:
            in_mw: Dictionary mapping table names to input message middleware
            out_results_mw: Output message middleware for sending results
            router_replicas: Number of router replicas (used for EOF threshold calculation)
            stop_event: Event to signal shutdown
            logger: Optional logger instance
            shard_index: Shard identifier for this worker instance
            out_factory: Optional factory function to recreate output middleware on failure
            persistence_dir: Directory for persisting state and recovery data
            write_buffer_size: Number of rows to buffer before persisting progress
        """
        self._in = in_mw
        self._out = out_results_mw
        self._out_factory = out_factory
        self._out_lock = threading.Lock()

        self._cache_stores: Dict[str, dict[str, dict[str, str]]] = {}
        self._cache_menu: Dict[str, dict[str, dict[str, str]]] = {}

        self._eof: Set[Tuple[TableName, str]] = set()
        self._lock = threading.Lock()
        self._threads: Dict[TableName, threading.Thread] = {}
        self._log = logger or log
        self._shard = int(shard_index)
        self._router_replicas = router_replicas

        self._pending_eofs: Dict[tuple[TableName, str], Set[str]] = {}

        self._num_input_queues = len(in_mw)

        self._stop_event = stop_event
        self._is_shutting_down = False

        self._persistence_dir = persistence_dir
        self._persistence_lock = threading.Lock()
        os.makedirs(self._persistence_dir, exist_ok=True)

        self._write_buffer_size = write_buffer_size
        self._written_rows: Dict[tuple[TableName, str, int], set[int]] = defaultdict(
            set
        )

        self._blacklist: Dict[str, float] = {}
        self._blacklist_lock = threading.Lock()

        self._blacklist_file = os.path.join(
            self._persistence_dir, f"blacklist_{shard_index}.json"
        )

        self._load_and_clean_blacklist()

        self._restore_state()

    def _get_pending_batch_path(
        self, table_name: TableName, client_id: str, batch_number: int
    ) -> str:
        """Get file path for a pending batch."""
        return os.path.join(
            self._persistence_dir,
            f"pending_{table_name}_{client_id}_{batch_number}.bin",
        )

    def _get_light_table_path(self, table_name: TableName, client_id: str) -> str:
        """Get file path for light table cache."""
        return os.path.join(
            self._persistence_dir, f"light_{table_name}_{client_id}.json"
        )

    def _get_eof_path(self, table_name: TableName, client_id: str) -> str:
        """Get file path for EOF metadata."""
        return os.path.join(self._persistence_dir, f"eof_{table_name}_{client_id}.json")

    def _get_written_rows_path(
        self, table_name: TableName, client_id: str, batch_number: int
    ) -> str:
        """Get file path for written rows tracking."""
        return os.path.join(
            self._persistence_dir,
            f"written_rows_{table_name}_{client_id}_{batch_number}.json",
        )

    def _save_pending_batch(
        self, table_name: TableName, client_id: str, batch_number: int, raw_data: bytes
    ) -> None:
        """Save a pending batch to disk."""
        path = self._get_pending_batch_path(table_name, client_id, batch_number)
        temp_path = path + ".tmp"
        try:
            with open(temp_path, "wb") as f:
                f.write(raw_data)
            os.rename(temp_path, path)
            self._log.info(
                "Saved pending batch: table=%s client=%s batch=%d size=%d bytes",
                table_name,
                client_id,
                batch_number,
                len(raw_data),
            )
        except Exception as e:
            self._log.error("Failed to save pending batch: %s", e)
            if os.path.exists(temp_path):
                os.remove(temp_path)
            raise

    def _delete_pending_batch(
        self, table_name: TableName, client_id: str, batch_number: int
    ) -> None:
        """Delete a pending batch from disk."""
        path = self._get_pending_batch_path(table_name, client_id, batch_number)
        try:
            if os.path.exists(path):
                os.remove(path)
                self._log.debug(
                    "Deleted pending batch: table=%s client=%s batch=%d",
                    table_name,
                    client_id,
                    batch_number,
                )
        except Exception as e:
            self._log.warning("Failed to delete pending batch: %s", e)

    def _load_pending_batches(
        self, table_name: TableName, client_id: str
    ) -> List[Tuple[int, bytes]]:
        """Load all pending batches for a table/client from disk."""
        pattern = f"pending_{table_name}_{client_id}_"
        batches = []
        try:
            for filename in os.listdir(self._persistence_dir):
                if filename.startswith(pattern) and filename.endswith(".bin"):
                    batch_num_str = filename[len(pattern) : -4]
                    batch_number = int(batch_num_str)
                    path = os.path.join(self._persistence_dir, filename)
                    with open(path, "rb") as f:
                        raw_data = f.read()
                    batches.append((batch_number, raw_data))
            return sorted(batches)
        except Exception as e:
            self._log.error("Failed to load pending batches: %s", e)
            return []

    def _persist_light_table(
        self, table_name: TableName, client_id: str, data: dict
    ) -> None:
        """Persist light table cache to disk."""
        path = self._get_light_table_path(table_name, client_id)
        temp_path = path + ".tmp"
        try:
            with open(temp_path, "w") as f:
                json.dump(data, f)
            os.rename(temp_path, path)
            self._log.debug(
                "Persisted light table: table=%s client=%s", table_name, client_id
            )
        except Exception as e:
            self._log.error("Failed to persist light table: %s", e)
            if os.path.exists(temp_path):
                os.remove(temp_path)
            raise

    def _persist_eof(self, table_name: TableName, client_id: str, traces: set) -> None:
        """Persist EOF metadata to disk."""
        path = self._get_eof_path(table_name, client_id)
        temp_path = path + ".tmp"
        try:
            with open(temp_path, "w") as f:
                json.dump(
                    {
                        "traces": list(traces),
                        "complete": len(traces) >= self._router_replicas,
                    },
                    f,
                )
            os.rename(temp_path, path)
            self._log.debug(
                "Persisted EOF: table=%s client=%s traces=%d",
                table_name,
                client_id,
                len(traces),
            )
        except Exception as e:
            self._log.error("Failed to persist EOF: %s", e)
            if os.path.exists(temp_path):
                os.remove(temp_path)
            raise

    def _persist_written_rows(
        self,
        table_name: TableName,
        client_id: str,
        batch_number: int,
        written_rows: set[int],
    ) -> None:
        """Persist written rows tracking to disk."""
        path = self._get_written_rows_path(table_name, client_id, batch_number)
        temp_path = path + ".tmp"
        try:
            with open(temp_path, "w") as f:
                json.dump({"written_rows": list(written_rows)}, f)
            os.rename(temp_path, path)
            self._log.debug(
                "Persisted written rows: table=%s client=%s bn=%s count=%d",
                table_name,
                client_id,
                batch_number,
                len(written_rows),
            )
        except Exception as e:
            self._log.error("Failed to persist written rows: %s", e)
            if os.path.exists(temp_path):
                os.remove(temp_path)
            raise

    def _delete_written_rows_tracking(
        self, table_name: TableName, client_id: str, batch_number: int
    ) -> None:
        """Delete written rows tracking file after batch is fully processed."""
        path = self._get_written_rows_path(table_name, client_id, batch_number)
        try:
            if os.path.exists(path):
                os.remove(path)
                self._log.debug(
                    "Deleted written rows tracking: table=%s client=%s bn=%s",
                    table_name,
                    client_id,
                    batch_number,
                )
        except Exception as e:
            self._log.warning("Failed to delete written rows tracking %s: %s", path, e)

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
                    self._log.info(
                        "Loaded blacklist: %d entries (removed %d old entries)",
                        len(self._blacklist),
                        len(data) - len(self._blacklist),
                    )
            except Exception as e:
                self._log.warning("Failed to load blacklist file: %s", e)
                self._blacklist = {}
        else:
            self._blacklist = {}
            self._log.info("Blacklist file not found, starting with empty blacklist")

        self._save_blacklist()

    def _save_blacklist(self) -> None:
        """Save blacklist to file."""
        try:
            with open(self._blacklist_file, "w") as f:
                json.dump(self._blacklist, f)
        except Exception as e:
            self._log.error("Failed to save blacklist file: %s", e)

    def _add_to_blacklist(self, client_id: str) -> None:
        """Add a client_id to the blacklist (both in memory and file)."""
        if not client_id:
            return

        current_time = time.time()
        with self._blacklist_lock:
            self._blacklist[client_id] = current_time
            self._save_blacklist()
            self._log.info("Added client_id to blacklist: %s", client_id)

    def _is_blacklisted(self, client_id: str) -> bool:
        """Check if a client_id is in the blacklist."""
        if not client_id:
            return False

        with self._blacklist_lock:
            return client_id in self._blacklist

    def _restore_light_tables(self, filename):
        """
        Restore light table cache (menu_items or stores) from a persisted file.
        Called during state restoration on startup.
        """
        try:
            parts = filename[6:-5].split("_", 1)
            table_name = int(parts[0])
            client_id = parts[1]

            path = os.path.join(self._persistence_dir, filename)
            with open(path, "r") as f:
                data = json.load(f)

            if table_name == TableName.MENU_ITEMS:
                self._cache_menu[client_id] = data
                self._log.info("Restored menu cache for client=%s", client_id)
            elif table_name == TableName.STORES:
                self._cache_stores[client_id] = data
                self._log.info("Restored stores cache for client=%s", client_id)
        except Exception as e:
            self._log.warning("Failed to restore light table from %s: %s", filename, e)

    def _restore_eof_state(self, filename):
        """
        Restore EOF state from a persisted file.
        Reconstructs pending EOF tracking and marks complete EOFs if applicable.
        Called during state restoration on startup.
        """
        try:
            parts = filename[4:-5].split("_", 1)
            table_name = int(parts[0])
            client_id = parts[1]

            path = os.path.join(self._persistence_dir, filename)
            with open(path, "r") as f:
                eof_data = json.load(f)

            key = (table_name, client_id)
            traces = eof_data.get("traces", [])

            if traces:
                self._pending_eofs[key] = set(traces)
                self._log.info(
                    "Restored partial EOF for table=%s client=%s traces=%d",
                    table_name,
                    client_id,
                    len(traces),
                )

            if eof_data.get("complete", False):
                self._eof.add(key)
                self._log.info(
                    "Restored complete EOF for table=%s client=%s",
                    table_name,
                    client_id,
                )
        except Exception as e:
            self._log.warning("Failed to restore EOF from %s: %s", filename, e)

    def _restore_written_rows(self, filename):
        """
        Restore written rows tracking from a persisted file.
        Used to resume batch processing from where it left off after a crash.
        Called during state restoration on startup.
        """
        try:
            parts = filename[13:-5].split("_", 2)
            table_name = int(parts[0])
            client_id = parts[1]
            batch_number = int(parts[2])

            path = os.path.join(self._persistence_dir, filename)
            with open(path, "r") as f:
                data = json.load(f)

            key = (table_name, client_id, batch_number)
            written_rows = set(data.get("written_rows", []))
            self._written_rows[key] = written_rows
            self._log.info(
                "Restored written rows for table=%s client=%s bn=%s count=%d",
                table_name,
                client_id,
                batch_number,
                len(written_rows),
            )
        except Exception as e:
            self._log.warning("Failed to restore written rows from %s: %s", filename, e)

    def _restore_state(self) -> None:
        """Restore state from disk on startup."""
        if not os.path.exists(self._persistence_dir):
            return

        self._log.info("Restoring state from disk: %s", self._persistence_dir)

        for filename in os.listdir(self._persistence_dir):
            if filename.startswith("light_") and filename.endswith(".json"):
                self._restore_light_tables(filename)

            elif filename.startswith("eof_") and filename.endswith(".json"):
                self._restore_eof_state(filename)

            elif filename.startswith("written_rows_") and filename.endswith(".json"):
                self._restore_written_rows(filename)

        self._log.info("State restoration complete")

    def _cleanup_persisted_client(self, client_id: str) -> None:
        """Clean up all persisted data for a client."""
        try:
            all_files = os.listdir(self._persistence_dir)
        except Exception as e:
            self._log.error(
                "Failed to list persistence dir %s: %s", self._persistence_dir, e
            )
            return

        matching_files = [f for f in all_files if client_id in f]
        self._log.info(
            "Cleanup persisted client=%s: found %d files, %d matching in %s",
            client_id,
            len(all_files),
            len(matching_files),
            self._persistence_dir,
        )

        deleted_count = 0
        for filename in matching_files:
            filepath = os.path.join(self._persistence_dir, filename)
            try:
                os.remove(filepath)
                deleted_count += 1
                self._log.debug("Cleaned up: %s", filename)
            except Exception as e:
                self._log.warning("Failed to cleanup %s: %s", filename, e)

        self._log.info(
            "Cleanup persisted client=%s: deleted %d/%d files",
            client_id,
            deleted_count,
            len(matching_files),
        )

    def _cleanup_client_state(self, client_id: str) -> None:
        """
        Clean all in-memory and persisted state for a given client_id.
        This includes:
        - Menu and stores caches
        - EOF tracking
        - Written rows tracking
        - Persisted data on disk
        """
        if not client_id:
            self._log.warning("_cleanup_client_state called with empty client_id")
            return

        self._log.info(
            "action: cleanup_client_state | result: starting | client_id: %s", client_id
        )

        with self._lock:
            self._cache_menu.pop(client_id, None)
            self._cache_stores.pop(client_id, None)

            eof_keys_to_remove = [key for key in self._eof if key[1] == client_id]
            for key in eof_keys_to_remove:
                self._eof.discard(key)

            keys_to_remove = [
                key for key in self._pending_eofs.keys() if key[1] == client_id
            ]
            for key in keys_to_remove:
                self._pending_eofs.pop(key, None)

            keys_to_remove = [
                key for key in self._written_rows.keys() if key[1] == client_id
            ]
            for key in keys_to_remove:
                del self._written_rows[key]

        with self._persistence_lock:
            self._cleanup_persisted_client(client_id)

        self._log.info(
            "action: cleanup_client_state | result: success | client_id: %s", client_id
        )

    def _handle_client_cleanup(
        self, cleanup_msg, queue_name: Optional[str] = None
    ) -> None:
        """
        Handle client cleanup message from joiner router.

        When a cleanup message is received, this method:
        1. Extracts the client_id from the message
        2. Adds the client to the blacklist to prevent further processing
        3. Cleans up all in-memory and persisted state for the client
        4. If this is shard 0, forwards the cleanup message to the results router
           (only shard 0 forwards to avoid duplicate messages downstream)

        This ensures that all state related to a client is removed when cleanup
        is requested, preventing memory leaks and stale data.
        """
        client_id = cleanup_msg.client_id if cleanup_msg.client_id else ""
        if not client_id:
            self._log.warning("Received client_cleanup message with empty client_id")
            return

        self._log.info(
            "CLEANUP recv client_id=%s",
            client_id,
        )

        self._add_to_blacklist(client_id)

        self._cleanup_client_state(client_id)

        if self._shard == 0:
            env = Envelope(type=MessageType.CLEAN_UP_MESSAGE, clean_up=cleanup_msg)
            raw = env.SerializeToString()
            self._log.info(
                "action: forward_cleanup_to_results_router | result: forwarding | client_id: %s",
                client_id,
            )
            self._safe_send(raw)
        else:
            self._log.debug(
                "action: cleanup_forward | result: skipped | client_id: %s | shard: %d (only shard 0 forwards)",
                client_id,
                self._shard,
            )

    def _log_db(self, where: str, db: DataBatch):
        try:
            t = db.payload.name
            q = db.query_ids
            n = len(db.payload.rows)
            self._log.debug("%s: table=%s queries=%s rows=%s", where, t, q, n)
        except Exception as e:
            self._log.debug("log_db failed: %s", e)

    def run(self):
        """
        Start consuming messages from all input queues.
        Spawns separate threads for each table's message queue.
        Blocks until shutdown is requested.
        """
        self._log.info("JoinerWorker shard=%d: iniciando consumidores", self._shard)
        self._start_queue(TableName.MENU_ITEMS, self._on_raw_menu)
        self._start_queue(TableName.STORES, self._on_raw_stores)
        self._start_queue(TableName.TRANSACTION_ITEMS, self._on_raw_ti)
        self._start_queue(TableName.TRANSACTIONS, self._on_raw_tx)
        self._start_queue(TableName.USERS, self._on_raw_users)

    def shutdown(self):
        """Initiates the shutdown of the worker and all its resources."""
        self._log.info("Shutting down JoinerWorker...")
        self._is_shutting_down = True

        self._log.info("Stopping all consumers...")
        for table_id in self._in.keys():
            self._stop_queue(table_id)

        self._log.info("Waiting for consumer threads to finish...")
        for table_name, t in list(self._threads.items()):
            if t.is_alive():
                t.join(timeout=5.0)
                if t.is_alive():
                    self._log.warning(
                        "Consumer thread for table=%s did not finish within timeout",
                        table_name,
                    )

        self._log.info("Closing all middleware connections...")

        for table_name, mw in self._in.items():
            try:
                if mw:
                    mw.close()
                    self._log.debug("Closed input middleware for table=%s", table_name)
            except Exception as e:
                self._log.warning(
                    "Error closing input middleware for table=%s: %s", table_name, e
                )

        try:
            self._out.close()
            self._log.debug("Closed output middleware")
        except Exception as e:
            self._log.warning("Error closing outbound middleware: %s", e)

        self._log.info("JoinerWorker shutdown complete.")

    def _start_queue(self, table_name: TableName, cb: Callable[[bytes], None]):
        """
        Start a consumer thread for the specified table's message queue.
        If the queue is already running, this is a no-op.
        """
        mw = self._in.get(table_name)
        if not mw:
            self._log.debug("No hay consumer para table_id=%s", table_name)
            return
        if table_name in self._threads and self._threads[table_name].is_alive():
            return
        t = threading.Thread(target=mw.start_consuming, args=(cb,), daemon=False)
        self._threads[table_name] = t
        t.start()
        self._log.info("Consumiendo table=%s", table_name)

    def _stop_queue(self, table_name: TableName):
        """
        Stop consuming messages from the specified table's queue.
        Calls stop_consuming() directly to ensure proper shutdown sequence.
        """
        mw = self._in.get(table_name)
        if not mw:
            return
        try:
            mw.stop_consuming()
            if self._log:
                self._log.info("Detenida cola table=%s", table_name)
        except Exception as e:
            if self._log:
                self._log.warning("Error deteniendo cola %s: %s", table_name, e)

    def _phase_ready(self, table_name: TableName, client_id: str) -> bool:
        """
        Check if all prerequisite tables have received EOF for the given client.

        For TRANSACTION_ITEMS and TRANSACTIONS, requires MENU_ITEMS and STORES EOF.
        For USERS, no prerequisites are needed.
        Returns True if the table can be processed, False if it must wait.
        """
        need = []
        if table_name == TableName.TRANSACTION_ITEMS:
            need = [TableName.MENU_ITEMS, TableName.STORES]
        elif table_name == TableName.TRANSACTIONS:
            need = [TableName.MENU_ITEMS, TableName.STORES]
        elif table_name == TableName.USERS:
            need = []
        else:
            need = []
        return all((t, client_id) in self._eof for t in need)

    def _requeue(self, table_name: TableName, raw: bytes):
        mw = self._in.get(table_name)
        try:
            mw.send(raw)
            self._log.debug(
                "requeue → same queue table_id=%s bytes=%d", table_name, len(raw)
            )
        except Exception as e:
            self._log.error("requeue failed table_id=%s: %s", table_name, e)

    def _handle_eof_or_cleanup(self, envelope: Envelope, table: TableName):
        """
        Handle EOF or cleanup messages from the envelope.

        For EOF messages: checks if the client is blacklisted, and if not, processes
        the EOF through _on_table_eof. Blacklisted clients' EOFs are discarded.

        For cleanup messages: delegates to _handle_client_cleanup to blacklist the
        client and clean up all associated state.

        Returns True if the message was handled successfully and should be ACKed,
        False otherwise (which will cause the message to be requeued).
        """
        if envelope.type == MessageType.EOF_MESSAGE:
            eof: EOFMessage = envelope.eof
            if self._is_blacklisted(eof.client_id):
                self._log.info(
                    "Discarding EOF from blacklisted client: table=%s cid=%s",
                    eof.table,
                    eof.client_id,
                )
                return True
            return self._on_table_eof(
                eof.table,
                eof.client_id,
                eof.trace,
            )
        if envelope.type == MessageType.CLEAN_UP_MESSAGE:
            self._handle_client_cleanup(
                envelope.clean_up,
                queue_name=NAME_TO_STR.get(table),
            )
            return True

    def _on_raw_light_table(self, raw: bytes, table: TableName):
        """
        Process messages for light tables (menu_items or stores).

        Parses the envelope and handles three message types:
        - EOF_MESSAGE: Delegates to _handle_eof_or_cleanup
        - CLEAN_UP_MESSAGE: Delegates to _handle_eof_or_cleanup
        - DATA_BATCH: Processes the batch to build the in-memory cache

        For data batches: checks if the client is blacklisted and discards if so.
        Otherwise, processes the batch using _process_light_table_buffered to build
        the cache (menu_items indexed by item_id, stores indexed by store_id).
        The cache is persisted incrementally to support crash recovery.

        Returns True to ACK immediately after processing or discarding.
        """
        envelope = Envelope()
        envelope.ParseFromString(raw)

        if envelope.type not in [
            MessageType.EOF_MESSAGE,
            MessageType.CLEAN_UP_MESSAGE,
            MessageType.DATA_BATCH,
        ]:
            self._log.warning("Unknown message type: %s. Skipping.", envelope.type)
            return True

        if envelope.type != MessageType.DATA_BATCH:
            self._handle_eof_or_cleanup(envelope, table)

        db: DataBatch = envelope.data_batch
        cid = db.client_id

        if self._is_blacklisted(cid):
            self._log.info(
                "Discarding batch from blacklisted client: table=%s bn=%s cid=%s",
                db.payload.name,
                db.payload.batch_number,
                cid,
            )
            return True
        bn = db.payload.batch_number

        self._log.debug(
            "IN: light table id=%i batch_number=%s shard=%s shards_info=%s queries=%s cid=%s",
            table,
            bn,
            self._shard,
            db.shards_info,
            db.query_ids,
            cid,
        )

        if table == TableName.MENU_ITEMS:
            return self._process_light_table_buffered(
                db, cid, bn, table, "item_id", self._cache_menu
            )
        else:
            return self._process_light_table_buffered(
                db, cid, bn, TableName.STORES, "store_id", self._cache_stores
            )

    def _on_raw_menu(self, raw: bytes):
        """
        Message handler for menu_items table queue.

        Delegates to _on_raw_light_table with TableName.MENU_ITEMS.
        This handler is called by the message middleware when a message arrives
        on the menu_items queue. The message is processed to build the menu cache
        used for joining with transaction_items in Q2 queries.

        Returns True to ACK immediately.
        """
        self._on_raw_light_table(raw, TableName.MENU_ITEMS)

    def _on_raw_stores(self, raw: bytes):
        """
        Message handler for stores table queue.

        Delegates to _on_raw_light_table with TableName.STORES.
        This handler is called by the message middleware when a message arrives
        on the stores queue. The message is processed to build the stores cache
        used for joining with transactions in Q3 queries.

        Returns True to ACK immediately.
        """
        self._on_raw_light_table(raw, TableName.STORES)

    def _on_raw_heavy_table(self, raw: bytes, table: TableName):
        """
        Process messages for heavy tables (transactions, transaction_items, or users).

        Parses the envelope and handles three message types:
        - EOF_MESSAGE: Delegates to _handle_eof_or_cleanup
        - CLEAN_UP_MESSAGE: Delegates to _handle_eof_or_cleanup
        - DATA_BATCH: Processes the batch based on table type

        For data batches: checks if the client is blacklisted and discards if so.
        Then checks if the phase is ready (prerequisite tables have EOF). If not ready,
        saves the batch to disk for later processing when prerequisites are met.

        If phase is ready, routes to the appropriate processor:
        - TRANSACTION_ITEMS: _process_ti_batch (joins with menu_items for Q2)
        - TRANSACTIONS: _process_tx_batch (joins with stores for Q3)
        - USERS: _process_users_batch (passthrough, no join needed)

        Returns True to ACK immediately after processing, saving, or discarding.
        """
        envelope = Envelope()
        envelope.ParseFromString(raw)

        if envelope.type not in [
            MessageType.EOF_MESSAGE,
            MessageType.CLEAN_UP_MESSAGE,
            MessageType.DATA_BATCH,
        ]:
            self._log.warning("Unknown message type: %s. Skipping.", envelope.type)
            return True

        if envelope.type != MessageType.DATA_BATCH:
            self._handle_eof_or_cleanup(envelope, TableName.TRANSACTION_ITEMS)

        db: DataBatch = envelope.data_batch
        cid = db.client_id

        if self._is_blacklisted(cid):
            self._log.info(
                "Discarding batch from blacklisted client: table=%s bn=%s cid=%s",
                db.payload.name,
                db.payload.batch_number,
                cid,
            )
            return True

        bn = db.payload.batch_number

        self._log.debug(
            "IN: %s batch_number=%s shard=%s shards_info=%s queries=%s cid=%s",
            NAME_TO_STR.get(table),
            bn,
            self._shard,
            db.shards_info,
            db.query_ids,
            cid,
        )

        if not self._phase_ready(table, cid):
            self._log.info(
                "phase not ready for table %s, saving batch to disk (cid=%s batch=%d)",
                NAME_TO_STR.get(table),
                cid,
                bn,
            )
            try:
                self._save_pending_batch(table, cid, bn, raw)
            except Exception as e:
                self._log.error("Failed to save pending batch: %s", e)
                raise
            return True

        if table == TableName.TRANSACTION_ITEMS:
            return self._process_ti_batch(raw, cid, bn)
        elif table == TableName.TRANSACTIONS:
            return self._process_tx_batch(raw, cid, bn)
        else:
            return self._process_users_batch(raw, cid, bn)

    def _on_raw_ti(self, raw: bytes):
        """
        Message handler for transaction_items table queue.

        Delegates to _on_raw_heavy_table with TableName.TRANSACTION_ITEMS.
        This handler is called by the message middleware when a message arrives
        on the transaction_items queue. The message is processed to join with
        menu_items data for Q2 queries, or passed through if Q2 is not present.

        Returns True to ACK immediately.
        """
        self._on_raw_heavy_table(raw, TableName.TRANSACTION_ITEMS)

    def _process_ti_batch(self, raw: bytes, cid: str, bn: int) -> bool:
        """
        Process a transaction_items batch with buffered writes.

        Handles batches either received fresh from the queue or loaded from disk
        after a crash. The processing logic:
        1. If Q2 is not in the query_ids, passes the batch through unchanged
        2. Otherwise, retrieves the menu_items cache for the client
        3. Joins transaction_items with menu_items using item_id
        4. Produces TRANSACTION_ITEMS_MENU_ITEMS with columns:
           transaction_id, name (from menu), quantity, subtotal, created_at
        5. Uses buffered writes to persist progress incrementally

        Returns True if processing succeeded, False if menu cache is unavailable
        (which will cause the message to be requeued).
        """
        envelope = Envelope()
        envelope.ParseFromString(raw)
        db: DataBatch = envelope.data_batch

        if Query.Q2 not in db.query_ids:
            self._log.debug("TI sin Q2 → passthrough")
            self._safe_send(raw)
            self._delete_pending_batch(TableName.TRANSACTION_ITEMS, cid, bn)
            return True

        menu_idx: Optional[dict[str, dict[str, str]]] = self._cache_menu.get((cid))
        if not menu_idx:
            self._log.warning("Menu cache no disponible; cannot process batch=%d", bn)
            return False

        return self._process_batch_buffered(
            db,
            cid,
            bn,
            TableName.TRANSACTION_ITEMS,
            TableName.TRANSACTION_ITEMS_MENU_ITEMS,
            ["transaction_id", "name", "quantity", "subtotal", "created_at"],
            menu_idx,
            "item_id",
            "name",
        )

    def _on_raw_tx(self, raw: bytes):
        """
        Message handler for transactions table queue.

        Delegates to _on_raw_heavy_table with TableName.TRANSACTIONS.
        This handler is called by the message middleware when a message arrives
        on the transactions queue. The message is processed to join with stores
        data for Q3 queries, or passed through if only Q1 is present.

        Returns True to ACK immediately.
        """
        self._on_raw_heavy_table(raw, TableName.TRANSACTIONS)

    def _process_tx_batch(self, raw: bytes, cid: str, bn: int) -> bool:
        """
        Process a transactions batch with buffered writes.

        Handles batches either received fresh from the queue or loaded from disk
        after a crash. The processing logic:
        1. If Q1 is in the query_ids, passes the batch through unchanged
        2. Otherwise, retrieves the stores cache for the client
        3. Joins transactions with stores using store_id
        4. Produces TRANSACTION_STORES with columns:
           transaction_id, store_name (from stores), final_amount, created_at, user_id
        5. Uses buffered writes to persist progress incrementally

        Returns True if processing succeeded, False if stores cache is unavailable
        (which will cause the message to be requeued).
        """
        envelope = Envelope()
        envelope.ParseFromString(raw)
        db: DataBatch = envelope.data_batch

        if Query.Q1 in db.query_ids:
            self._log.debug("TX Q1 passthrough")
            self._safe_send(raw)
            self._delete_pending_batch(TableName.TRANSACTIONS, cid, bn)
            return True

        stores_idx: Optional[Dict[str, Row]] = self._cache_stores.get((cid))
        if not stores_idx:
            self._log.warning("Stores cache no disponible; cannot process batch=%d", bn)
            return False

        return self._process_batch_buffered(
            db,
            cid,
            bn,
            TableName.TRANSACTIONS,
            TableName.TRANSACTION_STORES,
            ["transaction_id", "store_name", "final_amount", "created_at", "user_id"],
            stores_idx,
            "store_id",
            "store_name",
        )

    def _process_light_table_buffered(
        self,
        db: DataBatch,
        cid: str,
        bn: int,
        table: TableName,
        key_attr: str,
        cache_dict: Dict[str, dict],
    ) -> bool:
        """
        Process a light table batch with buffered writes.
        Processes rows in chunks, persisting cache progress after each chunk.
        On redelivery, resumes from where it left off.
        """
        tracking_key = (table, cid, bn)
        written_rows = self._written_rows[tracking_key]
        total_rows = len(db.payload.rows)

        self._log.info(
            "Processing light table with buffering: table=%s client=%s bn=%s total_rows=%d already_written=%d buffer_size=%d",
            table,
            cid,
            bn,
            total_rows,
            len(written_rows),
            self._write_buffer_size,
        )

        if cid not in cache_dict:
            cache_dict[cid] = {}

        rows_processed = 0
        rows_to_process = list(enumerate(iterate_rows_as_dicts(db.payload)))

        for row_idx, row_dict in rows_to_process:
            if row_idx in written_rows:
                continue

            key_val = norm(row_dict[key_attr])
            if key_val:
                cache_dict[cid][key_val] = row_dict

            written_rows.add(row_idx)
            rows_processed += 1

            if rows_processed >= self._write_buffer_size:
                try:
                    self._persist_light_table(table, cid, cache_dict[cid])
                    self._persist_written_rows(table, cid, bn, written_rows)
                except Exception as e:
                    self._log.error("Failed to persist light table progress: %s", e)
                    return False
                rows_processed = 0

        if rows_processed > 0 or len(written_rows) == total_rows:
            try:
                self._persist_light_table(table, cid, cache_dict[cid])
            except Exception as e:
                self._log.error("Failed to persist final light table state: %s", e)
                return False

        self._log.info(
            "Light table batch fully processed: table=%s client=%s bn=%s total_written=%d cache_size=%d",
            table,
            cid,
            bn,
            len(written_rows),
            len(cache_dict[cid]),
        )

        del self._written_rows[tracking_key]
        self._delete_written_rows_tracking(table, cid, bn)

        return True

    def _process_batch_buffered(
        self,
        db: DataBatch,
        cid: str,
        bn: int,
        source_table: TableName,
        result_table: TableName,
        out_cols: List[str],
        join_index: Dict[str, dict],
        join_key: str,
        join_col: str,
    ) -> bool:
        """
        Process a batch with buffered writes.
        Processes rows in chunks, persisting progress after each chunk.
        On redelivery, resumes from where it left off.
        """
        tracking_key = (source_table, cid, bn)
        written_rows = self._written_rows[tracking_key]
        total_rows = len(db.payload.rows)

        self._log.debug(
            "Processing batch with buffering: table=%s client=%s bn=%s total_rows=%d already_written=%d buffer_size=%d",
            source_table,
            cid,
            bn,
            total_rows,
            len(written_rows),
            self._write_buffer_size,
        )

        out_schema = TableSchema(columns=out_cols)

        current_buffer: List[Row] = []
        rows_to_process = list(enumerate(iterate_rows_as_dicts(db.payload)))

        for row_idx, r in rows_to_process:
            if row_idx in written_rows:
                continue

            key_val = norm(r[join_key])
            joined_item = join_index.get(key_val)
            if not joined_item:
                self._log.warning("No join match for %s=%s", join_key, key_val)
                written_rows.add(row_idx)
                continue

            joined_values = []
            for col in out_cols:
                if col == join_col:
                    joined_values.append(joined_item[col])
                else:
                    joined_values.append(r.get(col, ""))
            current_buffer.append(Row(values=joined_values))
            written_rows.add(row_idx)

            if len(current_buffer) >= self._write_buffer_size:
                try:
                    self._persist_written_rows(source_table, cid, bn, written_rows)
                except Exception as e:
                    self._log.error("Failed to persist written rows progress: %s", e)
                    return False

        if current_buffer:
            self._log.debug(
                "Flushing complete batch: rows=%d matched out of %d total",
                len(current_buffer),
                total_rows,
            )
            self._flush_buffer(current_buffer, db, result_table, out_schema)
            current_buffer.clear()
        else:
            self._log.debug(
                "No rows in buffer to flush (all rows skipped or no matches)"
            )
            self._flush_buffer([], db, result_table, out_schema)

        self._log.debug(
            "Batch fully processed: table=%s client=%s bn=%s total_written=%d",
            source_table,
            cid,
            bn,
            len(written_rows),
        )

        del self._written_rows[tracking_key]
        self._delete_written_rows_tracking(source_table, cid, bn)
        self._delete_pending_batch(source_table, cid, bn)

        return True

    def _flush_buffer(
        self,
        buffer: List[Row],
        original_batch: DataBatch,
        result_table: TableName,
        schema: TableSchema,
    ) -> None:
        """Flush a buffer of processed rows to the output.

        Even if the buffer is empty, we still send the databatch to maintain
        batch sequence and ensure the results router receives all batches.
        """
        joined_table = TableData(
            name=result_table,
            schema=schema,
            rows=buffer,
            batch_number=original_batch.payload.batch_number,
            status=original_batch.payload.status,
        )

        output_batch = DataBatch()
        output_batch.CopyFrom(original_batch)
        output_batch.payload.CopyFrom(joined_table)

        raw = Envelope(
            type=MessageType.DATA_BATCH, data_batch=output_batch
        ).SerializeToString()
        self._safe_send(raw)

        self._log.debug("Flushed buffer: rows=%d table=%s", len(buffer), result_table)

    def _on_raw_users(self, raw: bytes):
        """
        Message handler for users table queue.

        Delegates to _on_raw_heavy_table with TableName.USERS.
        This handler is called by the message middleware when a message arrives
        on the users queue. Users data is passed through without any joins,
        as it's used directly in Q4 queries.

        Returns True to ACK immediately.
        """
        self._on_raw_heavy_table(raw, TableName.USERS)

    def _process_users_batch(self, raw: bytes, cid: str, bn: int) -> bool:
        """
        Process a users batch (either fresh or from disk).

        Users data requires no joins, so this method simply:
        1. Sends the batch through to the output unchanged
        2. Deletes any pending batch file from disk (if it existed)

        Users batches are used directly in Q4 queries without modification.
        Returns True after successful processing.
        """
        self._safe_send(raw)
        self._delete_pending_batch(TableName.USERS, cid, bn)
        return True

    def _on_table_eof(
        self,
        table_name: TableName,
        client_id: str,
        trace: str,
        channel=None,
        delivery_tag=None,
        redelivered=False,
    ) -> bool:
        """
        Handle EOF (End of File) message for a table and client.

        Tracks EOF messages from multiple router replicas using trace identifiers
        to detect duplicates. When EOF is received:
        1. Checks if this trace was already processed (deduplication)
        2. Checks if EOF is already complete (all replicas received)
        3. Adds the trace to pending EOFs and persists state
        4. When threshold is reached (router_replicas), marks the table as complete
        5. Triggers processing of any pending batches that were waiting for this EOF

        The EOF state is persisted to disk before ACKing to support crash recovery.
        If persistence fails, the trace is removed and False is returned to requeue.

        Returns True to ACK immediately if successful, False to requeue on error.
        """
        key = (table_name, client_id)
        tname = NAME_TO_STR.get(table_name, f"#{table_name}")

        log.debug(
            "TABLE_EOF received: table=%s cid=%s trace=%s", tname, client_id, trace
        )

        recvd = self._pending_eofs.setdefault(key, set())

        if trace in recvd:
            log.info(
                "EOF from trace=%s already received for table=%s cid=%s",
                trace,
                tname,
                client_id,
            )
            return True

        already_complete = len(recvd) >= self._router_replicas
        if already_complete:
            log.info(
                "EOF already complete for table=%s cid=%s (received from %d replicas), ignoring duplicate trace=%s",
                tname,
                client_id,
                len(recvd),
                trace,
            )
            return True

        recvd.add(trace)

        try:
            self._persist_eof(table_name, client_id, recvd)
        except Exception as e:
            self._log.error("Failed to persist EOF: %s", e)
            recvd.discard(trace)
            return False

        log.info(
            "EOF recv table=%s cid=%s trace=%s progress=%d/%d",
            tname,
            client_id,
            trace,
            len(recvd),
            self._router_replicas,
        )

        if len(recvd) >= self._router_replicas:
            if key not in self._eof:
                self._eof.add(key)
                self._log.info(
                    "EOF marcado table_id=%s cid=%s; eof_set=%s",
                    table_name,
                    client_id,
                    sorted(self._eof),
                )

                self._process_pending_batches_for_phase(client_id)

        return True

    def _process_pending_batches_for_phase(self, client_id: str) -> None:
        """Process pending batches that may now be ready after EOF received."""
        if self._phase_ready(TableName.TRANSACTION_ITEMS, client_id):
            batches = self._load_pending_batches(TableName.TRANSACTION_ITEMS, client_id)
            if batches:
                self._log.info(
                    "Processing %d pending TI batches for client=%s",
                    len(batches),
                    client_id,
                )
                for bn, raw_data in batches:
                    try:
                        self._process_ti_batch(raw_data, client_id, bn)
                    except Exception as e:
                        self._log.error(
                            "Failed to process pending TI batch=%d: %s", bn, e
                        )

        if self._phase_ready(TableName.TRANSACTIONS, client_id):
            batches = self._load_pending_batches(TableName.TRANSACTIONS, client_id)
            if batches:
                self._log.info(
                    "Processing %d pending TX batches for client=%s",
                    len(batches),
                    client_id,
                )
                for bn, raw_data in batches:
                    try:
                        self._process_tx_batch(raw_data, client_id, bn)
                    except Exception as e:
                        self._log.error(
                            "Failed to process pending TX batch=%d: %s", bn, e
                        )

    def _safe_send(self, raw: bytes):
        """
        Send a message with automatic retry on failure.
        If the first send fails, attempts to recreate the output middleware
        using the factory function or reopen() method, then retries.
        Raises an exception if the retry also fails.
        """
        with self._out_lock:
            try:
                self._out.send(raw)
                return
            except Exception as e:
                self._log.warning("send failed once: %s; recreating publisher", e)

                try:
                    if getattr(self._out, "is_closed", None):
                        self._log.debug("out channel closed? %s", self._out.is_closed())

                    if self._out_factory is not None:
                        self._out = self._out_factory()
                    else:
                        if hasattr(self._out, "reopen"):
                            self._out.reopen()
                        else:
                            raise RuntimeError(
                                "No out_factory / reopen() to recreate publisher"
                            ) from e

                    self._out.send(raw)
                    return
                except Exception as e2:
                    self._log.error("SEND failed after recreate: %s", e2)
                    raise
