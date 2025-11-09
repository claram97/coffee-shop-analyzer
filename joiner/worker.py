from __future__ import annotations

import base64
import json
import logging
import os
import threading
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


def norm(v) -> str:
    return "" if v is None else str(v)


def index_by_attr(table_data: TableData, attr: str) -> Dict[str, dict[str, str]]:
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

        self._stop_event = stop_event
        self._is_shutting_down = False

        # Persistence
        self._persistence_dir = persistence_dir
        self._persistence_lock = threading.Lock()
        os.makedirs(self._persistence_dir, exist_ok=True)

        # In-memory batch deduplication: track received batches per (table, client_id, query_ids_tuple)
        # This mitigates duplicate batch propagation from joiner router (no persistence needed)
        from collections import defaultdict
        self._received_batches: Dict[tuple[TableName, str, tuple], set[int]] = defaultdict(set)

        # Buffered disk writes configuration
        self._write_buffer_size = write_buffer_size
        # Track which rows have been written per batch: (table, client, batch_number) -> set of row indices
        self._written_rows: Dict[tuple[TableName, str, int], set[int]] = defaultdict(set)

        # Restore state from disk
        self._restore_state()

    def _get_pending_batch_path(self, table_name: TableName, client_id: str, batch_number: int) -> str:
        """Get file path for a pending batch."""
        return os.path.join(
            self._persistence_dir, f"pending_{table_name}_{client_id}_{batch_number}.bin"
        )

    def _get_light_table_path(self, table_name: TableName, client_id: str) -> str:
        """Get file path for light table cache."""
        return os.path.join(
            self._persistence_dir, f"light_{table_name}_{client_id}.json"
        )

    def _get_eof_path(self, table_name: TableName, client_id: str) -> str:
        """Get file path for EOF metadata."""
        return os.path.join(self._persistence_dir, f"eof_{table_name}_{client_id}.json")

    def _get_written_rows_path(self, table_name: TableName, client_id: str, batch_number: int) -> str:
        """Get file path for written rows tracking."""
        return os.path.join(
            self._persistence_dir, f"written_rows_{table_name}_{client_id}_{batch_number}.json"
        )

    def _save_pending_batch(self, table_name: TableName, client_id: str, batch_number: int, raw_data: bytes) -> None:
        """Save a pending batch to disk."""
        path = self._get_pending_batch_path(table_name, client_id, batch_number)
        temp_path = path + ".tmp"
        try:
            with open(temp_path, "wb") as f:
                f.write(raw_data)
            os.rename(temp_path, path)
            self._log.info("Saved pending batch: table=%s client=%s batch=%d size=%d bytes",
                          table_name, client_id, batch_number, len(raw_data))
        except Exception as e:
            self._log.error("Failed to save pending batch: %s", e)
            if os.path.exists(temp_path):
                os.remove(temp_path)
            raise

    def _delete_pending_batch(self, table_name: TableName, client_id: str, batch_number: int) -> None:
        """Delete a pending batch from disk."""
        path = self._get_pending_batch_path(table_name, client_id, batch_number)
        try:
            if os.path.exists(path):
                os.remove(path)
                self._log.debug("Deleted pending batch: table=%s client=%s batch=%d",
                              table_name, client_id, batch_number)
        except Exception as e:
            self._log.warning("Failed to delete pending batch: %s", e)

    def _load_pending_batches(self, table_name: TableName, client_id: str) -> List[Tuple[int, bytes]]:
        """Load all pending batches for a table/client from disk."""
        pattern = f"pending_{table_name}_{client_id}_"
        batches = []
        try:
            for filename in os.listdir(self._persistence_dir):
                if filename.startswith(pattern) and filename.endswith(".bin"):
                    batch_num_str = filename[len(pattern):-4]
                    batch_number = int(batch_num_str)
                    path = os.path.join(self._persistence_dir, filename)
                    with open(path, "rb") as f:
                        raw_data = f.read()
                    batches.append((batch_number, raw_data))
            return sorted(batches)  # Process in order
        except Exception as e:
            self._log.error("Failed to load pending batches: %s", e)
            return []

    def _persist_light_table(self, table_name: TableName, client_id: str, data: dict) -> None:
        """Persist light table cache to disk."""
        path = self._get_light_table_path(table_name, client_id)
        temp_path = path + ".tmp"
        try:
            with open(temp_path, "w") as f:
                json.dump(data, f)
            os.rename(temp_path, path)
            self._log.debug("Persisted light table: table=%s client=%s", table_name, client_id)
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
                json.dump({
                    "traces": list(traces),
                    "complete": len(traces) >= self._router_replicas,
                }, f)
            os.rename(temp_path, path)
            self._log.debug("Persisted EOF: table=%s client=%s traces=%d",
                          table_name, client_id, len(traces))
        except Exception as e:
            self._log.error("Failed to persist EOF: %s", e)
            if os.path.exists(temp_path):
                os.remove(temp_path)
            raise

    def _persist_written_rows(self, table_name: TableName, client_id: str, batch_number: int, written_rows: set[int]) -> None:
        """Persist written rows tracking to disk."""
        path = self._get_written_rows_path(table_name, client_id, batch_number)
        temp_path = path + ".tmp"
        try:
            with open(temp_path, "w") as f:
                json.dump({"written_rows": list(written_rows)}, f)
            os.rename(temp_path, path)
            self._log.debug("Persisted written rows: table=%s client=%s bn=%s count=%d",
                          table_name, client_id, batch_number, len(written_rows))
        except Exception as e:
            self._log.error("Failed to persist written rows: %s", e)
            if os.path.exists(temp_path):
                os.remove(temp_path)
            raise

    def _delete_written_rows_tracking(self, table_name: TableName, client_id: str, batch_number: int) -> None:
        """Delete written rows tracking file after batch is fully processed."""
        path = self._get_written_rows_path(table_name, client_id, batch_number)
        try:
            if os.path.exists(path):
                os.remove(path)
                self._log.debug("Deleted written rows tracking: table=%s client=%s bn=%s",
                              table_name, client_id, batch_number)
        except Exception as e:
            self._log.warning("Failed to delete written rows tracking %s: %s", path, e)

    def _restore_state(self) -> None:
        """Restore state from disk on startup."""
        if not os.path.exists(self._persistence_dir):
            return

        self._log.info("Restoring state from disk: %s", self._persistence_dir)

        # Restore light tables
        for filename in os.listdir(self._persistence_dir):
            if filename.startswith("light_") and filename.endswith(".json"):
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

            elif filename.startswith("eof_") and filename.endswith(".json"):
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
                        self._log.info("Restored partial EOF for table=%s client=%s traces=%d",
                                      table_name, client_id, len(traces))

                    if eof_data.get("complete", False):
                        self._eof.add(key)
                        self._log.info("Restored complete EOF for table=%s client=%s",
                                      table_name, client_id)
                except Exception as e:
                    self._log.warning("Failed to restore EOF from %s: %s", filename, e)

            elif filename.startswith("written_rows_") and filename.endswith(".json"):
                try:
                    # Parse: written_rows_{table}_{client}_{batch}.json
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
                    self._log.info("Restored written rows for table=%s client=%s bn=%s count=%d",
                                  table_name, client_id, batch_number, len(written_rows))
                except Exception as e:
                    self._log.warning("Failed to restore written rows from %s: %s", filename, e)

        self._log.info("State restoration complete")

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
            self._log.warning(
                "DUPLICATE batch detected and discarded in joiner worker: table=%s bn=%s client=%s queries=%s",
                table, batch_number, client_id, data_batch.query_ids
            )
            return True
        
        # Mark batch as received
        self._received_batches[dedup_key].add(batch_number)
        self._log.debug(
            "Batch marked as received: table=%s bn=%s client=%s queries=%s",
            table, batch_number, client_id, data_batch.query_ids
        )
        return False

    def _cleanup_persisted_client(self, client_id: str) -> None:
        """Clean up all persisted data for a client."""
        for filename in os.listdir(self._persistence_dir):
            if client_id in filename:
                try:
                    os.remove(os.path.join(self._persistence_dir, filename))
                    self._log.debug("Cleaned up: %s", filename)
                except Exception as e:
                    self._log.warning("Failed to cleanup %s: %s", filename, e)

    def _log_db(self, where: str, db: DataBatch):
        try:
            t = db.payload.name
            q = db.query_ids
            n = len(db.payload.rows)
            self._log.debug("%s: table=%s queries=%s rows=%s", where, t, q, n)
        except Exception as e:
            self._log.debug("log_db failed: %s", e)

    def run(self):
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
        for t in self._threads.values():
            t.join()

        self._log.info("Closing outbound connections and data store...")
        try:
            self._out.close()
        except Exception as e:
            self._log.warning("Error closing outbound middleware: %s", e)

        self._log.info("JoinerWorker shutdown complete.")

    def _start_queue(self, table_name: TableName, cb: Callable[[bytes], None]):
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
        mw = self._in.get(table_name)
        if not mw:
            return
        try:
            threading.Thread(target=mw.stop_consuming, daemon=True).start()
            if self._log:
                self._log.info("Detenida cola table=%s", table_name)
        except Exception as e:
            if self._log:
                self._log.warning("Error deteniendo cola %s: %s", table_name, e)

    def _phase_ready(self, table_name: TableName, client_id: str) -> bool:
        need = []
        if table_name == TableName.TRANSACTION_ITEMS:
            need = [TableName.MENU_ITEMS, TableName.STORES]
        elif table_name == TableName.TRANSACTIONS:
            need = [TableName.MENU_ITEMS, TableName.STORES]
        elif table_name == TableName.USERS:
            need = []  # USERS doesn't need to wait for any table
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

    def _on_raw_menu(
        self, raw: bytes, channel=None, delivery_tag=None, redelivered=False
    ):
        """Process menu_items messages. Returns True to ACK immediately."""
        envelope = Envelope()
        envelope.ParseFromString(raw)
        if envelope.type == MessageType.EOF_MESSAGE:
            eof: EOFMessage = envelope.eof
            return self._on_table_eof(
                eof.table,
                eof.client_id,
                eof.trace,
                channel,
                delivery_tag,
                redelivered,
            )
        if envelope.type == MessageType.DATA_BATCH:
            db: DataBatch = envelope.data_batch
            
            # Check for duplicate batch and discard if already processed
            if self._is_duplicate_batch(db):
                return True  # ACK duplicate batch
        else:
            self._log.warning("Unknown message type: %s. Skipping.", envelope.type)
            return True
        cid = db.client_id
        bn = db.payload.batch_number

        self._log.debug(
            "IN: menu_items batch_number=%s shard=%s shards_info=%s queries=%s cid=%s",
            bn,
            self._shard,
            db.shards_info,
            db.query_ids,
            cid,
        )

        new_items = index_by_attr(db.payload, "item_id")
        # Merge new batch into existing cache (light tables can span multiple batches)
        if cid not in self._cache_menu:
            self._cache_menu[cid] = {}
        self._cache_menu[cid].update(new_items)
        self._log.debug("Cache menu_items batch_items=%d total_items=%d", 
                       len(new_items), len(self._cache_menu[cid]))
        
        # Persist to disk
        try:
            self._persist_light_table(TableName.MENU_ITEMS, cid, idx)
        except Exception as e:
            self._log.error("Failed to persist menu cache: %s", e)

        return True  # ACK immediately

    def _on_raw_stores(
        self, raw: bytes, channel=None, delivery_tag=None, redelivered=False
    ):
        """Process stores messages. Returns True to ACK immediately."""
        envelope = Envelope()
        envelope.ParseFromString(raw)
        if envelope.type == MessageType.EOF_MESSAGE:
            eof: EOFMessage = envelope.eof
            return self._on_table_eof(
                eof.table,
                eof.client_id,
                eof.trace,
                channel,
                delivery_tag,
                redelivered,
            )
        if envelope.type == MessageType.DATA_BATCH:
            db: DataBatch = envelope.data_batch
            
            # Check for duplicate batch and discard if already processed
            if self._is_duplicate_batch(db):
                return True  # ACK duplicate batch
        else:
            self._log.warning("Unknown message type: %s. Skipping.", envelope.type)
            return True
        cid = db.client_id
        bn = db.payload.batch_number

        self._log.debug(
            "IN: stores batch_number=%s shard=%s shards_info=%s queries=%s cid=%s",
            bn,
            self._shard,
            db.shards_info,
            db.query_ids,
            cid,
        )

        new_stores = index_by_attr(db.payload, "store_id")
        # Merge new batch into existing cache (light tables can span multiple batches)
        if cid not in self._cache_stores:
            self._cache_stores[cid] = {}
        self._cache_stores[cid].update(new_stores)
        self._log.debug("Cache stores batch_stores=%d total_stores=%d", 
                       len(new_stores), len(self._cache_stores[cid]))

        # Persist to disk
        try:
            self._persist_light_table(TableName.STORES, cid, idx)
        except Exception as e:
            self._log.error("Failed to persist stores cache: %s", e)

        return True  # ACK immediately

    def _on_raw_ti(
        self, raw: bytes, channel=None, delivery_tag=None, redelivered=False
    ):
        """Process transaction_items messages. Returns True to ACK immediately."""
        envelope = Envelope()
        envelope.ParseFromString(raw)
        if envelope.type == MessageType.EOF_MESSAGE:
            eof: EOFMessage = envelope.eof
            return self._on_table_eof(
                eof.table,
                eof.client_id,
                eof.trace,
                channel,
                delivery_tag,
                redelivered,
            )
        if envelope.type == MessageType.DATA_BATCH:
            db: DataBatch = envelope.data_batch
            
            # Check for duplicate batch and discard if already processed
            if self._is_duplicate_batch(db):
                return True  # ACK duplicate batch
        else:
            self._log.warning("Unknown message type: %s. Skipping.", envelope.type)
            return True
        cid = db.client_id
        bn = db.payload.batch_number

        self._log.debug(
            "IN: transaction_items batch_number=%s shard=%s shards_info=%s queries=%s cid=%s",
            bn,
            self._shard,
            db.shards_info,
            db.query_ids,
            cid,
        )

        if not self._phase_ready(TableName.TRANSACTION_ITEMS, cid):
            self._log.info("TI fase NO lista, saving batch to disk (cid=%s batch=%d)", cid, bn)
            try:
                self._save_pending_batch(TableName.TRANSACTION_ITEMS, cid, bn, raw)
            except Exception as e:
                self._log.error("Failed to save pending batch: %s", e)
            return True  # ACK immediately - batch saved to disk

        return self._process_ti_batch(raw, cid, bn)

    def _process_ti_batch(self, raw: bytes, cid: str, bn: int) -> bool:
        """Process a transaction_items batch with buffered writes (either fresh or from disk)."""
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

        # Use buffered write for Q2 join
        return self._process_batch_buffered(
            db, cid, bn, TableName.TRANSACTION_ITEMS, TableName.TRANSACTION_ITEMS_MENU_ITEMS,
            ["transaction_id", "name", "quantity", "subtotal", "created_at"],
            menu_idx, "item_id", "name"
        )

    def _on_raw_tx(
        self, raw: bytes, channel=None, delivery_tag=None, redelivered=False
    ):
        """Process transactions messages. Returns True to ACK immediately."""
        envelope = Envelope()
        envelope.ParseFromString(raw)
        if envelope.type == MessageType.EOF_MESSAGE:
            eof: EOFMessage = envelope.eof
            return self._on_table_eof(
                eof.table,
                eof.client_id,
                eof.trace,
                channel,
                delivery_tag,
                redelivered,
            )
        if envelope.type == MessageType.DATA_BATCH:
            db: DataBatch = envelope.data_batch
            
            # Check for duplicate batch and discard if already processed
            if self._is_duplicate_batch(db):
                return True  # ACK duplicate batch
        else:
            self._log.warning("Unknown message type: %s. Skipping.", envelope.type)
            return True
        cid = db.client_id
        bn = db.payload.batch_number

        self._log.debug(
            "IN: transactions batch_number=%s shard=%s shards_info=%s queries=%s cid=%s",
            bn,
            self._shard,
            db.shards_info,
            db.query_ids,
            cid,
        )

        if not self._phase_ready(TableName.TRANSACTIONS, cid):
            self._log.info("TX fase NO lista, saving batch to disk (cid=%s batch=%d)", cid, bn)
            try:
                self._save_pending_batch(TableName.TRANSACTIONS, cid, bn, raw)
            except Exception as e:
                self._log.error("Failed to save pending batch: %s", e)
            return True  # ACK immediately

        return self._process_tx_batch(raw, cid, bn)

    def _process_tx_batch(self, raw: bytes, cid: str, bn: int) -> bool:
        """Process a transactions batch with buffered writes (either fresh or from disk)."""
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

        # Use buffered write for Q3 join
        return self._process_batch_buffered(
            db, cid, bn, TableName.TRANSACTIONS, TableName.TRANSACTION_STORES,
            ["transaction_id", "store_name", "final_amount", "created_at", "user_id"],
            stores_idx, "store_id", "store_name"
        )

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
        join_col: str
    ) -> bool:
        """
        Process a batch with buffered writes.
        Processes rows in chunks, persisting progress after each chunk.
        On redelivery, resumes from where it left off.
        """
        tracking_key = (source_table, cid, bn)
        written_rows = self._written_rows[tracking_key]
        total_rows = len(db.payload.rows)
        
        self._log.info("Processing batch with buffering: table=%s client=%s bn=%s total_rows=%d already_written=%d buffer_size=%d",
                      source_table, cid, bn, total_rows, len(written_rows), self._write_buffer_size)

        out_schema = TableSchema(columns=out_cols)
        
        # Process rows in chunks
        current_buffer: List[Row] = []
        rows_to_process = list(enumerate(iterate_rows_as_dicts(db.payload)))
        
        for row_idx, r in rows_to_process:
            # Skip already written rows (from previous delivery)
            if row_idx in written_rows:
                continue
                
            # Join logic
            key_val = norm(r[join_key])
            joined_item = join_index.get(key_val)
            if not joined_item:
                self._log.warning("No join match for %s=%s", join_key, key_val)
                # Mark as written even if no match (to skip on redelivery)
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
            
            # Flush buffer when it reaches the configured size
            if len(current_buffer) >= self._write_buffer_size:
                self._flush_buffer(current_buffer, db, result_table, out_schema)
                current_buffer.clear()
                # Persist progress
                try:
                    self._persist_written_rows(source_table, cid, bn, written_rows)
                except Exception as e:
                    self._log.error("Failed to persist written rows progress: %s", e)
                    return False  # NACK to retry
        
        # Flush remaining rows
        if current_buffer:
            self._flush_buffer(current_buffer, db, result_table, out_schema)
            current_buffer.clear()
        
        # All rows processed successfully
        self._log.info("Batch fully processed: table=%s client=%s bn=%s total_written=%d",
                      source_table, cid, bn, len(written_rows))
        
        # Cleanup tracking
        del self._written_rows[tracking_key]
        self._delete_written_rows_tracking(source_table, cid, bn)
        self._delete_pending_batch(source_table, cid, bn)
        
        return True  # ACK

    def _flush_buffer(self, buffer: List[Row], original_batch: DataBatch, result_table: TableName, schema: TableSchema) -> None:
        """Flush a buffer of processed rows to the output."""
        if not buffer:
            return
            
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
        
        raw = Envelope(type=MessageType.DATA_BATCH, data_batch=output_batch).SerializeToString()
        self._safe_send(raw)
        
        self._log.debug("Flushed buffer: rows=%d table=%s", len(buffer), result_table)

    def _on_raw_users(
        self, raw: bytes, channel=None, delivery_tag=None, redelivered=False
    ):
        """Process users messages. Returns True to ACK immediately."""
        envelope = Envelope()
        envelope.ParseFromString(raw)
        if envelope.type == MessageType.EOF_MESSAGE:
            eof: EOFMessage = envelope.eof
            return self._on_table_eof(
                eof.table,
                eof.client_id,
                eof.trace,
                channel,
                delivery_tag,
                redelivered,
            )
        if envelope.type == MessageType.DATA_BATCH:
            db: DataBatch = envelope.data_batch
            
            # Check for duplicate batch and discard if already processed
            if self._is_duplicate_batch(db):
                return True  # ACK duplicate batch
        else:
            self._log.warning("Unknown message type: %s. Skipping.", envelope.type)
            return True
        cid = db.client_id
        bn = db.payload.batch_number

        self._log.debug(
            "IN: users batch_number=%s shard=%s shards_info=%s queries=%s cid=%s",
            bn,
            self._shard,
            db.shards_info,
            db.query_ids,
            cid,
        )

        if not self._phase_ready(TableName.USERS, cid):
            self._log.info("USERS fase NO lista, saving batch to disk (cid=%s batch=%d)", cid, bn)
            try:
                self._save_pending_batch(TableName.USERS, cid, bn, raw)
            except Exception as e:
                self._log.error("Failed to save pending batch: %s", e)
            return True  # ACK immediately

        return self._process_users_batch(raw, cid, bn)

    def _process_users_batch(self, raw: bytes, cid: str, bn: int) -> bool:
        """Process a users batch (either fresh or from disk)."""
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
        """Handle EOF message. Returns True to ACK immediately."""
        key = (table_name, client_id)
        tname = NAME_TO_STR.get(table_name, f"#{table_name}")

        if redelivered:
            log.info("TABLE_EOF REDELIVERED: table=%s cid=%s trace=%s", tname, client_id, trace)
        else:
            log.debug("TABLE_EOF received: table=%s cid=%s trace=%s", tname, client_id, trace)

        recvd = self._pending_eofs.setdefault(key, set())
        
        # Track by trace to make it idempotent
        if trace in recvd:
            log.info("EOF from trace=%s already received for table=%s cid=%s",
                    trace, tname, client_id)
            return True
        
        recvd.add(trace)

        # Persist EOF state
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
            self._pending_eofs.pop(key, None)
            self._eof.add(key)
            self._log.info(
                "EOF marcado table_id=%s cid=%s; eof_set=%s",
                table_name,
                client_id,
                sorted(self._eof),
            )

            # Process pending batches that are now ready
            self._process_pending_batches_for_phase(client_id)

            # Check if we can cleanup for this client
            self._maybe_cleanup_complete_lifecycle(client_id)

        return True  # ACK immediately

    def _process_pending_batches_for_phase(self, client_id: str) -> None:
        """Process pending batches that may now be ready after EOF received."""
        # Check TRANSACTION_ITEMS (needs MENU_ITEMS and STORES)
        if self._phase_ready(TableName.TRANSACTION_ITEMS, client_id):
            batches = self._load_pending_batches(TableName.TRANSACTION_ITEMS, client_id)
            if batches:
                self._log.info("Processing %d pending TI batches for client=%s", len(batches), client_id)
                for bn, raw_data in batches:
                    try:
                        self._process_ti_batch(raw_data, client_id, bn)
                    except Exception as e:
                        self._log.error("Failed to process pending TI batch=%d: %s", bn, e)

        # Check TRANSACTIONS (needs MENU_ITEMS and STORES)
        if self._phase_ready(TableName.TRANSACTIONS, client_id):
            batches = self._load_pending_batches(TableName.TRANSACTIONS, client_id)
            if batches:
                self._log.info("Processing %d pending TX batches for client=%s", len(batches), client_id)
                for bn, raw_data in batches:
                    try:
                        self._process_tx_batch(raw_data, client_id, bn)
                    except Exception as e:
                        self._log.error("Failed to process pending TX batch=%d: %s", bn, e)

        # Check USERS (needs TRANSACTIONS)
        if self._phase_ready(TableName.USERS, client_id):
            batches = self._load_pending_batches(TableName.USERS, client_id)
            if batches:
                self._log.info("Processing %d pending USERS batches for client=%s", len(batches), client_id)
                for bn, raw_data in batches:
                    try:
                        self._process_users_batch(raw_data, client_id, bn)
                    except Exception as e:
                        self._log.error("Failed to process pending USERS batch=%d: %s", bn, e)

    def _maybe_cleanup_complete_lifecycle(self, client_id: str) -> None:
        """Check if all EOFs received and no pending batches, then cleanup."""
        required_eofs = [
            (TableName.MENU_ITEMS, client_id),
            (TableName.STORES, client_id),
            (TableName.TRANSACTION_ITEMS, client_id),
            (TableName.TRANSACTIONS, client_id),
            (TableName.USERS, client_id),
        ]

        if not all(eof_key in self._eof for eof_key in required_eofs):
            return

        # Check if there are pending batches
        has_pending = False
        for table_name in [TableName.TRANSACTION_ITEMS, TableName.TRANSACTIONS, TableName.USERS]:
            batches = self._load_pending_batches(table_name, client_id)
            if batches:
                has_pending = True
                self._log.info("Client %s has %d pending batches for table=%s",
                             client_id, len(batches), table_name)

        if has_pending:
            self._log.info("Client %s has pending batches, not cleaning up yet", client_id)
            return

        self._log.info("Complete lifecycle for client=%s, cleaning up", client_id)

        # Cleanup caches
        self._cache_menu.pop(client_id, None)
        self._cache_stores.pop(client_id, None)

        # Cleanup persisted data
        self._cleanup_persisted_client(client_id)

    def _requeue(self, table_name: TableName, raw: bytes):
        """Acknowledge all light table messages for this key."""
        with self._ack_lock:
            ack_list = self._unacked_light_tables.get(key, [])
            if ack_list:
                self._log.info(
                    "ACKing %d light table messages: key=%s", len(ack_list), key
                )
                acked_count = 0
                failed_count = 0
                for channel, delivery_tag in ack_list:
                    try:
                        if not channel:
                            self._log.warning("Channel is None for light table key=%s delivery_tag=%s - skipping ACK", key, delivery_tag)
                            failed_count += 1
                            continue
                        channel.basic_ack(delivery_tag=delivery_tag)
                        acked_count += 1
                    except Exception as e:
                        self._log.warning(
                            "Failed to ACK light table key=%s delivery_tag=%s (will be redelivered): %s",
                            key,
                            delivery_tag,
                            e,
                        )
                        failed_count += 1
                
                if failed_count > 0:
                    self._log.warning("Failed to ACK %d/%d light table messages for key=%s - they will be redelivered", failed_count, len(ack_list), key)
                
                del self._unacked_light_tables[key]

    def _ack_eof(self, key: tuple[TableName, str]) -> None:
        """Acknowledge all EOF messages for this key."""
        with self._ack_lock:
            ack_list = self._unacked_eofs.get(key, [])
            if ack_list:
                self._log.info("ACKing %d EOF messages: key=%s", len(ack_list), key)
                acked_count = 0
                failed_count = 0
                for channel, delivery_tag in ack_list:
                    try:
                        if not channel:
                            self._log.warning("Channel is None for EOF key=%s delivery_tag=%s - skipping ACK", key, delivery_tag)
                            failed_count += 1
                            continue
                        channel.basic_ack(delivery_tag=delivery_tag)
                        acked_count += 1
                    except Exception as e:
                        self._log.warning(
                            "Failed to ACK EOF key=%s delivery_tag=%s (will be redelivered): %s",
                            key,
                            delivery_tag,
                            e,
                        )
                        failed_count += 1
                
                if failed_count > 0:
                    self._log.warning("Failed to ACK %d/%d EOF messages for key=%s - they will be redelivered", failed_count, len(ack_list), key)
                
                del self._unacked_eofs[key]

    def _safe_send(self, raw: bytes):
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
