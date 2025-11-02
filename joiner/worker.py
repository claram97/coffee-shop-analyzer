from __future__ import annotations

import logging
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

        self._pending_eofs: Dict[tuple[TableName, str], Set[int]] = {}
        self._part_counter: Dict[tuple[TableName, str], int] = {}

        self._stop_event = stop_event
        self._is_shutting_down = False

        # Store unacked messages: key=(table, client_id) -> list of (channel, delivery_tag)
        self._unacked_light_tables: Dict[tuple[TableName, str], list] = {}
        self._unacked_eofs: Dict[tuple[TableName, str], list] = {}
        self._ack_lock = threading.Lock()

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
            need = [TableName.TRANSACTIONS]
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
        """Process menu_items messages. Returns True to ACK immediately, False to delay."""
        envelope = Envelope()
        envelope.ParseFromString(raw)
        if envelope.type == MessageType.EOF_MESSAGE:
            eof: EOFMessage = envelope.eof
            return self._on_table_eof(
                eof.table,
                eof.client_id,
                channel,
                delivery_tag,
                redelivered,
            )
        if envelope.type == MessageType.DATA_BATCH:
            db: DataBatch = envelope.data_batch
        else:
            self._log.warning("Unknown message type: %s. Skipping.", envelope.type)
            return True
        cid = db.client_id
        bn = db.payload.batch_number

        if redelivered:
            self._log.info("REDELIVERED: menu_items batch_number=%s cid=%s", bn, cid)

        self._log.debug(
            "IN: menu_items batch_number=%s shard=%s shards_info=%s queries=%s cid=%s",
            bn,
            self._shard,
            db.shards_info,
            db.query_ids,
            cid,
        )

        # Store channel and delivery_tag for later ACK
        key = (TableName.MENU_ITEMS, cid)
        with self._ack_lock:
            if channel is not None and delivery_tag is not None:
                if key not in self._unacked_light_tables:
                    self._unacked_light_tables[key] = []
                self._unacked_light_tables[key].append((channel, delivery_tag))

        idx = index_by_attr(db.payload, "item_id")
        self._cache_menu[(cid)] = idx
        self._log.debug("Cache menu_items idx_size=%d", len(idx))

        # Delay ACK until complete lifecycle
        return False

    def _on_raw_stores(
        self, raw: bytes, channel=None, delivery_tag=None, redelivered=False
    ):
        """Process stores messages. Returns True to ACK immediately, False to delay."""
        envelope = Envelope()
        envelope.ParseFromString(raw)
        if envelope.type == MessageType.EOF_MESSAGE:
            eof: EOFMessage = envelope.eof
            return self._on_table_eof(
                eof.table,
                eof.client_id,
                channel,
                delivery_tag,
                redelivered,
            )
        if envelope.type == MessageType.DATA_BATCH:
            db: DataBatch = envelope.data_batch
        else:
            self._log.warning("Unknown message type: %s. Skipping.", envelope.type)
            return True
        cid = db.client_id
        bn = db.payload.batch_number

        if redelivered:
            self._log.info("REDELIVERED: stores batch_number=%s cid=%s", bn, cid)

        self._log.debug(
            "IN: stores batch_number=%s shard=%s shards_info=%s queries=%s cid=%s",
            bn,
            self._shard,
            db.shards_info,
            db.query_ids,
            cid,
        )

        # Store channel and delivery_tag for later ACK
        key = (TableName.STORES, cid)
        with self._ack_lock:
            if channel is not None and delivery_tag is not None:
                if key not in self._unacked_light_tables:
                    self._unacked_light_tables[key] = []
                self._unacked_light_tables[key].append((channel, delivery_tag))

        idx = index_by_attr(db.payload, "store_id")
        self._cache_stores[(cid)] = idx
        self._log.debug("Cache stores idx_size=%d", len(idx))

        # Delay ACK until complete lifecycle
        return False

    def _on_raw_ti(
        self, raw: bytes, channel=None, delivery_tag=None, redelivered=False
    ):
        """Process transaction_items messages. Returns True to ACK immediately, False to delay."""
        envelope = Envelope()
        envelope.ParseFromString(raw)
        if envelope.type == MessageType.EOF_MESSAGE:
            eof: EOFMessage = envelope.eof
            return self._on_table_eof(
                eof.table,
                eof.client_id,
                channel,
                delivery_tag,
                redelivered,
            )
        if envelope.type == MessageType.DATA_BATCH:
            db: DataBatch = envelope.data_batch
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
            self._log.debug("TI fase NO lista → requeue (cid=%s)", cid)
            self._requeue(TableName.TRANSACTION_ITEMS, raw)
            return True  # Don't ACK, will be redelivered

        if Query.Q2 not in db.query_ids:
            self._log.debug("TI sin Q2 → passthrough")
            self._safe_send(raw)
            return True

        menu_idx: Optional[dict[str, dict[str, str]]] = self._cache_menu.get((cid))
        if not menu_idx:
            self._log.warning("Menu cache no disponible aún; requeue batch TI")
            self._requeue(TableName.TRANSACTION_ITEMS, raw)
            return True

        out_cols = ["transaction_id", "name", "quantity", "subtotal", "created_at"]
        out_schema = TableSchema(columns=out_cols)
        out_rows: List[Row] = []

        for r in iterate_rows_as_dicts(db.payload):
            item_id = norm(r["item_id"])
            mi = menu_idx.get(item_id)
            if not mi:
                continue
            joined_item_values = []
            for col in out_cols:
                if col == "name":
                    joined_item_values.append(mi[col])
                else:
                    joined_item_values.append(r[col])
            out_rows.append(Row(values=joined_item_values))

        self._log.debug(
            "JOIN Q2: in=%d matched=%d", len(db.payload.rows), len(out_rows)
        )
        joined_table = TableData(
            name=TableName.TRANSACTION_ITEMS_MENU_ITEMS,
            schema=out_schema,
            rows=out_rows,
            batch_number=db.payload.batch_number,
            status=db.payload.status,
        )
        db.payload.CopyFrom(joined_table)
        self._log.debug("Q2 preserving TI batch_status=%d", db.payload.status)
        raw = Envelope(type=MessageType.DATA_BATCH, data_batch=db).SerializeToString()
        self._safe_send(raw)
        return True

    def _on_raw_tx(
        self, raw: bytes, channel=None, delivery_tag=None, redelivered=False
    ):
        """Process transactions messages. Returns True to ACK immediately, False to delay."""
        envelope = Envelope()
        envelope.ParseFromString(raw)
        if envelope.type == MessageType.EOF_MESSAGE:
            eof: EOFMessage = envelope.eof
            return self._on_table_eof(
                eof.table,
                eof.client_id,
                channel,
                delivery_tag,
                redelivered,
            )
        if envelope.type == MessageType.DATA_BATCH:
            db: DataBatch = envelope.data_batch
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
            self._log.debug("TX fase NO lista → requeue (cid=%s)", cid)
            self._requeue(TableName.TRANSACTIONS, raw)
            return True

        if Query.Q1 in db.query_ids:
            self._log.debug("TX Q1 passthrough")
            self._safe_send(raw)
            return True

        stores_idx: Optional[Dict[str, Row]] = self._cache_stores.get((cid))
        if not stores_idx:
            self._log.warning("Stores cache no disponible aún; drop batch TX")
            self._requeue(TableName.TRANSACTIONS, raw)
            return True

        out_cols = [
            "transaction_id",
            "store_name",
            "final_amount",
            "created_at",
            "user_id",
        ]
        out_schema = TableSchema(columns=out_cols)
        out_rows: List[Row] = []

        for r in iterate_rows_as_dicts(db.payload):
            sid = norm(r["store_id"])
            st = stores_idx.get(sid)
            if not st:
                self._log.warning("no store index for store_id %s", sid)
                continue
            joined_item_values = []
            for col in out_cols:
                if col == "store_name":
                    joined_item_values.append(st[col])
                else:
                    joined_item_values.append(r.get(col, ""))
            out_rows.append(Row(values=joined_item_values))

        self._log.debug(
            "JOIN %s: in=%d matched=%d",
            db.query_ids[0],
            len(db.payload.rows),
            len(out_rows),
        )
        joined_table = TableData(
            name=TableName.TRANSACTION_STORES,
            schema=out_schema,
            rows=out_rows,
            batch_number=db.payload.batch_number,
            status=db.payload.status,
        )
        db.payload.CopyFrom(joined_table)
        self._log.debug(
            "%s preserving TX batch_status=%d", db.query_ids[0], db.payload.status
        )
        raw = Envelope(type=MessageType.DATA_BATCH, data_batch=db).SerializeToString()
        self._safe_send(raw)
        return True

    def _on_raw_users(
        self, raw: bytes, channel=None, delivery_tag=None, redelivered=False
    ):
        """Process users messages. Returns True to ACK immediately, False to delay."""
        envelope = Envelope()
        envelope.ParseFromString(raw)
        if envelope.type == MessageType.EOF_MESSAGE:
            eof: EOFMessage = envelope.eof
            return self._on_table_eof(
                eof.table,
                eof.client_id,
                channel,
                delivery_tag,
                redelivered,
            )
        if envelope.type == MessageType.DATA_BATCH:
            db: DataBatch = envelope.data_batch
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
            self._log.warning("U fase NO lista → requeue (cid=%s)", cid)
            self._requeue(TableName.USERS, raw)
            return True

        self._safe_send(raw)
        return True

    def _on_table_eof(
        self,
        table_name: TableName,
        client_id: str,
        channel=None,
        delivery_tag=None,
        redelivered=False,
    ) -> bool:
        """
        Handle EOF message. Returns True to ACK immediately, False to delay.
        For EOFs: delay ACK until all EOFs are received and joins are complete.
        """
        key = (table_name, client_id)
        tname = NAME_TO_STR.get(table_name, f"#{table_name}")

        if redelivered:
            log.info("TABLE_EOF REDELIVERED: table=%s cid=%s", tname, client_id)
        else:
            log.debug("TABLE_EOF received: table=%s cid=%s", tname, client_id)

        # Store channel and delivery_tag for later ACK
        with self._ack_lock:
            if channel is not None and delivery_tag is not None:
                if key not in self._unacked_eofs:
                    self._unacked_eofs[key] = []
                self._unacked_eofs[key].append((channel, delivery_tag))

        recvd = self._pending_eofs.setdefault(key, set())
        next_idx = self._part_counter.get(key, 0) + 1
        self._part_counter[key] = next_idx
        recvd.add(next_idx)

        log.info(
            "EOF recv table=%s cid=%s progress=%d/%d",
            tname,
            client_id,
            len(recvd),
            self._router_replicas,
        )

        if len(recvd) >= self._router_replicas:
            self._pending_eofs.pop(key, None)
            self._part_counter.pop(key, None)
            self._eof.add(key)
            self._log.info(
                "EOF marcado table_id=%s cid=%s; eof_set=%s",
                table_name,
                client_id,
                sorted(self._eof),
            )

            # Check if we can ACK light tables and all EOFs for this client
            self._maybe_ack_complete_lifecycle(client_id)

        # Delay ACK until complete lifecycle
        return False

    def _maybe_ack_complete_lifecycle(self, client_id: str) -> None:
        """
        Check if all EOFs have been received for a client and all joins are complete.
        If so, ACK the light tables and all EOFs for that client.
        """
        # Check if we have all required EOFs for this client
        required_eofs = [
            (TableName.MENU_ITEMS, client_id),
            (TableName.STORES, client_id),
            (TableName.TRANSACTION_ITEMS, client_id),
            (TableName.TRANSACTIONS, client_id),
            (TableName.USERS, client_id),
        ]

        if all(eof_key in self._eof for eof_key in required_eofs):
            self._log.info(
                "Complete lifecycle detected for client_id=%s, ACKing light tables and EOFs",
                client_id,
            )

            # ACK light tables
            self._ack_light_table((TableName.MENU_ITEMS, client_id))
            self._ack_light_table((TableName.STORES, client_id))

            # ACK all EOFs
            for eof_key in required_eofs:
                self._ack_eof(eof_key)

            # Clean up caches for this client
            self._cache_menu.pop(client_id, None)
            self._cache_stores.pop(client_id, None)

    def _ack_light_table(self, key: tuple[TableName, str]) -> None:
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
