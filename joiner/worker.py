from __future__ import annotations

import copy
import hashlib
import logging
import os
import pickle
import struct
import threading
from collections import defaultdict
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

from middleware.middleware_client import MessageMiddleware
from protocol.constants import Opcodes
from protocol.databatch import DataBatch
from protocol.entities import (
    RawMenuItems,
    RawStore,
    RawTransaction,
    RawTransactionItem,
    RawTransactionItemMenuItem,
    RawTransactionStore,
    RawTransactionStoreUser,
    RawUser,
)
from protocol.messages import (
    EOFMessage,
    NewTransactionItemsMenuItems,
    NewTransactionStores,
    NewTransactionStoresUsers,
)

_RecordPtr = Tuple[int, int]

Q1, Q2, Q3, Q4 = 1, 2, 3, 4
log = logging.getLogger("joiner-worker")

NAME_TO_ID = {
    "transactions": Opcodes.NEW_TRANSACTION,
    "users": Opcodes.NEW_USERS,
    "transaction_items": Opcodes.NEW_TRANSACTION_ITEMS,
    "menu_items": Opcodes.NEW_MENU_ITEMS,
    "stores": Opcodes.NEW_STORES,
}
ID_TO_NAME = {v: k for (k, v) in NAME_TO_ID.items()}


def _eof_table_id(eof) -> Optional[int]:
    raw = getattr(eof, "table_type", None)
    if raw is None:
        print(f"WARNING: EOF message missing table_type: {eof}")
        return None
    s = str(raw).strip().lower()
    return NAME_TO_ID[s]


def norm(v) -> str:
    return "" if v is None else str(v)


def index_by_attr(rows: List[object], attr: str) -> Dict[str, object]:
    idx: Dict[str, object] = {}
    for r in rows:
        k = norm(getattr(r, attr, None))
        if k:
            idx[k] = r
    return idx


def metadata_only(db: DataBatch) -> None:
    if getattr(db, "batch_msg", None) and hasattr(db.batch_msg, "rows"):
        db.batch_msg.rows = []


def queries_set(db: DataBatch) -> set[int]:
    return set(getattr(db, "query_ids", []) or [])


class JoinerWorker:
    def __init__(
        self,
        in_mw: Dict[int, MessageMiddleware],
        out_results_mw: MessageMiddleware,
        router_replicas: int,
        stop_event: threading.Event,
        data_dir: str = "./data/joiner",
        logger=None,
        shard_index: int = 0,
        out_factory: Optional[Callable[[], MessageMiddleware]] = None,
    ):
        self._in = in_mw
        self._out = out_results_mw
        self._out_factory = out_factory
        self._out_lock = threading.Lock()

        self._cache_stores: Dict[str, Dict[str, RawStore]] = {}
        self._cache_menu: Dict[str, Dict[str, RawMenuItems]] = {}

        self._eof: Set[Tuple[int, str]] = set()
        self._lock = threading.Lock()
        self._threads: Dict[int, threading.Thread] = {}
        self._log = logger or log
        self._shard = int(shard_index)
        self._router_replicas = router_replicas

        self._pending_eofs: Dict[tuple[int, str], Set[int]] = {}
        self._part_counter: Dict[tuple[int, str], int] = {}

        self._stop_event = stop_event
        self._is_shutting_down = False

    def _log_db(self, where: str, db: DataBatch):
        try:
            t = getattr(db.batch_msg, "opcode", None)
            q = list(getattr(db, "query_ids", []) or [])
            n = len((db.batch_msg.rows or []) if getattr(db, "batch_msg", None) else [])
            self._log.debug("%s: table=%s queries=%s rows=%s", where, t, q, n)
        except Exception as e:
            self._log.debug("log_db failed: %s", e)

    def run(self):
        self._log.info("JoinerWorker shard=%d: iniciando consumidores", self._shard)
        self._start_queue(Opcodes.NEW_MENU_ITEMS, self._on_raw_menu)
        self._start_queue(Opcodes.NEW_STORES, self._on_raw_stores)
        self._start_queue(Opcodes.NEW_TRANSACTION_ITEMS, self._on_raw_ti)
        self._start_queue(Opcodes.NEW_TRANSACTION, self._on_raw_tx)
        self._start_queue(Opcodes.NEW_USERS, self._on_raw_users)

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

    def _start_queue(self, table_id: int, cb: Callable[[bytes], None]):
        mw = self._in.get(table_id)
        if not mw:
            self._log.debug("No hay consumer para table_id=%s", table_id)
            return
        if table_id in self._threads and self._threads[table_id].is_alive():
            return
        t = threading.Thread(target=mw.start_consuming, args=(cb,), daemon=False)
        self._threads[table_id] = t
        t.start()
        self._log.info("Consumiendo table_id=%s", table_id)

    def _stop_queue(self, table_id: int):
        mw = self._in.get(table_id)
        if not mw:
            return
        try:
            threading.Thread(target=mw.stop_consuming, daemon=True).start()
            if self._log:
                self._log.info("Detenida cola table_id=%s", table_id)
        except Exception as e:
            if self._log:
                self._log.warning("Error deteniendo cola %s: %s", table_id, e)

    def _decode_msg(
        self, body: bytes
    ) -> Tuple[str, Union[EOFMessage, DataBatch, int, None]]:
        if self._is_shutting_down or self._stop_event.is_set():
            self._log.warning("Shutdown in progress, skipping message.")
            return "shutdown", None

        if not body or len(body) < 1:
            self._log.error("Mensaje vacío")
            return "err", None

        opcode = body[0]

        if opcode == Opcodes.EOF:
            try:
                eof = EOFMessage.deserialize_from_bytes(body)
                self._log.info(
                    "EOF recibido table_type=%s cid=%s",
                    getattr(eof, "table_type", "?"),
                    getattr(eof, "client_id", "?"),
                )
                return "eof", eof
            except Exception as e:
                self._log.error("EOF inválido: %s", e)
                return "err", None

        if opcode == Opcodes.DATA_BATCH:
            try:
                db = DataBatch.deserialize_from_bytes(body)
                self._log_db("DataBatch recibido", db)
                return "db", db
            except Exception as e:
                self._log.error("DataBatch inválido: %s", e)
                return "err", None

        self._log.warning("Opcode no deseado: %s", opcode)
        return "bad", opcode

    def _phase_ready(self, table_id: int, client_id: str) -> bool:
        need = []
        if table_id == Opcodes.NEW_TRANSACTION_ITEMS:
            need = [Opcodes.NEW_MENU_ITEMS, Opcodes.NEW_STORES]
        elif table_id == Opcodes.NEW_TRANSACTION:
            need = [Opcodes.NEW_MENU_ITEMS, Opcodes.NEW_STORES]
        elif table_id == Opcodes.NEW_USERS:
            need = [Opcodes.NEW_TRANSACTION]
        else:
            need = []
        return all((t, client_id) in self._eof for t in need)

    def _requeue(self, table_id: int, raw: bytes):
        mw = self._in.get(table_id)
        try:
            mw.send(raw)
            self._log.debug(
                "requeue → same queue table_id=%s bytes=%d", table_id, len(raw)
            )
        except Exception as e:
            self._log.error("requeue failed table_id=%s: %s", table_id, e)

    def _on_raw_menu(self, raw: bytes):
        kind, msg = self._decode_msg(raw)
        if kind in ("err", "bad", "shutdown"):
            return
        if kind == "eof":
            eof: EOFMessage = msg
            self._on_table_eof(
                _eof_table_id(eof),
                getattr(eof, "client_id", ""),
            )
            return
        if kind != "db":
            return
        db: DataBatch = msg
        cid = getattr(db, "client_id", "")
        self._log.debug(
            "IN: menu_items batch_number=%s shard=%s shards_info=%s queries=%s cid=%s",
            getattr(db, "batch_number", "?") if db else "?",
            getattr(self, "_shard", "?"),
            getattr(db, "shards_info", "[]") if db else "?",
            list(getattr(db, "query_ids", []) or []) if db else [],
            cid,
        )
        rows: List[RawMenuItems] = (
            (db.batch_msg.rows or []) if getattr(db, "batch_msg", None) else []
        )
        idx = index_by_attr(rows, "product_id")
        self._cache_menu[(cid)] = idx
        self._log.info("Cache menu_items idx_size=%d", len(idx))

    def _on_raw_stores(self, raw: bytes):
        kind, msg = self._decode_msg(raw)
        if kind in ("err", "bad", "shutdown"):
            return
        if kind == "eof":
            eof: EOFMessage = msg
            self._on_table_eof(
                _eof_table_id(eof),
                getattr(eof, "client_id", ""),
            )
            return
        if kind != "db":
            return
        db: DataBatch = msg
        cid = getattr(db, "client_id", "")
        self._log.debug(
            "IN: stores batch_number=%s shard=%s shards_info=%s queries=%s cid=%s",
            getattr(db, "batch_number", "?") if db else "?",
            getattr(self, "_shard", "?"),
            getattr(db, "shards_info", "[]") if db else "?",
            list(getattr(db, "query_ids", []) or []) if db else [],
            cid,
        )
        rows: List[RawStore] = (
            (db.batch_msg.rows or []) if getattr(db, "batch_msg", None) else []
        )
        idx = index_by_attr(rows, "store_id")
        self._cache_stores[(cid)] = idx
        self._log.info("Cache stores idx_size=%d", len(idx))

    def _on_raw_ti(self, raw: bytes):
        kind, msg = self._decode_msg(raw)
        if kind in ("err", "bad", "shutdown"):
            return
        if kind == "eof":
            eof: EOFMessage = msg
            self._on_table_eof(
                _eof_table_id(eof),
                getattr(eof, "client_id", ""),
            )
            return
        if kind != "db":
            return
        db: DataBatch = msg
        cid = getattr(db, "client_id", "")

        self._log.debug(
            "IN: transaction_items batch_number=%s shard=%s shards_info=%s queries=%s cid=%s",
            getattr(db, "batch_number", "?") if db else "?",
            getattr(self, "_shard", "?"),
            getattr(db, "shards_info", "[]") if db else "?",
            list(getattr(db, "query_ids", []) or []) if db else [],
            cid,
        )

        if not self._phase_ready(Opcodes.NEW_TRANSACTION_ITEMS, cid):
            self._log.debug("TI fase NO lista → requeue (cid=%s)", cid)
            self._requeue(Opcodes.NEW_TRANSACTION_ITEMS, raw)
            return

        if Q2 not in queries_set(db):
            self._log.info("TI sin Q2 → passthrough")
            self._send(db)
            return

        menu_idx: Optional[Dict[str, RawMenuItems]] = self._cache_menu.get((cid))
        if not menu_idx:
            self._log.warning("Menu cache no disponible aún; drop batch TI")
            return

        ti_rows: List[RawTransactionItem] = (
            (db.batch_msg.rows or []) if getattr(db, "batch_msg", None) else []
        )
        out_rows: List[RawTransactionItemMenuItem] = []

        for r in ti_rows:
            item_id = norm(getattr(r, "item_id", None))
            mi = menu_idx.get(item_id)
            if not mi:
                continue

            quantity_value = norm(getattr(r, "quantity", ""))
            self._log.debug(
                "Q2 item quantity for item_id=%s: %s", item_id, quantity_value
            )

            joined_item = RawTransactionItemMenuItem(
                transaction_id=norm(getattr(r, "transaction_id", "")),
                item_name=norm(getattr(mi, "name", "")),
                quantity=quantity_value,
                subtotal=norm(getattr(r, "subtotal", "")),
                created_at=norm(getattr(r, "created_at", "")),
            )

            self._log.debug(
                "Q2 joined item: transaction_id=%s, item_name=%s, quantity=%s",
                joined_item.transaction_id,
                joined_item.item_name,
                joined_item.quantity,
            )

            out_rows.append(joined_item)

        self._log.debug("JOIN Q2: in=%d matched=%d", len(ti_rows), len(out_rows))
        joined_msg = NewTransactionItemsMenuItems()
        joined_msg.rows = out_rows
        batch_status = getattr(db.batch_msg, "batch_status", 0)
        joined_msg.batch_status = batch_status
        self._log.debug("Q2 preserving TI batch_status=%d", batch_status)
        db.table_ids = [Opcodes.NEW_MENU_ITEMS, Opcodes.NEW_TRANSACTION_ITEMS]
        db.batch_msg = joined_msg
        self._send(db)

    def _on_raw_tx(self, raw: bytes):
        kind, msg = self._decode_msg(raw)
        if kind in ("err", "bad", "shutdown"):
            return
        if kind == "eof":
            eof: EOFMessage = msg
            self._on_table_eof(
                _eof_table_id(eof),
                getattr(eof, "client_id", ""),
            )
            return
        if kind != "db":
            return
        db: DataBatch = msg
        cid = getattr(db, "client_id", "")

        self._log.debug(
            "IN: transactions batch_number=%s shard=%s shards_info=%s queries=%s cid=%s",
            getattr(db, "batch_number", "?") if db else "?",
            getattr(self, "_shard", "?"),
            getattr(db, "shards_info", "[]") if db else "?",
            list(getattr(db, "query_ids", []) or []) if db else [],
            cid,
        )

        if not self._phase_ready(Opcodes.NEW_TRANSACTION, cid):
            self._log.debug("TX fase NO lista → requeue (cid=%s)", cid)
            self._requeue(Opcodes.NEW_TRANSACTION, raw)
            return

        qset = queries_set(db)

        if Q1 in qset:
            self._log.debug("TX Q1 passthrough")
            self._send(db)
            return

        tx_rows: List[RawTransaction] = (
            (db.batch_msg.rows or []) if getattr(db, "batch_msg", None) else []
        )

        stores_idx: Optional[Dict[str, RawStore]] = self._cache_stores.get((cid))
        if not stores_idx:
            self._log.info("Stores cache no disponible aún; drop batch TX")
            return

        joined_tx_st: List[RawTransactionStore] = []
        for tx in tx_rows:
            sid = norm(getattr(tx, "store_id", None))
            st = stores_idx.get(sid)
            if not st:
                continue
            joined_tx_st.append(
                RawTransactionStore(
                    transaction_id=norm(getattr(tx, "transaction_id", "")),
                    store_id=sid,
                    store_name=norm(getattr(st, "store_name", "")),
                    city=norm(getattr(st, "city", "")),
                    final_amount=norm(getattr(tx, "final_amount", "")),
                    created_at=norm(getattr(tx, "created_at", "")),
                    user_id=norm(getattr(tx, "user_id", "")),
                )
            )
        self._log.debug("JOIN Q3: in=%d matched=%d", len(tx_rows), len(joined_tx_st))
        msg_join = NewTransactionStores()
        msg_join.rows = joined_tx_st
        batch_status = getattr(db.batch_msg, "batch_status", 0)
        msg_join.batch_status = batch_status
        self._log.debug("Q3 preserving TX batch_status=%d", batch_status)
        db.table_ids = [Opcodes.NEW_TRANSACTION_STORES]
        db.batch_msg = msg_join
        self._send(db)
        return

        self._send(db)

    def _on_raw_users(self, raw: bytes):
        kind, msg = self._decode_msg(raw)
        if kind in ("err", "bad", "shutdown"):
            return
        if kind == "eof":
            eof: EOFMessage = msg
            self._on_table_eof(
                _eof_table_id(eof),
                getattr(eof, "client_id", ""),
            )
            return
        if kind != "db":
            return
        db: DataBatch = msg
        cid = getattr(db, "client_id", "")

        self._log.debug(
            "IN: users batch_number=%s shard=%s shards_info=%s queries=%s cid=%s",
            getattr(db, "batch_number", "?") if db else "?",
            getattr(self, "_shard", "?"),
            getattr(db, "shards_info", "[]") if db else "?",
            list(getattr(db, "query_ids", []) or []) if db else [],
            cid,
        )

        if not self._phase_ready(Opcodes.NEW_USERS, cid):
            self._log.debug("U fase NO lista → requeue (cid=%s)", cid)
            self._requeue(Opcodes.NEW_USERS, raw)
            return

        self._send(db)

    def _on_table_eof(self, table_id: int, client_id: str) -> bool:
        key = (table_id, client_id)
        tname = ID_TO_NAME.get(table_id, f"#{table_id}")
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
                table_id,
                client_id,
                sorted(self._eof),
            )
            return True

        return False

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

    def _send(self, db: DataBatch):
        batch_num = getattr(db, "batch_number", "?")
        shard_num = getattr(self, "_shard", "?")
        queries = list(getattr(db, "query_ids", []) or [])
        batch_status = (
            getattr(db.batch_msg, "batch_status", None)
            if getattr(db, "batch_msg", None)
            else None
        )
        status_text = "UNKNOWN"
        if batch_status == 0:
            status_text = "CONTINUE"
        elif batch_status == 1:
            status_text = "EOF"
        elif batch_status == 2:
            status_text = "CANCEL"
        self._log.debug(
            "OUT: Sending batch table=%s batch_number=%s shard=%s queries=%s rows=%d batch_status=%s",
            getattr(db.batch_msg, "opcode", "?"),
            batch_num,
            shard_num,
            queries,
            len((db.batch_msg.rows or []) if getattr(db, "batch_msg", None) else []),
            status_text,
        )
        if getattr(db, "batch_msg", None) and hasattr(db.batch_msg, "to_bytes"):
            db.batch_bytes = db.batch_msg.to_bytes()
        raw = db.to_bytes()
        try:
            self._safe_send(raw)
            self._log.debug(
                "SEND ok bytes=%d table=%s queries=%s",
                len(raw),
                getattr(db.batch_msg, "opcode", "?"),
                list(getattr(db, "query_ids", []) or []),
            )
        except Exception as e:
            self._log.error("SEND failed: %s", e)
