# joiner/worker.py
from __future__ import annotations

import copy
import logging
import os
import shelve
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

Q1, Q2, Q3, Q4 = 1, 2, 3, 4
log = logging.getLogger("joiner-worker")  # logger módulo

NAME_TO_ID = {
    "transactions": Opcodes.NEW_TRANSACTION,
    "users": Opcodes.NEW_USERS,
    "transaction_items": Opcodes.NEW_TRANSACTION_ITEMS,
    "menu_items": Opcodes.NEW_MENU_ITEMS,
    "stores": Opcodes.NEW_STORES,
}


def _eof_table_id(eof) -> Optional[int]:
    # First try to get table_type directly from the EOF message
    raw = getattr(eof, "table_type", None)
    # If that fails, try using get_table_type method if available
    if (raw is None or raw == "") and hasattr(eof, "get_table_type"):
        try:
            raw = eof.get_table_type()
        except Exception:
            raw = None
    
    if raw is None:
        # If we still don't have a table type, log the issue and return None
        print(f"WARNING: EOF message missing table_type: {eof}")
        return None
        
    # Convert to lowercase string and normalize
    s = str(raw).strip().lower()
    
    # Try mapping by name first
    if s in NAME_TO_ID:
        return NAME_TO_ID[s]
        
    # Try parsing as an integer (opcode)
    try:
        num = int(s)
        return num if num in NAME_TO_ID.values() else None
    except ValueError:
        # Not an integer, log and return None
        print(f"WARNING: Invalid table_type in EOF message: {s}")
        return None
        return None


class DiskKV:
    def __init__(self, path: str):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self._path = path
        self._lock = threading.Lock()

    def put(self, bucket: str, key: str, value: Any) -> None:
        with self._lock, shelve.open(self._path) as db:
            db[f"{bucket}:{key}"] = value

    def get(self, bucket: str, key: str, default=None) -> Any:
        with self._lock, shelve.open(self._path) as db:
            return db.get(f"{bucket}:{key}", default)

    def append_list(self, bucket: str, key: str, value_item: Any) -> None:
        with self._lock, shelve.open(self._path, writeback=True) as db:
            k = f"{bucket}:{key}"
            lst = db.get(k)
            if lst is None:
                db[k] = [value_item]
            else:
                lst.append(value_item)
                db[k] = lst

    def pop_all(self, bucket: str, key: str) -> List[Any]:
        with self._lock, shelve.open(self._path, writeback=True) as db:
            k = f"{bucket}:{key}"
            lst = db.get(k, [])
            if k in db:
                del db[k]
            return list(lst)

    def keys_with_prefix(self, bucket: str) -> List[str]:
        out = []
        with self._lock, shelve.open(self._path) as db:
            p = f"{bucket}:"
            for k in db.keys():
                if k.startswith(p):
                    out.append(k.split(":", 1)[1])
        return out

    def delete(self, bucket: str, key: str) -> None:
        with self._lock, shelve.open(self._path, writeback=True) as db:
            k = f"{bucket}:{key}"
            if k in db:
                del db[k]


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
    """
    Fases:
      - Inicio: menu_items + stores (livianas).
      - EOF(menu_items) & EOF(stores) => habilita transaction_items.
      - EOF(transaction_items) => habilita transactions.
      - EOF(transactions) => habilita users.
    """

    def __init__(
        self,
        in_mw: Dict[int, MessageMiddleware],
        out_results_mw: MessageMiddleware,
        data_dir: str = "./data/joiner",
        logger=None,
        shard_index: int = 0,
    ):
        self._in = in_mw
        self._out = out_results_mw
        self._store = DiskKV(os.path.join(data_dir, "joiner.shelve"))

        self._cache_stores: Optional[Dict[str, RawStore]] = None
        self._cache_menu: Optional[Dict[str, RawMenuItems]] = None

        self._eof: Set[int] = set()
        self._lock = threading.Lock()
        self._threads: Dict[int, threading.Thread] = {}
        self._log = logger or log
        self._shard = int(shard_index)

    # ---------- helpers de log ----------
    def _log_db(self, where: str, db: DataBatch):
        try:
            t = getattr(db.batch_msg, "opcode", None)
            q = list(getattr(db, "query_ids", []) or [])
            n = len((db.batch_msg.rows or []) if getattr(db, "batch_msg", None) else [])
            self._log.debug("%s: table=%s queries=%s rows=%s", where, t, q, n)
        except Exception as e:
            self._log.debug("log_db failed: %s", e)

    # ---------- ciclo de vida ----------
    def run(self):
        self._log.info(
            "JoinerWorker shard=%d: iniciando consumo de livianas (menu_items, stores)",
            self._shard,
        )
        self._start_queue(Opcodes.NEW_MENU_ITEMS, self._on_raw_menu)
        self._start_queue(Opcodes.NEW_STORES, self._on_raw_stores)
        threading.Event().wait()

    def _start_queue(self, table_id: int, cb: Callable[[bytes], None]):
        mw = self._in.get(table_id)
        if not mw:
            self._log.debug("No hay consumer para table_id=%s", table_id)
            return
        if table_id in self._threads and self._threads[table_id].is_alive():
            return
        t = threading.Thread(target=mw.start_consuming, args=(cb,), daemon=True)
        self._threads[table_id] = t
        t.start()
        self._log.info("Consumiendo table_id=%s", table_id)

    # joiner/worker.py
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

    # ---------- decodificación ----------
    def _decode_msg(
        self, body: bytes
    ) -> Tuple[str, Union[EOFMessage, DataBatch, int, None]]:
        if not body or len(body) < 1:
            self._log.error("Mensaje vacío")
            return "err", None

        opcode = body[0]

        if opcode == Opcodes.EOF:
            try:
                eof = EOFMessage.deserialize_from_bytes(body)
                self._log.info(
                    "EOF recibido table_type=%s", getattr(eof, "table_type", "?")
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

    # ---------- handlers ----------
    def _on_raw_menu(self, raw: bytes):
        kind, msg = self._decode_msg(raw)
        db = msg if kind == "db" else None
        batch_num = getattr(db, "batch_number", "?") if db else "?"
        shards_info = getattr(db, "shards_info", "[]") if db else "?"
        shard_num = getattr(self, "_shard", "?")
        queries = list(getattr(db, "query_ids", []) or []) if db else []
        self._log.debug(
            "IN: menu_items batch_number=%s shard=%s shards_info=%s queries=%s",
            batch_num,
            shard_num,
            shards_info,
            queries,
        )
        kind, msg = self._decode_msg(raw)
        if kind == "eof":
            eof: EOFMessage = msg
            self._on_table_eof(int(_eof_table_id(eof)))
            self._stop_queue(Opcodes.NEW_MENU_ITEMS)
            self._maybe_enable_ti_phase()
            self._maybe_enable_tx_phase()
            return
        if kind != "db":
            return

        db: DataBatch = msg
        rows: List[RawMenuItems] = (
            (db.batch_msg.rows or []) if getattr(db, "batch_msg", None) else []
        )
        idx = index_by_attr(rows, "product_id")
        self._store.put("menu_items", "full", idx)
        self._cache_menu = idx
        self._log.info("Cache menu_items idx_size=%d", len(idx))

    def _on_raw_stores(self, raw: bytes):
        kind, msg = self._decode_msg(raw)
        db = msg if kind == "db" else None
        batch_num = getattr(db, "batch_number", "?") if db else "?"
        shard_num = getattr(self, "_shard", "?")
        queries = list(getattr(db, "query_ids", []) or []) if db else []
        shards_info = getattr(db, "shards_info", "[]") if db else "?"
        self._log.debug(
            "IN: stores batch_number=%s shard=%s shards_info=%s queries=%s",
            batch_num,
            shard_num,
            shards_info,
            queries,
        )
        kind, msg = self._decode_msg(raw)
        if kind == "eof":
            eof: EOFMessage = msg
            self._on_table_eof(_eof_table_id(eof))
            self._stop_queue(Opcodes.NEW_STORES)
            self._maybe_enable_ti_phase()
            self._maybe_enable_tx_phase()
            return
        if kind != "db":
            return

        db: DataBatch = msg
        rows: List[RawStore] = (
            (db.batch_msg.rows or []) if getattr(db, "batch_msg", None) else []
        )
        idx = index_by_attr(rows, "store_id")
        self._store.put("stores", "full", idx)
        self._cache_stores = idx
        self._log.info("Cache stores idx_size=%d", len(idx))

    def _on_raw_ti(self, raw: bytes):
        kind, msg = self._decode_msg(raw)
        db = msg if kind == "db" else None
        batch_num = getattr(db, "batch_number", "?") if db else "?"
        shard_num = getattr(self, "_shard", "?")
        queries = list(getattr(db, "query_ids", []) or []) if db else []
        shards_info = getattr(db, "shards_info", "[]") if db else "?"
        self._log.debug(
            "IN: transaction_items batch_number=%s shard=%s shards_info=%s queries=%s",
            batch_num,
            shard_num,
            shards_info,
            queries,
        )
        kind, msg = self._decode_msg(raw)
        if kind == "eof":
            eof: EOFMessage = msg
            self._on_table_eof(_eof_table_id(eof))
            self._stop_queue(Opcodes.NEW_TRANSACTION_ITEMS)
            return
        if kind != "db":
            return

        db: DataBatch = msg
        if Q2 not in queries_set(db):
            self._log.info("TI sin Q2 → passthrough")
            self._send(db)
            return

        menu_idx: Optional[Dict[str, RawMenuItems]] = (
            self._cache_menu or self._store.get("menu_items", "full", default=None)
        )
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
            out_rows.append(
                RawTransactionItemMenuItem(
                    transaction_id=norm(getattr(r, "transaction_id", "")),
                    item_name=norm(getattr(mi, "name", "")),
                    quantity=norm(getattr(r, "quantity", "")),
                    subtotal=norm(getattr(r, "subtotal", "")),
                    created_at=norm(getattr(r, "created_at", "")),
                )
            )

        self._log.debug("JOIN Q2: in=%d matched=%d", len(ti_rows), len(out_rows))
        joined_msg = NewTransactionItemsMenuItems()
        joined_msg.rows = out_rows
        # For Q2, we should preserve EOF status from TransactionItems
        # TransactionItems (TI) is our primary stream that signals the end of Q2
        batch_status = getattr(db.batch_msg, "batch_status", 0)
        joined_msg.batch_status = batch_status
        self._log.debug("Q2 preserving TI batch_status=%d", batch_status)
        db.table_ids = [Opcodes.NEW_MENU_ITEMS, Opcodes.NEW_TRANSACTION_ITEMS]
        db.batch_msg = joined_msg
        self._send(db)

    def _on_raw_tx(self, raw: bytes):
        kind, msg = self._decode_msg(raw)
        db = msg if kind == "db" else None
        queries = list(getattr(db, "query_ids", []) or []) if db else []
        rows_qtty = len(db.batch_msg.rows)
        self._log.debug("IN: transactions queries=%s rows_qtty=%d", queries, rows_qtty)
        kind, msg = self._decode_msg(raw)
        if kind == "eof":
            eof: EOFMessage = msg
            # Explicitly mark the EOF for NEW_TRANSACTION table
            # This ensures we don't rely on _eof_table_id which might return None or wrong ID
            table_id = _eof_table_id(eof)
            self._log.info(
                "EOF en _on_raw_tx: table_type=%s, table_id=%s, forcing table_id=%s", 
                getattr(eof, "table_type", "?"), table_id, Opcodes.NEW_TRANSACTION
            )
            # Always mark the correct table ID for transactions
            self._on_table_eof(Opcodes.NEW_TRANSACTION)
            self._stop_queue(Opcodes.NEW_TRANSACTION)
            self._maybe_enable_u_phase()
            return
        if kind != "db":
            return

        db: DataBatch = msg
        qset = queries_set(db)
        tx_rows: List[RawTransaction] = (
            (db.batch_msg.rows or []) if getattr(db, "batch_msg", None) else []
        )

        if qset == {Q1}:
            self._log.info("TX Q1 passthrough rows=%d", len(tx_rows))
            self._send(db)
            return

        stores_idx: Optional[Dict[str, RawStore]] = (
            self._cache_stores or self._store.get("stores", "full", default=None)
        )
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

        if Q3 in qset:
            self._log.info("JOIN Q3: in=%d matched=%d", len(tx_rows), len(joined_tx_st))
            msg_join = NewTransactionStores()
            msg_join.rows = joined_tx_st
            # For Q3, we should preserve EOF status from Transactions
            # Transactions (TX) is our primary stream that signals the end of Q3
            batch_status = getattr(db.batch_msg, "batch_status", 0)
            msg_join.batch_status = batch_status
            self._log.debug("Q3 preserving TX batch_status=%d", batch_status)
            db.table_ids = [Opcodes.NEW_TRANSACTION_STORES]
            db.batch_msg = msg_join
            self._send(db)
            return

        if Q4 in qset:
            template = copy.deepcopy(db)
            metadata_only(template)

            by_user: Dict[str, List[Any]] = defaultdict(list)
            for r in joined_tx_st:
                uid = norm(getattr(r, "user_id", None))
                by_user[uid].append(r)

            total_users = len(by_user) if by_user else 0
            self._log.info(
                "JOIN Q4 stage1: users=%d txxstore=%d", total_users, len(joined_tx_st)
            )

            total = total_users if total_users else 1
            total_u8 = min(total, 255)

            # For Q4, we must preserve EOF from Transactions (primary stream)
            # This template will be used when joining with Users data
            original_batch_status = getattr(db.batch_msg, "batch_status", 0)
            # Ensure template's batch_msg has the same batch_status as original transaction
            template.batch_msg.batch_status = original_batch_status
            self._log.debug("Q4 template preserving TX batch_status=%d for later User join", original_batch_status)
            
            for idx, (uid, lst) in enumerate(by_user.items()):
                idx_u8 = idx % 256
                template.meta[total_u8] = idx_u8
                template.batch_bytes = template.batch_msg.to_bytes()
                template_raw = template.to_bytes()
                item = {"template_raw": template_raw, "rows": lst}
                self._store.append_list("q4_by_user", uid, item)
                self._log.debug(
                    "Q4 stash uid=%s rows=%d chunk=%d/%d", uid, len(lst), idx + 1, total
                )
            return

        self._send(db)

    def _on_raw_users(self, raw: bytes):
        kind, msg = self._decode_msg(raw)
        db = msg if kind == "db" else None
        batch_num = getattr(db, "batch_number", "?") if db else "?"
        shard_num = getattr(self, "_shard", "?")
        queries = list(getattr(db, "query_ids", []) or []) if db else []
        shards_info = getattr(db, "shards_info", "[]") if db else "?"
        self._log.debug(
            "IN: users batch_number=%s shard=%s shards_info=%s queries=%s",
            batch_num,
            shard_num,
            shards_info,
            queries,
        )
        kind, msg = self._decode_msg(raw)
        if kind == "eof":
            eof: EOFMessage = msg
            self._on_table_eof(_eof_table_id(eof))
            self._stop_queue(Opcodes.NEW_USERS)
            self._flush_remaining_q4_without_user()
            return
        if kind != "db":
            return

        db: DataBatch = msg
        users: List[RawUser] = (
            (db.batch_msg.rows or []) if getattr(db, "batch_msg", None) else []
        )
        self._log.debug("USERS recv rows=%d", len(users))

        if Q4 in queries_set(db):
            uidx = index_by_attr(users, "user_id")

            for uid, u in uidx.items():
                items = self._store.pop_all("q4_by_user", uid)
                if not items:
                    continue

                self._log.info(
                    "Q4 stage2 join uid=%s pending_chunks=%d", uid, len(items)
                )
                for it in items:
                    template_raw: bytes = it["template_raw"]
                    txst_rows: List[RawTransactionStore] = it["rows"]

                    out_rows: List[RawTransactionStoreUser] = []
                    for r in txst_rows:
                        out_rows.append(
                            RawTransactionStoreUser(
                                transaction_id=r.transaction_id,
                                store_id=r.store_id,
                                store_name=r.store_name,
                                user_id=uid,
                                birthdate=norm(getattr(u, "birthdate", "")),
                                created_at=r.created_at,
                            )
                        )

                    out_db = DataBatch.deserialize_from_bytes(template_raw)
                    msg_join = NewTransactionStoresUsers()
                    msg_join.rows = out_rows
                    # For Q4, preserve the Transaction's EOF status from template
                    # The template already has the Transaction's batch_status
                    batch_status = getattr(out_db.batch_msg, "batch_status", 0)
                    msg_join.batch_status = batch_status
                    self._log.debug("Q4 preserving TX batch_status=%d from template during User join", batch_status)
                    out_db.table_ids = [Opcodes.NEW_TRANSACTION_STORES_USERS]
                    out_db.batch_msg = msg_join
                    self._log.info("Q4 out rows=%d for uid=%s", len(out_rows), uid)
                    self._send(out_db)

    # ---------- fases ----------
    def _on_table_eof(self, table_id: int):
        with self._lock:
            self._eof.add(int(table_id))
        self._log.info(
            "EOF marcado table_id=%s; eof_set=%s", table_id, sorted(self._eof)
        )

    def _maybe_enable_ti_phase(self):
        if (Opcodes.NEW_MENU_ITEMS in self._eof) and (Opcodes.NEW_STORES in self._eof):
            self._start_queue(Opcodes.NEW_TRANSACTION_ITEMS, self._on_raw_ti)
            self._log.info("Activada fase TI")

    def _maybe_enable_tx_phase(self):
        if (Opcodes.NEW_MENU_ITEMS in self._eof) and (Opcodes.NEW_STORES in self._eof):
            self._start_queue(Opcodes.NEW_TRANSACTION, self._on_raw_tx)
            self._log.info("Activada fase TX")

    def _maybe_enable_u_phase(self):
        # For Q4, we need Transactions EOF before enabling Users phase
        # This means for Q4, Transaction EOF is the primary indicator
        if Opcodes.NEW_TRANSACTION in self._eof:
            self._start_queue(Opcodes.NEW_USERS, self._on_raw_users)
            self._log.info("Activada fase USERS (Q4 depende del EOF de Transactions)")

    def _flush_remaining_q4_without_user(self):
        uids = self._store.keys_with_prefix("q4_by_user")
        if uids:
            self._log.info("Flush Q4 sin user: uids_pendientes=%d", len(uids))
        for uid in uids:
            items = self._store.pop_all("q4_by_user", uid)
            for it in items:
                template_raw: bytes = it["template_raw"]
                txst_rows: List[RawTransactionStore] = it["rows"]
                out_db = DataBatch.deserialize_from_bytes(template_raw)
                msg_join = NewTransactionStores()
                msg_join.rows = txst_rows
                
                # For Q4 without user, still preserve Transaction's EOF from template
                batch_status = getattr(out_db.batch_msg, "batch_status", 0)
                msg_join.batch_status = batch_status
                self._log.debug("Q4 (sin user) preserving TX batch_status=%d from template", batch_status)
                
                out_db.table_ids = [Opcodes.NEW_TRANSACTION_STORES]
                out_db.batch_msg = msg_join
                self._log.info("Q4 (sin user) out rows=%d uid=%s", len(txst_rows), uid)
                self._send(out_db)

    # ---------- envío ----------
    def _send(self, db: DataBatch):
        batch_num = getattr(db, "batch_number", "?")
        shard_num = getattr(self, "_shard", "?")
        queries = list(getattr(db, "query_ids", []) or [])
        batch_status = getattr(db.batch_msg, "batch_status", None) if getattr(db, "batch_msg", None) else None
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
            self._out.send(raw)
            self._log.debug(
                "SEND ok bytes=%d table=%s queries=%s",
                len(raw),
                getattr(db.batch_msg, "opcode", "?"),
                list(getattr(db, "query_ids", []) or []),
            )
        except Exception as e:
            self._log.error("SEND failed: %s", e)
