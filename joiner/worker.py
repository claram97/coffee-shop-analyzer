from __future__ import annotations

import copy
import os
import shelve
import threading
from collections import defaultdict
from typing import Any, Dict, List, Optional, Set

# ==== IDs de tablas ====
T_TRANSACTIONS = 0
T_USERS = 1
T_TRANSACTION_ITEMS = 2
T_MENU_ITEMS = 10
T_STORES = 11

# ==== Queries ====
Q1, Q2, Q3, Q4 = 1, 2, 3, 4

# ==== Proyecto (interfaces/helpers) ====
# from middleware_iface import MessageMiddleware
# from protocol_utils import (
#     databatch_from_bytes, databatch_to_bytes,
#     table_eof_from_bytes, table_eof_to_bytes
# )
# from protocol_types import DataBatch


# ---------------------- Storage simple ----------------------
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


# ---------------------- Utils de join ----------------------
def norm(v) -> str:
    return "" if v is None else str(v)


def rows_to_index(
    rows: List[Dict[str, Any]], key_field: str
) -> Dict[str, Dict[str, Any]]:
    return {norm(r.get(key_field)): r for r in rows}


def join_embed_rows(
    left_rows: List[Dict[str, Any]],
    right_index: Dict[str, Dict[str, Any]],
    left_key: str,
    right_key: str,
    right_prefix: str,
) -> List[Dict[str, Any]]:
    out = []
    for lr in left_rows:
        k = norm(lr.get(left_key))
        rr = right_index.get(k)
        if rr is None:
            out.append(lr)
        else:
            merged = dict(lr)
            for rk, rv in rr.items():
                merged[f"{right_prefix}.{rk}"] = rv
            out.append(merged)
    return out


def metadata_only(db) -> None:
    if db.batch_msg and hasattr(db.batch_msg, "rows"):
        db.batch_msg.rows = []


def queries_set(db) -> set[int]:
    return set(getattr(db, "query_ids", []) or [])


def table_id_of(db) -> int:
    return int(db.table_ids[0])


# ---------------------- Joiner Worker con gating por colas ----------------------
class JoinerWorker:
    """
    Fases controladas por qué colas están en consumo:
      - Inicio: solo menu_items y stores.
      - Al EOF(menu_items) y EOF(stores): comenzar a consumir transaction_items.
      - Al EOF(transaction_items): comenzar a consumir transactions.
      - Al EOF(transactions): comenzar a consumir users.
    """

    def __init__(
        self,
        in_mw: Dict[int, "MessageMiddleware"],  # {table_id: mw} una cola por tabla
        out_results_mw: "MessageMiddleware",
        data_dir: str = "./data/joiner",
    ):
        self._in = in_mw
        self._out = out_results_mw
        self._store = DiskKV(os.path.join(data_dir, "joiner.shelve"))

        self._cache_stores: Optional[Dict[str, Dict[str, Any]]] = None
        self._cache_menu: Optional[Dict[str, Dict[str, Any]]] = None

        self._eof: Set[int] = set()
        self._lock = threading.Lock()

        self._threads: Dict[int, threading.Thread] = {}

    # ---------- arranque ----------
    def run(self):
        # Solo livianas al inicio
        self._start_queue(T_MENU_ITEMS, self._on_raw_menu)
        self._start_queue(T_STORES, self._on_raw_stores)
        # Esperar indefinidamente; en prod, coordinás con el proceso principal
        threading.Event().wait()

    # ---------- control de colas ----------
    def _start_queue(self, table_id: int, cb: Callable[[bytes], None]):
        mw = self._in.get(table_id)
        if not mw:
            return
        if table_id in self._threads and self._threads[table_id].is_alive():
            return
        t = threading.Thread(target=mw.start_consuming, args=(cb,), daemon=True)
        self._threads[table_id] = t
        t.start()

    def _stop_queue(self, table_id: int):
        mw = self._in.get(table_id)
        if not mw:
            return
        try:
            mw.stop_consuming()
        except Exception:
            pass

    # ---------- callbacks por tabla ----------
    def _on_raw_menu(self, raw: bytes):
        eof_tid = table_eof_from_bytes(raw)
        if eof_tid is not None:
            self._on_table_eof(eof_tid)
            # Podés detener la cola tras EOF si querés liberar recursos
            self._stop_queue(T_MENU_ITEMS)
            self._maybe_enable_ti_phase()
            return

        db = databatch_from_bytes(raw)
        rows = db.batch_msg.rows or []
        idx = rows_to_index(rows, "item_id")
        self._store.put("menu_items", "full", idx)
        self._cache_menu = idx

        meta = copy.deepcopy(db)
        metadata_only(meta)
        self._send(meta)

    def _on_raw_stores(self, raw: bytes):
        eof_tid = table_eof_from_bytes(raw)
        if eof_tid is not None:
            self._on_table_eof(eof_tid)
            self._stop_queue(T_STORES)
            self._maybe_enable_ti_phase()
            return

        db = databatch_from_bytes(raw)
        rows = db.batch_msg.rows or []
        idx = rows_to_index(rows, "store_id")
        self._store.put("stores", "full", idx)
        self._cache_stores = idx

        meta = copy.deepcopy(db)
        metadata_only(meta)
        self._send(meta)

    def _on_raw_ti(self, raw: bytes):
        eof_tid = table_eof_from_bytes(raw)
        if eof_tid is not None:
            self._on_table_eof(eof_tid)
            self._stop_queue(T_TRANSACTION_ITEMS)
            self._maybe_enable_tx_phase()
            return

        db = databatch_from_bytes(raw)
        if Q2 not in queries_set(db):
            # Si llegara otra query por TI (inusual), forward tal cual
            self._send(db)
            return

        menu = self._cache_menu or self._store.get("menu_items", "full", default=None)
        if not menu:
            # Por contrato de gating, no debería pasar
            return
        rows = db.batch_msg.rows or []
        joined = join_embed_rows(rows, menu, "item_id", "item_id", "menu")
        if T_MENU_ITEMS not in db.table_ids:
            db.table_ids.append(T_MENU_ITEMS)
        db.batch_msg.rows = joined
        self._send(db)

    def _on_raw_tx(self, raw: bytes):
        eof_tid = table_eof_from_bytes(raw)
        if eof_tid is not None:
            self._on_table_eof(eof_tid)
            self._stop_queue(T_TRANSACTIONS)
            self._maybe_enable_u_phase()
            return

        db = databatch_from_bytes(raw)
        qset = queries_set(db)

        if qset == {Q1}:
            self._send(db)
            return

        stores = self._cache_stores or self._store.get("stores", "full", default=None)
        if not stores:
            # Por contrato de gating, no debería pasar
            return

        rows = db.batch_msg.rows or []
        joined_tx_st = join_embed_rows(rows, stores, "store_id", "store_id", "store")

        if Q3 in qset:
            if T_STORES not in db.table_ids:
                db.table_ids.append(T_STORES)
            db.batch_msg.rows = joined_tx_st
            self._send(db)
            return

        if Q4 in qset:
            # Persistir parcial por user_id: {template_raw, rows}
            template = copy.deepcopy(db)
            metadata_only(template)
            template_raw = databatch_to_bytes(template)

            by_user: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
            for r in joined_tx_st:
                by_user[norm(r.get("user_id"))].append(r)

            total = len(by_user) if by_user else 1
            for uid, lst in by_user.items():
                item = {"template_raw": template_raw, "rows": lst, "total": total}
                self._store.append_list("q4_by_user", uid, item)
            return

        # Cualquier otro mix inesperado: forward
        self._send(db)

    def _on_raw_users(self, raw: bytes):
        eof_tid = table_eof_from_bytes(raw)
        if eof_tid is not None:
            self._on_table_eof(eof_tid)
            self._stop_queue(T_USERS)
            # Flushear lo que quede sin user
            self._flush_remaining_q4_without_user()
            return

        db = databatch_from_bytes(raw)
        if Q4 in queries_set(db):
            rows = db.batch_msg.rows or []
        for u in rows:
            uid = norm(u.get("user_id"))
            items = self._store.pop_all("q4_by_user", uid)
            if not items:
                continue
            usr_idx = {uid: u}

            # enumerar para asignar 'index' consistente en este worker
            for i, it in enumerate(items):
                template_raw: bytes = it["template_raw"]
                tx_rows: List[Dict[str, Any]] = it["rows"]
                total: int = int(it.get("total", len(items)))

                joined = join_embed_rows(tx_rows, usr_idx, "user_id", "user_id", "user")
                out_db = databatch_from_bytes(template_raw)

                # asegurar tablas
                if T_STORES not in out_db.table_ids:
                    out_db.table_ids.append(T_STORES)
                if T_TRANSACTIONS not in out_db.table_ids:
                    out_db.table_ids.append(T_TRANSACTIONS)

                # set rows
                if out_db.batch_msg and hasattr(out_db.batch_msg, "rows"):
                    out_db.batch_msg.rows = joined

                # marcar fan-out para que el RC cuente correcto
                ci = getattr(out_db, "copy_info", None)
                if ci is None:
                    out_db.copy_info = []
                out_db.copy_info.append(CopyInfo(index=i, total=total))

                self._send(out_db)
        # Siempre metadata-only de users para contabilidad en RC
        meta = copy.deepcopy(db)
        metadata_only(meta)
        self._send(meta)

    # ---------- EOF / fases ----------
    def _on_table_eof(self, table_id: int):
        with self._lock:
            self._eof.add(int(table_id))

    def _maybe_enable_ti_phase(self):
        if (T_MENU_ITEMS in self._eof) and (T_STORES in self._eof):
            self._start_queue(T_TRANSACTION_ITEMS, self._on_raw_ti)

    def _maybe_enable_tx_phase(self):
        if T_TRANSACTION_ITEMS in self._eof:
            self._start_queue(T_TRANSACTIONS, self._on_raw_tx)

    def _maybe_enable_u_phase(self):
        if T_TRANSACTIONS in self._eof:
            self._start_queue(T_USERS, self._on_raw_users)

    # ---------- flush Q4 sin user al EOF(users) ----------
    def _flush_remaining_q4_without_user(self):
        uids = self._store.keys_with_prefix("q4_by_user")
        for uid in uids:
            items = self._store.pop_all("q4_by_user", uid)
            for it in items:
                template_raw: bytes = it["template_raw"]
                tx_rows: List[Dict[str, Any]] = it["rows"]
                out_db = databatch_from_bytes(template_raw)
                if T_STORES not in out_db.table_ids:
                    out_db.table_ids.append(T_STORES)
                if T_TRANSACTIONS not in out_db.table_ids:
                    out_db.table_ids.append(T_TRANSACTIONS)
                if out_db.batch_msg and hasattr(out_db.batch_msg, "rows"):
                    out_db.batch_msg.rows = tx_rows
                self._send(out_db)

    # ---------- envío ----------
    def _send(self, db):
        raw = databatch_to_bytes(db)
        self._out.send(raw)
