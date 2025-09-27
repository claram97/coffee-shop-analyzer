from __future__ import annotations

import copy
import os
import shelve
import threading
import time
from collections import defaultdict
from typing import Any, Callable, Dict, List, Optional, Tuple

# ==== IMPORTS de tu proyecto ====
# from middleware_iface import MessageMiddleware
# from protocol_utils import databatch_from_bytes, databatch_to_bytes
# from protocol_types import DataBatch

# ==== IDs de tablas (ajusta a tu mapping real) ====
T_TRANSACTIONS = 0
T_USERS = 1
T_TRANSACTION_ITEMS = 2
T_MENU_ITEMS = 10
T_STORES = 11

# ==== Queries ====
Q1, Q2, Q3, Q4 = 1, 2, 3, 4

# --------------------------------------------------
# Almacenamiento en disco (simple, idempotente)
# --------------------------------------------------


class DiskKV:
    """
    KV store simple sobre shelve.
    - Usa keys de texto.
    - Guarda listas de filas o diccionarios índice->fila.
    """

    def __init__(self, path: str):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self._path = path
        self._lock = threading.Lock()

    def put(self, bucket: str, key: str, value: Any) -> None:
        with self._lock, shelve.open(self._path) as db:
            k = f"{bucket}:{key}"
            db[k] = value

    def get(self, bucket: str, key: str, default=None) -> Any:
        with self._lock, shelve.open(self._path) as db:
            k = f"{bucket}:{key}"
            return db.get(k, default)

    def append_list(self, bucket: str, key: str, rows: List[Dict[str, Any]]) -> None:
        with self._lock, shelve.open(self._path, writeback=True) as db:
            k = f"{bucket}:{key}"
            lst = db.get(k)
            if lst is None:
                db[k] = list(rows)
            else:
                lst.extend(rows)
                db[k] = lst

    def items_with_prefix(self, bucket: str, prefix: str) -> List[Tuple[str, Any]]:
        out = []
        with self._lock, shelve.open(self._path) as db:
            p = f"{bucket}:{prefix}"
            for k in db.keys():
                if k.startswith(p):
                    out.append((k.split(":", 1)[1], db[k]))
        return out

    def delete(self, bucket: str, key: str) -> None:
        with self._lock, shelve.open(self._path, writeback=True) as db:
            k = f"{bucket}:{key}"
            if k in db:
                del db[k]


# --------------------------------------------------
# Utilidades
# --------------------------------------------------


def norm(v) -> str:
    """Normaliza ids a string para joins."""
    if v is None:
        return ""
    return str(v)


def rows_to_index(
    rows: List[Dict[str, Any]], key_field: str
) -> Dict[str, Dict[str, Any]]:
    idx = {}
    for r in rows:
        idx[norm(r.get(key_field))] = r
    return idx


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
            # Mantener registro aun sin match (depende de tu especificación)
            out.append(lr)
        else:
            merged = dict(lr)
            for rk, rv in rr.items():
                merged[f"{right_prefix}.{rk}"] = rv
            out.append(merged)
    return out


def metadata_only(db) -> None:
    """Vacía payload (rows) para enviar solo metadata al Results Controller."""
    if db.batch_msg and hasattr(db.batch_msg, "rows"):
        db.batch_msg.rows = []


def queries_set(db) -> set[int]:
    return set(getattr(db, "query_ids", []) or [])


def table_id_of(db) -> int:
    # asume tabla principal en table_ids[0]
    return int(db.table_ids[0])


# --------------------------------------------------
# Joiner Worker
# --------------------------------------------------


class JoinerWorker:
    """
    - Tiene una cola por tabla (inputs).
    - Mantiene caches en disco:
        * stores (broadcast)    -> índice por store_id
        * menu_items (broadcast)-> índice por item_id
        * tx_store parciales para Q4 -> por user_id
    - Reenvía cada batch (vacío o con datos joineados) al Results Controller.
    - Usa timeout para considerar “cierre” operativo.
    """

    def __init__(
        self,
        in_mw: Dict[int, "MessageMiddleware"],  # {table_id: mw}
        out_results_mw: "MessageMiddleware",
        data_dir: str = "./data/joiner",
        idle_timeout_sec: float = 15.0,
    ):
        self._in = in_mw
        self._out = out_results_mw
        self._store = DiskKV(os.path.join(data_dir, "joiner.shelve"))
        self._idle_timeout = idle_timeout_sec

        # heartbeat por cola para timeout
        self._last_msg_ts: Dict[int, float] = defaultdict(lambda: 0.0)

        # índices cacheados en RAM (opcional, chiquitos)
        self._cache_stores: Optional[Dict[str, Dict[str, Any]]] = None
        self._cache_menu: Optional[Dict[str, Dict[str, Any]]] = None

    # --------- arranque ----------

    def run(self):
        # consume cada cola en un hilo
        for tid, mw in self._in.items():
            t = threading.Thread(
                target=mw.start_consuming,
                args=(lambda raw, tid=tid: self._on_raw(tid, raw),),
                daemon=True,
            )
            t.start()

        # loop de timeout / mantenimiento
        while True:
            time.sleep(1.0)
            now = time.time()
            # si querés tomar acciones por timeout (logging, limpieza), hacelo acá.
            # Ejemplo: si hace mucho que no llega users, drenar pendientes, etc.

    # --------- callbacks por tabla ----------

    def _on_raw(self, table_id: int, raw: bytes):
        self._last_msg_ts[table_id] = time.time()
        db = databatch_from_bytes(raw)
        qset = queries_set(db)
        t = table_id_of(db)
        assert t == table_id, "table_ids[0] no coincide con cola"

        if t == T_MENU_ITEMS:
            self._handle_menu_items(db)
        elif t == T_STORES:
            self._handle_stores(db)
        elif t == T_TRANSACTION_ITEMS:
            if Q2 in qset:
                self._handle_ti_q2(db)
            else:
                self._forward_as_is(db)  # por si hay Q1/Q3/Q4 mezcladas (no debería)
        elif t == T_TRANSACTIONS:
            if Q1 in qset and len(qset) == 1:
                self._handle_tx_q1(db)
            elif Q3 in qset and (qset == {Q3} or qset == {Q1, Q3}):
                self._handle_tx_q3(db)
            elif Q4 in qset:
                self._handle_tx_q4_partial(db)
            else:
                self._forward_as_is(db)
        elif t == T_USERS:
            if Q4 in qset:
                self._handle_users_q4(db)
            else:
                self._forward_as_is(db)
        else:
            self._forward_as_is(db)

    # --------- handlers por tabla / query ----------

    # --- tablas livianas: se guardan y se emite batch vacío al RC ---

    def _handle_menu_items(self, db):
        rows = db.batch_msg.rows or []
        # index by item_id
        idx = rows_to_index(rows, "item_id")
        self._store.put("menu_items", "full", idx)
        self._cache_menu = idx  # cache RAM
        # enviar metadata-only al RC
        db2 = copy.deepcopy(db)
        metadata_only(db2)
        self._send(db2)

    def _handle_stores(self, db):
        rows = db.batch_msg.rows or []
        # index by store_id
        idx = rows_to_index(rows, "store_id")
        self._store.put("stores", "full", idx)
        self._cache_stores = idx
        # enviar metadata-only al RC
        db2 = copy.deepcopy(db)
        metadata_only(db2)
        self._send(db2)

    # --- Q2: transaction_items ⨝ menu_items en memoria, enviar al RC ---

    def _handle_ti_q2(self, db):
        rows = db.batch_msg.rows or []
        menu = self._cache_menu or self._store.get("menu_items", "full", default=None)
        if not menu:
            # aún no tengo menú: guardo TI en disco hasta que llegue menú
            self._store.append_list("ti_backlog", "pending", rows)
            # igual enviar metadata al RC? Tu regla dice solo para livianas; aquí no.
            return

        joined = join_embed_rows(rows, menu, "item_id", "item_id", "menu")
        # marcar que el batch ahora incluye menu_items en table_ids (si procede)
        if T_MENU_ITEMS not in db.table_ids:
            db.table_ids.append(T_MENU_ITEMS)
        db.batch_msg.rows = joined
        self._send(db)

    # --- Q3: transactions ⨝ stores en memoria, enviar al RC ---

    def _handle_tx_q3(self, db):
        rows = db.batch_msg.rows or []
        stores = self._cache_stores or self._store.get("stores", "full", default=None)
        if not stores:
            # no tengo stores aún: backlog
            self._store.append_list("tx_q3_backlog", "pending", rows)
            return
        joined = join_embed_rows(rows, stores, "store_id", "store_id", "store")
        if T_STORES not in db.table_ids:
            db.table_ids.append(T_STORES)
        db.batch_msg.rows = joined
        self._send(db)

    # --- Q1: transactions directo al RC ---

    def _handle_tx_q1(self, db):
        self._send(db)

    # --- Q4 (parte 1): transactions ⨝ stores -> parcial en disco ---

    def _handle_tx_q4_partial(self, db):
        rows = db.batch_msg.rows or []
        stores = self._cache_stores or self._store.get("stores", "full", default=None)
        if not stores:
            # aún no tengo stores: backlog
            self._store.append_list("tx_q4_backlog_raw", "pending", rows)
            return
        txs = join_embed_rows(rows, stores, "store_id", "store_id", "store")
        # agrupar por user_id y guardar parcial por user
        by_user: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for r in txs:
            by_user[norm(r.get("user_id"))].append(r)
        for uid, lst in by_user.items():
            self._store.append_list("tx_store_by_user", uid, lst)
        # no se envía al RC todavía (según tu flujo)

    # --- Q4 (parte 2): users completa join con parciales y envía; users al RC vacío ---

    def _handle_users_q4(self, db):
        rows = db.batch_msg.rows or []
        out_batches: List[Dict[str, Any]] = []
        for u in rows:
            uid = norm(u.get("user_id"))
            txs = self._store.get("tx_store_by_user", uid, default=None)
            if not txs:
                continue
            # Completar join (embeder datos del user en cada tx)
            usr_idx = {uid: u}
            joined = join_embed_rows(txs, usr_idx, "user_id", "user_id", "user")
            out_batches.extend(joined)
            # limpieza opcional
            self._store.delete("tx_store_by_user", uid)

        if out_batches:
            db2 = copy.deepcopy(db)
            # El envío lo hacemos sobre el batch “parcial tx⨝stores”, no sobre users.
            # Para simplificar, enviamos en el frame de users pero con filas joineadas:
            db2.batch_msg.rows = out_batches
            # Aseguramos table_ids contengan stores y transactions además de users
            if T_STORES not in db2.table_ids:
                db2.table_ids.append(T_STORES)
            if T_TRANSACTIONS not in db2.table_ids:
                db2.table_ids.append(T_TRANSACTIONS)
            self._send(db2)

        # Enviar SIEMPRE metadata-only del batch de users (consistencia RC)
        db_meta = copy.deepcopy(db)
        metadata_only(db_meta)
        self._send(db_meta)

    # --------- envío al Results Controller ----------

    def _send(self, db):
        raw = databatch_to_bytes(db)
        self._out.send(raw)
