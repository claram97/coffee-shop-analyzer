from __future__ import annotations

import copy
import os
import shelve
import threading
from collections import defaultdict
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

from middleware.middleware_client import MessageMiddleware
from protocol.constants import Opcodes
from protocol.databatch import DataBatch
from protocol.messages import EOFMessage

Q1, Q2, Q3, Q4 = 1, 2, 3, 4


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


def _get_attr_or_key(row: Union[dict, object], key: str):
    if isinstance(row, dict):
        return row.get(key)
    return getattr(row, key, None)


def rows_to_index(rows, key_field: str) -> Dict[str, Dict[str, Any]]:
    """
    Indexa por atributo (o key) para listas de objetos Raw* o dicts.
    Devuelve dict[str, object] (guardamos el objeto original).
    """
    idx: Dict[str, Any] = {}
    for r in rows:
        k = norm(_get_attr_or_key(r, key_field))
        if k != "":
            idx[k] = r
    return idx


def join_embed_rows(
    left_rows,
    right_index: Dict[str, Any],
    left_key: str,
    right_prefix: str,
):
    """
    OJO: Para no romper la serialización del protocolo, dejamos los objetos
    de la izquierda tal cual y NO “mutamos” el tipo. Si necesitás realmente
    “embeddear” campos (menu.* o store.*) en el payload, lo más seguro es
    que right_index contenga dicts ya “plaineados”. Acá mantenemos compat:
    - Si lr es dict => devolvemos dict con los campos embeddeados.
    - Si lr es objeto => devolvemos el objeto tal cual (sin embed) para no
      romper to_bytes() de los Raw*.
    """
    out = []
    for lr in left_rows:
        k = norm(_get_attr_or_key(lr, left_key))
        rr = right_index.get(k)
        if rr is None:
            out.append(lr)
            continue

        if isinstance(lr, dict):
            merged = dict(lr)
            if not isinstance(rr, dict):
                rr = rr.__dict__
            for rk, rv in rr.items():
                merged[f"{right_prefix}.{rk}"] = rv
            out.append(merged)
        else:
            out.append(lr)
    return out


def metadata_only(db: DataBatch) -> None:
    if getattr(db, "batch_msg", None) and hasattr(db.batch_msg, "rows"):
        db.batch_msg.rows = []


def queries_set(db: DataBatch) -> set[int]:
    return set(getattr(db, "query_ids", []) or [])


class JoinerWorker:
    """
    Fases controladas por cuáles colas están en consumo:
      - Inicio: sólo menu_items y stores (tablas livianas).
      - Al TABLE_EOF(menu_items) y TABLE_EOF(stores) → consumir transaction_items.
      - Al TABLE_EOF(transaction_items) → consumir transactions.
      - Al TABLE_EOF(transactions) → consumir users.
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

        self._cache_stores: Optional[Dict[str, Any]] = None
        self._cache_menu: Optional[Dict[str, Any]] = None

        self._eof: Set[int] = set()
        self._lock = threading.Lock()
        self._threads: Dict[int, threading.Thread] = {}
        self._log = logger
        self._shard = int(shard_index)

    def run(self):
        if self._log:
            self._log.info(
                "Arrancando consumo de livianas: menu_items, stores (shard=%d)",
                self._shard,
            )
        self._start_queue(Opcodes.NEW_MENU_ITEMS, self._on_raw_menu)
        self._start_queue(Opcodes.NEW_STORES, self._on_raw_stores)
        threading.Event().wait()

    def _start_queue(self, table_id: int, cb: Callable[[bytes], None]):
        mw = self._in.get(table_id)
        if not mw:
            if self._log:
                self._log.debug("No hay MW para table_id=%s; no se inicia", table_id)
            return
        if table_id in self._threads and self._threads[table_id].is_alive():
            return
        t = threading.Thread(target=mw.start_consuming, args=(cb,), daemon=True)
        self._threads[table_id] = t
        t.start()
        if self._log:
            self._log.info("Consumiendo table_id=%s", table_id)

    def _stop_queue(self, table_id: int):
        mw = self._in.get(table_id)
        if not mw:
            return
        try:
            mw.stop_consuming()
            if self._log:
                self._log.info("Detenida cola table_id=%s", table_id)
        except Exception:
            pass

    def _decode_msg(
        self, body: bytes
    ) -> Tuple[str, Union[EOFMessage, DataBatch, int, None]]:
        if not body or len(body) < 1:
            if self._log:
                self._log.error("Mensaje vacío")
            return "err", None

        opcode = body[0]

        if opcode == Opcodes.EOF:
            try:
                eof = EOFMessage.deserialize_from_bytes(body)
                return "eof", eof
            except Exception as e:
                if self._log:
                    self._log.error(f"EOF inválido: {e}")
                return "err", None

        if opcode == Opcodes.DATA_BATCH:
            try:
                db = DataBatch.deserialize_from_bytes(body)
                return "db", db
            except Exception as e:
                if self._log:
                    self._log.error(f"DataBatch inválido: {e}")
                return "err", None

        if self._log:
            self._log.warning(f"Opcode no deseado: {opcode}")
        return "bad", opcode

    def _on_raw_menu(self, raw: bytes):
        kind, msg = self._decode_msg(raw)
        if kind == "eof":
            eof: EOFMessage = msg
            self._on_table_eof(int(eof.table_id))
            self._stop_queue(Opcodes.NEW_MENU_ITEMS)
            self._maybe_enable_ti_phase()
            return
        if kind != "db":
            return

        db: DataBatch = msg
        rows = (db.batch_msg.rows or []) if getattr(db, "batch_msg", None) else []
        idx = rows_to_index(rows, "product_id")
        self._store.put("menu_items", "full", idx)
        self._cache_menu = idx

        if self._shard == 0:
            meta = copy.deepcopy(db)
            metadata_only(meta)
            self._send(meta)

    def _on_raw_stores(self, raw: bytes):
        kind, msg = self._decode_msg(raw)
        if kind == "eof":
            eof: EOFMessage = msg
            self._on_table_eof(eof.table_id)
            self._stop_queue(Opcodes.NEW_STORES)
            self._maybe_enable_ti_phase()
            return
        if kind != "db":
            return

        db: DataBatch = msg
        rows = (db.batch_msg.rows or []) if getattr(db, "batch_msg", None) else []
        idx = rows_to_index(rows, "store_id")
        self._store.put("stores", "full", idx)
        self._cache_stores = idx

        if self._shard == 0:
            meta = copy.deepcopy(db)
            metadata_only(meta)
            self._send(meta)

    def _on_raw_ti(self, raw: bytes):
        kind, msg = self._decode_msg(raw)
        if kind == "eof":
            eof: EOFMessage = msg
            self._on_table_eof(eof.table_id)
            self._stop_queue(Opcodes.NEW_TRANSACTION_ITEMS)
            self._maybe_enable_tx_phase()
            return
        if kind != "db":
            return

        db: DataBatch = msg
        if Q2 not in queries_set(db):
            self._send(db)
            return

        menu = self._cache_menu or self._store.get("menu_items", "full", default=None)
        if not menu:
            if self._log:
                self._log.debug("Menu cache no disponible aún; drop batch TI")
            return

        rows = (db.batch_msg.rows or []) if getattr(db, "batch_msg", None) else []
        joined = join_embed_rows(rows, menu, "item_id", "menu")
        if getattr(db, "batch_msg", None) and hasattr(db.batch_msg, "rows"):
            db.batch_msg.rows = joined
        db.batch_bytes = db.batch_msg.to_bytes()
        self._send(db)

    def _on_raw_tx(self, raw: bytes):
        kind, msg = self._decode_msg(raw)
        if kind == "eof":
            eof: EOFMessage = msg
            self._on_table_eof(eof.table_id)
            self._stop_queue(Opcodes.NEW_TRANSACTION)
            self._maybe_enable_u_phase()
            return
        if kind != "db":
            return

        db: DataBatch = msg
        qset = queries_set(db)

        if qset == {Q1}:
            self._send(db)
            return

        stores = self._cache_stores or self._store.get("stores", "full", default=None)
        if not stores:
            if self._log:
                self._log.debug("Stores cache no disponible aún; drop batch TX")
            return

        rows = (db.batch_msg.rows or []) if getattr(db, "batch_msg", None) else []
        joined_tx_st = join_embed_rows(rows, stores, "store_id", "store")

        if Q3 in qset:
            if getattr(db, "batch_msg", None) and hasattr(db.batch_msg, "rows"):
                db.batch_msg.rows = joined_tx_st
            db.batch_bytes = db.batch_msg.to_bytes()
            self._send(db)
            return

        if Q4 in qset:
            template = copy.deepcopy(db)
            metadata_only(template)
            template_raw = template.serialize_to_bytes()

            by_user: Dict[str, List[Any]] = defaultdict(list)
            for r in joined_tx_st:
                uid = norm(_get_attr_or_key(r, "user_id"))
                by_user[uid].append(r)

            total = len(by_user) if by_user else 1
            for uid, lst in by_user.items():
                item = {"template_raw": template_raw, "rows": lst, "total": total}
                self._store.append_list("q4_by_user", uid, item)
            return

        self._send(db)

    def _on_raw_users(self, raw: bytes):
        kind, msg = self._decode_msg(raw)
        if kind == "eof":
            eof: EOFMessage = msg
            self._on_table_eof(eof.table_id)
            self._stop_queue(Opcodes.NEW_USERS)
            self._flush_remaining_q4_without_user()
            return
        if kind != "db":
            return

        db: DataBatch = msg
        rows = (db.batch_msg.rows or []) if getattr(db, "batch_msg", None) else []

        if Q4 in queries_set(db):
            for u in rows:
                uid = norm(_get_attr_or_key(u, "user_id"))
                items = self._store.pop_all("q4_by_user", uid)
                if not items:
                    continue
                usr_idx = {uid: u}

                for i, it in enumerate(items):
                    template_raw: bytes = it["template_raw"]
                    tx_rows: List[Any] = it["rows"]
                    total: int = int(it.get("total", len(items)))

                    joined = join_embed_rows(tx_rows, usr_idx, "user_id", "user")
                    out_db = DataBatch.deserialize_from_bytes(template_raw)

                    if getattr(out_db, "batch_msg", None) and hasattr(
                        out_db.batch_msg, "rows"
                    ):
                        out_db.batch_msg.rows = joined

                    out_db.batch_bytes = out_db.batch_msg.to_bytes()
                    self._send(out_db)

        meta = copy.deepcopy(db)
        metadata_only(meta)
        self._send(meta)

    def _on_table_eof(self, table_id: int):
        with self._lock:
            self._eof.add(int(table_id))

    def _maybe_enable_ti_phase(self):
        if (Opcodes.NEW_MENU_ITEMS in self._eof) and (Opcodes.NEW_STORES in self._eof):
            self._start_queue(Opcodes.NEW_TRANSACTION_ITEMS, self._on_raw_ti)
            if self._log:
                self._log.info("Activada fase TI")

    def _maybe_enable_tx_phase(self):
        if Opcodes.NEW_TRANSACTION_ITEMS in self._eof:
            self._start_queue(Opcodes.NEW_TRANSACTION, self._on_raw_tx)
            if self._log:
                self._log.info("Activada fase TX")

    def _maybe_enable_u_phase(self):
        if Opcodes.NEW_TRANSACTION in self._eof:
            self._start_queue(Opcodes.NEW_USERS, self._on_raw_users)
            if self._log:
                self._log.info("Activada fase USERS")

    def _flush_remaining_q4_without_user(self):
        uids = self._store.keys_with_prefix("q4_by_user")
        for uid in uids:
            items = self._store.pop_all("q4_by_user", uid)
            for it in items:
                template_raw: bytes = it["template_raw"]
                tx_rows: List[Any] = it["rows"]
                out_db = DataBatch.deserialize_from_bytes(template_raw)
                if getattr(out_db, "batch_msg", None) and hasattr(
                    out_db.batch_msg, "rows"
                ):
                    out_db.batch_msg.rows = tx_rows
                out_db.batch_bytes = out_db.batch_msg.to_bytes()
                self._send(out_db)

    def _send(self, db: DataBatch):
        raw = db.serialize_to_bytes()
        self._out.send(raw)
