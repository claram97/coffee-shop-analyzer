ahora?

# joiner/worker.py
from __future__ import annotations

import copy
import hashlib
import io
import logging
import os
import pickle
import shelve
import struct
import threading
from collections import defaultdict
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

from middleware.middleware_client import MessageMiddleware
from protocol.constants import Opcodes
from protocol.databatch import DataBatch
from protocol.entities import (RawMenuItems, RawStore, RawTransaction,
                               RawTransactionItem, RawTransactionItemMenuItem,
                               RawTransactionStore, RawTransactionStoreUser,
                               RawUser)
from protocol.messages import (EOFMessage, NewTransactionItemsMenuItems,
                               NewTransactionStores, NewTransactionStoresUsers)

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


class _Shard:
    __slots__ = ("path", "wlock", "rlock", "wfh", "rfh", "index")

    def __init__(self, path: str):
        self.path = path
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self.wfh = open(path, "ab", buffering=1024 * 1024)
        self.rfh = open(path, "rb", buffering=1024 * 1024)
        self.wlock = threading.Lock()
        self.rlock = threading.Lock()
        self.index: Dict[str, List[_RecordPtr]] = defaultdict(list)

    def close(self):
        try:
            self.wfh.close()
        except:
            pass
        try:
            self.rfh.close()
        except:
            pass


class FastSpool:
    """
    Append-only binario shardeado por uid, client_id y run_id.
    """

    def __init__(self, root_dir: str, shards: int = 64):
        self._root = root_dir
        self._shards = max(1, int(shards))
        self._table = [
            _Shard(os.path.join(root_dir, f"spool-{i:02d}.dat"))
            for i in range(self._shards)
        ]
        self._keys_lock = threading.Lock()
        self._keys: Dict[int, set[str]] = {i: set() for i in range(self._shards)}
        self._hdr = struct.Struct("<I")

    def _shard_of(self, key: str) -> int:
        h = hashlib.blake2b(key.encode("utf-8"), digest_size=2).digest()
        return int.from_bytes(h, "little") % self._shards

    def append_list(self, bucket: str, key: str, value_item: Any) -> None:
        payload = pickle.dumps(value_item, protocol=5)
        hdr = self._hdr.pack(len(payload))
        sid = self._shard_of(key)
        shard = self._table[sid]
        with shard.wlock:
            off = shard.wfh.tell()
            shard.wfh.write(hdr)
            shard.wfh.write(payload)
            shard.wfh.flush()
            shard.index[key].append((off, len(hdr) + len(payload)))
        with self._keys_lock:
            self._keys[sid].add(key)

    def pop_all(self, bucket: str, key: str) -> List[Any]:
        sid = self._shard_of(key)
        shard = self._table[sid]
        with shard.wlock:
            ptrs = shard.index.pop(key, None)
            if not ptrs:
                with self._keys_lock:
                    self._keys[sid].discard(key)
                return []
        out: List[Any] = []
        with shard.rlock:
            for off, length in ptrs:
                shard.rfh.seek(off)
                size = self._hdr.unpack(shard.rfh.read(self._hdr.size))[0]
                buf = shard.rfh.read(size)
                out.append(pickle.loads(buf))
        with self._keys_lock:
            self._keys[sid].discard(key)
        return out

    def keys_with_prefix(self, bucket: str) -> List[str]:
        with self._keys_lock:
            keys = []
            for s in range(self._shards):
                if self._keys[s]:
                    keys.extend(self._keys[s])
            return keys

    def close(self):
        for s in self._table:
            s.close()


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
        data_dir: str = "./data/joiner",
        logger=None,
        shard_index: int = 0,
    ):
        self._in = in_mw
        self._out = out_results_mw
        self._store = FastSpool(os.path.join(data_dir, "joiner.shelve"), shards=64)

        self._cache_stores: Dict[Tuple[str,str], Dict[str, RawStore]] = {}
        self._cache_menu: Dict[Tuple[str,str], Dict[str, RawMenuItems]] = {}

        self._eof: Set[Tuple[int, str, str]] = set()
        self._lock = threading.Lock()
        self._threads: Dict[int, threading.Thread] = {}
        self._log = logger or log
        self._shard = int(shard_index)
        self._router_replicas = router_replicas

        self._pending_eofs: Dict[tuple[int, str, str], Set[int]] = {}
        self._part_counter: Dict[tuple[int, str, str], int] = {}

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

    def _phase_ready(self, table_id: int, client_id: str, run_id: str) -> bool:
        need = []
        if table_id == Opcodes.NEW_TRANSACTION_ITEMS:
            need = [Opcodes.NEW_MENU_ITEMS, Opcodes.NEW_STORES]
        elif table_id == Opcodes.NEW_TRANSACTION:
            need = [Opcodes.NEW_MENU_ITEMS, Opcodes.NEW_STORES]
        elif table_id == Opcodes.NEW_USERS:
            need = [Opcodes.NEW_TRANSACTION]
        else:
            need = []
        return all((t, client_id, run_id) in self._eof for t in need)

    def _requeue(self, table_id: int, raw: bytes):
        mw = self._in.get(table_id)
        try:
            mw.send(raw)
            self._log.debug("requeue → same queue table_id=%s bytes=%d", table_id, len(raw))
        except Exception as e:
            self._log.error("requeue failed table_id=%s: %s", table_id, e)

    def _on_raw_menu(self, raw: bytes):
        kind, msg = self._decode_msg(raw)
        if kind == "eof":
            eof: EOFMessage = msg
            self._on_table_eof(
                _eof_table_id(eof),
                getattr(eof, "client_id", ""),
                getattr(eof, "run_id", "")
            )
            return
        if kind != "db":
            return
        db: DataBatch = msg
        cid = getattr(db, "client_id", "")
        rid = getattr(db, "run_id", "")
        self._log.debug(
            "IN: menu_items batch_number=%s shard=%s shards_info=%s queries=%s cid=%s rid=%s",
            getattr(db, "batch_number", "?") if db else "?",
            getattr(self, "_shard", "?"),
            getattr(db, "shards_info", "[]") if db else "?",
            list(getattr(db, "query_ids", []) or []) if db else [],
            cid, rid
        )
        rows: List[RawMenuItems] = (
            (db.batch_msg.rows or []) if getattr(db, "batch_msg", None) else []
        )
        idx = index_by_attr(rows, "product_id")
        self._cache_menu[(cid, rid)] = idx
        self._log.info("Cache menu_items idx_size=%d", len(idx))

    def _on_raw_stores(self, raw: bytes):
        kind, msg = self._decode_msg(raw)
        if kind == "eof":
            eof: EOFMessage = msg
            self._on_table_eof(
                _eof_table_id(eof),
                getattr(eof, "client_id", ""),
                getattr(eof, "run_id", "")
            )
            return
        if kind != "db":
            return
        db: DataBatch = msg
        cid = getattr(db, "client_id", "")
        rid = getattr(db, "run_id", "")
        self._log.debug(
            "IN: stores batch_number=%s shard=%s shards_info=%s queries=%s cid=%s rid=%s",
            getattr(db, "batch_number", "?") if db else "?",
            getattr(self, "_shard", "?"),
            getattr(db, "shards_info", "[]") if db else "?",
            list(getattr(db, "query_ids", []) or []) if db else [],
            cid, rid
        )
        rows: List[RawStore] = (
            (db.batch_msg.rows or []) if getattr(db, "batch_msg", None) else []
        )
        idx = index_by_attr(rows, "store_id")
        self._cache_stores[(cid, rid)] = idx
        self._log.info("Cache stores idx_size=%d", len(idx))

    def _on_raw_ti(self, raw: bytes):
        kind, msg = self._decode_msg(raw)
        if kind == "eof":
            eof: EOFMessage = msg
            self._on_table_eof(
                _eof_table_id(eof),
                getattr(eof, "client_id", ""),
                getattr(eof, "run_id", "")
            )
            return
        if kind != "db":
            return
        db: DataBatch = msg
        cid = getattr(db, "client_id", "")
        rid = getattr(db, "run_id", "")

        self._log.debug(
            "IN: transaction_items batch_number=%s shard=%s shards_info=%s queries=%s cid=%s rid=%s",
            getattr(db, "batch_number", "?") if db else "?",
            getattr(self, "_shard", "?"),
            getattr(db, "shards_info", "[]") if db else "?",
            list(getattr(db, "query_ids", []) or []) if db else [],
            cid, rid
        )

        if not self._phase_ready(Opcodes.NEW_TRANSACTION_ITEMS, cid, rid):
            self._log.debug("TI fase NO lista → requeue (cid=%s rid=%s)", cid, rid)
            self._requeue(Opcodes.NEW_TRANSACTION_ITEMS, raw)
            return

        if Q2 not in queries_set(db):
            self._log.info("TI sin Q2 → passthrough")
            self._send(db)
            return

        menu_idx: Optional[Dict[str, RawMenuItems]] = self._cache_menu.get((cid, rid))
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
        if kind == "eof":
            eof: EOFMessage = msg
            self._on_table_eof(
                _eof_table_id(eof),
                getattr(eof, "client_id", ""),
                getattr(eof, "run_id", "")
            )
            return
        if kind != "db":
            return
        db: DataBatch = msg
        cid = getattr(db, "client_id", "")
        rid = getattr(db, "run_id", "")

        self._log.debug(
            "IN: transactions batch_number=%s shard=%s shards_info=%s queries=%s cid=%s rid=%s",
            getattr(db, "batch_number", "?") if db else "?",
            getattr(self, "_shard", "?"),
            getattr(db, "shards_info", "[]") if db else "?",
            list(getattr(db, "query_ids", []) or []) if db else [],
            cid, rid
        )

        if not self._phase_ready(Opcodes.NEW_TRANSACTION, cid, rid):
            self._log.debug("TX fase NO lista → requeue (cid=%s rid=%s)", cid, rid)
            self._requeue(Opcodes.NEW_TRANSACTION, raw)
            return

        qset = queries_set(db)
        tx_rows: List[RawTransaction] = (
            (db.batch_msg.rows or []) if getattr(db, "batch_msg", None) else []
        )

        if Q1 in qset:
            self._log.info("TX Q1 passthrough rows=%d", len(tx_rows))
            self._send(db)
            return

        stores_idx: Optional[Dict[str, RawStore]] = self._cache_stores.get((cid, rid))
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
            batch_status = getattr(db.batch_msg, "batch_status", 0)
            msg_join.batch_status = batch_status
            self._log.debug("Q3 preserving TX batch_status=%d", batch_status)
            db.table_ids = [Opcodes.NEW_TRANSACTION_STORES]
            db.batch_msg = msg_join
            self._send(db)
            return

        if Q4 in qset:
            if not joined_tx_st:
                self._log.info(
                    "Q4: no TX×Store rows → forward vacío al finisher como *_STORES_USERS"
                )
                empty = NewTransactionStoresUsers()
                empty.rows = []
                empty.batch_status = getattr(db.batch_msg, "batch_status", 0)
                db.table_ids = [Opcodes.NEW_TRANSACTION_STORES_USERS]
                db.batch_msg = empty
                self._send(db)
                return
            template = copy.copy(db)
            metadata_only(template)

            by_user: Dict[str, List[Any]] = defaultdict(list)
            for r in joined_tx_st:
                uid = norm(getattr(r, "user_id", ""))
                uid = uid.split(".", maxsplit=1)[0] if uid.endswith(".0") else uid
                by_user[uid].append(r)

            total_users = len(by_user) if by_user else 0
            self._log.info(
                "JOIN Q4 stage1: users=%d txxstore=%d", total_users, len(joined_tx_st)
            )

            total = total_users if total_users else 1
            total_u8 = min(total, 255)

            original_batch_status = getattr(db.batch_msg, "batch_status", 0)
            template.batch_msg.batch_status = original_batch_status
            self._log.debug(
                "Q4 template preserving TX batch_status=%d for later User join",
                original_batch_status,
            )

            for idx, (uid, lst) in enumerate(by_user.items()):
                idx_u8 = idx % 256
                template.meta[total_u8] = idx_u8
                template.batch_bytes = template.batch_msg.to_bytes()
                template_raw = template.to_bytes()
                key = f"{client_id}:{run_id}:{uid}"
                self._store.append_list("q4_by_user", key, (template_raw, lst))
                self._log.info(
                    "Q4 stash key=%s rows=%d chunk=%d/%d", key, len(lst), idx + 1, total
                )
            return

        self._send(db)

    def _on_raw_users(self, raw: bytes):
        kind, msg = self._decode_msg(raw)
        if kind == "eof":
            eof: EOFMessage = msg
            self._on_table_eof(
                _eof_table_id(eof),
                getattr(eof, "client_id", ""),
                getattr(eof, "run_id", "")
            )
            return
        if kind != "db":
            return
        db: DataBatch = msg
        cid = getattr(db, "client_id", "")
        rid = getattr(db, "run_id", "")

        self._log.debug(
            "IN: users batch_number=%s shard=%s shards_info=%s queries=%s cid=%s rid=%s",
            getattr(db, "batch_number", "?") if db else "?",
            getattr(self, "_shard", "?"),
            getattr(db, "shards_info", "[]") if db else "?",
            list(getattr(db, "query_ids", []) or []) if db else [],
            cid, rid
        )

        if not self._phase_ready(Opcodes.NEW_USERS, cid, rid):
            self._log.debug("U fase NO lista → requeue (cid=%s rid=%s)", cid, rid)
            self._requeue(Opcodes.NEW_USERS, raw)
            return

        users: List[RawUser] = (
            (db.batch_msg.rows or []) if getattr(db, "batch_msg", None) else []
        )

        if Q4 in queries_set(db):
            uidx = index_by_attr(users, "user_id")

            for uid, u in uidx.items():
                key = f"{cid}:{rid}:{uid}"
                items = self._store.pop_all("q4_by_user", key)
                if not items:
                    continue

                self._log.info(
                    "Q4 stage2 join key=%s pending_chunks=%d", key, len(items)
                )
                for template_raw, txst_rows in items:
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
                    batch_status = getattr(out_db.batch_msg, "batch_status", 0)
                    msg_join.batch_status = batch_status
                    self._log.debug(
                        "Q4 preserving TX batch_status=%d from template during User join",
                        batch_status,
                    )
                    out_db.table_ids = [Opcodes.NEW_TRANSACTION_STORES_USERS]
                    out_db.batch_msg = msg_join
                    self._log.info("Q4 out rows=%d for uid=%s cid=%s rid=%s", len(out_rows), uid, cid, rid)
                    self._send(out_db)

    def _on_table_eof(self, table_id: int, client_id: str, run_id: str) -> bool:
        key = (table_id, client_id, run_id)
        tname = ID_TO_NAME.get(table_id, f"#{table_id}")
        recvd = self._pending_eofs.setdefault(key, set())
        next_idx = self._part_counter.get(key, 0) + 1
        self._part_counter[key] = next_idx
        recvd.add(next_idx)

        log.info(
            "EOF recv table=%s cid=%s rid=%s progress=%d/%d",
            tname,
            client_id,
            run_id,
            len(recvd),
            self._router_replicas,
        )

        if len(recvd) >= self._router_replicas:
            self._pending_eofs.pop(key, None)
            self._part_counter.pop(key, None)
            self._eof.add(key)
            self._log.info(
                "EOF marcado table_id=%s cid=%s rid=%s; eof_set=%s", table_id, client_id, run_id, sorted(self._eof)
            )
            return True

        return False

    def _flush_remaining_q4_without_user(self, client_id, run_id):
        keys = self._store.keys_with_prefix("q4_by_user")
        if keys:
            self._log.info("Flush Q4 sin user: keys_pendientes=%d", len(keys))
        for key in keys:
            if not key.startswith(f"{client_id}:{run_id}"):
                continue
            for template_raw, txst_rows in self._store.pop_all("q4_by_user", key):
                out_db = DataBatch.deserialize_from_bytes(template_raw)
                msg_join = NewTransactionStoresUsers()
                msg_join.rows = []

                batch_status = getattr(out_db.batch_msg, "batch_status", 0)
                msg_join.batch_status = batch_status
                self._log.debug(
                    "Q4 (sin user) preserving TX batch_status=%d from template",
                    batch_status,
                )
                out_db.batch_msg = msg_join
                self._log.info("Q4 (sin user) out rows=%d key=%s", len(txst_rows), key)
                self._send(out_db)

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
            self._out.send(raw)
            self._log.debug(
                "SEND ok bytes=%d table=%s queries=%s",
                len(raw),
                getattr(db.batch_msg, "opcode", "?"),
                list(getattr(db, "query_ids", []) or []),
            )
        except Exception as e:
            self._log.error("SEND failed: %s", e)
