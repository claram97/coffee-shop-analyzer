from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo
from protocol.databatch import DataBatch

from middleware.middleware_client import MessageMiddlewareQueue

MYT_TZ = ZoneInfo("Asia/Kuala_Lumpur")


# ---------- step desde bitmask ----------
def current_step_from_mask(mask: int) -> Optional[int]:
    if mask == 0:
        return None
    i = 0
    while (mask & 1) == 1:
        mask >>= 1
        i += 1
    return i - 1


# ---------- filtros atómicos ----------
def _parse_dt_utc(s: str) -> Optional[datetime]:
    try:
        return datetime.strptime(s, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    except Exception:
        return None


def hour_filter(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    kept = []
    for r in rows:
        ts = r.get("created_at")
        dt = _parse_dt_utc(ts)
        if not dt:
            continue
        if 6 <= dt.astimezone(MYT_TZ).hour <= 23:
            kept.append(r)
    return kept


def final_amount_filter(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    kept = []
    for r in rows:
        try:
            if float(r.get("final_amount", 0)) >= 75.0:
                kept.append(r)
        except Exception:
            continue
    return kept


def year_filter(
    rows: List[Dict[str, Any]], min_year: int = 2024, max_year: int = 2025
) -> List[Dict[str, Any]]:
    kept = []
    for r in rows:
        ts = r.get("created_at")
        try:
            y = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S").year
        except Exception:
            continue
        if min_year <= y <= max_year:
            kept.append(r)
    return kept


# ---------- registro ----------
FilterFn = Callable[[List[Dict[str, Any]]], List[Dict[str, Any]]]
FilterRegistry = Dict[Tuple[int, int, str], FilterFn]

TRANSACTIONS = 0
USERS = 1
TRANSACTION_ITEMS = 2


def qkey(queries: List[int]) -> str:
    return ",".join(str(q) for q in sorted(set(int(x) for x in queries)))


REGISTRY: FilterRegistry = {}
REGISTRY[(TRANSACTIONS, 0, qkey([1, 3, 4]))] = year_filter
REGISTRY[(TRANSACTIONS, 1, qkey([1, 3]))] = hour_filter
REGISTRY[(TRANSACTIONS, 2, qkey([1]))] = final_amount_filter
REGISTRY[(TRANSACTION_ITEMS, 0, qkey([2]))] = year_filter


# ---------- worker ----------
class FilterWorker:
    """
    Consume de una cola (Filter Workers pool), aplica el filtro adecuado según (table_id, step, queries)
    y reenvía el batch al Filter Router.
    """

    def __init__(
        self,
        host: str,
        in_queue: str,
        out_router_queue: str,
        filters: FilterRegistry | None = None,
    ):
        self._in = MessageMiddlewareQueue(host, in_queue)
        self._out = MessageMiddlewareQueue(host, out_router_queue)
        self._filters = filters or REGISTRY

    def run(self) -> None:
        self._in.start_consuming(self._on_raw)

    def _on_raw(self, raw: bytes) -> None:
        db = DataBatch.deserialize_from_bytes(raw)

        # Tabla principal
        if not getattr(db, "table_ids", None):
            self._out.send(raw)
            return
        try:
            table_id = int(db.table_ids[0])
        except Exception:
            self._out.send(raw)
            return

        # Step actual desde la máscara (u16 contiguous-ones)
        step = current_step_from_mask(int(getattr(db, "reserved_u16", 0)))
        if step is None:
            self._out.send(raw)
            return

        # Filas
        inner = getattr(db, "batch_msg", None)
        if inner is None or not hasattr(inner, "rows"):
            self._out.send(raw)
            return
        rows: List[Dict[str, Any]] = getattr(inner, "rows") or []

        # Queries → key
        queries = list(getattr(db, "query_ids", []) or [])
        key = (table_id, step, qkey(queries))

        # Resolver filtro
        fn = self._filters.get(key)
        if fn is None:
            self._out.send(raw)
            return

        # Aplicar filtro (fail-closed: si falla, reenviamos sin cambios)
        try:
            new_rows = fn(rows) or []
        except Exception:
            self._out.send(raw)
            return

        inner.rows = new_rows
        out_raw = db.to_bytes()
        self._out.send(out_raw)
