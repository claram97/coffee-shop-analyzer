from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

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
    def __init__(
        self,
        in_mw,
        out_router_mw,
        filters: FilterRegistry,
    ):
        self._in = in_mw
        self._out = out_router_mw
        self._filters = filters

    def run(self) -> None:
        self._in.start_consuming(self._on_raw)

    def _on_raw(self, raw: bytes) -> None:
        db = databatch_from_bytes(raw)

        if not db.table_ids:
            self._out.send(raw)
            return
        table_id = int(db.payload.table_id)

        step = current_step_from_mask(int(db.reserved_u16))
        if step is None:
            self._out.send(raw)
            return

        inner = db.batch_msg
        if inner is None or not hasattr(inner, "rows"):
            self._out.send(raw)
            return
        rows: List[Dict[str, Any]] = getattr(inner, "rows")

        queries = getattr(db, "query_ids", []) or []
        key = (table_id, step, qkey(queries))

        fn = self._filters.get(key)
        if fn is None:
            self._out.send(raw)
            return

        try:
            new_rows = fn(rows) or []
        except Exception:
            self._out.send(raw)
            return

        inner.rows = new_rows
        out_raw = databatch_to_bytes(db)
        self._out.send(out_raw)
