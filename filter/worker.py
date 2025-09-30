from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from middleware.middleware_client import MessageMiddlewareQueue
from protocol.constants import Opcodes
from protocol.databatch import DataBatch

# ---------------- Logging ----------------
LOGGER_NAME = "filter_worker"
logger = logging.getLogger(LOGGER_NAME)


def _setup_logging() -> None:
    """Configura logging sólo si el root logger no tiene handlers."""
    if logging.getLogger().handlers:
        return  # Respeta configuración existente (p. ej., si te lo setea Gunicorn)
    level_name = os.getenv("FILTER_WORKER_LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


_setup_logging()

# -------------- Constantes / Utiles --------------
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


def hour_filter(rows) -> List[Dict[str, Any]]:
    kept = []
    for r in rows:
        ts = r.created_at
        dt = _parse_dt_utc(ts)
        if not dt:
            continue
        if 6 <= dt.astimezone(MYT_TZ).hour <= 23:
            kept.append(r)
    return kept


def final_amount_filter(rows) -> List[Dict[str, Any]]:
    kept = []
    for r in rows:
        try:
            if float(r.final_amount, 0) >= 75.0:
                kept.append(r)
        except Exception:
            continue
    return kept


def year_filter(
    rows, min_year: int = 2024, max_year: int = 2025
) -> List[Dict[str, Any]]:
    kept = []
    for r in rows:
        ts = r.created_at
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


def qkey(queries: List[int]) -> str:
    return ",".join(str(q) for q in sorted(set(int(x) for x in queries)))


REGISTRY: FilterRegistry = {}
REGISTRY[(Opcodes.NEW_TRANSACTION, 0, qkey([1, 3, 4]))] = year_filter
REGISTRY[(Opcodes.NEW_TRANSACTION, 1, qkey([1, 3]))] = hour_filter
REGISTRY[(Opcodes.NEW_TRANSACTION, 2, qkey([1]))] = final_amount_filter
REGISTRY[(Opcodes.NEW_TRANSACTION_ITEMS, 0, qkey([2]))] = year_filter


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
        logger.info(
            "FilterWorker inicializado",
            extra={"host": host, "in_queue": in_queue, "out_queue": out_router_queue},
        )

    def run(self) -> None:
        logger.info("Comenzando consumo de mensajes…")
        self._in.start_consuming(self._on_raw)

    def _on_raw(self, raw: bytes) -> None:
        t0 = time.perf_counter()
        try:
            db = DataBatch.deserialize_from_bytes(raw)
        except Exception:
            logger.exception("Fallo deserializando DataBatch; reenviando sin cambios")
            self._out.send(raw)
            return

        if not getattr(db, "batch_msg", None):
            logger.debug("Batch sin table_ids: reenvío sin cambios")
            self._out.send(raw)
            return
        try:
            table_id = int(db.batch_msg.opcode)
        except Exception:
            logger.warning("table_id inválido; reenvío sin cambios")
            self._out.send(raw)
            return

        # Step actual desde la máscara (u16 contiguous-ones)
        step_mask = int(getattr(db, "reserved_u16", 0) or 0)
        step = current_step_from_mask(step_mask)
        if step is None:
            logger.debug(
                "Sin step activo en máscara: reenvío sin cambios",
                extra={"table_id": table_id, "mask": step_mask},
            )
            self._out.send(raw)
            return

        # Filas
        inner = getattr(db, "batch_msg", None)
        if inner is None or not hasattr(inner, "rows"):
            logger.warning(
                "Batch sin 'rows': reenvío sin cambios",
                extra={"table_id": table_id, "step": step},
            )
            self._out.send(raw)
            return
        rows: List[Dict[str, Any]] = getattr(inner, "rows") or []
        in_rows = len(rows)

        # Queries → key
        queries = list(getattr(db, "query_ids", []) or [])
        queries_key = qkey(queries)
        key = (table_id, step, queries_key)

        # Resolver filtro
        fn = self._filters.get(key)
        if fn is None:
            logger.info(
                "No hay filtro registrado para queries %s: reenvío sin cambios",
                queries_key,
                extra={
                    "table_id": table_id,
                    "step": step,
                    "queries": queries_key,
                    "rows": in_rows,
                },
            )
            self._out.send(raw)
            return

        # Aplicar filtro (fail-closed: si falla, reenviamos sin cambios)
        try:
            new_rows = fn(rows) or []
            out_rows = len(new_rows)
            inner.rows = new_rows
            db.batch_bytes = inner.to_bytes()
            out_raw = db.to_bytes()
            self._out.send(out_raw)
            dt_ms = (time.perf_counter() - t0) * 1000
            logger.info(
                "Filtro aplicado y reenviado",
                extra={
                    "table_id": table_id,
                    "step": step,
                    "queries": queries_key,
                    "rows_in": in_rows,
                    "rows_out": out_rows,
                    "latency_ms": round(dt_ms, 2),
                    "filter_name": getattr(fn, "__name__", "unknown"),
                },
            )
        except Exception:
            logger.exception(
                "Error aplicando filtro; reenvío sin cambios",
                extra={
                    "table_id": table_id,
                    "step": step,
                    "queries": queries_key,
                    "rows_in": in_rows,
                    "filter_name": getattr(fn, "__name__", "unknown"),
                },
            )
            self._out.send(raw)
