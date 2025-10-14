from __future__ import annotations

import logging
import os
import time
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple

from middleware.middleware_client import (
    MessageMiddlewareExchange,
    MessageMiddlewareQueue,
)
from protocol.constants import Opcodes
from protocol.databatch import DataBatch

LOGGER_NAME = "filter_worker"
logger = logging.getLogger(LOGGER_NAME)


def _setup_logging() -> None:
    """Configura logging sólo si el root logger no tiene handlers."""
    if logging.getLogger().handlers:
        return
    level_name = os.getenv("FILTER_WORKER_LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


_setup_logging()


def current_step_from_mask(mask: int) -> Optional[int]:
    if mask == 0:
        return None
    i = 0
    while (mask & 1) == 1:
        mask >>= 1
        i += 1
    return i - 1


def _parse_dt_local(s: str) -> Optional[datetime]:
    try:
        return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
    except Exception as e:
        logger.error("error in hour_filter: %s", e)
        return None


def hour_filter(rows) -> List[Any]:
    kept = []
    for r in rows:
        ts = r.created_at
        dt = _parse_dt_local(ts)
        if not dt:
            continue
        if 6 <= dt.hour <= 23:
            kept.append(r)
    return kept


def final_amount_filter(rows) -> List[Any]:
    kept = []
    for r in rows:
        final_amount_raw = getattr(r, "final_amount", None)
        if final_amount_raw is None:
            continue
        try:
            amount = float(str(final_amount_raw).strip())
        except Exception as e:
            logger.error("error in hour_filter: %s", e)
            continue
        if amount >= 75.0:
            kept.append(r)
    return kept


def year_filter(rows, min_year: int = 2024, max_year: int = 2025) -> List[Any]:
    kept = []
    for r in rows:
        ts = r.created_at
        try:
            y = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S").year
        except Exception as e:
            logger.error("exception while applying year_filter: %s", e)
            continue
        if min_year <= y <= max_year:
            kept.append(r)
    return kept


FilterFn = Callable[[List[Any]], List[Any]]
FilterRegistry = Dict[Tuple[int, int, str], FilterFn]


def qkey(queries: List[int]) -> str:
    return ",".join(str(q) for q in sorted(set(int(x) for x in queries)))


REGISTRY: FilterRegistry = {}
REGISTRY[(Opcodes.NEW_TRANSACTION, 0, qkey([1, 3, 4]))] = year_filter
REGISTRY[(Opcodes.NEW_TRANSACTION, 1, qkey([1, 3]))] = hour_filter
REGISTRY[(Opcodes.NEW_TRANSACTION, 2, qkey([1]))] = final_amount_filter
REGISTRY[(Opcodes.NEW_TRANSACTION_ITEMS, 0, qkey([2]))] = year_filter


class FilterWorker:
    """
    Consume de una cola (Filter Workers pool), aplica el filtro adecuado
    y publica el batch al **exchange** del Filter Router usando la RK calculada
    a partir de (batch_number % num_routers).
    """

    def __init__(
        self,
        host: str,
        in_queue: str,
        out_router_exchange: str,
        out_router_rk_fmt: str,
        num_routers: int,
        stop_event: threading.Event,
        filters: FilterRegistry | None = None,
    ):
        self._host = host
        self._in = MessageMiddlewareQueue(host, in_queue)
        self._filters = filters or REGISTRY

        self._out_ex = out_router_exchange
        self._rk_fmt = out_router_rk_fmt
        self._num_routers = max(1, int(num_routers))

        self._pub_cache: Dict[str, MessageMiddlewareExchange] = {}

        self._stop_event = stop_event

        logger.info(
            "FilterWorker inicializado | host=%s in_queue=%s out_exchange=%s rk_fmt=%s num_routers=%d",
            host,
            in_queue,
            out_router_exchange,
            out_router_rk_fmt,
            self._num_routers,
        )

    def shutdown(self):
        """Stops the consumer and closes all publisher connections."""
        logger.info("Shutting down FilterWorker...")

        try:
            self._in.stop_consuming()
            logger.info("Input consumer stopped.")
        except Exception as e:
            logger.warning(f"Error stopping input consumer: {e}")

        for rk, pub in self._pub_cache.items():
            try:
                pub.close()
            except Exception as e:
                logger.warning(f"Error closing publisher for rk='{rk}': {e}")

        self._pub_cache.clear()
        logger.info("FilterWorker shutdown complete.")

    def _publisher_for_rk(self, rk: str) -> MessageMiddlewareExchange:
        pub = self._pub_cache.get(rk)
        if pub is None:
            logger.info("Creando publisher a exchange=%s rk=%s", self._out_ex, rk)
            pub = MessageMiddlewareExchange(
                host=self._host,
                exchange_name=self._out_ex,
                route_keys=[rk],
            )
            self._pub_cache[rk] = pub
        return pub

    def _send_to_router(self, raw: bytes, batch_number: int | None) -> None:
        pid = 0 if batch_number is None else int(batch_number) % self._num_routers
        rk = self._rk_fmt.format(pid=pid)
        pub = self._publisher_for_rk(rk)
        pub.send(raw)

    def run(self) -> None:
        logger.info("Comenzando consumo de mensajes…")
        self._in.start_consuming(self._on_raw)

    def _on_raw(self, raw: bytes) -> None:
        if self._stop_event.is_set():
            logger.warning("Shutdown in progress, skipping message.")
            return
        t0 = time.perf_counter()
        try:
            db = DataBatch.deserialize_from_bytes(raw)
        except Exception:
            logger.exception("Fallo deserializando DataBatch; reenviando sin cambios")
            self._send_to_router(raw, batch_number=None)
            return

        bn = int(getattr(db, "batch_number", 0) or 0)

        if not getattr(db, "batch_msg", None):
            logger.warning("Batch sin batch_msg: reenvío sin cambios")
            self._send_to_router(raw, bn)
            return
        try:
            table_id = int(db.batch_msg.opcode)
        except Exception:
            logger.warning("table_id inválido; reenvío sin cambios")
            self._send_to_router(raw, bn)
            return

        step_mask = int(getattr(db, "reserved_u16", 0) or 0)
        step = current_step_from_mask(step_mask)
        if step is None:
            logger.debug(
                "Sin step activo en máscara: reenvío sin cambios | table_id=%s mask=%s",
                table_id,
                step_mask,
            )
            self._send_to_router(raw, bn)
            return

        inner = getattr(db, "batch_msg", None)
        if inner is None or not hasattr(inner, "rows"):
            logger.warning(
                "Batch sin 'rows': reenvío sin cambios | table_id=%s step=%s",
                table_id,
                step,
            )
            self._send_to_router(raw, bn)
            return
        rows = getattr(inner, "rows") or []
        in_rows = len(rows)
        if inner.opcode == Opcodes.NEW_TRANSACTION:
            try:
                if in_rows > 0:
                    sample_rows = rows[:2]
                    all_keys = set()
                    for row in sample_rows:
                        all_keys.update(row.__dict__.keys())

                    sample_data = [row.__dict__ for row in sample_rows]

                    logging.debug(
                        "action: batch_preview | batch_number: %d | opcode: %d | keys: %s | sample_count: %d | sample: %s",
                        getattr(inner, "batch_number", 0),
                        inner.opcode,
                        sorted(list(all_keys)),
                        len(sample_rows),
                        sample_data,
                    )
            except Exception as e:
                logging.exception(
                    "action: batch_preview | batch_number: %d | result: skip | exception: %s",
                    getattr(inner, "batch_number", 0),
                    e,
                )
        queries = list(getattr(db, "query_ids", []) or [])
        queries_key = qkey(queries)
        key = (table_id, step, queries_key)

        fn = self._filters.get(key)
        if fn is None:
            logger.warning(
                "No hay filtro registrado para queries=%s: reenvío sin cambios | table_id=%s step=%s rows=%d",
                queries_key,
                table_id,
                step,
                in_rows,
            )
            self._send_to_router(raw, bn)
            return

        try:
            new_rows = fn(rows) or []
            out_rows = len(new_rows)
            inner.rows = new_rows
            db.batch_bytes = inner.to_bytes()
            out_raw = db.to_bytes()
            self._send_to_router(out_raw, bn)

            dt_ms = (time.perf_counter() - t0) * 1000
            if table_id == Opcodes.NEW_TRANSACTION:
                logger.debug(
                    "Filtro aplicado y publicado | table_id=%s step=%s queries=%s rows_in=%d rows_out=%d latency_ms=%.2f filter=%s pid=%02d",
                    table_id,
                    step,
                    queries_key,
                    in_rows,
                    out_rows,
                    dt_ms,
                    getattr(fn, "__name__", "unknown"),
                    (bn % self._num_routers),
                )
        except Exception:
            logger.exception(
                "Error aplicando filtro; reenvío sin cambios | table_id=%s step=%s queries=%s rows_in=%d filter=%s",
                table_id,
                step,
                queries_key,
                in_rows,
                getattr(fn, "__name__", "unknown"),
            )
            self._send_to_router(raw, bn)
