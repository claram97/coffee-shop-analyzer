from __future__ import annotations

import logging
import os
import threading
import time
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple

from middleware.middleware_client import (
    MessageMiddlewareExchange,
    MessageMiddlewareQueue,
)
from protocol2.databatch_pb2 import Query
from protocol2.envelope_pb2 import Envelope, MessageType
from protocol2.table_data_pb2 import Row, TableName
from protocol2.table_data_utils import iterate_rows_as_dicts

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


def _parse_dt_local(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except Exception as e:
        logger.error("error parsing datetime: %s", e)
        return None


def _parse_final_amount(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(str(value).strip())
    except Exception as e:
        logger.error("error in final_amount_filter: %s", e)
        return None


def hour_filter(td) -> List[Any]:
    return [
        r
        for r in iterate_rows_as_dicts(td)
        if (dt := _parse_dt_local(r["created_at"])) is not None and 6 <= dt.hour <= 23
    ]


def final_amount_filter(td) -> List[Any]:
    return [
        r
        for r in iterate_rows_as_dicts(td)
        if (amount := _parse_final_amount(r["final_amount"])) is not None
        and amount >= 75.0
    ]


def year_filter(td, min_year: int = 2024, max_year: int = 2025) -> List[Any]:
    return [
        r
        for r in iterate_rows_as_dicts(td)
        if (dt := _parse_dt_local(r["created_at"])) is not None
        and min_year <= dt.year <= max_year
    ]


FilterFn = Callable[[List[Any]], List[Any]]
# TableName is a protobuf EnumTypeWrapper (not a plain Python type). Using it
# directly inside typing.Tuple triggers a TypeError at import time because the
# typing machinery expects real types. Use `int` for the enum slot (the enum
# values are integers) so annotations are safe but still informative.
FilterRegistry = Dict[Tuple[int, int, str], FilterFn]

QUERY_TO_INT = {
    Query.Q1: 1,
    Query.Q2: 2,
    Query.Q3: 3,
    Query.Q4: 4,
}


def qkey(queries: List[Query]) -> str:
    return ",".join(str(q) for q in sorted(set(QUERY_TO_INT[x] for x in queries)))


REGISTRY: FilterRegistry = {}
REGISTRY[(TableName.TRANSACTIONS, 0, qkey([Query.Q1, Query.Q3, Query.Q4]))] = (
    year_filter
)
REGISTRY[(TableName.TRANSACTIONS, 1, qkey([Query.Q1, Query.Q3]))] = hour_filter
REGISTRY[(TableName.TRANSACTIONS, 2, qkey([Query.Q1]))] = final_amount_filter
REGISTRY[(TableName.TRANSACTION_ITEMS, 0, qkey([Query.Q2]))] = year_filter


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
        logger.debug("Enviando batch al router | batch_number=%s", batch_number)
        pid = 0 if batch_number is None else int(batch_number) % self._num_routers
        rk = self._rk_fmt.format(pid=pid)
        pub = self._publisher_for_rk(rk)
        pub.send(raw)

    def run(self) -> None:
        logger.info("Comenzando consumo de mensajes…")
        self._in.start_consuming(self._on_raw)

    def _on_raw(self, raw: bytes) -> None:
        logger.debug("Mensaje recibido, tamaño=%d bytes", len(raw))
        # Small hex preview to help identify message payload in logs (first 48 bytes)
        try:
            preview = raw[:48].hex()
            logger.debug("raw preview: %s", preview)
        except Exception:
            logger.debug("raw preview unavailable")
        if self._stop_event.is_set():
            logger.warning("Shutdown in progress, skipping message.")
            return
        t0 = time.perf_counter()
        env = Envelope()
        try:
            env.ParseFromString(raw)
        except Exception as e:
            logger.exception("Failed to parse Envelope from raw bytes: %s", e)
            # If parsing fails, requeue or send upstream unchanged so other components
            # (or legacy consumers) can handle it. Here we NACK/reattempt by re-sending
            # to router to avoid dropping the message silently.
            try:
                self._send_to_router(raw, None)
            except Exception:
                logger.warning("Failed to re-send raw message after parse error")
            return
        if env.type != MessageType.DATA_BATCH:
            logger.warning("Expected databatch, got %s", env.type)
            return
        db = env.data_batch
        # Log envelope / DataBatch summary
        try:
            rows_count = len(db.payload.rows) if db and db.payload and db.payload.rows is not None else 0
        except Exception:
            rows_count = -1
        logger.debug("Parsed Envelope type=%s client_id=%s batch_rows=%s", env.type, getattr(db, "client_id", None), rows_count)
        # protocol2 DataBatch puts the batch_number inside payload.batch_number.
        # Provide a safe accessor that handles both legacy and protobuf shapes.
        def _safe_batch_number(database) -> int | None:
            logger.debug("Obteniendo batch_number seguro del DataBatch")
            try:
                # protobuf path: payload.batch_number
                if getattr(database, "payload", None) and getattr(database.payload, "batch_number", None) is not None:
                    return int(database.payload.batch_number)
                # legacy path: top-level batch_number
                if getattr(database, "batch_number", None) is not None:
                    return int(database.batch_number)
            except Exception:
                return None
            return None

        bn = _safe_batch_number(db)
        if not db.payload:
            logger.warning("Batch sin batch_msg: reenvío sin cambios")
            self._send_to_router(raw, bn)
            return

        inner = db.payload
        step_mask = db.filter_steps
        step = current_step_from_mask(step_mask)
        if step is None:
            logger.debug(
                "Sin step activo en máscara: reenvío sin cambios | table_id=%s mask=%s",
                inner.name,
                step_mask,
            )
            self._send_to_router(raw, bn)
            return
        if inner is None or not inner.rows:
            logger.warning(
                "Batch sin 'rows': reenvío sin cambios | table_id=%s step=%s",
                inner.name,
                step,
            )
            self._send_to_router(raw, bn)
            return

        queries = list(db.query_ids)
        queries_key = qkey(queries)
        key = (inner.name, step, queries_key)

        fn = self._filters.get(key)
        if fn is None:
            logger.warning(
                "No hay filtro registrado para queries=%s: reenvío sin cambios | table_id=%s step=%s",
                queries_key,
                inner.name,
                step,
            )
            self._send_to_router(raw, bn)
            return

        try:
            new_rows = fn(inner) or []
            # protobuf repeated composite fields (like `rows`) don't allow
            # direct assignment of a Python list. Convert filter output into
            # Row messages and replace the repeated field contents.
            rows_objs: list[Row] = []
            cols = []
            try:
                cols = list(inner.schema.columns)
            except Exception:
                cols = []

            for r in new_rows:
                if isinstance(r, Row):
                    rows_objs.append(r)
                elif isinstance(r, (list, tuple)):
                    # list of values in the schema order
                    rows_objs.append(Row(values=list(r)))
                elif isinstance(r, dict):
                    # build values according to schema column order
                    vals = [r.get(c) for c in cols] if cols else list(r.values())
                    rows_objs.append(Row(values=vals))
                else:
                    # fallback: coerce to string in a single-column row
                    rows_objs.append(Row(values=[str(r)]))

            # replace repeated field contents
            inner.rows.clear()
            if rows_objs:
                inner.rows.extend(rows_objs)
            # Assign the DataBatch into the Envelope using CopyFrom to avoid
            # "Assignment not allowed to message field" errors on protobuf
            # message fields.
            env.data_batch.CopyFrom(db)
            out_raw = env.SerializeToString()
            logger.debug(
                "Filtro aplicado con éxito: filas antes=%d, después=%d | table_id=%s step=%s queries=%s filter=%s",
                len(inner.rows) + (len(new_rows) - len(rows_objs)),
                len(new_rows),
                inner.name,
                step,
                queries_key,
                getattr(fn, "__name__", "unknown"),
            )
            self._send_to_router(out_raw, bn)

        except Exception:
            logger.exception(
                "Error aplicando filtro; reenvío sin cambios | table_id=%s step=%s queries=%s filter=%s",
                inner.name,
                step,
                queries_key,
                getattr(fn, "__name__", "unknown"),
            )
            self._send_to_router(raw, bn)
