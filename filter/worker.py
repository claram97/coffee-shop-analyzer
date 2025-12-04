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

    def _log_raw_preview(self, raw: bytes) -> None:
        try:
            preview = raw[:48].hex()
            logger.debug("raw preview: %s", preview)
        except Exception:
            logger.debug("raw preview unavailable")

    def _nack_on_shutdown(self, channel=None, delivery_tag=None) -> bool:
        if not self._stop_event.is_set():
            return False
        logger.warning("Shutdown in progress, NACKing message for redelivery.")
        if channel is not None and delivery_tag is not None:
            try:
                channel.basic_nack(delivery_tag=delivery_tag, requeue=True)
            except Exception as e:
                logger.warning("NACK failed during shutdown: %s", e)
        return True

    def _parse_envelope_or_forward(self, raw: bytes) -> Envelope | None:
        env = Envelope()
        try:
            env.ParseFromString(raw)
            return env
        except Exception as e:
            logger.exception("Failed to parse Envelope from raw bytes: %s", e)
            try:
                self._send_to_router(raw, None)
            except Exception:
                logger.warning("Failed to re-send raw message after parse error")
                raise
        return None

    def _log_databatch_summary(self, env: Envelope, db) -> None:
        try:
            rows_count = (
                len(db.payload.rows)
                if db and db.payload and db.payload.rows is not None
                else 0
            )
        except Exception:
            rows_count = -1
        logger.debug(
            "Parsed Envelope type=%s client_id=%s batch_rows=%s",
            env.type,
            getattr(db, "client_id", None),
            rows_count,
        )

    def _safe_batch_number(self, database) -> int | None:
        logger.debug("Obteniendo batch_number seguro del DataBatch")
        try:
            if (
                getattr(database, "payload", None)
                and getattr(database.payload, "batch_number", None) is not None
            ):
                return int(database.payload.batch_number)
            if getattr(database, "batch_number", None) is not None:
                return int(database.batch_number)
        except Exception:
            return None
        return None

    def _payload_or_forward(self, db, raw: bytes, bn: int | None):
        inner = getattr(db, "payload", None)
        if inner:
            return inner
        logger.warning("Batch sin batch_msg: reenvío sin cambios")
        self._send_to_router(raw, bn)
        return None

    def _step_or_forward(self, db, inner, raw: bytes, bn: int | None) -> int | None:
        step = current_step_from_mask(getattr(db, "filter_steps", 0))
        if step is not None:
            return step
        logger.debug(
            "Sin step activo en máscara: reenvío sin cambios | table_id=%s mask=%s",
            inner.name,
            getattr(db, "filter_steps", None),
        )
        self._send_to_router(raw, bn)
        return None

    def _resolve_filter(
        self, inner, step: int, queries: List[Query]
    ) -> Tuple[str, Optional[FilterFn]]:
        queries_key = qkey(list(queries))
        key = (inner.name, step, queries_key)
        return queries_key, self._filters.get(key)

    def _ensure_rows_or_forward(
        self, inner, step: int, raw: bytes, bn: int | None
    ) -> bool:
        if inner and inner.rows:
            return True
        logger.debug(
            "Batch sin 'rows': reenvío sin cambios | table_id=%s step=%s",
            getattr(inner, "name", None),
            step,
        )
        self._send_to_router(raw, bn)
        return False

    def _resolve_filter_or_forward(
        self,
        inner,
        step: int,
        queries: List[Query],
        raw: bytes,
        bn: int | None,
    ) -> tuple[Optional[str], Optional[FilterFn]]:
        queries_key, fn = self._resolve_filter(inner, step, queries)
        if fn is not None:
            return queries_key, fn
        logger.warning(
            "No hay filtro registrado para queries=%s: reenvío sin cambios | table_id=%s step=%s",
            queries_key,
            inner.name,
            step,
        )
        self._send_to_router(raw, bn)
        return None, None

    def _handle_data_batch(self, env: Envelope, raw: bytes) -> None:
        db = env.data_batch
        self._log_databatch_summary(env, db)
        bn = self._safe_batch_number(db)
        inner = self._payload_or_forward(db, raw, bn)
        if inner is None:
            return

        step = self._step_or_forward(db, inner, raw, bn)
        if step is None:
            return

        if not self._ensure_rows_or_forward(inner, step, raw, bn):
            return

        queries_key, fn = self._resolve_filter_or_forward(
            inner,
            step,
            list(db.query_ids),
            raw,
            bn,
        )
        if fn is None:
            return

        self._apply_filter_and_forward(env, db, inner, fn, queries_key, step, bn, raw)

    def _build_rows_from_filter_output(
        self, new_rows: List[Any], inner
    ) -> List[Row]:
                """
                Construye una lista de objetos `Row` a partir de la salida de un filtro.

                Parámetros:
                - `new_rows`: lista de elementos devueltos por la función de filtro. Cada
                    elemento puede ser un objeto `Row`, una secuencia (list/tuple), un
                    diccionario o cualquier otro valor convertible a string.
                - `inner`: objeto que representa el `batch` original; se utiliza sólo
                    para obtener el esquema (`inner.schema.columns`) cuando esté
                    disponible y así mantener el orden de columnas al crear `Row`.

                Comportamiento:
                - Si un elemento ya es instancia de `Row`, se reutiliza.
                - Si es una secuencia, se asume que contiene los valores de la fila.
                - Si es un diccionario, se extraen los valores en el orden de las
                    columnas del esquema si está disponible; en caso contrario se usan
                    los valores del diccionario en orden de iteración.
                - Para cualquier otro tipo se crea una fila con una única columna que
                    contiene la representación en string del valor.

                Devuelve:
                - Lista de objetos `Row` listos para asignar a `inner.rows`.
                """
                rows_objs: list[Row] = []
                try:
                        cols = list(inner.schema.columns)
                except Exception:
                        cols = []

                for r in new_rows:
                        if isinstance(r, Row):
                                rows_objs.append(r)
                        elif isinstance(r, (list, tuple)):
                                rows_objs.append(Row(values=list(r)))
                        elif isinstance(r, dict):
                                vals = [r.get(c) for c in cols] if cols else list(r.values())
                                rows_objs.append(Row(values=vals))
                        else:
                                rows_objs.append(Row(values=[str(r)]))
                return rows_objs

    def _replace_rows(self, inner, rows_objs: List[Row]) -> None:
        inner.rows.clear()
        if rows_objs:
            inner.rows.extend(rows_objs)

    def _log_filter_success(
        self,
        inner,
        step: int,
        queries_key: str,
        fn: FilterFn,
        before_rows: int,
        after_rows: int,
    ) -> None:
        logger.debug(
            "Filtro aplicado con éxito | table_id=%s step=%s queries=%s filter=%s before=%s after=%s",
            inner.name,
            step,
            queries_key,
            getattr(fn, "__name__", "unknown"),
            before_rows,
            after_rows,
        )

    def _apply_filter_and_forward(
        self,
        env: Envelope,
        db,
        inner,
        fn: FilterFn,
        queries_key: str,
        step: int,
        bn: int | None,
        raw: bytes,
    ) -> None:
        try:
            before_rows = len(getattr(inner, "rows", []) or [])
            new_rows = fn(inner) or []
            rows_objs = self._build_rows_from_filter_output(new_rows, inner)
            self._replace_rows(inner, rows_objs)

            env.data_batch.CopyFrom(db)
            out_raw = env.SerializeToString()
            self._log_filter_success(
                inner,
                step,
                queries_key,
                fn,
                before_rows,
                len(rows_objs),
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

    def _on_raw(self, raw: bytes, channel=None, delivery_tag=None) -> None:
        logger.debug("Mensaje recibido, tamaño=%d bytes", len(raw))
        self._log_raw_preview(raw)

        if self._nack_on_shutdown(channel, delivery_tag):
            return False

        env = self._parse_envelope_or_forward(raw)
        if env is None:
            return

        if env.type != MessageType.DATA_BATCH:
            logger.warning("Expected databatch, got %s", env.type)
            return
        self._handle_data_batch(env, raw)
