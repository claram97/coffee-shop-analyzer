from __future__ import annotations

"""
Filter Worker: consumes batches from the filters pool, applies the registered
per-table/step filters, handles CLEAN_UP blacklisting, and forwards results to
the Filter Router exchange.
"""

BLACKLIST_TTL_SECONDS = 600
RAW_LOG_PREVIEW_BYTES = 48
HOUR_FILTER_START = 6
HOUR_FILTER_END = 23
FINAL_AMOUNT_MIN = 75.0
YEAR_FILTER_MIN = 2024
YEAR_FILTER_MAX = 2025

import json
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
    """Devuelve el índice del bit menos significativo en 0 (paso actual)."""
    if mask == 0:
        return None
    i = 0
    while (mask & 1) == 1:
        mask >>= 1
        i += 1
    return i - 1


def _parse_dt_local(value: Optional[str]) -> Optional[datetime]:
    """Parsea ISO datetime en local; None en caso de error."""
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except Exception as e:
        logger.error("error parsing datetime: %s", e)
        return None


def _parse_final_amount(value: Any) -> Optional[float]:
    """Normaliza final_amount a float, None en caso de error."""
    if value is None:
        return None
    try:
        return float(str(value).strip())
    except Exception as e:
        logger.error("error in final_amount_filter: %s", e)
        return None


def hour_filter(td) -> List[Any]:
    """Filtro Q3: solo filas con created_at entre 6 y 23 hs."""
    return [
        r
        for r in iterate_rows_as_dicts(td)
        if (dt := _parse_dt_local(r["created_at"])) is not None
        and HOUR_FILTER_START <= dt.hour <= HOUR_FILTER_END
    ]


def final_amount_filter(td) -> List[Any]:
    """Filtro Q1: solo transacciones con final_amount >= 75."""
    return [
        r
        for r in iterate_rows_as_dicts(td)
        if (amount := _parse_final_amount(r["final_amount"])) is not None
        and amount >= FINAL_AMOUNT_MIN
    ]


def year_filter(
    td,
    min_year: int = YEAR_FILTER_MIN,
    max_year: int = YEAR_FILTER_MAX,
) -> List[Any]:
    """Filtra filas cuyo created_at cae dentro del rango de años."""
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
    """Clave determinística para un set de queries (ordenada y sin duplicados)."""
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
        state_dir: str | None = None,
        worker_id: int = 0,
    ):
        """Inicializa conexiones, blacklist y configuración de filtrado."""
        self._host = host
        self._in = MessageMiddlewareQueue(host, in_queue)
        self._filters = filters or REGISTRY

        self._out_ex = out_router_exchange
        self._rk_fmt = out_router_rk_fmt
        self._num_routers = max(1, int(num_routers))

        self._pub_cache: Dict[str, MessageMiddlewareExchange] = {}

        self._stop_event = stop_event

        self._state_dir = state_dir or os.getenv(
            "FILTER_WORKER_STATE_DIR", "/tmp/filter_worker_state"
        )
        self._worker_id = int(worker_id)
        os.makedirs(self._state_dir, exist_ok=True)
        self._blacklist: Dict[str, float] = {}
        self._blacklist_lock = threading.Lock()
        self._blacklist_file = os.path.join(
            self._state_dir, f"blacklist_{self._worker_id}.json"
        )
        self._load_and_clean_blacklist()

        logger.info(
            "FilterWorker inicializado | host=%s in_queue=%s out_exchange=%s rk_fmt=%s num_routers=%d",
            host,
            in_queue,
            out_router_exchange,
            out_router_rk_fmt,
            self._num_routers,
        )

    def shutdown(self):
        """Detiene el consumo y cierra publishers."""
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
        """Crea o reutiliza publisher hacia la RK dada."""
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
        """Encola un mensaje al Filter Router usando batch_number para elegir RK."""
        logger.debug("Enviando batch al router | batch_number=%s", batch_number)
        pid = 0 if batch_number is None else int(batch_number) % self._num_routers
        rk = self._rk_fmt.format(pid=pid)
        pub = self._publisher_for_rk(rk)
        pub.send(raw)

    def run(self) -> None:
        """Arranca el consumo de la cola de filtros."""
        logger.info("Comenzando consumo de mensajes…")
        self._in.start_consuming(self._on_raw)

    def _log_raw_preview(self, raw: bytes) -> None:
        """Loguea un pequeño preview hexadecimal del mensaje recibido."""
        try:
            preview = raw[:RAW_LOG_PREVIEW_BYTES].hex()
            logger.debug("raw preview: %s", preview)
        except Exception:
            logger.debug("raw preview unavailable")

    def _nack_on_shutdown(self, channel=None, delivery_tag=None) -> bool:
        """NACKea el mensaje si hay shutdown en progreso."""
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
        """Parsea un Envelope; si falla reenvía raw y deja que el broker reintente."""
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
                raise  # Forzar NACK/redelivery
        return None

    def _handle_cleanup_message(self, env: Envelope) -> bool:
        """Procesa mensajes CLEAN_UP y devuelve True si fueron manejados."""
        if env.type != MessageType.CLEAN_UP_MESSAGE:
            return False
        cleanup = env.clean_up
        cid = cleanup.client_id if cleanup.client_id else ""
        if not cid:
            logger.warning("CLEANUP sin client_id: se descarta")
            return True
        self._add_to_blacklist(cid)
        logger.info("Recibido CLEANUP → agregado a blacklist | client_id=%s", cid)
        return True

    def _log_databatch_summary(self, env: Envelope, db) -> None:
        """Loguea un resumen del DataBatch."""
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
        """Obtiene batch_number desde payload o toplevel si existe."""
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

    def _build_rows_from_filter_output(
        self, new_rows: List[Any], inner
    ) -> List[Row]:
        """Normaliza la salida del filtro a objetos Row."""
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
        """Ejecuta el filtro, reemplaza filas y reenvía; ante error reenvía sin cambios."""
        try:
            before_rows = len(getattr(inner, "rows", []))
        except Exception:
            before_rows = -1

        try:
            new_rows = fn(inner) or []
            rows_objs = self._build_rows_from_filter_output(new_rows, inner)

            inner.rows.clear()
            if rows_objs:
                inner.rows.extend(rows_objs)

            env.data_batch.CopyFrom(db)
            out_raw = env.SerializeToString()
            logger.debug(
                "Filtro aplicado con éxito: filas antes=%d, después=%d | table_id=%s step=%s queries=%s filter=%s",
                before_rows,
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

    def _on_raw(self, raw: bytes, channel=None, delivery_tag=None) -> None:
        """Handler principal de mensajes del pool de filtros."""
        logger.debug("Mensaje recibido, tamaño=%d bytes", len(raw))
        self._log_raw_preview(raw)

        if self._nack_on_shutdown(channel, delivery_tag):
            return False

        env = self._parse_envelope_or_forward(raw)
        if env is None:
            return

        if self._handle_cleanup_message(env):
            return

        if env.type != MessageType.DATA_BATCH:
            logger.warning("Expected databatch, got %s", env.type)
            return
        db = env.data_batch

        cid = getattr(db, "client_id", None) or ""
        if self._is_blacklisted(cid):
            logger.info(
                "Discarding batch from blacklisted client: cid=%s table_id=%s",
                cid,
                getattr(db.payload, "name", None),
            )
            return
        self._log_databatch_summary(env, db)

        bn = self._safe_batch_number(db)
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
            logger.debug(
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

        self._apply_filter_and_forward(
            env, db, inner, fn, queries_key, step, bn, raw
        )

    def _load_and_clean_blacklist(self) -> None:
        """Carga la blacklist desde disco y purga entradas antiguas (10 min)."""
        current_time = time.time()
        cutoff_time = current_time - BLACKLIST_TTL_SECONDS

        if os.path.exists(self._blacklist_file):
            try:
                with open(self._blacklist_file, "r") as f:
                    data = json.load(f)
                    self._blacklist = {
                        client_id: ts
                        for client_id, ts in data.items()
                        if ts > cutoff_time
                    }
                    logger.info(
                        "Loaded blacklist: %d entries (removed %d old entries)",
                        len(self._blacklist),
                        len(data) - len(self._blacklist),
                    )
            except Exception as e:
                logger.warning("Failed to load blacklist file: %s", e)
                self._blacklist = {}
        else:
            self._blacklist = {}
            logger.info("Blacklist file not found, starting with empty blacklist")

        self._save_blacklist()

    def _save_blacklist(self) -> None:
        """Persiste la blacklist en disco."""
        try:
            with open(self._blacklist_file, "w") as f:
                json.dump(self._blacklist, f)
        except Exception as e:
            logger.error("Failed to save blacklist file: %s", e)

    def _add_to_blacklist(self, client_id: str) -> None:
        """Agrega un client_id a la blacklist (memoria + disco)."""
        if not client_id:
            return

        current_time = time.time()
        with self._blacklist_lock:
            self._blacklist[client_id] = current_time
            self._save_blacklist()
            logger.info("Added client_id to blacklist: %s", client_id)

    def _is_blacklisted(self, client_id: str) -> bool:
        """True si el client_id está bloqueado."""
        if not client_id:
            return False

        with self._blacklist_lock:
            return client_id in self._blacklist
