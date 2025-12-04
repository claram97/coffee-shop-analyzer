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
    """Configure logging only if the root logger has no handlers.

    Sets up basic logging configuration using the FILTER_WORKER_LOG_LEVEL
    environment variable (defaults to INFO). This prevents duplicate
    handler registration when the module is imported multiple times.
    """
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
    """Extract the current filter step index from a bitmask.

    Finds the index of the first unset bit in the mask, which represents
    the current step that needs to be processed. Steps are numbered
    starting from 0.

    Args:
        mask: Bitmask where each bit represents a filter step completion status.

    Returns:
        The current step index (0-based), or None if mask is 0 or all bits are set.
    """
    if mask == 0:
        return None
    i = 0
    while (mask & 1) == 1:
        mask >>= 1
        i += 1
    return i - 1


def _parse_dt_local(value: Optional[str]) -> Optional[datetime]:
    """Parse a datetime string in ISO format.

    Args:
        value: ISO format datetime string (e.g., "2024-01-15T10:30:00").

    Returns:
        Parsed datetime object, or None if parsing fails or value is empty.
    """
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except Exception as e:
        logger.error("error parsing datetime: %s", e)
        return None


def _parse_final_amount(value: Any) -> Optional[float]:
    """Parse a value as a float, handling string conversion and whitespace.

    Args:
        value: Value to parse (will be converted to string and stripped).

    Returns:
        Parsed float value, or None if conversion fails or value is None.
    """
    if value is None:
        return None
    try:
        return float(str(value).strip())
    except Exception as e:
        logger.error("error in final_amount_filter: %s", e)
        return None


def hour_filter(td) -> List[Any]:
    """Filter rows to include only those created between 6 AM and 11 PM.

    Args:
        td: Table data containing rows with a "created_at" field.

    Returns:
        List of row dictionaries that match the hour criteria.
    """
    return [
        r
        for r in iterate_rows_as_dicts(td)
        if (dt := _parse_dt_local(r["created_at"])) is not None and 6 <= dt.hour <= 23
    ]


def final_amount_filter(td) -> List[Any]:
    """Filter rows to include only those with final_amount >= 75.0.

    Args:
        td: Table data containing rows with a "final_amount" field.

    Returns:
        List of row dictionaries that have final_amount >= 75.0.
    """
    return [
        r
        for r in iterate_rows_as_dicts(td)
        if (amount := _parse_final_amount(r["final_amount"])) is not None
        and amount >= 75.0
    ]


def year_filter(td, min_year: int = 2024, max_year: int = 2025) -> List[Any]:
    """Filter rows to include only those created within a year range.

    Args:
        td: Table data containing rows with a "created_at" field.
        min_year: Minimum year (inclusive). Defaults to 2024.
        max_year: Maximum year (inclusive). Defaults to 2025.

    Returns:
        List of row dictionaries that fall within the specified year range.
    """
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
    """Generate a canonical string key from a list of queries.

    Creates a sorted, comma-separated string of query IDs for use as a
    registry key. Ensures consistent ordering regardless of input order.

    Args:
        queries: List of Query enum values.

    Returns:
        Comma-separated string of sorted query IDs (e.g., "1,3,4").
    """
    return ",".join(str(q) for q in sorted(set(QUERY_TO_INT[x] for x in queries)))


REGISTRY: FilterRegistry = {}
REGISTRY[(TableName.TRANSACTIONS, 0, qkey([Query.Q1, Query.Q3, Query.Q4]))] = (
    year_filter
)
REGISTRY[(TableName.TRANSACTIONS, 1, qkey([Query.Q1, Query.Q3]))] = hour_filter
REGISTRY[(TableName.TRANSACTIONS, 2, qkey([Query.Q1]))] = final_amount_filter
REGISTRY[(TableName.TRANSACTION_ITEMS, 0, qkey([Query.Q2]))] = year_filter


class FilterWorker:
    """Consumes batches from a queue, applies filters, and routes to filter routers.

    Processes data batches from a filter workers pool queue, applies the
    appropriate filter function based on table name, filter step, and query
    combination, then publishes the filtered batch to the filter router exchange
    using a routing key calculated from (batch_number % num_routers).
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
        """Initialize the filter worker.

        Args:
            host: Message broker host address.
            in_queue: Queue name for consuming batches from the filter workers pool.
            out_router_exchange: Exchange name for publishing filtered batches to routers.
            out_router_rk_fmt: Format string for routing keys (e.g., "router.{pid:02d}").
            num_routers: Number of filter routers to distribute batches across.
            stop_event: Event to signal shutdown requests.
            filters: Optional filter registry. Defaults to the global REGISTRY.
        """
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

    def shutdown(self) -> None:
        """Stop the consumer and close all publisher connections.

        Gracefully shuts down the filter worker by stopping message consumption
        and closing all cached publisher connections. Logs warnings for errors
        but does not raise exceptions.

        Order of operations:
        1. Stop consuming and wait for in-flight messages to complete
           (this blocks until all callbacks finish, including any sends)
        2. Close all publisher connections (safe since all sends are complete)
        """
        logger.info("Shutting down FilterWorker...")

        try:
            # Step 1: Stop consuming - this blocks until all callbacks complete
            # Since sends are synchronous, all sends will complete before this returns
            self._in.stop_consuming()
            logger.info("Input consumer stopped.")
        except Exception as e:
            logger.warning(f"Error stopping input consumer: {e}")

        # Step 2: Close publishers - safe now since all sends are complete
        for rk, pub in self._pub_cache.items():
            try:
                pub.close()
            except Exception as e:
                logger.warning(f"Error closing publisher for rk='{rk}': {e}")

        self._pub_cache.clear()
        logger.info("FilterWorker shutdown complete.")

    def _publisher_for_rk(self, rk: str) -> MessageMiddlewareExchange:
        """Get or create a publisher for a specific routing key.

        Caches publishers per routing key to avoid creating duplicate connections.
        Creates a new publisher if one doesn't exist for the given routing key.

        Args:
            rk: Routing key to get/create a publisher for.

        Returns:
            MessageMiddlewareExchange publisher for the routing key.
        """
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
        """Send a batch to the appropriate filter router.

        Calculates the router partition ID from the batch number and sends
        the batch to the corresponding routing key. Uses partition 0 if
        batch_number is None.

        Args:
            raw: Serialized envelope message bytes to send.
            batch_number: Batch number used to determine router partition.

        Raises:
            Exception: If send fails. During shutdown, this may be expected
                if connections are being closed.
        """
        logger.debug("Enviando batch al router | batch_number=%s", batch_number)
        pid = 0 if batch_number is None else int(batch_number) % self._num_routers
        rk = self._rk_fmt.format(pid=pid)
        pub = self._publisher_for_rk(rk)
        try:
            pub.send(raw)
        except Exception as e:
            # Log error with context - during shutdown, send failures may be expected
            if self._stop_event.is_set():
                logger.debug(
                    "Send failed during shutdown: batch_number=%s rk=%s: %s",
                    batch_number,
                    rk,
                    e,
                )
            else:
                logger.error(
                    "Send failed: batch_number=%s rk=%s: %s",
                    batch_number,
                    rk,
                    e,
                )
            raise

    def run(self) -> None:
        """Start consuming messages from the input queue.

        Begins the message consumption loop, processing batches through
        the _on_raw callback. Blocks until shutdown is requested.
        """
        logger.info("Comenzando consumo de mensajes…")
        self._in.start_consuming(self._on_raw)

    def _log_raw_preview(self, raw: bytes) -> None:
        """Log a hexadecimal preview of the raw message bytes.

        Args:
            raw: Raw message bytes to preview (logs first 48 bytes).
        """
        try:
            preview = raw[:48].hex()
            logger.debug("raw preview: %s", preview)
        except Exception:
            logger.debug("raw preview unavailable")

    def _nack_on_shutdown(self) -> bool:
        """Check if shutdown is in progress.

        The middleware handles NACKing messages during shutdown automatically.
        This method only checks the shutdown status for early return.

        Returns:
            True if shutdown is in progress, False otherwise.
        """
        if not self._stop_event.is_set():
            return False
        logger.warning("Shutdown in progress, message will be NACKed by middleware.")
        return True

    def _parse_envelope_or_forward(self, raw: bytes) -> Envelope | None:
        """Parse an envelope from raw bytes, forwarding on parse failure.

        Attempts to parse the protobuf envelope. If parsing fails, forwards
        the raw message to the router unchanged to avoid message loss.

        Args:
            raw: Serialized envelope message bytes.

        Returns:
            Parsed Envelope object, or None if parsing failed and message was forwarded.

        Raises:
            Exception: If forwarding the message after parse failure also fails.
        """
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
        """Log a summary of the parsed data batch.

        Args:
            env: Parsed envelope message.
            db: DataBatch object from the envelope.
        """
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
        """Safely extract batch number from a DataBatch object.

        Checks both database.payload.batch_number and database.batch_number
        attributes, handling missing attributes gracefully.

        Args:
            database: DataBatch object to extract batch number from.

        Returns:
            Batch number as integer, or None if not found or conversion fails.
        """
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
        """Extract payload from DataBatch, forwarding if missing.

        Args:
            db: DataBatch object to extract payload from.
            raw: Original raw message bytes for forwarding if payload is missing.
            bn: Batch number for forwarding.

        Returns:
            Payload object if found, None if missing (message forwarded).
        """
        inner = getattr(db, "payload", None)
        if inner:
            return inner
        logger.warning("Batch sin batch_msg: reenvío sin cambios")
        self._send_to_router(raw, bn)
        return None

    def _step_or_forward(self, db, inner, raw: bytes, bn: int | None) -> int | None:
        """Extract current filter step from mask, forwarding if no active step.

        Args:
            db: DataBatch object containing filter_steps mask.
            inner: Payload object for logging.
            raw: Original raw message bytes for forwarding.
            bn: Batch number for forwarding.

        Returns:
            Current filter step index if found, None if no active step (message forwarded).
        """
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
        """Resolve the filter function for a given table, step, and queries.

        Args:
            inner: Payload object containing table name.
            step: Current filter step index.
            queries: List of queries associated with the batch.

        Returns:
            Tuple of (queries_key, filter_function). Filter function is None
            if no matching filter is registered.
        """
        queries_key = qkey(list(queries))
        key = (inner.name, step, queries_key)
        return queries_key, self._filters.get(key)

    def _ensure_rows_or_forward(
        self, inner, step: int, raw: bytes, bn: int | None
    ) -> bool:
        """Check if payload has rows, forwarding if missing.

        Args:
            inner: Payload object to check for rows.
            step: Current filter step for logging.
            raw: Original raw message bytes for forwarding.
            bn: Batch number for forwarding.

        Returns:
            True if rows exist, False if missing (message forwarded).
        """
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
        """Resolve filter function, forwarding if no filter is registered.

        Args:
            inner: Payload object containing table name.
            step: Current filter step index.
            queries: List of queries associated with the batch.
            raw: Original raw message bytes for forwarding.
            bn: Batch number for forwarding.

        Returns:
            Tuple of (queries_key, filter_function). Both are None if no
            filter is registered (message forwarded).
        """
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
        """Process a data batch message through the filter pipeline.

        Validates the batch structure, extracts metadata, resolves the
        appropriate filter function, and applies it. Forwards the batch
        unchanged if any validation step fails.

        Args:
            env: Parsed envelope message.
            raw: Original raw message bytes for forwarding if needed.
        """
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
        """Build a list of Row objects from filter function output.

        Converts filter output to protobuf Row objects, handling multiple
        input formats. Preserves column order from schema when available.

        Args:
            new_rows: List of elements returned by the filter function. Each
                element can be:
                - A Row object (reused as-is)
                - A sequence (list/tuple) containing row values
                - A dictionary with column names as keys
                - Any other value (converted to string in a single-column row)
            inner: Original batch payload object, used to extract schema
                column order when available.

        Returns:
            List of Row objects ready to assign to inner.rows.
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
        """Replace the rows in a payload with new filtered rows.

        Args:
            inner: Payload object whose rows will be replaced.
            rows_objs: List of Row objects to set as the new rows.
        """
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
        """Log successful filter application with statistics.

        Args:
            inner: Payload object containing table name.
            step: Filter step that was applied.
            queries_key: Canonical queries key string.
            fn: Filter function that was applied.
            before_rows: Number of rows before filtering.
            after_rows: Number of rows after filtering.
        """
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
        """Apply a filter function to a batch and forward the result.

        Executes the filter function, converts output to Row objects, replaces
        the batch rows, and sends the filtered batch to the router. On error,
        forwards the original batch unchanged.

        Args:
            env: Envelope message containing the batch.
            db: DataBatch object to filter.
            inner: Payload object containing the rows to filter.
            fn: Filter function to apply.
            queries_key: Canonical queries key string for logging.
            step: Filter step index for logging.
            bn: Batch number for routing.
            raw: Original raw message bytes for forwarding on error.
        """
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

    def _on_raw(self, raw: bytes) -> None:
        """Callback for processing raw messages from the queue.

        Main entry point for message processing. Handles shutdown checks,
        envelope parsing, and routes DATA_BATCH messages to the batch handler.

        Args:
            raw: Raw message bytes from the queue.
        """
        logger.debug("Mensaje recibido, tamaño=%d bytes", len(raw))
        self._log_raw_preview(raw)

        if self._nack_on_shutdown():
            return

        env = self._parse_envelope_or_forward(raw)
        if env is None:
            return

        if env.type != MessageType.DATA_BATCH:
            logger.warning("Expected databatch, got %s", env.type)
            return
        self._handle_data_batch(env, raw)
