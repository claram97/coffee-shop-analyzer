from __future__ import annotations

import hashlib
import logging
import random
import threading
import time
from collections import defaultdict
from dataclasses import dataclass
from random import randint
from typing import Any, Dict, List, Optional, Union

from middleware.middleware_client import (MessageMiddlewareExchange,
                                          MessageMiddlewareQueue)
from protocol2.databatch_pb2 import DataBatch, Query
from protocol2.envelope_pb2 import Envelope, MessageType
from protocol2.eof_message_pb2 import EOFMessage
from protocol2.table_data_pb2 import (Row, TableData, TableName, TableSchema,
                                      TableStatus)
from protocol2.table_data_utils import iterate_rows_as_dicts

TABLE_NAME_TO_STR = {
    TableName.MENU_ITEMS: "menu_items",
    TableName.STORES: "stores",
    TableName.TRANSACTION_ITEMS: "transaction_items",
    TableName.TRANSACTIONS: "transactions",
    TableName.USERS: "users",
}
LIGHT_TABLES = {TableName.MENU_ITEMS, TableName.STORES}


def is_bit_set(mask: int, idx: int) -> bool:
    return ((mask >> idx) & 1) == 1


def set_bit(mask: int, idx: int) -> int:
    return mask | (1 << idx)


def first_zero_bit(mask: int, total_bits: int) -> Optional[int]:
    for i in range(total_bits):
        if not is_bit_set(mask, i):
            return i
    return None


class TableConfig:
    def __init__(self, aggregators: int):
        self.aggregators = aggregators


class QueryPolicyResolver:
    def steps_remaining(
        self, batch_table_name: TableName, batch_queries: list[Query], steps_done: int
    ) -> bool:
        if batch_table_name == TableName.TRANSACTIONS:
            if len(batch_queries) == 3:
                return steps_done == 0
            if len(batch_queries) == 1 and batch_queries[0] == Query.Q4:
                return False
            if len(batch_queries) == 2:
                return steps_done == 1
            if len(batch_queries) == 1 and batch_queries[0] == Query.Q3:
                return False
            if len(batch_queries) == 1 and batch_queries[0] == Query.Q1:
                return steps_done == 2
        if batch_table_name == TableName.TRANSACTION_ITEMS:
            return steps_done == 0
        return False

    def get_duplication_count(self, batch_queries: list[Query]) -> int:
        return 2 if len(batch_queries) > 1 else 1

    def get_new_batch_queries(
        self, batch_table_name: TableName, batch_queries: list[Query], copy_number: int
    ) -> list[Query]:
        if batch_table_name == TableName.TRANSACTIONS:
            if len(batch_queries) == 3:
                return [Query.Q4] if copy_number == 1 else [Query.Q1, Query.Q3]
            if len(batch_queries) == 2:
                return [Query.Q3] if copy_number == 1 else [Query.Q1]
        return list(batch_queries)

    def total_steps(
        self, batch_table_name: TableName, batch_queries: list[Query]
    ) -> int:
        if batch_table_name == TableName.TRANSACTIONS:
            if len(batch_queries) == 3:
                return 3
            if len(batch_queries) == 2:
                return 2
            if batch_queries == [Query.Q1]:
                return 3
            if batch_queries in [[Query.Q3], [Query.Q4]]:
                return 1
        return 1


class FilterRouter:
    def __init__(
        self,
        producer: "ExchangeBusProducer",
        policy: QueryPolicyResolver,
        table_cfg: TableConfig,
    ):
        self._p = producer
        self._pol = policy
        self._cfg = table_cfg
        self._log = logging.getLogger("filter-router")
        self._pending_batches: Dict[tuple[TableName, str], int] = defaultdict(int)
        self._pending_eof: Dict[tuple[TableName, str], EOFMessage] = {}

    def process_message(self, msg: Envelope) -> None:
        self._log.debug("Processing message: %r, message type: %r", msg, msg.type)
        if msg.type == MessageType.DATA_BATCH:
            self._handle_data(msg.data_batch)
        elif msg.type == MessageType.EOF_MESSAGE:
            # EOF messages carry an EOFMessage, handle them with the
            # dedicated EOF handler instead of the DataBatch handler.
            self._handle_table_eof(msg.eof)
        else:
            self._log.warning("Unknown message type: %r", type(msg))

    def _handle_data(self, batch: DataBatch) -> None:
        # self._log.info("Handling DataBatch message: %r", batch)
        table = batch.payload.name
        queries = batch.query_ids
        mask = batch.filter_steps
        bn = batch.payload.batch_number
        cid = batch.client_id
        if table is None:
            self._log.warning("Batch sin table_id válido. bn=%s", bn)
            return

        self._log.debug(
            "recv DataBatch table=%s queries=%s mask=%s shards_info=%s bn=%s cid=%s",
            table,
            queries,
            bin(mask),
            getattr(batch, "shards_info", []),
            bn,
            cid,
        )
        key = (cid, table)

        if mask == 0:
            self._pending_batches[key] += 1
            self._log.debug("pending++ %s -> %d", key, self._pending_batches[key])

        total_steps = self._pol.total_steps(table, queries)
        next_step = first_zero_bit(mask, total_steps)
        if next_step is not None and self._pol.steps_remaining(
            table, queries, steps_done=next_step
        ):
            # DataBatch protobuf (protocol2) does not have `reserved_u16`.
            # Use the existing `filter_steps` field to store the updated mask.
            new_mask = set_bit(mask, next_step)
            try:
                batch.filter_steps = new_mask
            except Exception:
                # If assignment to filter_steps fails for any reason, fall back
                # to setting an attribute on the Python object (legacy-like).
                setattr(batch, "filter_steps", new_mask)

            self._log.debug(
                "→ filters step=%d table=%s new_mask=%s",
                next_step,
                table,
                bin(new_mask),
            )
            try:
                self._p.send_to_filters_pool(batch)
            except Exception as e:
                self._log.error("send_to_filters_pool failed: %s", e)
            return

        dup_count = int(self._pol.get_duplication_count(queries) or 1)
        if dup_count > 1:
            self._log.debug(
                "Fan-out x%d table=%s queries=%s", dup_count, table, queries
            )

            try:
                self._pending_batches[key] += dup_count
                for i in range(dup_count):
                    new_queries = self._pol.get_new_batch_queries(
                        table, queries, copy_number=i
                    ) or list(queries)
                    b = DataBatch()
                    b.CopyFrom(batch)
                    b.query_ids.clear()
                    b.query_ids.extend(new_queries)
                    self._handle_data(b)
            except Exception as e:
                self._log.error("requeue_to_router failed: %s", e)

            self._pending_batches[key] = max(0, self._pending_batches[key] - 1)
            self._log.debug(
                "pending-- (fanout parent) %s -> %d", key, self._pending_batches[key]
            )
            return

        try:
            self._send_to_some_aggregator(batch)
        except Exception as e:
            self._log.error("send_sharded failed: %s", e)

        self._pending_batches[key] = max(0, self._pending_batches[key] - 1)
        self._log.debug("pending-- %s -> %d", key, self._pending_batches[key])
        self._maybe_flush_pending_eof(key)

    def _send_to_some_aggregator(self, batch: DataBatch) -> None:
        if batch.payload.name in [TableName.MENU_ITEMS, TableName.STORES]:
            self._log.info(
                "_send_to_some_aggregator table=%s bn=%s",
                batch.payload.name,
                batch.payload.batch_number,
            )
        num_parts = max(1, int(self._cfg.aggregators))
        self._p.send_to_aggregator_partition(randint(0, num_parts - 1), batch)

    def _handle_table_eof(self, eof: EOFMessage) -> None:
        self._log.info("Handling TABLE_EOF message: %r", eof)
        key = (eof.table, eof.client_id)
        self._log.info("TABLE_EOF received: key=%s", key)
        self._pending_eof[key] = eof
        self._maybe_flush_pending_eof(key)

    def _maybe_flush_pending_eof(self, key: tuple[TableName, str]) -> None:
        pending = self._pending_batches.get(key, 0)
        eof = self._pending_eof.get(key)
        if eof is None or pending > 0:
            if eof is not None:
                self._log.info("TABLE_EOF deferred: key=%s pending=%d", key, pending)
            return
        total_parts = max(1, int(self._cfg.aggregators))
        self._log.info("TABLE_EOF -> aggregators: key=%s parts=%d", key, total_parts)
        for part in range(total_parts):
            try:
                self._p.send_table_eof_to_aggregator_partition(key, part)
            except Exception as e:
                self._log.error(
                    "send_table_eof_to_aggregator_partition failed part=%d key=%s: %s",
                    part,
                    key,
                    e,
                )
        self._pending_eof.pop(key, None)
        self._pending_batches.pop(key, None)


class ExchangeBusProducer:
    def __init__(
        self,
        host: str,
        filters_pool_queue: str,
        in_mw: MessageMiddlewareExchange,
        exchange_fmt: str = "ex.{table}",
        rk_fmt: str = "agg.{table}.{pid:02d}",
        *,
        max_retries: int = 5,
        base_backoff_ms: int = 100,
        backoff_multiplier: float = 2.0,
    ):
        self._log = logging.getLogger("filter-router.bus")
        self._host = host
        self._filters_pub = MessageMiddlewareQueue(host, filters_pool_queue)
        self._exchange_fmt = exchange_fmt
        self._rk_fmt = rk_fmt
        self._pub_cache: dict[tuple[TableName, str], MessageMiddlewareExchange] = {}
        self._pub_locks: dict[tuple[TableName, str], threading.Lock] = {}
        self._in_mw = in_mw

        self._max_retries = int(max_retries)
        self._base_backoff_ms = int(base_backoff_ms)
        self._backoff_multiplier = float(backoff_multiplier)

    def shutdown(self):
        """Closes all active publisher connections."""
        self._log.info("Shutting down ExchangeBusProducer...")

        try:
            self._filters_pub.close()
        except Exception as e:
            self._log.warning(f"Error closing filters_pool publisher: {e}")

        for pub in self._pub_cache.values():
            try:
                pub.close()
            except Exception as e:
                self._log.warning(f"Error closing cached publisher for key {pub}: {e}")

        self._pub_cache.clear()
        self._log.info("ExchangeBusProducer shutdown complete.")

    def _key_for(self, table_name: TableName, pid: int) -> tuple[TableName, str]:
        table = TABLE_NAME_TO_STR[table_name]
        ex = self._exchange_fmt.format(table=table)
        rk = self._rk_fmt.format(table=table, pid=pid)
        return (ex, rk)

    def _get_pub(self, table_name: str, pid: int) -> MessageMiddlewareExchange:
        table = TABLE_NAME_TO_STR[table_name]
        key = self._key_for(table, pid)
        pub = self._pub_cache.get(key)

        if pub is not None and getattr(pub, "is_closed", None):
            try:
                if pub.is_closed():
                    self._log.debug("publisher cached but closed: %s → recreate", key)
                    self._drop_pub(key)
                    pub = None
            except Exception:
                pass

        if pub is None:
            self._log.debug(
                "create publisher exchange=%s rk=%s host=%s", key[0], key[1], self._host
            )
            pub = MessageMiddlewareExchange(
                host=self._host, exchange_name=key[0], route_keys=[key[1]]
            )
            self._pub_cache[key] = pub
            if key not in self._pub_locks:
                self._pub_locks[key] = threading.Lock()
        return pub

    def _drop_pub(self, key: tuple[TableName, str]) -> None:
        pub = self._pub_cache.pop(key, None)
        if pub is not None:
            try:
                if hasattr(pub, "close"):
                    pub.close()
            except Exception:
                pass

    def _send_with_retry(self, key: tuple[TableName, str], payload: bytes) -> None:
        """
        Envía `payload` al exchange/rk indicado por `key`, con reintentos y recreación del publisher.
        Bloquea por key para serializar accesos concurrentes al mismo canal.
        """
        self._log.info("send_with_retry key=%s payload_size=%d", key, len(payload))
        lock = self._pub_locks.setdefault(key, threading.Lock())
        with lock:
            attempt = 0
            backoff = self._base_backoff_ms / 1000.0
            last_error = None

            while attempt <= self._max_retries:
                try:
                    pub = self._pub_cache.get(key)
                    if pub is None:
                        self._log.debug("pub cache miss -> create: %s", key)
                        pub = MessageMiddlewareExchange(
                            host=self._host, exchange_name=key[0], route_keys=[key[1]]
                        )
                        self._pub_cache[key] = pub
                    pub.send(payload)
                    return
                except Exception as e:
                    last_error = e
                    self._log.warning(
                        "send failed (attempt %d/%d) key=%s: %s",
                        attempt + 1,
                        self._max_retries,
                        key,
                        e,
                    )
                    self._drop_pub(key)

                    attempt += 1
                    if attempt > self._max_retries:
                        break

                    jitter = random.uniform(0, backoff * 0.2)
                    sleep_s = backoff + jitter
                    time.sleep(sleep_s)
                    backoff *= self._backoff_multiplier

            raise RuntimeError(
                f"Error sending message to {key}: retries exhausted"
            ) from last_error

    def send_to_filters_pool(self, batch: DataBatch) -> None:
        try:
            self._log.info("publish → filters_pool")
            envelope = Envelope(type=MessageType.DATA_BATCH, data_batch=batch)
            raw = envelope.SerializeToString()
            self._filters_pub.send(raw)
        except Exception as e:
            self._log.error("filters_pool send failed: %s", e)

    def send_to_aggregator_partition(self, partition_id: int, batch: DataBatch) -> None:
        table = batch.payload.name
        key = self._key_for(table, int(partition_id))
        try:
            self._log.debug(
                "publish → aggregator table=%s part=%d", table, int(partition_id)
            )
            envelope = Envelope(type=MessageType.DATA_BATCH, data_batch=batch)
            raw = envelope.SerializeToString()
            self._send_with_retry(key, raw)
        except Exception as e:
            self._log.error(
                "aggregator send failed table=%s part=%d: %s",
                table,
                int(partition_id),
                e,
            )

    def requeue_to_router(self, batch: DataBatch) -> None:
        env = Envelope(type=MessageType.DATA_BATCH, data_batch=batch)
        try:
            self._log.debug(
                "requeue_to_router: reinyectando batch table=%s queries=%s",
                batch.payload.name,
                batch.query_ids,
            )
            raw = env.SerializeToString()
            self._in_mw.send(raw)
        except Exception as e:
            self._log.error("requeue_to_router failed: %s", e)

    def send_table_eof_to_aggregator_partition(
        self, key: tuple[TableName, str], partition_id: int
    ) -> None:
        try:
            eof = EOFMessage(table=key[0], client_id=key[1])
            env = Envelope(type=MessageType.EOF_MESSAGE, eof=eof)
            payload = env.SerializeToString()
            k = self._key_for(key[0], int(partition_id))
            self._log.debug(
                "publish TABLE_EOF → aggregator key=%s part=%d", key, int(partition_id)
            )
            self._send_with_retry(k, payload)
        except Exception as e:
            self._log.error(
                "aggregator TABLE_EOF send failed key=%s part=%d: %s",
                key,
                int(partition_id),
                e,
            )


class RouterServer:
    def __init__(
        self,
        host: str,
        router_in: MessageMiddlewareExchange,
        producer: ExchangeBusProducer,
        policy: QueryPolicyResolver,
        table_cfg: TableConfig,
        stop_event: threading.Event,
    ):
        self._producer = producer
        self._mw_in = router_in
        self._router = FilterRouter(
            producer=producer, policy=policy, table_cfg=table_cfg
        )
        self._log = logging.getLogger("filter-router-server")
        self._stop_event = stop_event

    def run(self) -> None:
        self._log.debug("RouterServer starting consume")

        def _cb(body: bytes):
            if self._stop_event.is_set():
                self._log.warning("Shutdown in progress, skipping incoming message.")
                return
            try:
                if len(body) < 1:
                    self._log.error("Received empty message")
                    return
                msg = Envelope()
                msg.ParseFromString(body)
                self._router.process_message(msg)
            except Exception as e:
                self._log.exception("Error in router callback: %s", e)
            except Exception as e:
                self._log.exception("Error in router callback: %s", e)

        try:
            self._mw_in.start_consuming(_cb)
            self._log.debug("RouterServer consuming (thread started)")
        except Exception as e:
            self._log.exception("start_consuming failed: %s", e)

    def stop(self) -> None:
        """Stops the consumer and shuts down the producer."""
        self._log.info("Stopping Filter Router Server...")

        try:
            self._mw_in.stop_consuming()
            self._log.info("Input consumer stopped.")
        except Exception as e:
            self._log.warning(f"Error stopping input consumer: {e}")

        try:
            self._producer.shutdown()
        except Exception as e:
            self._log.warning(f"Error during producer shutdown: {e}")

        self._log.info("Filter Router Server stopped.")
