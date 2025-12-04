import logging
import os
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Set, Tuple

from google.protobuf.message import DecodeError

from middleware.middleware_client import MessageMiddlewareQueue
from protocol2.databatch_pb2 import DataBatch
from protocol2.envelope_pb2 import Envelope, MessageType
from protocol2.table_data_pb2 import TableName, TableStatus
from protocol2.table_data_utils import build_table_data, iterate_rows_as_dicts

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - [%(name)s] - %(message)s"
)
logger = logging.getLogger(__name__)
from query_strategy_append_only import get_strategy
from constants import QueryType
from persistence import FinisherPersistence

TABLE_NAME_TO_TYPE = {
    TableName.TRANSACTIONS: "Transactions",
    TableName.STORES: "Stores",
    TableName.MENU_ITEMS: "MenuItems",
    TableName.TRANSACTION_ITEMS: "TransactionItems",
    TableName.USERS: "Users",
    TableName.TRANSACTION_ITEMS_MENU_ITEMS: "TransactionItemsMenuItems",
    TableName.TRANSACTION_STORES: "TransactionStores",
}

EXPECTED_TABLES = {
    QueryType.Q1: ["Transactions"],
    QueryType.Q2: ["TransactionItemsMenuItems"],
    QueryType.Q3: ["TransactionStores"],
    QueryType.Q4: ["TransactionStores", "Users"],
}


@dataclass
class QueryState:
    """Represents the state for a single, in-flight query."""

    client_id: str
    query_id: str
    query_enum: int
    query_type: QueryType
    consolidated_data: Dict[str, Any] = field(default_factory=dict)
    batch_counters: Dict[str, Dict[int, Dict[str, Any]]] = field(default_factory=dict)

    # Definitive Structure:
    # {table: {batch_num: {
    #     "total_shards": int,  # First dimension's total shards (for backward compatibility)
    #     "shards_info": [(total_shards, shard_num), ...],  # All sharding dimensions
    #     "shards": {shard_key: {
    #         "expected_copies": int,
    #         "received_copies": Set[int],
    #         "shard_info": [(total_shards, shard_num), ...]
    #     }}
    # }}}
    completed_tables: Set[str] = field(default_factory=set)
    eof_received: Dict[str, int] = field(default_factory=dict)
    last_update_time: float = field(default_factory=time.time)
    completed_batch_counts: Dict[str, int] = field(default_factory=dict)
    strategy: Any = field(default=None, repr=False)
    lock: threading.Lock = field(default_factory=threading.Lock, repr=False)
    q4_counts: Dict[str, Dict[str, int]] = field(default_factory=dict, repr=False)
    q4_users: Dict[str, Dict[str, str]] = field(default_factory=dict, repr=False)


class ResultsFinisher:
    """
    Processes batches using per-query locking mechanism.
    """

    def __init__(
        self,
        input_client: MessageMiddlewareQueue,
        output_client: MessageMiddlewareQueue,
    ):

        self.input_client = input_client
        self.output_client = output_client

        self.active_queries: Dict[Tuple[str, str], QueryState] = {}
        self.global_lock = threading.Lock()

        state_dir = os.getenv("RESULTS_FINISHER_STATE_DIR")
        self.persistence = FinisherPersistence(state_dir)

        logger.info("Initialized ResultsFinisher for processing.")

    def _process_message(self, body: bytes):
        """
        Entry point invoked by the middleware consumer.

        The middleware wrapper handles ACK/NACK decisions based on our return value
        and any raised exceptions. Returning True means "message handled, ACK it".
        """
        try:
            envelope = self._parse_envelope(body)
        except DecodeError as e:
            logger.error("Failed to decode envelope. Discarding message. Error: %s", e)
            return True
        except Exception as e:
            logger.critical(f"Unexpected error in message handler: {e}", exc_info=True)
            raise

        if envelope is None:
            return True

        return self._route_envelope(envelope, body)

    def _parse_envelope(self, body: bytes) -> Optional[Envelope]:
        """Parse raw bytes into an Envelope; returns None for empty bodies."""
        try:
            if not body:
                logger.warning("Received empty message body, ignoring.")
                return None
            envelope = Envelope()
            envelope.ParseFromString(body)
            return envelope
        except DecodeError:
            raise

    def _route_envelope(self, envelope: Envelope, raw_body: bytes) -> bool:
        """Handle supported envelope types; return True to ACK the message."""
        if envelope.type == MessageType.CLEAN_UP_MESSAGE:
            try:
                cleanup_msg = envelope.clean_up
                self._handle_client_cleanup(cleanup_msg)
            except Exception as exc:
                logger.error(
                    "Failed to handle client cleanup message: %s",
                    exc,
                    exc_info=True,
                )
            return True

        if envelope.type != MessageType.DATA_BATCH:
            logger.warning(
                "Received envelope with unsupported type %s, ignoring.",
                envelope.type,
            )
            return True

        return self._persist_and_process_batch(envelope, raw_body)

    def _persist_and_process_batch(self, envelope: Envelope, raw_body: bytes) -> bool:
        """Persist incoming batch to disk, then process it."""
        batch = envelope.data_batch
        metadata = self._build_batch_metadata(batch)
        client_id = metadata["client_id"]
        query_id = metadata["query_id"]

        try:
            record = self.persistence.save_batch(metadata, raw_body)
            self.persistence.append_manifest(client_id, query_id, record)
        except Exception as exc:
            logger.error("Failed to persist incoming batch: %s", exc, exc_info=True)
            raise

        try:
            self._process_persisted_record(record, envelope=envelope)
        except Exception as exc:
            logger.error(
                "Failed to process persisted batch %s: %s",
                record.get("id"),
                exc,
                exc_info=True,
            )
        return True

    def _handle_data_batch(self, batch: DataBatch):
        if not batch.query_ids:
            return

        client_id = getattr(batch, "client_id", None)
        if not client_id:
            logger.warning(
                "Received DataBatch without client_id. Ignoring message with query_ids=%s.",
                list(batch.query_ids),
            )
            return

        query_enum = int(batch.query_ids[0])
        query_id = str(query_enum)

        try:
            qtype = QueryType(int(query_id))
        except ValueError:
            logger.warning("Invalid query_id '%s'", query_id)
            return

        payload = batch.payload
        if payload is None:
            logger.warning(
                "Received DataBatch without payload for query '%s' (client %s). Ignoring.",
                query_id,
                client_id,
            )
            return

        table_name = payload.name
        try:
            table_name_str = TableName.Name(table_name)
        except ValueError:
            table_name_str = f"UNKNOWN({table_name})"
        table_type = TABLE_NAME_TO_TYPE.get(table_name)
        expected_tables = EXPECTED_TABLES.get(qtype, [])

        # Para Q4 aceptamos TransactionStores y Users
        if not table_type or table_type not in expected_tables:
            logging.warning(
                "Query %s received unexpected table type '%s'. Expected one of %s. Ignoring.",
                query_id,
                table_name_str,
                sorted(expected_tables),
            )
            return

        with self.global_lock:
            state = self._get_or_create_query_state(
                client_id=client_id,
                query_id=query_id,
                query_enum=query_enum,
                query_type=qtype,
            )

        is_complete = False
        with state.lock:
            should_process_rows = self._update_batch_accounting(state, table_type, batch)

            # Consolidación especial para Q4
            if should_process_rows and qtype == QueryType.Q4:
                self._q4_consolidate(state, table_type, batch)
            elif should_process_rows:
                self._consolidate_batch_data(state, table_type, batch)

            state.last_update_time = time.time()
            if self._is_query_complete(state):
                is_complete = True

        if is_complete:
            logger.info(
                "All tables for query '%s' (client %s) are complete. Finalizing.",
                query_id,
                client_id,
            )
            self._finalize_query(state)
            self._cleanup_query_state(client_id, query_id)
        return True

    def _build_batch_metadata(self, batch: DataBatch) -> Dict[str, Any]:
        payload = batch.payload
        batch_number = 0
        table_name = "UNKNOWN"
        if payload is not None:
            batch_number = int(getattr(payload, "batch_number", 0))
            try:
                table_name = TableName.Name(payload.name)
            except ValueError:
                table_name = str(payload.name)

        query_enum = int(batch.query_ids[0]) if batch.query_ids else -1
        metadata = {
            "client_id": getattr(batch, "client_id", ""),
            "query_enum": query_enum,
            "query_id": str(query_enum),
            "batch_number": batch_number,
            "table_name": table_name,
            "timestamp": time.time(),
        }
        return metadata

    def _process_persisted_record(self, record: Dict[str, str], envelope: Envelope = None):
        env = envelope
        if env is None:
            raw = self.persistence.load_batch_bytes(record)
            if raw is None:
                raise RuntimeError(f"Persisted batch bytes missing for id={record.get('id')}")
            env = Envelope()
            env.ParseFromString(raw)

        if env.type != MessageType.DATA_BATCH:
            raise ValueError("Persisted envelope is not a DATA_BATCH message.")

        self._handle_data_batch(env.data_batch)

    def _replay_persisted_batches(self):
        recovered = 0
        for record in self.persistence.iter_batches():
            try:
                self._process_persisted_record(record)
                recovered += 1
            except Exception as exc:
                logger.error(
                    "Failed to replay batch %s: %s", record.get("id"), exc, exc_info=True
                )
        if recovered:
            logger.info("Replayed %d persisted batches.", recovered)

    @staticmethod
    def _normalize_user_id(value: Any) -> str:
        s = "" if value is None else str(value)
        return s.split(".", 1)[0] if s.endswith(".0") else s

    def _q4_consolidate(self, state: QueryState, table_type: str, batch: DataBatch):
        rows = list(iterate_rows_as_dicts(batch.payload))
        if not rows:
            return

        if table_type == "TransactionStores":
            counts = state.q4_counts
            for r in rows:
                store = (r.get("store_name") or "").strip()
                uid = self._normalize_user_id(r.get("user_id"))
                if not store or not uid:
                    continue
                store_map = counts.setdefault(store, {})
                store_map[uid] = store_map.get(uid, 0) + 1

        elif table_type == "Users":
            for u in rows:
                uid = self._normalize_user_id(u.get("user_id"))
                if not uid:
                    continue
                birth = (u.get("birthdate") or "").strip()
                if birth or uid not in state.q4_users:
                    state.q4_users[uid] = birth

    def _get_or_create_query_state(
        self, client_id: str, query_id: str, query_enum: int, query_type: QueryType
    ) -> QueryState:
        key = (client_id, query_id)
        if key not in self.active_queries:
            logger.info(
                "New query detected: ID '%s', Type '%s', Client '%s'.",
                query_id,
                query_type.name,
                client_id,
            )
            self.active_queries[key] = QueryState(
                client_id=client_id,
                query_id=query_id,
                query_enum=query_enum,
                query_type=query_type,
                strategy=get_strategy(query_type),
            )

        logger.debug("Retrieved state for query '%s' (client %s).", query_id, client_id)
        return self.active_queries[key]

    def _update_batch_accounting(
        self, state: QueryState, table_type: str, batch: DataBatch
    ) -> bool:
        shards_info_pb = list(batch.shards_info)
        if shards_info_pb:
            shards_info = [
                (max(1, int(si.total_shards)), int(si.shard_number))
                for si in shards_info_pb
            ]
        else:
            shards_info = [(1, 0)]

        shard_key = "_".join(f"{total}_{shard}" for total, shard in shards_info)
        total_shards = shards_info[0][0]

        batch_number = int(getattr(batch.payload, "batch_number", 0))
        table_batches = state.batch_counters.setdefault(table_type, {})
        batch_info = table_batches.setdefault(
            batch_number,
            {
                "total_shards": total_shards,
                "shards": {},
                "shards_info": shards_info,
                "completed": False,
            },
        )
        batch_info["total_shards"] = total_shards

        copy_total, copy_index = self._extract_copy_info(None)

        shard_entry = batch_info["shards"].setdefault(
            shard_key,
            {
                "expected_copies": copy_total,
                "received_bitmap": [False] * max(1, copy_total),
                "received_count": 0,
                "shard_info": shards_info,
            },
        )

        if copy_total > shard_entry.get("expected_copies", 1):
            bitmap_ref = shard_entry.get("received_bitmap", [])
            additional_slots = copy_total - len(bitmap_ref)
            if additional_slots > 0:
                bitmap_ref.extend([False] * additional_slots)
            shard_entry["received_bitmap"] = bitmap_ref
            shard_entry["expected_copies"] = copy_total
        else:
            shard_entry["expected_copies"] = max(
                shard_entry.get("expected_copies", 1), copy_total
            )

        bitmap = shard_entry.setdefault(
            "received_bitmap", [False] * max(1, shard_entry.get("expected_copies", 1))
        )
        if copy_index >= len(bitmap):
            bitmap.extend([False] * (copy_index - len(bitmap) + 1))
        is_duplicate = bitmap[copy_index]
        if not is_duplicate:
            bitmap[copy_index] = True
            shard_entry["received_count"] = shard_entry.get("received_count", 0) + 1
        else:
            logger.debug(
                "Duplicate DataBatch detected for query '%s', table '%s', batch %s, shard %s, copy %s.",
                state.query_id,
                table_type,
                batch_number,
                shard_key,
                copy_index,
            )

        expected_total_shards = self._calculate_total_expected_shards(batch_info)
        received_shard_instances = self._count_received_shards(batch_info)

        logger.debug(
            "Query '%s': Received batch %s for table '%s', shard_info: %s, copy %s/%s. BATCH STATE: %s/%s shard copies received.",
            state.query_id,
            batch_number,
            table_type,
            shards_info,
            copy_index + 1,
            shard_entry.get("expected_copies", copy_total),
            received_shard_instances,
            expected_total_shards,
        )

        if (
            not batch_info.get("completed")
            and received_shard_instances >= expected_total_shards
        ):
            batch_info["completed"] = True
            state.completed_batch_counts[table_type] = (
                state.completed_batch_counts.get(table_type, 0) + 1
            )

        status = batch.payload.status
        if status == TableStatus.EOF:
            logging.info(
                "Received EOF for query '%s', table '%s', batch %s.",
                state.query_id,
                table_type,
                batch_number,
            )
            state.eof_received[table_type] = max(
                state.eof_received.get(table_type, 0), batch_number
            )

        if table_type in state.eof_received:
            self._check_and_mark_table_as_complete(state, table_type)

        return not is_duplicate

    def _consolidate_batch_data(
        self, state: QueryState, table_type: str, batch: DataBatch
    ):
        rows = list(iterate_rows_as_dicts(batch.payload))
        if not rows:
            return
        if state.strategy is None:
            state.strategy = get_strategy(state.query_type)
        state.strategy.consolidate(state.consolidated_data, table_type, rows)

    def _check_and_mark_table_as_complete(self, state: QueryState, table_type: str):
        max_batch_num = state.eof_received.get(table_type)
        if max_batch_num is None:
            return

        if table_type in state.completed_tables:
            return

        completed_batches = state.completed_batch_counts.get(table_type, 0)
        if completed_batches < max_batch_num:
            return

        # All batches up to EOF have reached completion once.
        state.completed_tables.add(table_type)
        logging.info(f"Query '{state.query_id}': Table '{table_type}' is now complete.")

    def _calculate_total_expected_shards(self, batch_info):
        """
        Calculate the total number of expected shard combinations based on the shards_info.

        Instead of generating all combinations, we simply multiply the total_shards values
        from all dimensions to get the total expected number of unique shard combinations.

        For example, if shards_info contains [(2, x), (3, y)], we expect 2*3 = 6 combinations total.
        """
        base_expected = 1
        shards_info = batch_info.get("shards_info", [])
        if not shards_info:
            base_expected = max(1, len(batch_info.get("shards", {})) or 0)
        else:
            for total_shards, _ in shards_info:
                base_expected *= max(1, int(total_shards))

        shards = batch_info.get("shards", {})
        if not shards:
            return base_expected

        additional_copies = 0
        for shard_entry in shards.values():
            expected_copies = max(1, int(shard_entry.get("expected_copies", 1)))
            additional_copies += expected_copies - 1

        return base_expected + additional_copies

    def _count_received_shards(self, batch_info) -> int:
        shards = batch_info.get("shards", {})
        total_received = 0
        for shard_entry in shards.values():
            if "received_count" in shard_entry:
                total_received += int(shard_entry.get("received_count", 0))
            else:
                bitmap = shard_entry.get("received_bitmap")
                if isinstance(bitmap, list):
                    total_received += sum(1 for flag in bitmap if flag)
        return total_received

    def _extract_copy_info(self, meta: Optional[Dict[int, int]]) -> Tuple[int, int]:
        if not meta:
            return 1, 0

        try:
            normalized = {int(k): int(v) for k, v in meta.items()}
        except (TypeError, ValueError):
            return 1, 0

        positive_keys = [k for k in normalized.keys() if k > 0]
        if not positive_keys:
            return 1, 0

        copy_total = max(positive_keys)
        copy_index = normalized.get(copy_total, 0)

        copy_total = max(1, copy_total)
        if copy_index < 0:
            copy_index = 0
        if copy_index >= copy_total:
            copy_index = copy_index % copy_total

        return copy_total, copy_index

    def _is_query_complete(self, state: QueryState) -> bool:
        expected = EXPECTED_TABLES.get(state.query_type, [])
        if not expected:
            return False
        # Se marca cada tabla en _check_and_mark_table_as_complete()
        return all(t in state.completed_tables for t in expected)

    def _finalize_query(self, state: QueryState):
        try:
            if state.query_type == QueryType.Q4:
                result = self._q4_finalize(state)
                self._send_result(state, "success", result)
                return

            # queries anteriores: usar estrategia existente
            if state.strategy is None:
                state.strategy = get_strategy(state.query_type)
            final_result = state.strategy.finalize(state.consolidated_data)
            self._send_result(state, "success", final_result)
        except Exception as e:
            logger.error(
                f"Error during finalization of query '{state.query_id}': {e}",
                exc_info=True,
            )
            self._send_result(state, "error", error_message=str(e))

    def _q4_finalize(self, state: QueryState) -> Dict[str, list]:
        """
        Devuelve: { store_name: [ {birthdate: str, purchase_count: int}, ... (top-3) ] }
        """
        counts = state.q4_counts or {}
        users = state.q4_users or {}

        out: Dict[str, list] = {}
        for store, umap in counts.items():
            top = sorted(umap.items(), key=lambda kv: (-kv[1], kv[0]))[:3]
            rows = []
            for uid, cnt in top:
                rows.append(
                    {
                        "birthdate": users.get(uid, ""),
                        "purchase_count": cnt,
                    }
                )
            out[store] = rows
        return out

    def _cleanup_query_state(
        self, client_id: str, query_id: str, *, remove_persisted: bool = True
    ):
        with self.global_lock:
            key = (client_id, query_id)
            if key in self.active_queries:
                del self.active_queries[key]
        if remove_persisted:
            self._remove_persisted_query_files(client_id, query_id)
        logger.info(
            "In-memory cleanup complete for query '%s' (client %s).",
            query_id,
            client_id,
        )

    def _send_result(
        self,
        state: QueryState,
        status: str,
        result: Any = None,
        error_message: str = "",
    ):
        logger.info("Sending result for query '%s'", state.query_id)
        """Send query results using the appropriate protocol message type."""
        try:
            if status != "success" or result is None:
                columns = ["query_id", "error_code", "error_message"]
                rows = [
                    [
                        state.query_id,
                        "EXECUTION_ERROR" if status == "error" else "NULL_RESULT",
                        error_message or "Unknown error",
                    ]
                ]
                table_enum = TableName.TRANSACTIONS
                table_status = TableStatus.CANCEL
            else:
                table_enum, columns, rows = self._create_result_message(
                    state.query_type, result
                )
                if state.query_type == QueryType.Q1 and not rows:
                    return
                table_status = TableStatus.EOF

            table_data = build_table_data(
                table_name=table_enum,
                columns=columns,
                rows=[
                    ["" if value is None else str(value) for value in row]
                    for row in rows
                ],
                batch_number=1,
                status=table_status,
            )

            out_batch = DataBatch()
            out_batch.client_id = state.client_id
            out_batch.query_ids.append(state.query_enum)
            out_batch.payload.CopyFrom(table_data)

            envelope = Envelope(type=MessageType.DATA_BATCH, data_batch=out_batch)
            self.output_client.send(envelope.SerializeToString())
            logger.info(
                "Sent %s result for query '%s' using protobuf DataBatch",
                status,
                state.query_id,
            )
        except Exception as e:
            try:
                fallback_data = build_table_data(
                    table_name=TableName.TRANSACTIONS,
                    columns=["query_id", "error_code", "error_message"],
                    rows=[
                        [
                            state.query_id,
                            "INTERNAL_ERROR",
                            f"Internal error while sending result: {str(e)}",
                        ]
                    ],
                    batch_number=1,
                    status=TableStatus.CANCEL,
                )

                fallback_batch = DataBatch()
                fallback_batch.client_id = state.client_id
                fallback_batch.query_ids.append(state.query_enum)
                fallback_batch.payload.CopyFrom(fallback_data)
                envelope = Envelope(
                    type=MessageType.DATA_BATCH, data_batch=fallback_batch
                )
                self.output_client.send(envelope.SerializeToString())
            except Exception as fatal_e:
                logger.critical(
                    "FATAL: Failed to send even emergency error message: %s",
                    fatal_e,
                )

            logger.error(
                "Failed to create normal protocol message for query %s: %s",
                state.query_id,
                e,
                exc_info=True,
            )

    def _remove_persisted_query_files(self, client_id: str, query_id: str):
        entries = self.persistence.load_manifest(client_id, query_id)
        for entry in entries:
            data_file = entry.get("data_file")
            if data_file:
                data_path = os.path.join(self.persistence.batches_dir, data_file)
                if os.path.exists(data_path):
                    try:
                        os.remove(data_path)
                    except OSError:
                        pass
        self.persistence.delete_manifest(client_id, query_id)

    def _create_result_message(
        self, query_type: QueryType, result: Any
    ) -> Tuple[TableName, list[str], list[list[Any]]]:
        """Create the table schema and rows for the final result."""
        logging.info("Creating result message for query type %s", query_type.name)
        if query_type == QueryType.Q1:
            rows = [
                [
                    tx.get("transaction_id", ""),
                    tx.get("final_amount", 0),
                ]
                for tx in result.get("transactions", [])
            ]
            return TableName.TRANSACTIONS, ["transaction_id", "final_amount"], rows

        if query_type == QueryType.Q2:
            rows: list[list[Any]] = []
            for month, data in result.items():
                for entry in data.get("by_quantity", []):
                    rows.append(
                        [
                            month,
                            entry.get("name", ""),
                            entry.get("quantity", 0),
                            "",
                        ]
                    )
                for entry in data.get("by_revenue", []):
                    rows.append(
                        [
                            month,
                            entry.get("name", ""),
                            "",
                            entry.get("revenue", 0),
                        ]
                    )
            return (
                TableName.TRANSACTION_ITEMS_MENU_ITEMS,
                ["month", "name", "quantity", "revenue"],
                rows,
            )

        if query_type == QueryType.Q3:
            rows = [
                [store_name, period, amount]
                for store_name, periods in result.items()
                for period, amount in periods.items()
            ]
            return (
                TableName.TRANSACTION_STORES,
                ["store_name", "period", "amount"],
                rows,
            )

        if query_type == QueryType.Q4:
            rows = [
                [
                    store_name,
                    customer.get("birthdate", ""),
                    customer.get("purchase_count", 0),
                ]
                for store_name, customers in result.items()
                for customer in customers
            ]
            return (
                TableName.TRANSACTION_STORES,
                ["store_name", "birthdate", "purchase_count"],
                rows,
            )

        raise ValueError(f"No result message type defined for query type {query_type}")

    def start(self):
        logger.info("ResultsFinisher is starting...")
        self._replay_persisted_batches()
        self.input_client.start_consuming(self._process_message)

    def stop(self):
        """Stop the finisher gracefully.
        
        IMPORTANT: Order of operations matters to avoid race conditions:
        1. Stop consuming and wait for in-flight messages to complete
        2. Clean up active queries (may need to send final results)
        3. Close output client (no more sends after this)
        4. Close input client connection
        """
        logger.info("ResultsFinisher is shutting down...")
        # First, stop consuming and wait for in-flight messages to complete.
        # This blocks until the consumer thread has finished processing.
        self.input_client.stop_consuming()
        
        # Preserve persisted batches on shutdown so queries can be replayed
        # when the container restarts. Only remove disk state when a query
        # finishes successfully (see _cleanup_query_state defaults).
        # NOTE: This must happen BEFORE closing output_client in case cleanup
        # triggers any final result sends.
        self._cleanup_active_queries(remove_persisted=False)
        
        # Now it's safe to close output client - no more sends in progress
        self.output_client.close()
        
        # Finally close the input client connection
        self.input_client.close()
        logger.info("ResultsFinisher has stopped.")

    def _cleanup_active_queries(self, *, remove_persisted: bool = True):
        with self.global_lock:
            active = list(self.active_queries.keys())
        for client_id, query_id in active:
            try:
                self._cleanup_query_state(
                    client_id, query_id, remove_persisted=remove_persisted
                )
            except Exception as exc:
                logger.warning(
                    "Failed to cleanup query '%s' (client %s) during shutdown: %s",
                    query_id,
                    client_id,
                    exc,
                )

    def _handle_client_cleanup(self, cleanup_msg) -> None:
        """Handle client cleanup message from orchestrator."""
        client_id = cleanup_msg.client_id if cleanup_msg.client_id else ""
        if not client_id:
            logger.warning("Received client_cleanup message with empty client_id")
            return

        logger.info(
            "action: handle_client_cleanup | result: starting | client_id: %s",
            client_id,
        )
        self._cleanup_client_state(client_id)
        logger.info(
            "action: handle_client_cleanup | result: success | client_id: %s",
            client_id,
        )

    def _cleanup_client_state(self, client_id: str) -> None:
        """
        Clean all in-memory and persisted state for a given client_id.
        This includes:
        - Active queries in memory
        - All persisted manifests and batch files for the client
        """
        if not client_id:
            logger.warning("_cleanup_client_state called with empty client_id")
            return

        logger.info(
            "action: cleanup_client_state | result: starting | client_id: %s",
            client_id,
        )

        # Clean in-memory active queries
        with self.global_lock:
            keys_to_remove = [
                key for key in self.active_queries.keys() if key[0] == client_id
            ]
            for key in keys_to_remove:
                del self.active_queries[key]
                logger.debug(
                    "Removed active query '%s' (client %s) from memory",
                    key[1],
                    client_id,
                )

        # Clean persisted state
        # Find all manifest files for this client_id
        manifest_prefix = f"{client_id}__"
        manifest_dir = self.persistence.manifest_dir

        if not os.path.exists(manifest_dir):
            logger.debug(
                "Manifest directory does not exist, skipping persisted cleanup for client %s",
                client_id,
            )
            return

        try:
            manifest_files = [
                f
                for f in os.listdir(manifest_dir)
                if f.startswith(manifest_prefix) and f.endswith(".json")
            ]

            for manifest_file in manifest_files:
                # Extract query_id from filename: {client_id}__{query_id}.json
                query_id = manifest_file[len(manifest_prefix) : -5]  # Remove prefix and .json

                try:
                    # Load manifest and delete all associated batch files
                    entries = self.persistence.load_manifest(client_id, query_id)
                    for entry in entries:
                        data_file = entry.get("data_file")
                        if data_file:
                            data_path = os.path.join(
                                self.persistence.batches_dir, data_file
                            )
                            if os.path.exists(data_path):
                                try:
                                    os.remove(data_path)
                                    logger.debug(
                                        "Deleted batch data file: %s", data_path
                                    )
                                except OSError as e:
                                    logger.warning(
                                        "Failed to delete batch data file %s: %s",
                                        data_path,
                                        e,
                                    )                    # Delete the manifest file
                    self.persistence.delete_manifest(client_id, query_id)
                    logger.debug(
                        "Deleted manifest for query '%s' (client %s)",
                        query_id,
                        client_id,
                    )
                except Exception as e:
                    logger.warning(
                        "Failed to cleanup manifest %s for client %s: %s",
                        manifest_file,
                        client_id,
                        e,
                    )

            logger.info(
                "action: cleanup_client_state | result: success | client_id: %s | manifests_cleaned: %d",
                client_id,
                len(manifest_files),
            )
        except Exception as e:
            logger.error(
                "Failed to cleanup persisted state for client %s: %s",
                client_id,
                e,
                exc_info=True,
            )
