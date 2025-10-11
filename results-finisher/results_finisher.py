import logging
import os
import threading
import time
from dataclasses import dataclass, field
from typing import Dict, Any, Set, Tuple

from middleware.middleware_client import MessageMiddlewareQueue
from protocol import ProtocolError, Opcodes, BatchStatus, DataBatch
from protocol import messages, entities
from protocol.messages import TableMessage

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(name)s] - %(message)s'
)
logger = logging.getLogger(__name__)

# This is for testing different ways of handling the data.
# Will use this when we have the whole system running.
STRATEGY_MODE = os.getenv("STRATEGY_MODE", "append_only").lower()
if STRATEGY_MODE == "incremental":
    from query_strategy_incremental import get_strategy
    logger.info("Using INCREMENTAL aggregation strategy.")
else:
    from query_strategy_append_only import get_strategy
    logger.info("Using APPEND-ONLY aggregation strategy.")
from constants import QueryType

OPCODE_TO_TABLE_TYPE = {
    Opcodes.NEW_TRANSACTION: "Transactions",
    Opcodes.NEW_STORES: "Stores",
    Opcodes.NEW_MENU_ITEMS: "MenuItems",
    Opcodes.NEW_TRANSACTION_ITEMS: "TransactionItems",
    Opcodes.NEW_USERS: "Users",
    Opcodes.NEW_TRANSACTION_STORES: "TransactionStores",
    Opcodes.NEW_TRANSACTION_ITEMS_MENU_ITEMS: "TransactionItemsMenuItems",
    Opcodes.NEW_TRANSACTION_STORES_USERS: "TransactionStoresUsers",
}

QUERY_TYPE_TO_TABLE = {
    QueryType.Q1: "Transactions",
    QueryType.Q2: "TransactionItemsMenuItems",
    QueryType.Q3: "TransactionStores",
    QueryType.Q4: "TransactionStoresUsers",
}

@dataclass
class QueryState:
    """Represents the state for a single, in-flight query."""
    query_id: str
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

class ResultsFinisher:
    """
    Processes batches using per-query locking mechanism.
    """
    def __init__(self,
                 input_client: MessageMiddlewareQueue,
                 output_client: MessageMiddlewareQueue):
        
        self.input_client = input_client
        self.output_client = output_client
        
        self.active_queries: Dict[str, QueryState] = {}
        self.global_lock = threading.Lock()

        logger.info("Initialized ResultsFinisher for processing.")

    def _process_message(self, body: bytes):
        try:
            if not body or body[0] != Opcodes.DATA_BATCH:
                logger.warning("Received non-DATA_BATCH message, ignoring.")
                return

            batch = DataBatch.deserialize_from_bytes(body)
            self._handle_data_batch(batch)

        except (ProtocolError, ValueError) as e:
            logger.error(f"Message is malformed or invalid, discarding. Error: {e}")
        except Exception as e:
            logger.critical(f"Unexpected error in message handler: {e}", exc_info=True)

    def _handle_data_batch(self, batch: DataBatch):
        if not batch.query_ids:
            return
        
        query_id = str(batch.query_ids[0])
        table_type = OPCODE_TO_TABLE_TYPE.get(batch.batch_msg.opcode)
        expected_table = QUERY_TYPE_TO_TABLE.get(QueryType(int(query_id)))

        # If the batch's table type doesn't match the single one we expect for this query, ignore it.
        if not table_type or table_type != expected_table:
            logging.warning(f"Query {query_id} received unexpected table type '{table_type}'. Expected '{expected_table}'. Ignoring.")
            return

        with self.global_lock:
            state = self._get_or_create_query_state(query_id)
        
        is_complete = False
        with state.lock:
            self._update_batch_accounting(state, table_type, batch)
            self._consolidate_batch_data(state, table_type, batch)
            state.last_update_time = time.time()
            if self._is_query_complete(state):
                is_complete = True
        
        if is_complete:
            logger.info(f"All tables for query '{query_id}' are complete. Finalizing.")
            self._finalize_query(state)
            self._cleanup_query_state(query_id)

    def _get_or_create_query_state(self, query_id: str) -> QueryState:
        if query_id not in self.active_queries:
            try:
                query_type = QueryType(int(query_id))
                logger.info(f"New query detected: ID '{query_id}', Type '{query_type.name}'.")
                self.active_queries[query_id] = QueryState(
                    query_id=query_id,
                    query_type=query_type,
                    strategy=get_strategy(query_type)
                )
            except ValueError:
                raise ValueError(f"Cannot determine a valid QueryType from query_id '{query_id}'")
        
        logger.debug(f"Retrieved state for query '{query_id}'.")
        return self.active_queries[query_id]

    def _update_batch_accounting(self, state: QueryState, table_type: str, batch: DataBatch):
        shards_info = batch.shards_info if batch.shards_info else [(1, 0)]

        # Create a unique shard key from all sharding dimensions
        shard_key = "_".join([f"{total_shards}_{shard_num}" for total_shards, shard_num in shards_info])

        # Extract the primary shard dimension for single-dimension use cases
        primary_shard = shards_info[0]
        total_shards, shard_num = primary_shard

        table_batches = state.batch_counters.setdefault(table_type, {})
        batch_info = table_batches.setdefault(batch.batch_number, {
            "total_shards": total_shards,
            "shards": {},
            "shards_info": shards_info,  # Store the complete shards_info for verification
            "completed": False
        })
        batch_info.setdefault("completed", False)

        batch_query_id = batch.query_ids[0] if batch.query_ids else None

        # Update the total_shards with the first dimension's total shards value
        batch_info["total_shards"] = total_shards

        copy_total, copy_index = self._extract_copy_info(getattr(batch, "meta", {}))

        shard_entry = batch_info["shards"].setdefault(shard_key, {
            "expected_copies": copy_total,
            "received_bitmap": [False] * max(1, copy_total),
            "received_count": 0,
            "shard_info": shards_info
        })
        shard_entry.setdefault("received_count", 0)

        if copy_total > shard_entry.get("expected_copies", 1):
            bitmap_ref = shard_entry.get("received_bitmap", [])
            additional_slots = copy_total - len(bitmap_ref)
            if additional_slots > 0:
                bitmap_ref.extend([False] * additional_slots)
            shard_entry["received_bitmap"] = bitmap_ref
            shard_entry["expected_copies"] = copy_total
        else:
            shard_entry["expected_copies"] = max(shard_entry.get("expected_copies", 1), copy_total)

        bitmap = shard_entry.setdefault("received_bitmap", [False] * max(1, shard_entry.get("expected_copies", 1)))
        if copy_index >= len(bitmap):
            bitmap.extend([False] * (copy_index - len(bitmap) + 1))
        if not bitmap[copy_index]:
            bitmap[copy_index] = True
            shard_entry["received_count"] = shard_entry.get("received_count", 0) + 1

        # Calculate the number of received vs expected shards for this batch
        expected_total_shards = self._calculate_total_expected_shards(batch_info)
        received_shard_instances = self._count_received_shards(batch_info)

        logger.debug(
            "Query '%s': Received batch %s for table '%s', shard_info: %s, copy %s/%s. BATCH STATE: %s/%s shard copies received.",
            batch_query_id,
            batch.batch_number,
            table_type,
            shards_info,
            copy_index + 1,
            shard_entry.get("expected_copies", copy_total),
            received_shard_instances,
            expected_total_shards
        )

        if not batch_info.get("completed") and received_shard_instances >= expected_total_shards:
            batch_info["completed"] = True
            state.completed_batch_counts[table_type] = state.completed_batch_counts.get(table_type, 0) + 1
        
        # If this is an EOF batch, record it
        if batch.batch_msg.batch_status == BatchStatus.EOF:
            logging.info(f"Received EOF for query '{state.query_id}', table '{table_type}', batch {batch.batch_number}.")
            state.eof_received[table_type] = max(state.eof_received.get(table_type, 0), batch.batch_number)
        
        # Check for completion if we have already received an EOF for this table
        # This handles both: 
        # 1. The current batch is an EOF
        # 2. EOF arrived earlier but we're still receiving shard batches
        if table_type in state.eof_received:
            self._check_and_mark_table_as_complete(state, table_type)
    
    def _consolidate_batch_data(self, state: QueryState, table_type: str, batch: DataBatch):
        rows = getattr(batch.batch_msg, 'rows', [])
        if not rows: return
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

    def _extract_copy_info(self, meta: Dict[int, int]) -> Tuple[int, int]:
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
        expected_table = QUERY_TYPE_TO_TABLE.get(state.query_type)
        if not expected_table:
            return False
        return expected_table in state.completed_tables

    def _finalize_query(self, state: QueryState):
        try:
            if state.strategy is None:
                state.strategy = get_strategy(state.query_type)
            final_result = state.strategy.finalize(state.consolidated_data)
            self._send_result(state.query_id, "success", final_result)
        except Exception as e:
            logger.error(f"Error during finalization of query '{state.query_id}': {e}", exc_info=True)
            self._send_result(state.query_id, "error", error_message=str(e))

    def _cleanup_query_state(self, query_id: str):
        with self.global_lock:
            if query_id in self.active_queries:
                del self.active_queries[query_id]
            logger.info(f"In-memory cleanup complete for query '{query_id}'.")

    def _send_result(self, query_id: str, status: str, result: Any = None, error_message: str = ""):
        """Send query results using the appropriate protocol message type."""
        try:
            if status != "success" or result is None:
                # For error cases, create an error message
                error_result = messages.QueryResultError()
                error_result.rows.append(entities.ResultError(
                    query_id=query_id,
                    error_code="EXECUTION_ERROR" if status == "error" else "NULL_RESULT",
                    error_message=error_message or "Unknown error"
                ))
                
                # Wrap the error in a DataBatch
                batch = DataBatch(
                    query_ids=[int(query_id)] if query_id.isdigit() else [],
                    meta={},
                    table_ids=[error_result.opcode],
                    batch_bytes=error_result.to_bytes(),
                    shards_info=[(1, 0)]  # Standard format for no sharding
                )
                
                self.output_client.send(batch.to_bytes())
                logger.info(f"Sent error result for query '{query_id}' using protocol message")
                return
            
            # For successful results, use the appropriate message type
            query_type = QueryType(int(query_id))
            result_message = self._create_result_message(query_type, result)
            
            # Wrap the result in a DataBatch for consistent protocol handling
            batch = DataBatch(
                query_ids=[int(query_id)],
                meta={},
                table_ids=[result_message.opcode],
                batch_bytes=result_message.to_bytes(),
                shards_info=[(1, 0)]  # Standard format for no sharding
            )
            
            self.output_client.send(batch.to_bytes())
            logger.info(f"Sent final result for query '{query_id}' using protocol message")
        except Exception as e:
            # Emergency error handling using minimal protocol message
            # Even in case of internal errors, we still use the protocol
            try:
                emergency_error = messages.QueryResultError()
                emergency_error.rows.append(entities.ResultError(
                    query_id=query_id,
                    error_code="INTERNAL_ERROR",
                    error_message=f"Internal error while sending result: {str(e)}"
                ))
                
                emergency_batch = DataBatch(
                    query_ids=[],  # May not have valid query ID at this point
                    meta={},
                    table_ids=[emergency_error.opcode],
                    batch_bytes=emergency_error.to_bytes(),
                    shards_info=[(1, 0)]  # Standard format for no sharding
                )
                
                self.output_client.send(emergency_batch.to_bytes())
            except Exception as fatal_e:
                logger.critical(f"FATAL: Failed to send even emergency error message: {fatal_e}")
            
            logger.error(f"Failed to create normal protocol message for query {query_id}: {e}", exc_info=True)
            
    def _create_result_message(self, query_type: QueryType, result: Any) -> TableMessage:
        """Create the appropriate result message for the query type."""
        if query_type == QueryType.Q1:
            # Q1: Filtered transactions
            message = messages.QueryResult1()
            for tx in result.get('transactions', []):
                message.rows.append(entities.ResultFilteredTransaction(
                    transaction_id=str(tx.get('transaction_id', '')),
                    final_amount=str(tx.get('final_amount', 0))
                ))
            return message
            
        elif query_type == QueryType.Q2:
            # Q2: Product metrics by month
            message = messages.QueryResult2()
            # Create a dictionary to merge quantity and revenue for each product
            for month, data in result.items():
                # Create a dictionary to store product metrics by name
                product_metrics = {}
                
                # Process quantity metrics
                for product in data.get('by_quantity', []):
                    name = product.get('name', '')
                    quantity = product.get('quantity', 0)
                    product_metrics[name] = product_metrics.get(name, {'quantity': 0, 'revenue': 0.0})
                    product_metrics[name]['quantity'] = quantity
                
                # Process revenue metrics
                for product in data.get('by_revenue', []):
                    name = product.get('name', '')
                    revenue = product.get('revenue', 0.0)
                    product_metrics[name] = product_metrics.get(name, {'quantity': 0, 'revenue': 0.0})
                    product_metrics[name]['revenue'] = revenue
                
                # Create result rows with both quantity and revenue
                for name, metrics in product_metrics.items():
                    # Only include products with either non-zero quantity or revenue
                    if metrics['quantity'] != 0 or metrics['revenue'] != 0:
                        message.rows.append(entities.ResultProductMetrics(
                            month=month,
                            name=name,
                            quantity=str(metrics['quantity']),
                            revenue=str(metrics['revenue'])
                        ))
            return message
            
        elif query_type == QueryType.Q3:
            # Q3: TPV analysis by store and period
            message = messages.QueryResult3()
            for store_name, periods in result.items():
                for period, amount in periods.items():
                    message.rows.append(entities.ResultStoreTPV(
                        store_name=store_name,
                        period=period,
                        amount=str(amount)
                    ))
            return message
            
        elif query_type == QueryType.Q4:
            # Q4: Top customers by store
            message = messages.QueryResult4()
            for store_name, customers in result.items():
                for customer in customers:
                    message.rows.append(entities.ResultTopCustomer(
                        store_name=store_name,
                        birthdate=customer.get('birthdate', ''),
                        purchase_count=str(customer.get('purchase_count', 0))
                    ))
            return message
            
        else:
            raise ValueError(f"No result message type defined for query type {query_type}")


    def start(self):
        logger.info("ResultsFinisher is starting...")
        self.input_client.start_consuming(self._process_message)

    def stop(self):
        logger.info("ResultsFinisher is shutting down...")
        self.input_client.stop_consuming()
        self.input_client.close()
        self.output_client.close()
        logger.info("ResultsFinisher has stopped.")