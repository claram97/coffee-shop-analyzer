import logging
import os
import threading
import time
from dataclasses import dataclass, field
from typing import Dict, Any, Set

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
    #     "shards": {shard_key: {"received": bool, "shard_info": [(total_shards, shard_num), ...]}}
    # }}}
    completed_tables: Set[str] = field(default_factory=set)
    eof_received: Dict[str, int] = field(default_factory=dict)
    last_update_time: float = field(default_factory=time.time)
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
                self.active_queries[query_id] = QueryState(query_id=query_id, query_type=query_type)
            except ValueError:
                raise ValueError(f"Cannot determine a valid QueryType from query_id '{query_id}'")
        
        logger.info(f"Retrieved state for query '{query_id}'.")
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
            "shards_info": shards_info  # Store the complete shards_info for verification
        })

        batch_query_id = batch.query_ids[0] if batch.query_ids else None
        
        # Update the total_shards with the first dimension's total shards value
        batch_info["total_shards"] = total_shards
        
        # Use the combined shard key for tracking
        batch_info["shards"].setdefault(shard_key, {
            "received": True,
            "shard_info": shards_info  # Store which specific shard configuration this is
        })

        # Calculate the number of received vs expected shards for this batch
        expected_total_shards = self._calculate_total_expected_shards(batch_info)
        received_shard_keys = set(batch_info["shards"].keys())
        
        logger.info(f"Query '{batch_query_id}': Received batch {batch.batch_number} for table '{table_type}', shard_info: {shards_info}. BATCH STATE: {len(received_shard_keys)}/{expected_total_shards} shards received.")
        
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
        strategy = get_strategy(state.query_type)
        strategy.consolidate(state.consolidated_data, table_type, rows)

    def _check_and_mark_table_as_complete(self, state: QueryState, table_type: str):
        max_batch_num = state.eof_received.get(table_type)
        if max_batch_num is None:
            return

        all_batches_info = state.batch_counters.get(table_type, {})

        for i in range(1, max_batch_num + 1):
            batch_info = all_batches_info.get(i)
            if not batch_info: 
                return
            
            # Calculate the expected total number of shard combinations
            expected_total_shards = self._calculate_total_expected_shards(batch_info)
            
            # Get the actual shard keys we've received
            received_shard_keys = set(batch_info["shards"].keys())
            
            # Check if we've received all expected shard combinations
            if len(received_shard_keys) < expected_total_shards:
                logger.info(f"Query '{state.query_id}': Table '{table_type}' batch {i} has {len(received_shard_keys)} of {expected_total_shards} expected shards.")
                return
        
        # If all checks pass for all batches up to the EOF, the table is complete.
        state.completed_tables.add(table_type)
        logging.info(f"Query '{state.query_id}': Table '{table_type}' is now complete.")
    
    def _calculate_total_expected_shards(self, batch_info):
        """
        Calculate the total number of expected shard combinations based on the shards_info.
        
        Instead of generating all combinations, we simply multiply the total_shards values
        from all dimensions to get the total expected number of unique shard combinations.
        
        For example, if shards_info contains [(2, x), (3, y)], we expect 2*3 = 6 combinations total.
        """
        if not batch_info.get("shards"):
            return 0
        
        # Get the shards_info from the batch_info
        shards_info = batch_info.get("shards_info", [])
        
        # Simply multiply all total_shards values to get the total number of combinations
        total = 1
        for total_shards, _ in shards_info:
            total *= total_shards
            
        return total

    def _is_query_complete(self, state: QueryState) -> bool:
        expected_table = QUERY_TYPE_TO_TABLE.get(state.query_type)
        if not expected_table:
            return False
        return expected_table in state.completed_tables

    def _finalize_query(self, state: QueryState):
        try:
            strategy = get_strategy(state.query_type)
            final_result = strategy.finalize(state.consolidated_data)
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
            for month, data in result.items():
                # Handle quantity metrics
                for product in data.get('by_quantity', []):
                    message.rows.append(entities.ResultProductMetrics(
                        month=month,
                        name=product.get('name', ''),
                        quantity=str(product.get('quantity', 0)),
                        revenue=None
                    ))
                # Handle revenue metrics
                for product in data.get('by_revenue', []):
                    message.rows.append(entities.ResultProductMetrics(
                        month=month,
                        name=product.get('name', ''),
                        quantity=None,
                        revenue=str(product.get('revenue', 0))
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