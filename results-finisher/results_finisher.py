import logging
import os
import threading
import time
from dataclasses import dataclass, field
from typing import Dict, Any, Set

from middleware_client import MessageMiddlewareQueue
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
    #     "total_shards": int,
    #     "shards": {shard_num: {"received_copies": set(), "total_copies": int}}
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
            logger.warning(f"Query {query_id} received unexpected table type '{table_type}'. Expected '{expected_table}'. Ignoring.")
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
        
        return self.active_queries[query_id]

    def _update_batch_accounting(self, state: QueryState, table_type: str, batch: DataBatch):
        if batch.meta:
            total_copies_key = next(iter(batch.meta.keys()), 0)
            copy_num = batch.meta.get(total_copies_key, 0)
            total_copies = total_copies_key
        else:
            total_copies = 1
            copy_num = 0
            
        if total_copies <= 0:
            total_copies = 1
        
        total_shards = batch.total_shards if batch.total_shards > 0 else 1
        shard_num = batch.shard_num if batch.total_shards > 0 else 1

        table_batches = state.batch_counters.setdefault(table_type, {})
        batch_info = table_batches.setdefault(batch.batch_number, {
            "total_shards": total_shards,
            "shards": {}
        })
        
        batch_info["total_shards"] = total_shards

        shard_copies = batch_info["shards"].setdefault(shard_num, {
            "received_copies": set(),
            "total_copies": total_copies
        })
        
        shard_copies["total_copies"] = total_copies
        
        shard_copies["received_copies"].add(copy_num)
        
        if batch.batch_msg.batch_status == BatchStatus.EOF:
            state.eof_received[table_type] = max(state.eof_received.get(table_type, 0), batch.batch_number)

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
            if not batch_info: return # Batch 'i' has not arrived yet.

            # Level 1 Check: Have all shards for this batch arrived?
            if len(batch_info["shards"]) != batch_info["total_shards"]:
                return

            # Level 2 Check: For each shard, have all copies arrived?
            for shard_num, shard_copies in batch_info["shards"].items():
                if len(shard_copies["received_copies"]) != shard_copies["total_copies"]:
                    return
        
        # If all checks pass for all batches up to the EOF, the table is complete.
        state.completed_tables.add(table_type)
        logger.info(f"Query '{state.query_id}': Table '{table_type}' is now complete.")

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
                    opcode=Opcodes.DATA_BATCH,
                    query_ids=[int(query_id)] if query_id.isdigit() else [],
                    meta={},
                    table_ids=[error_result.opcode],
                    batch_bytes=error_result.to_bytes(),
                    batch_number=1,
                    total_shards=1,
                    shard_num=1
                )
                
                self.output_client.send(batch.to_bytes())
                logger.info(f"Sent error result for query '{query_id}' using protocol message")
                return
            
            # For successful results, use the appropriate message type
            query_type = QueryType(int(query_id))
            result_message = self._create_result_message(query_type, result)
            
            # Wrap the result in a DataBatch for consistent protocol handling
            batch = DataBatch(
                opcode=Opcodes.DATA_BATCH,
                query_ids=[int(query_id)],
                meta={},
                table_ids=[result_message.opcode],
                batch_bytes=result_message.to_bytes(),
                batch_number=1,
                total_shards=1,
                shard_num=1
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
                    opcode=Opcodes.DATA_BATCH,
                    query_ids=[],  # May not have valid query ID at this point
                    meta={},
                    table_ids=[emergency_error.opcode],
                    batch_bytes=emergency_error.to_bytes(),
                    batch_number=1,
                    total_shards=1,
                    shard_num=1
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