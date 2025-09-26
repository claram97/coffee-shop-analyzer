"""
Batch processing functions for creating filtered DataBatch messages.
"""

import logging
from typing import Dict, List, Callable

from ..protocol import Opcodes, DataBatch
from .filters import (
    filter_menu_items_columns, filter_stores_columns, filter_transaction_items_columns,
    filter_transactions_columns, filter_users_columns, filter_vouchers_columns
)
from .serialization import (
    serialize_filtered_menu_items, serialize_filtered_stores, serialize_filtered_transaction_items,
    serialize_filtered_transactions, serialize_filtered_users
)


class BatchProcessor:
    """Processes and filters batch messages for downstream consumers."""
    
    def __init__(self):
        """Initialize batch processor with filter and serialization mappings."""
        self._filter_functions = self._get_filter_functions_mapping()
        self._serialize_functions = self._get_serialize_functions_mapping()
        self._query_mappings = self._get_query_mappings()
        self._table_names = self._get_table_names_mapping()
        
    def _get_filter_functions_mapping(self) -> Dict[int, Callable]:
        """Return mapping of opcodes to filter functions."""
        return {
            Opcodes.NEW_MENU_ITEMS: filter_menu_items_columns,
            Opcodes.NEW_STORES: filter_stores_columns,
            Opcodes.NEW_TRANSACTION_ITEMS: filter_transaction_items_columns,
            Opcodes.NEW_TRANSACTION: filter_transactions_columns,
            Opcodes.NEW_USERS: filter_users_columns,
        }

    def _get_query_mappings(self) -> Dict[int, List[int]]:
        """Return mapping of opcodes to specific query IDs for each table."""
        return {
            Opcodes.NEW_TRANSACTION: [1, 3, 4],       # Transaction: query 1, query 3, query 4
            Opcodes.NEW_TRANSACTION_ITEMS: [2],        # TransactionItem: query 2
            Opcodes.NEW_MENU_ITEMS: [2],               # MenuItem: query 2
            Opcodes.NEW_STORES: [3],                   # Store: query 3
            Opcodes.NEW_USERS: [4],                    # User: query 4
        }

    def _get_serialize_functions_mapping(self) -> Dict[int, Callable]:
        """Return mapping of opcodes to serialization functions."""
        return {
            Opcodes.NEW_MENU_ITEMS: serialize_filtered_menu_items,
            Opcodes.NEW_STORES: serialize_filtered_stores,
            Opcodes.NEW_TRANSACTION_ITEMS: serialize_filtered_transaction_items,
            Opcodes.NEW_TRANSACTION: serialize_filtered_transactions,
            Opcodes.NEW_USERS: serialize_filtered_users,
        }
        
    def _get_table_names_mapping(self) -> Dict[int, str]:
        """Map opcodes to table names for logging/debugging."""
        return {
            Opcodes.NEW_MENU_ITEMS: "menu_items",
            Opcodes.NEW_STORES: "stores",
            Opcodes.NEW_TRANSACTION_ITEMS: "transaction_items",
            Opcodes.NEW_TRANSACTION: "transactions",
            Opcodes.NEW_USERS: "users",
        }

    def get_filter_function_for_opcode(self, opcode: int) -> Callable:
        """Get filter function for the given opcode."""
        filter_func = self._filter_functions.get(opcode)
        if filter_func is None:
            raise ValueError(f"No filter function defined for opcode {opcode}")
        return filter_func

    def get_query_ids_for_opcode(self, opcode: int) -> List[int]:
        """Get query IDs for the given opcode."""
        return self._query_mappings.get(opcode, [1])  # Default to [1] if not found

    def get_serialize_function_for_opcode(self, opcode: int) -> Callable:
        """Get serialization function for the given opcode."""
        serialize_func = self._serialize_functions.get(opcode)
        if serialize_func is None:
            raise ValueError(f"No serialize function defined for opcode {opcode}")
        return serialize_func
        
    def get_table_name_for_opcode(self, opcode: int) -> str:
        """Get table name for the given opcode."""
        return self._table_names.get(opcode, f"unknown_opcode_{opcode}")

    def filter_and_serialize_data(self, original_msg):
        """Filter data from original message and serialize it."""
        # Get filter function and apply filter
        filter_func = self.get_filter_function_for_opcode(original_msg.opcode)
        filtered_rows = filter_func(original_msg.rows)
        
        # Get serialization function and serialize filtered data
        serialize_func = self.get_serialize_function_for_opcode(original_msg.opcode)
        inner_body = serialize_func(filtered_rows, original_msg.batch_number, original_msg.batch_status)
        
        return filtered_rows, inner_body

    def log_data_batch_creation(self, original_msg, query_ids, filtered_rows, inner_body, batch_bytes):
        """Log detailed information about data batch creation."""
        table_name = self.get_table_name_for_opcode(original_msg.opcode)
        logging.info(
            "action: create_data_batch | table: %s | opcode: %d | query_ids: %s | "
            "original_rows: %d | filtered_rows: %d | batch_number: %d | "
            "inner_body_size: %d bytes | batch_bytes_size: %d bytes",
            table_name, original_msg.opcode, query_ids, 
            original_msg.amount, len(filtered_rows), original_msg.batch_number,
            len(inner_body), len(batch_bytes)
        )

    def log_filtered_sample(self, original_msg, filtered_rows):
        """Log sample of filtered data for debugging."""
        if filtered_rows:
            table_name = self.get_table_name_for_opcode(original_msg.opcode)
            sample_row = filtered_rows[0]
            logging.debug(
                "action: filtered_sample | table: %s | batch_number: %d | "
                "sample_keys: %s | sample_row: %s",
                table_name, original_msg.batch_number, list(sample_row.keys()), sample_row
            )

    def create_data_batch_wrapper(self, original_msg, query_ids, batch_bytes) -> DataBatch:
        """Create DataBatch wrapper with all necessary metadata."""
        wrapper = DataBatch(
            table_ids=[1],  # For now use 1 as generic table_id
            query_ids=query_ids,  # Specific queries for this table
            meta={},
            total_shards=0,
            shard_num=0,
            batch_bytes=batch_bytes  # The filtered serialized message!
        )
        
        # Assign batch_number for accessibility
        wrapper.batch_number = original_msg.batch_number
        
        return wrapper

    def add_backward_compatibility_data(self, wrapper: DataBatch, original_msg, filtered_rows):
        """Add filtered_data dictionary for backward compatibility."""
        table_name = self.get_table_name_for_opcode(original_msg.opcode)
        wrapper.filtered_data = {
            "table_name": table_name,
            "rows": filtered_rows,
            "original_row_count": original_msg.amount,
            "filtered_row_count": len(filtered_rows),
            "batch_number": original_msg.batch_number,
            "batch_status": original_msg.batch_status
        }

    def log_data_batch_ready(self, original_msg):
        """Log that data batch is ready for transmission."""
        table_name = self.get_table_name_for_opcode(original_msg.opcode)
        logging.info(
            "action: data_batch_ready | table: %s | batch_number: %d | "
            "ready_for_transmission: True | can_use_write_to: True",
            table_name, original_msg.batch_number
        )

    def create_filtered_data_batch(self, original_msg) -> DataBatch:
        """
        Take an original message (NewMenuItems, NewStores, etc.) and create a DataBatch wrapper
        with filtered columns and appropriate metadata.
        """
        # Get query IDs for this table
        query_ids = self.get_query_ids_for_opcode(original_msg.opcode)
        
        # Filter and serialize data
        filtered_rows, inner_body = self.filter_and_serialize_data(original_msg)
        
        # Create complete embedded message [opcode][length][body]
        batch_bytes = DataBatch.make_embedded(
            inner_opcode=original_msg.opcode,
            inner_body=inner_body
        )
        
        # Detailed log of what we're assembling
        self.log_data_batch_creation(original_msg, query_ids, filtered_rows, inner_body, batch_bytes)
        
        # Log sample of filtered data
        self.log_filtered_sample(original_msg, filtered_rows)
        
        # Create complete DataBatch wrapper
        wrapper = self.create_data_batch_wrapper(original_msg, query_ids, batch_bytes)
        
        # Create dictionary for backward compatibility
        self.add_backward_compatibility_data(wrapper, original_msg, filtered_rows)
        
        # Final log
        self.log_data_batch_ready(original_msg)
        
        return wrapper


# Global instance for convenience
batch_processor = BatchProcessor()

# Convenience function for backward compatibility
def create_filtered_data_batch(original_msg) -> DataBatch:
    """Create filtered data batch from original message.
    
    This is a convenience function that uses the global batch processor.
    """
    return batch_processor.create_filtered_data_batch(original_msg)