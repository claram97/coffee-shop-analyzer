"""
Defines the BatchProcessor class, responsible for transforming raw data messages
into filtered, serialized, and wrapped DataBatch messages for downstream processing.
"""

import logging
from typing import Dict, List, Callable

from protocol import Opcodes, DataBatch
from .filters import (
    filter_menu_items_columns, filter_stores_columns, filter_transaction_items_columns,
    filter_transactions_columns, filter_users_columns
)
from .serialization import (
    serialize_filtered_menu_items, serialize_filtered_stores, serialize_filtered_transaction_items,
    serialize_filtered_transactions, serialize_filtered_users
)


class BatchProcessor:
    """
    Orchestrates the process of transforming raw data batch messages into a
    filtered and serialized format suitable for downstream consumers.

    This involves selecting relevant data columns based on message type (opcode),
    serializing the result, and wrapping it in a standardized `DataBatch` message
    with appropriate metadata.
    """

    def __init__(self):
        """
        Initializes the BatchProcessor by setting up internal mappings for filter
        functions, serialization functions, query IDs, and table names, all
        keyed by message opcodes.
        """
        self._filter_functions = self._get_filter_functions_mapping()
        self._serialize_functions = self._get_serialize_functions_mapping()
        self._query_mappings = self._get_query_mappings()
        self._table_names = self._get_table_names_mapping()

    def _get_filter_functions_mapping(self) -> Dict[int, Callable]:
        """Creates and returns a dictionary that maps opcodes to their corresponding column filter functions."""
        return {
            Opcodes.NEW_MENU_ITEMS: filter_menu_items_columns,
            Opcodes.NEW_STORES: filter_stores_columns,
            Opcodes.NEW_TRANSACTION_ITEMS: filter_transaction_items_columns,
            Opcodes.NEW_TRANSACTION: filter_transactions_columns,
            Opcodes.NEW_USERS: filter_users_columns,
        }

    def _get_query_mappings(self) -> Dict[int, List[int]]:
        """Creates and returns a dictionary that maps opcodes to the query IDs they are relevant for."""
        return {
            Opcodes.NEW_TRANSACTION: [1, 3, 4],       # Transaction: query 1, query 3, query 4
            Opcodes.NEW_TRANSACTION_ITEMS: [2],        # TransactionItem: query 2
            Opcodes.NEW_MENU_ITEMS: [2],               # MenuItem: query 2
            Opcodes.NEW_STORES: [3],                   # Store: query 3
            Opcodes.NEW_USERS: [4],                    # User: query 4
        }

    def _get_serialize_functions_mapping(self) -> Dict[int, Callable]:
        """Creates and returns a dictionary that maps opcodes to their corresponding serialization functions."""
        return {
            Opcodes.NEW_MENU_ITEMS: serialize_filtered_menu_items,
            Opcodes.NEW_STORES: serialize_filtered_stores,
            Opcodes.NEW_TRANSACTION_ITEMS: serialize_filtered_transaction_items,
            Opcodes.NEW_TRANSACTION: serialize_filtered_transactions,
            Opcodes.NEW_USERS: serialize_filtered_users,
        }

    def _get_table_names_mapping(self) -> Dict[int, str]:
        """Creates and returns a dictionary that maps opcodes to human-readable table names for logging."""
        return {
            Opcodes.NEW_MENU_ITEMS: "menu_items",
            Opcodes.NEW_STORES: "stores",
            Opcodes.NEW_TRANSACTION_ITEMS: "transaction_items",
            Opcodes.NEW_TRANSACTION: "transactions",
            Opcodes.NEW_USERS: "users",
        }

    def get_filter_function_for_opcode(self, opcode: int) -> Callable:
        """
        Retrieves the appropriate filter function for a given opcode.

        Args:
            opcode: The opcode of the message.

        Returns:
            The filter function associated with the opcode.

        Raises:
            ValueError: If no filter function is registered for the given opcode.
        """
        filter_func = self._filter_functions.get(opcode)
        if filter_func is None:
            raise ValueError(f"No filter function defined for opcode {opcode}")
        return filter_func

    def get_query_ids_for_opcode(self, opcode: int) -> List[int]:
        """
        Retrieves the list of query IDs associated with a given opcode.

        Args:
            opcode: The opcode of the message.

        Returns:
            A list of integer query IDs. Defaults to [1] if not found.
        """
        return self._query_mappings.get(opcode, [1])

    def get_serialize_function_for_opcode(self, opcode: int) -> Callable:
        """
        Retrieves the appropriate serialization function for a given opcode.

        Args:
            opcode: The opcode of the message.

        Returns:
            The serialization function associated with the opcode.

        Raises:
            ValueError: If no serialization function is registered for the given opcode.
        """
        serialize_func = self._serialize_functions.get(opcode)
        if serialize_func is None:
            raise ValueError(f"No serialize function defined for opcode {opcode}")
        return serialize_func

    def get_table_name_for_opcode(self, opcode: int) -> str:
        """
        Retrieves the human-readable table name for a given opcode.

        Args:
            opcode: The opcode of the message.

        Returns:
            The table name string, or a default string if the opcode is unknown.
        """
        return self._table_names.get(opcode, f"unknown_opcode_{opcode}")

    def filter_and_serialize_data(self, original_msg):
        """
        Applies the appropriate filter and serialization functions to an original message's data.

        Args:
            original_msg: The incoming message object containing the raw data rows.

        Returns:
            A tuple containing:
            - filtered_rows (List[Dict]): A list of the filtered row dictionaries.
            - inner_body (bytes): The serialized binary representation of the filtered data.
        """
        filter_func = self.get_filter_function_for_opcode(original_msg.opcode)
        filtered_rows = filter_func(original_msg.rows)

        serialize_func = self.get_serialize_function_for_opcode(original_msg.opcode)
        inner_body = serialize_func(filtered_rows, original_msg.batch_number, original_msg.batch_status)

        return filtered_rows, inner_body

    def log_data_batch_creation(self, original_msg, query_ids, filtered_rows, inner_body, batch_bytes):
        """Logs a comprehensive summary of the data batch creation process."""
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
        """Logs a sample of the filtered data for debugging, showing the first row."""
        if filtered_rows:
            table_name = self.get_table_name_for_opcode(original_msg.opcode)
            sample_row = filtered_rows[0]
            logging.debug(
                "action: filtered_sample | table: %s | batch_number: %d | "
                "sample_keys: %s | sample_row: %s",
                table_name, original_msg.batch_number, list(sample_row.keys()), sample_row
            )

    def create_data_batch_wrapper(self, original_msg, query_ids, batch_bytes) -> DataBatch:
        """
        Constructs the final `DataBatch` message wrapper around the serialized data.

        Args:
            original_msg: The source message, used for metadata like batch number.
            query_ids: The list of query IDs relevant to this data.
            batch_bytes: The serialized inner message payload.

        Returns:
            A configured `DataBatch` instance.
        """
        wrapper = DataBatch(
            table_ids=[1],
            query_ids=query_ids,
            meta={},
            total_shards=0,
            shard_num=0,
            batch_bytes=batch_bytes
        )
        wrapper.batch_number = original_msg.batch_number
        return wrapper

    def add_backward_compatibility_data(self, wrapper: DataBatch, original_msg, filtered_rows):
        """
        Attaches a `filtered_data` dictionary to the `DataBatch` wrapper to maintain
        compatibility with older consumers that might expect data in this format.
        """
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
        """Logs a final confirmation that a `DataBatch` has been fully created and is ready."""
        table_name = self.get_table_name_for_opcode(original_msg.opcode)
        logging.info(
            "action: data_batch_ready | table: %s | batch_number: %d | "
            "ready_for_transmission: True | can_use_write_to: True",
            table_name, original_msg.batch_number
        )

    def create_filtered_data_batch(self, original_msg) -> DataBatch:
        """
        Creates a fully-formed `DataBatch` from an original data message.

        This method orchestrates the entire process of filtering, serializing,
        and wrapping the data, including logging and adding compatibility attributes.

        Args:
            original_msg: An incoming message object (e.g., NewMenuItems, NewStores).

        Returns:
            A `DataBatch` object ready for transmission or further processing.
        """
        query_ids = self.get_query_ids_for_opcode(original_msg.opcode)

        filtered_rows, inner_body = self.filter_and_serialize_data(original_msg)

        batch_bytes = DataBatch.make_embedded(
            inner_opcode=original_msg.opcode,
            inner_body=inner_body
        )

        self.log_data_batch_creation(original_msg, query_ids, filtered_rows, inner_body, batch_bytes)
        self.log_filtered_sample(original_msg, filtered_rows)

        wrapper = self.create_data_batch_wrapper(original_msg, query_ids, batch_bytes)

        self.add_backward_compatibility_data(wrapper, original_msg, filtered_rows)

        self.log_data_batch_ready(original_msg)

        return wrapper


# A global singleton instance of the processor for easy access throughout the application.
batch_processor = BatchProcessor()


def create_filtered_data_batch(original_msg) -> DataBatch:
    """
    A convenience function that uses the global `batch_processor` instance
    to create a filtered data batch.

    This serves as a simple entry point for the batch processing logic.

    Args:
        original_msg: The source message object.

    Returns:
        A fully processed `DataBatch` object.
    """
    return batch_processor.create_filtered_data_batch(original_msg)