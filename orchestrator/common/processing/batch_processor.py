"""
Defines the BatchProcessor class, responsible for transforming raw data messages
into filtered, serialized, and wrapped DataBatch messages for downstream processing.
"""

import logging
from time import perf_counter, process_time # Import process_time
from typing import Dict, List

from protocol.constants import Opcodes
from protocol.databatch import DataBatch

from .filters import filter_columns
from .serialization import serialize_data


class BatchProcessor:
    """
    Orchestrates the process of transforming raw data batch messages into a
    filtered and serialized format suitable for downstream consumers.
    """

    def __init__(self):
        """
        Initializes the BatchProcessor by setting up internal mappings for
        column configurations, query IDs, and table names.
        """
        self._columns_to_keep = self._get_columns_to_keep_mapping()
        self._query_mappings = self._get_query_mappings()
        self._table_names = self._get_table_names_mapping()

    def _get_columns_to_keep_mapping(self) -> Dict[int, List[str]]:
        """Creates a dictionary mapping opcodes to the list of columns to retain."""
        return {
            Opcodes.NEW_MENU_ITEMS: ["product_id", "name", "price"],
            Opcodes.NEW_STORES: ["store_id", "store_name"],
            Opcodes.NEW_TRANSACTION_ITEMS: [
                "transaction_id", "item_id", "quantity", "subtotal", "created_at",
            ],
            Opcodes.NEW_TRANSACTION: [
                "transaction_id", "store_id", "user_id", "final_amount", "created_at",
            ],
            Opcodes.NEW_USERS: ["user_id", "birthdate"],
        }

    def _get_query_mappings(self) -> Dict[int, List[int]]:
        """Creates and returns a dictionary that maps opcodes to relevant query IDs."""
        return {
            Opcodes.NEW_TRANSACTION: [1, 3, 4],
            Opcodes.NEW_TRANSACTION_ITEMS: [2],
            Opcodes.NEW_MENU_ITEMS: [],
            Opcodes.NEW_STORES: [],
            Opcodes.NEW_USERS: [4],
        }

    def _get_table_names_mapping(self) -> Dict[int, str]:
        """Creates and returns a dictionary that maps opcodes to table names."""
        return {
            Opcodes.NEW_MENU_ITEMS: "menu_items",
            Opcodes.NEW_STORES: "stores",
            Opcodes.NEW_TRANSACTION_ITEMS: "transaction_items",
            Opcodes.NEW_TRANSACTION: "transactions",
            Opcodes.NEW_USERS: "users",
        }

    def get_query_ids_for_opcode(self, opcode: int) -> List[int]:
        """Retrieves the list of query IDs associated with a given opcode."""
        return self._query_mappings.get(opcode, [])

    def get_table_name_for_opcode(self, opcode: int) -> str:
        """Retrieves the human-readable table name for a given opcode."""
        return self._table_names.get(opcode, f"unknown_opcode_{opcode}")

    def create_filtered_data_batch(self, original_msg, client_id: str) -> DataBatch:
        """
        Creates a fully-formed `DataBatch` and logs detailed wall vs. CPU timing
        to diagnose performance spikes.
        """
        if not hasattr(original_msg, 'rows') or original_msg.rows is None:
            logging.error(
                "action: process_batch | result: skip | reason: 'rows' attribute is missing or None | "
                "opcode: %d | batch_number: %d",
                original_msg.opcode,
                original_msg.batch_number
            )
            return None

        # --- Start Detailed Timing ---
        wall_start = perf_counter()
        cpu_start = process_time()

        opcode = original_msg.opcode
        query_ids = self.get_query_ids_for_opcode(opcode)
        columns_to_keep = self._columns_to_keep.get(opcode)

        if columns_to_keep is None:
            logging.error(
                "action: process_batch | result: skip | reason: unknown_opcode | "
                "opcode: %d | batch_number: %d",
                opcode,
                original_msg.batch_number
            )
            return None

        table_name = self.get_table_name_for_opcode(opcode)

        # --- Step 1: Serialize Data ---
        inner_body = serialize_data(
            original_msg.rows,
            original_msg.batch_number,
            original_msg.batch_status,
            tuple(columns_to_keep),
            table_name
        )
        wall_after_serialize = perf_counter()
        cpu_after_serialize = process_time()

        # --- Step 2: Embed Data ---
        batch_bytes = DataBatch.make_embedded(
            inner_opcode=opcode, inner_body=inner_body
        )
        wall_after_embedding = perf_counter()
        cpu_after_embedding = process_time()

        # --- Step 3: Create Wrapper Object ---
        wrapper = self.create_data_batch_wrapper(
            original_msg, query_ids, batch_bytes, client_id=client_id
        )
        wall_after_wrapper = perf_counter()
        cpu_after_wrapper = process_time()

        # --- Log Enhanced Timing Metrics ---
        logging.info(
            "action: batch_processor_timing | table: %s | batch_number: %d | "
            "total_wall_ms: %.3f | total_cpu_ms: %.3f | "
            "serialize_wall_ms: %.3f | serialize_cpu_ms: %.3f | "
            "embedding_wall_ms: %.3f | wrapper_wall_ms: %.3f",
            table_name,
            original_msg.batch_number,
            (wall_after_wrapper - wall_start) * 1000,
            (cpu_after_wrapper - cpu_start) * 1000,
            (wall_after_serialize - wall_start) * 1000,
            (cpu_after_serialize - cpu_start) * 1000,
            (wall_after_embedding - wall_after_serialize) * 1000,
            (wall_after_wrapper - wall_after_embedding) * 1000,
        )

        return wrapper
    
    def log_data_batch_creation(self, original_msg, query_ids, filtered_rows, inner_body, batch_bytes):
        table_name = self.get_table_name_for_opcode(original_msg.opcode)
        logging.debug(
            "action: create_data_batch | table: %s | opcode: %d | query_ids: %s | "
            "original_rows: %d | filtered_rows: %d | batch_number: %d | "
            "inner_body_size: %d bytes | batch_bytes_size: %d bytes",
            table_name,
            original_msg.opcode,
            query_ids,
            original_msg.amount,
            len(filtered_rows),
            original_msg.batch_number,
            len(inner_body),
            len(batch_bytes),
        )

    def log_filtered_sample(self, original_msg, filtered_rows):
        if filtered_rows:
            table_name = self.get_table_name_for_opcode(original_msg.opcode)
            sample_row = filtered_rows[0]
            logging.debug(
                "action: filtered_sample | table: %s | batch_number: %d | "
                "sample_keys: %s | sample_row: %s",
                table_name,
                original_msg.batch_number,
                list(sample_row.keys()),
                sample_row,
            )

    def create_data_batch_wrapper(self, original_msg, query_ids, batch_bytes, *, client_id: str) -> DataBatch:
        wrapper = DataBatch(
            table_ids=[],
            query_ids=query_ids,
            meta={},
            shards_info=[],
            batch_bytes=batch_bytes,
            client_id=client_id,
        )
        wrapper.batch_number = original_msg.batch_number
        return wrapper

    def add_backward_compatibility_data(self, wrapper: DataBatch, original_msg, filtered_rows):
        table_name = self.get_table_name_for_opcode(original_msg.opcode)
        wrapper.filtered_data = {
            "table_name": table_name,
            "rows": filtered_rows,
            "original_row_count": original_msg.amount,
            "filtered_row_count": len(filtered_rows),
            "batch_number": original_msg.batch_number,
            "batch_status": original_msg.batch_status,
        }

    def log_data_batch_ready(self, original_msg):
        table_name = self.get_table_name_for_opcode(original_msg.opcode)
        logging.debug(
            "action: data_batch_ready | table: %s | batch_number: %d | "
            "ready_for_transmission: True | can_use_write_to: True",
            table_name,
            original_msg.batch_number,
        )

batch_processor = BatchProcessor()

def create_filtered_data_batch(original_msg, client_id: str) -> DataBatch:
    return batch_processor.create_filtered_data_batch(original_msg, client_id)