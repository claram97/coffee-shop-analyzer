"""
Provides a set of functions for serializing filtered data structures into a
specific binary format. This is used to prepare data for network transmission
according to a custom protocol.
"""

import logging
import struct
from collections import OrderedDict
from typing import Dict, List, Sequence, Tuple


def serialize_header(n_rows: int, batch_number: int, batch_status: int) -> bytearray:
    """
    Serializes a common header for filtered data messages.

    The header format is: [i32 nRows][i64 batchNumber][u8 status]

    Args:
        n_rows: The total number of rows in the batch.
        batch_number: The sequence number of the batch.
        batch_status: The status code for the batch (e.g., Continue, EOF).

    Returns:
        A bytearray containing the serialized header.
    """
    body = bytearray()
    # [i32 nRows]: 4-byte little-endian signed integer
    body.extend(n_rows.to_bytes(4, "little", signed=True))
    # [i64 batchNumber]: 8-byte little-endian signed integer
    body.extend(batch_number.to_bytes(8, "little", signed=True))
    # [u8 status]: 1-byte unsigned integer
    body.extend(batch_status.to_bytes(1, "little", signed=False))
    return body


_KEY_METADATA_CACHE: OrderedDict[Tuple[str, ...], Tuple[Tuple[str, bytes], ...]] = (
    OrderedDict()
)
_KEY_METADATA_CACHE_SIZE = 32


def _key_metadata(required_keys: Tuple[str, ...]) -> Tuple[Tuple[str, bytes], ...]:
    cached = _KEY_METADATA_CACHE.get(required_keys)
    if cached is not None:
        _KEY_METADATA_CACHE.move_to_end(required_keys)
        return cached

    entries: list[tuple[str, bytes]] = []
    for key in required_keys:
        key_bytes = key.encode("utf-8")
        prefix = struct.pack("<I", len(key_bytes)) + key_bytes
        entries.append((key, prefix))

    result = tuple(entries)
    _KEY_METADATA_CACHE[required_keys] = result
    _KEY_METADATA_CACHE.move_to_end(required_keys)

    if len(_KEY_METADATA_CACHE) > _KEY_METADATA_CACHE_SIZE:
        _KEY_METADATA_CACHE.popitem(last=False)

    return result


def _serialize_row(row: Dict, metadata: Sequence[Tuple[str, bytes]]) -> bytearray:
    pair_count = len(metadata)
    prepared_values: list[Tuple[bytes, bytes]] = []
    total_length = 4  # Initial bytes for pair count

    for key, key_prefix in metadata:
        value_bytes = str(row.get(key, "")).encode("utf-8")
        prepared_values.append((key_prefix, value_bytes))
        total_length += len(key_prefix) + 4 + len(value_bytes)

    row_bytes = bytearray(total_length)
    mv = memoryview(row_bytes)
    struct.pack_into("<I", mv, 0, pair_count)
    offset = 4

    for key_prefix, value_bytes in prepared_values:
        prefix_len = len(key_prefix)
        mv[offset : offset + prefix_len] = key_prefix
        offset += prefix_len
        struct.pack_into("<I", mv, offset, len(value_bytes))
        offset += 4
        mv[offset : offset + len(value_bytes)] = value_bytes
        offset += len(value_bytes)

    return row_bytes


def serialize_filtered_data(
    filtered_rows: List[Dict],
    batch_number: int,
    batch_status: int,
    required_keys: List[str],
    table_name: str,
) -> bytes:
    """
    A generic function that serializes a list of filtered rows into a complete
    binary message payload, including a header and all serialized rows.

    Args:
        filtered_rows: The list of data rows (dictionaries) to serialize.
        batch_number: The sequence number of the batch.
        batch_status: The status code for the batch.
        required_keys: The list of keys to be serialized for each row.
        table_name: The name of the table, used for logging purposes.

    Returns:
        A bytes object containing the fully serialized message body.
    """
    n_rows = len(filtered_rows)

    # Start with the common header
    body = serialize_header(n_rows, batch_number, batch_status)

    metadata = _key_metadata(tuple(required_keys))

    # Append each serialized row
    for row in filtered_rows:
        body.extend(_serialize_row(row, metadata))

    logging.debug(
        f"action: serialize_{table_name} | rows: {n_rows} | body_size: {len(body)} bytes"
    )
    return bytes(body)


def serialize_filtered_menu_items(
    filtered_rows: List[Dict], batch_number: int, batch_status: int
) -> bytes:
    """
    Serializes filtered menu items data using a predefined set of required keys.

    Required keys: ["item_id", "name", "price"]

    Args:
        filtered_rows: A list of filtered menu item dictionaries.
        batch_number: The sequence number of the batch.
        batch_status: The status code for the batch.

    Returns:
        The serialized message body as a bytes object.
    """
    required_keys = ["item_id", "name", "price"]
    return serialize_filtered_data(
        filtered_rows, batch_number, batch_status, required_keys, "menu_items"
    )


def serialize_filtered_stores(
    filtered_rows: List[Dict], batch_number: int, batch_status: int
) -> bytes:
    """
    Serializes filtered store data using a predefined set of required keys.

    Required keys: ["store_id", "store_name"]

    Args:
        filtered_rows: A list of filtered store dictionaries.
        batch_number: The sequence number of the batch.
        batch_status: The status code for the batch.

    Returns:
        The serialized message body as a bytes object.
    """
    required_keys = ["store_id", "store_name"]
    return serialize_filtered_data(
        filtered_rows, batch_number, batch_status, required_keys, "stores"
    )


def serialize_filtered_transaction_items(
    filtered_rows: List[Dict], batch_number: int, batch_status: int
) -> bytes:
    """
    Serializes filtered transaction item data using a predefined set of required keys.

    Required keys: ["transaction_id", "item_id", "quantity", "subtotal", "created_at"]

    Args:
        filtered_rows: A list of filtered transaction item dictionaries.
        batch_number: The sequence number of the batch.
        batch_status: The status code for the batch.

    Returns:
        The serialized message body as a bytes object.
    """
    required_keys = ["transaction_id", "item_id", "quantity", "subtotal", "created_at"]
    return serialize_filtered_data(
        filtered_rows, batch_number, batch_status, required_keys, "transaction_items"
    )


def serialize_filtered_transactions(
    filtered_rows: List[Dict], batch_number: int, batch_status: int
) -> bytes:
    """
    Serializes filtered transaction data using a predefined set of required keys.

    Required keys: ["transaction_id", "store_id", "user_id", "final_amount", "created_at"]

    Args:
        filtered_rows: A list of filtered transaction dictionaries.
        batch_number: The sequence number of the batch.
        batch_status: The status code for the batch.

    Returns:
        The serialized message body as a bytes object.
    """
    required_keys = [
        "transaction_id",
        "store_id",
        "user_id",
        "final_amount",
        "created_at",
    ]
    return serialize_filtered_data(
        filtered_rows, batch_number, batch_status, required_keys, "transactions"
    )


def serialize_filtered_users(
    filtered_rows: List[Dict], batch_number: int, batch_status: int
) -> bytes:
    """
    Serializes filtered user data using a predefined set of required keys.

    Required keys: ["user_id", "birthdate"]

    Args:
        filtered_rows: A list of filtered user dictionaries.
        batch_number: The sequence number of the batch.
        batch_status: The status code for the batch.

    Returns:
        The serialized message body as a bytes object.
    """
    required_keys = ["user_id", "birthdate"]
    return serialize_filtered_data(
        filtered_rows, batch_number, batch_status, required_keys, "users"
    )
