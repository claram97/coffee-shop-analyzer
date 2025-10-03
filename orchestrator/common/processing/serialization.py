"""
Provides a set of functions for serializing filtered data structures into a
specific binary format. This is used to prepare data for network transmission
according to a custom protocol.
"""

import logging
from typing import Dict, List


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


def serialize_key_value_pair(key: str, value: str) -> bytearray:
    """
    Serializes a single key-value pair into the format [string key][string value].

    A "string" is represented as a 4-byte little-endian length prefix
    followed by the UTF-8 encoded bytes of the string.

    Args:
        key: The key to serialize.
        value: The value to serialize. It will be converted to a string.

    Returns:
        A bytearray containing the serialized key-value pair.
    """
    pair_bytes = bytearray()

    # Serialize key: [i32 length][bytes]
    key_bytes = key.encode("utf-8")
    pair_bytes.extend(len(key_bytes).to_bytes(4, "little", signed=True))
    pair_bytes.extend(key_bytes)

    # Serialize value: [i32 length][bytes]
    value_bytes = str(value).encode("utf-8")
    pair_bytes.extend(len(value_bytes).to_bytes(4, "little", signed=True))
    pair_bytes.extend(value_bytes)
    return pair_bytes


def serialize_row(row: Dict, required_keys: List[str]) -> bytearray:
    """
    Serializes a dictionary (row) into a binary format.

    The row format is: [i32 n_pairs][pair_1][pair_2]...[pair_n]

    Args:
        row: A dictionary representing a single row of data.
        required_keys: A list of keys to include from the row. If a key is
                       missing from the row, its value will be an empty string.

    Returns:
        A bytearray containing the serialized row.
    """
    row_bytes = bytearray()
    # [i32 n_pairs]: Number of key-value pairs to follow
    row_bytes.extend(len(required_keys).to_bytes(4, "little", signed=True))

    # Serialize each required key-value pair
    for key in required_keys:
        value = row.get(key, "")
        row_bytes.extend(serialize_key_value_pair(key, value))

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

    # Append each serialized row
    for row in filtered_rows:
        body.extend(serialize_row(row, required_keys))

    logging.debug(
        f"action: serialize_{table_name} | rows: {n_rows} | body_size: {len(body)} bytes"
    )
    return bytes(body)


def serialize_filtered_menu_items(
    filtered_rows: List[Dict], batch_number: int, batch_status: int
) -> bytes:
    """
    Serializes filtered menu items data using a predefined set of required keys.

    Required keys: ["product_id", "name", "price"]

    Args:
        filtered_rows: A list of filtered menu item dictionaries.
        batch_number: The sequence number of the batch.
        batch_status: The status code for the batch.

    Returns:
        The serialized message body as a bytes object.
    """
    required_keys = ["product_id", "name", "price"]
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

