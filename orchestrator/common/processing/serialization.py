"""
Serialization functions for converting filtered data to binary protocol format.
"""

import logging
from typing import List, Dict


def serialize_header(n_rows: int, batch_number: int, batch_status: int) -> bytearray:
    """Serialize common header for all filtered messages."""
    body = bytearray()
    # [i32 nRows][i64 batchNumber][u8 status]
    body.extend(n_rows.to_bytes(4, "little", signed=True))
    body.extend(batch_number.to_bytes(8, "little", signed=True))  
    body.extend(batch_status.to_bytes(1, "little", signed=False))
    return body


def serialize_key_value_pair(key: str, value: str) -> bytearray:
    """Serialize a key-value pair as [string key][string value]."""
    pair_bytes = bytearray()
    # [string key]
    key_bytes = key.encode("utf-8")
    pair_bytes.extend(len(key_bytes).to_bytes(4, "little", signed=True))
    pair_bytes.extend(key_bytes)
    # [string value] 
    value_bytes = str(value).encode("utf-8")
    pair_bytes.extend(len(value_bytes).to_bytes(4, "little", signed=True))
    pair_bytes.extend(value_bytes)
    return pair_bytes


def serialize_row(row: Dict, required_keys: List[str]) -> bytearray:
    """Serialize a row with required keys."""
    row_bytes = bytearray()
    # [i32 n_pairs]
    row_bytes.extend(len(required_keys).to_bytes(4, "little", signed=True))
    
    # Serialize each key-value pair
    for key in required_keys:
        value = row.get(key, "")
        row_bytes.extend(serialize_key_value_pair(key, value))
    
    return row_bytes


def serialize_filtered_data(filtered_rows: List[Dict], batch_number: int, batch_status: int, required_keys: List[str], table_name: str) -> bytes:
    """Generic function for serializing filtered data."""
    n_rows = len(filtered_rows)
    
    # Serialize header
    body = serialize_header(n_rows, batch_number, batch_status)
    
    # Serialize each filtered row
    for row in filtered_rows:
        body.extend(serialize_row(row, required_keys))
    
    logging.debug(f"action: serialize_{table_name} | rows: {n_rows} | body_size: {len(body)} bytes")
    return bytes(body)


def serialize_filtered_menu_items(filtered_rows: List[Dict], batch_number: int, batch_status: int) -> bytes:
    """Serialize filtered menu items data in NewMenuItems format."""
    required_keys = ["product_id", "name", "price"]
    return serialize_filtered_data(filtered_rows, batch_number, batch_status, required_keys, "menu_items")


def serialize_filtered_stores(filtered_rows: List[Dict], batch_number: int, batch_status: int) -> bytes:
    """Serialize filtered stores data in NewStores format.""" 
    required_keys = ["store_id", "store_name"]
    return serialize_filtered_data(filtered_rows, batch_number, batch_status, required_keys, "stores")


def serialize_filtered_transaction_items(filtered_rows: List[Dict], batch_number: int, batch_status: int) -> bytes:
    """Serialize filtered transaction items data in NewTransactionItems format."""
    required_keys = ["transaction_id", "item_id", "quantity", "subtotal", "created_at"]
    return serialize_filtered_data(filtered_rows, batch_number, batch_status, required_keys, "transaction_items")


def serialize_filtered_transactions(filtered_rows: List[Dict], batch_number: int, batch_status: int) -> bytes:
    """Serialize filtered transactions data in NewTransactions format."""
    required_keys = ["transaction_id", "store_id", "user_id", "final_amount", "created_at"]
    return serialize_filtered_data(filtered_rows, batch_number, batch_status, required_keys, "transactions")


def serialize_filtered_users(filtered_rows: List[Dict], batch_number: int, batch_status: int) -> bytes:
    """Serialize filtered users data in NewUsers format."""
    required_keys = ["user_id", "birthdate"]
    return serialize_filtered_data(filtered_rows, batch_number, batch_status, required_keys, "users")