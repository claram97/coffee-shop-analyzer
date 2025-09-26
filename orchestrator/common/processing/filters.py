"""
Data filtering functions for removing unnecessary columns from table data.
"""

from typing import List, Dict
from ..protocol.entities import RawMenuItems, RawStore, RawTransactionItem, RawTransaction, RawUser


def filter_menu_items_columns(rows: List[RawMenuItems]) -> List[Dict]:
    """Filter unnecessary columns from MenuItem: category, is_seasonal, available_from, available_to."""
    filtered_rows = []
    for row in rows:
        filtered_row = {
            "product_id": row.product_id,
            "name": row.name,
            "price": row.price
        }
        filtered_rows.append(filtered_row)
    return filtered_rows


def filter_stores_columns(rows: List[RawStore]) -> List[Dict]:
    """Filter unnecessary columns from Store: street, postal_code, city, state, latitude, longitude."""
    filtered_rows = []
    for row in rows:
        filtered_row = {
            "store_id": row.store_id,
            "store_name": row.store_name
        }
        filtered_rows.append(filtered_row)
    return filtered_rows


def filter_transaction_items_columns(rows: List[RawTransactionItem]) -> List[Dict]:
    """Filter unnecessary columns from TransactionItem: unit_price."""
    filtered_rows = []
    for row in rows:
        filtered_row = {
            "transaction_id": row.transaction_id,
            "item_id": row.item_id,
            "quantity": row.quantity,
            "subtotal": row.subtotal,
            "created_at": row.created_at
        }
        filtered_rows.append(filtered_row)
    return filtered_rows


def filter_transactions_columns(rows: List[RawTransaction]) -> List[Dict]:
    """Filter unnecessary columns from Transaction: payment_method_id, voucher_id, original_amount, discount_applied."""
    filtered_rows = []
    for row in rows:
        filtered_row = {
            "transaction_id": row.transaction_id,
            "store_id": row.store_id,
            "user_id": row.user_id,
            "final_amount": row.final_amount,
            "created_at": row.created_at
        }
        filtered_rows.append(filtered_row)
    return filtered_rows


def filter_users_columns(rows: List[RawUser]) -> List[Dict]:
    """Filter unnecessary columns from User: gender, registered_at."""
    filtered_rows = []
    for row in rows:
        filtered_row = {
            "user_id": row.user_id,
            "birthdate": row.birthdate
        }
        filtered_rows.append(filtered_row)
    return filtered_rows


def filter_vouchers_columns(rows) -> List[Dict]:
    """Filter all columns from Voucher since they're not used in any query."""
    # For now return empty list since these columns aren't used in current queries
    return []