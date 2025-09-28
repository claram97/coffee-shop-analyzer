"""
Provides a set of functions for filtering raw data objects, selecting only the
columns required for downstream processing and analysis. This helps reduce data
size and complexity by creating leaner data representations.
"""

from typing import List, Dict, Any
from protocol.entities import RawMenuItems, RawStore, RawTransactionItem, RawTransaction, RawUser


def filter_menu_items_columns(rows: List[RawMenuItems]) -> List[Dict]:
    """
    Selects a subset of columns from a list of RawMenuItems objects.

    This function retains essential product information (ID, name, price)
    while discarding columns like category, is_seasonal, available_from,
    and available_to.

    Args:
        rows: A list of RawMenuItems data objects.

    Returns:
        A new list of dictionaries, where each dictionary represents a
        menu item with a reduced set of key-value pairs.
    """
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
    """
    Selects a subset of columns from a list of RawStore objects.

    This function retains the store's ID and name while discarding detailed
    address and geolocation data (street, city, latitude, etc.).

    Args:
        rows: A list of RawStore data objects.

    Returns:
        A new list of dictionaries, each representing a store with
        only its ID and name.
    """
    filtered_rows = []
    for row in rows:
        filtered_row = {
            "store_id": row.store_id,
            "store_name": row.store_name
        }
        filtered_rows.append(filtered_row)
    return filtered_rows


def filter_transaction_items_columns(rows: List[RawTransactionItem]) -> List[Dict]:
    """
    Selects a subset of columns from a list of RawTransactionItem objects.

    Retains key transactional details like IDs, quantity, and subtotal,
    but discards the original unit_price.

    Args:
        rows: A list of RawTransactionItem data objects.

    Returns:
        A new list of dictionaries, each representing a transaction item
        with a reduced set of columns.
    """
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
    """
    Selects a subset of columns from a list of RawTransaction objects.

    Keeps core transaction identifiers and the final amount, while removing
    details about payment method, vouchers, original amount, and discounts.

    Args:
        rows: A list of RawTransaction data objects.

    Returns:
        A new list of dictionaries, each representing a transaction with a
        reduced set of key-value pairs.
    """
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
    """
    Selects a subset of columns from a list of RawUser objects.

    This function retains the user's ID and birthdate for analysis,
    while discarding personal details like gender and registration timestamp.

    Args:
        rows: A list of RawUser data objects.

    Returns:
        A new list of dictionaries, each representing a user with only
        their ID and birthdate.
    """
    filtered_rows = []
    for row in rows:
        filtered_row = {
            "user_id": row.user_id,
            "birthdate": row.birthdate
        }
        filtered_rows.append(filtered_row)
    return filtered_rows