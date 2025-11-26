import logging
from collections import defaultdict
from typing import Dict, List, Tuple

from utils.aggregationUtils import (
    create_query2_aggregation,
    create_query3_aggregation,
    log_query4_transactions,
)
from utils.datetimeUtils import calculate_semester, extract_year, parse_datetime
from utils.debug import log_query2_results, log_query3_results
from utils.filterUtils import filter_transaction_by_datetime, is_valid_year

# ============================================================================
# QUERY PROCESSORS
# ============================================================================


def aggregate_query2_data(transaction_items: List[dict]) -> Dict:
    """
    Aggregate transaction items by (year, month, item_id).

    Returns dictionary with aggregated quantities and revenues.
    """
    aggregated_data = create_query2_aggregation()

    for item in transaction_items:
        created_at = parse_datetime(item['created_at'])
        # For Q2, we use item_name instead of item_id when joining menu items
        # This handles both RawTransactionItem and RawTransactionItemMenuItem types
        item_id = item.get("item_id")
        item_name = item.get("item_name")

        # If we have RawTransactionItemMenuItem, use item_name as key
        # Otherwise fall back to item_id for regular RawTransactionItem
        key_id = item_name if item_name else item_id

        # Log the key elements for debugging
        logging.debug(
            f"Q2 aggregation - year: {created_at.year}, month: {created_at.month}, "
            f"key_id: {key_id}, item_id: {item_id}, item_name: {item_name}"
        )

        key = (created_at.year, created_at.month, key_id)

        # Handle quantity - ensure it's a proper numeric value
        try:
            quantity = item.get("quantity", "0")
            if quantity and quantity.strip():
                quantity_float = float(quantity)
                logging.debug(
                    f"Q2 aggregation - adding quantity {quantity_float} for {key}"
                )
                aggregated_data[key]["total_quantity"] += quantity_float
            else:
                logging.warning(f"Q2 aggregation - empty quantity for {key}")
        except (ValueError, TypeError) as e:
            logging.error(f"Q2 aggregation - invalid quantity '{quantity}': {e}")

        # Handle subtotal/revenue - ensure it's a proper numeric value
        try:
            subtotal = item.get("subtotal", "0")
            if subtotal and subtotal.strip():
                subtotal_float = float(subtotal)
                logging.debug(
                    f"Q2 aggregation - adding revenue {subtotal_float} for {key}"
                )
                aggregated_data[key]["total_revenue"] += subtotal_float
            else:
                logging.warning(f"Q2 aggregation - empty subtotal for {key}")
        except (ValueError, TypeError) as e:
            logging.error(f"Q2 aggregation - invalid subtotal '{subtotal}': {e}")

    return aggregated_data


def process_query_2(transaction_items: List[dict]) -> Dict:
    """
    Process Query 2: Aggregate transaction items by year, month, and item_id.

    Aggregations:
    - total_quantity: Sum of quantities
    - total_revenue: Sum of subtotals

    Returns:
        Dict[Tuple[int, int, str], Dict[str, float]]: Aggregated data by (year, month, item_id)
    """
    aggregated_data = aggregate_query2_data(transaction_items)
    log_query2_results(aggregated_data)
    return aggregated_data


def aggregate_query3_data(transactions: List[dict]) -> Tuple[Dict, int, int]:
    """
    Aggregate transactions by (store_id, year, semester) with filtering.

    Filters:
    - Year must be in VALID_YEARS
    - Hour must be between MIN_HOUR and MAX_HOUR

    Returns tuple: (aggregated_data, filtered_count, total_count)
    """
    aggregated_data = create_query3_aggregation()
    filtered_count = 0
    total_count = len(transactions)

    for transaction in transactions:
        if not filter_transaction_by_datetime(transaction):
            continue

        filtered_count += 1

        created_at = parse_datetime(transaction['created_at'])
        year = created_at.year
        month = created_at.month
        semester = calculate_semester(month)

        key = (transaction['store_id'], year, semester)

        aggregated_data[key]["transaction_count"] += 1

        # Handle empty or invalid original_amount
        try:
            original_amount = (
                float(transaction['original_amount'])
                if transaction['original_amount']
                else 0.0
            )
        except (ValueError, TypeError):
            logging.error(
                f"Invalid original_amount '{transaction['original_amount']}' for transaction, using 0.0"
            )
            original_amount = 0.0

        # Handle empty or invalid discount_applied
        try:
            discount_applied = (
                float(transaction['discount_applied'])
                if transaction['discount_applied']
                else 0.0
            )
        except (ValueError, TypeError):
            logging.error(
                f"Invalid discount_applied '{transaction['discount_applied']}' for transaction, using 0.0"
            )
            discount_applied = 0.0

        # Handle empty or invalid final_amount
        try:
            final_amount = (
                float(transaction['final_amount']) if transaction['final_amount'] else 0.0
            )
        except (ValueError, TypeError):
            logging.error(
                f"Invalid final_amount '{transaction['final_amount']}' for transaction, using 0.0"
            )
            final_amount = 0.0

        aggregated_data[key]["total_original_amount"] += original_amount
        aggregated_data[key]["total_discount_applied"] += discount_applied
        aggregated_data[key]["total_final_amount"] += final_amount

    return aggregated_data, filtered_count, total_count


def process_query_3(transactions: List[dict]) -> Dict:
    """
    Process Query 3: Aggregate transactions by store, year, and semester.

    Filters applied:
    - Year ∈ {2024, 2025}
    - Hour between 06:00 and 23:00

    Aggregations:
    - transaction_count: Count of transactions
    - total_original_amount: Sum of original amounts
    - total_discount_applied: Sum of discounts
    - total_final_amount: Sum of final amounts

    Returns:
        Dict[Tuple[str, int, int], Dict[str, Union[int, float]]]: Aggregated data by (store_id, year, semester)
    """
    aggregated_data, filtered_count, total_count = aggregate_query3_data(transactions)
    log_query3_results(aggregated_data, filtered_count, total_count)
    return aggregated_data


def aggregate_query4_transactions(
    transactions: List[dict],
) -> Tuple[Dict, int, int]:
    """
    Aggregate transactions by (store_id, user_id) for valid years.

    Returns tuple: (transaction_counts, filtered_count, total_count)
    """
    transaction_counts = defaultdict(int)
    filtered_count = 0
    total_count = len(transactions)

    for transaction in transactions:
        if not transaction.get("user_id", "").strip():
            continue
        year = extract_year(transaction['created_at'])

        if not is_valid_year(year):
            continue

        filtered_count += 1
        key = (transaction['store_id'], transaction['user_id'])
        transaction_counts[key] += 1

    return transaction_counts, filtered_count, total_count


def process_query_4_transactions(transactions: List[dict]) -> Dict:
    """
    Process Query 4 - TABLE 1: Count transactions by store and user.

    Filters:
    - Year ∈ {2024, 2025}

    Groups by: (store_id, user_id)
    Aggregation: Count of transactions

    Returns:
        Dict[Tuple[str, str], int]: Transaction counts by (store_id, user_id)
    """

    transaction_counts, filtered_count, total_count = aggregate_query4_transactions(
        transactions
    )
    log_query4_transactions(transaction_counts, filtered_count, total_count)
    return transaction_counts


# ============================================================================
# QUERY SERIALIZERS
# ============================================================================


def serialize_query2_results(query_result: Dict) -> List[dict]:
    """
    Serialize Query 2 aggregated results into RawTransactionItem entities.

    Args:
        query_result: Dict with key=(year, month, item_id) and
                     value={'total_quantity': float, 'total_revenue': float}

    Returns:
        List of RawTransactionItem with aggregated data

    Field Mapping (Re-interpretation):
        Original Field          → Aggregated Field
        ─────────────────────────────────────────────────────
        transaction_id          → "" (empty - not meaningful for aggregation)
        item_id                 → item_id (preserved from grouping key)
        quantity                → total_quantity (SUM of all quantities)
        unit_price              → "" (empty - not preserved)
        subtotal                → total_revenue (SUM of all subtotals)
        created_at              → "{year}-{month:02d}-01 00:00:00" (first day of month)

    Example:
        Input (3 raw items):
            RawTransactionItem(item_id="6", quantity="3", subtotal="28.5", created_at="2024-07-01 07:00:00")
            RawTransactionItem(item_id="6", quantity="1", subtotal="9.5", created_at="2024-07-01 08:00:00")
            RawTransactionItem(item_id="6", quantity="1", subtotal="9.5", created_at="2024-07-01 09:00:00")

        Output (1 aggregated item):
            RawTransactionItem(
                item_id="6",
                quantity="5.0",              # ← SUM(3 + 1 + 1)
                subtotal="47.5",             # ← SUM(28.5 + 9.5 + 9.5)
                created_at="2024-07-01 00:00:00"
            )
    """
    result = []
    for key, value in query_result.items():
        year, month, item_id = key

        # Ensure quantity is explicitly converted to string and not lost
        quantity_str = str(value.get("total_quantity", 0))
        revenue_str = str(value.get("total_revenue", 0))

        # Log serialized values for debugging
        logging.debug(
            f"Q2 serializing: item_id={item_id}, quantity={quantity_str}, revenue={revenue_str}"
        )

        transaction_item = {
            'transaction_item_id': '',
            'transaction_id': '',
            'item_id': str(item_id),
            'quantity': quantity_str,
            'subtotal': revenue_str,
            'created_at': f"{year}-{month:02d}-01 00:00:00",
        }

        result.append(transaction_item)
    return result


def serialize_query3_results(query_result: Dict) -> List[dict]:
    """
    Serialize Query 3 aggregated results into RawTransaction entities.

    Args:
        query_result: Dict with key=(store_id, year, semester) and
                     value={'transaction_count': int, 'total_original_amount': float,
                            'total_discount_applied': float, 'total_final_amount': float}

    Returns:
        List of RawTransaction with aggregated data

    Field Mapping (Re-interpretation):
        Original Field          → Aggregated Field
        ─────────────────────────────────────────────────────
        transaction_id          → transaction_count (COUNT of transactions)
        store_id                → store_id (preserved from grouping key)
        payment_method_id       → "" (empty - not preserved)
        user_id                 → "" (empty - not preserved)
        original_amount         → total_original_amount (SUM of original amounts)
        discount_applied        → total_discount_applied (SUM of discounts)
        final_amount            → total_final_amount (SUM of final amounts)
        created_at              → "{year}-{semester}" (e.g., "2024-1" or "2024-2")

    Example:
        Input (3 raw transactions from Store 1, 2024-S1):
            RawTransaction(store_id="1", original_amount="40.0", discount="2.0", final="38.0")
            RawTransaction(store_id="1", original_amount="35.0", discount="2.0", final="33.0")

        Output (1 aggregated transaction):
            RawTransaction(
                transaction_id="2",           # ← COUNT of transactions
                store_id="1",
                original_amount="75.0",       # ← SUM(40.0 + 35.0)
                discount_applied="4.0",       # ← SUM(2.0 + 2.0)
                final_amount="71.0",          # ← SUM(38.0 + 33.0)
                created_at="2024-1"           # ← Semester 1
            )
    """
    result = []
    for key, value in query_result.items():
        store_id, year, semester = key
        transaction = {
            'transaction_id': str(value["transaction_count"]),
            'store_id': str(store_id),
            'payment_method_id': '',
            'voucher_id': '',
            'user_id': '',
            'original_amount': str(value["total_original_amount"]),
            'discount_applied': str(value["total_discount_applied"]),
            'final_amount': str(value["total_final_amount"]),
            'created_at': f"{year}-{semester}",
        }
        result.append(transaction)
    return result


def serialize_query4_transaction_results(query_result: Dict) -> List[dict]:
    """
    Serialize Query 4 transaction aggregated results into RawTransaction entities.

    Args:
        query_result: Dict with key=(store_id, user_id) and value=count (int)

    Returns:
        List of RawTransaction with transaction counts

    Field Mapping (Re-interpretation):
        Original Field          → Aggregated Field
        ─────────────────────────────────────────────────────
        transaction_id          → transaction_count (COUNT per store-user pair)
        store_id                → store_id (preserved from grouping key)
        payment_method_id       → "" (empty - not needed for Query 4)
        user_id                 → user_id (preserved from grouping key)
        original_amount         → "" (empty - not aggregated for Query 4)
        discount_applied        → "" (empty - not aggregated for Query 4)
        final_amount            → "" (empty - not aggregated for Query 4)
        created_at              → "" (empty - not needed for Query 4)

    Example:
        Input (3 raw transactions from Store 1, User 1):
            RawTransaction(store_id="1", user_id="1", created_at="2024-03-15 08:00:00")
            RawTransaction(store_id="1", user_id="1", created_at="2024-04-16 14:30:00")

        Output (1 aggregated transaction):
            RawTransaction(
                transaction_id="2",           # ← COUNT of transactions
                store_id="1",
                user_id="1",
                payment_method_id="",         # ← Not used
                original_amount="",           # ← Not used
                discount_applied="",          # ← Not used
                final_amount="",              # ← Not used
                created_at=""                 # ← Not used
            )

    Note:
        This serialization only contains (store_id, user_id, count).
        The joiner will use this count to find TOP 3 users per store,
        then join with user birthdates to produce the final Query 4 result.
    """
    result = []
    for key, count in query_result.items():
        store_id, user_id = key
        transaction = {
            'transaction_id': str(count),
            'store_id': str(store_id),
            'payment_method_id': '',
            'voucher_id': '',
            'user_id': str(user_id),
            'original_amount': '',
            'discount_applied': '',
            'final_amount': '',
            'created_at': '',
        }
        result.append(transaction)
    return result


def update_databatch_with_results(databatch, result_rows: List) -> None:
    """
    Update a DataBatch with new aggregated results.

    This function performs three critical updates:
    1. Replaces the rows with aggregated data
    2. Updates the row count (amount field)
    3. Re-serializes the batch_msg into batch_bytes

    Args:
        databatch: The DataBatch to update
        result_rows: List of entity rows (RawTransaction, RawTransactionItem, etc.)

    Side Effects:
        - Modifies databatch.batch_msg.rows
        - Modifies databatch.batch_msg.amount
        - Modifies databatch.batch_bytes (re-serializes the entire batch_msg)

    Important:
        The batch_bytes MUST be updated because DataBatch.to_bytes() uses it directly.
        Without re-serialization, the old data would be sent downstream.

    Example:
        Before:
            databatch.batch_msg.rows = [100 raw transactions]
            databatch.batch_msg.amount = 100

        After aggregation and update:
            databatch.batch_msg.rows = [10 aggregated transactions]
            databatch.batch_msg.amount = 10
            databatch.batch_bytes = <newly serialized bytes>
    """
    databatch.batch_msg.rows = result_rows
    databatch.batch_msg.amount = len(result_rows)
    databatch.batch_bytes = databatch.batch_msg.to_bytes()
    return databatch
