from collections import defaultdict
from datetime import datetime
import logging
from typing import Dict, Tuple, List

from aggregator.utils.aggregationUtils import create_query2_aggregation, create_query3_aggregation, log_query4_transactions, log_query4_users
from aggregator.utils.datetimeUtils import calculate_semester, extract_year, parse_datetime
from aggregator.utils.debug import log_query2_results, log_query3_results
from aggregator.utils.filterUtils import VALID_YEARS, filter_transaction_by_datetime, is_valid_year
from protocol.entities import RawTransaction, RawTransactionItem, RawUser

# ============================================================================
# QUERY PROCESSORS
# ============================================================================

def aggregate_query2_data(transaction_items: List[RawTransactionItem]) -> Dict:
    """
    Aggregate transaction items by (year, month, item_id).
    
    Returns dictionary with aggregated quantities and revenues.
    """
    aggregated_data = create_query2_aggregation()

    for item in transaction_items:
        created_at = parse_datetime(item.created_at)
        key = (created_at.year, created_at.month, item.item_id)
        
        aggregated_data[key]['total_quantity'] += float(item.quantity)
        aggregated_data[key]['total_revenue'] += float(item.subtotal)

    return aggregated_data


def process_query_2(transaction_items: List[RawTransactionItem]) -> None:
    """
    Process Query 2: Aggregate transaction items by year, month, and item_id.
    
    Aggregations:
    - total_quantity: Sum of quantities
    - total_revenue: Sum of subtotals
    """
    aggregated_data = aggregate_query2_data(transaction_items)
    log_query2_results(aggregated_data)


def aggregate_query3_data(transactions: List[RawTransaction]) -> Tuple[Dict, int, int]:
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
        
        created_at = parse_datetime(transaction.created_at)
        year = created_at.year
        month = created_at.month
        semester = calculate_semester(month)
        
        key = (transaction.store_id, year, semester)
        
        aggregated_data[key]['transaction_count'] += 1
        aggregated_data[key]['total_original_amount'] += float(transaction.original_amount)
        aggregated_data[key]['total_discount_applied'] += float(transaction.discount_applied)
        aggregated_data[key]['total_final_amount'] += float(transaction.final_amount)

    return aggregated_data, filtered_count, total_count


def process_query_3(transactions: List[RawTransaction]) -> None:
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
    """
    aggregated_data, filtered_count, total_count = aggregate_query3_data(transactions)
    log_query3_results(aggregated_data, filtered_count, total_count)


def aggregate_query4_transactions(transactions: List[RawTransaction]) -> Tuple[Dict, int, int]:
    """
    Aggregate transactions by (store_id, user_id) for valid years.
    
    Returns tuple: (transaction_counts, filtered_count, total_count)
    """
    transaction_counts = defaultdict(int)
    filtered_count = 0
    total_count = len(transactions)

    for transaction in transactions:
        year = extract_year(transaction.created_at)
        
        if not is_valid_year(year):
            continue
            
        filtered_count += 1
        key = (transaction.store_id, transaction.user_id)
        transaction_counts[key] += 1

    return transaction_counts, filtered_count, total_count


def process_query_4_transactions(transactions: List[RawTransaction]) -> None:
    """
    Process Query 4 - TABLE 1: Count transactions by store and user.
    
    Filters:
    - Year ∈ {2024, 2025}
    
    Groups by: (store_id, user_id)
    Aggregation: Count of transactions
    """
    logging.info('Processing query 4 - Transactions')
    
    transaction_counts, filtered_count, total_count = aggregate_query4_transactions(transactions)
    log_query4_transactions(transaction_counts, filtered_count, total_count)


def process_query_4_users(users: List[RawUser]) -> None:
    """
    Process Query 4 - TABLE 2: Display user information.
    
    Shows user_id and birthdate for all users.
    """
    logging.info('Processing query 4 - Users')
    log_query4_users(users)
