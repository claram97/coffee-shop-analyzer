from collections import defaultdict
from datetime import datetime
import logging
from typing import Dict, Tuple, List

from protocol.entities import RawTransaction, RawTransactionItem, RawUser

# ============================================================================
# CONSTANTS
# ============================================================================
DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
VALID_YEARS = [2024, 2025]
MIN_HOUR = 6  # 06:00
MAX_HOUR = 23  # 23:00
FIRST_SEMESTER_MONTHS = 6  # Months 1-6 = Semester 1
FIRST_SEMESTER = 1
SECOND_SEMESTER = 2


# ============================================================================
# DATE/TIME UTILITIES
# ============================================================================

def parse_datetime(date_string: str) -> datetime:
    """Parse datetime string into datetime object."""
    return datetime.strptime(date_string, DATETIME_FORMAT)


def extract_year(date_string: str) -> int:
    """Extract year from datetime string."""
    return parse_datetime(date_string).year


def extract_month(date_string: str) -> int:
    """Extract month from datetime string."""
    return parse_datetime(date_string).month


def extract_hour(date_string: str) -> int:
    """Extract hour from datetime string."""
    return parse_datetime(date_string).hour


def calculate_semester(month: int) -> int:
    """Calculate semester (1 or 2) from month number."""
    return FIRST_SEMESTER if month <= FIRST_SEMESTER_MONTHS else SECOND_SEMESTER


# ============================================================================
# FILTERING UTILITIES
# ============================================================================

def is_valid_year(year: int) -> bool:
    """Check if year is within valid range."""
    return year in VALID_YEARS


def is_valid_hour(hour: int) -> bool:
    """Check if hour is within business hours."""
    return MIN_HOUR <= hour <= MAX_HOUR


def filter_transaction_by_datetime(transaction: RawTransaction) -> bool:
    """
    Filter transaction based on datetime constraints.
    Returns True if transaction meets the criteria.
    """
    created_at = parse_datetime(transaction.created_at)
    year = created_at.year
    hour = created_at.hour
    
    return is_valid_year(year) and is_valid_hour(hour)


# ============================================================================
# AGGREGATION UTILITIES
# ============================================================================

def create_query2_aggregation() -> Dict:
    """Create default dictionary for Query 2 aggregation."""
    return defaultdict(lambda: {'total_quantity': 0.0, 'total_revenue': 0.0})


def create_query3_aggregation() -> Dict:
    """Create default dictionary for Query 3 aggregation."""
    return defaultdict(lambda: {
        'transaction_count': 0,
        'total_original_amount': 0.0,
        'total_discount_applied': 0.0,
        'total_final_amount': 0.0
    })


# ============================================================================
# LOGGING UTILITIES
# ============================================================================

def log_query2_results(aggregated_data: Dict) -> None:
    """Log aggregated results for Query 2."""
    logging.debug('Query 2 Results - Aggregated by year, month, item_id:')
    logging.debug('Year | Month | Item_ID | Total_Quantity | Total_Revenue')
    logging.debug('-' * 60)

    for (year, month, item_id), totals in aggregated_data.items():
        logging.debug(
            f'{year:4d} | {month:5d} | {item_id:7s} | '
            f'{totals["total_quantity"]:13.1f} | {totals["total_revenue"]:12.2f}'
        )

    logging.debug(f'Total groups: {len(aggregated_data)}')
    logging.debug('')


def log_query3_results(aggregated_data: Dict, filtered_count: int, total_count: int) -> None:
    """Log aggregated results for Query 3."""
    logging.debug('Query 3 Results - Aggregated by store_id, year, semester:')
    valid_years_str = f"{{{VALID_YEARS[0]},{VALID_YEARS[1]}}}"
    logging.debug(
        f'Filtered {filtered_count}/{total_count} transactions '
        f'(year ∈ {valid_years_str}, hour ∈ [{MIN_HOUR:02d}:00-{MAX_HOUR:02d}:00])'
    )
    logging.debug('Store_ID | Year | Semester | Count | Original_Amount | Discounts | Final_Amount')
    logging.debug('-' * 80)

    for (store_id, year, semester), totals in aggregated_data.items():
        logging.debug(
            f'{store_id:8s} | {year:4d} | {semester:8d} | {totals["transaction_count"]:5d} | '
            f'{totals["total_original_amount"]:14.2f} | {totals["total_discount_applied"]:9.2f} | '
            f'{totals["total_final_amount"]:11.2f}'
        )

    logging.debug(f'Total groups: {len(aggregated_data)}')
    logging.debug('')


def log_query4_transactions(transaction_counts: Dict, filtered_count: int, total_count: int) -> None:
    """Log transaction counts for Query 4 - TABLE 1."""
    logging.debug(f'Query 4 - TABLE 1: Transaction counts by store and user ({VALID_YEARS[0]}-{VALID_YEARS[1]}):')
    valid_years_str = f"{{{VALID_YEARS[0]},{VALID_YEARS[1]}}}"
    logging.debug(f'Filtered {filtered_count}/{total_count} transactions (year ∈ {valid_years_str})')
    logging.debug('Store_ID | User_ID | Total_Purchases')
    logging.debug('-' * 40)

    for (store_id, user_id), total_purchases in transaction_counts.items():
        logging.debug(f'{store_id:8s} | {user_id:7s} | {total_purchases:15d}')

    logging.debug(f'Total customer-store combinations: {len(transaction_counts)}')
    logging.info('')


def log_query4_users(users: List[RawUser]) -> None:
    """Log user data for Query 4 - TABLE 2."""
    logging.debug('Query 4 - TABLE 2: Users data:')
    logging.debug('User_ID | Birthdate')
    logging.debug('-' * 25)

    for user in users:
        logging.debug(f'{user.user_id:7s} | {user.birthdate}')

    logging.debug(f'Total users: {len(users)}')
    logging.info('')


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
