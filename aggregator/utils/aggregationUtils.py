
# ============================================================================
# AGGREGATION UTILITIES
# ============================================================================

from collections import defaultdict
import logging
from typing import Dict, List

from aggregator.constants import VALID_YEARS
from protocol.entities import RawUser


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
