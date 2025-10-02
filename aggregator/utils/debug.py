# ============================================================================
# LOGGING UTILITIES
# ============================================================================

import logging
from typing import Dict

from utils.filterUtils import MAX_HOUR, MIN_HOUR, VALID_YEARS


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
