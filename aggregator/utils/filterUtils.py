from aggregator.constants import MAX_HOUR, MIN_HOUR, VALID_YEARS
from aggregator.utils.datetimeUtils import parse_datetime
from protocol.entities import RawTransaction



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
