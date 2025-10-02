
# ============================================================================
# DATE/TIME UTILITIES
# ============================================================================

from datetime import datetime

from constants import DATETIME_FORMAT, FIRST_SEMESTER, FIRST_SEMESTER_MONTHS, SECOND_SEMESTER


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

