"""
Provides a function for filtering raw data objects, selecting only the
columns required for downstream processing and analysis.
"""

from typing import List, Dict, Any


def filter_columns(rows: List[Any], columns_to_keep: List[str]) -> List[Dict[str, Any]]:
    """
    Selects a subset of attributes from a list of data objects and returns
    them as a new list of dictionaries.

    This function is primarily used for backward compatibility features and is
    no longer on the main performance-critical path for serialization.

    Args:
        rows: A list of data objects.
        columns_to_keep: A list of strings representing the attribute names
                         to retain.

    Returns:
        A new list of dictionaries, where each dictionary contains only the
        specified key-value pairs.
    """
    # Using a list comprehension is the most idiomatic and performant
    # way to accomplish this in pure Python.
    return [{col: getattr(row, col) for col in columns_to_keep} for row in rows]