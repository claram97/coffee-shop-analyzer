from enum import Enum

class QueryType(Enum):
    """Defines the valid, known query types to avoid magic strings."""
    Q1 = "Q1"
    Q2 = "Q2"
    Q3 = "Q3"
    Q4 = "Q4"
    UNKNOWN = "UNKNOWN"