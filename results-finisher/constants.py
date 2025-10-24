from enum import Enum

class QueryType(Enum):
    """
    Defines the types of queries the system can process.
    The integer values MUST correspond to the query_ids being sent.
    """
    Q1 = 0
    Q2 = 1
    Q3 = 2
    Q4 = 3
