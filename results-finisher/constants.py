from enum import Enum

class QueryType(Enum):
    """
    Defines the types of queries the system can process.
    The integer values MUST correspond to the query_ids being sent.
    """
    Q1 = 1
    Q2 = 2
    Q3 = 3
    Q4 = 4