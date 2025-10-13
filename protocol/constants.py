"""
Defines the core constants, custom exceptions, and enumerations for the
coffee shop data transfer protocol.

This includes message operation codes (opcodes), batch processing statuses,
and protocol-wide limits.
"""

# The maximum size in bytes for a single batch message payload. This acts as a
# safeguard against excessively large messages consuming too much memory.
MAX_BATCH_SIZE_BYTES = 1024 * 1024  # 1MB


class ProtocolError(Exception):
    """
    A custom exception raised for framing or validation errors encountered
    during the parsing or writing of protocol messages.

    Attributes:
        opcode (int, optional): Identifies the message context (the operation code)
                                in which the error occurred, aiding in debugging.
    """

    def __init__(self, message, opcode=None):
        super().__init__(message)
        self.opcode = opcode


class Opcodes:
    """
    Defines the protocol operation codes (opcodes) used to identify the
    type of each message.
    """
    # A wrapper message that contains another, more specific message type inside.
    # Used for routing and generic batch handling.
    DATA_BATCH = 0

    # Acknowledgment sent by a receiver to indicate successful processing of a batch.
    BATCH_RECV_SUCCESS = 1

    # Notification sent by a receiver to indicate a failure in processing a batch.
    BATCH_RECV_FAIL = 2

    # A control signal indicating the sender has finished transmitting all data.
    FINISHED = 3

    # A data message containing a batch of new menu items.
    NEW_MENU_ITEMS = 4

    # A data message containing a batch of new stores.
    NEW_STORES = 5

    # A data message containing a batch of new items within transactions.
    NEW_TRANSACTION_ITEMS = 6

    # A data message containing a batch of new transactions.
    NEW_TRANSACTION = 7

    # A data message containing a batch of new users.
    NEW_USERS = 8

    # A control signal indicating end of file/stream.
    EOF = 9

    # --- Joined Data Messages ---
    # A data message containing transactions joined with store information.
    NEW_TRANSACTION_STORES = 10

    # A data message containing transaction items joined with menu item information.
    NEW_TRANSACTION_ITEMS_MENU_ITEMS = 11

    # A data message containing transactions joined with store and user information.
    NEW_TRANSACTION_STORES_USERS = 12

    # --- Query Result Messages ---
    # Result message for Query 1: Filtered transactions
    QUERY_RESULT_1 = 20
    
    # Result message for Query 2: Product metrics ranked by sales and revenue
    QUERY_RESULT_2 = 21
    
    # Result message for Query 3: TPV analysis by store and semester
    QUERY_RESULT_3 = 22
    
    # Result message for Query 4: Top customers by store
    QUERY_RESULT_4 = 23
    
    # Error result message for any query type
    QUERY_RESULT_ERROR = 29

    # Client identification handshake, sent once per TCP connection
    CLIENT_HELLO = 30


class BatchStatus:
    """
    Defines status codes included in data batch messages to indicate the state
    of the data stream.
    """
    # Indicates that more data batches will follow this one.
    CONTINUE = 0

    # Indicates this is the final data batch in the stream (End of File).
    EOF = 1

    # Indicates that this batch was sent as part of a cancellation process.
    CANCEL = 2