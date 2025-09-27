"""
Protocol constants and opcode definitions for the coffee shop analyzer.
"""

# Protocol limits
MAX_BATCH_SIZE_BYTES = 1024 * 1024  # 1MB - Límite máximo del tamaño de batch en bytes


class ProtocolError(Exception):
    """Represents a framing/validation error while parsing or writing messages.

    `opcode` optionally identifies the message context in which the error occurred.
    """

    def __init__(self, message, opcode=None):
        super().__init__(message)
        self.opcode = opcode


class Opcodes:
    """Protocol operation codes for different message types."""
    DATA_BATCH = 0  # Envuelve a los mensajes de tabla
    BETS_RECV_SUCCESS = 1
    BETS_RECV_FAIL = 2
    FINISHED = 3
    NEW_MENU_ITEMS = 4
    NEW_STORES = 5
    NEW_TRANSACTION_ITEMS = 6
    NEW_TRANSACTION = 7
    NEW_USERS = 8


class BatchStatus:
    """Status values for batch messages."""
    CONTINUE = 0  # Hay más batches en el archivo
    EOF = 1       # Último batch del archivo
    CANCEL = 2    # Batch enviado por cancelación