"""
Protocol message classes for handling different types of communication.
"""

import socket
from typing import Tuple

from .constants import ProtocolError, Opcodes
from .entities import RawMenuItems, RawStore, RawTransactionItem, RawTransaction, RawUser
from .parsing import read_i32, read_i64, read_u8_with_remaining, read_string, recv_exactly


class TableMessage:
    """
    Base class for reading messages that contain tabular data.
    A table is a list of records, where each record is a key-value map.
    """

    def __init__(self, opcode: int, required_keys: Tuple[str, ...], row_factory):
        """Initialize a table message with opcode, required keys, and row factory.
        
        Args:
            opcode: The message opcode
            required_keys: Tuple of required column names
            row_factory: Function/class to create row objects from dict
        """
        self.opcode = opcode
        self.required_keys = required_keys
        self.rows = []  # Will store created objects (e.g., RawMenuItems, RawStore)
        self.amount = 0
        self.batch_number = 0  # Client batch number
        self.batch_status = 0  # Batch status (Continue/EOF/Cancel)
        # `row_factory` is a function or class that converts a dict to an object
        self._row_factory = row_factory

    def _validate_pair_count(self, n_pairs: int):
        """Validate that the number of pairs matches required keys."""
        if n_pairs != len(self.required_keys):
            raise ProtocolError(
                f"Expected {len(self.required_keys)} pairs, got {n_pairs}", self.opcode
            )

    def _read_key_value_pairs(self, sock: socket.socket, remaining: int, n_pairs: int) -> Tuple[dict[str, str], int]:
        """Read all key-value pairs for a row."""
        current_row_data: dict[str, str] = {}
        for _ in range(n_pairs):
            (key, remaining) = read_string(sock, remaining, self.opcode)
            (value, remaining) = read_string(sock, remaining, self.opcode)
            current_row_data[key] = value
        return current_row_data, remaining

    def _validate_required_keys(self, current_row_data: dict[str, str]):
        """Validate that all required keys are present."""
        missing_keys = [
            key for key in self.required_keys if key not in current_row_data
        ]
        if missing_keys:
            received_keys = list(current_row_data.keys())
            raise ProtocolError(
                f"Missing required keys: {missing_keys}. Received keys: {received_keys}. Expected: {list(self.required_keys)}",
                self.opcode,
            )

    def _create_row_object(self, current_row_data: dict[str, str]):
        """Create row object using the row_factory."""
        # Default: keys to lowercase
        kwargs = {key.lower(): value for key, value in current_row_data.items()}
        self.rows.append(self._row_factory(**kwargs))

    def _read_row(self, sock: socket.socket, remaining: int) -> int:
        """Read a single record (row) from the table."""
        # Read number of pairs
        (n_pairs, remaining) = read_i32(sock, remaining, self.opcode)
        
        # Validate number of pairs
        self._validate_pair_count(n_pairs)
        
        # Read all key-value pairs
        current_row_data, remaining = self._read_key_value_pairs(sock, remaining, n_pairs)
        
        # Validate that all required keys are present
        self._validate_required_keys(current_row_data)
        
        # Create row object
        self._create_row_object(current_row_data)
        
        return remaining

    def _read_table_header(self, sock: socket.socket, remaining: int) -> int:
        """Read table header: number of rows, batch number and status."""
        # Read number of rows
        (n_rows, remaining) = read_i32(sock, remaining, self.opcode)
        self.amount = n_rows
        
        # Read batch number
        (batch_number, remaining) = read_i64(sock, remaining, self.opcode)
        self.batch_number = batch_number
        
        # Read batch status
        (batch_status, remaining) = read_u8_with_remaining(sock, remaining, self.opcode)
        self.batch_status = batch_status
        
        return remaining

    def _read_all_rows(self, sock: socket.socket, remaining: int, n_rows: int) -> int:
        """Read all rows from the table."""
        for _ in range(n_rows):
            remaining = self._read_row(sock, remaining)
        return remaining

    def _validate_message_length(self, remaining: int):
        """Validate that no bytes remain unprocessed."""
        if remaining != 0:
            raise ProtocolError(
                "Indicated length doesn't match body length", self.opcode
            )

    def _handle_protocol_error(self, sock: socket.socket, remaining: int):
        """Handle protocol errors by consuming remaining bytes."""
        if remaining > 0:
            _ = recv_exactly(sock, remaining)

    def read_from(self, sock: socket.socket, length: int):
        """Parse the complete table message body.
        
        Client message format:
        [length:i32][nRows:i32][batchNumber:i64][status:u8][rows...]
        """
        remaining = length
        try:
            # Read table header
            remaining = self._read_table_header(sock, remaining)
            
            # Read all rows
            remaining = self._read_all_rows(sock, remaining, self.amount)

            # Validate no bytes remain unprocessed
            self._validate_message_length(remaining)
            
        except ProtocolError:
            self._handle_protocol_error(sock, remaining)
            raise


class NewMenuItems(TableMessage):
    """NEW_MENU_ITEMS message that inherits from TableMessage."""

    def __init__(self):
        required = (
            "product_id",
            "name",
            "category",
            "price",
            "is_seasonal",
            "available_from",
            "available_to",
        )
        super().__init__(
            opcode=Opcodes.NEW_MENU_ITEMS,
            required_keys=required,
            row_factory=RawMenuItems,
        )


class NewTransactionItems(TableMessage):
    """NEW_TRANSACTION_ITEMS message that inherits from TableMessage."""

    def __init__(self):
        required = (
            "transaction_id",
            "item_id",
            "quantity",
            "unit_price",
            "subtotal",
            "created_at",
        )
        super().__init__(
            opcode=Opcodes.NEW_TRANSACTION_ITEMS,
            required_keys=required,
            row_factory=RawTransactionItem,
        )


class NewTransactions(TableMessage):
    """NEW_TRANSACTIONS message that inherits from TableMessage."""

    def __init__(self):
        required = (
            "transaction_id",
            "store_id",
            "payment_method_id",
            "user_id",
            "original_amount",
            "discount_applied",
            "final_amount",
            "created_at",
        )
        super().__init__(
            opcode=Opcodes.NEW_TRANSACTION,
            required_keys=required,
            row_factory=RawTransaction,
        )


class NewUsers(TableMessage):
    """NEW_USERS message that inherits from TableMessage."""

    def __init__(self):
        required = ("user_id", "gender", "birthdate", "registered_at")
        super().__init__(
            opcode=Opcodes.NEW_USERS,
            required_keys=required,
            row_factory=RawUser,
        )


class NewStores(TableMessage):
    """NEW_STORES message that inherits from TableMessage."""

    def __init__(self):
        required = (
            "store_id",
            "store_name",
            "street",
            "postal_code",
            "city",
            "state",
            "latitude",
            "longitude",
        )
        super().__init__(
            opcode=Opcodes.NEW_STORES,
            required_keys=required,
            row_factory=RawStore,
        )


class Finished:
    """Inbound FINISHED message. Body is a single agency_id (i32 LE)."""

    def __init__(self):
        self.opcode = Opcodes.FINISHED
        self.agency_id = None
        self._length = 4

    def read_from(self, sock: socket.socket, length: int):
        """Validate fixed body length (4) and read agency_id."""
        if length != self._length:
            raise ProtocolError("invalid length", self.opcode)
        (agency_id, _) = read_i32(sock, length, self.opcode)
        self.agency_id = agency_id


class BatchRecvSuccess:
    """Outbound BATCH_RECV_SUCCESS response (empty body)."""

    def __init__(self):
        self.opcode = Opcodes.BATCH_RECV_SUCCESS

    def write_to(self, sock: socket.socket):
        """Frame and send the success response: [opcode][length=0]."""
        from .parsing import write_u8, write_i32
        write_u8(sock, self.opcode)
        write_i32(sock, 0)


class BatchRecvFail:
    """Outbound BATCH_RECV_SUCCESS response (empty body)."""

    def __init__(self):
        self.opcode = Opcodes.BATCH_RECV_FAIL

    def write_to(self, sock: socket.socket):
        """Frame and send the failure response: [opcode][length=0]."""
        from .parsing import write_u8, write_i32
        write_u8(sock, self.opcode)
        write_i32(sock, 0)