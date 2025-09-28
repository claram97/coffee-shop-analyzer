"""
Protocol message classes for handling different types of communication.
"""

import socket
from typing import Tuple

from .constants import ProtocolError, Opcodes
from .entities import RawMenuItems, RawStore, RawTransactionItem, RawTransaction, RawUser
from .parsing import BytesReader, read_i32, read_i64, read_u8_with_remaining, read_string


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

    def _read_key_value_pairs(self, reader: BytesReader, remaining: int, n_pairs: int) -> Tuple[dict[str, str], int]:
        """Read all key-value pairs for a row."""
        current_row_data: dict[str, str] = {}
        for _ in range(n_pairs):
            (key, remaining) = read_string(reader, remaining, self.opcode)
            (value, remaining) = read_string(reader, remaining, self.opcode)
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

    def _read_row(self, reader: BytesReader, remaining: int) -> int:
        """Read a single record (row) from the table."""
        # Read number of pairs
        (n_pairs, remaining) = read_i32(reader, remaining, self.opcode)
        
        # Validate number of pairs
        self._validate_pair_count(n_pairs)
        
        # Read all key-value pairs
        current_row_data, remaining = self._read_key_value_pairs(reader, remaining, n_pairs)
        
        # Validate that all required keys are present
        self._validate_required_keys(current_row_data)
        
        # Create row object
        self._create_row_object(current_row_data)
        
        return remaining

    def _read_table_header(self, reader: BytesReader, remaining: int) -> int:
        """Read table header: number of rows, batch number and status."""
        # Read number of rows
        (n_rows, remaining) = read_i32(reader, remaining, self.opcode)
        self.amount = n_rows
        
        # Read batch number
        (batch_number, remaining) = read_i64(reader, remaining, self.opcode)
        self.batch_number = batch_number
        
        # Read batch status
        (batch_status, remaining) = read_u8_with_remaining(reader, remaining, self.opcode)
        self.batch_status = batch_status
        
        return remaining

    def _read_all_rows(self, reader: BytesReader, remaining: int, n_rows: int) -> int:
        """Read all rows from the table."""
        for _ in range(n_rows):
            remaining = self._read_row(reader, remaining)
        return remaining

    def _validate_message_length(self, remaining: int):
        """Validate that no bytes remain unprocessed."""
        if remaining != 0:
            raise ProtocolError(
                "Indicated length doesn't match body length", self.opcode
            )

    def read_from(self, body_bytes: bytes):
        """
        Parse the complete table message body from a byte buffer.
        
        Args:
            body_bytes: The byte buffer containing the message body.
        """
        reader = BytesReader(body_bytes)
        remaining = len(body_bytes)
        
        try:
            # Read table header
            remaining = self._read_table_header(reader, remaining)
            
            # Read all rows
            remaining = self._read_all_rows(reader, remaining, self.amount)

            # Validate no bytes remain unprocessed
            self._validate_message_length(remaining)
            
        except ProtocolError:
            # The BytesReader will handle partially read data, so we just re-raise
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


class EOFMessage(TableMessage):
    """EOF message that inherits from TableMessage and specifies table type."""

    def __init__(self):
        # Require table_type to specify which table is ending
        super().__init__(
            opcode=Opcodes.EOF,
            required_keys=("table_type",),
            row_factory=lambda **kwargs: None  # Don't create row objects
        )
        self.table_type = ""  # Store table type directly
    
    def create_eof_message(self, batch_number: int, table_type: str):
        """Create an EOF message for a specific table type.
        
        Args:
            batch_number: The batch number to associate with this EOF
            table_type: The type of table ending (e.g., "menu_items", "stores", "transactions", etc.)
            
        Returns:
            self: Returns self for method chaining
        """
        from .constants import BatchStatus
        
        self.amount = 1  # One "virtual" row with table information
        self.batch_number = batch_number
        self.batch_status = BatchStatus.EOF
        self.table_type = table_type
        
        # No actual rows - rows list stays empty
        self.rows = []
        return self
    
    def _create_row_object(self, current_row_data: dict[str, str]):
        """Override to capture table_type but not create row objects."""
        # Extract table_type from the data but don't add to rows
        if "table_type" in current_row_data:
            self.table_type = current_row_data["table_type"]
        # Don't append to self.rows - EOF messages have no actual data rows
    
    def get_table_type(self) -> str:
        """Get the table type from the EOF message.
        
        Returns:
            The table type string
        """
        return self.table_type
    
    def to_bytes(self) -> bytes:
        """Serialize the EOF message to bytes following TableMessage protocol.
        
        Format: [opcode:u8][length:i32][nRows:i32][batchNumber:i64][status:u8][n_pairs:i32]["table_type"][table_type_value]
        
        Returns:
            The serialized message as bytes
        """
        import struct
        
        # Build the body first to calculate length
        body_parts = []
        
        # nRows (i32) - should be 1 for EOF with table_type info
        body_parts.append(struct.pack('<I', 1))  # Little endian uint32
        
        # batchNumber (i64) 
        body_parts.append(struct.pack('<Q', self.batch_number))  # Little endian uint64
        
        # status (u8)
        from .constants import BatchStatus
        body_parts.append(struct.pack('<B', BatchStatus.EOF))  # uint8
        
        # Row data: ["table_type", table_type_value]
        # n_pairs (i32) = 1
        body_parts.append(struct.pack('<I', 1))
        
        # Key: "table_type"
        key = "table_type"
        key_bytes = key.encode('utf-8')
        body_parts.append(struct.pack('<I', len(key_bytes)))  # key length
        body_parts.append(key_bytes)  # key data
        
        # Value: self.table_type
        if hasattr(self, 'table_type') and self.table_type:
            value_bytes = self.table_type.encode('utf-8')
        else:
            # Fallback if table_type not set
            value_bytes = b"unknown"
        
        body_parts.append(struct.pack('<I', len(value_bytes)))  # value length
        body_parts.append(value_bytes)  # value data
        
        # Join all body parts
        body = b''.join(body_parts)
        
        # Create complete message: [opcode:u8][length:i32][body]
        message_parts = []
        message_parts.append(struct.pack('<B', self.opcode))  # opcode (u8)
        message_parts.append(struct.pack('<I', len(body)))    # length (i32)
        message_parts.append(body)                            # body
        
        return b''.join(message_parts)


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
        from .socket_parsing import write_u8, write_i32
        write_u8(sock, self.opcode)
        write_i32(sock, 0)


class BatchRecvFail:
    """Outbound BATCH_RECV_SUCCESS response (empty body)."""

    def __init__(self):
        self.opcode = Opcodes.BATCH_RECV_FAIL

    def write_to(self, sock: socket.socket):
        """Frame and send the failure response: [opcode][length=0]."""
        from .socket_parsing import write_u8, write_i32
        write_u8(sock, self.opcode)
        write_i32(sock, 0)