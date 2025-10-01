"""
Protocol message classes for handling different types of communication.
"""

import inspect
import socket
import struct
from typing import Any, Iterator, Tuple

from .constants import Opcodes, ProtocolError
from .entities import (
    RawMenuItems,
    RawStore,
    RawTransaction,
    RawTransactionItem,
    RawUser,
    RawTransactionStore,
    RawTransactionItemMenuItem,
    RawTransactionStoreUser,
)
from .parsing import (
    BytesReader,
    read_i32,
    read_i64,
    read_string,
    read_u8_with_remaining,
)


class TableMessage:
    """
    Base class for reading messages that contain tabular data.
    A table is a list of records, where each record is a key-value map.
    """

    def __init__(self, opcode: int, required_keys: Tuple[str, ...], row_factory):
        """Initialize a table message with opcode and row factory.
        required_keys queda solo como referencia, pero no se valida.
        """
        self.opcode = opcode
        self.required_keys = required_keys or ()
        self.rows = []
        self.amount = 0
        self.batch_number = 0
        self.batch_status = 0
        self._row_factory = row_factory

    @staticmethod
    def _to_kv_iter(row: Any) -> Iterator[Tuple[str, str]]:
        """
        Convierte una fila (dict o objeto) en pares (key:str, value:str).
        - dict: usa sus items
        - objeto: intenta vars(obj); filtra attrs privados/callables
        """
        if isinstance(row, dict):
            for k, v in row.items():
                yield str(k), "" if v is None else str(v)
            return

        # objeto "row_factory"
        try:
            attrs = vars(row)
        except TypeError:
            # Fallback: nada serializable
            return

        for k, v in attrs.items():
            if k.startswith("_"):
                continue
            if callable(v):
                continue
            yield str(k), "" if v is None else str(v)

    @staticmethod
    def _pack_string(s: str) -> bytes:
        b = s.encode("utf-8")
        return struct.pack("<I", len(b)) + b

    def _compute_amount_if_needed(self) -> int:
        """
        Si amount no refleja len(rows), lo sincroniza.
        No hace 'strict check' porque read_from ya lo popula;
        pero si vos modificaste rows luego, esto mantiene coherencia.
        """
        try:
            n = len(self.rows or [])
        except Exception:
            n = 0
        self.amount = n
        return n

    # === serialización simétrica a read_from ===
    def to_bytes(self) -> bytes:
        """
        Serializa el mensaje en el framing:
        [opcode:u8][length:i32][body]

        body:
          [nRows:i32][batchNumber:i64][status:u8]
          repeated nRows:
            [n_pairs:i32]  (cantidad de pares key/value)
            n_pairs * ([key_len:i32][key][val_len:i32][val])
        """
        n_rows = self._compute_amount_if_needed()
        batch_number = int(getattr(self, "batch_number", 0) or 0)
        batch_status = int(getattr(self, "batch_status", 0) or 0)

        parts = []
        parts.append(struct.pack("<I", n_rows))  # nRows
        parts.append(struct.pack("<Q", batch_number))  # batchNumber (u64 little-endian)
        parts.append(struct.pack("<B", batch_status))  # status (u8)

        rows_iterable = self.rows or []
        for row in rows_iterable:
            kv = list(self._to_kv_iter(row))
            parts.append(struct.pack("<I", len(kv)))  # n_pairs
            for k, v in kv:
                parts.append(self._pack_string(k))
                parts.append(self._pack_string(v))

        body = b"".join(parts)
        header = struct.pack("<B", int(self.opcode)) + struct.pack("<I", len(body))
        return header + body

    def _validate_pair_count(self, n_pairs: int):
        """Validate that the number of pairs matches required keys."""
        if n_pairs != len(self.required_keys):
            raise ProtocolError(
                f"Expected {len(self.required_keys)} pairs, got {n_pairs}", self.opcode
            )

    def _read_key_value_pairs(
        self, reader: BytesReader, remaining: int, n_pairs: int
    ) -> Tuple[dict[str, str], int]:
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
        payload = {k.lower(): v for k, v in current_row_data.items()}

        if self._row_factory in (None, dict):
            self.rows.append(payload)
            return

        try:
            sig = inspect.signature(self._row_factory)
            kwargs = {}
            for name, param in sig.parameters.items():
                if name in payload:
                    kwargs[name] = payload[name]
                else:
                    if param.default is inspect._empty and param.kind in (
                        inspect.Parameter.POSITIONAL_OR_KEYWORD,
                        inspect.Parameter.KEYWORD_ONLY,
                    ):
                        kwargs[name] = None

            obj = self._row_factory(**kwargs)
            self.rows.append(obj)
        except Exception:
            self.rows.append(payload)

    def _read_table_header(self, reader: BytesReader, remaining: int) -> int:
        """Read table header: number of rows, batch number and status."""
        (n_rows, remaining) = read_i32(reader, remaining, self.opcode)
        self.amount = n_rows

        (batch_number, remaining) = read_i64(reader, remaining, self.opcode)
        self.batch_number = batch_number

        (batch_status, remaining) = read_u8_with_remaining(
            reader, remaining, self.opcode
        )
        self.batch_status = batch_status

        return remaining

    def _read_row(self, reader: BytesReader, remaining: int) -> int:
        (n_pairs, remaining) = read_i32(reader, remaining, self.opcode)
        current_row_data, remaining = self._read_key_value_pairs(
            reader, remaining, n_pairs
        )
        self._create_row_object(current_row_data)
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
            remaining = self._read_table_header(reader, remaining)

            remaining = self._read_all_rows(reader, remaining, self.amount)

            self._validate_message_length(remaining)

        except ProtocolError:
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


class NewTransactionStores(TableMessage):
    """NEW_TRANSACTION_STORES message for joined transaction and store data."""

    def __init__(self):
        required = (
            "transaction_id",
            "store_id",
            "store_name",
            "city",
            "final_amount",
            "created_at",
        )
        super().__init__(
            opcode=Opcodes.NEW_TRANSACTION_STORES,
            required_keys=required,
            row_factory=RawTransactionStore,
        )


class NewTransactionItemsMenuItems(TableMessage):
    """NEW_TRANSACTION_ITEMS_MENU_ITEMS message for joined transaction item and menu item data."""

    def __init__(self):
        required = (
            "transaction_id",
            "item_name",
            "quantity",
            "subtotal",
            "created_at",
        )
        super().__init__(
            opcode=Opcodes.NEW_TRANSACTION_ITEMS_MENU_ITEMS,
            required_keys=required,
            row_factory=RawTransactionItemMenuItem,
        )


class NewTransactionStoresUsers(TableMessage):
    """NEW_TRANSACTION_STORES_USERS message for joined transaction, store, and user data."""

    def __init__(self):
        required = (
            "transaction_id",
            "store_id",
            "store_name",
            "user_id",
            "birthdate",
            "created_at",
        )
        super().__init__(
            opcode=Opcodes.NEW_TRANSACTION_STORES_USERS,
            required_keys=required,
            row_factory=RawTransactionStoreUser,
        )


class EOFMessage(TableMessage):
    """EOF message that inherits from TableMessage and specifies table type."""

    def __init__(self):
        # Require table_type to specify which table is ending
        super().__init__(
            opcode=Opcodes.EOF,
            required_keys=("table_type",),
            row_factory=lambda **kwargs: None,  # Don't create row objects
        )
        self.table_type = ""  # Store table type directly

    @staticmethod
    def deserialize_from_bytes(body: bytes) -> "EOFMessage":
        """Deserialize an EOFMessage from bytes.

        Args:
            body: The byte buffer containing the message body.

        Returns:
            An instance of EOFMessage with parsed data.
        """
        eof_msg = EOFMessage()
        eof_msg.read_from(body)
        return eof_msg

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
        body_parts.append(struct.pack("<I", 1))  # Little endian uint32

        # batchNumber (i64)
        body_parts.append(struct.pack("<Q", self.batch_number))  # Little endian uint64

        # status (u8)
        from .constants import BatchStatus

        body_parts.append(struct.pack("<B", BatchStatus.EOF))  # uint8

        # Row data: ["table_type", table_type_value]
        # n_pairs (i32) = 1
        body_parts.append(struct.pack("<I", 1))

        # Key: "table_type"
        key = "table_type"
        key_bytes = key.encode("utf-8")
        body_parts.append(struct.pack("<I", len(key_bytes)))  # key length
        body_parts.append(key_bytes)  # key data

        # Value: self.table_type
        if hasattr(self, "table_type") and self.table_type:
            value_bytes = self.table_type.encode("utf-8")
        else:
            # Fallback if table_type not set
            value_bytes = b"unknown"

        body_parts.append(struct.pack("<I", len(value_bytes)))  # value length
        body_parts.append(value_bytes)  # value data

        # Join all body parts
        body = b"".join(body_parts)

        # Create complete message: [opcode:u8][length:i32][body]
        message_parts = []
        message_parts.append(struct.pack("<B", self.opcode))  # opcode (u8)
        message_parts.append(struct.pack("<I", len(body)))  # length (i32)
        message_parts.append(body)  # body

        return b"".join(message_parts)


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
        from .socket_parsing import write_i32, write_u8

        write_u8(sock, self.opcode)
        write_i32(sock, 0)


class BatchRecvFail:
    """Outbound BATCH_RECV_SUCCESS response (empty body)."""

    def __init__(self):
        self.opcode = Opcodes.BATCH_RECV_FAIL

    def write_to(self, sock: socket.socket):
        """Frame and send the failure response: [opcode][length=0]."""
        from .socket_parsing import write_i32, write_u8

        write_u8(sock, self.opcode)
        write_i32(sock, 0)
