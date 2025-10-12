"""
Protocol message classes for handling different types of communication.
"""

import inspect
import socket
import struct
import uuid
from typing import Any, Callable, Iterator, List, Optional, Tuple

from .constants import Opcodes, ProtocolError
from .entities import (
    RawMenuItems,
    RawStore,
    RawTransaction,
    RawTransactionItem,
    RawTransactionItemMenuItem,
    RawTransactionStore,
    RawTransactionStoreUser,
    RawUser,
    ResultFilteredTransaction,
    ResultProductMetrics,
    ResultStoreTPV,
    ResultTopCustomer,
    ResultError
)
from .parsing import (
    BytesReader,
    read_i32,
    read_i64,
    read_string,
    read_u8_with_remaining,
)

_ROW_FACTORY_SIGNATURE_CACHE: dict[Any, Optional[tuple[tuple[str, ...], tuple[str, ...], bool]]] = {}
_SKIP_ROW = object()

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
        self.amount = 0
        self.batch_number = 0
        self.batch_status = 0
        self._row_factory = row_factory
        self._row_factory_meta = self._prepare_row_factory(row_factory)
        self._rows_cache: List[Any] = []
        self._rows_loaded = True
        self._raw_body_bytes: Optional[bytes] = None
        self._raw_rows_offset = 0
        self._raw_rows_length = 0
        self.rows = []

    def _prepare_row_factory(
        self, factory: Optional[Callable[..., Any]]
    ) -> Optional[tuple[tuple[str, ...], tuple[str, ...], bool]]:
        if factory in (None, dict):
            return None

        cached = _ROW_FACTORY_SIGNATURE_CACHE.get(factory)
        if cached is not None:
            return cached

        try:
            sig = inspect.signature(factory)
        except (TypeError, ValueError):
            _ROW_FACTORY_SIGNATURE_CACHE[factory] = None
            return None

        required: list[str] = []
        optional: list[str] = []
        has_var_kw = False

        for name, param in sig.parameters.items():
            if param.kind == inspect.Parameter.VAR_KEYWORD:
                has_var_kw = True
                continue
            if param.kind not in (
                inspect.Parameter.POSITIONAL_ONLY,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
                inspect.Parameter.KEYWORD_ONLY,
            ):
                continue
            if param.default is inspect._empty:
                required.append(name)
            else:
                optional.append(name)

        meta = (tuple(required), tuple(optional), has_var_kw)
        _ROW_FACTORY_SIGNATURE_CACHE[factory] = meta
        return meta

    @property
    def rows(self) -> List[Any]:
        if not self._rows_loaded:
            self._rows_cache = self._load_rows_from_raw()
        return self._rows_cache

    @rows.setter
    def rows(self, value):
        if value is None:
            self._rows_cache = []
        elif isinstance(value, list):
            self._rows_cache = value
        else:
            self._rows_cache = list(value)
        self._rows_loaded = True
        self._raw_body_bytes = None
        self._raw_rows_offset = 0
        self._raw_rows_length = 0
        self.amount = len(self._rows_cache)

    def _load_rows_from_raw(self) -> List[Any]:
        if not self._raw_body_bytes or self._raw_rows_length == 0:
            self._rows_loaded = True
            self._raw_body_bytes = None
            return []

        buffer = self._raw_body_bytes[
            self._raw_rows_offset : self._raw_rows_offset + self._raw_rows_length
        ]
        reader = BytesReader(buffer)
        remaining = self._raw_rows_length

        rows, remaining = self._read_all_rows(reader, remaining, self.amount)

        self._validate_message_length(remaining)

        self._rows_loaded = True
        self._raw_body_bytes = None
        self._raw_rows_offset = 0
        self._raw_rows_length = 0
        self._rows_cache = rows
        return rows

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
            return payload

        try:
            if self._row_factory_meta is None:
                return self._row_factory(**payload)

            required, optional, has_var_kw = self._row_factory_meta
            kwargs: dict[str, Any] = {}

            for name in required:
                kwargs[name] = payload.get(name)

            for name in optional:
                if name in payload:
                    kwargs[name] = payload[name]

            if has_var_kw:
                for name, value in payload.items():
                    if name not in kwargs:
                        kwargs[name] = value

            return self._row_factory(**kwargs)
        except Exception:
            return payload

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

    def _read_row(self, reader: BytesReader, remaining: int) -> Tuple[Any, int]:
        (n_pairs, remaining) = read_i32(reader, remaining, self.opcode)
        current_row_data, remaining = self._read_key_value_pairs(
            reader, remaining, n_pairs
        )
        row_obj = self._create_row_object(current_row_data)
        return row_obj, remaining

    def _read_all_rows(
        self, reader: BytesReader, remaining: int, n_rows: int
    ) -> Tuple[List[Any], int]:
        """Read all rows from the table."""
        rows: list[Any] = []
        local_remaining = remaining
        for _ in range(n_rows):
            row_obj, local_remaining = self._read_row(reader, local_remaining)
            if row_obj is _SKIP_ROW:
                continue
            rows.append(row_obj)
        return rows, local_remaining

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
            self._raw_body_bytes = body_bytes
            self._raw_rows_offset = len(body_bytes) - remaining
            self._raw_rows_length = remaining

            if self.amount and remaining == 0:
                raise ProtocolError(
                    "Indicated length doesn't match body length", self.opcode
                )

            if remaining == 0 or self.amount == 0:
                self._rows_cache = []
                self._rows_loaded = True
                self._raw_body_bytes = None
                self._raw_rows_offset = 0
                self._raw_rows_length = 0
            else:
                self._rows_cache = []
                self._rows_loaded = False

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
            "voucher_id",
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


# --- Query Result Messages ---
class QueryResult1(TableMessage):
    """Result message for Query 1: Filtered transactions."""
    
    def __init__(self):
        required = ("transaction_id", "final_amount")
        super().__init__(
            opcode=Opcodes.QUERY_RESULT_1,
            required_keys=required,
            row_factory=ResultFilteredTransaction
        )


class QueryResult2(TableMessage):
    """Result message for Query 2: Product metrics ranked by sales and revenue."""
    
    def __init__(self):
        required = ("month", "name")
        # Optional fields: quantity, revenue (depending on metric type)
        super().__init__(
            opcode=Opcodes.QUERY_RESULT_2,
            required_keys=required,
            row_factory=ResultProductMetrics
        )


class QueryResult3(TableMessage):
    """Result message for Query 3: TPV analysis by store and semester."""
    
    def __init__(self):
        required = ("store_name", "period", "amount")
        super().__init__(
            opcode=Opcodes.QUERY_RESULT_3,
            required_keys=required,
            row_factory=ResultStoreTPV
        )


class QueryResult4(TableMessage):
    """Result message for Query 4: Top customers by store."""
    
    def __init__(self):
        required = ("store_name", "birthdate", "purchase_count")
        super().__init__(
            opcode=Opcodes.QUERY_RESULT_4,
            required_keys=required,
            row_factory=ResultTopCustomer
        )


class QueryResultError(TableMessage):
    """Error result message for any query type."""
    
    def __init__(self):
        required = ("query_id", "error_code", "error_message")
        super().__init__(
            opcode=Opcodes.QUERY_RESULT_ERROR,
            required_keys=required,
            row_factory=ResultError
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
        self.client_id: Optional[str] = None

    def read_from(self, body_bytes: bytes):
        reader = BytesReader(body_bytes)
        remaining = len(body_bytes)

        (n_rows, remaining) = read_i32(reader, remaining, self.opcode)
        if n_rows != 1:
            raise ProtocolError("EOF message must contain exactly one metadata row", self.opcode)

        (batch_number, remaining) = read_i64(reader, remaining, self.opcode)
        self.batch_number = batch_number

        (batch_status, remaining) = read_u8_with_remaining(reader, remaining, self.opcode)
        self.batch_status = batch_status

        (n_pairs, remaining) = read_i32(reader, remaining, self.opcode)
        if n_pairs != 1:
            raise ProtocolError("EOF message metadata row must contain exactly one key/value pair", self.opcode)

        key, remaining = read_string(reader, remaining, self.opcode)
        value, remaining = read_string(reader, remaining, self.opcode)

        if key != "table_type":
            raise ProtocolError("EOF metadata key must be 'table_type'", self.opcode)

        self.table_type = value
        self.rows = []
        self.amount = 1

        if remaining > 0:
            (flag, remaining) = read_u8_with_remaining(reader, remaining, self.opcode)
            if flag not in (0, 1):
                raise ProtocolError("invalid EOF client_id flag", self.opcode)

            if flag == 1:
                if remaining < 16:
                    raise ProtocolError("EOF client_id is truncated", self.opcode)
                client_bytes = reader.read(16)
                remaining -= 16
                self.client_id = str(uuid.UUID(bytes=client_bytes))
            else:
                self.client_id = None
        else:
            self.client_id = None

        if remaining != 0:
            raise ProtocolError("Indicated length doesn't match body length", self.opcode)

    @staticmethod
    def deserialize_from_bytes(buf: bytes) -> "EOFMessage":
        if not buf:
            raise ProtocolError("empty EOF buffer", Opcodes.EOF)

        if buf[0] == Opcodes.EOF and len(buf) >= 5:
            length = int.from_bytes(buf[1:5], "little", signed=True)
            if length < 0 or len(buf) != 5 + length:
                raise ProtocolError("invalid EOF frame length", Opcodes.EOF)
            body = buf[5:]
        else:
            body = buf

        msg = EOFMessage()
        msg.read_from(body)
        return msg

    def create_eof_message(self, batch_number: int, table_type: str, client_id: Optional[str] = None):
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
        self.amount = 1
        self.client_id = client_id
        return self

    def _create_row_object(self, current_row_data: dict[str, str]):
        """Override to capture table_type but not create row objects."""
        # Extract table_type from the data but don't add to rows
        if "table_type" in current_row_data:
            self.table_type = current_row_data["table_type"]
        # Don't append to self.rows - EOF messages have no actual data rows
        return _SKIP_ROW

    def get_table_type(self) -> str:
        """Get the table type from the EOF message.

        Returns:
            The table type string
        """
        return self.table_type

    def to_bytes(self) -> bytes:
        """Serialize the EOF message to bytes following TableMessage protocol.

        Format: [opcode:u8][length:i32]
                [nRows:i32][batchNumber:i64][status:u8]
                [n_pairs:i32]["table_type"][table_type_value]
                [has_client_id:u8][client_id?:16]

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

        # Client ID flag + optional bytes
        has_client_id = 1 if self.client_id else 0
        body_parts.append(struct.pack("<B", has_client_id))

        if has_client_id:
            try:
                uuid_bytes = uuid.UUID(str(self.client_id)).bytes
            except (ValueError, AttributeError) as exc:
                raise ProtocolError("client_id must be a valid UUID string", self.opcode) from exc
            body_parts.append(uuid_bytes)

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

    def read_from(self, data, length: int):
        """Validate fixed body length (4) and read agency_id.
        
        Args:
            data: Either a socket object or bytes buffer
            length: Expected body length (should be 4)
        """
        if length != self._length:
            raise ProtocolError("invalid length", self.opcode)
        
        # Handle different input types
        if isinstance(data, bytes):
            reader = BytesReader(data)
            (agency_id, _) = read_i32(reader, length, self.opcode)
        else:
            from .socket_parsing import read_i32 as socket_read_i32
            (agency_id, _) = socket_read_i32(data, length, self.opcode)
        
        self.agency_id = agency_id


class ClientHello:
    """Inbound CLIENT_HELLO message carrying a 16-byte client UUID."""

    def __init__(self):
        self.opcode = Opcodes.CLIENT_HELLO
        self.client_id_bytes = None
        self.client_id: Optional[str] = None
        self._length = 16

    def read_from(self, data, length: int):
        if length != self._length:
            raise ProtocolError("invalid length", self.opcode)

        if isinstance(data, bytes):
            reader = BytesReader(data)
            self.client_id_bytes = reader.read(self._length)
        else:
            from .socket_parsing import read_exact

            self.client_id_bytes = read_exact(data, self._length)

        if len(self.client_id_bytes) != self._length:
            raise ProtocolError("invalid client id size", self.opcode)

        try:
            self.client_id = str(uuid.UUID(bytes=self.client_id_bytes))
        except (ValueError, AttributeError) as exc:
            raise ProtocolError("invalid client id", self.opcode) from exc


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
