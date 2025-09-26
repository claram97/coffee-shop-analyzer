"""
DataBatch message implementation for wrapping table data with metadata.
"""

import logging
import socket
from typing import List, Dict, Optional

from .constants import ProtocolError, Opcodes
from .parsing import (
    read_u16, read_i32, read_i64, read_u8_with_remaining, 
    read_u8_list, read_u8_u8_dict, recv_exactly
)
from .messages import (
    Finished, NewMenuItems, NewStores, NewTransactionItems, 
    NewTransactions, NewUsers
)


def instantiate_message_for_opcode(opcode: int):
    """Create an appropriate message instance for the given opcode (inner)."""
    if opcode == Opcodes.FINISHED:
        return Finished()
    elif opcode == Opcodes.NEW_MENU_ITEMS:
        return NewMenuItems()
    elif opcode == Opcodes.NEW_STORES:
        return NewStores()
    elif opcode == Opcodes.NEW_TRANSACTION_ITEMS:
        return NewTransactionItems()
    elif opcode == Opcodes.NEW_TRANSACTION:
        return NewTransactions()
    elif opcode == Opcodes.NEW_USERS:
        return NewUsers()
    else:
        raise ProtocolError(f"invalid embedded opcode: {opcode}", Opcodes.DATA_BATCH)


class DataBatch:
    """
    DataBatch message format:
    [opcode=DATA_BATCH][i32 length]
      [u8 list: table_ids]
      [u8 list: query_ids]
      [u16 reserved]              # flags / reserved
      [i64 batch_number]
      [dict<u8,u8> meta]
      [u16 total_shards]
      [u16 shard_num]
      [embedded message]          # [u8 inner_opcode][i32 inner_len][inner_body]
    """

    def _initialize_fields(
        self,
        table_ids: Optional[List[int]],
        query_ids: Optional[List[int]], 
        meta: Optional[Dict[int, int]],
        total_shards: Optional[int],
        shard_num: Optional[int],
        reserved_u16: int,
        batch_bytes: Optional[bytes]
    ):
        """Initialize all DataBatch fields with defaults or provided values."""
        self.opcode = Opcodes.DATA_BATCH

        # If fields come as None, we assume this is for DESERIALIZATION
        # and initialize with neutral values; read_from will complete them later.
        self.table_ids: List[int] = [] if table_ids is None else list(table_ids)
        self.query_ids: List[int] = [] if query_ids is None else list(query_ids)
        self.reserved_u16: int = int(reserved_u16)
        self.meta: Dict[int, int] = {} if meta is None else dict(meta)
        self.total_shards: int = 0 if total_shards is None else int(total_shards)
        self.shard_num: int = 0 if shard_num is None else int(shard_num)

        # Embedded content:
        self.batch_msg = None  # parsed instance (deserialization only)
        self.batch_bytes: Optional[bytes] = batch_bytes  # raw framing (serialization)

    def _validate_u8_list(self, items: List[int], field_name: str):
        """Validate that a list contains only valid u8 values."""
        if len(items) > 255:
            raise ValueError(f"{field_name} must have at most 255 items")
        if any(not 0 <= x <= 255 for x in items):
            raise ValueError(f"{field_name} must contain only u8 values (0-255)")

    def _validate_u8_dict(self, meta_dict: Dict[int, int]):
        """Validate that a dictionary has valid u8 keys and values."""
        if len(meta_dict) > 255:
            raise ValueError("meta size must be <= 255")
        for k, v in meta_dict.items():
            if not (0 <= int(k) <= 255 and 0 <= int(v) <= 255):
                raise ValueError("meta keys/values must be u8")

    def _validate_u16_field(self, value: int, field_name: str):
        """Validate that a value is in valid u16 range."""
        if not (0 <= value <= 0xFFFF):
            raise ValueError(f"{field_name} must be u16 (0-65535)")

    def _validate_serialization_parameters(
        self,
        table_ids: Optional[List[int]],
        query_ids: Optional[List[int]],
        meta: Optional[Dict[int, int]]
    ):
        """Validate parameters only if object seems destined for serialization."""
        # Validations only if object seems destined for serialization
        if table_ids is not None:
            self._validate_u8_list(self.table_ids, "table_ids")
        
        if query_ids is not None:
            self._validate_u8_list(self.query_ids, "query_ids")
        
        if meta is not None:
            self._validate_u8_dict(self.meta)
        
        # Validate u16 fields
        self._validate_u16_field(self.reserved_u16, "reserved_u16")
        self._validate_u16_field(self.total_shards, "total_shards")
        self._validate_u16_field(self.shard_num, "shard_num")

    def __init__(
        self,
        *,
        # Parameters for SERIALIZATION (all previous to internal batch):
        table_ids: Optional[List[int]] = None,
        query_ids: Optional[List[int]] = None,
        meta: Optional[Dict[int, int]] = None,
        total_shards: Optional[int] = None,
        shard_num: Optional[int] = None,
        reserved_u16: int = 0,
        # Already framed bytes of embedded message (optional; required for write_to)
        batch_bytes: Optional[bytes] = None,
    ):
        # Initialize all fields
        self._initialize_fields(
            table_ids, query_ids, meta, total_shards, 
            shard_num, reserved_u16, batch_bytes
        )

        # Validate serialization parameters
        self._validate_serialization_parameters(table_ids, query_ids, meta)

    @staticmethod
    def make_embedded(inner_opcode: int, inner_body: bytes) -> bytes:
        """
        Assemble [u8 opcode][i32 length][body] from an opcode and unframed body.
        """
        if not (0 <= inner_opcode <= 255):
            raise ValueError("inner_opcode must be u8")
        if not isinstance(inner_body, (bytes, bytearray)):
            raise TypeError("inner_body must be bytes")
        framed = bytearray()
        framed.append(int(inner_opcode))
        framed.extend(len(inner_body).to_bytes(4, "little", signed=True))
        framed.extend(inner_body)
        return bytes(framed)

    def _read_data_batch_header(self, sock: socket.socket, remaining: int) -> int:
        """Read DataBatch header: table_ids, query_ids, reserved, batch_number, meta, shards."""
        self.table_ids, remaining = read_u8_list(sock, remaining, self.opcode)
        self.query_ids, remaining = read_u8_list(sock, remaining, self.opcode)
        self.reserved_u16, remaining = read_u16(sock, remaining, self.opcode)
        self.batch_number, remaining = read_i64(sock, remaining, self.opcode)
        self.meta, remaining = read_u8_u8_dict(sock, remaining, self.opcode)
        self.total_shards, remaining = read_u16(sock, remaining, self.opcode)
        self.shard_num, remaining = read_u16(sock, remaining, self.opcode)
        return remaining

    def _read_embedded_message(self, sock: socket.socket, remaining: int) -> int:
        """Read embedded message within DataBatch."""
        # Read opcode and length of inner message
        inner_opcode, remaining = read_u8_with_remaining(sock, remaining, self.opcode)
        inner_len, remaining = read_i32(sock, remaining, self.opcode)

        # Create and instantiate appropriate message
        inner_msg = instantiate_message_for_opcode(inner_opcode)
        inner_msg.read_from(sock, inner_len)  # this call consumes exactly inner_len or throws
        self.batch_msg = inner_msg
        
        # Update remaining after reading embedded message
        remaining -= inner_len
        return remaining

    def _validate_remaining_bytes(self, remaining: int):
        """Validate that no bytes remain unprocessed after deserialization."""
        if remaining != 0:
            raise ProtocolError(
                "Indicated length doesn't match body length", self.opcode
            )

    def _handle_protocol_error(self, sock: socket.socket, remaining: int):
        """Handle protocol errors by consuming remaining bytes."""
        if remaining > 0:
            _ = recv_exactly(sock, remaining)

    def read_from(self, sock: socket.socket, length: int):
        """Deserialize DataBatch from socket."""
        remaining = length
        try:
            # Read DataBatch header
            remaining = self._read_data_batch_header(sock, remaining)

            # Read embedded message
            remaining = self._read_embedded_message(sock, remaining)

            # Validate no bytes remain unprocessed
            self._validate_remaining_bytes(remaining)

        except ProtocolError:
            self._handle_protocol_error(sock, remaining)
            raise

    def write_to(self, sock: socket.socket):
        """
        Serialize DataBatch and write to `sock`.
        Requires `self.batch_bytes` with already framed embedded message
        ([u8 opcode][i32 length][body]).
        """
        # Generate bytes using to_bytes() and send them
        message_bytes = self.to_bytes()
        sock.sendall(message_bytes)

    def _validate_batch_bytes(self):
        """Validate that batch_bytes is present and has correct format."""
        if self.batch_bytes is None:
            raise ProtocolError(
                "missing embedded batch bytes for DataBatch serialization", self.opcode
            )

        # Light validation of embedded framing
        if len(self.batch_bytes) < 5:
            raise ProtocolError("invalid embedded framing (too short)", self.opcode)
        
        # Internal length consistency check
        inner_len = int.from_bytes(self.batch_bytes[1:5], "little", signed=True)
        if inner_len < 0 or inner_len != len(self.batch_bytes) - 5:
            raise ProtocolError(
                "invalid embedded framing (length mismatch)", self.opcode
            )

    def _serialize_u8_list(self, items: List[int]) -> bytearray:
        """Serialize list as [u8 count] + items."""
        result = bytearray()
        result.append(len(items))
        result.extend(bytes(items))
        return result

    def _serialize_u8_dict(self, meta_dict: Dict[int, int]) -> bytearray:
        """Serialize dictionary as [u8 count] + key-value pairs."""
        result = bytearray()
        result.append(len(meta_dict))
        for k, v in meta_dict.items():
            result.append(int(k))
            result.append(int(v))
        return result

    def _serialize_data_batch_body(self) -> bytearray:
        """Serialize complete DataBatch body."""
        body = bytearray()

        # [u8 list: table_ids]
        body.extend(self._serialize_u8_list(self.table_ids))

        # [u8 list: query_ids]
        body.extend(self._serialize_u8_list(self.query_ids))

        # [u16 reserved]
        body.extend(int(self.reserved_u16).to_bytes(2, "little", signed=False))

        # [i64 batch_number]
        body.extend(int(self.batch_number).to_bytes(8, "little", signed=True))

        # [dict<u8,u8> meta]
        body.extend(self._serialize_u8_dict(self.meta))

        # [u16 total_shards][u16 shard_num]
        body.extend(int(self.total_shards).to_bytes(2, "little", signed=False))
        body.extend(int(self.shard_num).to_bytes(2, "little", signed=False))

        # [embedded message bytes]
        body.extend(self.batch_bytes)

        return body

    def _create_final_message(self, body: bytearray) -> bytes:
        """Create final message with header [opcode][length][body]."""
        final_message = bytearray()
        final_message.append(int(self.opcode))  # opcode u8
        final_message.extend(len(body).to_bytes(4, "little", signed=True))  # length i32
        final_message.extend(body)  # complete body
        return bytes(final_message)

    def _log_serialization_details(self, body: bytearray, result_bytes: bytes):
        """Log serialization details for debugging."""
        logging.debug(
            "action: data_batch_to_bytes | batch_number: %d | "
            "table_ids: %s | query_ids: %s | total_shards: %d | shard_num: %d | "
            "body_size: %d bytes | final_size: %d bytes",
            getattr(self, 'batch_number', 0), self.table_ids, self.query_ids,
            self.total_shards, self.shard_num, len(body), len(result_bytes)
        )

    def to_bytes(self) -> bytes:
        """
        Generate complete DataBatch bytes without writing to socket.
        Requires `self.batch_bytes` with already framed embedded message.
        Returns complete serialized message as bytes.
        """
        # Validate we have necessary data
        self._validate_batch_bytes()

        # Serialize message body
        body = self._serialize_data_batch_body()

        # Create final message with framing
        result_bytes = self._create_final_message(body)
        
        # Log what we're generating
        self._log_serialization_details(body, result_bytes)
        
        return result_bytes