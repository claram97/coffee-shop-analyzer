"""
Defines the `DataBatch` message class, which acts as a container for transporting
table-specific data messages along with essential metadata for routing and processing.
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
    """
    A factory function that creates an empty message instance based on an opcode.
    This is a crucial step during deserialization to get the correct object
    to parse the inner message body.

    Args:
        opcode: The opcode of the inner message to instantiate.

    Returns:
        An instance of the corresponding message class (e.g., NewMenuItems).

    Raises:
        ProtocolError: If the opcode does not correspond to a known message type.
    """
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
    Represents a DataBatch message, which wraps a specific data message
    (like NewMenuItems) with metadata for routing, sharding, and batch control.

    Binary Format:
    [u8 opcode]         - Always DATA_BATCH (0)
    [i32 length]        - Length of the following body in bytes
    [-- BODY STARTS --]
      [u8 list table_ids] - List of table IDs this data may be relevant for.
      [u8 list query_ids] - List of query IDs this data is relevant for.
      [u16 reserved]      - Reserved for future flags.
      [i64 batch_number]  - The batch number from the original source message.
      [dict<u8,u8> meta]  - A dictionary for arbitrary metadata.
      [u16 total_shards]  - The total number of shards for this data.
      [u16 shard_num]     - The number of this specific shard.
      [embedded message]  - The inner message, framed as [u8 opcode][i32 len][body].
    [-- BODY ENDS --]
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
        """Internal helper to initialize all instance fields with defaults or provided values."""
        self.opcode = Opcodes.DATA_BATCH

        # When creating an object for deserialization, fields are initially None and
        # are populated by read_from().
        self.table_ids: List[int] = [] if table_ids is None else list(table_ids)
        self.query_ids: List[int] = [] if query_ids is None else list(query_ids)
        self.reserved_u16: int = int(reserved_u16)
        self.meta: Dict[int, int] = {} if meta is None else dict(meta)
        self.total_shards: int = 0 if total_shards is None else int(total_shards)
        self.shard_num: int = 0 if shard_num is None else int(shard_num)

        # The embedded content can be a parsed message object (after deserialization)
        # or raw bytes (before serialization).
        self.batch_msg = None
        self.batch_bytes: Optional[bytes] = batch_bytes

    def _validate_u8_list(self, items: List[int], field_name: str):
        """Internal helper to validate that a list contains values suitable for a u8 list."""
        if len(items) > 255:
            raise ValueError(f"{field_name} must have at most 255 items")
        if any(not 0 <= x <= 255 for x in items):
            raise ValueError(f"{field_name} must contain only u8 values (0-255)")

    def _validate_u8_dict(self, meta_dict: Dict[int, int]):
        """Internal helper to validate a dictionary has u8 keys and values."""
        if len(meta_dict) > 255:
            raise ValueError("meta size must be <= 255")
        for k, v in meta_dict.items():
            if not (0 <= int(k) <= 255 and 0 <= int(v) <= 255):
                raise ValueError("meta keys/values must be u8")

    def _validate_u16_field(self, value: int, field_name: str):
        """Internal helper to validate a value is within the u16 range."""
        if not (0 <= value <= 0xFFFF):
            raise ValueError(f"{field_name} must be u16 (0-65535)")

    def _validate_serialization_parameters(
        self,
        table_ids: Optional[List[int]],
        query_ids: Optional[List[int]],
        meta: Optional[Dict[int, int]]
    ):
        """Internal helper to run validations before serialization."""
        if table_ids is not None:
            self._validate_u8_list(self.table_ids, "table_ids")
        if query_ids is not None:
            self._validate_u8_list(self.query_ids, "query_ids")
        if meta is not None:
            self._validate_u8_dict(self.meta)

        self._validate_u16_field(self.reserved_u16, "reserved_u16")
        self._validate_u16_field(self.total_shards, "total_shards")
        self._validate_u16_field(self.shard_num, "shard_num")

    def __init__(
        self,
        *,
        table_ids: Optional[List[int]] = None,
        query_ids: Optional[List[int]] = None,
        meta: Optional[Dict[int, int]] = None,
        total_shards: Optional[int] = None,
        shard_num: Optional[int] = None,
        reserved_u16: int = 0,
        batch_bytes: Optional[bytes] = None,
    ):
        """
        Initializes a DataBatch message. Can be used to prepare a message for
        serialization or as an empty container for deserialization.
        """
        self._initialize_fields(
            table_ids, query_ids, meta, total_shards,
            shard_num, reserved_u16, batch_bytes
        )
        self._validate_serialization_parameters(table_ids, query_ids, meta)

    @staticmethod
    def make_embedded(inner_opcode: int, inner_body: bytes) -> bytes:
        """
        A utility to frame an inner message body with its opcode and length.
        Format: [u8 opcode][i32 length][body]

        Args:
            inner_opcode: The opcode of the message being embedded.
            inner_body: The raw, unframed body of the inner message.

        Returns:
            The fully framed inner message as a bytes object.
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
        """Reads the DataBatch-specific header fields from the socket."""
        self.table_ids, remaining = read_u8_list(sock, remaining, self.opcode)
        self.query_ids, remaining = read_u8_list(sock, remaining, self.opcode)
        self.reserved_u16, remaining = read_u16(sock, remaining, self.opcode)
        self.batch_number, remaining = read_i64(sock, remaining, self.opcode)
        self.meta, remaining = read_u8_u8_dict(sock, remaining, self.opcode)
        self.total_shards, remaining = read_u16(sock, remaining, self.opcode)
        self.shard_num, remaining = read_u16(sock, remaining, self.opcode)
        return remaining

    def _read_embedded_message(self, sock: socket.socket, remaining: int) -> int:
        """Reads and deserializes the embedded message from the socket."""
        inner_opcode, remaining = read_u8_with_remaining(sock, remaining, self.opcode)
        inner_len, remaining = read_i32(sock, remaining, self.opcode)

        inner_msg = instantiate_message_for_opcode(inner_opcode)
        inner_msg.read_from(sock, inner_len)
        self.batch_msg = inner_msg

        remaining -= inner_len
        return remaining

    def _validate_remaining_bytes(self, remaining: int):
        """Checks that all bytes of the message body have been consumed."""
        if remaining != 0:
            raise ProtocolError("Indicated length doesn't match body length", self.opcode)

    def _handle_protocol_error(self, sock: socket.socket, remaining: int):
        """In case of a protocol error, consumes any leftover bytes to clear the socket buffer."""
        if remaining > 0:
            _ = recv_exactly(sock, remaining)

    def read_from(self, sock: socket.socket, length: int):
        """
        Deserializes a DataBatch message from a socket, populating the instance's fields.

        Args:
            sock: The socket to read from.
            length: The total length of the message body to be read.
        """
        remaining = length
        try:
            remaining = self._read_data_batch_header(sock, remaining)
            remaining = self._read_embedded_message(sock, remaining)
            self._validate_remaining_bytes(remaining)
        except ProtocolError:
            self._handle_protocol_error(sock, remaining)
            raise

    def write_to(self, sock: socket.socket):
        """
        Serializes the DataBatch instance into bytes and writes it to the socket.
        Requires `self.batch_bytes` to be set with the pre-framed embedded message.
        """
        message_bytes = self.to_bytes()
        sock.sendall(message_bytes)

    def _validate_batch_bytes(self):
        """Internal helper to validate the embedded message bytes before serialization."""
        if self.batch_bytes is None:
            raise ProtocolError("missing embedded batch bytes for DataBatch serialization", self.opcode)
        if len(self.batch_bytes) < 5:
            raise ProtocolError("invalid embedded framing (too short)", self.opcode)

        inner_len = int.from_bytes(self.batch_bytes[1:5], "little", signed=True)
        if inner_len < 0 or inner_len != len(self.batch_bytes) - 5:
            raise ProtocolError("invalid embedded framing (length mismatch)", self.opcode)

    def _serialize_u8_list(self, items: List[int]) -> bytearray:
        """Serializes a list of integers as a u8-prefixed list."""
        result = bytearray([len(items)])
        result.extend(bytes(items))
        return result

    def _serialize_u8_dict(self, meta_dict: Dict[int, int]) -> bytearray:
        """Serializes a dictionary as a u8-prefixed list of key-value pairs."""
        result = bytearray([len(meta_dict)])
        for k, v in meta_dict.items():
            result.extend([int(k), int(v)])
        return result

    def _serialize_data_batch_body(self) -> bytearray:
        """Assembles the complete body of the DataBatch message for serialization."""
        body = bytearray()
        body.extend(self._serialize_u8_list(self.table_ids))
        body.extend(self._serialize_u8_list(self.query_ids))
        body.extend(int(self.reserved_u16).to_bytes(2, "little", signed=False))
        body.extend(int(self.batch_number).to_bytes(8, "little", signed=True))
        body.extend(self._serialize_u8_dict(self.meta))
        body.extend(int(self.total_shards).to_bytes(2, "little", signed=False))
        body.extend(int(self.shard_num).to_bytes(2, "little", signed=False))
        body.extend(self.batch_bytes)
        return body

    def _create_final_message(self, body: bytearray) -> bytes:
        """Wraps the serialized body with the final message frame: [opcode][length][body]."""
        final_message = bytearray([int(self.opcode)])
        final_message.extend(len(body).to_bytes(4, "little", signed=True))
        final_message.extend(body)
        return bytes(final_message)

    def _log_serialization_details(self, body: bytearray, result_bytes: bytes):
        """Logs details of the serialization process for debugging."""
        logging.debug(
            "action: data_batch_to_bytes | batch_number: %d | "
            "table_ids: %s | query_ids: %s | total_shards: %d | shard_num: %d | "
            "body_size: %d bytes | final_size: %d bytes",
            getattr(self, 'batch_number', 0), self.table_ids, self.query_ids,
            self.total_shards, self.shard_num, len(body), len(result_bytes)
        )

    def to_bytes(self) -> bytes:
        """
        Serializes the entire DataBatch message into a single bytes object.
        Requires `self.batch_bytes` to be set with the pre-framed embedded message.

        Returns:
            The complete, framed message as a bytes object.
        """
        self._validate_batch_bytes()
        body = self._serialize_data_batch_body()
        result_bytes = self._create_final_message(body)
        self._log_serialization_details(body, result_bytes)
        return result_bytes