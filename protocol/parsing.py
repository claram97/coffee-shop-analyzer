"""
Provides a suite of low-level, foundational utility functions for parsing and
writing the primitive data types of the binary protocol. This includes functions
for reliable I/O from a bytes buffer and handling specific byte-level data
structures.
"""

from typing import List, Dict, Tuple

from .constants import ProtocolError


class BytesReader:
    """
    A wrapper around a bytes object to allow for sequential reading of data,
    mimicking a stream. This is used to parse messages from a fixed-size
    byte buffer.
    """

    def __init__(self, data: bytes):
        self._data = data
        self._offset = 0

    def read(self, n: int) -> bytes:
        """
        Reads exactly `n` bytes from the current offset in the buffer.

        Args:
            n: The number of bytes to read.

        Returns:
            A bytes object containing the read data.

        Raises:
            EOFError: If there are not enough bytes left in the buffer to read.
            ValueError: If `n` is negative.
        """
        if n < 0:
            raise ValueError("invalid number of bytes to read")
        if self._offset + n > len(self._data):
            raise EOFError("not enough bytes to read")

        chunk = self._data[self._offset : self._offset + n]
        self._offset += n
        return chunk

    def is_eof(self) -> bool:
        """Returns True if the end of the buffer has been reached."""
        return self._offset >= len(self._data)


def read_u8(reader: BytesReader) -> int:
    """Reads one unsigned byte (u8) from the buffer."""
    return reader.read(1)[0]


def read_u16(reader: BytesReader, remaining: int, opcode: int) -> Tuple[int, int]:
    """
    Reads a 2-byte, little-endian unsigned integer (u16) from the buffer.

    Args:
        reader: The BytesReader to read from.
        remaining: The number of bytes remaining in the message body.
        opcode: The opcode of the current message for error reporting.

    Returns:
        A tuple containing the parsed integer and the updated remaining byte count.
    """
    if remaining < 2:
        raise ProtocolError("indicated length doesn't match body length", opcode)
    remaining -= 2
    val = int.from_bytes(reader.read(2), byteorder="little", signed=False)
    return val, remaining


def read_i32(reader: BytesReader, remaining: int, opcode: int) -> Tuple[int, int]:
    """
    Reads a 4-byte, little-endian signed integer (i32) from the buffer.

    Args:
        reader: The BytesReader to read from.
        remaining: The number of bytes remaining in the message body.
        opcode: The opcode of the current message for error reporting.

    Returns:
        A tuple containing the parsed integer and the updated remaining byte count.
    """
    if remaining < 4:
        raise ProtocolError("indicated length doesn't match body length", opcode)
    remaining -= 4
    val = int.from_bytes(reader.read(4), byteorder="little", signed=True)
    return val, remaining


def read_i64(reader: BytesReader, remaining: int, opcode: int) -> Tuple[int, int]:
    """
    Reads an 8-byte, little-endian signed integer (i64) from the buffer.

    Args:
        reader: The BytesReader to read from.
        remaining: The number of bytes remaining in the message body.
        opcode: The opcode of the current message for error reporting.

    Returns:
        A tuple containing the parsed integer and the updated remaining byte count.
    """
    if remaining < 8:
        raise ProtocolError("indicated length doesn't match body length", opcode)
    remaining -= 8
    val = int.from_bytes(reader.read(8), byteorder="little", signed=True)
    return val, remaining


def read_string(reader: BytesReader, remaining: int, opcode: int) -> Tuple[str, int]:
    """
    Reads a length-prefixed string from the buffer.

    The format is a 4-byte signed integer (length) followed by UTF-8 bytes.

    Args:
        reader: The BytesReader to read from.
        remaining: The number of bytes remaining in the message body.
        opcode: The opcode of the current message for error reporting.

    Returns:
        A tuple containing the decoded string and the updated remaining byte count.
    """
    (str_len, remaining) = read_i32(reader, remaining, opcode)
    if str_len < 0:
        raise ProtocolError("invalid string length", opcode)
    if str_len == 0:
        return "", remaining
    if remaining < str_len:
        raise ProtocolError("indicated length doesn't match body length", opcode)
    try:
        s = reader.read(str_len).decode("utf-8")
    except UnicodeDecodeError as e:
        raise ProtocolError("invalid UTF-8 string data", opcode) from e
    remaining -= str_len
    return s, remaining


def write_u8(buf: bytearray, value: int) -> None:
    """Writes a single unsigned byte (u8) to the buffer."""
    if not 0 <= value <= 255:
        raise ValueError("u8 value out of range (0-255)")
    buf.append(value)


def write_u16(buf: bytearray, value: int) -> None:
    """Writes a 2-byte, little-endian unsigned integer (u16) to the buffer."""
    if not 0 <= int(value) <= 0xFFFF:
        raise ValueError("u16 value out of range (0-65535)")
    buf.extend(int(value).to_bytes(2, byteorder="little", signed=False))


def write_i32(buf: bytearray, value: int) -> None:
    """Writes a 4-byte, little-endian signed integer (i32) to the buffer."""
    buf.extend(int(value).to_bytes(4, byteorder="little", signed=True))


def write_i64(buf: bytearray, value: int) -> None:
    """Writes an 8-byte, little-endian signed integer (i64) to the buffer."""
    buf.extend(int(value).to_bytes(8, byteorder="little", signed=True))


def write_string(buf: bytearray, s: str) -> None:
    """Writes a length-prefixed string to the buffer."""
    b = s.encode("utf-8")
    write_i32(buf, len(b))
    buf.extend(b)


def read_u8_with_remaining(reader: BytesReader, remaining: int, opcode: int) -> Tuple[int, int]:
    """Reads a u8 and decrements the remaining bytes counter."""
    if remaining < 1:
        raise ProtocolError("indicated length doesn't match body length", opcode)
    b = reader.read(1)[0]
    return b, remaining - 1


def read_u8_list(reader: BytesReader, remaining: int, opcode: int) -> Tuple[List[int], int]:
    """
    Reads a list of u8 values prefixed by a u8 count.
    Format: [u8 count][u8 item 1][u8 item 2]...
    """
    n, remaining = read_u8_with_remaining(reader, remaining, opcode)
    if remaining < n:
        raise ProtocolError("indicated length doesn't match body length for u8 list", opcode)

    items_bytes = reader.read(n)
    remaining -= n
    return list(items_bytes), remaining


def read_u8_u8_dict(reader: BytesReader, remaining: int, opcode: int) -> Tuple[Dict[int, int], int]:
    """
    Reads a dictionary of u8 -> u8 mappings prefixed by a u8 count.
    Format: [u8 count][u8 key 1][u8 value 1][u8 key 2][u8 value 2]...
    """
    n, remaining = read_u8_with_remaining(reader, remaining, opcode)
    if remaining < n * 2:
        raise ProtocolError("indicated length doesn't match body length for u8->u8 dict", opcode)

    out: Dict[int, int] = {}
    for _ in range(n):
        k, remaining = read_u8_with_remaining(reader, remaining, opcode)
        v, remaining = read_u8_with_remaining(reader, remaining, opcode)
        out[k] = v
    return out, remaining