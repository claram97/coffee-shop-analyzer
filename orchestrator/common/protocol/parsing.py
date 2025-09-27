"""
Provides a suite of low-level, foundational utility functions for parsing and
writing the primitive data types of the binary protocol. This includes functions
for reliable socket I/O and handling specific byte-level data structures.
"""

import socket
from typing import List, Dict, Tuple

from .constants import ProtocolError


def recv_exactly(sock: socket.socket, n: int) -> bytes:
    """
    Reads exactly `n` bytes from a socket, handling short reads by retrying.

    This function is a robust replacement for a simple `sock.recv(n)` call,
    ensuring that the entire requested amount of data is received before
    returning. It is essential for parsing messages with fixed-size fields.

    Args:
        sock: The socket to read from.
        n: The exact number of bytes to receive.

    Returns:
        A bytes object containing exactly `n` bytes of data.

    Raises:
        EOFError: If the peer closes the connection before `n` bytes are received.
        ProtocolError: On socket timeouts, OS errors, or if `n` is negative.
    """
    if n < 0:
        raise ProtocolError("invalid number of bytes to receive")
    data = bytearray(n)
    view = memoryview(data)
    read = 0
    while read < n:
        try:
            nrecv = sock.recv_into(view[read:], n - read)
        except socket.timeout as e:
            raise ProtocolError("receive operation timed out") from e
        except InterruptedError:
            continue
        except OSError as e:
            raise ProtocolError(f"receive operation failed: {e}") from e

        if nrecv == 0:
            raise EOFError("peer closed connection")
        read += nrecv
    return bytes(data)


def read_u8(sock: socket.socket) -> int:
    """Reads one unsigned byte (u8) from the socket."""
    return recv_exactly(sock, 1)[0]


def read_u16(sock: socket.socket, remaining: int, opcode: int) -> Tuple[int, int]:
    """
    Reads a 2-byte, little-endian unsigned integer (u16) from the socket.

    Args:
        sock: The socket to read from.
        remaining: The number of bytes remaining in the message body.
        opcode: The opcode of the current message for error reporting.

    Returns:
        A tuple containing the parsed integer and the updated remaining byte count.
    """
    if remaining < 2:
        raise ProtocolError("indicated length doesn't match body length", opcode)
    remaining -= 2
    val = int.from_bytes(recv_exactly(sock, 2), byteorder="little", signed=False)
    return val, remaining


def read_i32(sock: socket.socket, remaining: int, opcode: int) -> Tuple[int, int]:
    """
    Reads a 4-byte, little-endian signed integer (i32) from the socket.

    Args:
        sock: The socket to read from.
        remaining: The number of bytes remaining in the message body.
        opcode: The opcode of the current message for error reporting.

    Returns:
        A tuple containing the parsed integer and the updated remaining byte count.
    """
    if remaining < 4:
        raise ProtocolError("indicated length doesn't match body length", opcode)
    remaining -= 4
    val = int.from_bytes(recv_exactly(sock, 4), byteorder="little", signed=True)
    return val, remaining


def read_i64(sock: socket.socket, remaining: int, opcode: int) -> Tuple[int, int]:
    """
    Reads an 8-byte, little-endian signed integer (i64) from the socket.

    Args:
        sock: The socket to read from.
        remaining: The number of bytes remaining in the message body.
        opcode: The opcode of the current message for error reporting.

    Returns:
        A tuple containing the parsed integer and the updated remaining byte count.
    """
    if remaining < 8:
        raise ProtocolError("indicated length doesn't match body length", opcode)
    remaining -= 8
    val = int.from_bytes(recv_exactly(sock, 8), byteorder="little", signed=True)
    return val, remaining


def read_string(sock: socket.socket, remaining: int, opcode: int) -> Tuple[str, int]:
    """
    Reads a length-prefixed string from the socket.

    The format is a 4-byte signed integer (length) followed by UTF-8 bytes.

    Args:
        sock: The socket to read from.
        remaining: The number of bytes remaining in the message body.
        opcode: The opcode of the current message for error reporting.

    Returns:
        A tuple containing the decoded string and the updated remaining byte count.
    """
    (str_len, remaining) = read_i32(sock, remaining, opcode)
    if str_len < 0:
        raise ProtocolError("invalid string length", opcode)
    if str_len == 0:
        return "", remaining
    if remaining < str_len:
        raise ProtocolError("indicated length doesn't match body length", opcode)
    try:
        s = recv_exactly(sock, str_len).decode("utf-8")
    except UnicodeDecodeError as e:
        raise ProtocolError("invalid UTF-8 string data", opcode) from e
    remaining -= str_len
    return s, remaining


def write_u8(sock: socket.socket, value: int) -> None:
    """Writes a single unsigned byte (u8) to the socket."""
    if not 0 <= value <= 255:
        raise ValueError("u8 value out of range (0-255)")
    sock.sendall(bytes([value]))


def write_u16(sock: socket.socket, value: int) -> None:
    """Writes a 2-byte, little-endian unsigned integer (u16) to the socket."""
    if not 0 <= int(value) <= 0xFFFF:
        raise ValueError("u16 value out of range (0-65535)")
    sock.sendall(int(value).to_bytes(2, byteorder="little", signed=False))


def write_i32(sock: socket.socket, value: int) -> None:
    """Writes a 4-byte, little-endian signed integer (i32) to the socket."""
    sock.sendall(int(value).to_bytes(4, byteorder="little", signed=True))


def write_i64(sock: socket.socket, value: int) -> None:
    """Writes an 8-byte, little-endian signed integer (i64) to the socket."""
    sock.sendall(int(value).to_bytes(8, byteorder="little", signed=True))


def write_string(sock: socket.socket, s: str) -> None:
    """Writes a length-prefixed string to the socket."""
    b = s.encode("utf-8")
    write_i32(sock, len(b))
    sock.sendall(b)


def read_u8_with_remaining(sock: socket.socket, remaining: int, opcode: int) -> Tuple[int, int]:
    """Reads a u8 and decrements the remaining bytes counter."""
    if remaining < 1:
        raise ProtocolError("indicated length doesn't match body length", opcode)
    b = recv_exactly(sock, 1)[0]
    return b, remaining - 1


def read_u8_list(sock: socket.socket, remaining: int, opcode: int) -> Tuple[List[int], int]:
    """
    Reads a list of u8 values prefixed by a u8 count.
    Format: [u8 count][u8 item 1][u8 item 2]...
    """
    n, remaining = read_u8_with_remaining(sock, remaining, opcode)
    if remaining < n:
        raise ProtocolError("indicated length doesn't match body length for u8 list", opcode)

    items_bytes = recv_exactly(sock, n)
    remaining -= n
    return list(items_bytes), remaining


def read_u8_u8_dict(sock: socket.socket, remaining: int, opcode: int) -> Tuple[Dict[int, int], int]:
    """
    Reads a dictionary of u8 -> u8 mappings prefixed by a u8 count.
    Format: [u8 count][u8 key 1][u8 value 1][u8 key 2][u8 value 2]...
    """
    n, remaining = read_u8_with_remaining(sock, remaining, opcode)
    if remaining < n * 2:
        raise ProtocolError("indicated length doesn't match body length for u8->u8 dict", opcode)

    out: Dict[int, int] = {}
    for _ in range(n):
        k, remaining = read_u8_with_remaining(sock, remaining, opcode)
        v, remaining = read_u8_with_remaining(sock, remaining, opcode)
        out[k] = v
    return out, remaining