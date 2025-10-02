"""
Provides socket-specific utility functions for parsing and writing the primitive 
data types of the binary protocol. These functions work directly with sockets 
for network communication.
"""

import socket
from typing import List, Dict, Tuple

from .constants import ProtocolError


def recv_exact(sock: socket.socket, n: int) -> bytes:
    """
    Reliably receives exactly n bytes from a socket.
    
    This function handles partial receives by continuing to read until
    exactly n bytes have been received or an error occurs.
    
    Args:
        sock: The socket to read from.
        n: The exact number of bytes to receive.
    
    Returns:
        A bytes object containing exactly n bytes.
    
    Raises:
        EOFError: If the socket is closed before n bytes are received.
        ValueError: If n is negative.
    """
    if n < 0:
        raise ValueError("invalid number of bytes to receive")
    if n == 0:
        return b""
    
    data = b""
    while len(data) < n:
        chunk = sock.recv(n - len(data))
        if not chunk:
            raise EOFError(f"socket closed while reading, expected {n} bytes, got {len(data)}")
        data += chunk
    return data


def read_u8(sock: socket.socket) -> int:
    """Reads one unsigned byte (u8) from the socket."""
    return recv_exact(sock, 1)[0]


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
    data = recv_exact(sock, 2)
    val = int.from_bytes(data, byteorder="little", signed=False)
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
    data = recv_exact(sock, 4)
    val = int.from_bytes(data, byteorder="little", signed=True)
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
    data = recv_exact(sock, 8)
    val = int.from_bytes(data, byteorder="little", signed=True)
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
    
    data = recv_exact(sock, str_len)
    try:
        s = data.decode("utf-8")
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
    data = int(value).to_bytes(2, byteorder="little", signed=False)
    sock.sendall(data)


def write_i32(sock: socket.socket, value: int) -> None:
    """Writes a 4-byte, little-endian signed integer (i32) to the socket."""
    data = int(value).to_bytes(4, byteorder="little", signed=True)
    sock.sendall(data)


def write_i64(sock: socket.socket, value: int) -> None:
    """Writes an 8-byte, little-endian signed integer (i64) to the socket."""
    data = int(value).to_bytes(8, byteorder="little", signed=True)
    sock.sendall(data)


def write_string(sock: socket.socket, s: str) -> None:
    """Writes a length-prefixed string to the socket."""
    b = s.encode("utf-8")
    write_i32(sock, len(b))
    sock.sendall(b)


def read_u8_with_remaining(sock: socket.socket, remaining: int, opcode: int) -> Tuple[int, int]:
    """Reads a u8 and decrements the remaining bytes counter."""
    if remaining < 1:
        raise ProtocolError("indicated length doesn't match body length", opcode)
    b = recv_exact(sock, 1)[0]
    return b, remaining - 1


def read_u8_list(sock: socket.socket, remaining: int, opcode: int) -> Tuple[List[int], int]:
    """
    Reads a list of u8 values prefixed by a u8 count from the socket.
    Format: [u8 count][u8 item 1][u8 item 2]...
    """
    n, remaining = read_u8_with_remaining(sock, remaining, opcode)
    if remaining < n:
        raise ProtocolError("indicated length doesn't match body length for u8 list", opcode)

    items_bytes = recv_exact(sock, n)
    remaining -= n
    return list(items_bytes), remaining


def read_u8_u8_dict(sock: socket.socket, remaining: int, opcode: int) -> Tuple[Dict[int, int], int]:
    """
    Reads a dictionary of u8 -> u8 mappings prefixed by a u8 count from the socket.
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
