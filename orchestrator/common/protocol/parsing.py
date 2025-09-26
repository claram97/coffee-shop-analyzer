"""
Low-level protocol parsing utilities for reading and writing binary data.
"""

import socket
from typing import List, Dict, Tuple

from .constants import ProtocolError


def recv_exactly(sock: socket.socket, n: int) -> bytes:
    """Read exactly n bytes (retrying as needed) or raise EOFError on peer close.

    Converts timeouts/OS errors to ProtocolError. Prevents short reads.
    """
    if n < 0:
        raise ProtocolError("invalid body")
    data = bytearray(n)
    view = memoryview(data)
    read = 0
    while read < n:
        try:
            nrecv = sock.recv_into(view[read:], n - read)
        except socket.timeout as e:
            raise ProtocolError("recv timeout") from e
        except InterruptedError:
            continue
        except OSError as e:
            raise ProtocolError(f"recv failed: {e}") from e
        if nrecv == 0:
            raise EOFError("peer closed connection")
        read += nrecv
    return bytes(data)


def read_u8(sock: socket.socket) -> int:
    """Read one unsigned byte (u8)."""
    return recv_exactly(sock, 1)[0]


def read_u16(sock: socket.socket, remaining: int, opcode: int) -> Tuple[int, int]:
    """Lee un u16 little-endian y descuenta remaining."""
    if remaining < 2:
        raise ProtocolError("indicated length doesn't match body length", opcode)
    remaining -= 2
    val = int.from_bytes(recv_exactly(sock, 2), byteorder="little", signed=False)
    return val, remaining


def read_i32(sock: socket.socket, remaining: int, opcode: int) -> Tuple[int, int]:
    """Read a little-endian signed int32 and decrement `remaining` accordingly.

    Raises ProtocolError if fewer than 4 bytes remain to be read.
    """
    if remaining < 4:
        raise ProtocolError("indicated length doesn't match body length", opcode)
    remaining -= 4
    val = int.from_bytes(recv_exactly(sock, 4), byteorder="little", signed=True)
    return val, remaining


def read_i64(sock: socket.socket, remaining: int, opcode: int) -> Tuple[int, int]:
    """Lee un i64 little-endian y descuenta remaining."""
    if remaining < 8:
        raise ProtocolError("indicated length doesn't match body length", opcode)
    remaining -= 8
    val = int.from_bytes(recv_exactly(sock, 8), byteorder="little", signed=True)
    return val, remaining


def read_string(sock: socket.socket, remaining: int, opcode: int) -> Tuple[str, int]:
    """Read a protocol [string]: i32 length (validated) + UTF-8 bytes.

    Ensures a strictly positive length and sufficient remaining payload.
    Returns the decoded string and the updated `remaining`.
    """
    (key_len, remaining) = read_i32(sock, remaining, opcode)
    if key_len < 0:
        raise ProtocolError("invalid body", opcode)
    if key_len == 0:
        return ("", remaining)
    if remaining < key_len:
        raise ProtocolError("indicated length doesn't match body length", opcode)
    try:
        s = recv_exactly(sock, key_len).decode("utf-8")
    except UnicodeDecodeError as e:
        raise ProtocolError("invalid body", opcode) from e
    remaining -= key_len
    return (s, remaining)


def write_u8(sock, value: int) -> None:
    """Write a single unsigned byte (u8) using sendall()."""
    if not 0 <= value <= 255:
        raise ValueError("u8 out of range")
    sock.sendall(bytes([value]))


def write_u16(sock: socket.socket, value: int) -> None:
    if not 0 <= int(value) <= 0xFFFF:
        raise ValueError("u16 out of range")
    sock.sendall(int(value).to_bytes(2, byteorder="little", signed=False))


def write_i32(sock: socket.socket, value: int) -> None:
    """Write a little-endian signed int32 using sendall()."""
    sock.sendall(int(value).to_bytes(4, byteorder="little", signed=True))


def write_i64(sock: socket.socket, value: int) -> None:
    sock.sendall(int(value).to_bytes(8, byteorder="little", signed=True))


def write_string(sock: socket.socket, s: str) -> None:
    """Write a protocol [string]: i32 length prefix + UTF-8 bytes."""
    b = s.encode("utf-8")
    n = len(b)
    write_i32(sock, n)
    sock.sendall(b)


def read_u8_with_remaining(
    sock: socket.socket, remaining: int, opcode: int
) -> Tuple[int, int]:
    """Read a u8 and update remaining bytes counter."""
    if remaining < 1:
        raise ProtocolError("indicated length doesn't match body length", opcode)
    b = recv_exactly(sock, 1)[0]
    return b, remaining - 1


def read_u8_list(
    sock: socket.socket, remaining: int, opcode: int
) -> Tuple[List[int], int]:
    """Read a list of u8 values in format: [u8 count] + count * [u8]."""
    n, remaining = read_u8_with_remaining(sock, remaining, opcode)
    items = []
    for _ in range(n):
        v, remaining = read_u8_with_remaining(sock, remaining, opcode)
        items.append(v)
    return items, remaining


def read_u8_u8_dict(
    sock: socket.socket, remaining: int, opcode: int
) -> Tuple[Dict[int, int], int]:
    """Read a dict of u8->u8 mappings in format: [u8 count] + count * ([u8 key][u8 val])."""
    n, remaining = read_u8_with_remaining(sock, remaining, opcode)
    out: Dict[int, int] = {}
    for _ in range(n):
        k, remaining = read_u8_with_remaining(sock, remaining, opcode)
        v, remaining = read_u8_with_remaining(sock, remaining, opcode)
        out[k] = v
    return out, remaining