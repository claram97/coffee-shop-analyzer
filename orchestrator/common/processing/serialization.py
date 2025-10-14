"""
Provides a set of functions for serializing filtered data structures into a
specific binary format. This is used to prepare data for network transmission
according to a custom protocol.
"""

import logging
import struct
from functools import lru_cache
from typing import List, Sequence, Tuple, Any


def serialize_header(n_rows: int, batch_number: int, batch_status: int) -> bytearray:
    """Serializes a common header for filtered data messages."""
    body = bytearray()
    body.extend(n_rows.to_bytes(4, "little", signed=True))
    body.extend(batch_number.to_bytes(8, "little", signed=True))
    body.extend(batch_status.to_bytes(1, "little", signed=False))
    return body


@lru_cache(maxsize=32)
def _get_key_metadata(required_keys: Tuple[str, ...]) -> Tuple[Tuple[str, bytes], ...]:
    """Encodes and prefixes a tuple of keys. The result is cached for performance."""
    entries: list[tuple[str, bytes]] = []
    for key in required_keys:
        key_bytes = key.encode("utf-8")
        prefix = struct.pack("<I", len(key_bytes)) + key_bytes
        entries.append((key, prefix))
    return tuple(entries)


def _serialize_row(row: Any, metadata: Sequence[Tuple[str, bytes]]) -> bytes:
    """
    Serializes a single raw object into bytes by accessing its attributes.
    This is faster than working with intermediate dictionaries.
    """
    parts = [struct.pack("<I", len(metadata))]

    for key, key_prefix in metadata:
        # Use getattr() to access attributes from the raw object
        value = getattr(row, key, "")
        value_bytes = str(value).encode("utf-8")

        parts.append(key_prefix)
        parts.append(struct.pack("<I", len(value_bytes)))
        parts.append(value_bytes)

    # The b"".join() call is highly optimized in C for this exact task
    return b"".join(parts)


def serialize_data(
    rows: List[Any], 
    batch_number: int,
    batch_status: int,
    required_keys: Tuple[str, ...],
    table_name: str,
) -> bytes:
    """
    Serializes a list of raw data objects into a complete binary message payload.
    """
    n_rows = len(rows)
    body = serialize_header(n_rows, batch_number, batch_status)

    if not n_rows:
        return bytes(body)

    metadata = _get_key_metadata(required_keys)

    all_row_bytes = [_serialize_row(row, metadata) for row in rows]
    body.extend(b"".join(all_row_bytes))

    logging.debug(
        f"action: serialize_{table_name} | rows: {n_rows} | body_size: {len(body)} bytes"
    )
    return bytes(body)