"""
Provides the primary dispatcher function for receiving and routing all incoming
protocol messages based on their opcode.
"""
import socket
import struct
from .constants import ProtocolError, Opcodes
from .socket_parsing import read_u8, read_i32, recv_exact
from .messages import (
    Finished,
    NewMenuItems,
    NewStores,
    NewTransactionItems,
    NewTransactions,
    NewUsers,
    EOFMessage,
    ClientHello,
)
from .databatch import DataBatch

_OPCODE_TO_CLASS = {
    Opcodes.FINISHED: Finished,
    Opcodes.NEW_MENU_ITEMS: NewMenuItems,
    Opcodes.NEW_STORES: NewStores,
    Opcodes.NEW_TRANSACTION_ITEMS: NewTransactionItems,
    Opcodes.NEW_TRANSACTION: NewTransactions,
    Opcodes.NEW_USERS: NewUsers,
    Opcodes.EOF: EOFMessage,
    Opcodes.DATA_BATCH: DataBatch,
    Opcodes.CLIENT_HELLO: ClientHello,
}

def recv_raw_frame(sock: socket.socket) -> bytes:
    """
    Reads a complete message frame from a socket and returns its raw bytes.
    This is the fastest way to read a message without parsing its content.
    """
    opcode = read_u8(sock)
    (length, _) = read_i32(sock, 4, -1)
    if length < 0:
        raise ProtocolError("invalid message length")

    body_bytes = recv_exact(sock, length)
    
    header_bytes = struct.pack('<BI', opcode, length)
    return header_bytes + body_bytes



def deserialize_message_from_bytes(raw_bytes: bytes):
    """
    Deserializes a complete message from a raw bytes object.
    This is the counterpart to `recv_msg` for use in worker processes
    that read from a queue instead of a live socket.
    """
    if len(raw_bytes) < 5:
        raise ProtocolError("Message too short for valid protocol frame")

    opcode, length = struct.unpack('<BI', raw_bytes[:5])
    
    if length < 0:
        raise ProtocolError("invalid message length")
    if len(raw_bytes) != 5 + length:
        raise ProtocolError(f"Expected {5 + length} bytes, got {len(raw_bytes)} bytes")

    msg_class = _OPCODE_TO_CLASS.get(opcode)
    if not msg_class:
        raise ProtocolError(f"invalid opcode: {opcode}")
    
    msg = msg_class()

    body_bytes = raw_bytes[5:]
    if opcode in (Opcodes.FINISHED, Opcodes.CLIENT_HELLO):
        msg.read_from(body_bytes, length)
    else:
        msg.read_from(body_bytes)
    
    return msg




def recv_msg(sock: socket.socket):
    """
    Reads a complete message from a socket, handling the initial framing and
    dispatching the parsing to the appropriate message class.

    This function orchestrates the deserialization process by:
    1. Reading the 1-byte opcode to identify the message type.
    2. Reading the 4-byte length of the message body.
    3. Instantiating the correct message class based on the opcode.
    4. Delegating the parsing of the message body to that instance's
       `read_from()` method.

    Args:
        sock: The socket object to read the message from.

    Returns:
        A fully parsed message object corresponding to the received opcode
        (e.g., an instance of `DataBatch`, `NewMenuItems`, etc.).

    Raises:
        ProtocolError: If the message has an invalid length (< 0) or an
                       unknown opcode.
        EOFError: If the socket is closed unexpectedly while reading the
                  message header.
    """
    # Read the common message header: [u8 opcode][i32 length]
    opcode = read_u8(sock)
    (length, _) = read_i32(sock, 4, -1)
    if length < 0:
        raise ProtocolError("invalid message length")

    # Instantiate the appropriate message object based on the opcode
    msg = None
    if opcode == Opcodes.FINISHED:
        msg = Finished()
    elif opcode == Opcodes.NEW_MENU_ITEMS:
        msg = NewMenuItems()
    elif opcode == Opcodes.NEW_STORES:
        msg = NewStores()
    elif opcode == Opcodes.NEW_TRANSACTION_ITEMS:
        msg = NewTransactionItems()
    elif opcode == Opcodes.NEW_TRANSACTION:
        msg = NewTransactions()
    elif opcode == Opcodes.NEW_USERS:
        msg = NewUsers()
    elif opcode == Opcodes.EOF:
        msg = EOFMessage()
    elif opcode == Opcodes.DATA_BATCH: # Esto no lo debería mandar nunca el cliente mMMMMM
        msg = DataBatch()
    elif opcode == Opcodes.CLIENT_HELLO:
        msg = ClientHello()
    else:
        # If the opcode is not recognized, it's a protocol violation
        raise ProtocolError(f"invalid opcode: {opcode}")

    body_bytes = recv_exact(sock, length)
    if opcode == Opcodes.FINISHED:
        msg.read_from(body_bytes, length)
    elif opcode == Opcodes.CLIENT_HELLO:
        msg.read_from(body_bytes, length)
    else:
        msg.read_from(body_bytes)
    
    return msg