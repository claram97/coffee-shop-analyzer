"""
Provides the primary dispatcher function for receiving and routing all incoming
protocol messages based on their opcode.
"""

import socket
from .constants import ProtocolError, Opcodes
from .socket_parsing import read_u8, read_i32, recv_exact
from .messages import Finished, NewMenuItems, NewStores, NewTransactionItems, NewTransactions, NewUsers, EOFMessage
from .databatch import DataBatch


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
    elif opcode == Opcodes.DATA_BATCH:
        msg = DataBatch()
    else:
        # If the opcode is not recognized, it's a protocol violation
        raise ProtocolError(f"invalid opcode: {opcode}")

    # Handle different message types based on their read_from signature
    if opcode == Opcodes.FINISHED:
        # Finished message expects (sock, length) parameters
        msg.read_from(sock, length)
    else:
        # Other messages expect body_bytes parameter
        # Read the entire message body from the socket first
        body_bytes = recv_exact(sock, length)
        msg.read_from(body_bytes)
    
    return msg