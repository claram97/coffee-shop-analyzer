"""
Message dispatcher for receiving and routing different protocol messages.
"""

import socket
from .constants import ProtocolError, Opcodes
from .parsing import read_u8, read_i32
from .messages import Finished, NewMenuItems, NewStores, NewTransactionItems, NewTransactions, NewUsers
from .databatch import DataBatch


def recv_msg(sock: socket.socket):
    """Read a message and dispatch it to the appropriate class."""
    opcode = read_u8(sock)
    (length, _) = read_i32(sock, 4, -1)
    if length < 0:
        raise ProtocolError("invalid length")

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
    elif opcode == Opcodes.DATA_BATCH:
        msg = DataBatch()
    else:
        raise ProtocolError(f"invalid opcode: {opcode}")

    msg.read_from(sock, length)
    return msg