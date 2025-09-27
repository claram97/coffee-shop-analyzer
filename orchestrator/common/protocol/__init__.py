# Protocol package for message handling and communication definitions

from .constants import ProtocolError, Opcodes, BatchStatus, MAX_BATCH_SIZE_BYTES
from .entities import RawMenuItems, RawStore, RawTransactionItem, RawTransaction, RawUser
from .messages import (
    TableMessage, NewMenuItems, NewStores, NewTransactionItems, 
    NewTransactions, NewUsers, Finished, BatchRecvSuccess, BatchRecvFail
)
from .databatch import DataBatch
from .dispatcher import recv_msg
from .parsing import (
    recv_exactly, read_u8, read_u16, read_i32, read_i64, read_string,
    write_u8, write_u16, write_i32, write_i64, write_string
)

__all__ = [
    # Constants and exceptions
    'ProtocolError', 'Opcodes', 'BatchStatus', 'MAX_BATCH_SIZE_BYTES',
    
    # Data entities
    'RawMenuItems', 'RawStore', 'RawTransactionItem', 'RawTransaction', 'RawUser',
    
    # Message classes
    'TableMessage', 'NewMenuItems', 'NewStores', 'NewTransactionItems', 
    'NewTransactions', 'NewUsers', 'Finished', 'BatchRecvSuccess', 'BatchRecvFail',
    
    # Complex messages
    'DataBatch',
    
    # Message handling
    'recv_msg',
    
    # Low-level parsing
    'recv_exactly', 'read_u8', 'read_u16', 'read_i32', 'read_i64', 'read_string',  
    'write_u8', 'write_u16', 'write_i32', 'write_i64', 'write_string'
]