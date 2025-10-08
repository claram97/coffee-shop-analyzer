import eof_message_pb2 as _eof_message_pb2
import table_data_pb2 as _table_data_pb2
import query_request_pb2 as _query_request_pb2
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Envelope(_message.Message):
    __slots__ = ("type", "eof", "table_data", "query_request")
    class Opcode(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        UNKNOWN: _ClassVar[Envelope.Opcode]
        EOF: _ClassVar[Envelope.Opcode]
        FINISHED: _ClassVar[Envelope.Opcode]
        TABLE_DATA: _ClassVar[Envelope.Opcode]
        BATCH_RECV_SUrCESS: _ClassVar[Envelope.Opcode]
        BATCH_RECV_FAIL: _ClassVar[Envelope.Opcode]
        QUERY_REQUEST: _ClassVar[Envelope.Opcode]
    UNKNOWN: Envelope.Opcode
    EOF: Envelope.Opcode
    FINISHED: Envelope.Opcode
    TABLE_DATA: Envelope.Opcode
    BATCH_RECV_SUrCESS: Envelope.Opcode
    BATCH_RECV_FAIL: Envelope.Opcode
    QUERY_REQUEST: Envelope.Opcode
    TYPE_FIELD_NUMBER: _ClassVar[int]
    EOF_FIELD_NUMBER: _ClassVar[int]
    TABLE_DATA_FIELD_NUMBER: _ClassVar[int]
    QUERY_REQUEST_FIELD_NUMBER: _ClassVar[int]
    type: Envelope.Opcode
    eof: _eof_message_pb2.EOFMessage
    table_data: _table_data_pb2.TableData
    query_request: _query_request_pb2.QueryRequest
    def __init__(self, type: _Optional[_Union[Envelope.Opcode, str]] = ..., eof: _Optional[_Union[_eof_message_pb2.EOFMessage, _Mapping]] = ..., table_data: _Optional[_Union[_table_data_pb2.TableData, _Mapping]] = ..., query_request: _Optional[_Union[_query_request_pb2.QueryRequest, _Mapping]] = ...) -> None: ...
