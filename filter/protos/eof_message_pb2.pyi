import table_data_pb2 as _table_data_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class EOFMessage(_message.Message):
    __slots__ = ("table",)
    TABLE_FIELD_NUMBER: _ClassVar[int]
    table: _table_data_pb2.TableName
    def __init__(self, table: _Optional[_Union[_table_data_pb2.TableName, str]] = ...) -> None: ...
