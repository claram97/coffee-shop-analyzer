from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class QueryType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    QUERY_1: _ClassVar[QueryType]
    QUERY_2: _ClassVar[QueryType]
    QUERY_3: _ClassVar[QueryType]
    QUERY_4: _ClassVar[QueryType]
QUERY_1: QueryType
QUERY_2: QueryType
QUERY_3: QueryType
QUERY_4: QueryType

class QueryRequest(_message.Message):
    __slots__ = ("query_type",)
    QUERY_TYPE_FIELD_NUMBER: _ClassVar[int]
    query_type: QueryType
    def __init__(self, query_type: _Optional[_Union[QueryType, str]] = ...) -> None: ...
