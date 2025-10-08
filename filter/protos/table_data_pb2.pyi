from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class TableName(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    MENU_ITEMS: _ClassVar[TableName]
    STORES: _ClassVar[TableName]
    TRANSACTION_ITEMS: _ClassVar[TableName]
    TRANSACTIONS: _ClassVar[TableName]
    USERS: _ClassVar[TableName]
    QUERY_RESULTS_1: _ClassVar[TableName]
    QUERY_RESULTS_2: _ClassVar[TableName]
    QUERY_RESULTS_3: _ClassVar[TableName]
    QUERY_RESULTS_4: _ClassVar[TableName]

class BatchStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CONTINUE: _ClassVar[BatchStatus]
    EOF: _ClassVar[BatchStatus]
    CANCEL: _ClassVar[BatchStatus]
MENU_ITEMS: TableName
STORES: TableName
TRANSACTION_ITEMS: TableName
TRANSACTIONS: TableName
USERS: TableName
QUERY_RESULTS_1: TableName
QUERY_RESULTS_2: TableName
QUERY_RESULTS_3: TableName
QUERY_RESULTS_4: TableName
CONTINUE: BatchStatus
EOF: BatchStatus
CANCEL: BatchStatus

class TableSchema(_message.Message):
    __slots__ = ("columns",)
    COLUMNS_FIELD_NUMBER: _ClassVar[int]
    columns: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, columns: _Optional[_Iterable[str]] = ...) -> None: ...

class Row(_message.Message):
    __slots__ = ("values",)
    VALUES_FIELD_NUMBER: _ClassVar[int]
    values: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, values: _Optional[_Iterable[str]] = ...) -> None: ...

class TableData(_message.Message):
    __slots__ = ("name", "schema", "rows", "batch_number", "status")
    NAME_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_FIELD_NUMBER: _ClassVar[int]
    ROWS_FIELD_NUMBER: _ClassVar[int]
    BATCH_NUMBER_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    name: TableName
    schema: TableSchema
    rows: _containers.RepeatedCompositeFieldContainer[Row]
    batch_number: int
    status: BatchStatus
    def __init__(self, name: _Optional[_Union[TableName, str]] = ..., schema: _Optional[_Union[TableSchema, _Mapping]] = ..., rows: _Optional[_Iterable[_Union[Row, _Mapping]]] = ..., batch_number: _Optional[int] = ..., status: _Optional[_Union[BatchStatus, str]] = ...) -> None: ...
