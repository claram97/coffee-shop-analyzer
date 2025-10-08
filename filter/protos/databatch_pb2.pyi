import table_data_pb2 as _table_data_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Query(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    Q1: _ClassVar[Query]
    Q2: _ClassVar[Query]
    Q3: _ClassVar[Query]
    Q4: _ClassVar[Query]
Q1: Query
Q2: Query
Q3: Query
Q4: Query

class DataBatch(_message.Message):
    __slots__ = ("query_ids", "total_copies", "copy_num", "total_shards", "shard_num", "filter_steps", "payload")
    QUERY_IDS_FIELD_NUMBER: _ClassVar[int]
    TOTAL_COPIES_FIELD_NUMBER: _ClassVar[int]
    COPY_NUM_FIELD_NUMBER: _ClassVar[int]
    TOTAL_SHARDS_FIELD_NUMBER: _ClassVar[int]
    SHARD_NUM_FIELD_NUMBER: _ClassVar[int]
    FILTER_STEPS_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    query_ids: _containers.RepeatedScalarFieldContainer[Query]
    total_copies: int
    copy_num: int
    total_shards: int
    shard_num: int
    filter_steps: int
    payload: _table_data_pb2.TableData
    def __init__(self, query_ids: _Optional[_Iterable[_Union[Query, str]]] = ..., total_copies: _Optional[int] = ..., copy_num: _Optional[int] = ..., total_shards: _Optional[int] = ..., shard_num: _Optional[int] = ..., filter_steps: _Optional[int] = ..., payload: _Optional[_Union[_table_data_pb2.TableData, _Mapping]] = ...) -> None: ...
