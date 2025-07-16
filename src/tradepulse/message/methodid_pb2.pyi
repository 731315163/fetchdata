from google.protobuf import struct_pb2 as _struct_pb2
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class MethodID(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    UNDEFINED: _ClassVar[MethodID]
    EXCHANGE: _ClassVar[MethodID]
    MARKET: _ClassVar[MethodID]
    UPDATE_MARKET: _ClassVar[MethodID]
    OHLCV: _ClassVar[MethodID]
    UN_OHLCV: _ClassVar[MethodID]
    ORDERBOOK: _ClassVar[MethodID]
    TICKERS: _ClassVar[MethodID]
    TRADES: _ClassVar[MethodID]
    UN_TRADES: _ClassVar[MethodID]
    HISTORY_OHLCV: _ClassVar[MethodID]
    HISTORY_ORDERBOOK: _ClassVar[MethodID]
    HISTORY_TICKERS: _ClassVar[MethodID]
    HISTORY_TRADES: _ClassVar[MethodID]
UNDEFINED: MethodID
EXCHANGE: MethodID
MARKET: MethodID
UPDATE_MARKET: MethodID
OHLCV: MethodID
UN_OHLCV: MethodID
ORDERBOOK: MethodID
TICKERS: MethodID
TRADES: MethodID
UN_TRADES: MethodID
HISTORY_OHLCV: MethodID
HISTORY_ORDERBOOK: MethodID
HISTORY_TICKERS: MethodID
HISTORY_TRADES: MethodID

class InvokeMethod(_message.Message):
    __slots__ = ("method_id", "params")
    METHOD_ID_FIELD_NUMBER: _ClassVar[int]
    PARAMS_FIELD_NUMBER: _ClassVar[int]
    method_id: MethodID
    params: _struct_pb2.Struct
    def __init__(self, method_id: _Optional[_Union[MethodID, str]] = ..., params: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ...) -> None: ...
