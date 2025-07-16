

import logging

from google.protobuf.json_format import ParseDict

from tradepulse.message.google.protobuf.struct_pb2 import Struct
from tradepulse.message.methodid_pb2 import InvokeMethod, MethodID
from tradepulse.exchange import ExchangeABC 


# from exceptions import *  
from tradepulse.typenums import MarketType

from typing import cast
from tradepulse.message.google.protobuf.struct_pb2 import Struct
  
async def invoke_method(invoke_method: InvokeMethod,ex:ExchangeABC)  :
    
    method,params = invoke_method.method_id,invoke_method.params
    para = params.fields
    symbol = para["symbol"].string_value
    marktype = cast(MarketType, para["marktype"].string_value)
    since = int(para["since"].number_value)

    match method:
        case MethodID.OHLCV:
            timeframe = para["timeframe"].string_value
            return await ex.ohlcv(symbol=symbol,timeframe=timeframe,marketType=marktype,since=since)
        # case method_enum.orderbook:
        #     return ex.orderbook
        # case MethodID.TICKERS:
        #     return ex.tickers
        case MethodID.TRADES:
             return await ex.trades(symbol=symbol,since=since,marketType=marktype) 
        case _:
            return None




def create_invoke_method( method_id: MethodID, params: dict) -> InvokeMethod:
          # Convert dict to protobuf Struct
        struct = Struct()
        ParseDict(js_dict=params, message=struct)
        invoke = InvokeMethod(method_id=method_id, params=struct)   # type: ignore
        return invoke