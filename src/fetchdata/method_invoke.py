



from typing import cast
from exchange import ExchangeProtocol
from collections.abc import Callable
from enum import Enum
from fetchdata.message.google.protobuf.struct_pb2 import Struct,Value
from .message.methodid_pb2 import MethodID,InvokeMethod
from typenums import MarketType
  
async def invoke_method(invoke_method: InvokeMethod,ex:ExchangeProtocol)  :
    
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
            