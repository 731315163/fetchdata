



from exchange.exchange import Exchange
from collections.abc import Callable
from enum import Enum

class method_enum (Enum):
    ohlcv = "ohlcv"
    orderbook = "orderbook"
    tickers = "tickers"
    trades = "trades"
    history_ohlcv = "history_ohlcv"
    history_orderbook = "history_orderbook"
    history_tickers   = "history_tickers"
    histroy_trades = "history_trades"
    market = "market"
  
def match_method(method: method_enum,ex:Exchange) ->Callable|None :
    match method:
        case method_enum.ohlcv:
            return ex.ohlcv
        case method_enum.orderbook:
            return ex.orderbook
        case method_enum.tickers:
            return ex.tickers
        case method_enum.trades:
            return ex.trades
        case _:
            return None
            