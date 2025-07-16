from typing import Literal


DataType = Literal["ohlcv","orderbook","ticker","trades"]

DataTypeColumns = {
    "ohlcv": ["datetime", "open", "high", "low", "close", "volume"],
    "orderbook": ["timestamp", "bids", "asks"],
    "ticker": ["timestamp", "bid", "ask"],
    "trades": ["timestamp", "id", "price", "amount", "side"],
}

TRADES_DTYPES = {
    "timestamp": "int64",
    "id": "str",
    "type": "str",
    "side": "str",
    "price": "float64",
    "amount": "float64",
    "cost": "float64",
}

