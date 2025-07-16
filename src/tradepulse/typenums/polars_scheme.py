import polars as pl




TRADES_SCHEME = {
    "timestamp": pl.Datetime(time_unit="ms",time_zone="UTC"),
    "id": pl.String,
    "type": pl.String,
    "side": pl.String,
    "price": pl.Float64,
    "amount": pl.Float64,
    "cost": pl.Float64,
}

CANDLES_SCHEME ={
    "date" : pl.Datetime(time_unit="ms",time_zone="UTC"),
            "open": pl.Float64,
            "high": pl.Float64,
            "low": pl.Float64,
            "close": pl.Float64,
            "volume": pl.Float64,
    }


