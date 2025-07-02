"""
Functions to convert data from one format to another
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import polars as pl
import polars as pl
from polars import DataFrame

from typenums.constants import (DEFAULT_DATAFRAME_COLUMNS,
                                 DEFAULT_TRADES_COLUMNS, TRADES_DTYPES, Config,
                                 TradeList)
from exceptions import OperationalException
from typenums.timerange import TimeRange
from typenums import TRADES_SCHEME, CandleType, TradingMode,CANDLES_SCHEME
from util import timeframe_to_resample_freq
logger = logging.getLogger(__name__)




def convert_trades_to_ohlcv(
    pairs: list[str],
    timeframes: list[str],
    datadir: Path,
    timerange: TimeRange,
    erase: bool,
    data_format_ohlcv: str,
    data_format_trades: str,
    candle_type: CandleType,
) -> None:
    """
    Convert stored trades data to ohlcv data
    """
    from data.history import get_datahandler

    data_handler_trades = get_datahandler(datadir, data_format=data_format_trades)
    data_handler_ohlcv = get_datahandler(datadir, data_format=data_format_ohlcv)

    logger.info(
        f"About to convert pairs: '{', '.join(pairs)}', "
        f"intervals: '{', '.join(timeframes)}' to {datadir}"
    )
    trading_mode = TradingMode.FUTURES if candle_type != CandleType.SPOT else TradingMode.SPOT
    for pair in pairs:
        trades = data_handler_trades.trades_load(pair, trading_mode)
        for timeframe in timeframes:
            if erase:
                if data_handler_ohlcv.ohlcv_purge(pair, timeframe, candle_type=candle_type):
                    logger.info(f"Deleting existing data for pair {pair}, interval {timeframe}.")
            try:
                ohlcv = trades_to_ohlcv(trades, timeframe)
                # Store ohlcv
                data_handler_ohlcv.ohlcv_store(pair, timeframe, data=ohlcv, candle_type=candle_type)
            except ValueError:
                logger.warning(f"Could not convert {pair} to OHLCV.")



def trades_df_remove_duplicates(trades: pl.DataFrame) -> pl.DataFrame:
    """
    Removes duplicates from the trades DataFrame based on 'timestamp' and 'id' columns.
    :param trades: DataFrame with trade data
    :return: DataFrame with duplicates removed
    """
    return trades.unique(subset=["timestamp", "id"])


def trades_dict_to_list(trades: List[Dict[str, Any]]) -> TradeList:
    """
    Convert fetch_trades result into a List (to be more memory efficient).
    :param trades: List of trades, as returned by ccxt.fetch_trades.
    :return: List of Lists, with constants.DEFAULT_TRADES_COLUMNS as columns
    """
    return [[t[col] for col in DEFAULT_TRADES_COLUMNS] for t in trades]


def trades_convert_types(trades: pl.DataFrame) -> pl.DataFrame:
    """
    Convert Trades dtypes and add 'date' column
    """
    # 添加日期列（从时间戳转换）
    trades = trades.with_columns(
        pl.col("timestamp").alias("date")
    )
    return trades


def trades_list_to_df(trades: TradeList, convert: bool = False) -> pl.DataFrame:
    """
    Convert trades list to Polars DataFrame
    :param trades: List of Lists with trade data
    :param convert: Convert data types
    """
    if not trades:
        df = pl.DataFrame(schema=TRADES_SCHEME)
    else:
        df = pl.DataFrame(trades, schema=TRADES_SCHEME)

    if convert:
        df = trades_convert_types(df)

    return df


def trades_to_ohlcv(trades: pl.DataFrame, timeframe: str) -> pl.DataFrame:
    """
    Converts trades list to OHLCV DataFrame
    :param trades: Trades DataFrame
    :param timeframe: Timeframe to resample data to
    :return: OHLCV Dataframe
    """
    
    
    if trades.height == 0:  # Polars 使用 height 表示行数
        raise ValueError("Trade-list empty.")
    
    # 设置时间索引并排序
    trades = trades.sort("date")
    
    # 转换时间周期为 Polars 兼容的间隔
    resample_interval = timeframe_to_resample_freq(timeframe)
    
    # 执行重采样和聚合
    df_new = (
        trades.lazy()
        .group_by_dynamic
        ("date", every=resample_interval)
        .agg([
            pl.col("price").first().alias("open"),
            pl.col("price").max().alias("high"),
            pl.col("price").min().alias("low"),
            pl.col("price").last().alias("close"),
            pl.col("amount").sum().alias("volume"),
        ])
        .filter(pl.col("volume") > 0)
        .collect()
    )
    
    # 删除零成交量的行
    
    # 确保列顺序正确
    return df_new





def convert_trades_format(config: Config, convert_from: str, convert_to: str, erase: bool):
    """
    Convert trades from one format to another format.
    :param config: Config dictionary
    :param convert_from: Source format
    :param convert_to: Target format
    :param erase: Erase source data (does not apply if source and target format are identical)
    """
    if convert_from == "kraken_csv":
        if config["exchange"]["name"] != "kraken":
            raise OperationalException(
                "Converting from csv is only supported for kraken."
                "Please refer to the documentation for details about this special mode."
            )

    from data.history import get_datahandler

    src = get_datahandler(config["datadir"], convert_from)
    trg = get_datahandler(config["datadir"], convert_to)

    if "pairs" not in config:
        config["pairs"] = src.trades_get_pairs(config["datadir"])
    logger.info(f"Converting trades for {config['pairs']}")
    trading_mode: TradingMode = config.get("trading_mode", TradingMode.SPOT)
    
    for pair in config["pairs"]:
        data = src.trades_load(pair, trading_mode)
        
        # 确保数据是 Polars DataFrame
        if not isinstance(data, pl.DataFrame):
            data = trades_list_to_df(data)
        
        logger.info(f"Converting {data.height} trades for {pair}")
        trg.trades_store(pair, data, trading_mode)

        if erase and convert_from != convert_to:
            logger.info(f"Deleting source Trade data for {pair}.")
            src.trades_purge(pair, trading_mode)