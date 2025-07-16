"""
Functions to convert data from one format to another
"""

from datetime import timedelta
import logging

import polars as pl
from polars import DataFrame

from tradepulse.util import timeframe_to_seconds
from tradepulse.typenums.constants import Config
from tradepulse.data.timerange import TimeRange
from tradepulse.typenums import CandleType, TradingMode,CANDLES_SCHEME

logger = logging.getLogger(__name__)


def ohlcv_to_dataframe(
    ohlcv: list,
    timeframe: str,
    pair: str,
    *,
    fill_missing: bool = True,
    drop_incomplete: bool = True,
) -> DataFrame:
    """
    Converts a list with candle (OHLCV) data (in format returned by ccxt.fetch_ohlcv)
    to a Dataframe
    :param ohlcv: list with candle (OHLCV) data, as returned by exchange.async_get_candle_history
    :param timeframe: timeframe (e.g. 5m). Used to fill up eventual missing data
    :param pair: Pair this data is for (used to warn if fillup was necessary)
    :param fill_missing: fill up missing candles with 0 candles
                         (see ohlcv_fill_up_missing_data for details)
    :param drop_incomplete: Drop the last candle of the dataframe, assuming it's incomplete
    :return: DataFrame
    """
    logger.debug(f"Converting candle (OHLCV) data to dataframe for pair {pair}.")
    df = DataFrame(ohlcv, CANDLES_SCHEME)
   
    return clean_ohlcv_dataframe(
        df, timeframe, pair, fill_missing=fill_missing, drop_incomplete=drop_incomplete
    )


def clean_ohlcv_dataframe(
    data: DataFrame, timeframe: str, pair: str, *, fill_missing: bool, drop_incomplete: bool
) -> DataFrame:
    """
    Cleanse a OHLCV dataframe using Polars by
      * Grouping it by date (removes duplicate tics)
      * dropping last candles if requested
      * Filling up missing data (if requested)
    :param data: Polars DataFrame containing candle (OHLCV) data.
    :param timeframe: timeframe (e.g. 5m). Used to fill up eventual missing data
    :param pair: Pair this data is for (used to warn if fillup was necessary)
    :param fill_missing: fill up missing candles with 0 candles
    :param drop_incomplete: Drop the last candle of the dataframe, assuming it's incomplete
    :return: Polars DataFrame
    """
    # 分组聚合去重
    data = data.group_by("date").agg([
        pl.col("open").first(),
        pl.col("high").max(),
        pl.col("low").min(),
        pl.col("close").last(),
        pl.col("volume").max()
    ]).sort("date")
    
    # 删除最后一根K线
    if drop_incomplete:
        data = data.slice(0, data.height - 1)  # 等效于 drop(data.tail(1))
    
    # 填充缺失数据
    if fill_missing:
        return ohlcv_fill_up_missing_data(data, timeframe, pair)
    
    return data


def ohlcv_fill_up_missing_data(dataframe: DataFrame, timeframe: str, pair: str) -> DataFrame:
    """
    Fills up missing data with 0 volume rows,
    using the previous close as price for "open", "high", "low" and "close", volume is set to 0
    """
    # 确保日期列是正确的类型
    if dataframe["date"].dtype != pl.Datetime:
        dataframe = dataframe.with_columns(pl.col("date").cast(pl.Datetime))
    
    # 设置聚合函数映射
    ohlcv_dict = {
        "open": pl.col("open").first(),
        "high": pl.col("high").max(),
        "low": pl.col("low").min(),
        "close": pl.col("close").last(),
        "volume": pl.col("volume").sum()
    }
    
    # 转换时间周期为 Polars 兼容的间隔字符串
    tf_timedelt= timedelta(seconds= timeframe_to_seconds(timeframe))
    
    # 重采样并聚合数据
    df = (
        dataframe
        .sort("date")
        .upsample(time_column="date", every=tf_timedelt)
        .fill_null(strategy="forward")  # 前向填充 close
        .with_columns(
            pl.col("open").fill_null(pl.col("close")),  # 使用前一个 close 填充 open
            pl.col("high").fill_null(pl.col("close")),  # 使用前一个 close 填充 high
            pl.col("low").fill_null(pl.col("close")),   # 使用前一个 close 填充 low
            pl.col("volume").fill_null(0)               # 填充 volume 为 0
        )
    )
    
    # 计算缺失数据比例并记录日志
    len_before = len(dataframe)
    len_after = len(df)
    pct_missing = (len_after - len_before) / len_before if len_before > 0 else 0
    
    if len_before != len_after:
        message = (
            f"Missing data fillup for {pair}, {timeframe}: "
            f"before: {len_before} - after: {len_after} - {pct_missing:.2%}"
        )
        if pct_missing > 0.01:
            logger.info(message)
        else:
            logger.debug(message)
    
    return df


def trim_dataframe(
    df: DataFrame, timerange:TimeRange, *, df_date_col: str = "date", startup_candles: int = 0
) -> DataFrame:
    """
    Trim dataframe based on given timerange
    :param df: Dataframe to trim
    :param timerange: timerange (use start and end date if available)
    :param df_date_col: Column in the dataframe to use as Date column
    :param startup_candles: When not 0, is used instead the timerange start date
    :return: trimmed dataframe
    """
     
    # 确保日期列是正确的类型
    if df[df_date_col].dtype != pl.Datetime:
        df = df.with_columns(pl.col(df_date_col).cast(pl.Datetime))
    
    # 根据 startup_candles 或 timerange 修剪数据
    if startup_candles > 0:
        # 跳过指定数量的起始蜡烛
        df = df.slice(startup_candles, len(df) - startup_candles)
    elif timerange.starttype == "date":
        # 根据开始日期过滤
        df = df.filter(pl.col(df_date_col) >= timerange.startdt)
    
    # 根据结束日期过滤
    if timerange.stoptype == "date":
        df = df.filter(pl.col(df_date_col) <= timerange.stopdt)
    
    return df


def trim_dataframes(
    preprocessed: dict[str, DataFrame], timerange, startup_candles: int
) -> dict[str, DataFrame]:
    """
    Trim startup period from analyzed dataframes
    :param preprocessed: Dict of pair: dataframe
    :param timerange: timerange (use start and end date if available)
    :param startup_candles: Startup-candles that should be removed
    :return: Dict of trimmed dataframes
    """
    processed: dict[str, DataFrame] = {}

    for pair, df in preprocessed.items():
        trimed_df = trim_dataframe(df, timerange, startup_candles=startup_candles)
        if not trimed_df.is_empty():
            processed[pair] = trimed_df
        else:
            logger.warning(
                f"{pair} has no data left after adjusting for startup candles, skipping."
            )
    return processed


# def order_book_to_dataframe(bids: list, asks: list) -> DataFrame:
#     """
#     TODO: This should get a dedicated test
#     Gets order book list, returns dataframe with below format per suggested by creslin
#     -------------------------------------------------------------------
#      b_sum       b_size       bids       asks       a_size       a_sum
#     -------------------------------------------------------------------
#     """
#     cols = ["bids", "b_size"]

#     bids_frame = DataFrame(bids, columns=cols)
#     # add cumulative sum column
#     bids_frame["b_sum"] = bids_frame["b_size"].cumsum()
#     cols2 = ["asks", "a_size"]
#     asks_frame = DataFrame(asks, columns=cols2)
#     # add cumulative sum column
#     asks_frame["a_sum"] = asks_frame["a_size"].cumsum()

#     frame = pd.concat(
#         [
#             bids_frame["b_sum"],
#             bids_frame["b_size"],
#             bids_frame["bids"],
#             asks_frame["asks"],
#             asks_frame["a_size"],
#             asks_frame["a_sum"],
#         ],
#         axis=1,
#         keys=["b_sum", "b_size", "bids", "asks", "a_size", "a_sum"],
#     )
#     # logger.info('order book %s', frame )
#     return frame


def convert_ohlcv_format(
    config: Config,
    convert_from: str,
    convert_to: str,
    erase: bool,
):
    """
    Convert OHLCV from one format to another
    :param config: Config dictionary
    :param convert_from: Source format
    :param convert_to: Target format
    :param erase: Erase source data (does not apply if source and target format are identical)
    """
    from tradepulse.data.history import get_datahandler

    src = get_datahandler(config["datadir"], convert_from)
    trg = get_datahandler(config["datadir"], convert_to)
    timeframes = config.get("timeframes", [config.get("timeframe")])
    logger.info(f"Converting candle (OHLCV) for timeframe {timeframes}")

    candle_types = [
        CandleType.from_string(ct)
        for ct in config.get("candle_types", [c.value for c in CandleType])
    ]
    logger.info(candle_types)
    paircombs = src.ohlcv_get_available_data(config["datadir"], TradingMode.SPOT)
    paircombs.extend(src.ohlcv_get_available_data(config["datadir"], TradingMode.FUTURES))

    if "pairs" in config:
        # Filter pairs
        paircombs = [comb for comb in paircombs if comb[0] in config["pairs"]]

    if "timeframes" in config:
        paircombs = [comb for comb in paircombs if comb[1] in config["timeframes"]]
    paircombs = [comb for comb in paircombs if comb[2] in candle_types]

    paircombs = sorted(paircombs, key=lambda x: (x[0], x[1], x[2].value))

    formatted_paircombs = "\n".join(
        [f"{pair}, {timeframe}, {candle_type}" for pair, timeframe, candle_type in paircombs]
    )

    logger.info(
        f"Converting candle (OHLCV) data for the following pair combinations:\n"
        f"{formatted_paircombs}"
    )
    for pair, timeframe, candle_type in paircombs:
        data = src.ohlcv_load(
            pair=pair,
            timeframe=timeframe,
            timerange=None,
            fill_missing=False,
            drop_incomplete=False,
            startup_candles=0,
            candle_type=candle_type,
        )
        logger.info(f"Converting {len(data)} {timeframe} {candle_type} candles for {pair}")
        if len(data) > 0:
            trg.ohlcv_store(pair=pair, timeframe=timeframe, data=data, candle_type=candle_type)
            if erase and convert_from != convert_to:
                logger.info(f"Deleting source data for {pair} / {timeframe}")
                src.ohlcv_purge(pair=pair, timeframe=timeframe, candle_type=candle_type)



