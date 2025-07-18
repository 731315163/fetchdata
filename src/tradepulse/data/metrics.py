import logging
import math
from dataclasses import dataclass
from datetime import datetime

import numpy as np
import polars as pl

logger = logging.getLogger(__name__)


def calculate_market_change(
    data: dict[str, pl.DataFrame], column: str = "close", min_date: datetime | None = None
) -> float:
    """
    Calculate market change based on "column".
    Calculation is done by taking the first non-null and the last non-null element of each column
    and calculating the pctchange as "(last - first) / first".
    Then the results per pair are combined as mean.

    :param data: Dict of Dataframes, dict key should be pair.
    :param column: Column in the original dataframes to use
    :param min_date: Minimum date to consider for calculations. Market change should only be
        calculated for data actually backtested, excluding startup periods.
    :return:
    """
    tmp_means = []
    for pair, df in data.items():
        df1 = df
        if min_date is not None:
            df1 = df1[df1["date"] >= min_date]
        if df1.is_empty():
            logger.warning(f"Pair {pair} has no data after {min_date}.")
            continue
        start = df1[column].drop_nulls()[0]
        end = df1[column].drop_nulls()[-1]
        tmp_means.append((end - start) / start)

    if not tmp_means:
        return 0.0
    return float(np.mean(tmp_means))


def combine_dataframes_by_column(
    data: dict[str, pl.DataFrame], column: str = "close"
) -> pl.DataFrame:
    """
    Combine multiple dataframes "column"
    :param data: Dict of Dataframes, dict key should be pair.
    :param column: Column in the original dataframes to use
    :return: DataFrame with the column renamed to the dict key.
    :raise: ValueError if no data is provided.
    """
    if not data:
        raise ValueError("No data provided.")
   
    df_comb = pl.concat(
        [data[pair].rename({column: pair}).select(pair) for pair in data], how="horizontal"
    )
    return df_comb


def combined_dataframes_with_rel_mean(
    data: dict[str, pl.DataFrame], fromdt: datetime, todt: datetime, column: str = "close"
) -> pl.DataFrame:
    """
    Combine multiple dataframes "column"
    :param data: Dict of Dataframes, dict key should be pair.
    :param column: Column in the original dataframes to use
    :return: DataFrame with the column renamed to the dict key, and a column
        named mean, containing the mean of all pairs.
    :raise: ValueError if no data is provided.
    """
    df_comb = combine_dataframes_by_column(data, column)
    # Trim dataframes to the given timeframe
    df_comb = df_comb.filter((df_comb["date"] >= fromdt) & (df_comb["date"] < todt))
    
    df_comb = df_comb.with_columns([
        pl.sum_horizontal(pl.col("*").is_not_null()).alias("count"),
        pl.mean_horizontal(pl.col("*")).alias("mean")
    ])
    df_comb = df_comb.with_columns([
        (pl.col("mean").diff(n=1, null_behavior="forward") / pl.col("mean").shift(1))
        .fill_null(0)
        .cumsum()
        .alias("rel_mean")
    ])
    return df_comb.select(["mean", "rel_mean", "count"])


def combine_dataframes_with_mean(
    data: dict[str, pl.DataFrame], column: str = "close"
) -> pl.DataFrame:
    """
    Combine multiple dataframes "column"
    :param data: Dict of Dataframes, dict key should be pair.
    :param column: Column in the original dataframes to use
    :return: DataFrame with the column renamed to the dict key, and a column
        named mean, containing the mean of all pairs.
    :raise: ValueError if no data is provided.
    """
    df_comb = combine_dataframes_by_column(data, column)

    df_comb["mean"] = df_comb.mean(axis=1)

    return df_comb


def create_cum_profit(
    df: pl.DataFrame, trades: pl.DataFrame, col_name: str, timeframe: str
) -> pl.DataFrame:
    """
    Adds a column `col_name` with the cumulative profit for the given trades array.
    :param df: DataFrame with date index
    :param trades: DataFrame containing trades (requires columns close_date and profit_abs)
    :param col_name: Column name that will be assigned the results
    :param timeframe: Timeframe used during the operations
    :return: Returns df with one additional column, col_name, containing the cumulative profit.
    :raise: ValueError if trade-dataframe was found empty.
    """
    if len(trades) == 0:
        raise ValueError("Trade dataframe empty.")
    from util import timeframe_to_resample_freq

    timeframe_freq = timeframe_to_resample_freq(timeframe)
    # Resample to timeframe to make sure trades match candles
    _trades_sum = trades.resample(timeframe_freq, on="close_date")[["profit_abs"]].sum()
    df.loc[:, col_name] = _trades_sum["profit_abs"].cumsum()
    # Set first value to 0
    df.loc[df.iloc[0].name, col_name] = 0
    # FFill to get continuous
    df[col_name] = df[col_name].ffill()
    return df


def _calc_drawdown_series(
    profit_results: pl.DataFrame, *, date_col: str, value_col: str, starting_balance: float
) -> pl.DataFrame:
    max_drawdown_df = pl.DataFrame()
    max_drawdown_df["cumulative"] = profit_results[value_col].cumsum()
    max_drawdown_df["high_value"] = np.maximum(0, max_drawdown_df["cumulative"].cummax())
    max_drawdown_df["drawdown"] = max_drawdown_df["cumulative"] - max_drawdown_df["high_value"]
    max_drawdown_df["date"] = profit_results.loc[:, date_col]
    if starting_balance:
        cumulative_balance = starting_balance + max_drawdown_df["cumulative"]
        max_balance = starting_balance + max_drawdown_df["high_value"]
        max_drawdown_df["drawdown_relative"] = (max_balance - cumulative_balance) / max_balance
    else:
        # NOTE: This is not completely accurate,
        # but might good enough if starting_balance is not available
        max_drawdown_df["drawdown_relative"] = (
            max_drawdown_df["high_value"] - max_drawdown_df["cumulative"]
        ) / max_drawdown_df["high_value"]
    return max_drawdown_df


def calculate_underwater(
    trades: pl.DataFrame,
    *,
    date_col: str = "close_date",
    value_col: str = "profit_ratio",
    starting_balance: float = 0.0,
):
    """
    Calculate max drawdown and the corresponding close dates
    :param trades: DataFrame containing trades (requires columns close_date and profit_ratio)
    :param date_col: Column in DataFrame to use for dates (defaults to 'close_date')
    :param value_col: Column in DataFrame to use for values (defaults to 'profit_ratio')
    :return: Tuple (float, highdate, lowdate, highvalue, lowvalue) with absolute max drawdown,
             high and low time and high and low value.
    :raise: ValueError if trade-dataframe was found empty.
    """
    if len(trades) == 0:
        raise ValueError("Trade dataframe empty.")
    profit_results = trades.sort_values(date_col).reset_index(drop=True)
    max_drawdown_df = _calc_drawdown_series(
        profit_results, date_col=date_col, value_col=value_col, starting_balance=starting_balance
    )

    return max_drawdown_df


@dataclass()
class DrawDownResult:
    drawdown_abs: float = 0.0
    high_date: pl.Timestamp = None
    low_date: pl.Timestamp = None
    high_value: float = 0.0
    low_value: float = 0.0
    relative_account_drawdown: float = 0.0


def calculate_max_drawdown(
    trades: pl.DataFrame,
    *,
    date_col: str = "close_date",
    value_col: str = "profit_abs",
    starting_balance: float = 0,
    relative: bool = False,
) -> DrawDownResult:
    """
    Calculate max drawdown and the corresponding close dates
    :param trades: DataFrame containing trades (requires columns close_date and profit_ratio)
    :param date_col: Column in DataFrame to use for dates (defaults to 'close_date')
    :param value_col: Column in DataFrame to use for values (defaults to 'profit_abs')
    :param starting_balance: Portfolio starting balance - properly calculate relative drawdown.
    :return: DrawDownResult object
             with absolute max drawdown, high and low time and high and low value,
             and the relative account drawdown
    :raise: ValueError if trade-dataframe was found empty.
    """
    if len(trades) == 0:
        raise ValueError("Trade dataframe empty.")
    profit_results = trades.sort_values(date_col).reset_index(drop=True)
    max_drawdown_df = _calc_drawdown_series(
        profit_results, date_col=date_col, value_col=value_col, starting_balance=starting_balance
    )

    idxmin = (
        max_drawdown_df["drawdown_relative"].idxmax()
        if relative
        else max_drawdown_df["drawdown"].idxmin()
    )

    high_idx = max_drawdown_df.iloc[: idxmin + 1]["high_value"].idxmax()
    high_date = profit_results.loc[high_idx, date_col]
    low_date = profit_results.loc[idxmin, date_col]
    high_val = max_drawdown_df.loc[high_idx, "cumulative"]
    low_val = max_drawdown_df.loc[idxmin, "cumulative"]
    max_drawdown_rel = max_drawdown_df.loc[idxmin, "drawdown_relative"]

    return DrawDownResult(
        drawdown_abs=abs(max_drawdown_df.loc[idxmin, "drawdown"]),
        high_date=high_date,
        low_date=low_date,
        high_value=high_val,
        low_value=low_val,
        relative_account_drawdown=max_drawdown_rel,
    )


def calculate_csum(trades: pl.DataFrame, starting_balance: float = 0) -> tuple[float, float]:
    """
    Calculate min/max cumsum of trades, to show if the wallet/stake amount ratio is sane
    :param trades: DataFrame containing trades (requires columns close_date and profit_percent)
    :param starting_balance: Add starting balance to results, to show the wallets high / low points
    :return: Tuple (float, float) with cumsum of profit_abs
    :raise: ValueError if trade-dataframe was found empty.
    """
    if len(trades) == 0:
        raise ValueError("Trade dataframe empty.")

    csum_df = pl.DataFrame()
    csum_df["sum"] = trades["profit_abs"].cumsum()
    csum_min = csum_df["sum"].min() + starting_balance
    csum_max = csum_df["sum"].max() + starting_balance

    return csum_min, csum_max


def calculate_cagr(days_passed: int, starting_balance: float, final_balance: float) -> float:
    """
    Calculate CAGR
    :param days_passed: Days passed between start and ending balance
    :param starting_balance: Starting balance
    :param final_balance: Final balance to calculate CAGR against
    :return: CAGR
    """
    if final_balance < 0:
        # With leveraged trades, final_balance can become negative.
        return 0
    return (final_balance / starting_balance) ** (1 / (days_passed / 365)) - 1


def calculate_expectancy(trades: pl.DataFrame) -> tuple[float, float]:
    """
    Calculate expectancy
    :param trades: DataFrame containing trades (requires columns close_date and profit_abs)
    :return: expectancy, expectancy_ratio
    """

    expectancy = 0.0
    expectancy_ratio = 100.0

    if len(trades) > 0:
        winning_trades = trades.loc[trades["profit_abs"] > 0]
        losing_trades = trades.loc[trades["profit_abs"] < 0]
        profit_sum = winning_trades["profit_abs"].sum()
        loss_sum = abs(losing_trades["profit_abs"].sum())
        nb_win_trades = len(winning_trades)
        nb_loss_trades = len(losing_trades)

        average_win = (profit_sum / nb_win_trades) if nb_win_trades > 0 else 0
        average_loss = (loss_sum / nb_loss_trades) if nb_loss_trades > 0 else 0
        winrate = nb_win_trades / len(trades)
        loserate = nb_loss_trades / len(trades)

        expectancy = (winrate * average_win) - (loserate * average_loss)
        if average_loss > 0:
            risk_reward_ratio = average_win / average_loss
            expectancy_ratio = ((1 + risk_reward_ratio) * winrate) - 1

    return expectancy, expectancy_ratio


def calculate_sortino(
    trades: pl.DataFrame, min_date: datetime, max_date: datetime, starting_balance: float
) -> float:
    """
    Calculate sortino
    :param trades: DataFrame containing trades (requires columns profit_abs)
    :return: sortino
    """
    if (len(trades) == 0) or (min_date is None) or (max_date is None) or (min_date == max_date):
        return 0

    total_profit = trades["profit_abs"] / starting_balance
    days_period = max(1, (max_date - min_date).days)

    expected_returns_mean = total_profit.sum() / days_period

    down_stdev = np.std(trades.loc[trades["profit_abs"] < 0, "profit_abs"] / starting_balance)

    if down_stdev != 0 and not np.isnan(down_stdev):
        sortino_ratio = expected_returns_mean / down_stdev * np.sqrt(365)
    else:
        # Define high (negative) sortino ratio to be clear that this is NOT optimal.
        sortino_ratio = -100

    # print(expected_returns_mean, down_stdev, sortino_ratio)
    return sortino_ratio


def calculate_sharpe(
    trades: pl.DataFrame, min_date: datetime, max_date: datetime, starting_balance: float
) -> float:
    """
    Calculate sharpe
    :param trades: DataFrame containing trades (requires column profit_abs)
    :return: sharpe
    """
    if (len(trades) == 0) or (min_date is None) or (max_date is None) or (min_date == max_date):
        return 0

    total_profit = trades["profit_abs"] / starting_balance
    days_period = max(1, (max_date - min_date).days)

    expected_returns_mean = total_profit.sum() / days_period
    up_stdev = np.std(total_profit)

    if up_stdev != 0:
        sharp_ratio = expected_returns_mean / up_stdev * np.sqrt(365)
    else:
        # Define high (negative) sharpe ratio to be clear that this is NOT optimal.
        sharp_ratio = -100

    # print(expected_returns_mean, up_stdev, sharp_ratio)
    return sharp_ratio


def calculate_calmar(
    trades: pl.DataFrame, min_date: datetime, max_date: datetime, starting_balance: float
) -> float:
    """
    Calculate calmar
    :param trades: DataFrame containing trades (requires columns close_date and profit_abs)
    :return: calmar
    """
    if (len(trades) == 0) or (min_date is None) or (max_date is None) or (min_date == max_date):
        return 0

    total_profit = trades["profit_abs"].sum() / starting_balance
    days_period = max(1, (max_date - min_date).days)

    # adding slippage of 0.1% per trade
    # total_profit = total_profit - 0.0005
    expected_returns_mean = total_profit / days_period * 100

    # calculate max drawdown
    try:
        drawdown = calculate_max_drawdown(
            trades, value_col="profit_abs", starting_balance=starting_balance
        )
        max_drawdown = drawdown.relative_account_drawdown
    except ValueError:
        max_drawdown = 0

    if max_drawdown != 0:
        calmar_ratio = expected_returns_mean / max_drawdown * math.sqrt(365)
    else:
        # Define high (negative) calmar ratio to be clear that this is NOT optimal.
        calmar_ratio = -100

    # print(expected_returns_mean, max_drawdown, calmar_ratio)
    return calmar_ratio


def calculate_sqn(trades: pl.DataFrame, starting_balance: float) -> float:
    """
    Calculate System Quality Number (SQN) - Van K. Tharp.
    SQN measures systematic trading quality and takes into account both
    the number of trades and their standard deviation.

    :param trades: DataFrame containing trades (requires column profit_abs)
    :param starting_balance: Starting balance of the trading system
    :return: SQN value
    """
    if len(trades) == 0:
        return 0.0

    total_profit = trades["profit_abs"] / starting_balance
    number_of_trades = len(trades)

    # Calculate average trade and standard deviation
    average_profits = total_profit.mean()
    profits_std = total_profit.std()

    if profits_std != 0 and not np.isnan(profits_std):
        sqn = math.sqrt(number_of_trades) * (average_profits / profits_std)
    else:
        # Define negative SQN to indicate this is NOT optimal
        sqn = -100.0

    return round(sqn, 4)
