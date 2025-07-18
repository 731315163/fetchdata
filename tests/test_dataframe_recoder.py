import pytest
import polars as pl
import pyarrow as pa
from datetime import datetime, timedelta, timezone
from tradepulse.data import DataRecoder, DataKey
from tradepulse.typenums import MarketType, DataType
from tradepulse.data.dataframe_recoder import DataFrameRecoder  # 替换为实际模块名

# Fixtures
@pytest.fixture
def sample_df():
    return pl.DataFrame({
        "timestamp": [1620000000000, 1620000060000],  # ms timestamps
        "price": [100.0, 101.0],
       
    })

@pytest.fixture
def datetime_df():
    return pl.DataFrame({
        "timestamp": [
          1620000070000, 1620000090000
        ],
        "price": [200.0, 201.0]
    })

@pytest.fixture
def recoder():
    return DataFrameRecoder(
        pair="BTC/USDT",
        marketType="spot",
        datatype="trades",
        timeframe="1m"
    )

# Test append method
def test_append_with_valid_data(recoder: DataFrameRecoder, sample_df: pl.DataFrame):
    recoder.append(sample_df)
    assert len(recoder) == 2
    assert recoder.data.equals(sample_df)

def test_append_missing_time_column(recoder: DataFrameRecoder):
    df = pl.DataFrame({"price": [100.0]})
    with pytest.raises(ValueError):
        recoder.append(df)

def test_append_time_conversion(recoder: DataFrameRecoder, sample_df: pl.DataFrame, datetime_df: pl.DataFrame):
    recoder.append(datetime_df)  # Datetime类型
    recoder.append(sample_df)    # Int64类型
    assert recoder.data["timestamp"].dtype == pl.Int64

# Test prepend method
def test_prepend_empty_data(recoder: DataFrameRecoder, sample_df: pl.DataFrame):
    assert len(recoder) == 0
    recoder.prepend(sample_df)
    assert len(recoder) == 2
    assert recoder.data.equals(sample_df)

def test_prepend_column_mismatch(recoder: DataFrameRecoder):
    df1 = pl.DataFrame({"timestamp": [1], "price": [100.0]})
    df2 = pl.DataFrame({"timestamp": [2], "volume": [1000]})
    recoder.prepend(df1)
    with pytest.raises(pl.exceptions.ShapeError):
        recoder.prepend(df2)

# Test _ensure_time_format
def test_time_format_conversion(recoder: DataFrameRecoder, sample_df: pl.DataFrame, datetime_df: pl.DataFrame):
    recoder.append(datetime_df)
    converted = recoder._ensure_time_format(sample_df)
    assert converted["timestamp"].dtype == pl.Int64




def test_prune_expired_data(mock_datetime, recoder: DataFrameRecoder, sample_df: pl.DataFrame):
    # 设置mock时间（当前时间）
    mock_datetime.now.return_value.microsecond = 1620000000000 + 60_000_000  # +60s
    mock_datetime.side_effect = lambda *args, **kw: datetime(*args, **kw)
    
    recoder.timeout = 60_000_000  # 60秒
    recoder.append(sample_df)  # 包含时间戳 1620000000000 和 1620000060000
    
    # 执行修剪
    recoder.prune_expired_data()
    
    # 应该只保留较新的记录
    assert len(recoder) == 1
    assert recoder.data["timestamp"][0] == datetime(2021, 5, 1, 22, 0, tzinfo=timezone.utc)

# Test __getitem__
def test_getitem_by_time_range(recoder: DataFrameRecoder, sample_df: pl.DataFrame):
    recoder.append(sample_df)
    result = recoder[1620000000000:1620000060000]
    assert isinstance(result, pl.DataFrame)
    assert result.height == 2

def test_getitem_by_int_index(recoder, sample_df):
    recoder.append(sample_df)
    result = recoder[1620000060000]
    assert isinstance(result, pa.Table)
    assert result.num_rows == 1

def test_getitem_by_tuple(recoder, sample_df):
    recoder.append(sample_df)
    result = recoder[(1620000000000, 1620000060000)]
    assert result.num_rows == 2

# Test error handling
def test_invalid_column_count(recoder):
    df1 = pl.DataFrame({"a": [1, 2], "b": [3, 4]})
    df2 = pl.DataFrame({"a": [5, 6]})
    recoder.prepend(df1)
    with pytest.raises(ValueError):
        recoder.prepend(df2)

def test_empty_data(recoder: DataFrameRecoder):
    assert recoder.is_empty
    assert len(recoder) == 0