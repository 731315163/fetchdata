import pytest
import polars as pl
import pyarrow as pa
from tradepulse.data.serialize import serialize_dataframe, deserialize_dataframe
from datetime import datetime

def test_serialize_non_empty():
    """测试非空数据序列化"""
    df = pl.DataFrame({
        "timestamp": [datetime.now(), datetime.now()],
        "value": [100.0, 200.0]
    })
    serialized = serialize_dataframe(df)
    assert len(serialized) > 0
    assert deserialize_dataframe(serialized).equals(df)

def test_serialize_empty():
    """测试空数据序列化"""
    empty_df = pl.DataFrame()
    serialized = serialize_dataframe(empty_df)
    
    assert deserialize_dataframe(serialized).is_empty()

def test_serialize_arrow_table():
    """测试Arrow Table序列化"""
    df = pl.DataFrame({"a": [1, 2, 3]})
    table = df.to_arrow()
    serialized = serialize_dataframe(table)
    
    assert deserialize_dataframe(serialized).equals(df)

def test_serialize_invalid_type():
    """测试不支持的类型"""
    with pytest.raises(ValueError):
        serialize_dataframe("invalid_data")

def test_deserialize_invalid_format():
    """测试无效格式反序列化"""
    with pytest.raises(pl.exceptions.ComputeError):
        deserialize_dataframe(b"invalid_header_data")

def test_roundtrip_with_metadata():
    """测试带元数据的Roundtrip"""
    df = pl.DataFrame({
        "timestamp": [datetime.now(), datetime.now()],
        "value": [100.0, 200.0]
    }).with_columns(pl.col("timestamp").cast(pl.Datetime(time_unit="ms")))
    
    serialized = serialize_dataframe(df)
    deserialized = deserialize_dataframe(serialized)
    assert deserialized.equals(df)
    assert deserialized["timestamp"].dtype == pl.Datetime(time_unit="ms")