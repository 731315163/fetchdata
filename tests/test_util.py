import math
import pytest
from tradepulse.util import pre_date,next_date,timestamp_to_timestamp

from tradepulse.typenums import TimeStamp
from datetime import datetime, timedelta, timezone



def test_pre_date():
    # 示例1：向上取整到最近的整点
    interval = timedelta(hours=1)
    date = datetime(2023, 10, 1, 13, 45, 30)
    assert pre_date(interval, date)==  datetime(2023, 10, 1, 13)

  

    # 示例3：向上取整到最近的30分钟
    interval = timedelta(minutes=30)
    print(pre_date(interval, date))  # 输出: 2023-10-01 14:00:00

    # 示例4：向下取整到最近的30分钟
    print(next_date(interval, date))  # 输出: 2023-10-01 13:30:00


def test_next_date():
    # 示例1：向上取整到最近的整点
    interval = timedelta(hours=1)
    date = datetime(2023, 10, 1, 13, 45, 30)
    # assert pre_date(interval, date)==  datetime(2023, 10, 1, 13)

    # 示例2：向下取整到最近的整点
    assert next_date(interval, date)==datetime(2023, 10, 1, 14)

    # 示例3：向上取整到最近的30分钟
    interval = timedelta(minutes=30)
    print(pre_date(interval, date))  # 输出: 2023-10-01 14:00:00

    # 示例4：向下取整到最近的30分钟
    print(next_date(interval, date))  # 输出: 2023-10-01 13:30:00



def test_clamp():
    # 示例1：向上取整到最近的整点
    interval = timedelta(hours=1)
    date = datetime(2023, 10, 1, 13, 45, 30)
    assert pre_date(interval, date)==  datetime(2023, 10, 1, 13)

  

    # 示例3：向上取整到最近的30分钟
    interval = timedelta(minutes=30)
    print(pre_date(interval, date))  # 输出: 2023-10-01 14:00:00

    # 示例4：向下取整到最近的30分钟
    print(next_date(interval, date))  # 输出: 2023-10-01 13:30:00


    # 示例1：向上取整到最近的整点
    interval = timedelta(hours=1)
    date = datetime(2023, 10, 1, 13, 45, 30)
    # assert pre_date(interval, date)==  datetime(2023, 10, 1, 13)

    # 示例2：向下取整到最近的整点
    assert next_date(interval, date)==datetime(2023, 10, 1, 14)

    # 示例3：向上取整到最近的30分钟
    interval = timedelta(minutes=30)
    print(pre_date(interval, date))  # 输出: 2023-10-01 14:00:00

    # 示例4：向下取整到最近的30分钟
    print(next_date(interval, date))  # 输出: 2023-10-01 13:30:00


test_data = [
    # Negative timestamp
    (-1, "ms", ValueError,None),
    
    # Seconds to seconds
    (1_000_000_000, "s", 1_000_000_000, int),
    
    # Seconds to milliseconds
    (1_000_000_000, "ms", 1_000_000_000_000, int),
    
    # Milliseconds to seconds
    (1_000_000_000_000, "s", 1_000_000_000, int),
    
    # Milliseconds to milliseconds
    (1_000_000_000_000, "ms", 1_000_000_000_000, int),
    
    # Microseconds to seconds
    (1_000_000_000_000_000, "s", 1_000_000_000, int),
    
    # Microseconds to milliseconds
    (1_000_000_000_000_000, "ms", 1_000_000_000_000, int),
    
    # Nanoseconds to seconds
    (1_000_000_000_000_000_000, "s", 1_000_000_000, int),
    
    # Nanoseconds to milliseconds
    (1_000_000_000_000_000_000, "ms", 1_000_000_000_000, int),
    
    # Float input with integer result
    (1000.0, "s", 1000, int),
    (1000.0, "ms", 1000000, int),
    
    # Float input with fractional result
    (1500.5, "s", 1500.5, float),
    (1500.5, "ms", 1500500, int),
    
    # Boundary: Seconds (just below 1e10)
    (9999999999, "s", 9999999999, int),
    
    # Boundary: Milliseconds (exact 1e10)
    (10000000000, "s", 10000000, int),
    
    # Boundary: Microseconds (exact 1e14)
    (10**14, "s", 100000000, int),
    
    # Boundary: Nanoseconds (exact 1e17)
    (10**17, "s", 100000000, int),
]

@pytest.mark.parametrize(argnames="timestamp, export_unit, expected_result, expected_type", argvalues=test_data)
def test_timestamp_to_timestamp(timestamp, export_unit, expected_result, expected_type):
    """
    Test timestamp conversion logic for various input units and export targets.
    
    Args:
        timestamp: Input timestamp (seconds, milliseconds, microseconds, or nanoseconds).
        export_unit: Target unit ('s' for seconds, 'ms' for milliseconds).
        expected_result: Expected numeric result after conversion.
        expected_type: Expected type of the result (int or float).
    """
    if expected_result is ValueError:
        with pytest.raises(ValueError):
            timestamp_to_timestamp(timestamp, export_unit)
    else:
        result = timestamp_to_timestamp(timestamp, export_unit)
        assert isinstance(result, expected_type), f"Expected type {expected_type}, got {type(result)}"
        assert result == pytest.approx(expected_result), f"Expected {expected_result}, got {result}"





# 测试数据
timestamp_test_data = [
    # 有效输入
    (1717029200000, "ms", 1717029200000, TimeStamp),  # 毫秒输入
    (1717029200, "s", 1717029200, TimeStamp),        # 秒输入
    (datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc), "ms", 1717243200000, datetime),  # datetime输入
                     # None输入
    (-1, "ms", -1, ValueError),                       # 负数输入
]



comparison_test_data = [
    # 相等比较
    (1000, 1000, True),
    (1000, 2000, False),
    
    # 大小比较
    (1000, 2000, True),
    (2000, 1000, False),
    
    # 浮点数比较
    (1000.0, 1000, True),
    (1000.5, 1000, True),  # 使用近似比较
]

@pytest.fixture
def sample_timestamp():
    """创建一个示例TimeStamp对象用于测试"""
    return TimeStamp(1717029200000)

@pytest.mark.parametrize("input_val, export_unit, expected, input_type", timestamp_test_data)
def test_timestamp_initialization(input_val, export_unit, expected, input_type):
    """测试TimeStamp初始化逻辑"""
   
    if input_type == ValueError:
        with pytest.raises(ValueError):
            TimeStamp(input_val)
    else:
        if isinstance(expected, type) and issubclass(expected, Exception):
            with pytest.raises(expected):
                TimeStamp(input_val)
        else:
            ts = TimeStamp(input_val)
            assert isinstance(ts, TimeStamp)
            
            if export_unit == "ms":
                assert ts.ms == pytest.approx(expected)
            else:
                assert ts.s == pytest.approx(expected * 1000)

def test_s_property(sample_timestamp):
    """测试s属性（秒）"""
    assert sample_timestamp.s == sample_timestamp * 1000

def test_ms_property(sample_timestamp):
    """测试ms属性（毫秒）"""
    assert sample_timestamp.ms == sample_timestamp

def test_to_datetime_utc(sample_timestamp):
    """测试to_datetime_utc方法"""
    dt = sample_timestamp.to_datetime_utc()
    assert dt.tzinfo == timezone.utc
    assert dt.timestamp() * 1000 == pytest.approx(float(sample_timestamp))


clamp_test_data = [
        # 正间隔取整（向上取整）
        (1000, 1717029210500, 1717029211000),  # 精确匹配（base_time本身就是1000的倍数）
        (1000, 1717029215500, 1717029216000),  # 1500ms → 向上取整到2000ms
        
        # 负间隔取整（向下取整）
        (-1000, 1717029210000, 1717029210000),  # 精确匹配
        (-1000, 1717029211500, 1717029211000),  # 11500ms → 向下取整到11000ms
        
        # 边界情况
        (0, 1500000, 1500000),  # 无效间隔返回原始值
        (1000, -1, -1),       # 负时间戳返回原始值
    ]

@pytest.mark.parametrize("interval, dt, expected", clamp_test_data)
def test_clamp_method(interval,dt,expected):
    """测试clamp方法"""
    base_time = TimeStamp(dt)  # 2024-06-01 12:00:00 UTC
    
    delta = timedelta(milliseconds=interval)
        
    result = base_time.clamp(delta)  # 使用base_time作为时间戳
        
    assert result.ms == pytest.approx(expected)

@pytest.mark.parametrize("a,b, eq", [
    (1000,1000, True),
    (0, 0,True),
    (10001000,10000000, False)
 
])
def test_assertmathislose(a,b,eq):
    math.isclose(
            a,
            b,
            abs_tol=1e-9,
            rel_tol=1e-9
        )     


def test_invalid_comparisons():
    """测试无效的比较操作"""
    ts = TimeStamp(1000)
 
    with pytest.raises(TypeError):
        ts == [1000]
    with pytest.raises(TypeError):
        ts > {"value": 1000}

# 测试基本创建功能
def test_empty():
    # 测试空实例
    assert TimeStamp.empty() == TimeStamp.empty()
    assert TimeStamp.empty() is TimeStamp.empty()
    assert TimeStamp(None) == TimeStamp.empty()
    assert TimeStamp(TimeStamp._EMPTY_VALUE) is TimeStamp.empty()
    assert TimeStamp.empty().is_empty

    assert not TimeStamp(1000_1000_000).is_empty
    

# 测试不同输入类型的转换
def test_input_types():
    now = datetime.now()
    now_ts = now.timestamp() * 1000
    
    # 测试datetime输入
    ts = TimeStamp(now)
    assert ts.ms == pytest.approx(now_ts)
    
    # 测试float输入
    assert TimeStamp(1620000000000.0).ms == 1620000000000.0
    
    # 测试int输入
    assert TimeStamp(1620000000000).ms == 1620000000000
    
    # 测试现有TimeStamp实例
    original = TimeStamp(now)
    copy = TimeStamp(original)
    assert copy is not original  # 应该是新实例但值相同
    assert copy == original

@pytest.mark.parametrize("ts1, , ts2",[
    (TimeStamp(1620000000000), TimeStamp(1620000001000)),  # 1秒后
    ( TimeStamp(1000), TimeStamp(2000))
])
def test_comparisons(ts1,ts2):
    ts1_time = ts1.s  # 2021-05-01 12:00:00 UTC
    ts2_time = ts2.s
    
    
    # 等于比较
    assert ts1 == ts1
    assert ts1 == ts1_time
    assert ts1 != ts2_time
    
    
    assert ts2 == ts2_time
    
    
    assert ts1 != ts2
    
    # 小于比较
    assert ts1 < ts2
    assert not (ts2 < ts1)
    
    # 大于比较
    assert ts2 > ts1
    assert not (ts1 > ts2)
    
    # 小于等于和大于等于
    assert ts1 <= ts1
    assert ts1 <= ts2
    assert ts2 >= ts1

# 测试特殊值比较
def test_special_value_comparisons():
    empty = TimeStamp.empty()
    normal = TimeStamp(1620000000000)
    
    assert empty != normal
    assert not (empty < normal)
    assert empty > normal  # 因为无穷大特性

# 测试时间戳转换
def test_unit_conversions():
    # 测试秒到毫秒
    assert TimeStamp.timestamp_to_timestamp(1620000000, "ms") == 1620000000000
    # 测试微秒到毫秒
    assert TimeStamp.timestamp_to_timestamp(1620000000000000, "ms") == 1620000000000
    # 测试纳秒到秒
    assert TimeStamp.timestamp_to_timestamp(1620000000000000000, "s") == 1620000000

# 测试clamp方法
def test_clamp():
    base_time = TimeStamp(1620000000000)  # 2021-05-01 12:00:00 UTC
    
    # 1分钟间隔
    interval = timedelta(minutes=1)
    clamped = base_time.clamp(interval)
    assert clamped == base_time  # 已经是整分
    
    # 测试向上取整
    half_min = base_time + 30000  # 30秒后
    clamped =  half_min.clamp(interval)
    assert clamped > half_min
    
    # 测试向下取整
    negative_interval = timedelta(minutes=-1)
    clamped = half_min.clamp(negative_interval)
    assert clamped < half_min

# 测试datetime转换
def test_datetime_conversion():
    now = datetime.now()
    ts = TimeStamp(now)
    converted = ts.to_datetime()
    assert abs((converted - now).total_seconds()) < 1e-6  # 允许微小误差

    utc_now = datetime.now(timezone.utc)
    ts_utc = TimeStamp(utc_now)
    converted_utc = ts_utc.to_datetime_utc()
    assert abs((converted_utc - utc_now).total_seconds()) < 1e-6

# 测试错误情况
def test_error_cases():
    with pytest.raises(ValueError):
        TimeStamp(-1)  # 负数时间戳
    
    # 不支持的类型
    with pytest.raises(TypeError):
        TimeStamp(["not", "a", "timestamp"])