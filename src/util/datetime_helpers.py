import math
import re
from datetime import datetime, timedelta, timezone
from time import time
from typing import Literal

from typenums.constants import DATETIME_PRINT_FORMAT


def dt_now() -> datetime:
    """Return the current datetime in UTC."""
    return datetime.now(timezone.utc)


def dt_utc(
    year: int,
    month: int,
    day: int,
    hour: int = 0,
    minute: int = 0,
    second: int = 0,
    microsecond: int = 0,
) -> datetime:
    """Return a datetime in UTC."""
    return datetime(year, month, day, hour, minute, second, microsecond, tzinfo=timezone.utc)


def dt_ts(dt: datetime | None = None) -> int:
    """
    Return dt in ms as a timestamp in UTC.
    If dt is None, return the current datetime in UTC.
    """
    if dt:
        return int(dt.timestamp() * 1000)
    return int(time() * 1000)


def dt_ts_def(dt: datetime | None, default: int = 0) -> int:
    """
    Return dt in ms as a timestamp in UTC.
    If dt is None, return the given default.
    """
    if dt:
        return int(dt.timestamp() * 1000)
    return default


def dt_ts_none(dt: datetime | None) -> int | None:
    """
    Return dt in ms as a timestamp in UTC.
    If dt is None, return the given default.
    """
    if dt:
        return int(dt.timestamp() * 1000)
    return None


def dt_floor_day(dt: datetime) -> datetime:
    """Return the floor of the day for the given datetime."""
    return dt.replace(hour=0, minute=0, second=0, microsecond=0)


def dt_from_ts(timestamp: float) -> datetime:
    """
    Return a datetime from a timestamp.
    :param timestamp: timestamp in seconds or milliseconds
    """
    if timestamp > 1e10:
        # Timezone in ms - convert to seconds
        timestamp /= 1000
    return datetime.fromtimestamp(timestamp, tz=timezone.utc)


def shorten_date(_date: str) -> str:
    """
    Trim the date so it fits on small screens
    """
    new_date = re.sub("seconds?", "sec", _date)
    new_date = re.sub("minutes?", "min", new_date)
    new_date = re.sub("hours?", "h", new_date)
    new_date = re.sub("days?", "d", new_date)
    new_date = re.sub("^an?", "1", new_date)
    return new_date




def format_date(date: datetime | None) -> str:
    """
    Return a formatted date string.
    Returns an empty string if date is None.
    :param date: datetime to format
    """
    if date:
        return date.strftime(DATETIME_PRINT_FORMAT)
    return ""


def format_ms_time(date: int | float) -> str:
    """
    convert MS date to readable format.
    : epoch-string in ms
    """
    return dt_from_ts(date).strftime("%Y-%m-%dT%H:%M:%S")


def format_ms_time_det(date: int | float) -> str:
    """
    convert MS date to readable format - detailed.
    : epoch-string in ms
    """
    # return dt_from_ts(date).isoformat(timespec="milliseconds")
    return dt_from_ts(date).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]





def next_date(interval: timedelta, date: datetime = datetime.now(tz=timezone.utc)) -> datetime:
    """
    根据给定的时间间隔向上取整日期时间
    
    Args:
        interval: 时间间隔
        date: 要处理的日期时间，默认为当前时间
    
    Returns:
        向上取整后的日期时间
    """
  
    # 计算时间间隔的总秒数
    seconds = interval.total_seconds() 
    if seconds == 0:
        return date
    
    # 获取基准时间（通常为0时刻）
    base_time = datetime(date.year, date.month, date.day,tzinfo=date.tzinfo)
    
    # 计算当前时间与基准时间的差值（秒）
    delta_seconds = (date - base_time).total_seconds()
    
    # 向上取整
    quotient = (delta_seconds + seconds - 1) // seconds
    new_seconds = quotient * seconds
    
    # 返回结果
    return base_time + timedelta(seconds=new_seconds)

def pre_date(interval: timedelta, date: datetime = datetime.now(tz=timezone.utc)) -> datetime:
    """
    根据给定的时间间隔向下取整日期时间
    
    Args:
        interval: 时间间隔
        date: 要处理的日期时间，默认为当前时间
    
    Returns:
        向下取整后的日期时间
    """
    
    # 计算时间间隔的总秒数
    seconds = interval.total_seconds()
    if seconds == 0:
        return date
    
    # 获取基准时间（通常为0时刻）
    
    base_time = datetime(year=date.year, month=date.month, day=date.day,tzinfo=date.tzinfo)
    
    # 计算当前时间与基准时间的差值（秒）
    delta_seconds = (date - base_time).total_seconds()
    
    # 向下取整
    quotient = delta_seconds // seconds
    new_seconds = quotient * seconds
    
    # 返回结果
    return base_time + timedelta(seconds=new_seconds)






def timestamp_to_timestamp(timestamp: float | int, export_unit: Literal["ms", "s"] = "ms") -> float | int:
    """
    将任意单位的时间戳统一转换为指定单位（秒或毫秒）
    
    Args:
        timestamp: 输入时间戳（可能是秒/毫秒/微秒/纳秒）
        export_unit: 输出单位，'s'表示秒，'ms'表示毫秒（默认）
    
    Returns:
        转换到目标单位的数值（整数或浮点数）
        
    单位识别逻辑：
        < 1e10   : 秒级（当前时间戳约1.7e9）
        < 1e14   : 毫秒级（当前约1.7e12）
        < 1e17   : 微秒级（当前约1.7e15）
        >= 1e17  : 纳秒级（当前约1.7e18）
    """
    # Handle negative timestamps
    if timestamp < 0:
        raise ValueError("Negative timestamps are not supported.")
    
    # Use integer thresholds to avoid float precision issues
    if timestamp < 10**10:  # 秒级时间戳
        seconds = timestamp
    elif timestamp < 10**14:  # 毫秒级时间戳
        seconds = timestamp / 1000
    elif timestamp < 10**17:  # 微秒级时间戳
        seconds = timestamp / 1_000_000
    else:  # 纳秒级时间戳
        seconds = timestamp / 1_000_000_000

    # Convert to target unit
    if export_unit == "ms":
        result = seconds * 1000
    else:
        result = seconds

    # Return integer if result has no fractional part
    if isinstance(result, float) and result.is_integer():
        return int(result)
    return result


def clamp(interval: float|int, dt: float|int  = datetime.now().timestamp()) -> float:
    """
    根据给定时间间隔为正数向上取整数值 ，为负数向下取整以秒为单位
    
    Args:
        interval: 取整间隔（不为0）
        value: 要处理的数值，默认为
    
    Returns:
        向上取整后的数值（interval的整数倍）
    """
   
    if interval == 0 or dt < 0:
        return dt  # 无效间隔直接返回原值
    if interval > 0:
        return math.ceil(dt / interval) * interval
    # 计算倍数并向上取整
    else:
        interval = abs(interval)
        return math.floor(dt / interval) *interval

