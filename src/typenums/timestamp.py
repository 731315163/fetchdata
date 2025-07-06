





import math
from datetime import datetime, timedelta, timezone
from types import UnionType
from typing import Any, Literal, Self, Union


class TimeStamp(float):
    """
    TimeStamp class
    """

     
    # 定义特殊的空时间戳值，使用不可能的时间戳
    _EMPTY_VALUE = float('inf')
    
    # 私有类变量存储空实例
    _empty_instance = None
    
    @classmethod
    def get_empty(cls) -> 'TimeStamp':
        """获取空时间戳实例（单例）"""
        if cls._empty_instance is None:
            cls._empty_instance = super().__new__(cls, cls._EMPTY_VALUE)
        return cls._empty_instance
    
    # 使用类属性替代property
    @property
    def empty(cls) -> 'TimeStamp':
        return cls.get_empty()
    
    @property
    def is_empty(self) -> bool:
        """检查是否为空时间戳"""
        return self ==TimeStamp.empty
    def __new__(cls, timestamp:  float| int|datetime):
        # Convert different input types to timestamp in milliseconds
      
      
        if timestamp is None or timestamp == cls._EMPTY_VALUE:
            return cls.get_empty()
        if isinstance(timestamp, TimeStamp):
            ms = timestamp.ms
        else:
            if isinstance(timestamp, datetime):
                ts = timestamp.timestamp() * 1000  # Convert to milliseconds
            elif isinstance(timestamp, (float,int)):
                ts = timestamp
            ms = cls.timestamp_to_timestamp(ts, unit="ms")
        return super().__new__(cls, ms)
       
        
    @property
    def s(self):
        '''
        返回秒
        '''
        return self / 1000.0 # 因为self是毫秒，所以秒是除以1000

    @property
    def ms(self):
        '''
        返回毫秒
        '''
        return float(self)

    @staticmethod
    def timestamp_to_timestamp( timestamp: float | int, unit: Literal["ms", "s"] = "ms") -> float | int:
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
        if unit == "ms":
            result = seconds * 1000
        else:
            result = seconds

        # Return integer if result has no fractional part
        if isinstance(result, float) and result.is_integer():
            return int(result)
        return result

    def clamp(self, delta: timedelta) -> 'TimeStamp':
        """
        根据给定时间间隔为正数向上取整数值 ，为负数向下取整以秒为单位
        
        Args:
            interval: 取整间隔（不为0）
            value: 要处理的数值，默认为
        
        Returns:
            向上取整后的数值（interval的整数倍）
        """
        interval = delta.total_seconds()*1000
        timestamp = self 
        if interval == 0:
            return timestamp  # 无效间隔直接返回原值
        if interval > 0:
            return TimeStamp( math.ceil(timestamp / interval) * interval)
        # 计算倍数并向上取整
        else:
            interval = abs(interval)
            return TimeStamp( timestamp=math.floor(timestamp / interval) *interval)

    
    def __str__(self) -> str:
        if self.is_empty:
            return "TimeStamp.empty"
        dt = self.to_datetime()
        return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    def to_datetime(self) -> datetime:
        """Convert timestamp to datetime object"""
        return datetime.fromtimestamp(self.s )
    def to_datetime_utc(self) -> datetime:
        """Convert timestamp to datetime object"""
        return datetime.fromtimestamp(self.s ,tz=timezone.utc)
    # def to_seconds(self) -> float:
    #     """Return timestamp in seconds"""
    #     return self.ms / 1000

    def  _eq_(self, ms):
     
        return math.isclose(
            self.ms,
            ms,
            abs_tol=1e-9,
            rel_tol=1e-9
        )

    def _get_ms(self,other)->float:
        if isinstance(other, TimeStamp):
            ms = other.ms
        elif isinstance(other, (int, float)):
            ms = TimeStamp.timestamp_to_timestamp( timestamp=other,unit="ms")
        else:
            ms=float('inf')
        return ms

    def __eq__(self, other):
      
        ms = self._get_ms(other)
        return self._eq_(ms)

    def __lt__(self, other):

        ms = self._get_ms(other)
        if ms == float('inf'):
            return NotImplemented    
        if not self._eq_(ms) and self.ms < ms:
            return True
        return False
        

    def __ge__(self, other):
        
        ms = self._get_ms(other)
        if ms == float('inf'):
            return NotImplemented
        if self._eq_(ms) or self.ms > ms:
            return True
        return False

    def __ne__(self, other: Any) -> bool:
        return not self.__eq__(other)

    

    def __gt__(self, other: Any) -> bool:
        return not (self.__lt__(other) or self.__eq__(other))

    def __le__(self, other: Any) -> bool:
        return self.__lt__(other) or self.__eq__(other)


    def __repr__(self) -> str:
        return f"TimeStamp({self.ms}, unit='ms')"
        # 算术运算方法
    def __add__(self, other:  timedelta| int| float) -> 'TimeStamp':
        """加法运算：TimeStamp + other"""
        if self.is_empty:
            return self
            
        if isinstance(other, TimeStamp):
            raise TypeError("不能将两个时间戳相加，时间戳相加没有实际意义")
            
        if isinstance(other, timedelta):
            # 加上timedelta，返回新的TimeStamp
            new_ms = self + other.total_seconds() * 1000
            return TimeStamp(new_ms)
            
        if isinstance(other, (int, float)):
            # 加上数值（表示毫秒）
            new_ms = self.ms + other
            return TimeStamp(new_ms)
            
        return NotImplemented
    
    def __radd__(self, other: timedelta| int| float) -> 'TimeStamp':
        """反向加法：other + TimeStamp"""
        return self.__add__(other)
    
    def __sub__(self, other: Union[timedelta, int, float]) -> 'TimeStamp':
        """减法运算：TimeStamp - other"""
        if self.is_empty:
            return self
            
        # if isinstance(other, TimeStamp):
        #     # 两个时间戳相减，返回timedelta
        #     diff_ms = self.ms - other.ms
        #     return timedelta(milliseconds=diff_ms)
            
        if isinstance(other, timedelta):
            # 减去timedelta，返回新的TimeStamp
            new_ms = self.s - other.total_seconds() 
            return TimeStamp(new_ms)
            
        if isinstance(other, (int, float)):
            # 减去数值（表示毫秒）
            new_ms = self.ms - other
            return TimeStamp(new_ms)
            
        return NotImplemented
    
    # def __rsub__(self, other: Any) -> 'TimeStamp':
    #     """反向减法：other - TimeStamp"""
    #     if isinstance(other, TimeStamp):
    #         # 两个时间戳相减，返回timedelta
    #         diff_ms = other._timestamp_ms - self._timestamp_ms
    #         return timedelta(milliseconds=diff_ms)
            
    #     if isinstance(other, (int, float)):
    #         # 数值减去时间戳，返回新的TimeStamp
    #         new_ms = other - self._timestamp_ms
    #         return TimeStamp(new_ms, unit="ms")
            
    #     return NotImplemented
    
    def __mul__(self, other: Union[int, float]) -> 'TimeStamp':
        """乘法运算：TimeStamp * 数值"""
        if self.is_empty:
            return self
            
        if isinstance(other, (int, float)):
            new_ms = self.ms * other
            return TimeStamp(new_ms)
            
        return NotImplemented
    
    def __rmul__(self, other: Union[int, float]) -> 'TimeStamp':
        """反向乘法：数值 * TimeStamp"""
        return self.__mul__(other)
    
    # def __truediv__(self, other: Union[int, float]) -> 'TimeStamp':
    #     """除法运算：TimeStamp / 数值"""
    #     if self.is_empty:
    #         return self
            
    #     if isinstance(other, (int, float)):
    #         if other == 0:
    #             raise ZeroDivisionError("时间戳不能除以零")
    #         new_ms = self._timestamp_ms / other
    #         return TimeStamp(new_ms, unit="ms")
            
    #     return NotImplemented
    
  
    

    

