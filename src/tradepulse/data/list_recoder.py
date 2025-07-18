

from datetime import datetime, timedelta, timezone
from typing import List, Any
from  tradepulse. data.protocol import DataRecoder
from tradepulse. typenums import DataType, MarketType
from bisect import bisect_left
class ListDataRecoder(DataRecoder[list]):
    def __init__(self, pair: str, marketType: MarketType, datatype: DataType, timeframe="", 
                data: List[list] | None = None, timeout: timedelta = timedelta(minutes=10)):
        self.data = [] if data is None else data
        super().__init__(pair=pair, marketType=marketType, datatype=datatype, 
                        timeframe=timeframe, timeout_ms=timeout)
        self.timekey = "timestamp"  # 保留接口一致性
        self.timestamp=[]
    @staticmethod
    def Empty() -> list:
        return []
    
    @property
    def empty(self) -> list:
        return ListDataRecoder.Empty()
    
    @property
    def rawdata(self) -> list:
        return self.data
    
    @property
    def is_empty(self) -> bool:
        return len(self.data) == 0
    
    def append(self, data: List[list], dt: datetime | int | float | None = None):
        if not data:
            return
            
        # 如果提供了时间，则更新时间列（需要自定义逻辑）
        if dt is not None:
            # 添加时间处理逻辑（示例：假设时间戳在索引0位置）
            for item in data:
                if isinstance(item, list):
                    if isinstance(dt, datetime):
                        item[0] = int(dt.timestamp() * 1000)  # 替换为实际索引
                    else:
                        item[0] = int(dt)
        
        self.data.extend(data)
    
    def prepend(self, data: List[list], dt: datetime | int | float | None = None):
        if not data:
            return
            
        # 如果提供了时间，则更新时间列
        if dt is not None:
            # 添加时间处理逻辑（示例：假设时间戳在索引0位置）
            for item in data:
                if isinstance(item, list):
                    if isinstance(dt, datetime):
                        item[0] = int(dt.timestamp() * 1000)  # 替换为实际索引
                    else:
                        item[0] = int(dt)
                    
        self.data = data + self.data
    
# ... existing code ...
    def prune_expired_data(self,td:timedelta|int|None=None):
        """删除超时的数据（保留时间戳 >= cutoff 的项）
        
        假设：
        - 数据已按时间戳升序排列
        - item[0] 存储的是毫秒级时间戳
        - self.timeout 是正确定义的timedelta对象
        """
        if self.is_empty:
            return
            
        # 统一使用毫秒时间单位
        current_time = datetime.now(timezone.utc)  # 转换为毫秒
        cutoff = int((current_time - self.timeout).timestamp()*1000 )  # 使用total_seconds()确保正确计算
        
        # 使用二分查找找到第一个不小于cutoff的时间戳位置
        try:
            # 提取时间戳用于二分查找
            timestamps = [item[0] for item in self.data if isinstance(item, (list, tuple)) and len(item) > 0]
            
            # 使用bisect_left找到第一个>=cutoff的位置
            
            index = bisect_left(timestamps, cutoff)
            
            # 保留有效数据
            self.data = self.data[index:]
        except Exception as e:
            # 记录异常并重新抛出带有上下文信息的异常
            raise RuntimeError("数据清理过程中发生错误") from e
# ... existing code ...
    
    def __getitem__(self, index) -> list:
        """支持索引访问和切片"""
        if isinstance(index, slice):
            start = index.start
            end = index.stop
            return [item for item in self.data 
                   if (start is None or item[0] >= start) and  # 根据实际索引调整
                   (end is None or item[0] <= end)]
        
        elif isinstance(index, (int, float)):
            # 按时间戳查找（示例实现）
            return [item for item in self.data 
                   if item[0] == index]  # 根据实际索引调整
            
        return []
    
    def __len__(self) -> int:
        return len(self.data)