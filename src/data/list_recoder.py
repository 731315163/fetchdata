

from datetime import datetime, timedelta, timezone
from typing import List, Any
from data.protocol import DataRecoder
from typenums import DataType, MarketType

class ListDataRecoder(DataRecoder[list]):
    @staticmethod
    def Empty() -> list:
        return []
    
    @property
    def empty(self) -> list:
        return ListDataRecoder.Empty()
    
    def __init__(self, pair: str, marketType: MarketType, datatype: DataType, timeframe="", 
                data: List[list] | None = None, timeout: timedelta = timedelta(minutes=10)):
        self.data = [] if data is None else data
        super().__init__(pair=pair, marketType=marketType, datatype=datatype, 
                        timeframe=timeframe, timeout_ms=timeout)
        self.timekey = "timestamp"  # 保留接口一致性
    
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
                    item[0] = int(dt.timestamp() * 1000)  # 替换为实际索引
        
        self.data.extend(data)
    
    def prepend(self, data: List[list], dt: datetime | int | float | None = None):
        if not data:
            return
            
        # 如果提供了时间，则更新时间列
        if dt is not None:
            # 添加时间处理逻辑（示例：假设时间戳在索引0位置）
            for item in data:
                if isinstance(item, list):
                    item[0] = int(dt.timestamp() * 1000)  # 替换为实际索引
                    
        self.data = data + self.data
    
    def prune_expired_data(self, td: timedelta | int | None = None):
        """删除超时的数据"""
        if self.is_empty:
            return
            
        # 计算截止时间（微秒）
        cutoff = int((datetime.now(timezone.utc).timestamp() - self.timeout / 1e6) * 1e6)
        
        # 假设每个列表项的第一个元素是时间戳（毫秒）
        self.data = [item for item in self.data 
                    if item[0] >= cutoff]  # 根据实际索引调整
    
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