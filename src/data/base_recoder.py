
from datetime import datetime, timedelta, timezone
import logging
from polars import DataFrame
from data.protocol import DataRecoder

import pyarrow as arrow
from collections import OrderedDict
from typenums import MarketType,DataType



class BaseDataRecoder(DataRecoder[list[list]]):
    def __init__(self, pair: str, marketType: MarketType, datatype: DataType, timeframe="", timeout: timedelta = timedelta(minutes=10)):
         
        self.first_datetime =0
        self.last_datetime = 0
        self.lazyfront = []
        self.rear = []
        super().__init__(pair=pair, marketType=marketType, datatype=datatype, timeframe=timeframe)
        self.timeout = timeout
        self.timekey = "timestamp"



    def normalize_datetime(self, dt: datetime | int | float | None) -> datetime | None:
        """将不同类型的时间表示转换为datetime对象"""
        if dt is None:
            return None
        if isinstance(dt, (int, float)):
            # 根据时间戳长度判断单位
            timestamp = float(dt)
            if timestamp > 1e15:  # 纳秒 (16-19位)
                timestamp /= 1e9
            elif timestamp > 1e12:  # 微秒 (13-16位)
                timestamp /= 1e6
            elif timestamp > 1e9:   # 毫秒 (10-13位)
                timestamp /= 1000
            # 秒级时间戳 (9-10位)
            return datetime.fromtimestamp(timestamp, tz=timezone.utc)
        # 确保datetime对象有时区信息
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt

    def append(self, data:list[list], dt: datetime | int | float | None = None):
     
        self.rear.extend(data)
        self.last_datetime = self.normalize_datetime(dt)
     
           
        

     

    def prepend(self, data:list[list], dt: datetime | int | float | None = None):
        """向数据记录器前置数据"""
        self.lazyfront.append(data)
        self.data = data
       


    def prune_expired_data(self, td: timedelta | int | float | None = None):
        """删除超时的数据"""
        ...

    def __getitem__(self, index):
        
        """支持索引访问和切片"""
        ...
      
    def __len__(self) -> int:
        return len(self.data)