
from abc import ABC, abstractmethod
from collections import namedtuple
from datetime import datetime, timedelta

import pyarrow as pa

from typenums import DataType, MarketType, State, TimeStamp

DataKey = namedtuple('DataKey', ['pair', 'timeframe', 'marketType', 'datatype'])

class DataRecoder[T](ABC):
    def __init__(self,pair:str,*,marketType:MarketType,datatype:DataType, timeframe="",timeout_ms=timedelta(minutes=60)):
        self.pair:str= pair
        self.timeframe: str =timeframe
        self.marketType:MarketType = marketType
        self.datatype: DataType =datatype
        self.first_datetime :TimeStamp=-1
        self.last_datetime :TimeStamp=-1
        # self.timekey =''
        self.timeout:float = timeout_ms.microseconds
        self.state:State = State.RUNNING
       
    @property
    def key (self):
            key = DataKey(self.pair,self.timeframe,self.marketType,self.datatype)
            return key
    @property
    @abstractmethod
    def is_empty(self)->bool:...

    @property
    @abstractmethod
    def empty(self)->T:...
    @staticmethod
    @abstractmethod
    def Empty()->T:...
    @abstractmethod
    def append(self, data:T,dt:datetime|int|None=None):...
    @abstractmethod
    def prepend(self,data:T,dt:datetime|int|None=None):...
    # def is_timeout(self,last_time=None):
    #     last_time = last_time if last_time else self.last_update
    #     return  last_time - self.first_update > self.timeout
    @abstractmethod
    def prune_expired_data(self,td:timedelta|int|None = None):
        """删除超时的数据"""
        ...
    @abstractmethod
    def __getitem__(self, index)->T:
        """支持索引访问和切片"""
        ...
    @abstractmethod  
    def __len__(self) -> int:...
    # @abstractmethod
    # def window(self):...
    # # def del_timeout_data(self,dt:datetime|None=None):
    # #     timeout = self.last_update - self.timeout 
    # #     self.data = self.data.select(pl.col(self.timekey) > timeout)