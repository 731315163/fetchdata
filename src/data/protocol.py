
from abc import ABC, abstractmethod
from collections import namedtuple
from datetime import datetime, timedelta, timezone
import time
from typing import Literal
import logging
from typenums import MarketType,TRADES_SCHEME,CANDLES_SCHEME,DataType,State

DataKey = namedtuple('DataKey', ['pair', 'timeframe', 'marketType', 'datatype'])

class DataRecoder[T](ABC):
    def __init__(self,pair:str,*,marketType:MarketType,datatype:DataType, timeframe="",timeout_ms=timedelta(minutes=60)):
        self.pair:str= pair
        self.timeframe: str =timeframe
        self.marketType:MarketType = marketType
        self.datatype: DataType =datatype
        self.first_datetime :int= -1
        self.last_datetime :int =-1
        # self.timekey =''
        self.timeout:int = timeout_ms.microseconds
        self.state:State = State.RUNNING
       
    @property
    def key (self):
            key = DataKey(self.pair,self.timeframe,self.marketType,self.datatype)
            return key
  
    

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