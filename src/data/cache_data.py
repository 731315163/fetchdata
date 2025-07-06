from abc import ABC, abstractmethod
import asyncio
import logging
import time
from typing import Literal, TypeVar, overload
from ccxt import abstract
from polars import DataFrame
from datetime import datetime, timedelta, timezone
import ccxt.async_support as ccxt_async
import polars as pl
import data
from typenums import CandleType, MarketType,TimeStamp
from .protocol import DataRecoder,DataType,TRADES_SCHEME,CANDLES_SCHEME,DataKey
from .dataframe_recoder import DataFrameRecoder

        

class DataCache[T](dict[DataKey,DataRecoder[T]]):
    def __init__(self, exchange_id='',exchange_name=''):
        """
        
        参数:
            exchange_id: 
            cache_time_minutes: 数据缓存的时间（分钟），默认为60分钟
        """
        super().__init__()
        self.id = exchange_id
        self.name = exchange_name
        # self.cache : dict[tuple,DataRecoder[T]]={}
        # self.ticks_cache:dict[str,DataRecoder] = {}
        # self.trades_cache:dict[str,DataRecoder] = {}
        # self.ohlcv_cache:dict[str,DataRecoder] = {}
        # self.orders_cache:dict[str,DataRecoder] = {}
      
        self.lock = asyncio.Lock()
        # self.ohlcv_lock = asyncio.Lock()
        # self.tickers_lock = asyncio.Lock()
        # self.trades_lock = asyncio.Lock()
    def set_cache_time(self,key ,timedeta: timedelta):
        if key in self:
            self[key].timeout = timedeta.total_seconds()
        else:
            raise KeyError(f"{key} not found in cache")

    # def getkey(self,pair,*,timeframe,marketType,datatype:DataType):
    #     return pair, timeframe, marketType, datatype
    
    @overload
    def get_recoder(self, pair: str, *, timeframe: str, marketType: MarketType, datatype: DataType) -> DataRecoder:
       ...

    @overload
    def get_recoder(self, *, key: DataKey) -> DataRecoder:
        ...

    @abstractmethod
    def get_recoder(self, pair=None, *, timeframe=None, marketType=None, datatype=None, key=None) -> DataRecoder:
        ...
    # @abstractmethod
    # def get_daterecator(self,pair,*,timeframe,marketType,datatype)->DataRecoder:...
    
    # def get_daterecator_by_key(self,key:tuple)->DataRecoder:
    #     pair,timeframe,marketType,datatype = key
    #     return self.get_daterecator(pair,timeframe=timeframe,marketType=marketType,datatype=datatype)
    
    
    @abstractmethod
    def time_range(self,data:T)->tuple[int,int]:...

    @abstractmethod
    def convert(self,data,datatype)->T:...
    
    def append(self,key:DataKey, data,dt:datetime|int|None=None):
        pair,timeframe,marketType,datatype = key
        convert_data = self.convert(data,datatype=datatype)
        self.get_recoder(key = key).append(convert_data,dt)
        
    def prepend(self,key:DataKey,data,dt:datetime|int|None=None):
        pair,timeframe,marketType,datatype = key
        convert_data = self.convert(data,datatype)
        self.get_recoder(key= key).prepend(convert_data,dt)
    
    def prune_expired_data(self,key:DataKey,td:timedelta|int|None = None):
        """删除超时的数据"""
        
        pair,timeframe,marketType,datatype = key
        self.get_recoder(pair,timeframe=timeframe,marketType=marketType,datatype=datatype).prune_expired_data(td)

class DataframeCache(DataCache[DataFrame]):
    def get_recoder(self, pair=None, *, timeframe=None, marketType=None, datatype=None, key=None) -> DataRecoder:
        if key is None:
            key = DataKey(pair, timeframe, marketType, datatype)
        
        cache = self.get(key,None)
        if cache is None:
            cache = DataFrameRecoder(pair=key.pair,timeframe=key.timeframe,marketType=key.marketType,datatype=key.datatype)
            self[key] = cache
        return cache
       
    def convert(self,data,datatype:DataType)->DataFrame:
        if isinstance(data,DataFrame): return data
        match datatype:
            case "trades":
                schema  = TRADES_SCHEME
            case _:
                schema = CANDLES_SCHEME
        
        df= DataFrame(data,schema=schema)
        return df

    def time_range(self,data:DataFrame)->tuple[int,int]:
        first  = data[0,0]
        last = data[-1,0]
        return first,last


class CacheFactory():

    data_docker:dict[str,DataCache]= {}
    default_cache = DataframeCache
    
    @classmethod
    def set(cls,exchange_id:str,cache:DataCache):
        cls.data_docker[exchange_id] = cache
    @classmethod
    def get(cls,exchange_id:str) -> DataCache:
        if exchange_id not in cls.data_docker: 
            cls.data_docker[exchange_id] = cls.default_cache(exchange_id)
        return cls.data_docker[exchange_id]
   

