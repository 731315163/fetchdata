import asyncio
import logging
import time
from ccxt import abstract
from coverage import data
from polars import DataFrame
from datetime import datetime, timedelta, timezone
import ccxt.async_support as ccxt_async
import polars as pl
from typenums import MarketType
from .protocol import CacheProtocol

class DataRecoder[T]():
    def __init__(self,data:T|None=None,  timeout:timedelta=timedelta(minutes=10)):
        super().__init__()
        self.pair:str= ""
        self.timeframe =""
        self.type:MarketType = "spot"
        self.key = "_".join([self.pair,self.timeframe,self.type])
        self.timekey =''
        self.timeout:timedelta = timeout
        self.data  :T|None= data
        self.first_update = datetime.now(timezone.utc)
        self.last_update = datetime.now(timezone.utc)
    def append(self, data:T,dt:datetime|None=None):
        if dt is None:
            try:
                dt = data.select(pl.col(self.timekey).max())
            except Exception as e:
                dt = datetime.now(timezone.utc)
        self.last_update = dt
        self.data= self.data.vstack(data)
    def is_timeout(self,last_time=None):
        last_time = last_time if last_time else self.last_update
        return  last_time - self.first_update > self.timeout
    def del_timeout_data(self,dt:datetime|None=None):
        timeout = self.last_update - self.timeout 
        self.data = self.data.select(pl.col(self.timekey) > timeout)
        

class DataCache:
    def __init__(self, exchange_id='',exhcange_name=''):
        """
        初始化加密货币数据缓存
        
        参数:
            exchange_id: 
            cache_time_minutes: 数据缓存的时间（分钟），默认为60分钟
        """
        self.id = exchange_id
        self.name = exhcange_name
        self.cache : dict[str,DataRecoder]={}
        # self.ticks_cache:dict[str,DataRecoder] = {}
        # self.trades_cache:dict[str,DataRecoder] = {}
        # self.ohlcv_cache:dict[str,DataRecoder] = {}
        # self.orders_cache:dict[str,DataRecoder] = {}
        self.logger = logging.getLogger(name=str(__name__))
        self.lock = asyncio.Lock()
        # self.ohlcv_lock = asyncio.Lock()
        # self.tickers_lock = asyncio.Lock()
        # self.trades_lock = asyncio.Lock()
    def set_cache_time(self,key :str,timedeta: timedelta):
        if key in self.cache:
            self.cache[key].timeout = timedeta
        else:
            raise KeyError(f"{key} not found in cache")



class CacheManager():

    data_docker:dict[str,CacheProtocol]= {}
    default_cache = DataCache
    
    @classmethod
    def set(cls,exchange_id:str,cache:CacheProtocol):
        cls.data_docker[exchange_id] = cache
    @classmethod
    def get(cls,exchange_id:str) -> CacheProtocol:
        if exchange_id not in cls.data_docker: 
            cls.data_docker[exchange_id] = cls.default_cache(exchange_id)
        return cls.data_docker[exchange_id]
   

