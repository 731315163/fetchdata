import asyncio
import logging
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from typing import Any


from data import CacheFactory, DataKey
from exceptions import *
from typenums import MarketType, State
from typenums.Literal import TimeFrame
from util import timeframe_to_msecs

from .ccxtexchange_factory import CCXTExchangeFactory
from .protocol import CCXTExchangeProtocol
from .protocol import ExchangeProtocol as EX

logger = logging.getLogger(__name__)



class Exchange(EX):

    def __init__(self, exchange_name:str="", config:dict={}) :
        # self.lock = asyncio.Lock()
        self.exchange_name = exchange_name
        if exchange_name.strip() != "":
            config["name"] = exchange_name
        self.config = config
        self.exchange: CCXTExchangeProtocol = CCXTExchangeFactory.get_exchange_instance(config=self.config)
        self.windows = {str,timedelta}
        # self.since_lock = asyncio.Lock()
        self.since:dict[DataKey,int] = {}
        # self.until_lock = asyncio.Lock()
        self.until: dict[DataKey, int] = {}
        # self.cache_lock= asyncio.Lock()
        self.cache = CacheFactory.get(exchange_id=self.exchange.name)
        self.data_internal_ratio = 10
        self. rateLimit=self.exchange.rateLimit
    
        
    def set_since(self,key:DataKey,since:int) -> None:
         if key not in self.since or self.since[key] > since:
             self.since[key] = since
    def set_until(self,key:DataKey,until:int) -> None:
         if key not in self.until or self.until[key] < until:
             self.until[key] = until
  
   

    def un_watch_trades(self,pair:str,until:int,marketType: MarketType = "future" ) -> None:
         key = DataKey(pair=pair,timeframe="",marketType=marketType,datatype="trades")
         self.set_until(key,until)

    def un_watch_ohlcv(self,pair:str,timeframe:TimeFrame,until:int,marketType: MarketType = "future") -> None:
        key = DataKey(pair=pair,timeframe=timeframe,marketType=marketType,datatype="ohlcv")
        self.set_until(key,until)
    async def update(self) -> None:
        """后台任务，持续更新最新的交易数据"""
        while True:
            try:
                # 先处理需要停止的until数据
                await self._check_and_unwatch_data()
                
                await self._update_newest_data()
                # 分批次补齐历史数据
                await self._update_old_data()
                    
            except Exception as e:
                logger.error(f"Background update error: {e}")
                await asyncio.sleep(3)  # 发生错误时等待更长时间

    async def _check_and_unwatch_data(self):
        """检查until时间，停止不再需要的数据流"""
        current_time = int(datetime.now(timezone.utc).timestamp() * 1000)
        
        for key in list(self.until.keys()):
            if self.until[key] < current_time:
                try:
                    # 停止对应的数据流
                    await self._unwatch_data_stream(key)
                    # 移除已处理的until记录
                    del self.until[key]
                    self.cache.get_recoder(key=key).state = State.STOPPED
                except Exception as e:
                    logger.error(f"Error unwatching data stream {key}: {e}")

    async def _unwatch_data_stream(self, key: DataKey):
        """根据数据类型停止对应的数据流"""
     
            
        # 统一处理DataKey和tuple格式的key
       
        pair = key.pair
        timeframe = key.timeframe
        marketType = key.marketType
        datatype = key.datatype
      
            
        try:
            if datatype == "trades":
                # 停止交易数据流的逻辑
                await self.exchange.un_watch_trades(pair)
            elif datatype == "ohlcv":
                # 停止K线数据流的逻辑
                await self.exchange.un_watch_ohlcv(pair, timeframe)
            # 添加其他数据类型的处理
            else:
                logger.warning(f"Unsupported data type for unwatch: {datatype}")
                
        except Exception as e:
            logger.error(f"Error in _unwatch_data_stream for {key}: {e}")

    async def _update_newest_data(self) -> None:
        """获取最新的交易数据并更新缓存"""
        
        for key, df in self.cache.items():
           
            pair = key.pair
            timeframe = key.timeframe
            marketType = key.marketType
            datatype = key.datatype
            
            if df.state == State.RUNNING or df.state == State.PAUSED:
                # 计算时间窗口
                try:
                    # 根据数据类型获取最新数据
                    new_data = None
                    
                    if datatype == "trades":
                        new_data = await self.exchange.watch_trades(
                            symbol=pair, 
                            # since=df.last_datetime - 1
                        )
                    elif datatype == "ohlcv":
                        new_data = await self.exchange.watch_ohlcv(symbol=pair, timeframe=timeframe)
                    # 添加其他数据类型的处理
                    
                    # 更新缓存
                    if new_data:
                           self.cache.append(key=key,data=new_data)
                            
                except Exception as e:
                    logger.error(f"Error fetching newest data for {key}: {e}")
                    continue

    async def _update_old_data(self) -> None:
        """
        分批次获取比当前缓存更早的交易数据
        """
        
        async with asyncio.TaskGroup() as tg:
            for key in self.since.keys():
                since = self.since[key]
                df = self.cache.get_recoder(key=key)
                if since > df.first_datetime:
                    continue
                pair, timeframe, marketType, datatype = key
                if datatype == "trades":
                    tg.create_task(self._fetch_old_trades(key, since=since ))
                elif datatype == "ohlcv":
                    tg.create_task(self._fetch_old_ohlcv(key, since=since))
                else:
                    logger.warning(f"Unsupported data type for fetching old data: {datatype}")

    async def _fetch_old_data(self, key: DataKey, since: int, 
                             fetch_func: Callable[[int],Any], #get_next_time: Callable[[Any], int],
                             batch: int=0,
                             *args, **kwargs):
        """
        通用历史数据获取方法
        batch == 0 一直运行到获取所有数据为止
        
        """
        current_first_time = self.cache.get_recoder(key=key).first_datetime
        
        if current_first_time is None or since >= current_first_time:
            return
            
        result = []
        start_time = current_first_time - batch if batch > 0 else since
        start_time = max(since,start_time)
        max_retries = 5
        retries = 0
        
        while start_time < current_first_time and retries < max_retries:
            try:
              
                _result = await fetch_func(start_time, *args, **kwargs)
                
                if not _result:
                    retries += 1
                    await asyncio.sleep(retries)
                    continue
                
                result.extend(_result)
                start_time = _result[-1][0]
                retries = 0
                
            except Exception as e:
                logger.error(f"Error fetching old {key} data error: {e}")
                retries += 1
                await asyncio.sleep(1)
        
        if result:
            self.cache.prepend(key=key, data=result)

    async def _fetch_old_trades(self, key: DataKey, since: int) -> None:
        async def fetch_data(lsince:int):
            return await self.exchange.fetch_trades(symbol=key.pair,since=lsince)
        time_delta = 60 * 1000  *self.data_internal_ratio
        """获取比当前缓存更早的交易数据"""
        await self._fetch_old_data(
            key=key, since=since, batch=time_delta,
            fetch_func= fetch_data 
        )

    async def _fetch_old_ohlcv(self, key: DataKey, since: int) -> None:
        """获取比当前缓存更早的K线数据"""
        async def fetch_data(lsince:int):
            return await self.exchange.fetch_ohlcv(symbol=key.pair,timeframe=key.timeframe,since=lsince)
        time_delta=timeframe_to_msecs(key.timeframe)*self.data_internal_ratio
        await self._fetch_old_data(
            key=key, since=since, batch=time_delta,
            fetch_func= fetch_data 
        )

    async def trades(self, symbol: str, since:int,marketType: MarketType = "future", limit=None, params=None):
        key = DataKey(symbol,timeframe="", marketType=marketType,datatype= "trades")
        data = self.cache.get_recoder(key=key)
        if data and data.first_datetime < since:
            return data[(since,None)]
        else :
            self.set_since(key=key,since=since)

    async def ohlcv(self, symbol: str,timeframe: str, since:int,marketType: MarketType = "future",limit=None,  params=None):
            key = DataKey(
                pair=symbol,
                timeframe=timeframe,
                marketType=marketType,
                datatype="ohlcv"
            )
            
            # 先尝试从缓存获取数据
            if key in self.cache:
                cached_data = self.cache.get_recoder(key=key)
                if cached_data and cached_data.first_datetime < since:
                    # 返回缓存数据
                    return await cached_data[(since,-1)]
                else:
                    self.set_since(key=key,since=since)
    async def tickers(self, symbol, since:int,marketType):...
    # async def orderbook(self, symbol: str,since:int):
    #     # 处理单个symbol的情况
    #         symbol = symbol
    #         key = DataKey(
    #             pair=symbol,
    #             timeframe="", 
    #             marketType=None,
    #             datatype="orderbook"
    #         )
            
    #         # 先尝试从缓存获取数据
    #         if key in self.cache:
    #             cached_data = self.cache.get_recoder(key=key)
    #             if cached_data and await cached_data.length() > 0:
    #                 # 返回缓存数据
    #                 return await cached_data.to_list()
                    
           
                


      
        