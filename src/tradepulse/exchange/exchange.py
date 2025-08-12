import asyncio
import logging
from collections.abc import Callable
from datetime import datetime, timedelta
from typing import Any, NamedTuple, TypeVar

from ccxt import pro

from tradepulse. data import CacheFactory, DataKey

# from exceptions import *  
from tradepulse. typenums import MarketType, State, TimeFrame, TimeStamp
from tradepulse.util import (
    clamp,
    dt_now,
    timeframe_to_seconds,
    timeframe_to_timedelta,
    timestamp_to_timestamp,
)

from .ccxtexchange_factory import CCXTExchangeFactory
from .protocol import CCXTExchangeProtocol, ExchangeABC

logger = logging.getLogger(__name__)


TimeMarkerMinGap= NamedTuple("TimeMarkerMinGap", [("time_marker", TimeStamp),("internal", timedelta) ])

class Exchange[T](ExchangeABC[T]):

    def __init__(self, exchange_name:str="", config:dict={}) :
        # self.lock = asyncio.Lock()
        self.exchange_name = exchange_name
        if exchange_name.strip() != "":
            config["name"] = exchange_name
        self.config = config
        self.exchange: CCXTExchangeProtocol = CCXTExchangeFactory.get_exchange_instance(config=self.config)
        self.windows = {str,timedelta}
        # self.since_lock = asyncio.Lock()
        self.since:dict[DataKey,TimeMarkerMinGap] = {}
        # self.until_lock = asyncio.Lock()
        self.until: dict[DataKey,TimeMarkerMinGap] = {}
        # self.cache_lock= asyncio.Lock()
        
        self.data_internal_ratio = 10
        self.rateLimit=self.exchange.rateLimit
        self._cache = CacheFactory.get(type(T))

          
       
    @property
    def cache(self):
        return self._cache     
    def set_since(self,key:DataKey,since:float|int,internal:timedelta|str) -> None:
        if isinstance(internal,str):
             internal = timeframe_to_timedelta(internal)
        
        if key not in self.since or self.since[key].time_marker > since:
             self.since[key] =TimeMarkerMinGap(time_marker=TimeStamp(since),internal=internal)
    def set_until(self,key:DataKey,until:float|int,internal:timedelta|str) -> None:
        if isinstance(internal,str):
             internal = timeframe_to_timedelta(internal)
        if key not in self.until or self.until[key].time_marker < until:
             self.until[key] = TimeMarkerMinGap(time_marker=TimeStamp( until),internal=internal)
  
   

    async def un_watch_trades(self,pair:str,until:float|int=0,marketType: MarketType = "future" ) -> None:
         key = DataKey(pair=pair,timeframe="",marketType=marketType,datatype="trades")
         self.set_until(key=key,until=until,internal=timedelta(minutes=1))

    async def un_watch_ohlcv(self,pair:str,timeframe:TimeFrame,until:float|int=0,marketType: MarketType = "future") -> None:
        key = DataKey(pair=pair,timeframe=timeframe,marketType=marketType,datatype="ohlcv")
        self.set_until(key=key,until=until,internal=timeframe)
    async def update(self) -> None:
        """请无限循环运行该函数
        持续更新最新的交易数据"""
        try:
            
            # 先处理需要停止的until数据
            await self._check_and_unwatch_data()
            
            await self._update_newest_data()
            # 分批次补齐历史数据
            await self._update_history_data()
                
        except Exception as e:
            logger.error(f"Background update error: {e}")
            await asyncio.sleep(1)  # 发生错误时等待更长时间
    
    async def _check_and_unwatch_data(self):
        """检查until时间，停止不再需要的数据流"""
        current_time =dt_now().timestamp()
        
        for key in list(self.until.keys()):
            if self.until[key].time_marker < current_time:
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
       
      
            
        try:
            if key.datatype == "trades":
                # 停止交易数据流的逻辑
                await self.exchange.un_watch_trades(key.pair)
            elif key.datatype == "ohlcv":
                # 停止K线数据流的逻辑
                await self.exchange.un_watch_ohlcv(key.pair, key.timeframe)
            # 添加其他数据类型的处理
            else:
                logger.warning(f"Unsupported data type for unwatch: {key.datatype}")
                
        except Exception as e:
            logger.error(f"Error in _unwatch_data_stream for {key}: {e}")

    async def _update_newest_data(self) -> None:
        """获取最新的交易数据并更新缓存"""
        
        for key, df in self.cache.items():
           
            
            if df.state == State.RUNNING or df.state == State.PAUSED:
                # 计算时间窗口
                try:
                    # 根据数据类型获取最新数据
                    new_data = None
                    
                    if key.datatype == "trades":
                        new_data = await self.exchange.watch_trades(
                            symbol=key.pair, 
                            # since=df.last_datetime - 1
                        )
                    elif key.datatype == "ohlcv":
                        new_data = await self.exchange.watch_ohlcv(symbol=key.pair, timeframe=key.timeframe)
            
                    
                    # 更新缓存
                    if new_data:
                        self.cache.append(key=key,data=new_data)
                            
                except Exception as e:
                    logger.error(f"Error fetching newest data for {key}: {e}")
                    continue


    async def _fetch_history_data(self, key: DataKey, since: float|int, 
                             fetch_func: Callable[[float|int],Any], #get_next_time: Callable[[Any], int],
                             batch: timedelta|float=timedelta(seconds=0),
                             *args, **kwargs):
        """
        通用历史数据获取方法
        batch == 0 一直运行到获取所有数据为止
        
        """
        current_first_time = self.cache.get_recoder(key=key).first_time
        
        if current_first_time is None or since >= current_first_time:
            return
            
        result = []
        if isinstance(batch,timedelta):
            batch = batch.total_seconds()*1000

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

    async def _fetch_history_trades(self, key: DataKey, since: float|int) -> None:
        async def fetch_data(lsince:float|int):
            lsince = int( timestamp_to_timestamp(lsince))
            return await self.exchange.fetch_trades(symbol=key.pair,since=lsince)
        time_delta = timedelta(minutes=1)  *self.data_internal_ratio
        """获取比当前缓存更早的交易数据"""
        await self._fetch_history_data(
            key=key, since=since, batch=time_delta,
            fetch_func= fetch_data 
        )

    async def _fetch_history_ohlcv(self, key: DataKey, since: float|int) -> None:
        """获取比当前缓存更早的K线数据"""
        async def fetch_data(lsince:float|int):
            lsince = int( timestamp_to_timestamp(lsince))
            return await self.exchange.fetch_ohlcv(symbol=key.pair,timeframe=key.timeframe,since=lsince)
        time_delta=timedelta(seconds= timeframe_to_seconds(key.timeframe)*self.data_internal_ratio)
        await self._fetch_history_data(
            key=key, since=since, batch=time_delta,
            fetch_func= fetch_data 
        )
    
    async def _fetch_funding_rate_history(self, key: DataKey, since: float|int) -> None:
        """获取比当前缓存更早的K线数据"""
        async def fetch_data(lsince:float|int):
            lsince = int( timestamp_to_timestamp(lsince))
            return await self.exchange.fetch_funding_rate_history(symbol=key.pair,since=lsince)
        
        
        time_delta = timedelta(hours=1)  *self.data_internal_ratio
        """获取比当前缓存更早的交易数据"""
        await self._fetch_history_data(
            key=key, since=since, batch=time_delta,
            fetch_func= fetch_data 
        )
    def _chekc_update_is_time(self,last_time:datetime,current_time:datetime,internal:timedelta) -> bool:
        return current_time - last_time > internal

    async def _update_history_data(self,current_time:datetime) -> None:
        """
        分批次获取比当前缓存更早的交易数据
        """
        removelist=[]
        async with asyncio.TaskGroup() as tg:
            for key in self.since.keys():
                since,internal = self.since[key]

                df = self.cache.get_recoder(key=key)
                first_time =df.first_time
                last_time =df.last_time
              
                current_first_time = df.first_time
                pair, timeframe, marketType, datatype = key
                if  since >= current_first_time:
                    removelist.append(key)
                since = clamp(interval=internal.total_seconds(),dt=since)
                if not self._chekc_update_is_time(last_time.to_datetime_utc(),current_time,internal):
                            continue
                match datatype:
                    case "trades":
                        
                        tg.create_task(self._fetch_history_trades(key, since=since))
                    case "ohlcv":
                        tg.create_task(self._fetch_history_ohlcv(key, since=since))
                    case "funding_rate":

                        tg.create_task(self._fetch_funding_rate_history(key, since=since))
                    case _:
                         logger.warning(f"Unsupported data type for fetching old data: {datatype}")
                   
        for k in removelist:
            del self.since[k]
  
                    
           
                


      
        