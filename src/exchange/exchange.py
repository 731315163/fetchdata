from collections.abc import Awaitable, Callable
import logging
from typing import Any, Iterable

from typenums import DataType, State
from .exchange_factory import ExchangeFactory
from .protocol import ExchangeProtocol
from data import CacheFactory, DataKey,DataRecoder
import asyncio
from datetime import datetime,timedelta,timezone,time
import polars as pl
from typenums.Literal import TimeFrame
import ccxt
from exceptions import *
from util import timeframe_to_msecs
logger = logging.getLogger(__name__)
class Exchange:

    def __init__(self, exchange_name: str, config) :
        self.lock = asyncio.Lock()
        self.exchange_name = exchange_name
        self.exchange: ExchangeProtocol = ExchangeFactory.get_exchange(exchange_name, config=config)
        self.windows = {str,timedelta}
        self.since:dict[tuple,int] = {}
        self.until:dict[tuple,int] = {}
        self.cache = CacheFactory.get(exchange_id=self.exchange_name)
        self.data_internal_ratio = 60
        # self._trades_pagination= self.exchange.has["pagination"]


    def get_active_symbols(self):
        if self.exchange.symbols is not None:
            return [symbol for symbol in self.exchange.symbols if self. is_active_symbol( symbol)]
        return []
    def set_since(self,key:DataKey,since:int):
        if key not in self.since or(key in self.since and since < self.since[key]):
            self.since[key] = since
    def set_until(self,key:DataKey,until:int):
        if key not in self.until or(key in self.until and until > self.until[key]):
            self.until[key] = until

    def is_active_symbol(self, symbol):
        if self.exchange.markets is None:
           return False
        return ('.' not in symbol) and (('active' not in self.exchange.markets[symbol]) or (self.exchange.markets[symbol]['active']))
    async def un_tickers(self, symbol:str,until:int|None = None):
        length  = len(symbols)
        if length > 1:
            result = await self.exchange.watch_tickers(list(symbols))
        elif length == 1:
            reust = await self.exchange.watch_ticker(symbols[0])
        elif length <1:
            logger.log(logging.ERROR, "No symbols provided")
    
    async def un_trades(self, *symbols):
       result= await self.exchange.watch_trades_for_symbols(symbols=list(symbols))
    async def un_ohlcv(self, timeframe:TimeFrame, *symbols):
        symbols = list(symbols)
        
        if len(symbols) == 1:
            return self.exchange.fetch_ohlcv(symbols[0], '1d')
    async def un_orderbook(self, *symbols):
        length = len(symbols)
        symbols = list(symbols)
        if length == 1:
            return self.exchange.watch_order_book(symbols[0])
        elif length > 1:
            return self.exchange.watch_orders(symbols)
    async def tickers(self, symbol:str,markettype):
        key = DataKey(symbol,timeframe= "",marketType=markettype,datatype="trades")
        datacache= self.cache.get_recoder(key=key)
        length  = len(symbols)
        if length > 1:
            result = await self.exchange.watch_tickers(list(symbols))
        elif length == 1:
            reust = await self.exchange.watch_ticker(symbols[0])
        elif length <1:
            logger.error( "No symbols provided")
    
    async def trades(self,symbol:str, since:int,until = -1,limit =None,params={} ):
        
       result= await self.exchange.watch_trades_for_symbols(symbols=list(symbols),since=since,limit=limit,params=params)
       return result
    async def ohlcv(self,symbol:str,timeframe: str,since=None,limit =None,params={}):
        if length == 1:
            pair , timeframe = symbols[0]
            return await self.exchange.watch_ohlcv(symbol=pair, timeframe=timeframe)
        elif length > 1:
            return  await self.exchange.watch_ohlcv_for_symbols(symbolsAndTimeframes=symbols,since=since,limit=limit,params=params)
    async def orderbook(self, symbols:list):
       
            return self.exchange.watch_order_book_for_symbols(symbols)
       
    
    async def update(self) -> None:
        """后台任务，持续更新最新的交易数据"""
        while True:
            try:
                if not self.fetching_newest:
                    self.fetching_newest = True
                    await self._update_newest_data()
                    self.fetching_newest = False
            except Exception as e:
                logger.error(f"Background update error: {e}")
                await asyncio.sleep(5)  # 发生错误时等待更长时间
    
    async def _update_newest_data(self) -> None:
        """获取最新的交易数据并更新缓存"""
        if not self.cache:
            return  
        for key,df in self.cache.items():
            pair,timeframe,marketType,datatype = key
            if df.state == State.RUNNING or df.state == State.PAUSED:
                since = self.cache.get_recoder(key=key)
                taks = asyncio.create_task(self.exchange.watch_trades(symbol=pair,since = df.first_datetime )) 
                
        try:
            trades = await self.exchange.watch_trades(self.symbol, latest_timestamp)
            new_trades = [t for t in trades if t['timestamp'] > latest_timestamp]
            if new_trades:
                async with self.lock:
                    self._update_cache(new_trades)
        except Exception as e:
            print(f"Error fetching newest data: {e}")
    
    async def _update_old_data_for_cache(self,key:DataKey, since:int,fetch:Callable[[str, int], Awaitable[list[list]]]):
        cache = self.cache.get_recoder(key=key)
        last_datetime = cache.first_datetime-1
        timeframe =  key.timeframe if key.timeframe.strip() != "" else "1s"
        internal = timeframe_to_msecs(timeframe)*self.data_internal_ratio
        chunck_since = max( last_datetime-internal,since)
        cache_list = []
        while chunck_since < last_datetime:
            result = await fetch(key.pair, chunck_since)
            cache_list.extend(result)
            chunck_since = int( result[-1][0])

        await self.cache.prepend(key, cache_list)

    
    async def _update_old_data(self) -> None:
        """
        获取比当前缓存更早的交易数据

        参数:
            since: 开始时间戳(毫秒)
        """
        if not self.cache:
            return  # 缓存为空时不执行
        async with asyncio.taskgroups.TaskGroup() as tg:
            for key in self.cache.keys():
                if key not in self.since:
                    continue
                since = self.since.get(key)
                recoder = self.cache.get_recoder(key=key)
            
                end_timestamp = recoder.first_datetime if recoder else None
                if not since or not end_timestamp or since >= end_timestamp:
                    continue  # 不需要获取更早的数据
           
                current_time = end_timestamp - 1
                if current_time is not None and since is not None and current_time > since:

                    try:
                        # 使用fetch_trades获取历史数据
                        trades =tg.create_task( self.exchange.fetch_trades(key.pair, since))
                        result= await trades
                        if not trades:
                            break

                        async with self.lock:
                            # 只添加比当前缓存更早的数据
                            older_trades = []
                            # 确保 trades 是可迭代对象
                            trades_list = []
                            if isinstance(trades, list):
                                trades_list = trades
                            elif hasattr(trades, '__await__'):
                                # 如果是协程，等待其完成
                                trades_list = await trades
                            elif isinstance(trades, Iterable):
                                trades_list = list(trades)

                            # 验证 timestamp 存在性
                            if end_timestamp is not None:
                                older_trades = [t for t in trades_list 
                                        if isinstance(t, dict) and 'timestamp' in t 
                                        and isinstance(t['timestamp'], (int, float)) 
                                        and t['timestamp'] < end_timestamp]

                        if older_trades:
                            self._prepend_to_cache(older_trades)

                    # 更新下一次请求的时间范围
                    if trades_list:
                        since = trades_list[-1]['timestamp']  # 使用获取到的最后一条数据的时间戳作为下次请求的起点
                    time_delta = min(time_delta * 2, max_time_delta)  # 成功后增加时间间隔
                except Exception as e:
                    logger.error(f"Error fetching older data from {since} to {current_time}: {e}")
                    time_delta = min(time_delta * 2, max_time_delta)  # 发生错误时，适当增加时间间隔
     
            