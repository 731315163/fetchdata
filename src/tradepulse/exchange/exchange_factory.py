from collections.abc import MutableSequence, Sequence
from datetime import timedelta
from typing import TypeVar

from tradepulse.data import DataRecoder
from .exchange import DataKey, Exchange, MarketType
from .protocol import ExchangeABC 
import polars as pl
# 定义交易所类型的联合类型
T= TypeVar("T", bound=pl.DataFrame|list)

class DataFrameExchange(Exchange[pl.DataFrame]) :

    def _get_data(self,lazy_df: pl.LazyFrame,timekey:str, index: int|float|slice|tuple|list|None = None,limit=None) -> pl.DataFrame:
        start = None
        end =None
        if isinstance(index, slice):
        # 处理切片：假设start/end为时间戳
            start,end = index.start ,index.stop
        elif isinstance(index, (int,float)) :
            start = index
        elif isinstance(index, (tuple,Sequence,MutableSequence)) and len(index)>0:
            start = index[0]
            if len(index) >= 2:
                end = index[1]
                
            
        if start is not None:
            lazy_df = lazy_df.filter(pl.col(timekey) >= start)
        if end is not None:
            lazy_df = lazy_df.filter(pl.col(timekey) <= end)
        if limit is not None:
            lazy_df = lazy_df.limit(limit)
        df = lazy_df.collect()  # 一次性触发执行
        return df
    async def trades(self, symbol: str, since:float|int,marketType: MarketType = "future",limit=None, params=None)->pl.DataFrame:
            key = DataKey(symbol,timeframe="", marketType=marketType,datatype= "trades")
       
            
            datarecoder:DataRecoder[pl.DataFrame]  = self.cache.get_recoder(key=key)

            if datarecoder and datarecoder.first_datetime <= since:
                self._get_data(lazy_df=datarecoder.rawdata.lazy(),timekey="datetime",index=since,limit=limit)
            
            self.set_since(key=key,since=since,internal = timedelta(minutes=1))
            return self.cache.empty()
            
            
    async def ohlcv(self, symbol: str,timeframe: str, since:float|int,marketType: MarketType = "future",limit=None,  params=None)->pl.DataFrame:
            key = DataKey(
                pair=symbol,
                timeframe=timeframe,
                marketType=marketType,
                datatype="ohlcv"
            )
           
       
            # 先尝试从缓存获取数据
            if key in self.cache:
                datarecoder:DataRecoder[pl.DataFrame]  = self.cache.get_recoder(key=key)
                if datarecoder and datarecoder.first_datetime < since:
                    # 返回缓存数据
                    self._get_data(lazy_df=datarecoder.rawdata.lazy(),timekey="datetime",index=since,limit=limit)
            self.set_since(key=key,since=since,internal=timeframe)
            return self.cache.empty()
            
            
            
    async def tickers(self, symbol:str, since:float|int,marketType: MarketType = "future",limit=None,  params=None)->pl.DataFrame:...
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


class ExchangeFactory():
  
    @classmethod
    def get_exchange(cls, name: str="", config: dict = {}) -> ExchangeABC:
        return DataFrameExchange(exchange_name=name,config=config)


