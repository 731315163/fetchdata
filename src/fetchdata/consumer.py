import logging
import asyncio
from datetime import datetime,timedelta,timezone,time
import polars as pl
from communication.zeromq.factory import Factory
from typing import Any, cast

from data import DataKey
from exchange import ExchangeProtocol
from typenums import MarketType
from typenums.Literal import TimeFrame

from .message.methodid_pb2 import MethodID,InvokeMethod
from .message.google.protobuf.struct_pb2 import Struct 
from google.protobuf.message import Message
logger = logging.getLogger(__name__)
class Clinet(ExchangeProtocol):
    @staticmethod
    def serailize(object:Message)-> bytes:
        return object.SerializeToString()
    def __init__(self, address:str = "localhost:6102") :
        self.request = Factory.create_Resquester(protocol="inproc",address=address)
        self.lock = asyncio.Lock()
        self.last_trades_sendtime = {}
        self.last_ohlcv_sendtime = {}
        self.last_orderbook_sendtime = {}
        self.protocol = ExchangeProtocol
    def send(self,method:MethodID,params:dict[str,Any]):...


    def get_active_symbols(self):
      ...

    def is_active_symbol(self, symbol):
      ...

   
    def set_since(self,key:DataKey,since:int) -> None:...
    def set_until(self,key:DataKey,until:int) -> None:...
     
    def un_watch_trades(self,pair:str,marketType:str ,until:int) -> None:...
   
    def un_watch_ohlcv(self,pair:str,timeframe:TimeFrame,marketType:str,until:int) -> None:...
    async def update(self) -> None:...
    async def trades(self, symbol: str, since:int,marketType: MarketType = "future", limit=None, params=None):...
     
    async def ohlcv(self, symbol: str,timeframe: str,marketType:str, since:int,limit=None,  params=None):...

    async def tickers(self, symbol, since:int,marketType="future"):
        parameter = {}
        parameter["symbols"] = symbol
        await self.request.send((MethodID.TICKERS,parameter),serializer=Clinet.serailize)
        result= await self.request.recv()
        return result
 