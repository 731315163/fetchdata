import asyncio
import logging

import polars as pl
from communication.zeromq.factory import Factory
from google.protobuf.json_format import ParseDict

from tradepulse.message.google.protobuf.struct_pb2 import Struct
from tradepulse.message.methodid_pb2 import InvokeMethod, MethodID
from tradepulse.exchange import ExchangeABC 


# from exceptions import *  
from tradepulse.typenums import MarketType,TimeFrame
from communication import CommunicationProtocol
from tradepulse.data.serialize import deserialize_dataframe
from .method_invoke import create_invoke_method
logger = logging.getLogger(__name__)




class Client(ExchangeABC[pl.DataFrame]):
    @staticmethod
    def serailize(object: InvokeMethod) -> bytes:
        return object.SerializeToString()
    
        
    def __init__(self, address: str = "localhost:6102",protocol: CommunicationProtocol = "inproc"):
        self.request = Factory.create_Resquester(protocol=protocol, address=address)
        self.lock = asyncio.Lock()
        self.last_trades_sendtime = {}
        self.last_ohlcv_sendtime = {}
        self.last_orderbook_sendtime = {}
        self.symbols_cache = None
        self.active_symbols_cache = None

    async def  send(self, method: MethodID, params: dict) -> bytes:
        """Send a request with MethodID and parameters"""
        invoke = create_invoke_method(method, params)
        return await self.request.send(invoke, serializer=Client.serailize)
    async def un_watch_trades(self,pair:str,until:float|int,marketType: MarketType = "future" ) -> None:
         await self.send(MethodID.UN_TRADES, {"pair": pair, "until": until, "marketType": marketType})
   
    async def un_watch_ohlcv(self,pair:str,timeframe:TimeFrame,until:float|int,marketType: MarketType = "future") -> None:
         await self.send(MethodID.UN_OHLCV, {"pair": pair,"timeframe":TimeFrame, "until": until, "marketType": marketType})
   
    async def trades(self, symbol: str, since:float|int,marketType: MarketType = "future", wait_full_data = True, limit=None, params=None)->pl.DataFrame:
     
        """Get trades data"""
        _params:dict = {
            "symbol": symbol,
        }
        if since > 0:
            _params["since"] = since
        if params is not None:
            _params.update(params)
        response = await self.send(MethodID.TRADES, _params)
        return deserialize_dataframe(response)
    async def ohlcv(self, symbol: str,timeframe: str, since:float|int,marketType: MarketType = "future",wait_full_data = True,limit=None,  params=None)->pl.DataFrame:
        """Get OHLCV data"""
        _params: dict = {
            "symbol": symbol,
            "timeframe": timeframe,
        }
        if since>0:
            _params["since"] = since
        
        response = await self.send(MethodID.OHLCV, _params)
        return deserialize_dataframe(response)
    async def tickers(self, symbol:str, since:float|int,marketType: MarketType = "future",wait_full_data = True,limit=None,  params=None)->pl.DataFrame:...


       

    async def update(self):
        await self.send(MethodID.UPDATE_MARKET,params={})

       



