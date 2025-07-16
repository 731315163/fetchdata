

from tradepulse.exchange import ExchangeABC
from .client import Client
from .server import Server
import asyncio

import polars as pl
from communication.zeromq.factory import Factory

from tradepulse.message.methodid_pb2 import InvokeMethod, MethodID


# from exceptions import *  
from tradepulse.typenums import MarketType,TimeFrame
from communication import CommunicationProtocol
from tradepulse.data.serialize import deserialize_dataframe
from .method_invoke import create_invoke_method

class InProc(ExchangeABC[pl.DataFrame]):
    def __init__(self, address: str = "localhost:6102"):
        self.server = Server(address)
        self.client = Client(address)

 
