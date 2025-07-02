

from typing import Any, cast

from ccxt.static_dependencies.ecdsa import der
from communication.zeromq.factory import Factory
from google.protobuf.message import Message

from exchange import ExchangeProtocol
from exchange.exchange_factory import ExchangeFactory,Exchange,ExchangeConifg

from .message.google.protobuf.struct_pb2 import Struct
from .message.methodid_pb2 import InvokeMethod, MethodID
from .method_invoke import MethodID, invoke_method


class Server():
    def __init__(self,address:str = "localhost:6102",config:dict={}):
        
        self.server = Factory.create_Responder(protocol="inproc",address=address)
        self.exchange = ExchangeFactory.get_exchange(config=config)

    async def start(self):
        await self.server.start()
    @staticmethod
    def deserialize(data:bytes)-> InvokeMethod:
        return InvokeMethod.FromString(data)
    async def recv(self):
        await self.exchange.update()
        invokemsg: InvokeMethod  = await self.server.recv(deserializer=Server.deserialize)
        response = await invoke_method(invoke_method=invokemsg,ex=self.exchange)
        if response is None: return
        await self.server.send(message=response)
        