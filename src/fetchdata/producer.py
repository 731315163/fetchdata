

from communication.zeromq.factory import Factory
import pickle
from typing import Any, cast

from exchange import Exchange

from .msgtype import method_enum,match_method




class producer():
    def __init__(self,address:str = "localhost:6102"):
        self.exchange = Exchange("binance",{})
        self.server = Factory.create_Responder(protocol="inproc",address=address)

    async def start(self):
        await self.server.start()
    @staticmethod
    def deserialize(data)-> tuple[method_enum, dict[str,Any]]:
        return pickle.loads(data)
    async def recv(self):
        id,parameters = await  self.server.recv(deserializer=producer.deserialize)
        id = cast(method_enum,id)
        parameters = cast(dict[str,Any],parameters)
        call = match_method(id,self.exchange)
        if call is None: return
        else:
            return await call(**parameters)
        