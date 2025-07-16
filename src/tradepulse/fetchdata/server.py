



from communication.zeromq.factory import Factory

from tradepulse.exchange.exchange_factory import ExchangeFactory

from tradepulse.message.methodid_pb2 import InvokeMethod
from .method_invoke import invoke_method
from tradepulse.data.serialize import serialize_dataframe





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
        invokemsg: InvokeMethod  = await self.server.recv(deserializer=Server.deserialize)
        response = await invoke_method(invoke_method=invokemsg,ex=self.exchange)
        if response is None:
            return
        await self.server.send(message=response,serializer=serialize_dataframe)








    
