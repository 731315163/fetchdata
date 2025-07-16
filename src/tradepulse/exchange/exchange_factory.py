from .exchange import Exchange
from .protocol import ExchangeABC 

# 定义交易所类型的联合类型



class ExchangeFactory():
  
    @classmethod
    def get_exchange(cls, name: str="", config: dict = {}) -> ExchangeABC:
        return Exchange(exchange_name=name,config=config)


