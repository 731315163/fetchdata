from abc import ABC, abstractmethod
from typing import TypedDict

from ccxt.pro import binance, bitmex, bitstamp, coinbase, kucoin, okx

from data import DataKey
from typenums import MarketType
from typenums.Literal import TimeFrame

from .exchange import Exchange
from .protocol import CCXTExchangeProtocol
from .protocol import ExchangeABC 

# 定义交易所类型的联合类型



class ExchangeFactory():
  
    @classmethod
    def get_exchange(cls, name: str="", config: dict = {}) -> ExchangeABC:
        return Exchange(exchange_name=name,config=config)


