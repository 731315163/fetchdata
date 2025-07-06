from typing import TypedDict

from ccxt.pro import binance, bitmex, bitstamp, coinbase, kucoin, okx


from .protocol import CCXTExchangeProtocol

# 定义交易所类型的联合类型

class ExchangeConifg (TypedDict):
    apiKey: str
    secret: str
    password: str
    name :str




class CCXTExchangeFactory:
    """交易所工厂类，用于根据名称创建不同的交易所实例"""
    
    # 支持的交易所映射
    EXCHANGES: dict[str, type] = {
        binance.__name__: binance,
        okx.__name__: okx,
        bitmex.__name__: bitmex,
        bitstamp.__name__: bitstamp,
        coinbase.__name__: coinbase,
        kucoin.__name__: kucoin,
    }
    exchanges= {}
    @classmethod
    def getkey_by_config(cls,config:dict)->tuple:
        return config["name"] ,config.get("api_key","")
    @classmethod
    def get_exchange(cls, name: str, config: dict|str = {}) -> CCXTExchangeProtocol:
        """
        根据名称创建并返回交易所实例
        
        Args:
            name: 交易所名称
            config: 交易所配置参数
            
        Returns:
            交易所实例
            
        Raises:
            ValueError: 当请求的交易所不被支持时
        """
        if name not in cls.EXCHANGES:
            raise ValueError(f"Exchange {name} not supported")
        
        
        
        # 创建并返回交易所实例
        return cls.EXCHANGES[name](config)
    @classmethod
    def get_exchange_instance(cls, config: dict = {}) -> CCXTExchangeProtocol:
        """
        根据名称创建并返回交易所实例
        
        Args:
            name: 交易所名称
            config: 交易所配置参数
            
        Returns:
            交易所实例
            
        Raises:
            ValueError: 当请求的交易所不被支持时
        """
        name = config["name"]
        if name not in cls.EXCHANGES:
            raise ValueError(f"Exchange {name} not supported")
        key = cls.getkey_by_config(config)
        if key in cls.exchanges:
            return cls.exchanges[key]
        else:
            ex = cls.get_exchange(name, config)
            cls.exchanges[key] = ex
            # 创建并返回交易所实例
            return ex
    @classmethod
    def del_exchange_instance(cls, config)  :   
         key = cls.getkey_by_config(config)
         if key in cls.exchanges:
            del cls.exchanges[key]