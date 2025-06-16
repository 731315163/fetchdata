from ccxt.pro import binance,okx,bitmex,bitstamp,coinbase,kucoin
from .protocol import ExchangeProtocol
# 定义交易所类型的联合类型


class ExchangeFactory:
    """交易所工厂类，用于根据名称创建不同的交易所实例"""
    
    # 支持的交易所映射
    EXCHANGES: dict[str, type] = {
        "binance": binance,
        "okx": okx,
        "bitmex": bitmex,
        "bitstamp": bitstamp,
        "coinbase": coinbase,
        "kucoin": kucoin,
    }
    
    @classmethod
    def get_exchange(cls, name: str, config: dict|str = {}) -> ExchangeProtocol:
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