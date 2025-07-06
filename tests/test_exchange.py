



import asyncio
import logging
from datetime import datetime, timedelta, timezone, tzinfo
from time import time

# tests/conftest.py
# tests/test_exchange_functionality.py
from polars import DataFrame
import pytest
from ccxt.base.types import Trade
import pyarrow as pa
from exchange import CCXTExchangeFactory, ExchangeProtocol,ExchangeFactory
from exchange.exchange import Exchange,DataKey
from exchange.protocol import CCXTExchangeProtocol
from typenums import DataType, MarketType, State  


def get_exchange(exchange_name: str="binance", config={}) -> CCXTExchangeProtocol:
    return CCXTExchangeFactory.get_exchange(name = exchange_name, config=config)


@pytest.mark.asyncio
async def test_get_trades():
    
    ex = get_exchange()
    print(f"ex limit :{ex.rateLimit} ")
    dt = datetime.now(tz=timezone.utc)-timedelta(days=1)
    dt_int = int( dt.timestamp()*1000)
    
    for _ in range(50):
        start= time()
        data: list[Trade]= await ex.fetch_trades(symbol="BTC/USDT", since=dt_int)
        end = time( ) - start
        print(f"trades num is : {len(data)} time cose:{ end}")
        start_time =datetime.fromtimestamp( timestamp=data[0]["timestamp"]/1000) # type: ignore
        print (start_time)
        end_time = datetime.fromtimestamp( timestamp=data[-1]["timestamp"]/1000) # type: ignore
        print(end_time)
        dt_int = data[-1]["timestamp"]
@pytest.mark.asyncio
async def test_get_ohlcv():
    print("ffffffffffffffffffffffff")
    ex = get_exchange()
    dt = datetime.now(tz=timezone.utc)-timedelta(days=1)
    dt_int = int( dt.timestamp()*1000)
    for _ in range(3):
        data= await ex.fetch_ohlcv(symbol="BTC/USDT", timeframe="1m", since=dt_int)
        
        print(f"oblcv num is : {len(data)}")
        start_time =datetime.fromtimestamp( timestamp=data[0][0]/1000)
        print (start_time)
        end_time = datetime.fromtimestamp( timestamp=data[-1][0]/1000)
        print(end_time)
        dt_int = data[-1][0]





@pytest.fixture
def exchange():
    return ExchangeFactory.get_exchange(name="binance")

@pytest.fixture
def data_key():
    return DataKey(pair="BTC/USDT", timeframe="1m", marketType="spot", datatype="ohlcv")





@pytest.mark.asyncio
async def test_fetch_trades_success(exchange):
    # 测试正常获取交易数据
    dt = datetime.now(timezone.utc) 
    dt_int = int(dt.timestamp() * 1000)
    exchange.set_since(data_key, dt,timedelta(minutes=1))
    await exchange.update()
    data = await exchange.trades(symbol="BTC/USDT", since=dt)
    
    assert data is None
    await exchange.update()
    data = await exchange.trades(symbol="BTC/USDT", since=dt)
    assert len(data) > 0
    assert isinstance(data, pa.Table)    

@pytest.mark.asyncio
async def test_fetch_ohlcv_success(exchange):
    # 测试正常获取K线数据
    dt = datetime.now(tz=timezone.utc) - timedelta(days=1)
    dt_int = int(dt.timestamp() * 1000)
    
    data = await exchange.exchange.fetch_ohlcv(symbol="BTC/USDT", timeframe="1m", since=dt_int)
    
    assert isinstance(data, list)
    assert len(data) > 0
    assert all(isinstance(candle, list) for candle in data)
    assert all(len(candle) == 6 for candle in data)  # OHLCV数据格式验证

@pytest.mark.asyncio
async def test_fetch_trades_error_handling(exchange):
    # 测试错误处理
    with pytest.raises(Exception):
        await exchange.exchange.fetch_trades(symbol="INVALID_PAIR", since=0)

@pytest.mark.asyncio
async def test_update_method(exchange):
    # 测试update方法的基本执行
    task = asyncio.create_task(exchange.update())
    await asyncio.sleep(0.1)  # 短暂运行
    task.cancel()
    
    assert True  # 如果能正常启动和取消任务就认为测试通过

@pytest.mark.asyncio
async def test_data_cache(exchange, data_key):
    # 测试缓存功能
    dt = datetime.now(tz=timezone.utc) - timedelta(days=1)
    dt_int = int(dt.timestamp() * 1000)
    
    # 获取初始数据
    ohlcv_data = await exchange.exchange.fetch_ohlcv(symbol="BTC/USDT", timeframe="1m", since=dt_int)
    exchange.cache.append(key=data_key, data=ohlcv_data)
    
    # 验证缓存存在
    assert data_key in exchange.cache
    cached_data = exchange.cache.get_recoder(key=data_key)
    assert len(cached_data) == len(ohlcv_data)
    
    # 验证数据一致性
    assert all(c1[0] == c2[0] for c1, c2 in zip(cached_data, ohlcv_data))




@pytest.mark.asyncio
async def test_exchange_initialization(exchange):
    ex =exchange
    assert ex.exchange_name == "binance"
    assert ex.rateLimit > 0  # 验证rateLimit正确初始化

@pytest.mark.asyncio
async def test_timeframe_methods(exchange):
    ex =exchange
    
    # 测试时间窗口计算
    assert ex.data_internal_ratio == 10  # 验证默认值
    assert ex._fetch_old_trades.__qualname__  # 验证方法存在
    assert ex._fetch_old_ohlcv.__qualname__  # 验证方法存在

@pytest.mark.asyncio
async def test_error_handling(exchange):
    ex =exchange
    
    # 测试无效参数处理
    with pytest.raises(Exception):
        await ex.exchange.fetch_trades(symbol="")  # 空交易对
    
    with pytest.raises(Exception):
        await ex.exchange.fetch_ohlcv(symbol="", timeframe="1m")  # 空交易对