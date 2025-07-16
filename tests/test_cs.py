# test_fetchdata.py
from ccxt import test
import pytest
import asyncio

# 项目模块导入
from tradepulse.exchange.protocol import ExchangeABC
from tradepulse.fetchdata import Server,Client
from tradepulse.message.methodid_pb2 import MethodID, InvokeMethod
from tradepulse.message.google.protobuf.struct_pb2 import Struct
from tradepulse.fetchdata.method_invoke import invoke_method, create_invoke_method
from tradepulse.exchange.exchange_factory import ExchangeFactory
from tradepulse.data.serialize import serialize_dataframe, deserialize_dataframe
import polars as pl

# 测试配置
TEST_ADDRESS = "localhost:6102"
TEST_EXCHANGE_CONFIG = {"test_mode": True}

# 测试用DataFrame
TEST_DF = pl.DataFrame({
    "timestamp": [1620000000, 1620003600],
    "open": [30000.0, 30100.0],
    "high": [30200.0, 30300.0],
    "low": [29800.0, 29900.0],
    "close": [30100.0, 30250.0],
    "volume": [100.5, 200.3]
})

class TestExchange(ExchangeABC[pl.DataFrame]):
    """测试用交易所实现"""
    async def ohlcv(self, symbol: str, timeframe: str, marketType: str, since: float):
        """返回测试DataFrame"""
        return TEST_DF
    
    async def trades(self, symbol: str, since: float, marketType: str):
        """返回测试DataFrame"""
        return TEST_DF

    async def update(self):
        """模拟更新"""
        return True

@pytest.fixture(scope="module")
def event_loop():
    """创建事件循环"""
    loop = asyncio.get_event_loop()
    yield loop



@pytest.fixture(scope="module")
async def test_server():
    """创建并启动测试服务器"""
        # 创建服务器实例
    server = Server(address=TEST_ADDRESS, config=TEST_EXCHANGE_CONFIG)
    await server.start()
    # 使用测试交易所
    return server       
@pytest.fixture(scope="module")
async def server_loop(server: Server,loop:int = 0):
    """运行服务器事件循环"""
    while loop > 0:
        await server.recv()
        loop -= 1
  

@pytest.fixture(scope="module")
def test_client():
    """创建测试客户端"""
    client = Client(address=TEST_ADDRESS)
    return client
    
 

# ======================
# method_invoke.py 测试
# ======================
class TestMethodInvoke:
    @pytest.mark.asyncio
    async def test_create_invoke_method(self):
        """测试create_invoke_method函数"""
        params = {
            "symbol": "BTC/USD",
            "timeframe": "1h",
            "since": 1620000000,
            "marktype": "spot"
        }
        
        invoke = create_invoke_method(MethodID.OHLCV, params)
        
        # 验证MethodID
        assert invoke.method_id == MethodID.OHLCV
        
        # 验证Struct转换
        assert invoke.params.fields["symbol"].string_value == "BTC/USD"
        assert invoke.params.fields["timeframe"].string_value == "1h"
        assert invoke.params.fields["since"].number_value == 1620000000
        assert invoke.params.fields["marktype"].string_value == "spot"

    @pytest.mark.asyncio
    async def test_invoke_ohlcv(self):
        """测试invoke_method处理OHLCV请求"""
        params = {
            "symbol": "BTC/USD",
            "timeframe": "1h",
            "since": 1620000000,
            "marktype": "spot"
        }
        test_exchange = TestExchange()
        invoke = create_invoke_method(MethodID.OHLCV, params)
        result = await invoke_method(invoke, test_exchange)
        
        # 验证DataFrame结构
        assert isinstance(result, pl.DataFrame)
        assert result.shape == TEST_DF.shape
     

    @pytest.mark.asyncio
    async def test_invoke_trades(self):
        test_exchange = TestExchange()
        """测试invoke_method处理trades请求"""
        params = {
            "symbol": "BTC/USD",
            "since": 1620000000,
            "marktype": "spot"
        }
        
        invoke = create_invoke_method(MethodID.TRADES, params)
        result = await invoke_method(invoke, test_exchange)
        
        # 验证DataFrame结构
        assert isinstance(result, pl.DataFrame)
        assert result.shape == TEST_DF.shape
        assert result.columns == TEST_DF.columns
        assert result.dtypes == TEST_DF.dtypes

    @pytest.mark.asyncio
    async def test_unknown_method(self, test_exchange):
        """测试未知方法处理"""
        invoke = InvokeMethod(
            method_id=MethodID.UNDEFINED,
            params=Struct()
        )
        
        result = await invoke_method(invoke, test_exchange)
        assert result is None

# ==================
# client.py 测试
# ==================
class TestClient:
    @pytest.mark.asyncio
    async def test_send_request(self, test_client):
        """测试send方法发送请求"""
        params = {
            "symbol": "BTC/USD",
            "timeframe": "1h",
            "since": 1620000000,
            "marktype": "spot"
        }
        
        result_df = await test_client.send(MethodID.OHLCV, params)
        
        # 验证DataFrame结构
        assert isinstance(result_df, pl.DataFrame)
        assert result_df.shape == TEST_DF.shape

    @pytest.mark.asyncio
    async def test_ohlcv_method(self,test_server, test_client):
        """测试ohlcv方法"""
        result = await test_client.ohlcv("BTC/USD", "1h", since=1620000000)
        
        # 验证DataFrame结构
        assert isinstance(result, pl.DataFrame)
        assert result.shape == TEST_DF.shape
        assert result.columns == TEST_DF.columns

    @pytest.mark.asyncio
    async def test_trades_method(self, test_client):
        """测试trades方法"""
        result = await test_client.trades("BTC/USD", since=1620000000)
        
        # 验证DataFrame结构
        assert isinstance(result, pl.DataFrame)
        assert result.shape == TEST_DF.shape
        assert result.columns == TEST_DF.columns

# ==================
# producer.py 测试
# ==================
class TestServer:
    @pytest.mark.asyncio
    async def test_server_start_stop(self, test_server):
        """测试服务器启动和停止"""
        assert test_server is not None
        
    @pytest.mark.asyncio
    async def test_full_communication_flow(self, test_server: Server, test_client: Client):
        async with asyncio.TaskGroup() as tg:
            tg.create_task( test_client.request.start())
            """测试完整通信流程"""
            tg.create_task(server_loop(test_server, loop=1))
            result = await tg.create_task( test_client.ohlcv("BTC/USD", "1h", since=1620000000))
            
        # 验证DataFrame结构
        assert isinstance(result, pl.DataFrame)
        assert result.shape == TEST_DF.shape
        assert result.columns == TEST_DF.columns

# ==================
# data/serialize.py 测试
# ==================
class TestDataSerialization:
    def test_serialize_deserialize_consistency(self):
        """测试序列化/反序列化一致性"""
        # 测试空数据
        empty_df = pl.DataFrame()
        serialized = serialize_dataframe(empty_df)
        deserialized = deserialize_dataframe(serialized)
        assert deserialized.is_empty()
        
        # 测试正常数据
        test_data = TEST_DF
        serialized = serialize_dataframe(test_data)
        deserialized = deserialize_dataframe(serialized)
        
        assert isinstance(deserialized, pl.DataFrame)
        assert deserialized.shape == test_data.shape
        assert deserialized.columns == test_data.columns
        assert deserialized.dtypes == test_data.dtypes

    def test_large_data_transfer(self):
        """测试大数据传输"""
        # 创建更大的测试DataFrame
        large_df = pl.concat([TEST_DF] * 1000)
        
        serialized = serialize_dataframe(large_df)
        deserialized = deserialize_dataframe(serialized)
        
        assert deserialized.shape == large_df.shape
        assert len(deserialized) == len(large_df)

