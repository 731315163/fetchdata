import logging
from .exchange_factory import ExchangeFactory
from .protocol import ExchangeProtocol
from cache import CacheManager,CacheProtocol 
import asyncio
from datetime import datetime,timedelta,timezone,time
import polars as pl
logger = logging.getLogger(__name__)
class Exchange:

    def __init__(self, exchange_name: str, config) :
        self.lock = asyncio.Lock()
        self.exchange_name = exchange_name
        self.exchange: ExchangeProtocol = ExchangeFactory.get_exchange(exchange_name, config=config)
        self.last_trades_sendtime = {}
        self.last_ohlcv_sendtime = {}
        self.last_orderbook_sendtime = {}
        self.datacache = CacheManager.get (self.exchange_name)


    def get_active_symbols(self):
        if self.exchange.symbols is not None:
            return [symbol for symbol in self.exchange.symbols if self. is_active_symbol( symbol)]
        return []

    def is_active_symbol(self, symbol):
        if self.exchange.markets is None:
           return False
        return ('.' not in symbol) and (('active' not in self.exchange.markets[symbol]) or (self.exchange.markets[symbol]['active']))
    async def tickers(self, *symbols):
        length  = len(symbols)
        if length > 1:
            result = await self.exchange.watch_tickers(list(symbols))
        elif length == 1:
            reust = await self.exchange.watch_ticker(symbols[0])
        elif length <1:
            logger.log(logging.ERROR, "No symbols provided")
    
    async def trades(self, *symbols):
       result= await self.exchange.watch_trades_for_symbols(symbols=list(symbols))
    async def ohlcv(self, *symbols):
        symbols = list(symbols)
        if len(symbols) == 1:
            return self.exchange.fetch_ohlcv(symbols[0], '1d')
    async def orderbook(self, *symbols):
        length = len(symbols)
        symbols = list(symbols)
        if length == 1:
            return self.exchange.watch_order_book(symbols[0])
        elif length > 1:
            return self.exchange.watch_orders(symbols)
        
    # async def fetch_ticker(self, *symbols):
    #     if len(symbols) == 1:

    #     ticker = await self.exchange.fetchTicker(symbol)
    #     print(self.exchange.id, symbol, ticker)
    #     return ticker


    # async def fetch_tickers(self):
    #     await self.exchange.load_markets()
    #     symbols_to_load = get_active_symbols(self.exchange)
    #     input_coroutines = [fetch_ticker(self.exchange, symbol) for symbol in symbols_to_load]
    #     tickers = await asyncio.gather(*input_coroutines, return_exceptions=True)
    #     for ticker, symbol in zip(tickers, symbols_to_load):
    #         if not isinstance(ticker, dict):
    #             print(self.exchange.id, symbol, 'error')
    #         else:
    #             print(self.exchange.id, symbol, 'ok')
    #     print(self.exchange.id, 'fetched', len(list(tickers)), 'tickers')
    async def update_tickers_cache(self):
        """更新交易对行情缓存"""
        async with self.lock:
            try:
                tickers = await self.exchange.fetch_tickers()
                # 将字典转换为Polars DataFrame
                data = []
                for symbol, ticker in tickers.items():
                    data.append({
                        'symbol': symbol,
                        'timestamp': ticker.get('timestamp', int(time.time() * 1000)),
                        'datetime': ticker.get('datetime', datetime.now().isoformat()),
                        'high': ticker.get('high'),
                        'low': ticker.get('low'),
                        'bid': ticker.get('bid'),
                        'ask': ticker.get('ask'),
                        'last': ticker.get('last'),
                        'baseVolume': ticker.get('baseVolume'),
                        'quoteVolume': ticker.get('quoteVolume')
                    })
                
                self.tickers_cache = pl.DataFrame(data)
                self.last_updated['tickers'] = datetime.now()
                print(f"Ticker cache updated with {len(data)} symbols")
            except Exception as e:
                print(f"Error updating tickers cache: {e}")
    
    async def update_ohlcv_cache(self, symbol, timeframe):
        """更新K线数据缓存"""
        async with self.lock:
            try:
                ohlcv = await self.exchange.fetch_ohlcv(symbol, timeframe)
                key = f"{symbol}_{timeframe}"
                
                # 转换OHLCV数据为DataFrame
                columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
                df = pl.DataFrame(ohlcv, columns=columns)
                df = df.with_columns(
                    pl.from_epoch("timestamp", time_unit="ms").alias("datetime")
                )
                
                self.ohlcv_cache[key] = df
                self.last_updated[key] = datetime.now()
                print(f"OHLCV cache updated for {symbol} {timeframe}: {len(df)} bars")
            except Exception as e:
                print(f"Error updating OHLCV cache for {symbol} {timeframe}: {e}")
    
    async def update_order_book_cache(self, symbol, limit=100):
        """更新订单簿缓存"""
        async with self.lock:
            try:
                order_book = await self.exchange.fetch_order_book(symbol, limit)
                self.orders_cache[symbol] = {
                    'timestamp': order_book.get('timestamp', int(time.time() * 1000)),
                    'datetime': order_book.get('datetime', datetime.now().isoformat()),
                    'bids': order_book.get('bids', []),
                    'asks': order_book.get('asks', [])
                }
                self.last_updated[f'orders_{symbol}'] = datetime.now()
                print(f"Order book cache updated for {symbol}")
            except Exception as e:
                print(f"Error updating order book cache for {symbol}: {e}")
    
    async def watch_tickers(self, interval_seconds=30):
        """使用watch API异步监听行情更新"""
        task_name = 'watch_tickers'
        if task_name in self.tasks:
            return  # 已经在运行
        
        async def _watch_tickers():
            while True:
                try:
                    tickers = await self.exchange.watch_tickers()
                    async with self.lock:
                        # 处理接收到的行情数据
                        data = []
                        for symbol, ticker in tickers.items():
                            data.append({
                                'symbol': symbol,
                                'timestamp': ticker.get('timestamp', int(time.time() * 1000)),
                                'datetime': ticker.get('datetime', datetime.now().isoformat()),
                                'high': ticker.get('high'),
                                'low': ticker.get('low'),
                                'bid': ticker.get('bid'),
                                'ask': ticker.get('ask'),
                                'last': ticker.get('last'),
                                'baseVolume': ticker.get('baseVolume'),
                                'quoteVolume': ticker.get('quoteVolume')
                            })
                        
                        # 更新缓存
                        if self.tickers_cache is not None:
                            new_df = pl.DataFrame(data)
                            # 合并新旧数据，保留最新的记录
                            self.tickers_cache = (
                                self.tickers_cache
                                .join(new_df, on='symbol', how='outer', suffix='_new')
                                .select([
                                    pl.col('symbol'),
                                    pl.col('timestamp_new').fill_null(pl.col('timestamp')).alias('timestamp'),
                                    pl.col('datetime_new').fill_null(pl.col('datetime')).alias('datetime'),
                                    pl.col('high_new').fill_null(pl.col('high')).alias('high'),
                                    pl.col('low_new').fill_null(pl.col('low')).alias('low'),
                                    pl.col('bid_new').fill_null(pl.col('bid')).alias('bid'),
                                    pl.col('ask_new').fill_null(pl.col('ask')).alias('ask'),
                                    pl.col('last_new').fill_null(pl.col('last')).alias('last'),
                                    pl.col('baseVolume_new').fill_null(pl.col('baseVolume')).alias('baseVolume'),
                                    pl.col('quoteVolume_new').fill_null(pl.col('quoteVolume')).alias('quoteVolume')
                                ])
                            )
                        else:
                            self.tickers_cache = pl.DataFrame(data)
                        
                        self.last_updated['tickers'] = datetime.now()
                        print(f"Ticker cache updated via watch with {len(data)} symbols")
                except Exception as e:
                    print(f"Error watching tickers: {e}")
                    await asyncio.sleep(5)  # 出错后等待一段时间再重试
        
        self.tasks[task_name] = asyncio.create_task(_watch_tickers())
    
    async def watch_ohlcv(self, symbol, timeframe, interval_seconds=60):
        """使用watch API异步监听K线数据更新"""
        task_name = f'watch_ohlcv_{symbol}_{timeframe}'
        if task_name in self.tasks:
            return  # 已经在运行
        
        async def _watch_ohlcv():
            while True:
                try:
                    ohlcv = await self.exchange.watch_ohlcv(symbol, timeframe)
                    async with self.lock:
                        key = f"{symbol}_{timeframe}"
                        # 处理接收到的K线数据
                        columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
                        new_df = pl.DataFrame(ohlcv, columns=columns)
                        new_df = new_df.with_columns(
                            pl.from_epoch("timestamp", time_unit="ms").alias("datetime")
                        )
                        
                        # 更新缓存
                        if key in self.cache:
                            # 合并新旧数据，保留最新的记录
                            self.ohlcv_cache[key] = (
                                self.ohlcv_cache[key]
                                .join(new_df, on='timestamp', how='outer', suffix='_new')
                                .select([
                                    pl.col('timestamp'),
                                    pl.col('datetime_new').fill_null(pl.col('datetime')).alias('datetime'),
                                    pl.col('open_new').fill_null(pl.col('open')).alias('open'),
                                    pl.col('high_new').fill_null(pl.col('high')).alias('high'),
                                    pl.col('low_new').fill_null(pl.col('low')).alias('low'),
                                    pl.col('close_new').fill_null(pl.col('close')).alias('close'),
                                    pl.col('volume_new').fill_null(pl.col('volume')).alias('volume')
                                ])
                                .sort('timestamp')
                            )
                        else:
                            self.ohlcv_cache[key] = new_df
                        
                        self.last_updated[key] = datetime.now()
                        print(f"OHLCV cache updated via watch for {symbol} {timeframe}: {len(new_df)} bars")
                except Exception as e:
                    print(f"Error watching OHLCV for {symbol} {timeframe}: {e}")
                    await asyncio.sleep(5)  # 出错后等待一段时间再重试
        
        self.tasks[task_name] = asyncio.create_task(_watch_ohlcv())
    
    async def watch_order_book(self, symbol, limit=100, interval_seconds=5):
        """使用watch API异步监听订单簿更新"""
        task_name = f'watch_order_book_{symbol}'
        if task_name in self.tasks:
            return  # 已经在运行
        
        async def _watch_order_book():
            while True:
                try:
                    order_book = await self.exchange.watch_order_book(symbol, limit)
                    async with self.lock:
                        self.orders_cache[symbol] = {
                            'timestamp': order_book.get('timestamp', int(time.time() * 1000)),
                            'datetime': order_book.get('datetime', datetime.now().isoformat()),
                            'bids': order_book.get('bids', []),
                            'asks': order_book.get('asks', [])
                        }
                        self.last_updated[f'orders_{symbol}'] = datetime.now()
                        print(f"Order book cache updated via watch for {symbol}")
                except Exception as e:
                    print(f"Error watching order book for {symbol}: {e}")
                    await asyncio.sleep(5)  # 出错后等待一段时间再重试
        
        self.tasks[task_name] = asyncio.create_task(_watch_order_book())
    
    async def unwatch_tickers(self):
        """停止监听行情更新"""
        task_name = 'watch_tickers'
        if task_name in self.tasks:
            self.tasks[task_name].cancel()
            del self.tasks[task_name]
            print("Stopped watching tickers")
    
    async def unwatch_ohlcv(self, symbol, timeframe):
        """停止监听K线数据更新"""
        task_name = f'watch_ohlcv_{symbol}_{timeframe}'
        if task_name in self.tasks:
            self.tasks[task_name].cancel()
            del self.tasks[task_name]
            print(f"Stopped watching OHLCV for {symbol} {timeframe}")
    
    async def unwatch_order_book(self, symbol):
        """停止监听订单簿更新"""
        task_name = f'watch_order_book_{symbol}'
        if task_name in self.tasks:
            self.tasks[task_name].cancel()
            del self.tasks[task_name]
            print(f"Stopped watching order book for {symbol}")
    
    async def cleanup_old_data(self):
        """清理过期的缓存数据"""
        async with self.lock:
            cutoff_time = datetime.now() - timedelta(minutes=self.cache_time_minutes)
            
            # 清理tickers缓存
            if self.tickers_cache is not None:
                self.tickers_cache = self.tickers_cache.filter(
                    pl.col('timestamp') >= cutoff_time.timestamp() * 1000
                )
            
            # 清理OHLCV缓存
            for key, df in list(self.ohlcv_cache.items()):
                self.ohlcv_cache[key] = df.filter(
                    pl.col('timestamp') >= cutoff_time.timestamp() * 1000
                )
            
            # 清理订单簿缓存（因为订单簿是最新的快照，不需要按时间清理）
            
            # 清理最后更新时间记录
            for key in list(self.last_updated.keys()):
                if self.last_updated[key] < cutoff_time:
                    del self.last_updated[key]
            
            print(f"Cleaned up old data (older than {self.cache_time_minutes} minutes)")
    
    async def start_cleanup_task(self, interval_minutes=5):
        """启动定期清理任务"""
        async def _cleanup_task():
            while True:
                await self.cleanup_old_data()
                await asyncio.sleep(interval_minutes * 60)
        
        self.tasks['cleanup'] = asyncio.create_task(_cleanup_task())
    
    async def close(self):
        """关闭所有任务和连接"""
        # 取消所有任务
        for task_name, task in self.tasks.items():
            task.cancel()
            print(f"Cancelled task: {task_name}")
        
        # 关闭交易所连接
        await self.exchange.close()
        print("Exchange connection closed")