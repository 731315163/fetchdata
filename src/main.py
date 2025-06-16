import ccxt.pro as ccxtpro
from asyncio import run

async def main():
    exchange = ccxtpro.kraken({'newUpdates': False})
    while True:
        orderbook = await exchange.watch_order_book('BTC/USD')
        print(orderbook['asks'][0], orderbook['bids'][0])
    await exchange.close()


run(main())