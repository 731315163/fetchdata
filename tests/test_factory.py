from tradepulse.exchange import CCXTExchangeFactory


def test_get_exchange():
    exchange = CCXTExchangeFactory.get_exchange("bitmex", config={})
    assert exchange.id == "bitmex"
    assert exchange.name == "BitMEX"
    assert exchange.has["watchOrderBook"]
    assert exchange.has["watchTrades"]
    assert exchange.has["watchTicker"]
    assert exchange.has["watchOHLCV"]
    assert exchange.has["watchBalance"]

