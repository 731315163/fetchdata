from fetchdata import ExchangeFactory





def test_get_exchange():
    exchange = ExchangeFactory.get_exchange("bitmex", config={})
    assert exchange.id == "bitmex"
    assert exchange.name == "BitMEX"
    assert exchange.has["watchOrderBook"]
    assert exchange.has["watchTrades"]
    assert exchange.has["watchTicker"]
    assert exchange.has["watchOHLCV"]
    assert exchange.has["watchBalance"]
