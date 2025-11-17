import asyncio

from kalshi_platform.ticker_plant.processor import (
    InMemoryPublisher,
    TickerPlantProcessor,
)


def test_orderbook_delta_triggers_bbo():
    publisher = InMemoryPublisher()
    processor = TickerPlantProcessor(publisher.publish)

    delta_message = {
        "type": "orderbook_delta",
        "ticker": "TEST",
        "side": "yes",
        "price": 60.0,
        "delta_size": 50,
        "sequence": 1,
        "timestamp": 1,
    }
    asyncio.run(processor.handle_message(delta_message))

    # Add ask to produce BBO change
    delta_message_no = {
        "type": "orderbook_delta",
        "ticker": "TEST",
        "side": "no",
        "price": 70.0,
        "delta_size": 50,
        "sequence": 2,
        "timestamp": 2,
    }
    asyncio.run(processor.handle_message(delta_message_no))

    assert "bbo" in publisher.messages
    assert publisher.messages["bbo"][-1]["ticker"] == "TEST"


def test_trade_and_ticker_passthrough():
    publisher = InMemoryPublisher()
    processor = TickerPlantProcessor(publisher.publish)

    trade_msg = {
        "type": "trade",
        "ticker": "TEST",
        "yes_price": 60,
        "count": 10,
        "created_time": "2025-11-16T00:00:00Z",
        "taker_side": "yes",
    }
    ticker_msg = {
        "type": "ticker",
        "ticker": "TEST",
        "last_price": 61,
        "best_bid": 60,
        "best_ask": 40,
    }

    asyncio.run(processor.handle_message(trade_msg))
    asyncio.run(processor.handle_message(ticker_msg))

    assert publisher.messages["trade"][0]["ticker"] == "TEST"
    assert publisher.messages["ticker"][0]["last_price"] == 61

