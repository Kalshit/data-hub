from kalshi_platform.ticker_plant.order_book import OrderBook


def test_order_book_generates_bbo_and_depth():
    book = OrderBook("TEST")
    book.apply_delta("yes", 62.0, 100, sequence=1, timestamp_ms=1)
    book.apply_delta("yes", 61.0, 50, sequence=2, timestamp_ms=2)
    book.apply_delta("no", 45.0, 80, sequence=3, timestamp_ms=3)

    bbo = book.get_bbo()
    assert bbo.bid_price == 62.0
    assert bbo.ask_price == 45.0
    assert bbo.mid_price == (62.0 + 45.0) / 2

    depth = book.get_depth(levels=2)
    assert depth["yes"][0] == (62.0, 100)
    assert depth["yes"][1] == (61.0, 50)
    assert depth["no"][0] == (45.0, 80)


def test_order_book_cross_detection():
    book = OrderBook("TEST")
    book.apply_delta("yes", 60.0, 10)
    book.apply_delta("no", 65.0, 10)
    assert not book.is_crossed()

    book.apply_delta("no", 58.0, 5)
    assert book.is_crossed()

