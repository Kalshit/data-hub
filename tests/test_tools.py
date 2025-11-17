from kalshi_platform.tools.historical_fetcher import HistoricalDataFetcher
from kalshi_platform.tools.market_data_recorder import MarketDataRecorder


class FakeWriter:
    def __init__(self):
        self.trades = []
        self.deltas = []
        self.tickers = []
        self.bbos = []

    def write_trade(self, message):
        self.trades.append(message)

    def write_orderbook_delta(self, message):
        self.deltas.append(message)

    def write_ticker(self, message):
        self.tickers.append(message)

    def write_bbo(self, message):
        self.bbos.append(message)

    def flush(self):
        return None

    def close(self):
        return None


def test_market_data_recorder_batches_messages():
    writer = FakeWriter()
    recorder = MarketDataRecorder(writer, batch_size=2)
    recorder.record("trade", {"ticker": "TEST"})
    recorder.record("orderbook_delta", {"ticker": "TEST"})
    recorder.flush()

    assert len(writer.trades) == 1
    assert len(writer.deltas) == 1


class DummySigner:
    def build_headers(self, method: str, path: str):
        return {"KALSHI-ACCESS-KEY": "dummy"}


class FakeResponse:
    def __init__(self, status_code: int, payload: dict):
        self.status_code = status_code
        self._payload = payload

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400 and self.status_code != 429:
            raise RuntimeError("HTTP error")


class FakeSession:
    def __init__(self):
        self.calls = 0

    def get(self, url, params=None, headers=None, timeout=None):
        self.calls += 1
        if self.calls == 1:
            return FakeResponse(429, {})
        return FakeResponse(
            200,
            {
                "trades": [
                    {"ticker": "TEST", "trade_id": "1", "timestamp": 0},
                    {"ticker": "TEST", "trade_id": "2", "timestamp": 1},
                ],
                "cursor": "",
            },
        )


def test_historical_fetcher_retries_and_writes():
    writer = FakeWriter()
    session = FakeSession()
    fetcher = HistoricalDataFetcher(
        base_url="https://demo-api.kalshi.co",
        signer=DummySigner(),  # type: ignore[arg-type]
        writer=writer,
        session=session,  # type: ignore[arg-type]
        sleep_fn=lambda _: None,
    )

    total = fetcher.fetch_trades("TEST")
    assert total == 2
    assert len(writer.trades) == 2

