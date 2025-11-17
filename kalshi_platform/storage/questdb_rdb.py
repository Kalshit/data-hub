"""
QuestDB hot-tier (RDB) writer for real-time market data ingestion.

Wraps the QuestDB ILP protocol with type-safe helpers for trade, delta,
ticker, and BBO message formats. Supports sub-millisecond latency writes
to in-memory tables.
"""
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from questdb.ingress import Protocol, Sender, TimestampNanos  # type: ignore

def _timestamp_ns(timestamp_ms: Optional[int] = None) -> TimestampNanos:
    """
    Convert millisecond timestamp to nanoseconds for QuestDB.
    
    Args:
        timestamp_ms: Unix milliseconds, defaults to current time
        
    Returns:
        Nanosecond precision timestamp as integer
    """
    if timestamp_ms is None:
        timestamp_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
    return TimestampNanos(timestamp_ms * 1_000_000)

@dataclass
class QuestDBRDBConfig:
    """Configuration for QuestDB ILP connection."""
    host: str = "localhost"
    port: int = 9009
    username: Optional[str] = None
    password: Optional[str] = None
    use_tls: bool = False
    buffer_capacity: int = 1024 * 1024

class QuestDBRDBWriter:
    """
    Domain-aware QuestDB writer for Kalshi market data.
    
    Provides typed write methods for each message kind (trade, delta, ticker,
    BBO) with automatic schema mapping and timestamp conversion. Delegates
    to the official QuestDB ILP Sender for wire protocol handling.
    
    Attributes:
        config: Connection and buffer settings
    """
    def __init__(
        self,
        config: QuestDBRDBConfig,
        sender: Optional[Any] = None,
    ) -> None:
        self.config = config
        self._sender = sender or self._build_sender()
    
    def write_trade(self, message: Dict[str, Any]) -> None:
        """
        Write trade execution to 'trades' table.
        
        Args:
            message: Trade payload with ticker, price, count, taker_side
        """
        self._sender.row(
            "trades",
            symbols={
                "ticker": message["ticker"],
                "taker_side": message.get("taker_side", "yes"),
            },
            columns={
                "price": message.get("price"),
                "yes_price": message.get("yes_price"),
                "no_price": message.get("no_price"),
                "count": message.get("count"),
                "trade_id": message.get("trade_id"),
            },
            at=_timestamp_ns(message.get("timestamp")),
        )
    
    def write_orderbook_delta(self, message: Dict[str, Any]) -> None:
        """
        Write order book delta to 'orderbook_deltas' table.
        
        Args:
            message: Delta payload with ticker, side, price, delta_size
        """
        self._sender.row(
            "orderbook_deltas",
            symbols={
                "ticker": message["ticker"],
                "side": message.get("side"),
            },
            columns={
                "price": message.get("price"),
                "delta_size": message.get("delta_size"),
                "sequence": message.get("sequence"),
            },
            at=_timestamp_ns(message.get("timestamp")),
        )
    
    def write_ticker(self, message: Dict[str, Any]) -> None:
        """
        Write ticker snapshot to 'tickers' table.
        
        Args:
            message: Ticker payload with last price, volume, open interest
        """
        self._sender.row(
            "tickers",
            symbols={"ticker": message["ticker"]},
            columns={
                "last_price": message.get("last_price"),
                "best_bid": message.get("best_bid"),
                "best_ask": message.get("best_ask"),
                "volume": message.get("volume"),
                "open_interest": message.get("open_interest"),
            },
            at=_timestamp_ns(message.get("timestamp")),
        )
    
    def write_bbo(self, message: Dict[str, Any]) -> None:
        """
        Write best bid/offer to 'bbo' table.
        
        Args:
            message: BBO payload with bid/ask prices and sizes
        """
        self._sender.row(
            "bbo",
            symbols={"ticker": message["ticker"]},
            columns={
                "bid_price": message.get("bid_price"),
                "bid_size": message.get("bid_size"),
                "ask_price": message.get("ask_price"),
                "ask_size": message.get("ask_size"),
                "mid_price": message.get("mid_price"),
                "spread": message.get("spread"),
            },
            at=_timestamp_ns(message.get("timestamp")),
        )
    
    def flush(self) -> None:
        """Force buffered rows to QuestDB immediately."""
        self._sender.flush()
    
    def close(self) -> None:
        """Flush pending rows and close ILP connection."""
        self._sender.close()
    
    def _build_sender(self) -> Any:
        """Construct QuestDB Sender from stored configuration."""
        protocol = Protocol.Tcps if self.config.use_tls else Protocol.Tcp
        sender = Sender(
            protocol=protocol,
            host=self.config.host,
            port=self.config.port,
            username=self.config.username,
            password=self.config.password,
            init_buf_size=self.config.buffer_capacity,
        )
        sender.establish()
        return sender
