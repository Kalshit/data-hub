"""
Ticker plant processor for normalizing and routing WebSocket messages.

Maintains in-memory order books per ticker, detects BBO changes, and
publishes type-specific messages to downstream subscribers.
"""
from __future__ import annotations
import asyncio
import logging
from typing import Any, Awaitable, Callable, Dict, Optional, Tuple
from .order_book import BBO, OrderBook

LOGGER = logging.getLogger(__name__)
PublisherFn = Callable[[str, Dict[str, Any]], Awaitable[None]]

class InMemoryPublisher:
    """
    In-memory message collector for testing and development.
    
    Buffers published messages by channel for later inspection.
    Production deployments should replace this with multicast UDP
    or Aeron IPC publishers as described in the architecture docs.
    """
    def __init__(self) -> None:
        self.messages: Dict[str, list[Dict[str, Any]]] = {}
    
    async def publish(self, channel: str, payload: Dict[str, Any]) -> None:
        """Append message to channel-specific buffer."""
        self.messages.setdefault(channel, []).append(payload)

class TickerPlantProcessor:
    """
    Central message processor maintaining order books and routing updates.
    
    Receives raw WebSocket messages, reconstructs order books, detects
    best bid/offer changes, and fans out to typed pub/sub channels.
    
    Attributes:
        publisher: Async callable accepting (channel, payload) pairs
        order_books: Per-ticker order book instances
        last_bbo: Cached (bid, ask) prices for change detection
    """
    def __init__(self, publisher: PublisherFn) -> None:
        self.publisher = publisher
        self.order_books: Dict[str, OrderBook] = {}
        self.last_bbo: Dict[str, Tuple[Optional[float], Optional[float]]] = {}
    
    async def handle_message(self, message: Dict[str, Any]) -> None:
        """
        Route incoming message to appropriate handler.
        
        Args:
            message: WebSocket payload with "type" discriminator field
        """
        msg_type = message.get("type")
        if msg_type == "orderbook_delta":
            await self._handle_orderbook_delta(message)
        elif msg_type == "trade":
            await self.publisher("trade", message)
        elif msg_type == "ticker":
            await self.publisher("ticker", message)
        else:
            LOGGER.debug("Ignoring unsupported message type: %s", msg_type)
    
    async def _handle_orderbook_delta(self, message: Dict[str, Any]) -> None:
        """
        Apply order book delta and conditionally publish BBO update.
        
        Updates in-memory book, publishes raw delta, then computes new BBO.
        If top of book changed, publishes separate BBO message for clients
        only interested in best prices.
        
        Args:
            message: Delta payload with ticker, side, price, delta_size
        """
        ticker = message["ticker"]
        side = message["side"]
        price = message["price"]
        delta = message["delta_size"]
        sequence = message.get("sequence")
        timestamp = message.get("timestamp")
        book = self._get_order_book(ticker)
        book.apply_delta(
            side=side,
            price=price,
            delta_size=delta,
            sequence=sequence,
            timestamp_ms=timestamp,
        )
        await self.publisher("orderbook_delta", message)
        bbo = book.get_bbo()
        if self._should_publish_bbo(bbo):
            await self.publisher("bbo", bbo.__dict__)
    
    def _get_order_book(self, ticker: str) -> OrderBook:
        """Lazy-initialize order book for ticker on first reference."""
        if ticker not in self.order_books:
            self.order_books[ticker] = OrderBook(ticker)
        return self.order_books[ticker]
    
    def _should_publish_bbo(self, bbo: BBO) -> bool:
        """
        Determine if BBO changed since last publication.
        
        Caches (bid, ask) tuple per ticker to suppress redundant
        BBO messages when only non-top levels update.
        
        Args:
            bbo: Current best bid/offer snapshot
            
        Returns:
            True if bid or ask price differs from cached values
        """
        last_bid, last_ask = self.last_bbo.get(bbo.ticker, (None, None))
        changed = (bbo.bid_price, bbo.ask_price) != (last_bid, last_ask)
        if changed:
            self.last_bbo[bbo.ticker] = (bbo.bid_price, bbo.ask_price)
        return changed
