"""
Async Kalshi WebSocket client with gap detection.

Manages persistent connections to wss://api.elections.kalshi.com
with exponential backoff, heartbeat, and sequence validation.
"""
from __future__ import annotations
import asyncio
import contextlib
import json
import logging
import time
from typing import Any, Awaitable, Callable, Dict, Iterable, Optional, Set
import websockets
from websockets.exceptions import ConnectionClosedError, ConnectionClosedOK

MessageHandler = Callable[[Dict[str, Any]], Awaitable[None]]
LOGGER = logging.getLogger(__name__)

class KalshiWebSocketClient:
    """
    WebSocket client for Kalshi real-time market data streams.
    
    Subscribes to trade, ticker, and orderbook_delta channels per ticker,
    maintains sequence tracking for gap detection, and automatically
    reconnects with exponential backoff on disconnection.
    
    Attributes:
        ws_url: WebSocket endpoint URL
        auth_token: Optional bearer token for authenticated channels
        message_handler: Async callback invoked for each received message
        heartbeat_interval: Seconds between ping messages
        reconnect_backoff: Initial backoff delay in seconds (doubles on retry)
    """
    def __init__(
        self,
        ws_url: str,
        auth_token: Optional[str],
        message_handler: MessageHandler,
        heartbeat_interval: float = 15.0,
        reconnect_backoff: float = 2.0,
    ) -> None:
        self.ws_url = ws_url
        self.auth_token = auth_token
        self.message_handler = message_handler
        self.heartbeat_interval = heartbeat_interval
        self.reconnect_backoff = reconnect_backoff
        self._subscriptions: Dict[str, Set[str]] = {}
        self._last_sequence: Dict[str, int] = {}
        self._last_heartbeat = time.time()
        self._running = False
    
    def subscribe(self, channel: str, tickers: Iterable[str]) -> None:
        """
        Register interest in a channel for given tickers.
        
        Args:
            channel: One of "trade", "ticker", "orderbook_delta"
            tickers: Market identifiers to subscribe
        """
        merged = self._subscriptions.setdefault(channel, set())
        merged.update(token.upper() for token in tickers)
    
    async def connect_forever(self) -> None:
        """
        Main loop maintaining persistent connection with exponential backoff.
        
        Continues reconnecting until stop() is called. Backoff resets on
        successful connection, doubles on failure up to 60 second maximum.
        """
        self._running = True
        backoff = self.reconnect_backoff
        while self._running:
            try:
                headers = {}
                if self.auth_token:
                    headers["Authorization"] = (
                        f"Bearer {self.auth_token}"
                    )
                LOGGER.info("Connecting to %s", self.ws_url)
                async with websockets.connect(
                    self.ws_url, extra_headers=headers
                ) as ws:
                    await self._on_connect(ws)
                    await self._listen(ws)
                backoff = self.reconnect_backoff
            except (ConnectionClosedError, ConnectionClosedOK, OSError) as exc:
                LOGGER.warning("WebSocket disconnected: %s", exc)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)
    
    async def stop(self) -> None:
        """Signal connect_forever to exit its reconnection loop."""
        self._running = False
    
    def detect_sequence_gap(self, ticker: str, sequence: int) -> bool:
        """
        Check if sequence number indicates missing messages.
        
        Args:
            ticker: Market identifier
            sequence: Current message sequence number
            
        Returns:
            True if gap detected (sequence != last + 1), False otherwise
        """
        last = self._last_sequence.get(ticker)
        self._last_sequence[ticker] = sequence
        if last is None:
            return False
        return sequence != last + 1
    
    async def _on_connect(
        self, ws: websockets.WebSocketClientProtocol
    ) -> None:
        """Initialize heartbeat and push subscriptions."""
        self._last_heartbeat = time.time()
        await self._push_subscriptions(ws)
    
    async def _push_subscriptions(
        self, ws: websockets.WebSocketClientProtocol
    ) -> None:
        """
        Send subscription message to server.
        
        Args:
            ws: Active WebSocket connection
        """
        if not self._subscriptions:
            return
        payload = []
        for channel, tickers in self._subscriptions.items():
            payload.append(
                {"channel": channel, "tickers": sorted(tickers)}
            )
        await ws.send(
            json.dumps({"type": "subscribe", "subscriptions": payload})
        )
    
    async def _listen(self, ws: websockets.WebSocketClientProtocol) -> None:
        """
        Consume messages from WebSocket until disconnection.
        
        Spawns heartbeat monitor task and ensures cleanup on exit.
        
        Args:
            ws: Active WebSocket connection
        """
        heartbeat_task = asyncio.create_task(self._monitor_heartbeat(ws))
        try:
            async for raw in ws:
                self._last_heartbeat = time.time()
                message = json.loads(raw)
                await self._handle_message(message)
        finally:
            heartbeat_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await heartbeat_task
    
    async def _monitor_heartbeat(
        self, ws: websockets.WebSocketClientProtocol
    ) -> None:
        """
        Periodically send pings and close connection if no data received.
        
        Args:
            ws: Active WebSocket connection
        """
        while True:
            await asyncio.sleep(self.heartbeat_interval)
            elapsed = time.time() - self._last_heartbeat
            if elapsed > self.heartbeat_interval * 2:
                LOGGER.warning("Heartbeat timeout, closing")
                await ws.close()
                break
            await ws.send(json.dumps({"type": "ping"}))
    
    async def _handle_message(self, message: Dict[str, Any]) -> None:
        """
        Process incoming message, detect gaps, and forward to handler.
        
        Args:
            message: Parsed JSON payload from WebSocket
        """
        ticker = message.get("ticker")
        sequence = message.get("sequence")
        if ticker and sequence is not None:
            if self.detect_sequence_gap(ticker, sequence):
                LOGGER.warning("Sequence gap detected for %s", ticker)
        await self.message_handler(message)

__all__ = ["KalshiWebSocketClient"]
