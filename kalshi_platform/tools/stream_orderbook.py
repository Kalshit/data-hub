"""
Stream real-time order book data from Kalshi WebSocket to QuestDB RDB.

Connects to Kalshi WebSocket, subscribes to orderbook_delta/trade/ticker
channels, and persists to QuestDB via ILP for sub-millisecond writes.
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

from kalshi_platform.config import KalshiAPIConfig, QuestDBConfig, WebSocketConfig, ensure_env_loaded
from kalshi_platform.ingestion.ws_client import KalshiWebSocketClient
from kalshi_platform.storage.questdb_rdb import QuestDBRDBConfig, QuestDBRDBWriter
from kalshi_platform.api.public_client import PublicKalshiClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


class OrderBookStreamer:
    """
    Streams order book data from Kalshi WebSocket to QuestDB.
    
    Handles message routing, batching, and periodic flushing.
    """
    
    def __init__(
        self,
        writer: QuestDBRDBWriter,
        flush_interval: float = 1.0,
    ) -> None:
        self.writer = writer
        self.flush_interval = flush_interval
        self.stats = {"trades": 0, "deltas": 0, "tickers": 0, "snapshots": 0, "snapshot_levels": 0}
        self._flush_task: Optional[asyncio.Task] = None
    
    async def handle_message(self, message: Dict[str, Any]) -> None:
        """Route incoming WebSocket message to appropriate writer."""
        msg_type = message.get("type")
        
        if msg_type in ("subscribed", "ok", "error", "pong"):
            return
        
        msg_data = message.get("msg", {})
        ticker = msg_data.get("market_ticker") or msg_data.get("ticker")
        seq = message.get("seq")
        
        if not ticker:
            logger.debug(f"Skipping message without ticker: {msg_type}")
            return
        
        msg_data["ticker"] = ticker
        msg_data["seq"] = seq
        
        if msg_type == "trade":
            self.writer.write_trade(msg_data)
            self.stats["trades"] += 1
            
        elif msg_type == "orderbook_delta":
            self.writer.write_orderbook_delta(msg_data)
            self.stats["deltas"] += 1
            
        elif msg_type == "orderbook_snapshot":
            yes_levels = msg_data.get("yes", [])
            no_levels = msg_data.get("no", [])
            self.writer.write_orderbook_snapshot(
                ticker=ticker,
                seq=seq or 0,
                yes_levels=yes_levels,
                no_levels=no_levels,
            )
            total_levels = len(yes_levels) + len(no_levels)
            self.stats["snapshots"] += 1
            self.stats["snapshot_levels"] = self.stats.get("snapshot_levels", 0) + total_levels
            logger.info(f"[SNAPSHOT] {ticker}: {len(yes_levels)} yes, {len(no_levels)} no levels")
            
        elif msg_type == "ticker":
            self.writer.write_ticker(msg_data)
            self.stats["tickers"] += 1
    
    async def periodic_flush(self) -> None:
        """Flush writer buffer periodically and log stats."""
        while True:
            await asyncio.sleep(self.flush_interval)
            self.writer.flush()
            
            total = sum(self.stats.values())
            if total > 0:
                logger.info(
                    f"Flushed: {self.stats['snapshots']} snapshots ({self.stats['snapshot_levels']} levels), "
                    f"{self.stats['deltas']} deltas, "
                    f"{self.stats['trades']} trades, "
                    f"{self.stats['tickers']} tickers"
                )
                self.stats = {"trades": 0, "deltas": 0, "tickers": 0, "snapshots": 0, "snapshot_levels": 0}
    
    def start_flush_task(self) -> None:
        """Start background flush task."""
        self._flush_task = asyncio.create_task(self.periodic_flush())
    
    def stop_flush_task(self) -> None:
        """Cancel background flush task."""
        if self._flush_task:
            self._flush_task.cancel()


def get_tickers_for_series(series: str) -> List[str]:
    """Fetch active market tickers for a series."""
    client = PublicKalshiClient()
    markets = client.summarize_markets(series, status="open")
    return [m.ticker for m in markets]


async def run_stream(
    ws_url: str,
    tickers: List[str],
    writer: QuestDBRDBWriter,
    channels: List[str],
    api_key: Optional[str] = None,
    private_key_path: Optional[Path] = None,
) -> None:
    """Main streaming loop."""
    streamer = OrderBookStreamer(writer)
    
    ws_client = KalshiWebSocketClient(
        ws_url=ws_url,
        message_handler=streamer.handle_message,
        api_key=api_key,
        private_key_path=private_key_path,
    )
    
    for channel in channels:
        ws_client.subscribe(channel, tickers)
    
    logger.info(f"Subscribing to {len(tickers)} tickers on channels: {channels}")
    logger.info("Press Ctrl+C to stop")
    
    streamer.start_flush_task()
    
    try:
        await ws_client.connect_forever()
    except asyncio.CancelledError:
        pass
    finally:
        await ws_client.stop()
        streamer.stop_flush_task()
        writer.flush()
        writer.close()
        logger.info("Stream stopped")


def main() -> None:
    ensure_env_loaded()
    env_ws = WebSocketConfig.from_env()
    env_qdb = QuestDBConfig.from_env()
    env_api = KalshiAPIConfig.from_env()
    
    parser = argparse.ArgumentParser(
        description="Stream order book data from Kalshi to QuestDB."
    )
    parser.add_argument(
        "--tickers",
        nargs="+",
        help="Market tickers to subscribe (e.g., KXBTC-25DEC31)",
    )
    parser.add_argument(
        "--series",
        help="Fetch all open tickers for this series (e.g., KXBTC)",
    )
    parser.add_argument(
        "--channels",
        nargs="+",
        default=["orderbook_delta", "trade", "ticker"],
        help="Channels to subscribe (default: orderbook_delta trade ticker)",
    )
    parser.add_argument(
        "--ws-url",
        default=env_ws.ws_url,
        help="WebSocket URL",
    )
    parser.add_argument(
        "--questdb-host",
        default=env_qdb.rdb_host,
        help="QuestDB ILP host",
    )
    parser.add_argument(
        "--questdb-port",
        type=int,
        default=env_qdb.rdb_port,
        help="QuestDB ILP port",
    )
    
    args = parser.parse_args()
    
    tickers = args.tickers or []
    if args.series:
        logger.info(f"Fetching tickers for series: {args.series}")
        tickers.extend(get_tickers_for_series(args.series))
    
    if not tickers:
        logger.error("No tickers specified. Use --tickers or --series")
        sys.exit(1)
    
    api_key = env_api.api_key
    private_key_path = Path(env_api.private_key_path) if env_api.private_key_path else None
    
    if not api_key or not private_key_path:
        logger.error("KALSHI_API_KEY and KALSHI_PRIVATE_KEY_PATH required for WebSocket auth")
        sys.exit(1)
    
    logger.info(f"Tickers: {len(tickers)}")
    logger.info(f"Channels: {args.channels}")
    logger.info(f"QuestDB: {args.questdb_host}:{args.questdb_port}")
    
    writer = QuestDBRDBWriter(
        QuestDBRDBConfig(
            host=args.questdb_host,
            port=args.questdb_port,
        )
    )
    
    asyncio.run(
        run_stream(
            ws_url=args.ws_url,
            tickers=tickers,
            writer=writer,
            channels=args.channels,
            api_key=api_key,
            private_key_path=private_key_path,
        )
    )


if __name__ == "__main__":
    main()

