"""
Comprehensive Kalshi historical backfill to QuestDB HDB.

Fetches and persists:
- All series metadata
- Events within each series
- Markets within each event
- Historical trades for each market
- OHLC aggregations (daily and hourly)

Supports resume capability via progress tracking.
Supports filtering by minimum volume to focus on active markets.
"""
from __future__ import annotations

import argparse
import base64
import datetime as dt
import logging
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import requests
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey

from kalshi_platform.api.public_client import PublicKalshiClient, API_BASE_URL
from kalshi_platform.config import (
    KalshiAPIConfig,
    QuestDBConfig,
    ensure_env_loaded,
)
from kalshi_platform.storage.questdb_hdb_writer import (
    QuestDBHDBWriter,
    QuestDBHDBWriterConfig,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

MAX_RETRY_ATTEMPTS = 10
TRADE_BATCH_SIZE = 100
METADATA_BATCH_SIZE = 50

# Kalshi public market data URL template
MARKET_DATA_URL = "https://kalshi-public-docs.s3.amazonaws.com/reporting/market_data_{date}.json"


TICKER_CACHE_FILE = Path("high_volume_tickers_cache.json")


def fetch_high_volume_tickers(
    min_volume: int = 10000,
    start_date: Optional[dt.date] = None,
    end_date: Optional[dt.date] = None,
    use_cache: bool = True,
) -> Set[str]:
    """
    Fetch list of tickers with volume >= min_volume from Kalshi's daily data dumps.
    
    Scans ALL days in the date range because Kalshi has different tickers each day
    (daily contracts like KXBTC-25NOV24, KXBTC-25NOV25, etc.).
    
    The daily market data JSON contains records with:
    - ticker_name: The market ticker (e.g., "AMAZONFTC-29DEC31")
    - daily_volume: Daily trading volume
    - block_volume: Block trading volume
    
    Args:
        min_volume: Minimum daily_volume or block_volume to include
        start_date: Start of date range (defaults to 2024-01-01)
        end_date: End of date range (defaults to yesterday)
        use_cache: If True, try to load from cache file first
        
    Returns:
        Set of ticker strings that meet the volume threshold on ANY day
    """
    import json
    
    if start_date is None:
        start_date = dt.date(2024, 1, 1)
    if end_date is None:
        end_date = dt.date.today() - dt.timedelta(days=1)
    
    # Try loading from cache
    if use_cache and TICKER_CACHE_FILE.exists():
        try:
            cache_data = json.loads(TICKER_CACHE_FILE.read_text())
            cached_min_vol = cache_data.get("min_volume", 0)
            cached_start = cache_data.get("start_date", "")
            cached_end = cache_data.get("end_date", "")
            cached_tickers = set(cache_data.get("tickers", []))
            
            # Use cache if it matches our parameters (or has lower volume threshold)
            if (
                cached_min_vol <= min_volume
                and cached_start == str(start_date)
                and cached_end == str(end_date)
                and cached_tickers
            ):
                logger.info(
                    f"Loaded {len(cached_tickers)} tickers from cache "
                    f"(min_volume={cached_min_vol}, {cached_start} to {cached_end})"
                )
                return cached_tickers
        except (json.JSONDecodeError, KeyError) as e:
            logger.warning(f"Could not load ticker cache: {e}")
    
    high_volume_tickers: Set[str] = set()
    days_processed = 0
    days_failed = 0
    
    # Calculate total days for progress
    total_days = (end_date - start_date).days + 1
    logger.info(f"Scanning {total_days} days of market data for high-volume tickers...")
    
    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime("%Y-%m-%d")
        url = MARKET_DATA_URL.format(date=date_str)
        
        try:
            response = requests.get(url, timeout=120)
            response.raise_for_status()
            data = response.json()
            
            # Data is a list of market records
            contracts = data if isinstance(data, list) else []
            day_count = 0
            
            for contract in contracts:
                daily_vol = contract.get("daily_volume", 0) or 0
                block_vol = contract.get("block_volume", 0) or 0
                
                if daily_vol >= min_volume or block_vol >= min_volume:
                    ticker = contract.get("ticker_name")
                    if ticker and ticker not in high_volume_tickers:
                        high_volume_tickers.add(ticker)
                        day_count += 1
            
            days_processed += 1
            if days_processed % 30 == 0 or day_count > 0:
                logger.info(
                    f"  {date_str}: +{day_count} new tickers "
                    f"(total: {len(high_volume_tickers)}, "
                    f"progress: {days_processed}/{total_days})"
                )
                
        except requests.RequestException as e:
            days_failed += 1
            if days_failed <= 5:  # Only log first few failures
                logger.debug(f"  {date_str}: not available ({e})")
        
        current_date += dt.timedelta(days=1)
    
    logger.info(
        f"Scanned {days_processed} days, found {len(high_volume_tickers)} "
        f"tickers with volume >= {min_volume:,}"
    )
    
    if days_failed > 0:
        logger.info(f"  ({days_failed} days had no data available)")
    
    # Save to cache
    if high_volume_tickers:
        cache_data = {
            "min_volume": min_volume,
            "start_date": str(start_date),
            "end_date": str(end_date),
            "tickers": list(high_volume_tickers),
        }
        TICKER_CACHE_FILE.write_text(json.dumps(cache_data))
        logger.info(f"Saved {len(high_volume_tickers)} tickers to cache file")
    
    return high_volume_tickers


@dataclass
class KalshiSigner:
    """RSA-PSS request signer per https://docs.kalshi.com."""
    
    api_key: str
    private_key_path: Path
    
    def __post_init__(self) -> None:
        """Load PEM private key from disk."""
        with self.private_key_path.open("rb") as f:
            self.private_key: RSAPrivateKey = (
                serialization.load_pem_private_key(
                    f.read(), password=None, backend=default_backend()
                )
            )
    
    def build_headers(self, method: str, path: str) -> Dict[str, str]:
        """Generate ACCESS-KEY, ACCESS-SIGNATURE, ACCESS-TIMESTAMP."""
        ts_ms = int(time.time() * 1000)
        msg = f"{ts_ms}{method}{path}"
        sig = self.private_key.sign(
            msg.encode("utf-8"),
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.DIGEST_LENGTH
            ),
            hashes.SHA256()
        )
        return {
            "KALSHI-ACCESS-KEY": self.api_key,
            "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode(),
            "KALSHI-ACCESS-TIMESTAMP": str(ts_ms)
        }


class AuthenticatedClient:
    """
    Authenticated Kalshi API client for trade history.
    
    Uses RSA-PSS signing for authenticated endpoints while
    delegating unauthenticated calls to PublicKalshiClient.
    """
    
    def __init__(
        self,
        base_url: str,
        signer: KalshiSigner,
        session: Optional[requests.Session] = None,
    ) -> None:
        # Normalize base_url to include /trade-api/v2
        normalized = base_url.rstrip("/")
        if "/trade-api/" not in normalized:
            normalized = f"{normalized}/trade-api/v2"
        self.base_url = normalized
        self.signer = signer
        self.session = session or requests.Session()
    
    def _request(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Signed GET with exponential backoff on rate limit."""
        for attempt in range(MAX_RETRY_ATTEMPTS):
            hdrs = self.signer.build_headers("GET", path)
            resp = self.session.get(
                f"{self.base_url}{path}",
                params=params,
                headers=hdrs,
                timeout=30,
            )
            if resp.status_code == 429:
                sleep_time = min(60, 2 ** attempt)
                logger.warning(f"Rate limited, sleeping {sleep_time}s...")
                time.sleep(sleep_time)
                continue
            resp.raise_for_status()
            return resp.json()
        raise RuntimeError(f"Max retries ({MAX_RETRY_ATTEMPTS}) exceeded")
    
    def fetch_trades_paginated(
        self,
        ticker: str,
        min_ts: Optional[int] = None,
        max_ts: Optional[int] = None,
        limit: int = 1000,
    ) -> List[Dict[str, Any]]:
        """
        Fetch all trades for a ticker with cursor pagination.
        
        Args:
            ticker: Market ticker
            min_ts: Minimum timestamp (Unix seconds)
            max_ts: Maximum timestamp (Unix seconds)
            limit: Results per page
            
        Returns:
            List of all trades in the range
        """
        path = "/markets/trades"
        params: Dict[str, Any] = {"limit": limit, "ticker": ticker}
        if min_ts is not None:
            params["min_ts"] = min_ts
        if max_ts is not None:
            params["max_ts"] = max_ts
        
        all_trades: List[Dict[str, Any]] = []
        cursor: Optional[str] = None
        
        for _ in range(10000):  # Safety limit
            if cursor:
                params["cursor"] = cursor
            payload = self._request(path, params)
            trades = payload.get("trades", [])
            all_trades.extend(trades)
            cursor = payload.get("cursor")
            if not cursor:
                break
        
        return all_trades


class FullBackfiller:
    """
    Orchestrates complete historical backfill workflow.
    
    Workflow:
    1. Fetch all series -> persist to series table
    2. For each series, fetch events -> persist to events table
    3. For each event, fetch markets -> persist to markets table
    4. For each market, backfill trades day-by-day -> trades_hdb table
    5. Generate OHLC aggregations from trades
    """
    
    def __init__(
        self,
        public_client: PublicKalshiClient,
        auth_client: AuthenticatedClient,
        hdb_writer: QuestDBHDBWriter,
        start_date: dt.date,
        end_date: Optional[dt.date] = None,
        series_filter: Optional[str] = None,
        high_volume_tickers: Optional[Set[str]] = None,
    ) -> None:
        self.public_client = public_client
        self.auth_client = auth_client
        self.hdb_writer = hdb_writer
        self.start_date = start_date
        self.end_date = end_date or dt.date.today()
        self.series_filter = series_filter
        self.high_volume_tickers = high_volume_tickers  # None means no filter
        
        # Statistics
        self.stats = {
            "series": 0,
            "events": 0,
            "markets": 0,
            "markets_skipped": 0,
            "trades": 0,
            "candlesticks": 0,
        }
    
    def run(self) -> Dict[str, int]:
        """
        Execute full backfill workflow.
        
        Returns:
            Statistics dictionary with counts
        """
        logger.info("=" * 60)
        logger.info("Starting full historical backfill")
        logger.info(f"Date range: {self.start_date} to {self.end_date}")
        if self.series_filter:
            logger.info(f"Series filter: {self.series_filter}")
        if self.high_volume_tickers is not None:
            logger.info(f"Volume filter: {len(self.high_volume_tickers)} high-volume tickers")
        logger.info("=" * 60)
        
        logger.info("Connecting to QuestDB...")
        self.hdb_writer.reconnect()
        
        logger.info("Creating HDB tables...")
        self.hdb_writer.create_tables()
        
        logger.info("Fetching series metadata...")
        series_list = self._fetch_series()
        
        for series in series_list:
            series_ticker = series.get("ticker")
            if not series_ticker:
                continue
            
            logger.info(f"Processing series: {series_ticker}")
            
            # Fetch events for this series
            events = self._fetch_events(series_ticker)
            
            # Fetch markets for this series
            markets = self._fetch_markets(series_ticker)
            
            # Backfill trades and candlesticks for each market
            for market in markets:
                market_ticker = market.get("ticker")
                if not market_ticker:
                    continue
                
                # Skip low-volume tickers if filter is active
                if self.high_volume_tickers is not None:
                    if market_ticker not in self.high_volume_tickers:
                        self.stats["markets_skipped"] += 1
                        continue
                
                self._backfill_trades(market_ticker)
                self._backfill_candlesticks(series_ticker, market_ticker)
        
        # Generate OHLC aggregations from trades
        logger.info("Generating OHLC aggregations from trades...")
        self._generate_ohlc()
        
        logger.info("=" * 60)
        logger.info("Backfill complete!")
        logger.info(f"Series: {self.stats['series']}")
        logger.info(f"Events: {self.stats['events']}")
        logger.info(f"Markets: {self.stats['markets']}")
        if self.stats['markets_skipped'] > 0:
            logger.info(f"Markets skipped (low volume): {self.stats['markets_skipped']}")
        logger.info(f"Trades: {self.stats['trades']}")
        logger.info(f"Candlesticks: {self.stats['candlesticks']}")
        logger.info(f"OHLC Daily: {self.stats.get('ohlc_daily', 0)}")
        logger.info(f"OHLC Hourly: {self.stats.get('ohlc_hourly', 0)}")
        logger.info("=" * 60)
        
        return self.stats
    
    def _fetch_series(self) -> List[Dict[str, Any]]:
        """Fetch and persist all series."""
        series_list: List[Dict[str, Any]] = []
        batch: List[Dict[str, Any]] = []
        
        try:
            for series in self.public_client.list_series():
                series_ticker = series.get("ticker", "")
                
                # Apply filter if specified
                if self.series_filter:
                    if not series_ticker.upper().startswith(
                        self.series_filter.upper()
                    ):
                        continue
                
                series_list.append(series)
                batch.append(series)
                
                if len(batch) >= METADATA_BATCH_SIZE:
                    self.hdb_writer.write_series_batch(batch)
                    self.stats["series"] += len(batch)
                    batch = []
            
            # Write remaining batch
            if batch:
                self.hdb_writer.write_series_batch(batch)
                self.stats["series"] += len(batch)
            
            logger.info(f"Fetched {len(series_list)} series")
            
        except Exception as e:
            logger.error(f"Error fetching series: {e}")
        
        return series_list
    
    def _fetch_events(self, series_ticker: str) -> List[Dict[str, Any]]:
        """Fetch and persist events for a series."""
        events_list: List[Dict[str, Any]] = []
        batch: List[Dict[str, Any]] = []
        
        try:
            for event in self.public_client.list_events(
                series_ticker=series_ticker
            ):
                events_list.append(event)
                batch.append(event)
                
                if len(batch) >= METADATA_BATCH_SIZE:
                    self.hdb_writer.write_events_batch(batch)
                    self.stats["events"] += len(batch)
                    batch = []
            
            if batch:
                self.hdb_writer.write_events_batch(batch)
                self.stats["events"] += len(batch)
            
            logger.info(
                f"  Fetched {len(events_list)} events for {series_ticker}"
            )
            
        except Exception as e:
            logger.error(f"Error fetching events for {series_ticker}: {e}")
        
        return events_list
    
    def _fetch_markets(self, series_ticker: str) -> List[Dict[str, Any]]:
        """Fetch and persist markets for a series."""
        markets_list: List[Dict[str, Any]] = []
        batch: List[Dict[str, Any]] = []
        
        try:
            for market in self.public_client.list_markets_paginated(
                series_ticker=series_ticker
            ):
                markets_list.append(market)
                batch.append(market)
                
                if len(batch) >= METADATA_BATCH_SIZE:
                    self.hdb_writer.write_markets_batch(batch)
                    self.stats["markets"] += len(batch)
                    batch = []
            
            if batch:
                self.hdb_writer.write_markets_batch(batch)
                self.stats["markets"] += len(batch)
            
            logger.info(
                f"  Fetched {len(markets_list)} markets for {series_ticker}"
            )
            
        except Exception as e:
            logger.error(f"Error fetching markets for {series_ticker}: {e}")
        
        return markets_list
    
    def _backfill_trades(self, ticker: str) -> int:
        """
        Backfill trades for a single market ticker.
        
        Uses day-by-day fetching to handle large date ranges
        and supports resume via progress tracking.
        
        Args:
            ticker: Market ticker
            
        Returns:
            Number of trades written
        """
        # Check for existing progress
        progress = self.hdb_writer.get_backfill_progress(ticker)
        if progress and progress.get("last_trade_time"):
            resume_date = progress["last_trade_time"].date()
            if resume_date >= self.start_date:
                logger.info(
                    f"    Resuming {ticker} from {resume_date}"
                )
                current_date = resume_date
            else:
                current_date = self.start_date
        else:
            current_date = self.start_date
        
        total_trades = 0
        batch: List[Dict[str, Any]] = []
        last_trade_time: Optional[dt.datetime] = None
        
        while current_date <= self.end_date:
            # Calculate timestamp range for the day
            ts_start = int(
                dt.datetime.combine(current_date, dt.time.min).timestamp()
            )
            ts_end = int(
                dt.datetime.combine(
                    current_date + dt.timedelta(days=1), dt.time.min
                ).timestamp()
            )
            
            try:
                trades = self.auth_client.fetch_trades_paginated(
                    ticker=ticker,
                    min_ts=ts_start,
                    max_ts=ts_end,
                )
                
                for trade in trades:
                    batch.append(trade)
                    
                    # Track last trade time for progress
                    trade_time = trade.get("created_time")
                    if trade_time:
                        if isinstance(trade_time, str):
                            try:
                                parsed = dt.datetime.fromisoformat(
                                    trade_time.replace("Z", "+00:00")
                                )
                                if (
                                    last_trade_time is None
                                    or parsed > last_trade_time
                                ):
                                    last_trade_time = parsed
                            except ValueError:
                                pass
                    
                    if len(batch) >= TRADE_BATCH_SIZE:
                        written = self.hdb_writer.write_trades_batch(batch)
                        total_trades += written
                        self.stats["trades"] += written
                        batch = []
                
                if trades:
                    logger.debug(
                        f"    {ticker} {current_date}: {len(trades)} trades"
                    )
                
            except Exception as e:
                logger.warning(
                    f"    Error fetching trades for {ticker} on "
                    f"{current_date}: {e}"
                )
            
            current_date += dt.timedelta(days=1)
        
        # Write remaining batch
        if batch:
            written = self.hdb_writer.write_trades_batch(batch)
            total_trades += written
            self.stats["trades"] += written
        
        # Update progress
        if last_trade_time:
            self.hdb_writer.update_backfill_progress(
                ticker=ticker,
                last_trade_time=last_trade_time,
                trade_count=total_trades,
            )
        
        if total_trades > 0:
            logger.info(f"    {ticker}: {total_trades} trades backfilled")
        
        return total_trades
    
    def _backfill_candlesticks(
        self,
        series_ticker: str,
        ticker: str,
    ) -> int:
        """
        Backfill candlestick (OHLC) data for a market from the API.
        
        Args:
            series_ticker: Series ticker
            ticker: Market ticker
            
        Returns:
            Number of candlesticks written
        """
        # Calculate timestamp range
        start_ts = int(
            dt.datetime.combine(self.start_date, dt.time.min).timestamp()
        )
        end_ts = int(
            dt.datetime.combine(self.end_date, dt.time.max).timestamp()
        )
        
        total_candles = 0
        
        # Fetch daily candlesticks (1440 minutes = 1 day)
        try:
            response = self.public_client.get_market_candlesticks(
                series_ticker=series_ticker,
                ticker=ticker,
                start_ts=start_ts,
                end_ts=end_ts,
                period_interval=1440,  # Daily
            )
            candles = response.get("candlesticks", [])
            if candles:
                written = self.hdb_writer.write_candlesticks_batch(
                    candles=candles,
                    ticker=ticker,
                    series_ticker=series_ticker,
                    period_interval=1440,
                )
                total_candles += written
                self.stats["candlesticks"] += written
                
        except Exception as e:
            logger.debug(f"    Candlesticks for {ticker}: {e}")
        
        if total_candles > 0:
            logger.debug(f"    {ticker}: {total_candles} candlesticks")
        
        return total_candles
    
    def _generate_ohlc(self) -> None:
        """
        Generate OHLC aggregations from trades_hdb table.
        
        Uses QuestDB SAMPLE BY for efficient time-series aggregation.
        """
        try:
            logger.info("  Generating daily OHLC (ohlc_1d)...")
            daily_count = self.hdb_writer.generate_ohlc_daily()
            self.stats["ohlc_daily"] = daily_count
            logger.info(f"    Generated {daily_count} daily OHLC records")
        except Exception as e:
            logger.warning(f"  Failed to generate daily OHLC: {e}")
        
        try:
            logger.info("  Generating hourly OHLC (ohlc_1h)...")
            hourly_count = self.hdb_writer.generate_ohlc_hourly()
            self.stats["ohlc_hourly"] = hourly_count
            logger.info(f"    Generated {hourly_count} hourly OHLC records")
        except Exception as e:
            logger.warning(f"  Failed to generate hourly OHLC: {e}")


def parse_date(val: str) -> dt.date:
    """Parse YYYY-MM-DD to date."""
    return dt.datetime.strptime(val, "%Y-%m-%d").date()


def main() -> None:
    """CLI entry point for full historical backfill."""
    ensure_env_loaded()
    
    # Load environment config
    env_api: Optional[KalshiAPIConfig] = None
    try:
        env_api = KalshiAPIConfig.from_env()
    except ValueError:
        pass
    env_qdb = QuestDBConfig.from_env()
    
    # Parse arguments
    parser = argparse.ArgumentParser(
        description="Full Kalshi historical backfill to QuestDB HDB.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Backfill all data since 2024
  python -m kalshi_platform.tools.full_backfill --start-date 2024-01-01

  # Backfill specific series
  python -m kalshi_platform.tools.full_backfill --start-date 2024-01-01 --series KXINXY

  # Backfill with custom date range
  python -m kalshi_platform.tools.full_backfill --start-date 2024-06-01 --end-date 2024-12-31
        """,
    )
    
    parser.add_argument(
        "--start-date",
        required=True,
        help="Start date for backfill (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end-date",
        help="End date for backfill (YYYY-MM-DD), defaults to today",
    )
    parser.add_argument(
        "--series",
        help="Filter to specific series prefix (e.g., KXINXY)",
    )
    parser.add_argument(
        "--min-volume",
        type=int,
        default=0,
        help="Only backfill tickers with daily/block volume >= this value (e.g., 10000)",
    )
    parser.add_argument(
        "--api-key",
        help="Kalshi API key (defaults to KALSHI_API_KEY env var)",
    )
    parser.add_argument(
        "--private-key",
        help="Path to RSA private key (defaults to KALSHI_PRIVATE_KEY_PATH env var)",
    )
    parser.add_argument(
        "--base-url",
        default=API_BASE_URL,
        help="Kalshi API base URL",
    )
    parser.add_argument(
        "--questdb-host",
        help="QuestDB host (defaults to QUESTDB_HDB_HOST env var)",
    )
    parser.add_argument(
        "--questdb-port",
        type=int,
        help="QuestDB Postgres port (defaults to QUESTDB_HDB_PORT env var)",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose logging",
    )
    parser.add_argument(
        "--reset-tables",
        action="store_true",
        help="Drop and recreate all tables before backfill (use when schema changes)",
    )
    parser.add_argument(
        "--continue",
        dest="continue_backfill",
        action="store_true",
        help="Continue from where it left off (don't drop existing data)",
    )
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Resolve API credentials
    api_key = args.api_key or (env_api.api_key if env_api else None)
    private_key = args.private_key or (
        str(env_api.private_key_path) if env_api else None
    )
    
    if not api_key or not private_key:
        logger.error(
            "API credentials required. Provide --api-key/--private-key "
            "or set KALSHI_API_KEY/KALSHI_PRIVATE_KEY_PATH environment variables."
        )
        sys.exit(1)
    
    # Resolve QuestDB config
    questdb_host = args.questdb_host or env_qdb.hdb_host
    questdb_port = args.questdb_port or env_qdb.hdb_port
    
    # Parse dates
    start_date = parse_date(args.start_date)
    end_date = parse_date(args.end_date) if args.end_date else dt.date.today()
    
    # Initialize clients
    public_client = PublicKalshiClient(base_url=args.base_url)
    
    signer = KalshiSigner(
        api_key=api_key,
        private_key_path=Path(private_key),
    )
    auth_client = AuthenticatedClient(
        base_url=args.base_url,
        signer=signer,
    )
    
    hdb_config = QuestDBHDBWriterConfig(
        host=questdb_host,
        port=questdb_port,
        username=env_qdb.hdb_username,
        password=env_qdb.hdb_password,
        database=env_qdb.hdb_database,
    )
    hdb_writer = QuestDBHDBWriter(config=hdb_config)
    
    # Reset tables if requested (unless --continue is specified)
    if args.reset_tables and not args.continue_backfill:
        logger.info("Resetting tables (drop and recreate)...")
        hdb_writer.reset_tables()
    elif not args.continue_backfill:
        # Default: reset tables to avoid duplicates
        logger.info("Resetting tables to avoid duplicates (use --continue to skip)...")
        hdb_writer.reset_tables()
    
    # Fetch high-volume tickers if filter is specified
    high_volume_tickers: Optional[Set[str]] = None
    if args.min_volume > 0:
        high_volume_tickers = fetch_high_volume_tickers(
            min_volume=args.min_volume,
            start_date=start_date,
            end_date=end_date,
        )
        if not high_volume_tickers:
            logger.warning("No high-volume tickers found, proceeding without filter")
            high_volume_tickers = None
    
    # Run backfill
    try:
        backfiller = FullBackfiller(
            public_client=public_client,
            auth_client=auth_client,
            hdb_writer=hdb_writer,
            start_date=start_date,
            end_date=end_date,
            series_filter=args.series,
            high_volume_tickers=high_volume_tickers,
        )
        stats = backfiller.run()
        
        # Exit with success
        sys.exit(0)
        
    except KeyboardInterrupt:
        logger.info("Backfill interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Backfill failed: {e}")
        sys.exit(1)
    finally:
        hdb_writer.close()


if __name__ == "__main__":
    main()

