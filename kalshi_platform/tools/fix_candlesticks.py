"""
Fix candlesticks table by re-fetching data from Kalshi API.

QuestDB is append-only, so we:
1. Read existing ticker/series combinations
2. Drop and recreate the table
3. Re-fetch from API with proper OHLC parsing
"""
from __future__ import annotations

import argparse
import datetime as dt
import logging
import sys
from typing import Any, Dict, List, Set, Tuple

from kalshi_platform.api.public_client import PublicKalshiClient, API_BASE_URL
from kalshi_platform.config import QuestDBConfig, ensure_env_loaded
from kalshi_platform.storage.questdb_hdb_writer import (
    QuestDBHDBWriter,
    QuestDBHDBWriterConfig,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def get_existing_candlestick_tickers(writer: QuestDBHDBWriter) -> List[Tuple[str, str]]:
    """
    Get all unique (ticker, series_ticker) pairs from candlesticks table.
    
    Returns:
        List of (ticker, series_ticker) tuples
    """
    if not writer._table_exists("candlesticks"):
        logger.warning("candlesticks table does not exist")
        return []
    
    cursor = writer.connection.cursor()
    cursor.execute(
        "SELECT DISTINCT ticker, series_ticker FROM candlesticks"
    )
    results = [(row[0], row[1]) for row in cursor.fetchall()]
    cursor.close()
    
    logger.info(f"Found {len(results)} unique ticker/series combinations")
    return results


def get_tickers_from_backfill_progress(writer: QuestDBHDBWriter) -> List[Tuple[str, str]]:
    """
    Get all unique (ticker, series_ticker) pairs by joining backfill_progress 
    with markets and events tables.
    
    Returns:
        List of (ticker, series_ticker) tuples
    """
    if not writer._table_exists("backfill_progress"):
        logger.warning("backfill_progress table does not exist")
        return []
    
    cursor = writer.connection.cursor()
    
    # Join backfill_progress -> markets -> events to get series_ticker
    cursor.execute("""
        SELECT DISTINCT bp.ticker, e.series_ticker
        FROM backfill_progress bp
        INNER JOIN markets m ON bp.ticker = m.ticker
        INNER JOIN events e ON m.event_ticker = e.ticker
        WHERE e.series_ticker IS NOT NULL
    """)
    results = [(row[0], row[1]) for row in cursor.fetchall()]
    cursor.close()
    
    logger.info(f"Found {len(results)} unique ticker/series combinations from backfill_progress")
    return results


def fix_candlesticks(
    writer: QuestDBHDBWriter,
    client: PublicKalshiClient,
    start_date: dt.date,
    end_date: dt.date,
    period_interval: int = 1440,
    use_backfill_progress: bool = False,
) -> int:
    """
    Re-fetch and fix all candlestick data.
    
    Args:
        writer: QuestDB writer
        client: Kalshi API client
        start_date: Start date for fetching
        end_date: End date for fetching
        period_interval: Candle period in minutes (1440 = daily)
        use_backfill_progress: If True, get tickers from backfill_progress table
        
    Returns:
        Number of candlesticks written
    """
    # Get ticker combinations from appropriate source
    if use_backfill_progress:
        ticker_pairs = get_tickers_from_backfill_progress(writer)
    else:
        ticker_pairs = get_existing_candlestick_tickers(writer)
    
    if not ticker_pairs:
        logger.warning("No existing candlesticks to fix")
        return 0
    
    # Calculate timestamps
    start_ts = int(dt.datetime.combine(start_date, dt.time.min).timestamp())
    end_ts = int(dt.datetime.combine(end_date, dt.time.max).timestamp())
    
    # Drop and recreate candlesticks table
    logger.info("Dropping candlesticks table...")
    writer.drop_table("candlesticks")
    writer._create_table_if_missing("candlesticks")
    
    total_written = 0
    errors = 0
    
    for i, (ticker, series_ticker) in enumerate(ticker_pairs, 1):
        try:
            logger.info(f"[{i}/{len(ticker_pairs)}] Fetching {ticker} ({series_ticker})...")
            
            response = client.get_market_candlesticks(
                series_ticker=series_ticker,
                ticker=ticker,
                start_ts=start_ts,
                end_ts=end_ts,
                period_interval=period_interval,
            )
            
            candles = response.get("candlesticks", [])
            
            if candles:
                written = writer.write_candlesticks_batch(
                    candles=candles,
                    ticker=ticker,
                    series_ticker=series_ticker,
                    period_interval=period_interval,
                )
                total_written += written
                logger.info(f"  -> Written {written} candlesticks")
            else:
                logger.debug(f"  -> No candlesticks returned")
                
        except Exception as e:
            errors += 1
            logger.warning(f"  -> Error: {e}")
    
    logger.info("=" * 50)
    logger.info(f"Fix complete!")
    logger.info(f"  Tickers processed: {len(ticker_pairs)}")
    logger.info(f"  Candlesticks written: {total_written}")
    logger.info(f"  Errors: {errors}")
    
    return total_written


def main() -> None:
    """CLI entry point."""
    ensure_env_loaded()
    env_qdb = QuestDBConfig.from_env()
    
    parser = argparse.ArgumentParser(
        description="Fix candlesticks table by re-fetching from Kalshi API."
    )
    parser.add_argument(
        "--start-date",
        default="2024-01-01",
        help="Start date (YYYY-MM-DD), default: 2024-01-01",
    )
    parser.add_argument(
        "--end-date",
        help="End date (YYYY-MM-DD), default: today",
    )
    parser.add_argument(
        "--period-interval",
        type=int,
        default=1440,
        help="Candle period in minutes (1440=daily, 60=hourly), default: 1440",
    )
    parser.add_argument(
        "--questdb-host",
        help="QuestDB host (defaults to env var)",
    )
    parser.add_argument(
        "--questdb-port",
        type=int,
        help="QuestDB port (defaults to env var)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Just show what would be done without making changes",
    )
    parser.add_argument(
        "--from-backfill-progress",
        action="store_true",
        help="Get tickers from backfill_progress table instead of candlesticks table",
    )
    
    args = parser.parse_args()
    
    # Parse dates
    start_date = dt.datetime.strptime(args.start_date, "%Y-%m-%d").date()
    end_date = (
        dt.datetime.strptime(args.end_date, "%Y-%m-%d").date()
        if args.end_date
        else dt.date.today()
    )
    
    # Initialize connections
    questdb_host = args.questdb_host or env_qdb.hdb_host
    questdb_port = args.questdb_port or env_qdb.hdb_port
    
    hdb_config = QuestDBHDBWriterConfig(
        host=questdb_host,
        port=questdb_port,
        username=env_qdb.hdb_username,
        password=env_qdb.hdb_password,
        database=env_qdb.hdb_database,
    )
    writer = QuestDBHDBWriter(config=hdb_config)
    client = PublicKalshiClient(base_url=API_BASE_URL)
    
    logger.info(f"Connected to QuestDB at {questdb_host}:{questdb_port}")
    logger.info(f"Date range: {start_date} to {end_date}")
    logger.info(f"Period interval: {args.period_interval} minutes")
    logger.info(f"Ticker source: {'backfill_progress' if args.from_backfill_progress else 'candlesticks'}")
    
    if args.dry_run:
        if args.from_backfill_progress:
            ticker_pairs = get_tickers_from_backfill_progress(writer)
        else:
            ticker_pairs = get_existing_candlestick_tickers(writer)
        logger.info(f"DRY RUN: Would re-fetch {len(ticker_pairs)} ticker/series pairs")
        for ticker, series in ticker_pairs[:10]:
            logger.info(f"  - {ticker} ({series})")
        if len(ticker_pairs) > 10:
            logger.info(f"  ... and {len(ticker_pairs) - 10} more")
        return
    
    try:
        fix_candlesticks(
            writer=writer,
            client=client,
            start_date=start_date,
            end_date=end_date,
            period_interval=args.period_interval,
            use_backfill_progress=args.from_backfill_progress,
        )
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Failed: {e}")
        sys.exit(1)
    finally:
        writer.close()


if __name__ == "__main__":
    main()

