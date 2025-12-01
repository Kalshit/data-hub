"""
Fix markets table by re-fetching data from Kalshi API.

Populates missing fields like `category` that weren't captured initially.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from pathlib import Path
from typing import List, Set

from kalshi_platform.api.public_client import PublicKalshiClient, API_BASE_URL
from kalshi_platform.config import QuestDBConfig, ensure_env_loaded
from kalshi_platform.storage.questdb_hdb_writer import (
    QuestDBHDBWriter,
    QuestDBHDBWriterConfig,
)

# Default cache file location
TICKER_CACHE_FILE = Path(__file__).parent.parent.parent / "high_volume_tickers_cache.json"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def get_tickers_from_cache(cache_path: Path = TICKER_CACHE_FILE) -> List[str]:
    """
    Load tickers from high_volume_tickers_cache.json file.
    
    Returns:
        List of ticker strings
    """
    if not cache_path.exists():
        logger.warning(f"Cache file not found: {cache_path}")
        return []
    
    try:
        data = json.loads(cache_path.read_text())
        tickers = data.get("tickers", [])
        logger.info(f"Loaded {len(tickers)} tickers from cache file")
        logger.info(f"  Cache params: min_volume={data.get('min_volume')}, "
                   f"dates={data.get('start_date')} to {data.get('end_date')}")
        return tickers
    except (json.JSONDecodeError, KeyError) as e:
        logger.error(f"Failed to load cache file: {e}")
        return []


def get_existing_market_tickers(writer: QuestDBHDBWriter) -> List[str]:
    """
    Get all unique tickers from markets table.
    
    Returns:
        List of ticker strings
    """
    if not writer._table_exists("markets"):
        logger.warning("markets table does not exist")
        return []
    
    cursor = writer.connection.cursor()
    cursor.execute("SELECT DISTINCT ticker FROM markets")
    results = [row[0] for row in cursor.fetchall()]
    cursor.close()
    
    logger.info(f"Found {len(results)} unique tickers in markets table")
    return results


def get_all_tickers_from_tables(writer: QuestDBHDBWriter) -> List[str]:
    """
    Get all unique tickers from multiple tables (trades_hdb, candlesticks, backfill_progress, markets).
    
    Returns:
        List of unique ticker strings
    """
    all_tickers: Set[str] = set()
    cursor = writer.connection.cursor()
    
    # From trades_hdb
    if writer._table_exists("trades_hdb"):
        cursor.execute("SELECT DISTINCT ticker FROM trades_hdb")
        tickers = [row[0] for row in cursor.fetchall() if row[0]]
        logger.info(f"  trades_hdb: {len(tickers)} tickers")
        all_tickers.update(tickers)
    
    # From candlesticks
    if writer._table_exists("candlesticks"):
        cursor.execute("SELECT DISTINCT ticker FROM candlesticks")
        tickers = [row[0] for row in cursor.fetchall() if row[0]]
        logger.info(f"  candlesticks: {len(tickers)} tickers")
        all_tickers.update(tickers)
    
    # From backfill_progress
    if writer._table_exists("backfill_progress"):
        cursor.execute("SELECT DISTINCT ticker FROM backfill_progress")
        tickers = [row[0] for row in cursor.fetchall() if row[0]]
        logger.info(f"  backfill_progress: {len(tickers)} tickers")
        all_tickers.update(tickers)
    
    # From markets (if exists)
    if writer._table_exists("markets"):
        cursor.execute("SELECT DISTINCT ticker FROM markets")
        tickers = [row[0] for row in cursor.fetchall() if row[0]]
        logger.info(f"  markets: {len(tickers)} tickers")
        all_tickers.update(tickers)
    
    cursor.close()
    
    result = list(all_tickers)
    logger.info(f"Total unique tickers: {len(result)}")
    return result


def fix_markets(
    writer: QuestDBHDBWriter,
    client: PublicKalshiClient,
    from_all_tables: bool = False,
    from_cache: bool = False,
    cache_path: Path = TICKER_CACHE_FILE,
) -> int:
    """
    Re-fetch and fix all market data.
    
    Args:
        writer: QuestDB writer
        client: Kalshi API client
        from_all_tables: If True, get tickers from all tables, not just markets
        from_cache: If True, get tickers from cache file
        cache_path: Path to ticker cache file
        
    Returns:
        Number of markets written
    """
    # Get tickers from appropriate source
    if from_cache:
        tickers = get_tickers_from_cache(cache_path)
    elif from_all_tables:
        logger.info("Gathering tickers from all tables...")
        tickers = get_all_tickers_from_tables(writer)
    else:
        tickers = get_existing_market_tickers(writer)
    
    if not tickers:
        logger.warning("No existing markets to fix")
        return 0
    
    # Drop and recreate markets table
    logger.info("Dropping markets table...")
    writer.drop_table("markets")
    writer._create_table_if_missing("markets")
    
    total_written = 0
    errors = 0
    batch: List[dict] = []
    batch_size = 50
    
    for i, ticker in enumerate(tickers, 1):
        try:
            if i % 100 == 0 or i == len(tickers):
                logger.info(f"[{i}/{len(tickers)}] Fetching markets...")
            
            response = client.get_market(ticker)
            market = response.get("market", {})
            
            if market:
                batch.append(market)
                
                if len(batch) >= batch_size:
                    written = writer.write_markets_batch(batch)
                    total_written += written
                    batch = []
            
            # Small delay to avoid rate limiting
            if i % 50 == 0:
                time.sleep(0.5)
                
        except Exception as e:
            errors += 1
            if errors <= 10:
                logger.warning(f"  Error fetching {ticker}: {e}")
    
    # Write remaining batch
    if batch:
        written = writer.write_markets_batch(batch)
        total_written += written
    
    logger.info("=" * 50)
    logger.info(f"Fix complete!")
    logger.info(f"  Tickers processed: {len(tickers)}")
    logger.info(f"  Markets written: {total_written}")
    logger.info(f"  Errors: {errors}")
    
    return total_written


def main() -> None:
    """CLI entry point."""
    ensure_env_loaded()
    env_qdb = QuestDBConfig.from_env()
    
    parser = argparse.ArgumentParser(
        description="Fix markets table by re-fetching from Kalshi API."
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
        "--from-all-tables",
        action="store_true",
        help="Get tickers from all tables (trades_hdb, candlesticks, backfill_progress) not just markets",
    )
    parser.add_argument(
        "--from-cache",
        action="store_true",
        help="Get tickers from high_volume_tickers_cache.json file",
    )
    parser.add_argument(
        "--cache-file",
        type=Path,
        default=TICKER_CACHE_FILE,
        help=f"Path to ticker cache file (default: {TICKER_CACHE_FILE})",
    )
    
    args = parser.parse_args()
    
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
    
    if args.dry_run:
        if args.from_cache:
            tickers = get_tickers_from_cache(args.cache_file)
        elif args.from_all_tables:
            logger.info("Gathering tickers from all tables...")
            tickers = get_all_tickers_from_tables(writer)
        else:
            tickers = get_existing_market_tickers(writer)
        logger.info(f"DRY RUN: Would re-fetch {len(tickers)} markets")
        for ticker in tickers[:10]:
            logger.info(f"  - {ticker}")
        if len(tickers) > 10:
            logger.info(f"  ... and {len(tickers) - 10} more")
        return
    
    try:
        fix_markets(
            writer=writer,
            client=client,
            from_all_tables=args.from_all_tables,
            from_cache=args.from_cache,
            cache_path=args.cache_file,
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

