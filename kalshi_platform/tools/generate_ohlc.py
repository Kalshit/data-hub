"""
Generate OHLC tables from trades_hdb.

- ohlc_1d: Aggregate daily from trades_hdb using SAMPLE BY 1d
- ohlc_1h: Aggregate hourly from trades_hdb using SAMPLE BY 1h
"""
from __future__ import annotations

import argparse
import logging
import sys

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


def generate_daily_from_trades(writer: QuestDBHDBWriter) -> int:
    """
    Generate daily OHLC from trades_hdb using SAMPLE BY.
    
    Returns:
        Number of records inserted
    """
    if not writer._table_exists("trades_hdb"):
        logger.warning("trades_hdb table does not exist")
        return 0
    
    writer._create_table_if_missing("ohlc_1d")
    
    cursor = writer.connection.cursor()
    
    # Aggregate daily from trades
    cursor.execute("""
        INSERT INTO ohlc_1d (ticker, open, high, low, close, volume, trade_count, vwap, timestamp)
        SELECT
            ticker,
            first(yes_price) as open,
            max(yes_price) as high,
            min(yes_price) as low,
            last(yes_price) as close,
            sum(count) as volume,
            count() as trade_count,
            sum(yes_price * count) / sum(count) as vwap,
            created_time as timestamp
        FROM trades_hdb
        SAMPLE BY 1d ALIGN TO CALENDAR
    """)
    writer.connection.commit()
    
    # Get count
    cursor.execute("SELECT COUNT(*) FROM ohlc_1d")
    result = cursor.fetchone()
    cursor.close()
    
    return result[0] if result else 0


def generate_hourly_from_trades(writer: QuestDBHDBWriter) -> int:
    """
    Generate hourly OHLC from trades_hdb using SAMPLE BY.
    
    Returns:
        Number of records inserted
    """
    if not writer._table_exists("trades_hdb"):
        logger.warning("trades_hdb table does not exist")
        return 0
    
    writer._create_table_if_missing("ohlc_1h")
    
    cursor = writer.connection.cursor()
    
    # Aggregate hourly from trades
    cursor.execute("""
        INSERT INTO ohlc_1h (ticker, open, high, low, close, volume, trade_count, vwap, timestamp)
        SELECT
            ticker,
            first(yes_price) as open,
            max(yes_price) as high,
            min(yes_price) as low,
            last(yes_price) as close,
            sum(count) as volume,
            count() as trade_count,
            sum(yes_price * count) / sum(count) as vwap,
            created_time as timestamp
        FROM trades_hdb
        SAMPLE BY 1h ALIGN TO CALENDAR
    """)
    writer.connection.commit()
    
    # Get count
    cursor.execute("SELECT COUNT(*) FROM ohlc_1h")
    result = cursor.fetchone()
    cursor.close()
    
    return result[0] if result else 0


def main() -> None:
    """CLI entry point."""
    ensure_env_loaded()
    env_qdb = QuestDBConfig.from_env()
    
    parser = argparse.ArgumentParser(
        description="Generate OHLC tables from trades_hdb using SAMPLE BY."
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
        "--daily-only",
        action="store_true",
        help="Only generate daily OHLC (ohlc_1d from trades)",
    )
    parser.add_argument(
        "--hourly-only",
        action="store_true",
        help="Only generate hourly OHLC (ohlc_1h from trades)",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Drop existing OHLC tables before generating",
    )
    
    args = parser.parse_args()
    
    # Initialize connection
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
    
    logger.info(f"Connected to QuestDB at {questdb_host}:{questdb_port}")
    
    try:
        # Reset tables if requested
        if args.reset:
            if not args.hourly_only:
                logger.info("Dropping ohlc_1d table...")
                writer.drop_table("ohlc_1d")
            if not args.daily_only:
                logger.info("Dropping ohlc_1h table...")
                writer.drop_table("ohlc_1h")
        
        # Generate daily OHLC from trades
        if not args.hourly_only:
            logger.info("Generating daily OHLC from trades_hdb -> ohlc_1d...")
            daily_count = generate_daily_from_trades(writer)
            logger.info(f"  -> {daily_count:,} daily OHLC records")
        
        # Generate hourly OHLC from trades
        if not args.daily_only:
            logger.info("Generating hourly OHLC from trades_hdb -> ohlc_1h...")
            hourly_count = generate_hourly_from_trades(writer)
            logger.info(f"  -> {hourly_count:,} hourly OHLC records")
        
        logger.info("Done!")
        
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

