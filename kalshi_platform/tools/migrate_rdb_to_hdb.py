"""
Migrate data from QuestDB RDB (hot) to HDB (warm) storage.

Designed to run as EOD cron job. Moves yesterday's data from
in-memory RDB tables to partitioned HDB tables, optionally
exporting to Parquet and cleaning up old partitions.
"""
from __future__ import annotations

import argparse
import datetime as dt
import logging
import sys

import psycopg2

from kalshi_platform.config import QuestDBConfig, ensure_env_loaded
from kalshi_platform.storage.questdb_hdb import QuestDBHDBClient, QuestDBHDBConfig

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

TABLES_TO_MIGRATE = ["trades", "orderbook_deltas", "tickers", "bbo"]


def get_connection(host: str, port: int, database: str, username: str, password: str):
    """Create psycopg2 connection to QuestDB."""
    return psycopg2.connect(
        host=host,
        port=port,
        dbname=database,
        user=username,
        password=password,
    )


def table_exists(cursor, table_name: str) -> bool:
    """Check if table exists in QuestDB."""
    cursor.execute("SELECT table_name FROM tables()")
    tables = {row[0] for row in cursor.fetchall()}
    return table_name in tables


def get_row_count(cursor, table_name: str, date: str) -> int:
    """Count rows for a specific date in RDB table."""
    cursor.execute(f"""
        SELECT COUNT(*) FROM {table_name}
        WHERE timestamp >= '{date}'
        AND timestamp < '{date}'::timestamp + INTERVAL '1' DAY
    """)
    return cursor.fetchone()[0]


def migrate_table(cursor, table_name: str, date: str) -> int:
    """
    Migrate one day of data from RDB to HDB table.
    
    Returns number of rows migrated.
    """
    hdb_table = f"{table_name}_hdb"
    
    row_count = get_row_count(cursor, table_name, date)
    if row_count == 0:
        logger.info(f"  {table_name}: no data for {date}")
        return 0
    
    cursor.execute(f"""
        INSERT INTO {hdb_table}
        SELECT * FROM {table_name}
        WHERE timestamp >= '{date}'
        AND timestamp < '{date}'::timestamp + INTERVAL '1' DAY
    """)
    
    logger.info(f"  {table_name} -> {hdb_table}: {row_count:,} rows")
    return row_count


def cleanup_rdb(cursor, table_name: str, date: str) -> int:
    """Delete migrated data from RDB table."""
    cursor.execute(f"""
        DELETE FROM {table_name}
        WHERE timestamp >= '{date}'
        AND timestamp < '{date}'::timestamp + INTERVAL '1' DAY
    """)
    return cursor.rowcount


def run_migration(
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
    date: str,
    tables: list,
    cleanup: bool = False,
    dry_run: bool = False,
) -> dict:
    """Execute migration for specified date."""
    conn = get_connection(host, port, database, username, password)
    cursor = conn.cursor()
    
    stats = {"migrated": 0, "tables": 0, "cleaned": 0}
    
    logger.info(f"Migration for {date}")
    logger.info(f"  Tables: {tables}")
    logger.info(f"  Cleanup RDB: {cleanup}")
    logger.info(f"  Dry run: {dry_run}")
    
    for table in tables:
        if not table_exists(cursor, table):
            logger.warning(f"  {table}: RDB table not found, skipping")
            continue
        
        hdb_table = f"{table}_hdb"
        if not table_exists(cursor, hdb_table):
            logger.warning(f"  {hdb_table}: HDB table not found, skipping")
            continue
        
        if dry_run:
            count = get_row_count(cursor, table, date)
            logger.info(f"  [DRY RUN] {table}: would migrate {count:,} rows")
            stats["migrated"] += count
        else:
            count = migrate_table(cursor, table, date)
            stats["migrated"] += count
            if count > 0:
                stats["tables"] += 1
            
            if cleanup and count > 0:
                cleaned = cleanup_rdb(cursor, table, date)
                stats["cleaned"] += cleaned
                logger.info(f"  {table}: cleaned {cleaned:,} rows from RDB")
    
    if not dry_run:
        conn.commit()
    
    cursor.close()
    conn.close()
    
    return stats


def main() -> None:
    ensure_env_loaded()
    env_qdb = QuestDBConfig.from_env()
    
    parser = argparse.ArgumentParser(
        description="Migrate RDB data to HDB (run as EOD cron job)."
    )
    parser.add_argument(
        "--date",
        help="Date to migrate (YYYY-MM-DD), defaults to yesterday",
    )
    parser.add_argument(
        "--tables",
        nargs="+",
        default=TABLES_TO_MIGRATE,
        help=f"Tables to migrate (default: {TABLES_TO_MIGRATE})",
    )
    parser.add_argument(
        "--cleanup",
        action="store_true",
        help="Delete migrated data from RDB after migration",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be migrated without making changes",
    )
    parser.add_argument(
        "--questdb-host",
        default=env_qdb.hdb_host,
        help="QuestDB host",
    )
    parser.add_argument(
        "--questdb-port",
        type=int,
        default=env_qdb.hdb_port,
        help="QuestDB Postgres port",
    )
    
    args = parser.parse_args()
    
    date = args.date or (dt.date.today() - dt.timedelta(days=1)).isoformat()
    
    logger.info("=" * 50)
    logger.info("RDB → HDB Migration")
    logger.info("=" * 50)
    
    stats = run_migration(
        host=args.questdb_host,
        port=args.questdb_port,
        database=env_qdb.hdb_database,
        username=env_qdb.hdb_username,
        password=env_qdb.hdb_password,
        date=date,
        tables=args.tables,
        cleanup=args.cleanup,
        dry_run=args.dry_run,
    )
    
    logger.info("=" * 50)
    logger.info(f"Migration complete: {stats['migrated']:,} rows, {stats['tables']} tables")
    if stats["cleaned"] > 0:
        logger.info(f"Cleaned from RDB: {stats['cleaned']:,} rows")
    logger.info("=" * 50)


if __name__ == "__main__":
    main()

