"""
Quick sanity check for Kalshi QuestDB tables.

Fast, probabilistic checks - not exhaustive validation.
"""
from __future__ import annotations

import argparse
import sys

try:
    from colorama import Fore, Style, init as colorama_init
    colorama_init(autoreset=True)
except ImportError:
    class Fore:
        RED = GREEN = YELLOW = CYAN = MAGENTA = ""
    class Style:
        BRIGHT = RESET_ALL = ""

from kalshi_platform.config import QuestDBConfig, ensure_env_loaded
from kalshi_platform.storage.questdb_hdb_writer import QuestDBHDBWriter, QuestDBHDBWriterConfig


def main() -> None:
    ensure_env_loaded()
    env_qdb = QuestDBConfig.from_env()
    
    parser = argparse.ArgumentParser(description="Quick data sanity check")
    parser.add_argument("--questdb-host", help="QuestDB host")
    parser.add_argument("--questdb-port", type=int, help="QuestDB port")
    args = parser.parse_args()
    
    config = QuestDBHDBWriterConfig(
        host=args.questdb_host or env_qdb.hdb_host,
        port=args.questdb_port or env_qdb.hdb_port,
        username=env_qdb.hdb_username,
        password=env_qdb.hdb_password,
        database=env_qdb.hdb_database,
    )
    writer = QuestDBHDBWriter(config=config)
    cursor = writer.connection.cursor()
    
    print(f"\n{Style.BRIGHT}{'='*50}")
    print(f"{Style.BRIGHT}Quick Sanity Check")
    print(f"{Style.BRIGHT}{'='*50}\n")
    
    passed = 0
    failed = 0
    
    print(f"{Fore.CYAN}[Table Counts]")
    tables = ["trades_hdb", "markets", "candlesticks", "ohlc_1d", "ohlc_1h", "series", "events"]
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        status = f"{Fore.GREEN}✓" if count > 0 else f"{Fore.YELLOW}○"
        print(f"  {status} {table}: {count:,}")
        if count > 0:
            passed += 1
    
    print(f"\n{Fore.CYAN}[Sample Duplicate Check - last 10k trades]")
    cursor.execute("""
        SELECT COUNT(*) as total, COUNT_DISTINCT(trade_id) as unique_ids
        FROM (SELECT trade_id FROM trades_hdb LIMIT -10000)
    """)
    row = cursor.fetchone()
    total, unique = row[0], row[1]
    if total == unique:
        print(f"  {Fore.GREEN}✓ No duplicates in sample")
        passed += 1
    else:
        print(f"  {Fore.RED}✗ {total - unique} duplicates in sample")
        failed += 1
    
    print(f"\n{Fore.CYAN}[Price Sanity - sample 1000 trades]")
    cursor.execute("""
        SELECT COUNT(*) FROM (
            SELECT yes_price FROM trades_hdb LIMIT -1000
        ) WHERE yes_price < 0 OR yes_price > 100
    """)
    bad_prices = cursor.fetchone()[0]
    if bad_prices == 0:
        print(f"  {Fore.GREEN}✓ All prices in 0-100 range")
        passed += 1
    else:
        print(f"  {Fore.RED}✗ {bad_prices} prices out of range")
        failed += 1
    
    print(f"\n{Fore.CYAN}[OHLC Sanity]")
    cursor.execute("SELECT COUNT(*) FROM ohlc_1d WHERE high < low")
    bad_ohlc = cursor.fetchone()[0]
    if bad_ohlc == 0:
        print(f"  {Fore.GREEN}✓ ohlc_1d: high >= low")
        passed += 1
    else:
        print(f"  {Fore.RED}✗ ohlc_1d: {bad_ohlc} rows with high < low")
        failed += 1
    
    cursor.execute("SELECT COUNT(*) FROM ohlc_1h WHERE high < low")
    bad_ohlc = cursor.fetchone()[0]
    if bad_ohlc == 0:
        print(f"  {Fore.GREEN}✓ ohlc_1h: high >= low")
        passed += 1
    else:
        print(f"  {Fore.RED}✗ ohlc_1h: {bad_ohlc} rows with high < low")
        failed += 1
    
    print(f"\n{Fore.CYAN}[Ticker Coverage]")
    cursor.execute("SELECT COUNT_DISTINCT(ticker) FROM trades_hdb")
    trade_tickers = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT_DISTINCT(ticker) FROM markets")
    market_tickers = cursor.fetchone()[0]
    coverage = (min(trade_tickers, market_tickers) / max(trade_tickers, market_tickers) * 100) if max(trade_tickers, market_tickers) > 0 else 0
    print(f"  trades: {trade_tickers:,} tickers, markets: {market_tickers:,} tickers")
    if coverage > 50:
        print(f"  {Fore.GREEN}✓ {coverage:.0f}% overlap")
        passed += 1
    else:
        print(f"  {Fore.YELLOW}○ {coverage:.0f}% overlap (may be expected)")
    
    print(f"\n{Style.BRIGHT}{'='*50}")
    if failed == 0:
        print(f"{Fore.GREEN}{Style.BRIGHT}✓ All {passed} checks passed!")
    else:
        print(f"{Fore.GREEN}✓ Passed: {passed}")
        print(f"{Fore.RED}✗ Failed: {failed}")
    print(f"{'='*50}\n")
    
    cursor.close()
    writer.close()
    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()
