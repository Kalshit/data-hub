"""
CLI demo: series -> markets -> orderbook -> trades.
"""
from __future__ import annotations
import argparse
import json
import os
from typing import Any, Optional
from kalshi_platform.api.public_client import API_BASE_URL, PublicKalshiClient
from kalshi_platform.config import ensure_env_loaded

def print_header(title: str) -> None:
    """Print 80-char bordered section header."""
    print("=" * 80 + f"\n{title.upper()}\n" + "=" * 80)

def demo_series(client: PublicKalshiClient, series_ticker: str) -> str:
    """Fetch and display series metadata returning title."""
    print_header(f"Series: {series_ticker}")
    series_payload = client.get_series(series_ticker)
    series = series_payload.get("series", {})
    print(json.dumps(series, indent=2)[:800] + "\n")
    return series.get("title", series_ticker)

def demo_markets(
    client: PublicKalshiClient, series_ticker: str
) -> Optional[str]:
    """Display open markets returning first ticker or None."""
    print_header("Active Markets")
    markets = client.summarize_markets(series_ticker, status="open")
    if not markets:
        print("No active markets found.")
        return None
    for market in markets:
        print(
            f"{market.ticker:<15} {market.title:<50} "
            f"Yes:{market.yes_price or '-':>5}¢ "
            f"Volume:{market.volume or 0}"
        )
    print()
    return markets[0].ticker

def demo_orderbook(client: PublicKalshiClient, market_ticker: str) -> None:
    """Display top 5 price levels for both yes and no sides."""
    print_header(f"Orderbook for {market_ticker}")
    orderbook = client.get_market_orderbook(market_ticker).get("orderbook", {})
    for side in ("yes", "no"):
        print(f"{side.upper()} BIDS:")
        for price, qty in orderbook.get(side, [])[:5]:
            print(f"  {price:>5}¢ x {qty}")
        print()

def demo_trades(
    client: PublicKalshiClient, market_ticker: Optional[str]
) -> None:
    """Fetch and display 10 most recent trades."""
    print_header("Recent Trades (first 10)")
    if not market_ticker:
        print("Skipping trade fetch: no market ticker found.")
        return
    for idx, trade in enumerate(client.iter_trades(
        ticker=market_ticker
    )):
        print(
            f"{trade['created_time']} | {trade['ticker']} | "
            f"{trade['yes_price']}¢ x {trade['count']} "
            f"({trade['taker_side']})"
        )
        if idx >= 9:
            break

def main() -> None:
    """Parse CLI args and run demo sequence."""
    ensure_env_loaded()
    p = argparse.ArgumentParser(
        description="Demo Kalshi public REST endpoints."
    )
    p.add_argument(
        "--series", default="KXHIGHNY",
        help="Series ticker (default: KXHIGHNY)."
    )
    p.add_argument(
        "--base-url",
        default=os.getenv("KALSHI_BASE_URL"),
        help="Override API base URL (default env or production).",
    )
    args = p.parse_args()
    base_url = args.base_url or API_BASE_URL
    client = PublicKalshiClient(base_url=base_url)
    series_title = demo_series(client, args.series)
    first_market = demo_markets(client, args.series)
    if first_market:
        demo_orderbook(client, first_market)
    demo_trades(client, first_market)
    print("\nDone! Explore other series with --series <ticker>.")

if __name__ == "__main__":
    main()

