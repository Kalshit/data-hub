from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from kalshi_platform.api.public_client import PublicKalshiClient  # noqa: E402


def snapshot_series(
    series: str,
    iterations: int,
    interval: float,
    out_dir: Path,
) -> None:
    client = PublicKalshiClient()
    tickers = [
        market.ticker
        for market in client.summarize_markets(series, status="open")
    ]
    if not tickers:
        raise RuntimeError(f"No open markets found for series '{series}'")
    out_dir.mkdir(parents=True, exist_ok=True)

    for idx in range(iterations):
        ts = int(time.time())
        print(f"[{idx + 1}/{iterations}] snapshot @ {ts}")
        for ticker in tickers:
            payload = client.get_market_orderbook(ticker)
            orderbook = payload.get("orderbook") or {}
            entry = {"timestamp": ts, "ticker": ticker, "orderbook": orderbook}
            log_path = out_dir / f"{ticker}.jsonl"
            with log_path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(entry) + "\n")
        if idx + 1 < iterations:
            time.sleep(interval)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Log orderbook snapshots for every market in a series."
    )
    parser.add_argument("--series", default="KXINXY", help="Series ticker")
    parser.add_argument(
        "--iterations",
        type=int,
        default=3,
        help="How many snapshots to capture",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=10.0,
        help="Seconds between snapshots",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path("logs"),
        help="Directory for JSONL output",
    )
    args = parser.parse_args()
    snapshot_series(
        series=args.series,
        iterations=args.iterations,
        interval=args.interval,
        out_dir=args.out_dir,
    )


if __name__ == "__main__":
    main()

