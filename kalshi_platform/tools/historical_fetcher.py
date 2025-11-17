"""Authenticated Kalshi trade backfill with RSA-PSS signing."""
from __future__ import annotations
import argparse, base64, time
import datetime as dt
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional
import requests
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from kalshi_platform.config import (
    KalshiAPIConfig,
    QuestDBConfig,
    ensure_env_loaded,
)
from kalshi_platform.storage.questdb_rdb import (
    QuestDBRDBConfig,
    QuestDBRDBWriter,
)

MAX_RETRY_ATTEMPTS = 10

@dataclass
class KalshiSigner:
    """RSA-PSS request signer per https://docs.kalshi.com."""
    api_key: str
    private_key_path: Path
    def __post_init__(self) -> None:
        """Load PEM private key from disk."""
        with self.private_key_path.open("rb") as f:
            self.private_key: rsa.RSAPrivateKey = (
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

class HistoricalDataFetcher:
    """Trade history retrieval with rate limit backoff."""
    def __init__(
        self, base_url: str, signer: KalshiSigner,
        writer: QuestDBRDBWriter,
        session: Optional[requests.Session] = None,
        sleep_fn: Optional[Any] = None
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.signer = signer
        self.writer = writer
        self.session = session or requests.Session()
        self._sleep = sleep_fn or time.sleep
    def _request(self, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """Signed GET with bounded exponential backoff on 429."""
        for attempt in range(MAX_RETRY_ATTEMPTS):
            hdrs = self.signer.build_headers("GET", path)
            resp = self.session.get(
                f"{self.base_url}{path}", params=params,
                headers=hdrs, timeout=30
            )
            if resp.status_code != 429:
                resp.raise_for_status()
                return resp.json()
            self._sleep(min(60, 2**attempt))
        raise RuntimeError(f"Max retries ({MAX_RETRY_ATTEMPTS}) exceeded")
    def fetch_trades(
        self, ticker: str, min_ts: Optional[int] = None,
        max_ts: Optional[int] = None
    ) -> int:
        """Cursor-paginated trade fetch returning count written."""
        path, total = "/trade-api/v2/markets/trades", 0
        params: Dict[str, Any] = {"limit": 1000, "ticker": ticker}
        if min_ts is not None:
            params["min_ts"] = min_ts
        if max_ts is not None:
            params["max_ts"] = max_ts
        cursor: Optional[str] = None
        for _ in range(10000):
            if cursor: params["cursor"] = cursor
            payload = self._request(path, params)
            trades = payload.get("trades", [])
            for trade in trades: self.writer.write_trade(trade)
            total += len(trades)
            cursor = payload.get("cursor")
            if not cursor: break
        self.writer.flush()
        return total
    def backfill_range(
        self, ticker: str, start: dt.date, end: dt.date
    ) -> int:
        """Day-by-day backfill returning aggregate count."""
        current, total = start, 0
        day_count = (end - start).days + 1
        for _ in range(day_count):
            if current > end: break
            ts_start = int(
                dt.datetime.combine(current, dt.time()).timestamp()
            )
            ts_end = int(
                dt.datetime.combine(
                    current + dt.timedelta(days=1), dt.time()
                ).timestamp()
            )
            total += self.fetch_trades(ticker, min_ts=ts_start,
                                       max_ts=ts_end)
            current += dt.timedelta(days=1)
        return total

def parse_date(val: str) -> dt.date:
    """Parse YYYY-MM-DD to date."""
    return dt.datetime.strptime(val, "%Y-%m-%d").date()

def main() -> None:
    """CLI for authenticated trade backfill."""
    ensure_env_loaded()
    env_api: Optional[KalshiAPIConfig] = None
    try:
        env_api = KalshiAPIConfig.from_env()
    except ValueError:
        env_api = None
    env_qdb = QuestDBConfig.from_env()
    p = argparse.ArgumentParser(
        description="Backfill Kalshi trades via signed API."
    )
    p.add_argument("--ticker", required=True)
    p.add_argument("--start", required=True, help="YYYY-MM-DD")
    p.add_argument("--end", required=True, help="YYYY-MM-DD")
    p.add_argument(
        "--api-key",
        help="Defaults to KALSHI_API_KEY environment variable.",
    )
    p.add_argument(
        "--private-key",
        help="Defaults to KALSHI_PRIVATE_KEY_PATH environment variable.",
    )
    p.add_argument(
        "--base-url",
        help="Defaults to KALSHI_BASE_URL environment variable.",
    )
    p.add_argument(
        "--questdb-host",
        help="Defaults to QUESTDB_RDB_HOST environment variable.",
    )
    p.add_argument(
        "--questdb-port",
        type=int,
        help="Defaults to QUESTDB_RDB_PORT environment variable.",
    )
    args = p.parse_args()
    api_key = args.api_key or (env_api.api_key if env_api else None)
    private_key = args.private_key or (
        str(env_api.private_key_path) if env_api else None
    )
    base_url = (
        args.base_url
        or (env_api.base_url if env_api else KalshiAPIConfig.base_url)
    )
    if not api_key or not private_key:
        raise SystemExit(
            "Provide --api-key/--private-key or set KALSHI_API_KEY / "
            "KALSHI_PRIVATE_KEY_PATH."
        )
    questdb_host = args.questdb_host or env_qdb.rdb_host
    questdb_port = args.questdb_port or env_qdb.rdb_port
    w = QuestDBRDBWriter(
        QuestDBRDBConfig(host=questdb_host, port=questdb_port)
    )
    s = KalshiSigner(api_key=api_key, private_key_path=Path(private_key))
    f = HistoricalDataFetcher(base_url, s, w)
    cnt = f.backfill_range(args.ticker, parse_date(args.start),
                           parse_date(args.end))
    print(f"Inserted {cnt} trades for {args.ticker} "
          f"({args.start} to {args.end})")

if __name__ == "__main__":
    main()
