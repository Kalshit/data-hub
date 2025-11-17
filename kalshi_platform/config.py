"""
Environment-driven configuration helpers.

Provides dataclasses for Kalshi API, QuestDB hot/historical tiers, and
WebSocket client settings. The module also exposes a lightweight .env
loader to avoid introducing external dependencies.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


def load_env_file(path: Path | str = Path(".env")) -> None:
    """
    Populate os.environ from a simple KEY=VALUE file if present.

    Lines beginning with # or blank lines are ignored. Existing environment
    variables are left unchanged to favor explicitly exported values.
    """
    env_path = Path(path)
    if not env_path.exists():
        return
    for raw_line in env_path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


@dataclass
class KalshiAPIConfig:
    """Kalshi REST authentication details."""

    api_key: str
    private_key_path: Path
    base_url: str = "https://demo-api.kalshi.co"

    @classmethod
    def from_env(cls) -> KalshiAPIConfig:
        """Create config from environment variables."""
        api_key = os.getenv("KALSHI_API_KEY")
        private_key = os.getenv("KALSHI_PRIVATE_KEY_PATH")
        if not api_key or not private_key:
            raise ValueError(
                "KALSHI_API_KEY and KALSHI_PRIVATE_KEY_PATH must be set"
            )
        return cls(
            api_key=api_key,
            private_key_path=Path(private_key),
            base_url=os.getenv("KALSHI_BASE_URL", cls.base_url),
        )


@dataclass
class QuestDBConfig:
    """QuestDB connection parameters for hot and historical tiers."""

    rdb_host: str = "localhost"
    rdb_port: int = 9009
    rdb_username: Optional[str] = None
    rdb_password: Optional[str] = None
    rdb_use_tls: bool = False
    rdb_buffer_capacity: int = 1024 * 1024

    hdb_host: str = "localhost"
    hdb_port: int = 8812
    hdb_database: str = "qdb"
    hdb_username: str = "admin"
    hdb_password: str = "quest"
    hdb_data_path: Path = Path("data/hdb")
    hdb_retention_days: int = 90

    @classmethod
    def from_env(cls) -> QuestDBConfig:
        """Create config from environment variables."""
        return cls(
            rdb_host=os.getenv("QUESTDB_RDB_HOST", cls.rdb_host),
            rdb_port=int(os.getenv("QUESTDB_RDB_PORT", cls.rdb_port)),
            rdb_username=os.getenv("QUESTDB_RDB_USERNAME"),
            rdb_password=os.getenv("QUESTDB_RDB_PASSWORD"),
            rdb_use_tls=os.getenv("QUESTDB_RDB_USE_TLS", "false").lower()
            in {"1", "true", "yes"},
            rdb_buffer_capacity=int(
                os.getenv("QUESTDB_RDB_BUFFER_CAPACITY", cls.rdb_buffer_capacity)
            ),
            hdb_host=os.getenv("QUESTDB_HDB_HOST", cls.hdb_host),
            hdb_port=int(os.getenv("QUESTDB_HDB_PORT", cls.hdb_port)),
            hdb_database=os.getenv("QUESTDB_HDB_DATABASE", cls.hdb_database),
            hdb_username=os.getenv("QUESTDB_HDB_USERNAME", cls.hdb_username),
            hdb_password=os.getenv("QUESTDB_HDB_PASSWORD", cls.hdb_password),
            hdb_data_path=Path(
                os.getenv("QUESTDB_HDB_DATA_PATH", str(cls.hdb_data_path))
            ),
            hdb_retention_days=int(
                os.getenv(
                    "QUESTDB_HDB_RETENTION_DAYS", cls.hdb_retention_days
                )
            ),
        )


@dataclass
class WebSocketConfig:
    """Kalshi WebSocket connection parameters."""

    ws_url: str = "wss://api.elections.kalshi.com/trade-api/ws/v2"
    auth_token: Optional[str] = None
    heartbeat_interval: float = 15.0
    reconnect_backoff: float = 2.0

    @classmethod
    def from_env(cls) -> WebSocketConfig:
        """Create config from environment variables."""
        return cls(
            ws_url=os.getenv("KALSHI_WS_URL", cls.ws_url),
            auth_token=os.getenv("KALSHI_WS_AUTH_TOKEN"),
            heartbeat_interval=float(
                os.getenv("KALSHI_WS_HEARTBEAT_INTERVAL", cls.heartbeat_interval)
            ),
            reconnect_backoff=float(
                os.getenv("KALSHI_WS_RECONNECT_BACKOFF", cls.reconnect_backoff)
            ),
        )


def ensure_env_loaded(path: Path | str = Path(".env")) -> None:
    """
    Convenience wrapper combining file loading and validation.

    Call this at CLI entrypoints before referencing Kalshi or QuestDB
    configuration to ensure the environment has values available.
    """
    load_env_file(path)

