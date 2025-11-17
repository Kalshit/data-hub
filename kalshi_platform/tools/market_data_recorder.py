"""Persist ticker plant messages into QuestDB with batching."""
from __future__ import annotations
import argparse, json, os
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Tuple
from kalshi_platform.config import QuestDBConfig, ensure_env_loaded
from kalshi_platform.storage.questdb_rdb import (
    QuestDBRDBConfig,
    QuestDBRDBWriter,
)

class MarketDataRecorder:
    """Buffer and batch-write ticker messages to QuestDB."""
    def __init__(
        self, writer: QuestDBRDBWriter, batch_size: int = 500
    ) -> None:
        self.writer = writer
        self.batch_size = batch_size
        self._buffer: List[Tuple[str, Dict[str, Any]]] = []
        self._dispatch: Dict[str, Callable[[Dict[str, Any]], None]] = {
            "trade": writer.write_trade,
            "orderbook_delta": writer.write_orderbook_delta,
            "ticker": writer.write_ticker,
            "bbo": writer.write_bbo,
        }
    def record(self, channel: str, payload: Dict[str, Any]) -> None:
        """Append to buffer and flush when threshold reached."""
        self._buffer.append((channel, payload))
        if len(self._buffer) >= self.batch_size:
            self.flush()
    def flush(self) -> None:
        """Write buffered messages via dispatch table and clear."""
        if not self._buffer:
            return
        for ch, pl in self._buffer:
            handler = self._dispatch.get(ch)
            if handler:
                handler(pl)
        self.writer.flush()
        self._buffer.clear()
    def close(self) -> None:
        """Final flush and close writer."""
        self.flush()
        self.writer.close()

def load_feed(path: Path) -> Iterable[Tuple[str, Dict[str, Any]]]:
    """Parse JSONL yielding (channel, payload) tuples."""
    with path.open("r", encoding="utf-8") as h:
        for line in h:
            if not line.strip():
                continue
            raw = json.loads(line)
            yield raw["channel"], raw["message"]

def main() -> None:
    """CLI for batch recording ticker feed to QuestDB."""
    ensure_env_loaded()
    env_qdb = QuestDBConfig.from_env()
    p = argparse.ArgumentParser(
        description="Batch Kalshi ticker messages to QuestDB."
    )
    p.add_argument("--feed-file", required=True,
                   help="JSONL with {'channel','message'} records")
    p.add_argument(
        "--batch-size",
        type=int,
        default=int(
            os.getenv("RECORDER_BATCH_SIZE", "500")  # type: ignore[arg-type]
        ),
    )
    p.add_argument(
        "--questdb-host",
        help="Defaults to QUESTDB_RDB_HOST.",
    )
    p.add_argument(
        "--questdb-port",
        type=int,
        help="Defaults to QUESTDB_RDB_PORT.",
    )
    args = p.parse_args()
    questdb_host = args.questdb_host or env_qdb.rdb_host
    questdb_port = args.questdb_port or env_qdb.rdb_port
    w = QuestDBRDBWriter(
        QuestDBRDBConfig(host=questdb_host, port=questdb_port)
    )
    rec = MarketDataRecorder(w, batch_size=args.batch_size)
    try:
        for ch, pl in load_feed(Path(args.feed_file)):
            rec.record(ch, pl)
        rec.flush()
    finally:
        rec.close()

if __name__ == "__main__":
    main()

