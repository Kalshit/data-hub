from pathlib import Path
import tempfile

from kalshi_platform.storage.questdb_hdb import QuestDBHDBClient, QuestDBHDBConfig
from kalshi_platform.storage.questdb_rdb import QuestDBRDBConfig, QuestDBRDBWriter


class FakeSender:
    def __init__(self) -> None:
        self.rows = []
        self.closed = False

    def row(self, table, symbols=None, columns=None, at=None):
        self.rows.append((table, symbols, columns, at))

    def flush(self):
        return True

    def close(self):
        self.closed = True


def test_rdb_writer_shapes_rows():
    sender = FakeSender()
    writer = QuestDBRDBWriter(QuestDBRDBConfig(), sender=sender)

    writer.write_trade(
        {
            "ticker": "TEST",
            "taker_side": "yes",
            "price": 55,
            "count": 10,
            "trade_id": "abc",
            "timestamp": 1234567890000,
        }
    )

    table, symbols, columns, at = sender.rows[0]
    assert table == "trades"
    assert symbols["ticker"] == "TEST"
    assert columns["trade_id"] == "abc"
    assert at == 1234567890000 * 1_000_000


class FakeCursor:
    def __init__(self, log):
        self.log = log

    def execute(self, sql, params=()):
        self.log.append((sql.strip(), params))

    def close(self):
        return None


class FakeConnection:
    def __init__(self):
        self.log = []

    def cursor(self):
        return FakeCursor(self.log)

    def commit(self):
        return None


def test_hdb_client_builds_sql():
    conn = FakeConnection()
    tmp_dir = Path(tempfile.mkdtemp())
    client = QuestDBHDBClient(conn, QuestDBHDBConfig(data_path=tmp_dir, retention_days=7))

    client.migrate_from_rdb("trades", "2024-11-15")
    client.export_to_parquet("trades_hdb", "2024-11-15")
    client.cleanup_old_partitions()

    statements = [stmt for stmt, _ in conn.log]
    assert "INSERT INTO trades_hdb" in statements[0]
    assert "COPY (" in statements[1]
    assert "ALTER TABLE trades_hdb" in statements[2]

