"""
QuestDB historical-tier (HDB) for warm storage.

SQL helpers for migrating RDB to HDB tables, Parquet export,
and partition pruning.
"""
from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import Any

@dataclass
class QuestDBHDBConfig:
    """HDB storage configuration."""
    data_path: Path = Path("data/hdb")
    retention_days: int = 90

class QuestDBHDBClient:
    """
    SQL interface for QuestDB historical storage operations.
    
    Wraps a PostgreSQL wire protocol connection (typically psycopg2 or similar)
    to execute DDL and DML commands for table creation, data migration, Parquet
    export, and partition lifecycle management.
    
    Attributes:
        connection: DB-API 2.0 compatible connection object
        config: Path and retention policy settings
    """
    def __init__(self, connection: Any, config: QuestDBHDBConfig) -> None:
        self.connection = connection
        self.config = config
    
    def create_tables(self) -> None:
        """
        Initialize HDB tables with daily partitioning.
        
        Creates trades_hdb and orderbook_deltas_hdb if they do not exist,
        using timestamp column for partition key.
        """
        sql = """
        CREATE TABLE IF NOT EXISTS trades_hdb AS (
            SELECT * FROM trades
        ) TIMESTAMP(timestamp) PARTITION BY DAY;

        CREATE TABLE IF NOT EXISTS orderbook_deltas_hdb AS (
            SELECT * FROM orderbook_deltas
        ) TIMESTAMP(timestamp) PARTITION BY DAY;
        """
        self._execute(sql)
    
    def migrate_from_rdb(self, table: str, date: str) -> None:
        """
        Copy one day of data from RDB to HDB table.
        
        Args:
            table: Base table name (e.g., "trades" -> "trades_hdb")
            date: Date string in YYYY-MM-DD format
        """
        sql = f"""
        INSERT INTO {table}_hdb
        SELECT * FROM {table}
        WHERE timestamp >= CAST(%s AS TIMESTAMP)
          AND timestamp < CAST(%s AS TIMESTAMP) + INTERVAL '1 day';
        """
        self._execute(sql, (date, date))
    
    def export_to_parquet(self, table: str, date: str) -> Path:
        """
        Export one day of data to Snappy-compressed Parquet file.
        
        Args:
            table: Table name to export
            date: Date string in YYYY-MM-DD format
            
        Returns:
            Path to written Parquet file
        """
        target_dir = self.config.data_path / table / f"date={date}"
        target_dir.mkdir(parents=True, exist_ok=True)
        output_path = target_dir / f"{table}.parquet"
        sql = f"""
        COPY (
            SELECT * FROM {table}
            WHERE timestamp >= CAST(%s AS TIMESTAMP)
              AND timestamp < CAST(%s AS TIMESTAMP) + INTERVAL '1 day'
        ) TO '{output_path.as_posix()}'
        (FORMAT parquet, COMPRESSION 'SNAPPY');
        """
        self._execute(sql, (date, date))
        return output_path
    
    def cleanup_old_partitions(self) -> None:
        """
        Drop partitions older than configured retention period.
        
        Uses retention_days from config to determine cutoff date.
        """
        sql = """
        ALTER TABLE trades_hdb DROP PARTITION LIST
        BEFORE now() - CAST(%s || 'd' AS INTERVAL);
        ALTER TABLE orderbook_deltas_hdb DROP PARTITION LIST
        BEFORE now() - CAST(%s || 'd' AS INTERVAL);
        """
        days = str(self.config.retention_days)
        self._execute(sql, (days, days))
    
    def _execute(self, sql: str, params: tuple | None = None) -> None:
        """
        Execute SQL statement with parameter substitution and auto-commit.
        
        Args:
            sql: SQL command (possibly with %s placeholders)
            params: Optional tuple of parameter values
        """
        cursor = self.connection.cursor()
        try:
            cursor.execute(sql, params or ())
            self.connection.commit()
        finally:
            cursor.close()
