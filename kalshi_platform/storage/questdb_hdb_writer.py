"""
QuestDB HDB writer using Postgres wire protocol.

Provides direct SQL access to QuestDB for historical data persistence,
table creation, batch inserts, and OHLC aggregation queries.
"""
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Set
import logging

logger = logging.getLogger(__name__)


def _register_datetime_adapter() -> None:
    try:
        from psycopg2.extensions import register_adapter, AsIs
        
        def adapt_datetime(dt):
            if dt.tzinfo is not None:
                dt = dt.replace(tzinfo=None)
            return AsIs(f"'{dt.isoformat()}'")
        
        register_adapter(datetime, adapt_datetime)
    except ImportError:
        pass

_register_datetime_adapter()


@dataclass
class QuestDBHDBWriterConfig:
    """Configuration for QuestDB Postgres wire connection."""
    host: str = "localhost"
    port: int = 8812
    database: str = "qdb"
    username: str = "admin"
    password: str = "quest"


TABLE_DEFINITIONS = {
    "series": """
        CREATE TABLE series (
            ticker SYMBOL CAPACITY 1024 CACHE,
            title STRING,
            category STRING,
            frequency STRING,
            tags STRING,
            fee_type STRING,
            inserted_at TIMESTAMP
        ) TIMESTAMP(inserted_at) PARTITION BY YEAR WAL
    """,
    "events": """
        CREATE TABLE events (
            ticker SYMBOL CAPACITY 4096 CACHE,
            series_ticker SYMBOL CAPACITY 1024 CACHE,
            title STRING,
            sub_title STRING,
            category STRING,
            mutually_exclusive BOOLEAN,
            inserted_at TIMESTAMP
        ) TIMESTAMP(inserted_at) PARTITION BY YEAR WAL
    """,
    "markets": """
        CREATE TABLE markets (
            ticker SYMBOL CAPACITY 65536 CACHE,
            event_ticker SYMBOL CAPACITY 4096 CACHE,
            title STRING,
            subtitle STRING,
            category STRING,
            status SYMBOL CAPACITY 16,
            yes_bid DOUBLE,
            yes_ask DOUBLE,
            no_bid DOUBLE,
            no_ask DOUBLE,
            last_price DOUBLE,
            volume LONG,
            volume_24h LONG,
            open_interest LONG,
            liquidity LONG,
            close_time TIMESTAMP,
            expiration_time TIMESTAMP,
            created_time TIMESTAMP
        ) TIMESTAMP(created_time) PARTITION BY YEAR WAL
    """,
    "trades_hdb": """
        CREATE TABLE trades_hdb (
            ticker SYMBOL CAPACITY 65536 CACHE,
            trade_id STRING,
            yes_price DOUBLE,
            no_price DOUBLE,
            count LONG,
            taker_side SYMBOL CAPACITY 8,
            created_time TIMESTAMP
        ) TIMESTAMP(created_time) PARTITION BY DAY WAL
    """,
    "ohlc_1d": """
        CREATE TABLE ohlc_1d (
            ticker SYMBOL CAPACITY 65536 CACHE,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume LONG,
            trade_count LONG,
            vwap DOUBLE,
            timestamp TIMESTAMP
        ) TIMESTAMP(timestamp) PARTITION BY MONTH WAL
    """,
    "ohlc_1h": """
        CREATE TABLE ohlc_1h (
            ticker SYMBOL CAPACITY 65536 CACHE,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume LONG,
            trade_count LONG,
            vwap DOUBLE,
            timestamp TIMESTAMP
        ) TIMESTAMP(timestamp) PARTITION BY MONTH WAL
    """,
    "backfill_progress": """
        CREATE TABLE backfill_progress (
            ticker SYMBOL CAPACITY 65536 CACHE,
            last_trade_time TIMESTAMP,
            trade_count LONG,
            updated_time TIMESTAMP
        ) TIMESTAMP(updated_time) PARTITION BY YEAR WAL
    """,
    "candlesticks": """
        CREATE TABLE candlesticks (
            ticker SYMBOL CAPACITY 65536 CACHE,
            series_ticker SYMBOL CAPACITY 1024 CACHE,
            period_interval INT,
            open_price DOUBLE,
            high_price DOUBLE,
            low_price DOUBLE,
            close_price DOUBLE,
            volume LONG,
            open_interest LONG,
            end_period_ts TIMESTAMP
        ) TIMESTAMP(end_period_ts) PARTITION BY MONTH WAL
    """,
}


class QuestDBHDBWriter:
    """
    SQL-based writer for QuestDB historical database.
    
    Uses psycopg2 for Postgres wire protocol access to QuestDB,
    enabling DDL operations, batch inserts, and aggregation queries
    that aren't possible via ILP.
    
    Attributes:
        config: Connection parameters
        connection: Active database connection
    """
    
    MAX_CONNECT_RETRIES = 5
    CONNECT_RETRY_DELAY = 2  # seconds
    
    def __init__(
        self,
        config: QuestDBHDBWriterConfig,
        connection: Optional[Any] = None,
    ) -> None:
        self.config = config
        self._connection = connection
        self._existing_tables: Optional[Set[str]] = None
    
    @property
    def connection(self) -> Any:
        """Lazy connection establishment with reconnect on failure."""
        if self._connection is None:
            self._connection = self._connect()
        else:
            # Check if connection is still alive
            try:
                cursor = self._connection.cursor()
                cursor.execute("SELECT 1")
                cursor.close()
            except Exception:
                logger.warning("Connection lost, reconnecting...")
                self._connection = None
                self._connection = self._connect()
        return self._connection
    
    def reconnect(self) -> None:
        """Force reconnection to QuestDB."""
        if self._connection is not None:
            try:
                self._connection.close()
            except Exception:
                pass
        self._connection = None
        self._existing_tables = None
        # Trigger reconnect
        _ = self.connection
    
    def _connect(self) -> Any:
        """Establish psycopg2 connection to QuestDB with retries."""
        import psycopg2
        import time
        
        last_error = None
        for attempt in range(1, self.MAX_CONNECT_RETRIES + 1):
            try:
                conn = psycopg2.connect(
                    host=self.config.host,
                    port=self.config.port,
                    dbname=self.config.database,
                    user=self.config.username,
                    password=self.config.password,
                    connect_timeout=30,
                )
                logger.info(f"Connected to QuestDB at {self.config.host}:{self.config.port}")
                return conn
            except psycopg2.OperationalError as e:
                last_error = e
                if attempt < self.MAX_CONNECT_RETRIES:
                    logger.warning(
                        f"Connection attempt {attempt}/{self.MAX_CONNECT_RETRIES} failed: {e}. "
                        f"Retrying in {self.CONNECT_RETRY_DELAY}s..."
                    )
                    time.sleep(self.CONNECT_RETRY_DELAY)
        
        raise last_error
    
    def _get_existing_tables(self) -> Set[str]:
        """Query QuestDB for list of existing tables."""
        if self._existing_tables is not None:
            return self._existing_tables
        
        cursor = self.connection.cursor()
        cursor.execute("SELECT table_name FROM tables()")
        self._existing_tables = {row[0] for row in cursor.fetchall()}
        cursor.close()
        return self._existing_tables
    
    def _table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the database."""
        return table_name in self._get_existing_tables()
    
    def _create_table_if_missing(self, table_name: str) -> bool:
        """
        Create a table if it doesn't exist.
        
        Args:
            table_name: Name of the table to create
            
        Returns:
            True if table was created, False if it already existed
        """
        if self._table_exists(table_name):
            return False
        
        if table_name not in TABLE_DEFINITIONS:
            raise ValueError(f"Unknown table: {table_name}")
        
        cursor = self.connection.cursor()
        cursor.execute(TABLE_DEFINITIONS[table_name])
        self.connection.commit()
        cursor.close()
        
        # Invalidate cache
        self._existing_tables = None
        logger.info(f"Created table: {table_name}")
        return True
    
    def create_tables(self) -> None:
        """Initialize all HDB tables that don't exist yet."""
        for table_name in TABLE_DEFINITIONS:
            self._create_table_if_missing(table_name)
    
    def drop_table(self, table_name: str) -> bool:
        """
        Drop a table if it exists.
        
        Args:
            table_name: Name of the table to drop
            
        Returns:
            True if table was dropped, False if it didn't exist
        """
        if not self._table_exists(table_name):
            return False
        
        cursor = self.connection.cursor()
        cursor.execute(f"DROP TABLE {table_name}")
        self.connection.commit()
        cursor.close()
        
        # Invalidate cache
        self._existing_tables = None
        logger.info(f"Dropped table: {table_name}")
        return True
    
    def reset_tables(self) -> None:
        """Drop and recreate all HDB tables."""
        for table_name in TABLE_DEFINITIONS:
            self.drop_table(table_name)
        self.create_tables()
    
    def write_series(self, series: Dict[str, Any]) -> None:
        """
        Insert a series record.
        
        Args:
            series: Series payload from Kalshi API
        """
        self._create_table_if_missing("series")
        cursor = self.connection.cursor()
        cursor.execute(
            """
            INSERT INTO series (
                ticker, title, category, frequency, tags, fee_type, inserted_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                series.get("ticker"),
                series.get("title"),
                series.get("category"),
                series.get("frequency"),
                ",".join(series.get("tags", [])) if series.get("tags") else None,
                series.get("fee_type"),
                datetime.utcnow(),            ),
        )
        self.connection.commit()
        cursor.close()
    
    def write_series_batch(self, series_list: List[Dict[str, Any]]) -> int:
        """
        Batch insert series records.
        
        Args:
            series_list: List of series payloads
            
        Returns:
            Number of records written
        """
        if not series_list:
            return 0
        
        self._create_table_if_missing("series")
        cursor = self.connection.cursor()
        count = 0
        now = datetime.utcnow()
        
        for series in series_list:
            cursor.execute(
                """
                INSERT INTO series (
                    ticker, title, category, frequency, tags, fee_type, inserted_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    series.get("ticker"),
                    series.get("title"),
                    series.get("category"),
                    series.get("frequency"),
                    ",".join(series.get("tags", [])) if series.get("tags") else None,
                    series.get("fee_type"),
                    now,                ),
            )
            count += 1
        
        self.connection.commit()
        cursor.close()
        return count
    
    def write_event(self, event: Dict[str, Any]) -> None:
        """
        Insert an event record.
        
        Args:
            event: Event payload from Kalshi API
        """
        self._create_table_if_missing("events")
        cursor = self.connection.cursor()
        cursor.execute(
            """
            INSERT INTO events (
                ticker, series_ticker, title, sub_title, category,
                mutually_exclusive, inserted_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                event.get("event_ticker") or event.get("ticker"),
                event.get("series_ticker"),
                event.get("title"),
                event.get("sub_title"),
                event.get("category"),
                event.get("mutually_exclusive"),
                datetime.utcnow(),            ),
        )
        self.connection.commit()
        cursor.close()
    
    def write_events_batch(self, events: List[Dict[str, Any]]) -> int:
        """
        Batch insert event records.
        
        Args:
            events: List of event payloads
            
        Returns:
            Number of records written
        """
        if not events:
            return 0
        
        self._create_table_if_missing("events")
        cursor = self.connection.cursor()
        count = 0
        now = datetime.utcnow()
        
        for event in events:
            cursor.execute(
                """
                INSERT INTO events (
                    ticker, series_ticker, title, sub_title, category,
                    mutually_exclusive, inserted_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    event.get("event_ticker") or event.get("ticker"),
                    event.get("series_ticker"),
                    event.get("title"),
                    event.get("sub_title"),
                    event.get("category"),
                    event.get("mutually_exclusive"),
                    now,                ),
            )
            count += 1
        
        self.connection.commit()
        cursor.close()
        return count
    
    def write_market(self, market: Dict[str, Any]) -> None:
        """
        Insert a market record.
        
        Args:
            market: Market payload from Kalshi API
        """
        self._create_table_if_missing("markets")
        cursor = self.connection.cursor()
        cursor.execute(
            """
            INSERT INTO markets (
                ticker, event_ticker, title, subtitle,
                category, status, yes_bid, yes_ask,
                no_bid, no_ask, last_price, volume, volume_24h,
                open_interest, liquidity, close_time, expiration_time,
                created_time
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s
            )
            """,
            (
                market.get("ticker"),
                market.get("event_ticker"),
                market.get("title"),
                market.get("subtitle"),
                market.get("category"),
                market.get("status"),
                market.get("yes_bid"),
                market.get("yes_ask"),
                market.get("no_bid"),
                market.get("no_ask"),
                market.get("last_price"),
                market.get("volume"),
                market.get("volume_24h"),
                market.get("open_interest"),
                market.get("liquidity"),
                _parse_timestamp(market.get("close_time")),
                _parse_timestamp(market.get("expiration_time")),
                _parse_timestamp(market.get("created_time"), default_to_now=True),            ),
        )
        self.connection.commit()
        cursor.close()
    
    def write_markets_batch(self, markets: List[Dict[str, Any]]) -> int:
        """
        Batch insert market records.
        
        Args:
            markets: List of market payloads
            
        Returns:
            Number of records written
        """
        if not markets:
            return 0
        
        self._create_table_if_missing("markets")
        cursor = self.connection.cursor()
        count = 0
        
        for market in markets:
            cursor.execute(
                """
                INSERT INTO markets (
                    ticker, event_ticker, title, subtitle,
                    category, status, yes_bid, yes_ask,
                    no_bid, no_ask, last_price, volume, volume_24h,
                    open_interest, liquidity, close_time, expiration_time,
                    created_time
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s
                )
                """,
                (
                    market.get("ticker"),
                    market.get("event_ticker"),
                    market.get("title"),
                    market.get("subtitle"),
                    market.get("category"),
                    market.get("status"),
                    market.get("yes_bid"),
                    market.get("yes_ask"),
                    market.get("no_bid"),
                    market.get("no_ask"),
                    market.get("last_price"),
                    market.get("volume"),
                    market.get("volume_24h"),
                    market.get("open_interest"),
                    market.get("liquidity"),
                    _parse_timestamp(market.get("close_time")),
                    _parse_timestamp(market.get("expiration_time")),
                    _parse_timestamp(market.get("created_time"), default_to_now=True),                ),
            )
            count += 1
        
        self.connection.commit()
        cursor.close()
        return count
    
    def write_trade(self, trade: Dict[str, Any]) -> None:
        """
        Insert a trade record.
        
        Args:
            trade: Trade payload from Kalshi API
        """
        self._create_table_if_missing("trades_hdb")
        cursor = self.connection.cursor()
        cursor.execute(
            """
            INSERT INTO trades_hdb (
                ticker, trade_id, yes_price, no_price,
                count, taker_side, created_time
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                trade.get("ticker"),
                trade.get("trade_id"),
                trade.get("yes_price"),
                trade.get("no_price"),
                trade.get("count"),
                trade.get("taker_side"),
                _parse_timestamp(trade.get("created_time"), default_to_now=True),            ),
        )
        self.connection.commit()
        cursor.close()
    
    def write_trades_batch(self, trades: List[Dict[str, Any]]) -> int:
        """
        Batch insert trade records.
        
        Args:
            trades: List of trade payloads
            
        Returns:
            Number of records written
        """
        if not trades:
            return 0
        
        self._create_table_if_missing("trades_hdb")
        cursor = self.connection.cursor()
        count = 0
        
        for trade in trades:
            cursor.execute(
                """
                INSERT INTO trades_hdb (
                    ticker, trade_id, yes_price, no_price,
                    count, taker_side, created_time
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    trade.get("ticker"),
                    trade.get("trade_id"),
                    trade.get("yes_price"),
                    trade.get("no_price"),
                    trade.get("count"),
                    trade.get("taker_side"),
                    _parse_timestamp(trade.get("created_time"), default_to_now=True),                ),
            )
            count += 1
        
        self.connection.commit()
        cursor.close()
        return count
    
    def write_candlestick(
        self,
        candle: Dict[str, Any],
        ticker: str,
        series_ticker: str,
        period_interval: int,
    ) -> None:
        """
        Insert a candlestick record.
        
        Args:
            candle: Candlestick payload from Kalshi API
            ticker: Market ticker
            series_ticker: Series ticker
            period_interval: Candle period in minutes
        """
        self._create_table_if_missing("candlesticks")
        cursor = self.connection.cursor()
        cursor.execute(
            """
            INSERT INTO candlesticks (
                ticker, series_ticker, period_interval,
                open_price, high_price, low_price, close_price,
                volume, open_interest, end_period_ts
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                ticker,
                series_ticker,
                period_interval,
                candle.get("open"),
                candle.get("high"),
                candle.get("low"),
                candle.get("close"),
                candle.get("volume"),
                candle.get("open_interest"),
                _parse_timestamp(candle.get("end_period_ts"), default_to_now=True),
            ),
        )
        self.connection.commit()
        cursor.close()
    
    def write_candlesticks_batch(
        self,
        candles: List[Dict[str, Any]],
        ticker: str,
        series_ticker: str,
        period_interval: int,
    ) -> int:
        """
        Batch insert candlestick records.
        
        Args:
            candles: List of candlestick payloads
            ticker: Market ticker
            series_ticker: Series ticker
            period_interval: Candle period in minutes
            
        Returns:
            Number of records written
        """
        if not candles:
            return 0
        
        self._create_table_if_missing("candlesticks")
        cursor = self.connection.cursor()
        count = 0
        
        for candle in candles:
            cursor.execute(
                """
                INSERT INTO candlesticks (
                    ticker, series_ticker, period_interval,
                    open_price, high_price, low_price, close_price,
                    volume, open_interest, end_period_ts
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    ticker,
                    series_ticker,
                    period_interval,
                    candle.get("open"),
                    candle.get("high"),
                    candle.get("low"),
                    candle.get("close"),
                    candle.get("volume"),
                    candle.get("open_interest"),
                    _parse_timestamp(candle.get("end_period_ts"), default_to_now=True),
                ),
            )
            count += 1
        
        self.connection.commit()
        cursor.close()
        return count
    
    def update_backfill_progress(
        self,
        ticker: str,
        last_trade_time: datetime,
        trade_count: int,
    ) -> None:
        """
        Record backfill progress for resume capability.
        
        Args:
            ticker: Market ticker
            last_trade_time: Timestamp of last processed trade
            trade_count: Total trades processed for this ticker
        """
        self._create_table_if_missing("backfill_progress")
        cursor = self.connection.cursor()
        cursor.execute(
            """
            INSERT INTO backfill_progress (
                ticker, last_trade_time, trade_count, updated_time
            ) VALUES (%s, %s, %s, %s)
            """,
            (
                ticker,
                last_trade_time,
                trade_count,
                datetime.utcnow()
            ),
        )
        self.connection.commit()
        cursor.close()
    
    def get_backfill_progress(self, ticker: str) -> Optional[Dict[str, Any]]:
        """
        Get last backfill progress for a ticker.
        
        Args:
            ticker: Market ticker
            
        Returns:
            Progress record or None if not found or table doesn't exist
        """
        if not self._table_exists("backfill_progress"):
            return None
        
        cursor = self.connection.cursor()
        cursor.execute(
            """
            SELECT ticker, last_trade_time, trade_count, updated_time
            FROM backfill_progress
            WHERE ticker = %s
            ORDER BY updated_time DESC
            LIMIT 1
            """,
            (ticker,),
        )
        row = cursor.fetchone()
        cursor.close()
        
        if row:
            return {
                "ticker": row[0],
                "last_trade_time": row[1],
                "trade_count": row[2],
                "updated_time": row[3],
            }
        return None
    
    def generate_ohlc_daily(
        self,
        ticker: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> int:
        """
        Generate daily OHLC aggregations from trades.
        
        Uses QuestDB's SAMPLE BY for efficient time-based aggregation.
        
        Args:
            ticker: Optional ticker filter
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            
        Returns:
            Number of OHLC records generated
        """
        if not self._table_exists("trades_hdb"):
            logger.warning("trades_hdb table doesn't exist, skipping OHLC generation")
            return 0
        
        self._create_table_if_missing("ohlc_1d")
        
        cursor = self.connection.cursor()
        
        where_clauses = []
        params = []
        param_idx = 1
        
        if ticker:
            where_clauses.append(f"ticker = ${param_idx}")
            params.append(ticker)
            param_idx += 1
        if start_date:
            where_clauses.append(f"created_time >= ${param_idx}")
            params.append(start_date)
            param_idx += 1
        if end_date:
            where_clauses.append(f"created_time < ${param_idx}")
            params.append(end_date)
            param_idx += 1
        
        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        
        # QuestDB SAMPLE BY aggregation
        sql = f"""
        INSERT INTO ohlc_1d
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
        {where_sql}
        SAMPLE BY 1d ALIGN TO CALENDAR
        """
        
        cursor.execute(sql, tuple(params) if params else None)
        self.connection.commit()
        
        # Get count of inserted records
        cursor.execute("SELECT count() FROM ohlc_1d")
        result = cursor.fetchone()
        cursor.close()
        return result[0] if result else 0
    
    def generate_ohlc_hourly(
        self,
        ticker: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> int:
        """
        Generate hourly OHLC aggregations from trades.
        
        Args:
            ticker: Optional ticker filter
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            
        Returns:
            Number of OHLC records generated
        """
        if not self._table_exists("trades_hdb"):
            logger.warning("trades_hdb table doesn't exist, skipping OHLC generation")
            return 0
        
        self._create_table_if_missing("ohlc_1h")
        
        cursor = self.connection.cursor()
        
        where_clauses = []
        params = []
        param_idx = 1
        
        if ticker:
            where_clauses.append(f"ticker = ${param_idx}")
            params.append(ticker)
            param_idx += 1
        if start_date:
            where_clauses.append(f"created_time >= ${param_idx}")
            params.append(start_date)
            param_idx += 1
        if end_date:
            where_clauses.append(f"created_time < ${param_idx}")
            params.append(end_date)
            param_idx += 1
        
        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        
        sql = f"""
        INSERT INTO ohlc_1h
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
        {where_sql}
        SAMPLE BY 1h ALIGN TO CALENDAR
        """
        
        cursor.execute(sql, tuple(params) if params else None)
        self.connection.commit()
        
        cursor.execute("SELECT count() FROM ohlc_1h")
        result = cursor.fetchone()
        cursor.close()
        return result[0] if result else 0
    
    def get_trade_count(self, ticker: Optional[str] = None) -> int:
        """
        Get total trade count, optionally filtered by ticker.
        
        Args:
            ticker: Optional ticker filter
            
        Returns:
            Number of trades, or 0 if table doesn't exist
        """
        if not self._table_exists("trades_hdb"):
            return 0
        
        cursor = self.connection.cursor()
        if ticker:
            cursor.execute(
                "SELECT count() FROM trades_hdb WHERE ticker = %s",
                (ticker,),
            )
        else:
            cursor.execute("SELECT count() FROM trades_hdb")
        result = cursor.fetchone()
        cursor.close()
        return result[0] if result else 0
    
    def get_market_tickers(self) -> List[str]:
        """
        Get all unique market tickers from the markets table.
        
        Returns:
            List of ticker strings, or empty list if table doesn't exist
        """
        if not self._table_exists("markets"):
            return []
        
        cursor = self.connection.cursor()
        cursor.execute("SELECT DISTINCT ticker FROM markets")
        result = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return result
    
    def close(self) -> None:
        """Close database connection."""
        if self._connection:
            self._connection.close()
            self._connection = None


def _parse_timestamp(value: Any, default_to_now: bool = False) -> Optional[datetime]:
    """
    Parse various timestamp formats to naive datetime.
    
    Args:
        value: Timestamp string, int (ms), or None
        default_to_now: If True, return current time when value is None/invalid
        
    Returns:
        Naive datetime or None
    """
    if value is None:
        return datetime.utcnow() if default_to_now else None
    
    if isinstance(value, datetime):
        # Strip timezone if present
        if value.tzinfo is not None:
            return value.replace(tzinfo=None)
        return value
    
    if isinstance(value, (int, float)):
        if value <= 0:
            return datetime.utcnow() if default_to_now else None
        # Assume milliseconds if value is large enough
        if value > 1e10:
            return datetime.utcfromtimestamp(value / 1000)
        return datetime.utcfromtimestamp(value)
    
    if isinstance(value, str):
        if not value or value.strip() == "":
            return datetime.utcnow() if default_to_now else None
        # Handle timezone-aware ISO strings (strip +00:00 or Z suffix)
        clean_value = value.replace("+00:00", "").replace("Z", "")
        for fmt in (
            "%Y-%m-%dT%H:%M:%S.%f",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d %H:%M:%S.%f",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d",
        ):
            try:
                return datetime.strptime(clean_value, fmt)
            except ValueError:
                continue
    
    return datetime.utcnow() if default_to_now else None


__all__ = ["QuestDBHDBWriter", "QuestDBHDBWriterConfig"]
