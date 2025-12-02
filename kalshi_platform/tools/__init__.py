"""
CLI tools for Kalshi data management.

Available tools (run with python -m kalshi_platform.tools.<name>):

Data Management:
- full_backfill: Complete historical backfill to QuestDB
- fix_markets: Re-fetch and fix markets table data
- fix_candlesticks: Re-fetch and fix candlesticks table data
- generate_ohlc: Generate OHLC aggregations from trades
- check_consistency: Quick data sanity check

Real-Time Streaming:
- stream_orderbook: Stream order book data from WebSocket to RDB
- migrate_rdb_to_hdb: EOD migration from RDB (hot) to HDB (warm)

Demo & Recording:
- historical_fetcher: Fetch historical trades for specific ticker
- market_data_recorder: Record real-time market data
- public_demo: Demo public API endpoints
"""
