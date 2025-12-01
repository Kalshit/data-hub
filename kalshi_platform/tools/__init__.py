"""
CLI tools for Kalshi data management.

Available tools (run with python -m kalshi_platform.tools.<name>):
- full_backfill: Complete historical backfill to QuestDB
- fix_markets: Re-fetch and fix markets table data
- fix_candlesticks: Re-fetch and fix candlesticks table data
- generate_ohlc: Generate OHLC aggregations from trades
- check_consistency: Validate data consistency across tables
- historical_fetcher: Fetch historical trades for specific ticker
- market_data_recorder: Record real-time market data
- public_demo: Demo public API endpoints
"""
