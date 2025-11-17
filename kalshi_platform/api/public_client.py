"""
Public Kalshi REST client for unauthenticated endpoints.

Wraps docs.kalshi.com API with helpers for series, markets,
orderbook, and trades retrieval.
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, Generator, List, Optional
import requests

API_BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"

class KalshiApiError(RuntimeError):
    """Raised when the Kalshi API returns an unexpected payload."""
    pass

@dataclass(frozen=True)
class MarketSummary:
    """Condensed market snapshot for display purposes."""
    ticker: str
    title: str
    event_ticker: str
    yes_price: Optional[int]
    volume: Optional[int]

class PublicKalshiClient:
    """
    Lightweight helper for unauthenticated market data endpoints.
    
    Maintains a reusable requests.Session for efficient multi-request
    workflows in demonstrations and CLI tools.
    
    Attributes:
        base_url: API root, defaults to production elections endpoint
        timeout: Request timeout in seconds
        session: Underlying HTTP session for connection pooling
    """
    def __init__(
        self,
        base_url: str = API_BASE_URL,
        timeout: float = 10.0,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = session or requests.Session()
    
    def get_series(self, ticker: str) -> Dict[str, Any]:
        """
        Retrieve series metadata by ticker.
        
        Args:
            ticker: Series identifier (e.g., "KXHIGHNY")
            
        Returns:
            JSON response containing series details
        """
        return self._request("GET", f"/series/{ticker.upper()}")
    
    def get_event(self, ticker: str) -> Dict[str, Any]:
        """
        Retrieve event metadata by ticker.
        
        Args:
            ticker: Event identifier
            
        Returns:
            JSON response containing event details
        """
        return self._request("GET", f"/events/{ticker.upper()}")
    
    def get_markets(
        self,
        series_ticker: Optional[str] = None,
        status: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        List markets with optional filtering.
        
        Args:
            series_ticker: Filter by series (e.g., "KXHIGHNY")
            status: Filter by status ("open", "closed", etc.)
            limit: Maximum results to return
            
        Returns:
            JSON response containing markets array
        """
        params: Dict[str, Any] = {}
        if series_ticker:
            params["series_ticker"] = series_ticker.upper()
        if status:
            params["status"] = status
        if limit:
            params["limit"] = int(limit)
        return self._request("GET", "/markets", params=params)
    
    def get_market_orderbook(self, ticker: str) -> Dict[str, Any]:
        """
        Retrieve current orderbook for a market.
        
        Args:
            ticker: Market identifier
            
        Returns:
            JSON response with yes/no bid ladders
        """
        return self._request("GET", f"/markets/{ticker.upper()}/orderbook")
    
    def iter_trades(
        self,
        ticker: Optional[str] = None,
        min_ts: Optional[int] = None,
        max_ts: Optional[int] = None,
        page_limit: int = 1000,
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Iterate over paginated trades from GET /markets/trades.
        
        Args:
            ticker: Optional market filter
            min_ts: Start timestamp (Unix milliseconds)
            max_ts: End timestamp (Unix milliseconds)
            page_limit: Results per page (max 1000)
            
        Yields:
            Individual trade dictionaries
        """
        params: Dict[str, Any] = {"limit": page_limit}
        if ticker:
            params["ticker"] = ticker.upper()
        if min_ts:
            params["min_ts"] = int(min_ts)
        if max_ts:
            params["max_ts"] = int(max_ts)
        cursor: Optional[str] = None
        while True:
            if cursor:
                params["cursor"] = cursor
            payload = self._request("GET", "/markets/trades", params=params)
            trades = payload.get("trades", [])
            if not isinstance(trades, list):
                raise KalshiApiError("Expected list of trades")
            for trade in trades:
                yield trade
            cursor = payload.get("cursor")
            if not cursor:
                break
    
    def summarize_markets(
        self, series_ticker: str, status: str = "open"
    ) -> List[MarketSummary]:
        """
        Fetch and condense markets for display.
        
        Args:
            series_ticker: Series to query
            status: Market status filter
            
        Returns:
            List of MarketSummary objects
        """
        markets_payload = self.get_markets(
            series_ticker=series_ticker, status=status
        )
        markets_raw = markets_payload.get("markets", [])
        if not isinstance(markets_raw, list):
            raise KalshiApiError("Malformed market payload")
        summaries: List[MarketSummary] = []
        for market in markets_raw:
            summaries.append(
                MarketSummary(
                    ticker=market.get("ticker", ""),
                    title=market.get("title", ""),
                    event_ticker=market.get("event_ticker", ""),
                    yes_price=market.get("yes_price"),
                    volume=market.get("volume"),
                )
            )
        return summaries
    
    def _request(
        self, method: str, path: str, params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Execute HTTP request with error handling.
        
        Args:
            method: HTTP verb
            path: API path relative to base_url
            params: Optional query parameters
            
        Returns:
            Parsed JSON response as dictionary
            
        Raises:
            KalshiApiError: If response is not a JSON object
        """
        url = f"{self.base_url}{path}"
        response = self.session.request(
            method=method,
            url=url,
            params=params,
            timeout=self.timeout,
        )
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, dict):
            raise KalshiApiError(
                f"Unexpected payload for {path}: {data!r}"
            )
        return data

__all__ = [
    "PublicKalshiClient", "KalshiApiError",
    "MarketSummary", "API_BASE_URL"
]
