"""
Order book utilities used by the ticker plant.

Provides in-memory order book reconstruction with BBO calculation for
binary prediction markets where YES orders act as bids and NO orders
act as asks.
"""
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Iterator, List, Optional, Tuple
from sortedcontainers import SortedDict

@dataclass
class BBO:
    """Best bid and offer snapshot with derived market statistics."""
    ticker: str
    bid_price: Optional[float]
    bid_size: Optional[int]
    ask_price: Optional[float]
    ask_size: Optional[int]
    mid_price: Optional[float]
    spread: Optional[float]
    timestamp: int

class OrderBook:
    """
    In-memory order book maintaining YES/NO price ladders.
    
    Binary prediction markets map YES positions to bids (highest price wins)
    and NO positions to asks (lowest price wins). Updates are applied via
    sequenced delta messages to maintain consistency.
    
    Attributes:
        ticker: Market identifier
        bids: YES orders sorted by descending price
        asks: NO orders sorted by ascending price
        sequence: Last processed sequence number
        last_update: Timestamp of most recent update in milliseconds
    """
    def __init__(self, ticker: str) -> None:
        self.ticker = ticker
        self.bids: SortedDict[float, int] = SortedDict()
        self.asks: SortedDict[float, int] = SortedDict()
        self.sequence: int = 0
        self.last_update: int = 0
    
    def apply_delta(
        self,
        side: str,
        price: float,
        delta_size: int,
        sequence: Optional[int] = None,
        timestamp_ms: Optional[int] = None,
    ) -> None:
        """
        Apply size change to order book at specified price level.
        
        Args:
            side: "yes" for bids, "no" for asks
            price: Price level to modify
            delta_size: Signed size change (positive=add, negative=remove)
            sequence: Optional sequence number for gap detection
            timestamp_ms: Optional explicit timestamp, defaults to now
        """
        book = self.bids if side == "yes" else self.asks
        current_size = book.get(price, 0)
        new_size = current_size + delta_size
        if new_size <= 0:
            book.pop(price, None)
        else:
            book[price] = new_size
        if sequence is not None:
            self.sequence = max(self.sequence, sequence)
        self.last_update = timestamp_ms or int(
            datetime.now(tz=timezone.utc).timestamp() * 1000
        )
    
    def get_bbo(self) -> BBO:
        """
        Calculate best bid and offer with derived statistics.
        
        Returns:
            BBO snapshot containing top of book and computed mid/spread
        """
        bid_price, bid_size = self._best_bid()
        ask_price, ask_size = self._best_ask()
        mid = None
        spread = None
        if bid_price is not None and ask_price is not None:
            mid = round((bid_price + ask_price) / 2, 4)
            spread = round(ask_price - bid_price, 4)
        return BBO(
            ticker=self.ticker,
            bid_price=bid_price,
            bid_size=bid_size,
            ask_price=ask_price,
            ask_size=ask_size,
            mid_price=mid,
            spread=spread,
            timestamp=self.last_update,
        )
    
    def get_depth(self, levels: int = 5) -> Dict[str, List[Tuple[float, int]]]:
        """
        Retrieve multi-level market depth.
        
        Args:
            levels: Number of price levels to return per side
            
        Returns:
            Dictionary with "yes" and "no" keys mapping to (price, size) tuples
        """
        yes_levels = list(
            self._iter_levels(self.bids, reverse=True, limit=levels)
        )
        no_levels = list(
            self._iter_levels(self.asks, reverse=False, limit=levels)
        )
        return {"yes": yes_levels, "no": no_levels}
    
    def is_crossed(self) -> bool:
        """
        Detect if market is crossed (bid >= ask).
        
        Returns:
            True if best bid meets or exceeds best ask, indicating
            arbitrage opportunity or stale data
        """
        bid_price, _ = self._best_bid()
        ask_price, _ = self._best_ask()
        if bid_price is None or ask_price is None:
            return False
        return bid_price >= ask_price
    
    def _best_bid(self) -> Tuple[Optional[float], Optional[int]]:
        """Extract highest YES price (last item in sorted bids)."""
        if not self.bids:
            return None, None
        return self.bids.peekitem(-1)
    
    def _best_ask(self) -> Tuple[Optional[float], Optional[int]]:
        """Extract lowest NO price (first item in sorted asks)."""
        if not self.asks:
            return None, None
        return self.asks.peekitem(0)
    
    @staticmethod
    def _iter_levels(
        book: SortedDict, reverse: bool, limit: int
    ) -> Iterator[Tuple[float, int]]:
        """
        Yield price levels up to specified limit.
        
        Args:
            book: Sorted dictionary of price -> size mappings
            reverse: If True, iterate from high to low prices
            limit: Maximum number of levels to yield
            
        Yields:
            (price, size) tuples in requested order
        """
        items = list(book.items())
        if reverse:
            items = reversed(items)
        for idx, (price, size) in enumerate(items):
            if idx >= limit:
                break
            yield price, size
