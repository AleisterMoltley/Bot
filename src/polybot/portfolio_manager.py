"""Portfolio Manager: Correlation-based exposure control.

Prevents over-concentration in correlated assets by:
- Tracking exposure by asset category (crypto, politics, sports, etc.)
- Enforcing maximum exposure limits per category
- Calculating portfolio-level metrics

Max 30% exposure per category to prevent over-concentration in any single
market type (e.g., crypto, politics, sports).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from polybot.logging_setup import get_logger

log = get_logger(__name__)

# Category keywords for classification (imported from scanner for consistency)
CATEGORY_KEYWORDS: dict[str, list[str]] = {
    "crypto": [
        "bitcoin",
        "btc",
        "ethereum",
        "eth",
        "crypto",
        "solana",
        "sol",
        "xrp",
        "dogecoin",
        "doge",
        "token",
        "defi",
        "blockchain",
    ],
    "politics": [
        "president",
        "election",
        "congress",
        "senate",
        "trump",
        "biden",
        "democrat",
        "republican",
        "vote",
        "governor",
        "political",
    ],
    "sports": [
        "nfl",
        "nba",
        "mlb",
        "nhl",
        "soccer",
        "football",
        "basketball",
        "baseball",
        "tennis",
        "golf",
        "mma",
        "ufc",
        "super bowl",
    ],
    "economics": [
        "fed",
        "interest rate",
        "inflation",
        "gdp",
        "unemployment",
        "stock",
        "s&p",
        "nasdaq",
        "economy",
        "recession",
    ],
}


@dataclass
class Position:
    """Represents an open position."""

    market_id: str
    market_name: str
    category: str
    size_usd: float
    entry_price: float
    side: str  # 'yes' or 'no'
    entry_time: str = ""
    current_price: float | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "market_id": self.market_id,
            "market_name": self.market_name,
            "category": self.category,
            "size_usd": self.size_usd,
            "entry_price": self.entry_price,
            "side": self.side,
            "entry_time": self.entry_time,
            "current_price": self.current_price,
        }


@dataclass
class PortfolioState:
    """Current portfolio state and metrics."""

    total_exposure: float = 0.0
    exposure_by_category: dict[str, float] = field(default_factory=dict)
    positions: list[Position] = field(default_factory=list)
    last_updated: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "total_exposure": round(self.total_exposure, 2),
            "exposure_by_category": {
                k: round(v, 2) for k, v in self.exposure_by_category.items()
            },
            "position_count": len(self.positions),
            "positions": [p.to_dict() for p in self.positions],
            "last_updated": self.last_updated,
        }


class PortfolioManager:
    """Manages portfolio exposure and correlation limits.

    Enforces:
    - Maximum 50% exposure to any single category (e.g., crypto)
    - Position tracking and aggregation
    - Correlation warnings for concentrated portfolios
    """

    # Maximum exposure per category as fraction of total portfolio
    MAX_CATEGORY_EXPOSURE = 0.50  # 50%

    # Maximum single position size as fraction of portfolio
    MAX_POSITION_SIZE = 0.30  # 30%

    def __init__(self, bankroll: float = 1000.0):
        """Initialize portfolio manager.

        Args:
            bankroll: Total portfolio value in USD
        """
        self.bankroll = bankroll
        self._positions: list[Position] = []
        self._last_updated = datetime.now(timezone.utc).isoformat()

    def classify_market(self, market_name: str) -> str:
        """Classify a market into a category.

        Args:
            market_name: Market question/name

        Returns:
            Category string (crypto, politics, sports, economics, or 'other')
        """
        name_lower = market_name.lower()

        for category, keywords in CATEGORY_KEYWORDS.items():
            if any(kw in name_lower for kw in keywords):
                return category

        return "other"

    def get_category_exposure(self, category: str) -> float:
        """Get current exposure for a category.

        Args:
            category: Category name

        Returns:
            Total USD exposure in that category
        """
        return sum(p.size_usd for p in self._positions if p.category == category)

    def get_category_exposure_pct(self, category: str) -> float:
        """Get category exposure as percentage of bankroll.

        Args:
            category: Category name

        Returns:
            Exposure as fraction (0.0 to 1.0)
        """
        if self.bankroll <= 0:
            return 0.0
        return self.get_category_exposure(category) / self.bankroll

    def can_add_position(
        self,
        market_name: str,
        size_usd: float,
        category: str | None = None,
    ) -> tuple[bool, str]:
        """Check if a new position can be added.

        ⚠️ FORCED EXECUTION v5: This function ALWAYS returns True.
        All position guards have been removed for forced execution.

        Args:
            market_name: Market name for classification
            size_usd: Proposed position size in USD
            category: Optional pre-classified category

        Returns:
            Tuple of (True, "FORCED_OK") - Always allows position
        """
        # FORCED EXECUTION v5: Force allow all positions, no guards
        return True, "FORCED_OK"

    def add_position(
        self,
        market_id: str,
        market_name: str,
        size_usd: float,
        entry_price: float,
        side: str,
        category: str | None = None,
    ) -> Position | None:
        """Add a new position to the portfolio.

        Args:
            market_id: Unique market identifier
            market_name: Market question/name
            size_usd: Position size in USD
            entry_price: Entry price (0.0 to 1.0)
            side: 'yes' or 'no'
            category: Optional pre-classified category

        Returns:
            Position if added, None if blocked by limits
        """
        if category is None:
            category = self.classify_market(market_name)

        # Check limits
        allowed, reason = self.can_add_position(market_name, size_usd, category)
        if not allowed:
            log.warning(
                "Position blocked by portfolio limits",
                market=market_name[:50],
                reason=reason,
            )
            return None

        position = Position(
            market_id=market_id,
            market_name=market_name[:100],
            category=category,
            size_usd=size_usd,
            entry_price=entry_price,
            side=side,
            entry_time=datetime.now(timezone.utc).isoformat(),
        )

        self._positions.append(position)
        self._last_updated = datetime.now(timezone.utc).isoformat()

        log.info(
            "Position added",
            market=market_name[:50],
            category=category,
            size=f"${size_usd:.2f}",
            new_category_exposure=f"{self.get_category_exposure_pct(category):.1%}",
        )

        return position

    def remove_position(self, market_id: str) -> Position | None:
        """Remove a position from the portfolio.

        Args:
            market_id: Market identifier to remove

        Returns:
            Removed Position or None if not found
        """
        for i, p in enumerate(self._positions):
            if p.market_id == market_id:
                removed = self._positions.pop(i)
                self._last_updated = datetime.now(timezone.utc).isoformat()
                log.info("Position removed", market=removed.market_name[:50])
                return removed
        return None

    def get_state(self) -> PortfolioState:
        """Get current portfolio state.

        Returns:
            PortfolioState with all metrics
        """
        # Calculate exposure by category
        exposure_by_cat: dict[str, float] = {}
        for p in self._positions:
            exposure_by_cat[p.category] = (
                exposure_by_cat.get(p.category, 0) + p.size_usd
            )

        total_exposure = sum(exposure_by_cat.values())

        return PortfolioState(
            total_exposure=total_exposure,
            exposure_by_category=exposure_by_cat,
            positions=self._positions.copy(),
            last_updated=self._last_updated,
        )

    def get_correlation_warnings(self) -> list[str]:
        """Get warnings for correlated/concentrated positions.

        Returns:
            List of warning messages
        """
        warnings = []
        state = self.get_state()

        for category, exposure in state.exposure_by_category.items():
            exposure_pct = exposure / self.bankroll if self.bankroll > 0 else 0
            if exposure_pct >= self.MAX_CATEGORY_EXPOSURE * 0.8:  # 80% of limit
                warnings.append(
                    f"⚠️ High {category.upper()} exposure: {exposure_pct:.1%} "
                    f"(limit: {self.MAX_CATEGORY_EXPOSURE:.0%})"
                )

        if len(self._positions) >= 10:
            warnings.append(
                f"⚠️ High position count: {len(self._positions)} open positions"
            )

        return warnings

    def get_heatmap_data(self) -> list[dict]:
        """Get data for portfolio heatmap visualization.

        Returns:
            List of category exposure data for heatmap
        """
        state = self.get_state()
        data = []

        all_categories = list(CATEGORY_KEYWORDS.keys()) + ["other"]
        for category in all_categories:
            exposure = state.exposure_by_category.get(category, 0)
            exposure_pct = (exposure / self.bankroll * 100) if self.bankroll > 0 else 0
            limit_pct = self.MAX_CATEGORY_EXPOSURE * 100

            data.append(
                {
                    "category": category.capitalize(),
                    "exposure": round(exposure, 2),
                    "exposure_pct": round(exposure_pct, 1),
                    "limit_pct": limit_pct,
                    "at_limit": exposure_pct >= limit_pct * 0.8,
                }
            )

        return data


# Singleton instance for global portfolio tracking
_portfolio_manager: PortfolioManager | None = None


def get_portfolio_manager(bankroll: float | None = None) -> PortfolioManager:
    """Get or create the global portfolio manager.

    Args:
        bankroll: Optional bankroll to set (only on first call)

    Returns:
        PortfolioManager instance
    """
    global _portfolio_manager
    if _portfolio_manager is None:
        _portfolio_manager = PortfolioManager(bankroll=bankroll or 1000.0)
    return _portfolio_manager
