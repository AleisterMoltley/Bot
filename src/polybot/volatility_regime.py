"""Volatility Regime Detection: Adjusts position sizing based on market volatility.

Detects whether the market is in a low, normal, or high volatility regime
and adjusts Kelly fraction accordingly to reduce risk in volatile periods.

Volatility Adjustments:
- Low volatility: 1.2x Kelly (more aggressive)
- Normal volatility: 1.0x Kelly (standard)
- High volatility: 0.6x Kelly (conservative)
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from polybot.logging_setup import get_logger

log = get_logger(__name__)


class VolatilityRegime(Enum):
    """Market volatility regime classification."""

    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"


@dataclass
class VolatilityState:
    """Current volatility regime state."""

    regime: VolatilityRegime
    volatility_score: float  # 0-100 scale
    kelly_multiplier: float  # Adjustment factor for Kelly sizing
    last_updated: str
    data_points: int = 0
    high_vol_threshold: float = 70.0
    low_vol_threshold: float = 30.0

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "regime": self.regime.value,
            "volatility_score": round(self.volatility_score, 1),
            "kelly_multiplier": round(self.kelly_multiplier, 2),
            "last_updated": self.last_updated,
            "data_points": self.data_points,
        }


class VolatilityRegimeDetector:
    """Detects market volatility regime for position sizing adjustments.

    Uses market price movements and spread data to classify the current
    volatility environment and provide Kelly fraction adjustments.
    """

    # Kelly multipliers for each regime
    MULTIPLIER_LOW_VOL = 1.2  # More aggressive in calm markets
    MULTIPLIER_NORMAL = 1.0  # Standard sizing
    MULTIPLIER_HIGH_VOL = 0.6  # Conservative in volatile markets

    # Volatility thresholds (0-100 scale)
    HIGH_VOL_THRESHOLD = 70.0
    LOW_VOL_THRESHOLD = 30.0

    # Rolling window size for volatility calculation
    WINDOW_SIZE = 20

    def __init__(self):
        """Initialize the volatility detector."""
        self._price_history: list[float] = []
        self._spread_history: list[float] = []
        self._last_state: VolatilityState | None = None

    def add_price_data(self, price: float, spread: float | None = None) -> None:
        """Add a new price observation.

        Args:
            price: Current market price (0.0 to 1.0 for prediction markets)
            spread: Optional bid-ask spread
        """
        self._price_history.append(price)
        if spread is not None:
            self._spread_history.append(spread)

        # Keep rolling window
        if len(self._price_history) > self.WINDOW_SIZE * 2:
            self._price_history = self._price_history[-self.WINDOW_SIZE * 2 :]
        if len(self._spread_history) > self.WINDOW_SIZE * 2:
            self._spread_history = self._spread_history[-self.WINDOW_SIZE * 2 :]

    def calculate_volatility_score(self) -> float:
        """Calculate current volatility score (0-100 scale).

        Uses:
        - Price standard deviation
        - Average spread (if available)
        - Recent price range

        Returns:
            Volatility score from 0 (calm) to 100 (extreme)
        """
        if len(self._price_history) < 5:
            return 50.0  # Default to normal if insufficient data

        prices = self._price_history[-self.WINDOW_SIZE :]

        # Calculate price volatility
        mean_price = sum(prices) / len(prices)
        variance = sum((p - mean_price) ** 2 for p in prices) / len(prices)
        std_dev = variance**0.5

        # For prediction markets (0-1 range), normalize std dev
        # Max possible std dev for binary is 0.5
        price_vol_score = min(100, (std_dev / 0.2) * 100)

        # Calculate price range score
        price_range = max(prices) - min(prices)
        range_score = min(100, (price_range / 0.4) * 100)

        # Add spread component if available
        if self._spread_history:
            spreads = self._spread_history[-self.WINDOW_SIZE :]
            avg_spread = sum(spreads) / len(spreads)
            # Wider spreads indicate higher volatility
            spread_score = min(100, (avg_spread / 0.05) * 100)
            # Weighted average: 50% price vol, 30% range, 20% spread
            vol_score = price_vol_score * 0.5 + range_score * 0.3 + spread_score * 0.2
        else:
            # Without spread: 60% price vol, 40% range
            vol_score = price_vol_score * 0.6 + range_score * 0.4

        return max(0, min(100, vol_score))

    def get_regime(self, vol_score: float | None = None) -> VolatilityRegime:
        """Determine the current volatility regime.

        Args:
            vol_score: Optional pre-calculated volatility score

        Returns:
            VolatilityRegime enum value
        """
        if vol_score is None:
            vol_score = self.calculate_volatility_score()

        if vol_score >= self.HIGH_VOL_THRESHOLD:
            return VolatilityRegime.HIGH
        elif vol_score <= self.LOW_VOL_THRESHOLD:
            return VolatilityRegime.LOW
        else:
            return VolatilityRegime.NORMAL

    def get_kelly_multiplier(self, regime: VolatilityRegime | None = None) -> float:
        """Get the Kelly fraction multiplier for current regime.

        Args:
            regime: Optional pre-determined regime

        Returns:
            Multiplier for Kelly fraction (0.6 to 1.2)
        """
        if regime is None:
            regime = self.get_regime()

        if regime == VolatilityRegime.LOW:
            return self.MULTIPLIER_LOW_VOL
        elif regime == VolatilityRegime.HIGH:
            return self.MULTIPLIER_HIGH_VOL
        else:
            return self.MULTIPLIER_NORMAL

    def get_state(self) -> VolatilityState:
        """Get the current volatility state.

        Returns:
            VolatilityState with all metrics
        """
        vol_score = self.calculate_volatility_score()
        regime = self.get_regime(vol_score)
        multiplier = self.get_kelly_multiplier(regime)

        state = VolatilityState(
            regime=regime,
            volatility_score=vol_score,
            kelly_multiplier=multiplier,
            last_updated=datetime.now(timezone.utc).isoformat(),
            data_points=len(self._price_history),
            high_vol_threshold=self.HIGH_VOL_THRESHOLD,
            low_vol_threshold=self.LOW_VOL_THRESHOLD,
        )

        self._last_state = state
        return state

    def adjust_kelly_fraction(self, base_kelly: float) -> float:
        """Adjust a Kelly fraction based on current volatility.

        Args:
            base_kelly: The base Kelly fraction (e.g., 0.10)

        Returns:
            Adjusted Kelly fraction
        """
        multiplier = self.get_kelly_multiplier()
        adjusted = base_kelly * multiplier

        log.debug(
            "Kelly adjustment",
            base=f"{base_kelly:.3f}",
            multiplier=f"{multiplier:.2f}",
            adjusted=f"{adjusted:.3f}",
            regime=self.get_regime().value,
        )

        return adjusted


# Singleton instance
_volatility_detector: VolatilityRegimeDetector | None = None


def get_volatility_detector() -> VolatilityRegimeDetector:
    """Get or create the global volatility detector.

    Returns:
        VolatilityRegimeDetector instance
    """
    global _volatility_detector
    if _volatility_detector is None:
        _volatility_detector = VolatilityRegimeDetector()
    return _volatility_detector


def get_volatility_adjusted_kelly(base_kelly: float) -> float:
    """Convenience function to get volatility-adjusted Kelly fraction.

    Args:
        base_kelly: Base Kelly fraction

    Returns:
        Adjusted Kelly fraction based on current volatility regime
    """
    detector = get_volatility_detector()
    return detector.adjust_kelly_fraction(base_kelly)
