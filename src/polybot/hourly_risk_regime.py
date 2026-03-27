"""Hourly Risk Regime: Time-of-day based position sizing adjustments.

Adjusts risk multipliers based on historical P&L patterns by hour of day.
Uses Berlin time (CET/CEST) for all calculations, mapped from original
Eastern Time (ET) analysis.

Risk Levels:
- Grey (0.0): Bot inactive - no trading during these hours
- Red (0.3): Conservative - reduced position sizing
- Green (1.6-1.8): Aggressive - larger position sizes in high-performance hours

The RISK_MAP is keyed by Berlin hour (0-23) and maps to risk multipliers.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

import pytz

from polybot.logging_setup import get_logger

log = get_logger(__name__)

# Berlin timezone for all calculations
BERLIN_TZ = pytz.timezone("Europe/Berlin")

# Risk multiplier map by Berlin hour (0-23)
# Based on ET → Berlin shift (+6 hours) and historical PnL patterns
# Grey = 0.0 (inactive), Red = 0.3 (conservative), Green = 1.6-1.8 (aggressive)
RISK_MAP: dict[int, float] = {
    0: 1.6,  # 6p ET → 0a Berlin  (grün/green)
    1: 1.6,  # 7p ET → 1a Berlin  (grün/green)
    2: 1.6,  # 8p ET → 2a Berlin  (grün/green)
    3: 0.3,  # 9p ET → 3a Berlin  (rot/red)
    4: 1.6,  # 10p ET → 4a Berlin (grün/green)
    5: 1.6,  # 11p ET → 5a Berlin (grün/green)
    6: 0.0,  # 12a ET → 6a Berlin (grau/inactive)
    7: 0.0,  # 1a ET → 7a Berlin  (grau/inactive)
    8: 1.6,  # 2a ET → 8a Berlin  (grün/green)
    9: 1.8,  # 3a ET → 9a Berlin  (grün/green - best hour!)
    10: 1.6,  # 4a ET → 10a Berlin (grün/green)
    11: 0.3,  # 5a ET → 11a Berlin (rot/red)
    12: 0.3,  # 6a ET → 12p Berlin (rot/red)
    13: 0.3,  # 7a ET → 13p Berlin (rot/red)
    14: 1.6,  # 8a ET → 14p Berlin (grün/green)
    15: 1.8,  # 9a ET → 15p Berlin (grün/green - best hour!)
    16: 1.6,  # 10a ET → 16p Berlin (grün/green)
    17: 0.3,  # 11a ET → 17p Berlin (rot/red)
    18: 0.0,  # 12p ET → 18p Berlin (grau/inactive)
    19: 0.0,  # 1p ET → 19p Berlin (grau/inactive)
    20: 1.6,  # 2p ET → 20p Berlin (grün/green)
    21: 1.6,  # 3p ET → 21p Berlin (grün/green)
    22: 1.6,  # 4p ET → 22p Berlin (grün/green)
    23: 0.3,  # 5p ET → 23p Berlin (rot/red)
}

# Classification labels for display
RISK_LABELS: dict[float, str] = {
    0.0: "inactive",
    0.3: "conservative",
    1.6: "aggressive",
    1.8: "aggressive",
}

# Colors for dashboard display (CSS color values)
RISK_COLORS: dict[str, str] = {
    "inactive": "#6B7280",  # Grey (slate-500)
    "conservative": "#EF4444",  # Red
    "aggressive": "#22C55E",  # Green
}


@dataclass
class HourlyRiskState:
    """Current hourly risk regime state."""

    berlin_hour: int
    multiplier: float
    risk_level: str  # 'inactive', 'conservative', 'aggressive'
    color: str
    is_active: bool
    timestamp: str

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for API response."""
        return {
            "berlin_hour": self.berlin_hour,
            "multiplier": self.multiplier,
            "risk_level": self.risk_level,
            "color": self.color,
            "is_active": self.is_active,
            "timestamp": self.timestamp,
        }


class HourlyRiskRegime:
    """Time-of-day risk regime manager.

    Adjusts position sizing based on historical performance patterns
    by hour of day (Berlin time).
    """

    def __init__(self) -> None:
        """Initialize the hourly risk regime."""
        self._risk_map = RISK_MAP
        self._last_multiplier: float | None = None
        self._last_hour: int | None = None

    def get_berlin_hour(self) -> int:
        """Get current hour in Berlin timezone.

        Returns:
            Hour of day (0-23) in Berlin time
        """
        return datetime.now(BERLIN_TZ).hour

    def get_multiplier(self, hour: int | None = None) -> float:
        """Get risk multiplier for a specific hour.

        Args:
            hour: Hour of day (0-23) in Berlin time.
                  If None, uses current Berlin hour.

        Returns:
            Risk multiplier (0.0, 0.3, 1.6, or 1.8)
        """
        if hour is None:
            hour = self.get_berlin_hour()

        multiplier = self._risk_map.get(hour, 1.0)

        # Log changes
        if self._last_hour != hour or self._last_multiplier != multiplier:
            risk_level = RISK_LABELS.get(multiplier, "normal")
            log.info(
                "Hourly risk regime",
                berlin_hour=hour,
                multiplier=multiplier,
                level=risk_level,
            )
            self._last_hour = hour
            self._last_multiplier = multiplier

        return multiplier

    def is_active(self, hour: int | None = None) -> bool:
        """Check if trading is active for given hour.

        Args:
            hour: Hour of day (0-23) in Berlin time.
                  If None, uses current Berlin hour.

        Returns:
            True if multiplier > 0 (trading allowed)
        """
        return self.get_multiplier(hour) > 0.0

    def get_risk_level(self, hour: int | None = None) -> str:
        """Get human-readable risk level for an hour.

        Args:
            hour: Hour of day (0-23) in Berlin time.
                  If None, uses current Berlin hour.

        Returns:
            Risk level string: 'inactive', 'conservative', or 'aggressive'
        """
        multiplier = self.get_multiplier(hour)
        return RISK_LABELS.get(multiplier, "normal")

    def get_color(self, hour: int | None = None) -> str:
        """Get display color for an hour's risk level.

        Args:
            hour: Hour of day (0-23) in Berlin time.
                  If None, uses current Berlin hour.

        Returns:
            CSS color string (hex)
        """
        risk_level = self.get_risk_level(hour)
        return RISK_COLORS.get(risk_level, "#6B7280")

    def get_state(self) -> HourlyRiskState:
        """Get current hourly risk state.

        Returns:
            HourlyRiskState with all current metrics
        """
        berlin_hour = self.get_berlin_hour()
        multiplier = self.get_multiplier(berlin_hour)
        risk_level = self.get_risk_level(berlin_hour)

        return HourlyRiskState(
            berlin_hour=berlin_hour,
            multiplier=multiplier,
            risk_level=risk_level,
            color=self.get_color(berlin_hour),
            is_active=multiplier > 0.0,
            timestamp=datetime.now(BERLIN_TZ).isoformat(),
        )

    def get_heatmap_data(self) -> list[dict[str, Any]]:
        """Get 24-hour heatmap data for dashboard display.

        Returns:
            List of dicts with hour, multiplier, risk_level, and color
        """
        current_hour = self.get_berlin_hour()
        heatmap = []

        for hour in range(24):
            multiplier = self._risk_map.get(hour, 1.0)
            risk_level = RISK_LABELS.get(multiplier, "normal")

            heatmap.append(
                {
                    "hour": hour,
                    "hour_label": f"{hour:02d}:00",
                    "multiplier": multiplier,
                    "risk_level": risk_level,
                    "color": RISK_COLORS.get(risk_level, "#6B7280"),
                    "is_current": hour == current_hour,
                }
            )

        return heatmap


# Singleton instance
_hourly_risk_regime: HourlyRiskRegime | None = None


def get_hourly_risk_regime() -> HourlyRiskRegime:
    """Get or create the global hourly risk regime instance.

    Returns:
        HourlyRiskRegime singleton instance
    """
    global _hourly_risk_regime
    if _hourly_risk_regime is None:
        _hourly_risk_regime = HourlyRiskRegime()
    return _hourly_risk_regime


def get_hourly_multiplier() -> float:
    """Convenience function to get current hourly risk multiplier.

    Returns:
        Current risk multiplier based on Berlin time
    """
    return get_hourly_risk_regime().get_multiplier()


def is_trading_active() -> bool:
    """Convenience function to check if trading is active.

    Returns:
        True if trading is allowed at current Berlin hour
    """
    return get_hourly_risk_regime().is_active()
