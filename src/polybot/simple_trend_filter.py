"""Simple Trend Filter module for Up/Down crypto scanning.

Provides statistical functions for filtering markets based on
trend strength and volatility. Only strong + calm trends pass.

Functions:
- linear_regression_slope: Calculate slope of price movement
- std_dev: Calculate standard deviation for volatility

Thresholds:
- Slope: abs(slope) >= 0.028 required (strong trend)
- Volatility: vol <= 0.085 required (calm market)
"""

from __future__ import annotations


def linear_regression_slope(values: list[float]) -> float:
    """Calculate the linear regression slope of a list of values.

    Uses least squares method to find the best-fit line slope.
    The slope indicates trend direction and strength:
    - Positive slope = upward trend
    - Negative slope = downward trend
    - Higher absolute value = stronger trend

    Args:
        values: List of price values (minimum 2 required)

    Returns:
        Slope of the regression line. Returns 0.0 if insufficient data.
    """
    n = len(values)
    if n < 2:
        return 0.0

    sum_x = sum_y = sum_xy = sum_xx = 0.0
    for i in range(n):
        sum_x += i
        sum_y += values[i]
        sum_xy += i * values[i]
        sum_xx += i * i

    denom = n * sum_xx - sum_x * sum_x
    if denom == 0:
        return 0.0

    return (n * sum_xy - sum_x * sum_y) / denom


def std_dev(values: list[float]) -> float:
    """Calculate the standard deviation of a list of values.

    Uses population standard deviation formula (divide by n, not n-1).
    Lower values indicate calmer, more predictable markets.

    Args:
        values: List of price values (minimum 2 required)

    Returns:
        Standard deviation. Returns 0.0 if insufficient data.
    """
    if len(values) < 2:
        return 0.0

    mean = sum(values) / len(values)
    squared_diffs = [(v - mean) ** 2 for v in values]
    return (sum(squared_diffs) / len(values)) ** 0.5


# Default thresholds for trend filter
MIN_SLOPE_THRESHOLD = 0.028  # Minimum absolute slope for strong trend
MAX_VOLATILITY_THRESHOLD = 0.085  # Maximum volatility for calm market

# Global state for trend filter
_trend_filter_enabled = False


def is_trend_filter_enabled() -> bool:
    """Check if Simple Trend Filter is enabled."""
    return _trend_filter_enabled


def enable_trend_filter() -> None:
    """Enable Simple Trend Filter."""
    global _trend_filter_enabled
    _trend_filter_enabled = True


def disable_trend_filter() -> None:
    """Disable Simple Trend Filter."""
    global _trend_filter_enabled
    _trend_filter_enabled = False


def passes_trend_filter(
    prices: list[float],
    min_slope: float = MIN_SLOPE_THRESHOLD,
    max_volatility: float = MAX_VOLATILITY_THRESHOLD,
) -> tuple[bool, float, float]:
    """Check if prices pass the Simple Trend Filter.

    A market passes if:
    1. Absolute slope >= min_slope (strong trend)
    2. Volatility <= max_volatility (calm market)

    Args:
        prices: List of recent prices (e.g., last 10 prices)
        min_slope: Minimum absolute slope threshold (default: 0.028)
        max_volatility: Maximum volatility threshold (default: 0.085)

    Returns:
        Tuple of (passes, slope, volatility)
    """
    if len(prices) < 2:
        return False, 0.0, 0.0

    slope = linear_regression_slope(prices)
    vol = std_dev(prices)

    passes = abs(slope) >= min_slope and vol <= max_volatility
    return passes, slope, vol


def get_trend_filter_status() -> dict:
    """Get the current status of the Simple Trend Filter.

    Returns:
        Dict with enabled status and threshold values
    """
    return {
        "enabled": _trend_filter_enabled,
        "min_slope_threshold": MIN_SLOPE_THRESHOLD,
        "max_volatility_threshold": MAX_VOLATILITY_THRESHOLD,
        "description": "Only strong (slope >= 0.028) + calm (vol <= 0.085) trends pass",
    }
