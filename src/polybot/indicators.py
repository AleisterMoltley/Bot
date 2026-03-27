"""Statistical indicator functions for trading analysis.

Provides numpy-powered statistical indicators for technical analysis.
Bugfixed and improved from old TypeScript implementation.

Functions:
    linear_regression_slope: Calculate trend direction and strength
    std_dev: Calculate population standard deviation for volatility

Example:
    >>> from polybot.indicators import linear_regression_slope, std_dev
    >>> prices = [1.0, 2.0, 3.0, 4.0, 5.0]
    >>> slope = linear_regression_slope(prices)
    >>> volatility = std_dev(prices)
"""

from __future__ import annotations

from typing import Sequence

import numpy as np

__all__ = ["linear_regression_slope", "std_dev"]


def linear_regression_slope(values: Sequence[float] | np.ndarray) -> float:
    """Calculate the linear regression slope of a sequence of values.

    Uses numpy's polyfit to find the best-fit line slope (x = index 0…n-1).
    The slope indicates trend direction and strength:

    - Positive slope = upward trend
    - Negative slope = downward trend
    - Higher absolute value = stronger trend

    Args:
        values: Sequence of numeric values (e.g., prices). Minimum 2 required.

    Returns:
        Slope of the regression line. Returns 0.0 if len(values) < 2.

    Examples:
        >>> linear_regression_slope([1.0, 2.0, 3.0, 4.0, 5.0])
        1.0
        >>> linear_regression_slope([5.0, 4.0, 3.0, 2.0, 1.0])
        -1.0
        >>> linear_regression_slope([1.0])
        0.0
    """
    arr = np.asarray(values, dtype=np.float64)
    if len(arr) < 2:
        return 0.0

    x = np.arange(len(arr))
    slope: float = np.polyfit(x, arr, 1)[0]
    return float(slope)


def std_dev(values: Sequence[float] | np.ndarray) -> float:
    """Calculate the population standard deviation (ddof=0) of values.

    Uses numpy's std function with ddof=0 (population standard deviation).
    Lower values indicate calmer, more predictable markets.

    Args:
        values: Sequence of numeric values (e.g., prices). Minimum 2 required.

    Returns:
        Population standard deviation. Returns 0.0 if len(values) < 2.

    Examples:
        >>> std_dev([2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0])
        2.0
        >>> std_dev([5.0, 5.0, 5.0, 5.0])
        0.0
        >>> std_dev([1.0])
        0.0
    """
    arr = np.asarray(values, dtype=np.float64)
    if len(arr) < 2:
        return 0.0

    result: float = np.std(arr, ddof=0)
    return float(result)
