"""Tests for the Simple Trend Filter module."""

import pytest

from polybot.simple_trend_filter import (
    linear_regression_slope,
    std_dev,
    passes_trend_filter,
    is_trend_filter_enabled,
    enable_trend_filter,
    disable_trend_filter,
    get_trend_filter_status,
    MIN_SLOPE_THRESHOLD,
    MAX_VOLATILITY_THRESHOLD,
)


class TestLinearRegressionSlope:
    """Test linear_regression_slope function."""

    def test_empty_list_returns_zero(self):
        """Empty list should return 0."""
        assert linear_regression_slope([]) == 0.0

    def test_single_value_returns_zero(self):
        """Single value should return 0 (need at least 2 points)."""
        assert linear_regression_slope([5.0]) == 0.0

    def test_two_equal_values_returns_zero(self):
        """Two equal values should have zero slope (flat line)."""
        result = linear_regression_slope([1.0, 1.0])
        assert result == 0.0

    def test_upward_trend(self):
        """Ascending values should have positive slope."""
        result = linear_regression_slope([1.0, 2.0, 3.0, 4.0, 5.0])
        assert result > 0
        assert pytest.approx(result, rel=0.01) == 1.0

    def test_downward_trend(self):
        """Descending values should have negative slope."""
        result = linear_regression_slope([5.0, 4.0, 3.0, 2.0, 1.0])
        assert result < 0
        assert pytest.approx(result, rel=0.01) == -1.0

    def test_noisy_uptrend(self):
        """Noisy upward trend should still have positive slope."""
        # General upward trend with noise
        prices = [1.0, 1.2, 1.1, 1.4, 1.3, 1.6, 1.5, 1.8, 1.7, 2.0]
        result = linear_regression_slope(prices)
        assert result > 0

    def test_flat_market(self):
        """Flat market should have near-zero slope."""
        prices = [1.0, 1.01, 0.99, 1.0, 1.02, 0.98, 1.0, 1.01, 0.99, 1.0]
        result = linear_regression_slope(prices)
        assert abs(result) < 0.01

    def test_strong_trend_above_threshold(self):
        """Strong trend should exceed MIN_SLOPE_THRESHOLD."""
        # 10% increase per step
        prices = [1.0, 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 1.9]
        result = linear_regression_slope(prices)
        assert abs(result) >= MIN_SLOPE_THRESHOLD


class TestStdDev:
    """Test std_dev function."""

    def test_empty_list_returns_zero(self):
        """Empty list should return 0."""
        assert std_dev([]) == 0.0

    def test_single_value_returns_zero(self):
        """Single value should return 0."""
        assert std_dev([5.0]) == 0.0

    def test_identical_values_returns_zero(self):
        """Identical values should have zero std dev."""
        result = std_dev([5.0, 5.0, 5.0, 5.0, 5.0])
        assert result == 0.0

    def test_simple_variance(self):
        """Test with known values."""
        # Values: 2, 4, 4, 4, 5, 5, 7, 9
        # Mean = 5
        # Variance = ((2-5)^2 + (4-5)^2*3 + (5-5)^2*2 + (7-5)^2 + (9-5)^2) / 8
        # Variance = (9 + 3 + 0 + 4 + 16) / 8 = 32/8 = 4
        # Std Dev = 2
        values = [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0]
        result = std_dev(values)
        assert pytest.approx(result, rel=0.01) == 2.0

    def test_low_volatility(self):
        """Low volatility values should be below threshold."""
        # Small variations around 1.0
        prices = [1.0, 1.01, 0.99, 1.0, 1.01, 0.99, 1.0, 1.01, 0.99, 1.0]
        result = std_dev(prices)
        assert result < MAX_VOLATILITY_THRESHOLD

    def test_high_volatility(self):
        """High volatility values should exceed threshold."""
        # Large swings
        prices = [1.0, 1.2, 0.8, 1.3, 0.7, 1.4, 0.6, 1.5, 0.5, 1.6]
        result = std_dev(prices)
        assert result > MAX_VOLATILITY_THRESHOLD


class TestPassesTrendFilter:
    """Test passes_trend_filter function."""

    def test_insufficient_data_fails(self):
        """Insufficient data should fail."""
        passes, slope, vol = passes_trend_filter([])
        assert passes is False
        assert slope == 0.0
        assert vol == 0.0

        passes, slope, vol = passes_trend_filter([1.0])
        assert passes is False

    def test_strong_calm_trend_passes(self):
        """Strong trend with low volatility should pass."""
        # Clear upward trend with low noise
        # Prices with 0.0289 slope and 0.0831 volatility - just within thresholds
        prices = [0.50, 0.529, 0.558, 0.587, 0.616, 0.645, 0.674, 0.703, 0.732, 0.760]
        passes, slope, vol = passes_trend_filter(prices)
        assert passes is True
        assert slope >= MIN_SLOPE_THRESHOLD
        assert vol <= MAX_VOLATILITY_THRESHOLD

    def test_weak_trend_fails(self):
        """Weak trend (low slope) should fail."""
        # Very flat market
        prices = [1.0, 1.001, 1.002, 1.001, 1.002, 1.001, 1.002, 1.001, 1.002, 1.001]
        passes, slope, vol = passes_trend_filter(prices)
        assert passes is False
        assert abs(slope) < MIN_SLOPE_THRESHOLD

    def test_volatile_market_fails(self):
        """High volatility market should fail even with trend."""
        # Strong trend but very noisy
        prices = [1.0, 1.2, 0.9, 1.3, 0.8, 1.4, 0.7, 1.5, 0.6, 1.6]
        passes, slope, vol = passes_trend_filter(prices)
        assert passes is False
        assert vol > MAX_VOLATILITY_THRESHOLD

    def test_custom_thresholds(self):
        """Custom thresholds should be respected."""
        prices = [1.0, 1.02, 1.04, 1.06, 1.08, 1.10, 1.12, 1.14, 1.16, 1.18]

        # With relaxed thresholds
        passes, slope, vol = passes_trend_filter(
            prices, min_slope=0.01, max_volatility=0.1
        )
        assert passes is True

        # With strict thresholds
        passes, slope, vol = passes_trend_filter(
            prices, min_slope=0.1, max_volatility=0.001
        )
        assert passes is False


class TestTrendFilterState:
    """Test global trend filter state functions."""

    def test_initial_state_disabled(self):
        """Trend filter should be disabled initially."""
        # Reset state first
        disable_trend_filter()
        assert is_trend_filter_enabled() is False

    def test_enable_filter(self):
        """Enable should set state to True."""
        disable_trend_filter()  # Reset
        enable_trend_filter()
        assert is_trend_filter_enabled() is True

    def test_disable_filter(self):
        """Disable should set state to False."""
        enable_trend_filter()
        disable_trend_filter()
        assert is_trend_filter_enabled() is False

    def test_get_status(self):
        """get_trend_filter_status should return correct data."""
        enable_trend_filter()
        status = get_trend_filter_status()

        assert status["enabled"] is True
        assert status["min_slope_threshold"] == MIN_SLOPE_THRESHOLD
        assert status["max_volatility_threshold"] == MAX_VOLATILITY_THRESHOLD
        assert "description" in status

        disable_trend_filter()
        status = get_trend_filter_status()
        assert status["enabled"] is False

    def test_toggle_multiple_times(self):
        """State should toggle correctly multiple times."""
        disable_trend_filter()
        assert is_trend_filter_enabled() is False

        enable_trend_filter()
        assert is_trend_filter_enabled() is True

        enable_trend_filter()  # Enable again
        assert is_trend_filter_enabled() is True

        disable_trend_filter()
        assert is_trend_filter_enabled() is False

        disable_trend_filter()  # Disable again
        assert is_trend_filter_enabled() is False


class TestThresholdValues:
    """Test threshold constants."""

    def test_slope_threshold_value(self):
        """MIN_SLOPE_THRESHOLD should be 0.028."""
        assert MIN_SLOPE_THRESHOLD == 0.028

    def test_volatility_threshold_value(self):
        """MAX_VOLATILITY_THRESHOLD should be 0.085."""
        assert MAX_VOLATILITY_THRESHOLD == 0.085
