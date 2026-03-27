"""Tests for the numpy-powered indicators module."""

import numpy as np
import pytest

from polybot.indicators import linear_regression_slope, std_dev


class TestLinearRegressionSlope:
    """Test linear_regression_slope function (numpy implementation)."""

    def test_empty_list_returns_zero(self):
        """Empty list should return 0."""
        assert linear_regression_slope([]) == 0.0

    def test_single_value_returns_zero(self):
        """Single value should return 0 (need at least 2 points)."""
        assert linear_regression_slope([5.0]) == 0.0

    def test_two_equal_values_returns_zero(self):
        """Two equal values should have zero slope (flat line)."""
        result = linear_regression_slope([1.0, 1.0])
        assert result == pytest.approx(0.0, abs=1e-10)

    def test_upward_trend(self):
        """Ascending values should have positive slope."""
        result = linear_regression_slope([1.0, 2.0, 3.0, 4.0, 5.0])
        assert result > 0
        assert result == pytest.approx(1.0, rel=0.01)

    def test_downward_trend(self):
        """Descending values should have negative slope."""
        result = linear_regression_slope([5.0, 4.0, 3.0, 2.0, 1.0])
        assert result < 0
        assert result == pytest.approx(-1.0, rel=0.01)

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

    def test_numpy_array_input(self):
        """Should accept numpy array input."""
        arr = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
        result = linear_regression_slope(arr)
        assert result == pytest.approx(1.0, rel=0.01)

    def test_consistent_with_numpy_polyfit(self):
        """Result should match direct numpy.polyfit calculation."""
        values = [1.5, 2.3, 3.1, 4.2, 5.0, 5.8, 7.1, 8.0]
        x = np.arange(len(values))
        expected = np.polyfit(x, values, 1)[0]
        result = linear_regression_slope(values)
        assert result == pytest.approx(expected, rel=1e-10)


class TestStdDev:
    """Test std_dev function (numpy implementation with ddof=0)."""

    def test_empty_list_returns_zero(self):
        """Empty list should return 0."""
        assert std_dev([]) == 0.0

    def test_single_value_returns_zero(self):
        """Single value should return 0."""
        assert std_dev([5.0]) == 0.0

    def test_identical_values_returns_zero(self):
        """Identical values should have zero std dev."""
        result = std_dev([5.0, 5.0, 5.0, 5.0, 5.0])
        assert result == pytest.approx(0.0, abs=1e-10)

    def test_simple_variance(self):
        """Test with known values.

        Values: 2, 4, 4, 4, 5, 5, 7, 9
        Mean = 5
        Variance = ((2-5)^2 + (4-5)^2*3 + (5-5)^2*2 + (7-5)^2 + (9-5)^2) / 8
        Variance = (9 + 3 + 0 + 4 + 16) / 8 = 32/8 = 4
        Std Dev = 2
        """
        values = [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0]
        result = std_dev(values)
        assert result == pytest.approx(2.0, rel=0.01)

    def test_low_volatility(self):
        """Low volatility values should have small std dev."""
        # Small variations around 1.0
        prices = [1.0, 1.01, 0.99, 1.0, 1.01, 0.99, 1.0, 1.01, 0.99, 1.0]
        result = std_dev(prices)
        assert result < 0.01

    def test_high_volatility(self):
        """High volatility values should have large std dev."""
        # Large swings
        prices = [1.0, 1.2, 0.8, 1.3, 0.7, 1.4, 0.6, 1.5, 0.5, 1.6]
        result = std_dev(prices)
        assert result > 0.3

    def test_numpy_array_input(self):
        """Should accept numpy array input."""
        arr = np.array([2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0])
        result = std_dev(arr)
        assert result == pytest.approx(2.0, rel=0.01)

    def test_population_std_dev_ddof_zero(self):
        """Should use population std dev (ddof=0), not sample std dev."""
        values = [2.0, 4.0, 6.0]
        # Mean = 4
        # Population variance = ((2-4)^2 + (4-4)^2 + (6-4)^2) / 3 = 8/3
        # Population std dev = sqrt(8/3) ≈ 1.6330
        expected_population = np.std(values, ddof=0)
        result = std_dev(values)
        assert result == pytest.approx(expected_population, rel=1e-10)

        # Sample std dev (ddof=1) would be different
        sample_std = np.std(values, ddof=1)
        assert result != pytest.approx(sample_std, rel=0.01)

    def test_consistent_with_numpy_std(self):
        """Result should match direct numpy.std calculation with ddof=0."""
        values = [1.5, 2.3, 3.1, 4.2, 5.0, 5.8, 7.1, 8.0]
        expected = np.std(values, ddof=0)
        result = std_dev(values)
        assert result == pytest.approx(expected, rel=1e-10)


class TestModuleExports:
    """Test that module exports are correct."""

    def test_all_exports(self):
        """__all__ should export both functions."""
        from polybot import indicators

        assert "linear_regression_slope" in indicators.__all__
        assert "std_dev" in indicators.__all__
        assert len(indicators.__all__) == 2

    def test_import_from_polybot(self):
        """Functions should be importable from polybot package."""
        from polybot import linear_regression_slope as lrs
        from polybot import std_dev as sd

        # Verify they work
        assert lrs([1.0, 2.0, 3.0]) == pytest.approx(1.0, rel=0.01)
        assert sd([1.0, 1.0, 1.0]) == pytest.approx(0.0, abs=1e-10)
