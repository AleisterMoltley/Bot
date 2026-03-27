"""Tests for smart timing functions in signals module."""

from datetime import datetime, timezone
from unittest.mock import patch


from polybot.signals import (
    SignalResult,
    compute_signal,
    get_market_timing_info,
    get_seconds_until_market_close,
    get_smart_scan_interval,
    should_skip_trade_near_close,
)


class TestGetSecondsUntilMarketClose:
    """Tests for get_seconds_until_market_close function."""

    def test_returns_positive_value(self):
        """Test that the function returns a non-negative value."""
        result = get_seconds_until_market_close()
        assert result >= 0
        assert result <= 300  # Max 5 minutes for 5-min markets

    def test_with_specific_market_end_time(self):
        """Test with a specific market end time."""
        from datetime import timedelta

        now = datetime.now(timezone.utc)
        # Set end time 60 seconds from now
        end_time = now + timedelta(seconds=60)

        result = get_seconds_until_market_close(end_time)
        # Should be approximately 60 seconds (allow some tolerance)
        assert 55 <= result <= 65

    def test_past_end_time_returns_zero(self):
        """Test that past end time returns 0."""
        past = datetime(2020, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        result = get_seconds_until_market_close(past)
        assert result == 0


class TestGetSmartScanInterval:
    """Tests for get_smart_scan_interval function."""

    def test_final_phase_interval(self):
        """Test interval for final phase (<2 min to close)."""
        with patch("polybot.signals.get_settings") as mock_settings:
            mock_settings.return_value.smart_scan_enabled = True
            mock_settings.return_value.high_frequency_interval = 15
            mock_settings.return_value.normal_interval = 45
            mock_settings.return_value.scan_interval_seconds = 30

            # 60 seconds to close = final phase
            interval = get_smart_scan_interval(60)
            assert interval == 15  # HIGH_FREQUENCY_INTERVAL

    def test_active_phase_interval(self):
        """Test interval for active phase (2-5 min to close)."""
        with patch("polybot.signals.get_settings") as mock_settings:
            mock_settings.return_value.smart_scan_enabled = True
            mock_settings.return_value.high_frequency_interval = 15
            mock_settings.return_value.normal_interval = 45
            mock_settings.return_value.scan_interval_seconds = 30

            # 200 seconds to close = active phase
            interval = get_smart_scan_interval(200)
            assert interval == 25  # Medium frequency

    def test_early_phase_interval(self):
        """Test interval for early phase (>5 min to close)."""
        with patch("polybot.signals.get_settings") as mock_settings:
            mock_settings.return_value.smart_scan_enabled = True
            mock_settings.return_value.high_frequency_interval = 15
            mock_settings.return_value.normal_interval = 45
            mock_settings.return_value.scan_interval_seconds = 30

            # 400 seconds to close = early phase
            interval = get_smart_scan_interval(400)
            assert interval == 45  # NORMAL_INTERVAL

    def test_smart_scan_disabled(self):
        """Test that disabled smart scan returns fixed interval."""
        with patch("polybot.signals.get_settings") as mock_settings:
            mock_settings.return_value.smart_scan_enabled = False
            mock_settings.return_value.scan_interval_seconds = 30

            # Should return fixed interval regardless of seconds_to_close
            interval = get_smart_scan_interval(60)
            assert interval == 30  # scan_interval_seconds


class TestShouldSkipTradeNearClose:
    """Tests for should_skip_trade_near_close function."""

    def test_high_spread_last_minute_rejected(self):
        """Test that high spread trades are rejected in last minute."""
        should_skip, reason = should_skip_trade_near_close(
            seconds_to_close=30,
            spread_pct=0.8,  # High spread
            max_spread_pct=0.5,
        )
        assert should_skip is True
        assert "High spread" in reason

    def test_low_spread_last_minute_allowed(self):
        """Test that low spread trades are allowed in last minute."""
        should_skip, reason = should_skip_trade_near_close(
            seconds_to_close=30,
            spread_pct=0.3,  # Low spread
            max_spread_pct=0.5,
        )
        assert should_skip is False
        assert reason == "OK"

    def test_high_spread_early_allowed(self):
        """Test that high spread trades are allowed outside last minute."""
        should_skip, reason = should_skip_trade_near_close(
            seconds_to_close=120,  # 2 minutes
            spread_pct=0.8,  # High spread
            max_spread_pct=0.5,
        )
        assert should_skip is False
        assert reason == "OK"


class TestGetMarketTimingInfo:
    """Tests for get_market_timing_info function."""

    def test_returns_expected_fields(self):
        """Test that timing info contains expected fields."""
        with patch("polybot.signals.get_settings") as mock_settings:
            mock_settings.return_value.smart_scan_enabled = True
            mock_settings.return_value.high_frequency_interval = 15
            mock_settings.return_value.normal_interval = 45
            mock_settings.return_value.scan_interval_seconds = 30

            info = get_market_timing_info()

            assert "seconds_to_close" in info
            assert "recommended_interval" in info
            assert "phase" in info
            assert "timestamp" in info

    def test_phase_values(self):
        """Test that phase values are correct."""
        with patch("polybot.signals.get_settings") as mock_settings:
            mock_settings.return_value.smart_scan_enabled = True
            mock_settings.return_value.high_frequency_interval = 15
            mock_settings.return_value.normal_interval = 45
            mock_settings.return_value.scan_interval_seconds = 30

            # The phase depends on actual time, so we just check it's valid
            info = get_market_timing_info()
            assert info["phase"] in ["early", "active", "final"]


class TestComputeSignal:
    """Tests for compute_signal function (existing functionality)."""

    def test_compute_signal_returns_result(self):
        """Test that compute_signal returns a SignalResult."""
        closes = [
            100.0,
            101.0,
            102.0,
            103.0,
            104.0,
            105.0,
            104.0,
            103.0,
            102.0,
            103.0,
            104.0,
            105.0,
            106.0,
            107.0,
            108.0,
            109.0,
            110.0,
            111.0,
            112.0,
            113.0,
            114.0,
            115.0,
            116.0,
            117.0,
            118.0,
            119.0,
            120.0,
            121.0,
            122.0,
            123.0,
            124.0,
            125.0,
            126.0,
            127.0,
            128.0,
            129.0,
        ]

        result = compute_signal(closes)

        assert isinstance(result, SignalResult)
        assert result.direction in ["up", "down", "hold"]
        assert 0 <= result.confidence <= 100
        assert isinstance(result.components, dict)

    def test_compute_signal_with_volumes(self):
        """Test compute_signal with volume data."""
        closes = [100.0] * 36  # Simple flat prices
        volumes = [1000.0] * 36

        result = compute_signal(closes, volumes=volumes)

        assert isinstance(result, SignalResult)

    def test_compute_signal_insufficient_data(self):
        """Test compute_signal with insufficient data."""
        closes = [100.0, 101.0]  # Too few data points

        result = compute_signal(closes)

        # Should still return a result, just with low confidence
        assert isinstance(result, SignalResult)
