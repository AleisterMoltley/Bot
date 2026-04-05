"""Tests for timing filter and edge-bucketed Kelly sizing."""

import time
import pytest


class TestShouldTradeThisSlot:
    """Test the should_trade_this_slot timing filter."""

    def _make_slug(self, seconds_left: float) -> str:
        """Create a slug with a specific time remaining."""
        ts = int(time.time() + seconds_left - 300)  # slot_start = end - 300
        return f"btc-updown-5m-{ts}"

    def test_in_sweet_spot(self):
        from polybot.scanner import should_trade_this_slot
        slug = self._make_slug(50)  # 50 seconds left — in sweet spot
        assert should_trade_this_slot(slug) is True

    def test_too_close_to_end(self):
        from polybot.scanner import should_trade_this_slot
        slug = self._make_slug(10)  # 10 seconds left — too risky
        assert should_trade_this_slot(slug) is False

    def test_too_early_no_edge(self):
        from polybot.scanner import should_trade_this_slot
        slug = self._make_slug(200)  # 200 seconds left — too early
        assert should_trade_this_slot(slug, edge=0.01) is False

    def test_too_early_strong_edge_overrides(self):
        from polybot.scanner import should_trade_this_slot
        slug = self._make_slug(200)  # 200 seconds left
        assert should_trade_this_slot(slug, edge=0.05) is True  # 5% edge overrides

    def test_boundary_75_seconds(self):
        from polybot.scanner import should_trade_this_slot
        slug = self._make_slug(75)  # exactly at boundary
        # At 75 seconds, seconds_left == MIN_SECONDS_BEFORE_CLOSE → enters sweet spot
        assert should_trade_this_slot(slug) is True

    def test_boundary_20_seconds(self):
        from polybot.scanner import should_trade_this_slot
        slug = self._make_slug(20)  # exactly at minimum
        assert should_trade_this_slot(slug) is False

    def test_invalid_slug(self):
        from polybot.scanner import should_trade_this_slot
        assert should_trade_this_slot("invalid-slug") is False

    def test_expired_slot(self):
        from polybot.scanner import should_trade_this_slot
        slug = self._make_slug(-10)  # already expired
        assert should_trade_this_slot(slug) is False


class TestGetSecondsUntilClose:
    """Test seconds_until_close helper."""

    def test_future_slot(self):
        from polybot.scanner import get_seconds_until_close
        ts = int(time.time()) + 100  # slot started 200 seconds ago
        slug = f"btc-updown-5m-{ts}"
        sec = get_seconds_until_close(slug)
        assert 390 < sec < 410  # ~400 seconds left (ts + 300 - now)

    def test_invalid_slug(self):
        from polybot.scanner import get_seconds_until_close
        assert get_seconds_until_close("bad") == -1.0


class TestBucketedKelly:
    """Test edge-bucketed Kelly position sizing."""

    def test_zero_edge(self):
        from polybot.risk import calculate_bucketed_kelly
        assert calculate_bucketed_kelly(0.0, 1000.0) == 0.0

    def test_negative_edge(self):
        from polybot.risk import calculate_bucketed_kelly
        assert calculate_bucketed_kelly(-0.01, 1000.0) == 0.0

    def test_zero_bankroll(self):
        from polybot.risk import calculate_bucketed_kelly
        assert calculate_bucketed_kelly(0.03, 0.0) == 0.0

    def test_weak_edge_conservative(self):
        from polybot.risk import calculate_bucketed_kelly
        # 1% edge → bucket multiplier = 0.20
        size = calculate_bucketed_kelly(0.01, 1000.0)
        assert size > 0
        assert size < 20  # very conservative

    def test_normal_edge(self):
        from polybot.risk import calculate_bucketed_kelly
        # 2% edge → bucket multiplier = 0.50
        size = calculate_bucketed_kelly(0.02, 1000.0)
        assert size > 5

    def test_strong_edge(self):
        from polybot.risk import calculate_bucketed_kelly
        # 4% edge → bucket multiplier = 0.80
        size = calculate_bucketed_kelly(0.04, 1000.0)
        assert size > 10

    def test_extreme_edge(self):
        from polybot.risk import calculate_bucketed_kelly
        # 6% edge → bucket multiplier = 1.10
        size = calculate_bucketed_kelly(0.06, 1000.0)
        assert size > 15

    def test_hard_cap(self):
        from polybot.risk import calculate_bucketed_kelly
        # Very large bankroll — should be capped at max_position
        size = calculate_bucketed_kelly(0.10, 100000.0, max_position=60.0)
        assert size <= 60.0

    def test_min_position(self):
        from polybot.risk import calculate_bucketed_kelly
        # Tiny bankroll — should be at least min_position if edge > 0
        size = calculate_bucketed_kelly(0.01, 50.0, min_position=5.0)
        assert size >= 5.0

    def test_bucket_ordering(self):
        """Stronger edges should produce larger positions."""
        from polybot.risk import calculate_bucketed_kelly
        bankroll = 500.0
        s1 = calculate_bucketed_kelly(0.01, bankroll)  # weak
        s2 = calculate_bucketed_kelly(0.025, bankroll)  # normal
        s3 = calculate_bucketed_kelly(0.04, bankroll)  # strong
        s4 = calculate_bucketed_kelly(0.06, bankroll)  # extreme
        assert s1 < s2 < s3 < s4

    def test_custom_base_kelly(self):
        from polybot.risk import calculate_bucketed_kelly
        full = calculate_bucketed_kelly(0.03, 1000.0, base_kelly=1.0)
        half = calculate_bucketed_kelly(0.03, 1000.0, base_kelly=0.5)
        assert full > half

    def test_quarter_kelly_conservative(self):
        from polybot.risk import calculate_bucketed_kelly
        size = calculate_bucketed_kelly(0.03, 1000.0, base_kelly=0.25)
        assert size > 0
        assert size < 30
