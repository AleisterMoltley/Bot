"""Tests for the risk_manager module."""

from unittest.mock import patch

import pytest

from polybot.config import MIN_BALANCE_USD, MIN_TRADE_SIZE_USD
from polybot.risk_manager import RiskManager, RiskState, get_risk_manager


class TestRiskThresholdConstants:
    """Tests for risk threshold constants."""

    def test_min_balance_usd_default(self):
        """Test MIN_BALANCE_USD has correct default value."""
        assert MIN_BALANCE_USD == 0.3

    def test_min_trade_size_usd_default(self):
        """Test MIN_TRADE_SIZE_USD has correct default value (EXECUTION FIX v3)."""
        # EXECUTION FIX v3: Lowered from 5.0 to 1.0
        assert MIN_TRADE_SIZE_USD == 1.0


class TestRiskState:
    """Tests for RiskState dataclass."""

    def test_default_state(self):
        """Test RiskState with default values."""
        state = RiskState()
        assert state.daily_loss == 0.0
        assert state.daily_profit == 0.0
        assert state.daily_trades == 0
        assert state.consecutive_losses == 0
        assert state.consecutive_wins == 0
        assert state.is_paused is False
        assert state.pause_reason == ""
        assert state.peak_balance == 0.0
        assert state.current_drawdown == 0.0


class TestRiskManager:
    """Tests for RiskManager class."""

    @pytest.fixture
    def risk_manager(self):
        """Create a fresh RiskManager for each test."""
        return RiskManager()

    def test_initialization(self, risk_manager):
        """Test RiskManager initializes with default state."""
        state = risk_manager.get_state()
        assert state.daily_loss == 0.0
        assert state.is_paused is False

    def test_check_can_trade_default(self, risk_manager):
        """Test that trading is always allowed (FORCED EXECUTION v5)."""
        can_trade, reason = risk_manager.check_can_trade()
        assert can_trade is True
        assert reason == "FORCED_EXECUTION"

    def test_check_liquidity_sufficient(self, risk_manager):
        """Test liquidity check always passes (FORCED EXECUTION v5)."""
        with patch("polybot.risk_manager.get_settings") as mock_settings:
            mock_settings.return_value.min_liquidity_usd = 5000.0
            passes, reason = risk_manager.check_liquidity(10000.0)
            assert passes is True
            assert reason == "OK"

    def test_check_liquidity_insufficient(self, risk_manager):
        """Test liquidity check always passes (FORCED EXECUTION v5)."""
        with patch("polybot.risk_manager.get_settings") as mock_settings:
            mock_settings.return_value.min_liquidity_usd = 5000.0
            # FORCED EXECUTION v5: Even insufficient liquidity passes
            passes, reason = risk_manager.check_liquidity(1000.0)
            assert passes is True
            assert reason == "OK"

    def test_record_trade_win(self, risk_manager):
        """Test recording a winning trade."""
        state = risk_manager.record_trade(10.0)
        assert state.daily_profit == 10.0
        assert state.daily_trades == 1
        assert state.consecutive_wins == 1
        assert state.consecutive_losses == 0

    def test_record_trade_loss(self, risk_manager):
        """Test recording a losing trade."""
        state = risk_manager.record_trade(-5.0)
        assert state.daily_loss == 5.0
        assert state.daily_trades == 1
        assert state.consecutive_losses == 1
        assert state.consecutive_wins == 0

    def test_streak_tracking(self, risk_manager):
        """Test that streaks are tracked correctly."""
        # Record 3 wins
        risk_manager.record_trade(10.0)
        risk_manager.record_trade(10.0)
        state = risk_manager.record_trade(10.0)
        assert state.consecutive_wins == 3
        assert state.consecutive_losses == 0

        # A loss resets win streak
        state = risk_manager.record_trade(-5.0)
        assert state.consecutive_wins == 0
        assert state.consecutive_losses == 1

    def test_circuit_breaker_consecutive_losses(self, risk_manager):
        """Test that consecutive losses no longer trigger circuit breaker (FORCED EXECUTION v5)."""
        with patch("polybot.risk_manager.get_settings") as mock_settings:
            mock_settings.return_value.max_daily_loss = 100.0
            mock_settings.return_value.circuit_breaker_consecutive_losses = 3
            mock_settings.return_value.max_daily_trades = 50

            # Record 3 consecutive losses
            risk_manager.record_trade(-5.0)
            risk_manager.record_trade(-5.0)
            risk_manager.record_trade(-5.0)

            # FORCED EXECUTION v5: Trading is always allowed
            can_trade, reason = risk_manager.check_can_trade()
            assert can_trade is True
            assert reason == "FORCED_EXECUTION"

    def test_circuit_breaker_daily_loss(self, risk_manager):
        """Test that daily loss limit no longer triggers circuit breaker (FORCED EXECUTION v5)."""
        with patch("polybot.risk_manager.get_settings") as mock_settings:
            mock_settings.return_value.max_daily_loss = 20.0
            mock_settings.return_value.circuit_breaker_consecutive_losses = 10
            mock_settings.return_value.max_daily_trades = 50

            # Exceed daily loss
            risk_manager.record_trade(-25.0)

            # FORCED EXECUTION v5: Trading is always allowed
            can_trade, reason = risk_manager.check_can_trade()
            assert can_trade is True
            assert reason == "FORCED_EXECUTION"

    def test_reset_circuit_breaker(self, risk_manager):
        """Test manual circuit breaker reset (now no-op since always allowed)."""
        with patch("polybot.risk_manager.get_settings") as mock_settings:
            mock_settings.return_value.max_daily_loss = 100.0
            mock_settings.return_value.circuit_breaker_consecutive_losses = 2
            mock_settings.return_value.max_daily_trades = 50

            # Trigger losses (no longer blocks trading)
            risk_manager.record_trade(-5.0)
            risk_manager.record_trade(-5.0)

            # FORCED EXECUTION v5: Trading is always allowed
            can_trade, _ = risk_manager.check_can_trade()
            assert can_trade is True

            # Reset (no-op)
            risk_manager.reset_circuit_breaker()

            # Should still be able to trade
            can_trade, reason = risk_manager.check_can_trade()
            assert can_trade is True
            assert reason == "FORCED_EXECUTION"

    def test_get_status_dict(self, risk_manager):
        """Test status dictionary contains expected fields."""
        with patch("polybot.risk_manager.get_settings") as mock_settings:
            mock_settings.return_value.max_daily_loss = 25.0
            mock_settings.return_value.circuit_breaker_consecutive_losses = 3
            mock_settings.return_value.max_daily_trades = 50

            status = risk_manager.get_status_dict()

            assert "is_paused" in status
            assert "daily_loss" in status
            assert "daily_profit" in status
            assert "daily_trades" in status
            assert "consecutive_losses" in status
            assert "consecutive_wins" in status
            assert "max_daily_trades" in status
            assert "trades_remaining" in status

    def test_update_balance_peak_tracking(self, risk_manager):
        """Test balance tracking updates peak correctly."""
        risk_manager.update_balance(1000.0)
        assert risk_manager.get_state().peak_balance == 1000.0
        assert risk_manager.get_state().current_drawdown == 0.0

        # New high
        risk_manager.update_balance(1100.0)
        assert risk_manager.get_state().peak_balance == 1100.0

        # Drawdown
        risk_manager.update_balance(1000.0)
        assert risk_manager.get_state().current_drawdown > 0

    def test_update_balance_low_balance_warning(self, risk_manager):
        """Test that low balance triggers warning."""
        # Update with balance below MIN_BALANCE_USD (0.3)
        # Mock the logger to capture log calls
        with patch("polybot.risk_manager.log") as mock_log:
            risk_manager.update_balance(0.1)  # Below 0.3 minimum
            # Check that warning was logged
            mock_log.warning.assert_called()
            # Verify the warning contains expected message
            call_args = mock_log.warning.call_args
            # Use .args for cleaner access to positional arguments
            assert len(call_args.args) > 0
            assert "Low balance warning" in call_args.args[0]

    def test_check_trade_size_above_minimum(self, risk_manager):
        """Test that trade size always passes (FORCED EXECUTION v5)."""
        passes, reason = risk_manager.check_trade_size(10.0)
        assert passes is True
        assert reason == "FORCED_OK"

    def test_check_trade_size_below_minimum(self, risk_manager):
        """Test that trade size ALWAYS passes (FORCED EXECUTION v5)."""
        passes, reason = risk_manager.check_trade_size(3.0)
        assert passes is True
        assert reason == "FORCED_OK"

    def test_check_trade_size_at_minimum(self, risk_manager):
        """Test that trade size at minimum passes (FORCED EXECUTION v5)."""
        from polybot.config import MIN_TRADE_SIZE_USD

        passes, reason = risk_manager.check_trade_size(MIN_TRADE_SIZE_USD)
        assert passes is True
        assert reason == "FORCED_OK"


class TestRiskManagerSingleton:
    """Tests for global risk manager singleton."""

    def test_get_risk_manager_returns_same_instance(self):
        """Test that get_risk_manager returns a singleton."""
        manager1 = get_risk_manager()
        manager2 = get_risk_manager()
        assert manager1 is manager2
