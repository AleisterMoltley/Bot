"""ULTIMATIVE Test-Suite für PolyBot RiskManager
Ziel: Maximale Kapitalsicherung + langfristiger Profit

Diese Tests stellen sicher, dass der Bot:
- Niemals unbegrenzt verliert (kein FORCED_EXECUTION mehr)
- Bei allen definierten Limits zuverlässig pausiert
- Cooldowns korrekt ablaufen und automatisch wieder starten
- Position-Sizing bei Hot/Cold-Streaks profit-optimal reagiert
- Kombinierte Stress-Szenarien (wie reale Markt-Crashs) überlebt
- Status für Dashboard immer korrekt ist

Run with: pytest tests/test_risk_manager.py -v --cov=polybot.risk_manager
"""

import time
from unittest.mock import patch, MagicMock

import pytest

from polybot.risk_manager import RiskManager, get_risk_manager, RiskState


# ── Fixtures & Helpers ───────────────────────────────────────────────────────────


@pytest.fixture
def risk_manager() -> RiskManager:
    """Frischer RiskManager mit komplett resetztem State."""
    rm = RiskManager()
    rm._state = RiskState()  # harter Reset aller Zähler
    return rm


def _mock_settings(**overrides):
    """Zentrale, realistische Settings-Factory (optimiert für Profit + Sicherheit)."""
    defaults = {
        "max_daily_loss": 25.0,
        "circuit_breaker_consecutive_losses": 4,
        "max_daily_trades": 30,
        "max_drawdown_pct": 25.0,
        "min_liquidity_usd": 200.0,
        "min_trade_size_usd": 1.0,
        "max_position_usd": 50.0,
        "max_position_size_pct": 30.0,
        "min_balance_usd": 0.3,
        "max_concurrent_positions": 3,
    }
    defaults.update(overrides)
    mock = MagicMock()
    for k, v in defaults.items():
        setattr(mock, k, v)
    return mock


def record_losses(rm: RiskManager, count: int, amount: float = -8.0):
    """Helper: Mehrere Losses auf einmal (für schnelle Tests)."""
    for _ in range(count):
        rm.record_trade(amount)


def record_wins(rm: RiskManager, count: int, amount: float = 12.0):
    """Helper: Mehrere Wins."""
    for _ in range(count):
        rm.record_trade(amount)


# ── Core & Initial State ─────────────────────────────────────────────────────────


class TestRiskManagerCore:

    def test_initial_state_is_completely_safe(self, risk_manager):
        state = risk_manager.get_state()
        assert state.daily_loss == 0.0
        assert state.daily_profit == 0.0
        assert state.consecutive_losses == 0
        assert state.consecutive_wins == 0
        assert state.is_paused is False
        assert state.open_positions == 0
        assert state.execution_failures == 0
        assert state.current_drawdown == 0.0
        assert state.peak_balance == 0.0

    def test_can_trade_is_ok_when_nothing_happened(self, risk_manager):
        with patch("polybot.risk_manager.get_settings", return_value=_mock_settings()):
            can, reason = risk_manager.check_can_trade()
            assert can is True
            assert reason == "OK"


# ── Liquidity & Trade Size Checks ────────────────────────────────────────────────


class TestLiquidityAndSizeChecks:

    @pytest.mark.parametrize(
        "liquidity,min_liq,should_pass",
        [
            (150, 200, False),
            (200, 200, True),
            (5000, 200, True),
            (199.99, 200, False),
        ],
    )
    def test_liquidity_guard(self, risk_manager, liquidity, min_liq, should_pass):
        with patch(
            "polybot.risk_manager.get_settings",
            return_value=_mock_settings(min_liquidity_usd=min_liq),
        ):
            passes, reason = risk_manager.check_liquidity(liquidity)
            assert passes is should_pass
            if not should_pass:
                assert "Insufficient liquidity" in reason

    @pytest.mark.parametrize(
        "size,balance,expected_pass,reason_snippet",
        [
            (0.5, 100, False, "too small"),
            (10, 100, True, "ok"),
            (60, 100, False, "too large"),
            (40, 100, False, "position limit"),
            (15, 100, True, "ok"),
        ],
    )
    def test_trade_size_guard(
        self, risk_manager, size, balance, expected_pass, reason_snippet
    ):
        with patch(
            "polybot.risk_manager.get_settings",
            return_value=_mock_settings(
                min_trade_size_usd=1.0,
                max_position_usd=50.0,
                max_position_size_pct=30.0,
            ),
        ):
            passes, reason = risk_manager.check_trade_size(size, balance)
            assert passes is expected_pass
            assert reason_snippet in reason.lower()


# ── Circuit Breakers (die echten Profit-Schützer) ────────────────────────────────


@pytest.mark.circuit_breaker
class TestCircuitBreakers:

    @pytest.mark.parametrize(
        "threshold,losses,should_block",
        [(3, 2, False), (3, 3, True), (4, 4, True), (5, 6, True)],
    )
    def test_consecutive_losses_breaker(
        self, risk_manager, threshold, losses, should_block
    ):
        with patch(
            "polybot.risk_manager.get_settings",
            return_value=_mock_settings(
                circuit_breaker_consecutive_losses=threshold
            ),
        ):
            record_losses(risk_manager, losses)
            can, reason = risk_manager.check_can_trade()
            assert can is not should_block
            if should_block:
                assert "consecutive losses" in reason.lower()
                assert risk_manager.get_state().is_paused is True

    @pytest.mark.parametrize(
        "max_loss,recorded,should_block",
        [(25, -20, False), (25, -30, True), (15, -15.01, True)],
    )
    def test_daily_loss_breaker(self, risk_manager, max_loss, recorded, should_block):
        with patch(
            "polybot.risk_manager.get_settings",
            return_value=_mock_settings(max_daily_loss=max_loss),
        ):
            risk_manager.record_trade(recorded)
            can, reason = risk_manager.check_can_trade()
            assert can is not should_block
            if should_block:
                assert "Daily loss limit" in reason

    def test_daily_trade_limit(self, risk_manager):
        with patch(
            "polybot.risk_manager.get_settings",
            return_value=_mock_settings(max_daily_trades=5),
        ):
            record_wins(risk_manager, 5)
            can, reason = risk_manager.check_can_trade()
            assert can is False
            assert "Daily trade limit" in reason

    def test_drawdown_breaker(self, risk_manager):
        with patch(
            "polybot.risk_manager.get_settings",
            return_value=_mock_settings(max_drawdown_pct=20.0),
        ):
            risk_manager.update_balance(2000.0)  # Peak
            risk_manager.update_balance(1400.0)  # 30% Drawdown
            assert risk_manager.get_state().is_paused is True

    def test_concurrent_positions_breaker(self, risk_manager):
        with patch(
            "polybot.risk_manager.get_settings",
            return_value=_mock_settings(max_concurrent_positions=2),
        ):
            risk_manager.set_open_positions(2)
            can, reason = risk_manager.check_can_trade()
            assert can is False
            assert "concurrent positions" in reason.lower()

    def test_execution_failures_breaker(self, risk_manager):
        with patch("polybot.risk_manager.get_settings", return_value=_mock_settings()):
            for _ in range(4):
                risk_manager.record_execution_failure()
            can, reason = risk_manager.check_can_trade()
            assert can is False
            assert "execution failures" in reason.lower()


# ── Cooldown, Reset & Recovery ───────────────────────────────────────────────────


@pytest.mark.cooldown
class TestCooldownAndRecovery:

    def test_cooldown_expires_and_resumes_trading(self, risk_manager):
        """Simulate cooldown expiry by manipulating pause_time."""
        with patch(
            "polybot.risk_manager.get_settings",
            return_value=_mock_settings(circuit_breaker_consecutive_losses=2),
        ):
            record_losses(risk_manager, 2)
            assert risk_manager.check_can_trade()[0] is False

            # Simulate time passing beyond cooldown
            risk_manager._state.pause_time = time.time() - 9999
            risk_manager._state.consecutive_losses = 0  # reset by hypothetical win

            can, reason = risk_manager.check_can_trade()
            assert can is True
            assert reason == "OK"
            assert risk_manager.get_state().is_paused is False

    def test_win_resets_consecutive_loss_streak(self, risk_manager):
        with patch("polybot.risk_manager.get_settings", return_value=_mock_settings()):
            record_losses(risk_manager, 3)
            assert risk_manager.get_state().consecutive_losses == 3
            record_wins(risk_manager, 1)
            assert risk_manager.get_state().consecutive_losses == 0
            assert risk_manager.get_state().consecutive_wins == 1

    def test_manual_reset_restores_full_trading(self, risk_manager):
        with patch("polybot.risk_manager.get_settings", return_value=_mock_settings()):
            record_losses(risk_manager, 6)
            for _ in range(3):
                risk_manager.record_execution_failure()

            risk_manager.reset_circuit_breaker()

            state = risk_manager.get_state()
            assert not state.is_paused
            assert state.consecutive_losses == 0
            assert state.execution_failures == 0

    def test_cooldown_does_not_expire_while_still_within_window(self, risk_manager):
        """Paused state persists if cooldown hasn't elapsed."""
        with patch(
            "polybot.risk_manager.get_settings",
            return_value=_mock_settings(circuit_breaker_consecutive_losses=2),
        ):
            record_losses(risk_manager, 2)
            # Don't manipulate time — should still be paused
            can, _ = risk_manager.check_can_trade()
            assert can is False
            assert risk_manager.get_state().is_paused is True


# ── Position & Execution Tracking ────────────────────────────────────────────────


class TestTracking:

    def test_open_close_position_tracking(self, risk_manager):
        for _ in range(5):
            risk_manager.record_position_opened()
        assert risk_manager.get_state().open_positions == 5

        for _ in range(3):
            risk_manager.record_position_closed()
        assert risk_manager.get_state().open_positions == 2

    def test_close_never_goes_negative(self, risk_manager):
        risk_manager.record_position_closed()
        risk_manager.record_position_closed()
        assert risk_manager.get_state().open_positions == 0

    def test_successful_execution_resets_failure_counter(self, risk_manager):
        for _ in range(4):
            risk_manager.record_execution_failure()
        assert risk_manager.get_state().execution_failures == 4
        risk_manager.record_position_opened()
        assert risk_manager.get_state().execution_failures == 0

    def test_set_open_positions_from_db_sync(self, risk_manager):
        risk_manager.set_open_positions(7)
        assert risk_manager.get_state().open_positions == 7
        risk_manager.set_open_positions(-3)  # clamps to 0
        assert risk_manager.get_state().open_positions == 0


# ── Trade Recording ──────────────────────────────────────────────────────────────


class TestTradeRecording:

    def test_record_win(self, risk_manager):
        with patch("polybot.risk_manager.get_settings", return_value=_mock_settings()):
            state = risk_manager.record_trade(10.0)
            assert state.daily_profit == 10.0
            assert state.daily_trades == 1
            assert state.consecutive_wins == 1
            assert state.consecutive_losses == 0

    def test_record_loss(self, risk_manager):
        with patch("polybot.risk_manager.get_settings", return_value=_mock_settings()):
            state = risk_manager.record_trade(-5.0)
            assert state.daily_loss == 5.0
            assert state.daily_trades == 1
            assert state.consecutive_losses == 1
            assert state.consecutive_wins == 0

    def test_streak_tracking_win_to_loss(self, risk_manager):
        with patch("polybot.risk_manager.get_settings", return_value=_mock_settings()):
            record_wins(risk_manager, 3)
            assert risk_manager.get_state().consecutive_wins == 3

            risk_manager.record_trade(-1.0)
            assert risk_manager.get_state().consecutive_wins == 0
            assert risk_manager.get_state().consecutive_losses == 1

    def test_recent_trades_capped_at_20(self, risk_manager):
        with patch("polybot.risk_manager.get_settings", return_value=_mock_settings()):
            for i in range(30):
                risk_manager.record_trade(float(i))
            assert len(risk_manager.get_state().recent_trades) == 20

    def test_win_rate_calculation(self, risk_manager):
        with patch("polybot.risk_manager.get_settings", return_value=_mock_settings()):
            record_wins(risk_manager, 7)
            record_losses(risk_manager, 3)
            win_rate = risk_manager.get_recent_win_rate()
            assert 0.69 < win_rate < 0.71  # 7/10

    def test_win_rate_empty_is_neutral(self, risk_manager):
        assert risk_manager.get_recent_win_rate() == 0.5


# ── Balance & Drawdown ───────────────────────────────────────────────────────────


class TestBalanceAndDrawdown:

    def test_peak_tracking(self, risk_manager):
        with patch("polybot.risk_manager.get_settings", return_value=_mock_settings()):
            risk_manager.update_balance(1000.0)
            assert risk_manager.get_state().peak_balance == 1000.0

            risk_manager.update_balance(1100.0)
            assert risk_manager.get_state().peak_balance == 1100.0

            risk_manager.update_balance(1050.0)
            assert risk_manager.get_state().peak_balance == 1100.0  # stays at peak

    def test_drawdown_calculation(self, risk_manager):
        with patch("polybot.risk_manager.get_settings", return_value=_mock_settings()):
            risk_manager.update_balance(1000.0)
            risk_manager.update_balance(800.0)
            assert abs(risk_manager.get_state().current_drawdown - 20.0) < 0.1

    def test_low_balance_warning(self, risk_manager):
        with patch("polybot.risk_manager.get_settings", return_value=_mock_settings()):
            with patch("polybot.risk_manager.log") as mock_log:
                risk_manager.update_balance(0.1)
                mock_log.warning.assert_called()
                assert "Low balance warning" in mock_log.warning.call_args.args[0]

    def test_new_high_resets_drawdown(self, risk_manager):
        with patch("polybot.risk_manager.get_settings", return_value=_mock_settings()):
            risk_manager.update_balance(1000.0)
            risk_manager.update_balance(800.0)
            assert risk_manager.get_state().current_drawdown > 0
            risk_manager.update_balance(1200.0)  # new ATH
            assert risk_manager.get_state().current_drawdown == 0.0


# ── Sizing Factor (direkt profit-relevant) ───────────────────────────────────────


class TestSizingFactor:

    @pytest.mark.parametrize("wins,losses,expected", [
        (3, 0, 1.0),     # hot streak
        (10, 0, 1.0),    # very hot
        (1, 0, 0.85),    # warm
        (0, 0, 0.75),    # neutral
        (0, 1, 0.55),    # slight cold
        (0, 2, 0.55),    # cold
        (0, 3, 0.4),     # very cold
        (0, 10, 0.4),    # ice cold
    ])
    def test_sizing_by_streak(self, risk_manager, wins, losses, expected):
        risk_manager._state.consecutive_wins = wins
        risk_manager._state.consecutive_losses = losses
        assert risk_manager.get_sizing_factor() == expected


# ── Status Dictionary (für Dashboard & Monitoring) ───────────────────────────────


class TestStatusDictionary:

    def test_status_dict_contains_all_dashboard_keys(self, risk_manager):
        with patch("polybot.risk_manager.get_settings", return_value=_mock_settings()):
            status = risk_manager.get_status_dict()
            required_keys = {
                "is_paused", "pause_reason", "daily_loss", "daily_profit",
                "daily_net", "daily_trades", "max_daily_trades", "trades_remaining",
                "consecutive_losses", "consecutive_wins", "max_consecutive_losses",
                "current_drawdown_pct", "max_daily_loss", "loss_remaining",
                "sizing_factor", "recent_win_rate",
                "open_positions", "max_concurrent_positions", "execution_failures",
            }
            assert required_keys.issubset(status.keys()), (
                f"Missing keys: {required_keys - status.keys()}"
            )

    def test_status_values_after_trading(self, risk_manager):
        with patch("polybot.risk_manager.get_settings", return_value=_mock_settings()):
            record_wins(risk_manager, 3)
            risk_manager.record_trade(-5.0)

            status = risk_manager.get_status_dict()
            assert status["daily_profit"] == 36.0  # 3 × 12
            assert status["daily_loss"] == 5.0
            assert status["daily_net"] == 31.0
            assert status["daily_trades"] == 4
            assert status["consecutive_losses"] == 1
            assert status["consecutive_wins"] == 0


# ── Extreme Stress & Combined Scenarios ──────────────────────────────────────────


@pytest.mark.stress
class TestExtremeStress:

    def test_massive_combined_failure_scenario(self, risk_manager):
        """Simuliert den worst-case: viele Losses + Drawdown + Execution Failures."""
        with patch(
            "polybot.risk_manager.get_settings",
            return_value=_mock_settings(
                max_daily_loss=20.0,
                max_drawdown_pct=15.0,
                max_concurrent_positions=2,
            ),
        ):
            risk_manager.update_balance(5000.0)
            record_losses(risk_manager, 10, -10.0)
            risk_manager.update_balance(3800.0)  # >15% drawdown
            for _ in range(5):
                risk_manager.record_execution_failure()
            risk_manager.set_open_positions(3)

            can, reason = risk_manager.check_can_trade()
            assert can is False
            assert any(
                x in reason.lower()
                for x in [
                    "daily loss", "drawdown", "consecutive",
                    "execution", "concurrent", "paused",
                ]
            )

    def test_rapid_loss_recovery_cycle(self, risk_manager):
        """Bot verliert, pausiert, recovered, handelt wieder."""
        with patch(
            "polybot.risk_manager.get_settings",
            return_value=_mock_settings(circuit_breaker_consecutive_losses=3),
        ):
            # Phase 1: Losses → pause
            record_losses(risk_manager, 3)
            assert risk_manager.check_can_trade()[0] is False

            # Phase 2: Manual reset
            risk_manager.reset_circuit_breaker()
            assert risk_manager.check_can_trade()[0] is True

            # Phase 3: Wins → healthy state
            record_wins(risk_manager, 5)
            assert risk_manager.get_state().consecutive_wins == 5
            assert risk_manager.get_sizing_factor() == 1.0

    def test_all_breakers_fire_independently(self, risk_manager):
        """Jeder Breaker muss unabhängig feuern können."""
        with patch("polybot.risk_manager.get_settings", return_value=_mock_settings()):
            # Daily loss
            risk_manager.record_trade(-30.0)
            can1, r1 = risk_manager.check_can_trade()
            assert can1 is False
            assert "Daily loss" in r1

        # Fresh manager for consecutive losses
        rm2 = RiskManager()
        rm2._state = RiskState()
        with patch(
            "polybot.risk_manager.get_settings",
            return_value=_mock_settings(circuit_breaker_consecutive_losses=2),
        ):
            record_losses(rm2, 2, -1.0)
            can2, r2 = rm2.check_can_trade()
            assert can2 is False
            assert "consecutive losses" in r2.lower()

        # Fresh manager for execution failures
        rm3 = RiskManager()
        rm3._state = RiskState()
        with patch("polybot.risk_manager.get_settings", return_value=_mock_settings()):
            for _ in range(3):
                rm3.record_execution_failure()
            can3, r3 = rm3.check_can_trade()
            assert can3 is False
            assert "execution failures" in r3.lower()


# ── Singleton Behavior ───────────────────────────────────────────────────────────


class TestSingleton:

    def test_get_risk_manager_is_singleton_and_state_is_shared(self):
        rm1 = get_risk_manager()
        rm2 = get_risk_manager()
        assert rm1 is rm2

        # Mutations on one are visible on the other
        initial_trades = rm1.get_state().daily_trades
        rm1.record_trade(-1.0)
        assert rm2.get_state().daily_trades == initial_trades + 1
