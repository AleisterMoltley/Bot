"""Risk Manager — All guards ACTIVE.

Circuit breakers, daily loss limits, drawdown stops, liquidity checks,
and trade size checks are fully enforced. Adaptive cooldown scales
pause duration with streak severity.

Guards:
- check_can_trade(): daily loss limit, consecutive-loss circuit breaker,
  drawdown stop, daily trade cap, timed cooldown expiry
- check_trade_size(): min/max trade size, max % of balance
- check_liquidity(): minimum liquidity threshold
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

from polybot.config import get_settings
from polybot.logging_setup import get_logger

log = get_logger(__name__)

# Adaptive cooldown: pause duration scales with streak severity
COOLDOWN_BASE_SECONDS = 120
COOLDOWN_SCALE_FACTOR = 1.5
COOLDOWN_MAX_SECONDS = 1800


@dataclass
class RiskState:
    """Current risk management state."""

    daily_loss: float = 0.0
    daily_profit: float = 0.0
    daily_trades: int = 0
    last_reset_date: str = ""

    consecutive_losses: int = 0
    consecutive_wins: int = 0

    is_paused: bool = False
    pause_reason: str = ""
    pause_time: float = 0.0
    pause_duration: float = 0.0

    peak_balance: float = 0.0
    current_drawdown: float = 0.0

    open_positions: int = 0
    execution_failures: int = 0  # consecutive fill/revert failures

    recent_trades: list = field(default_factory=list)
    updated_at: float = field(default_factory=time.time)


class RiskManager:
    """Centralized risk management — ALL guards active."""

    def __init__(self) -> None:
        self._state = RiskState()
        self._ensure_daily_reset()

    def _ensure_daily_reset(self) -> None:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if self._state.last_reset_date != today:
            log.info("Daily risk counters reset", new_date=today)
            self._state.daily_loss = 0.0
            self._state.daily_profit = 0.0
            self._state.daily_trades = 0
            self._state.last_reset_date = today
            if self._state.pause_reason in ("daily_loss_limit", "daily_trade_limit"):
                self._state.is_paused = False
                self._state.pause_reason = ""

    def _check_cooldown_expired(self) -> bool:
        """Check if a timed pause has expired and auto-resume."""
        if not self._state.is_paused:
            return False
        if self._state.pause_duration <= 0:
            return False  # indefinite pause — needs manual reset
        elapsed = time.time() - self._state.pause_time
        if elapsed >= self._state.pause_duration:
            log.info(
                "Cooldown expired — resuming trading",
                reason=self._state.pause_reason,
                elapsed=f"{elapsed:.0f}s",
            )
            self._state.is_paused = False
            self._state.pause_reason = ""
            self._state.pause_time = 0.0
            self._state.pause_duration = 0.0
            return True
        return False

    def check_can_trade(self) -> tuple[bool, str]:
        """Check all risk limits before allowing a trade.

        Returns:
            (allowed: bool, reason: str)
        """
        self._ensure_daily_reset()
        settings = get_settings()

        # Check if timed cooldown expired
        self._check_cooldown_expired()

        # 1. Already paused (circuit breaker active)
        if self._state.is_paused:
            remaining = 0.0
            if self._state.pause_duration > 0:
                remaining = max(
                    0, self._state.pause_duration - (time.time() - self._state.pause_time)
                )
            return False, (
                f"PAUSED: {self._state.pause_reason} "
                f"(remaining: {remaining:.0f}s)"
            )

        # 2. Daily loss limit
        if self._state.daily_loss >= settings.max_daily_loss:
            self._trigger_circuit_breaker("daily_loss_limit")
            return False, (
                f"Daily loss limit reached "
                f"(${self._state.daily_loss:.2f} >= ${settings.max_daily_loss:.2f})"
            )

        # 3. Consecutive losses → adaptive cooldown
        if self._state.consecutive_losses >= settings.circuit_breaker_consecutive_losses:
            cooldown = self._calc_cooldown(self._state.consecutive_losses)
            self._trigger_circuit_breaker("consecutive_losses", duration=cooldown)
            return False, (
                f"Circuit breaker: {self._state.consecutive_losses} consecutive losses "
                f"(cooldown: {cooldown:.0f}s)"
            )

        # 4. Drawdown stop
        if (
            self._state.current_drawdown > 0
            and self._state.current_drawdown >= settings.max_drawdown_pct
        ):
            self._trigger_circuit_breaker("max_drawdown", duration=3600)
            return False, (
                f"Drawdown stop: {self._state.current_drawdown:.1f}% "
                f">= {settings.max_drawdown_pct:.1f}%"
            )

        # 5. Daily trade count
        if self._state.daily_trades >= settings.max_daily_trades:
            self._trigger_circuit_breaker("daily_trade_limit")
            return False, (
                f"Daily trade limit: {self._state.daily_trades} "
                f">= {settings.max_daily_trades}"
            )

        # 6. Max concurrent positions
        if self._state.open_positions >= settings.max_concurrent_positions:
            return False, (
                f"Max concurrent positions: {self._state.open_positions} "
                f">= {settings.max_concurrent_positions}"
            )

        # 7. Execution failure cooldown (3+ consecutive failures → 5 min pause)
        if self._state.execution_failures >= 3:
            self._trigger_circuit_breaker("execution_failures", duration=300)
            return False, (
                f"Too many execution failures: {self._state.execution_failures} consecutive"
            )

        return True, "OK"

    def check_liquidity(self, liquidity_usd: float) -> tuple[bool, str]:
        """Check if market has sufficient liquidity.

        Returns:
            (passes: bool, reason: str)
        """
        settings = get_settings()
        if liquidity_usd < settings.min_liquidity_usd:
            return False, (
                f"Insufficient liquidity: ${liquidity_usd:.0f} "
                f"< ${settings.min_liquidity_usd:.0f}"
            )
        return True, "OK"

    def check_trade_size(
        self, trade_size_usd: float, balance: float = 0.0
    ) -> tuple[bool, str]:
        """Check if trade size is within acceptable limits.

        Returns:
            (passes: bool, reason: str)
        """
        settings = get_settings()

        if trade_size_usd < settings.min_trade_size_usd:
            return False, (
                f"Trade too small: ${trade_size_usd:.2f} "
                f"< ${settings.min_trade_size_usd:.2f}"
            )

        if trade_size_usd > settings.max_position_usd:
            return False, (
                f"Trade too large: ${trade_size_usd:.2f} "
                f"> ${settings.max_position_usd:.2f}"
            )

        if balance > 0:
            pct = (trade_size_usd / balance) * 100
            if pct > settings.max_position_size_pct:
                return False, (
                    f"Trade exceeds position limit: {pct:.1f}% "
                    f"> {settings.max_position_size_pct:.1f}% of balance"
                )

        return True, "OK"

    def get_sizing_factor(self) -> float:
        """Performance-based sizing multiplier (0.4-1.0).

        Hot streak: full size. Cold: half size. Prevents compounding losses.
        """
        if self._state.consecutive_wins >= 3:
            return 1.0
        elif self._state.consecutive_wins >= 1:
            return 0.85
        elif self._state.consecutive_losses == 0:
            return 0.75
        elif self._state.consecutive_losses <= 2:
            return 0.55
        else:
            return 0.4

    def record_trade(self, profit: float, private_key: str = "") -> RiskState:
        """Record trade result and update state. Auto-saves 1% to piggybank on win."""
        self._ensure_daily_reset()
        self._state.daily_trades += 1
        self._state.updated_at = time.time()

        self._state.recent_trades.append(profit)
        if len(self._state.recent_trades) > 20:
            self._state.recent_trades = self._state.recent_trades[-20:]

        if profit >= 0:
            self._state.daily_profit += profit
            self._state.consecutive_wins += 1
            self._state.consecutive_losses = 0
            log.info("WIN", profit=f"${profit:.2f}",
                     net=f"${self._state.daily_profit - self._state.daily_loss:.2f}",
                     streak=f"+{self._state.consecutive_wins}")

            # 🐷 Piggybank: auto-save 1% of profit
            if profit > 0 and private_key:
                try:
                    from polybot.piggybank import on_profit_async
                    import asyncio
                    try:
                        loop = asyncio.get_running_loop()
                        loop.create_task(on_profit_async(profit, private_key))
                    except RuntimeError:
                        from polybot.piggybank import on_profit
                        on_profit(profit, private_key)
                except Exception as e:
                    log.debug("Piggybank skipped: %s", e)
        else:
            self._state.daily_loss += abs(profit)
            self._state.consecutive_losses += 1
            self._state.consecutive_wins = 0
            log.warning("LOSS", loss=f"${abs(profit):.2f}",
                        net=f"${self._state.daily_profit - self._state.daily_loss:.2f}",
                        streak=f"-{self._state.consecutive_losses}")

        # Check if risk limits are now breached after this trade
        can_trade, reason = self.check_can_trade()
        if not can_trade:
            log.warning("Risk limit hit after trade", reason=reason)
        return self._state

    def update_balance(self, current_balance: float) -> None:
        """Track peak balance and drawdown. Logs warning if balance drops below minimum."""
        if current_balance > self._state.peak_balance:
            self._state.peak_balance = current_balance
            self._state.current_drawdown = 0.0
        elif self._state.peak_balance > 0:
            self._state.current_drawdown = (
                (self._state.peak_balance - current_balance) / self._state.peak_balance
            ) * 100

        settings = get_settings()
        if current_balance < settings.min_balance_usd:
            log.warning("Low balance warning",
                        balance=f"${current_balance:.2f}",
                        minimum=f"${settings.min_balance_usd:.2f}")

        # Check drawdown after balance update
        if (
            self._state.current_drawdown > 0
            and self._state.current_drawdown >= settings.max_drawdown_pct
            and not self._state.is_paused
        ):
            self._trigger_circuit_breaker("max_drawdown", duration=3600)

    def get_recent_win_rate(self) -> float:
        trades = self._state.recent_trades
        if not trades:
            return 0.5
        return sum(1 for t in trades if t >= 0) / len(trades)

    def _calc_cooldown(self, consecutive_losses: int) -> float:
        settings = get_settings()
        excess = max(0, consecutive_losses - settings.circuit_breaker_consecutive_losses)
        return min(COOLDOWN_BASE_SECONDS * (COOLDOWN_SCALE_FACTOR ** excess), COOLDOWN_MAX_SECONDS)

    def _trigger_circuit_breaker(self, reason: str, duration: float = 0.0) -> None:
        if not self._state.is_paused:
            self._state.is_paused = True
            self._state.pause_reason = reason
            self._state.pause_time = time.time()
            self._state.pause_duration = duration
            log.warning("🛑 CIRCUIT BREAKER", reason=reason,
                        duration=f"{duration:.0f}s" if duration else "indefinite",
                        loss=f"${self._state.daily_loss:.2f}",
                        streak=f"-{self._state.consecutive_losses}",
                        dd=f"{self._state.current_drawdown:.1f}%")

    def reset_circuit_breaker(self) -> None:
        self._state.is_paused = False
        self._state.pause_reason = ""
        self._state.pause_time = 0.0
        self._state.pause_duration = 0.0
        self._state.consecutive_losses = 0
        self._state.execution_failures = 0
        log.info("Circuit breaker manually reset")

    def record_position_opened(self) -> None:
        """Track a new open position."""
        self._state.open_positions += 1
        self._state.execution_failures = 0  # successful execution resets failure counter

    def record_position_closed(self) -> None:
        """Track a closed position."""
        self._state.open_positions = max(0, self._state.open_positions - 1)

    def record_execution_failure(self) -> None:
        """Track a failed trade execution (fill failure, revert, timeout)."""
        self._state.execution_failures += 1
        log.warning(
            "Execution failure recorded",
            consecutive=self._state.execution_failures,
        )

    def set_open_positions(self, count: int) -> None:
        """Set open position count (e.g. from DB sync)."""
        self._state.open_positions = max(0, count)

    def get_state(self) -> RiskState:
        self._ensure_daily_reset()
        return self._state

    def get_status_dict(self) -> dict:
        self._ensure_daily_reset()
        settings = get_settings()
        return {
            "is_paused": self._state.is_paused,
            "pause_reason": self._state.pause_reason,
            "daily_loss": round(self._state.daily_loss, 2),
            "daily_profit": round(self._state.daily_profit, 2),
            "daily_net": round(self._state.daily_profit - self._state.daily_loss, 2),
            "daily_trades": self._state.daily_trades,
            "max_daily_trades": settings.max_daily_trades,
            "trades_remaining": max(0, settings.max_daily_trades - self._state.daily_trades),
            "consecutive_losses": self._state.consecutive_losses,
            "consecutive_wins": self._state.consecutive_wins,
            "max_consecutive_losses": settings.circuit_breaker_consecutive_losses,
            "current_drawdown_pct": round(self._state.current_drawdown, 2),
            "max_daily_loss": settings.max_daily_loss,
            "loss_remaining": round(max(0, settings.max_daily_loss - self._state.daily_loss), 2),
            "sizing_factor": self.get_sizing_factor(),
            "recent_win_rate": round(self.get_recent_win_rate(), 3),
            "open_positions": self._state.open_positions,
            "max_concurrent_positions": settings.max_concurrent_positions,
            "execution_failures": self._state.execution_failures,
        }


_risk_manager: RiskManager | None = None


def get_risk_manager() -> RiskManager:
    global _risk_manager
    if _risk_manager is None:
        _risk_manager = RiskManager()
    return _risk_manager
