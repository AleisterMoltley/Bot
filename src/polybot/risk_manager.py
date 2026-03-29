"""Advanced Risk Manager for High-Frequency Trading.

Provides comprehensive risk controls including:
- Circuit breaker (consecutive losses + daily loss limits)
- Max daily trade limit
- Max drawdown stop
- Position sizing with Kelly criterion
- Liquidity filtering
- Balance and trade size warnings
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from polybot.config import get_settings
from polybot.logging_setup import get_logger

if TYPE_CHECKING:
    pass

log = get_logger(__name__)


@dataclass
class RiskState:
    """Current risk management state."""

    # Daily stats (reset at midnight UTC)
    daily_loss: float = 0.0
    daily_profit: float = 0.0
    daily_trades: int = 0
    last_reset_date: str = ""

    # Streak tracking
    consecutive_losses: int = 0
    consecutive_wins: int = 0

    # Circuit breaker
    is_paused: bool = False
    pause_reason: str = ""
    pause_time: float = 0.0

    # Max drawdown tracking
    peak_balance: float = 0.0
    current_drawdown: float = 0.0

    # Last update timestamp
    updated_at: float = field(default_factory=time.time)


class RiskManager:
    """Centralized risk management for high-frequency trading.

    Features:
    - Daily loss limit with circuit breaker
    - Consecutive loss circuit breaker
    - Max daily trades limit
    - Max drawdown stop
    - Liquidity filtering
    - Auto-reset at midnight UTC
    """

    def __init__(self) -> None:
        self._state = RiskState()
        self._ensure_daily_reset()

    def _ensure_daily_reset(self) -> None:
        """Reset daily counters if date has changed."""
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if self._state.last_reset_date != today:
            log.info(
                "Daily risk counters reset",
                previous_date=self._state.last_reset_date or "N/A",
                new_date=today,
            )
            self._state.daily_loss = 0.0
            self._state.daily_profit = 0.0
            self._state.daily_trades = 0
            self._state.last_reset_date = today
            # Don't reset consecutive losses - they span days
            # Reset circuit breaker if it was due to daily limits
            if self._state.pause_reason in (
                "daily_loss_limit",
                "daily_trade_limit",
            ):
                self._state.is_paused = False
                self._state.pause_reason = ""

    def check_can_trade(self) -> tuple[bool, str]:
        """Check if trading is allowed under current risk limits.

        Returns:
            (can_trade: bool, reason: str)
        """
        self._ensure_daily_reset()
        settings = get_settings()

        # Circuit breaker active?
        if self._state.is_paused:
            # Auto-unpause after cooldown (15 min for loss streak, 60 min for drawdown)
            cooldown = 900.0 if self._state.pause_reason == "consecutive_losses" else 3600.0
            elapsed = time.time() - self._state.pause_time
            if elapsed >= cooldown:
                log.info(
                    "Circuit breaker auto-reset after cooldown",
                    reason=self._state.pause_reason,
                    elapsed_min=f"{elapsed / 60:.1f}",
                )
                self._state.is_paused = False
                self._state.pause_reason = ""
                self._state.consecutive_losses = 0
            else:
                remaining = (cooldown - elapsed) / 60
                return False, f"circuit_breaker:{self._state.pause_reason} ({remaining:.0f}min remaining)"

        # Daily loss limit
        if self._state.daily_loss >= settings.max_daily_loss:
            self._trigger_circuit_breaker("daily_loss_limit")
            return False, f"daily_loss_limit:${self._state.daily_loss:.2f}>=${settings.max_daily_loss:.2f}"

        # Daily trade limit
        if self._state.daily_trades >= settings.max_daily_trades:
            return False, f"daily_trade_limit:{self._state.daily_trades}>={settings.max_daily_trades}"

        # Consecutive losses
        if self._state.consecutive_losses >= settings.circuit_breaker_consecutive_losses:
            self._trigger_circuit_breaker("consecutive_losses")
            return False, f"consecutive_losses:{self._state.consecutive_losses}>={settings.circuit_breaker_consecutive_losses}"

        return True, "OK"

    def check_liquidity(self, liquidity_usd: float) -> tuple[bool, str]:
        """Check if market has sufficient liquidity.

        Args:
            liquidity_usd: Market liquidity in USD

        Returns:
            (passes: bool, reason: str)
        """
        settings = get_settings()
        if liquidity_usd < settings.min_liquidity_usd:
            return False, f"low_liquidity:${liquidity_usd:.0f}<${settings.min_liquidity_usd:.0f}"
        return True, "OK"

    def record_trade(self, profit: float) -> RiskState:
        """Record a trade result and update risk state.

        Args:
            profit: Trade profit (positive) or loss (negative)

        Returns:
            Updated RiskState
        """
        self._ensure_daily_reset()

        self._state.daily_trades += 1
        self._state.updated_at = time.time()

        if profit >= 0:
            # Winning trade
            self._state.daily_profit += profit
            self._state.consecutive_wins += 1
            self._state.consecutive_losses = 0
            log.info(
                "Trade recorded: WIN",
                profit=f"${profit:.2f}",
                daily_profit=f"${self._state.daily_profit:.2f}",
                streak=f"+{self._state.consecutive_wins}",
            )
        else:
            # Losing trade
            loss = abs(profit)
            self._state.daily_loss += loss
            self._state.consecutive_losses += 1
            self._state.consecutive_wins = 0
            log.warning(
                "Trade recorded: LOSS",
                loss=f"${loss:.2f}",
                daily_loss=f"${self._state.daily_loss:.2f}",
                streak=f"-{self._state.consecutive_losses}",
            )

        # Check if this triggered circuit breaker
        self.check_can_trade()

        return self._state

    def update_balance(self, current_balance: float) -> None:
        """Update balance tracking for drawdown calculation.

        Args:
            current_balance: Current account balance in USD
        """
        if current_balance > self._state.peak_balance:
            self._state.peak_balance = current_balance
            self._state.current_drawdown = 0.0
        elif self._state.peak_balance > 0:
            self._state.current_drawdown = (
                (self._state.peak_balance - current_balance) / self._state.peak_balance
            ) * 100

        # Check max drawdown
        settings = get_settings()
        if self._state.current_drawdown >= settings.max_drawdown_pct:
            self._trigger_circuit_breaker("max_drawdown")
            log.warning(
                "Max drawdown reached",
                drawdown=f"{self._state.current_drawdown:.1f}%",
                max=f"{settings.max_drawdown_pct:.1f}%",
            )

        # Warn if balance is below minimum threshold
        if current_balance < settings.min_balance_usd:
            log.warning(
                "⚠️ Low balance warning",
                current_balance=f"${current_balance:.2f}",
                min_balance=f"${settings.min_balance_usd:.2f}",
            )

    def check_trade_size(self, trade_size_usd: float) -> tuple[bool, str]:
        """Check if trade size is acceptable.

        Args:
            trade_size_usd: Proposed trade size in USD

        Returns:
            (passes: bool, reason: str)
        """
        settings = get_settings()
        if trade_size_usd < settings.min_trade_size_usd_env:
            return False, f"trade_too_small:${trade_size_usd:.2f}<${settings.min_trade_size_usd_env:.2f}"
        return True, "OK"

    def _trigger_circuit_breaker(self, reason: str) -> None:
        """Trigger the circuit breaker."""
        if not self._state.is_paused:
            self._state.is_paused = True
            self._state.pause_reason = reason
            self._state.pause_time = time.time()
            log.warning(
                "Circuit breaker TRIGGERED",
                reason=reason,
                daily_loss=f"${self._state.daily_loss:.2f}",
                consecutive_losses=self._state.consecutive_losses,
                daily_trades=self._state.daily_trades,
            )

    def reset_circuit_breaker(self) -> None:
        """Manually reset the circuit breaker."""
        self._state.is_paused = False
        self._state.pause_reason = ""
        self._state.pause_time = 0.0
        self._state.consecutive_losses = 0
        log.info("Circuit breaker manually reset")

    def get_state(self) -> RiskState:
        """Get current risk state."""
        self._ensure_daily_reset()
        return self._state

    def get_status_dict(self) -> dict:
        """Get risk status as dictionary for API."""
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
            "trades_remaining": max(
                0, settings.max_daily_trades - self._state.daily_trades
            ),
            "consecutive_losses": self._state.consecutive_losses,
            "consecutive_wins": self._state.consecutive_wins,
            "max_consecutive_losses": settings.circuit_breaker_consecutive_losses,
            "current_drawdown_pct": round(self._state.current_drawdown, 2),
            "max_daily_loss": settings.max_daily_loss,
            "loss_remaining": round(
                max(0, settings.max_daily_loss - self._state.daily_loss), 2
            ),
        }


# Global singleton
_risk_manager: RiskManager | None = None


def get_risk_manager() -> RiskManager:
    """Get or create the global risk manager instance."""
    global _risk_manager
    if _risk_manager is None:
        _risk_manager = RiskManager()
    return _risk_manager
