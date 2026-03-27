"""Aggressive compounding module for maximizing portfolio growth.

Implements aggressive reinvestment strategies with Full Kelly criterion
for high-EV opportunities in Up/Down crypto markets.

Strategy:
- 68% of profits reinvested immediately
- Full Kelly sizing for >12% EV opportunities
- Compounding enabled/disabled via state
"""

from __future__ import annotations

from polybot.config import get_settings
from polybot.logging_setup import get_logger

log = get_logger(__name__)

# Full Kelly EV threshold
FULL_KELLY_EV_THRESHOLD = 0.12  # 12% minimum EV for full Kelly

# Reinvestment percentage
REINVEST_PERCENTAGE = 0.68  # 68% of profits reinvested


class AggressiveCompounder:
    """Aggressive compounding manager for maximizing growth.

    Reinvests 68% of profits immediately and uses Full Kelly
    for high-EV opportunities (>12% EV).
    """

    def __init__(
        self,
        reinvest_pct: float = REINVEST_PERCENTAGE,
        full_kelly_ev_threshold: float = FULL_KELLY_EV_THRESHOLD,
    ):
        """Initialize the AggressiveCompounder.

        Args:
            reinvest_pct: Percentage of profits to reinvest (0.0 to 1.0)
            full_kelly_ev_threshold: Minimum EV to use Full Kelly (0.0 to 1.0)
        """
        self.reinvest_pct = reinvest_pct
        self.full_kelly_ev_threshold = full_kelly_ev_threshold
        self.enabled = False
        self.total_compounded = 0.0
        self.compound_count = 0

    def enable(self) -> None:
        """Enable aggressive compounding."""
        self.enabled = True
        log.info(
            "Aggressive compounding ENABLED",
            reinvest_pct=f"{self.reinvest_pct:.0%}",
            full_kelly_threshold=f"{self.full_kelly_ev_threshold:.0%}",
        )

    def disable(self) -> None:
        """Disable aggressive compounding."""
        self.enabled = False
        log.info("Aggressive compounding DISABLED")

    def compound(self, pnl: float, current_balance: float) -> float:
        """Apply compounding to the current balance.

        Reinvests a percentage of profits back into the trading balance.

        Args:
            pnl: Profit/Loss from the trade (positive = profit, negative = loss)
            current_balance: Current trading balance

        Returns:
            New balance after compounding
        """
        if not self.enabled:
            return current_balance + pnl

        if pnl > 0:
            # Reinvest percentage of profits
            reinvest = pnl * self.reinvest_pct
            new_balance = current_balance + reinvest

            self.total_compounded += reinvest
            self.compound_count += 1

            log.info(
                "Compounding applied",
                pnl=f"${pnl:.2f}",
                reinvest=f"${reinvest:.2f}",
                new_balance=f"${new_balance:.2f}",
                total_compounded=f"${self.total_compounded:.2f}",
            )
            return new_balance

        # For losses, just add the PnL (negative) to balance
        return current_balance + pnl

    def calculate_position_size(
        self,
        ev: float,
        edge: float,
        bankroll: float,
        liquidity: float,
    ) -> float:
        """Calculate position size with Full Kelly for high-EV opportunities.

        Uses Full Kelly for trades with >12% EV, with max sizing
        capped at 35% of available liquidity.

        Args:
            ev: Expected Value of the opportunity
            edge: Edge over market
            bankroll: Current bankroll in USD
            liquidity: Market liquidity in USD

        Returns:
            Position size in USD
        """
        if not self.enabled:
            # Fall back to standard half-Kelly
            from polybot.risk import calculate_position_size

            return calculate_position_size(ev, edge, bankroll)

        # Skip low-EV opportunities
        if ev < self.full_kelly_ev_threshold:
            log.debug(
                "EV below Full Kelly threshold",
                ev=f"{ev:.2%}",
                threshold=f"{self.full_kelly_ev_threshold:.0%}",
            )
            return 0.0

        # Full Kelly calculation: f* = edge / variance
        # For binary outcomes, variance ≈ 0.5
        kelly_fraction = min(1.0, edge / 0.5)

        # Calculate position in USD
        position_usd = bankroll * kelly_fraction

        # Cap at 35% of liquidity to avoid market impact
        max_liquidity_size = liquidity * 0.35
        position_usd = min(position_usd, max_liquidity_size)

        # Apply settings constraints
        settings = get_settings()
        if position_usd < settings.kelly_min_trade_usd:
            return 0.0
        position_usd = min(position_usd, settings.max_order_size_usd)

        log.info(
            "Full Kelly position calculated",
            ev=f"{ev:.2%}",
            kelly_fraction=f"{kelly_fraction:.2%}",
            position=f"${position_usd:.2f}",
            max_liquidity=f"${max_liquidity_size:.2f}",
        )

        return position_usd

    def get_status(self) -> dict:
        """Get compounding status for display.

        Returns:
            Dict with compounding statistics
        """
        return {
            "enabled": self.enabled,
            "reinvest_pct": self.reinvest_pct,
            "full_kelly_threshold": self.full_kelly_ev_threshold,
            "total_compounded": self.total_compounded,
            "compound_count": self.compound_count,
        }


# Global compounder instance
_compounder: AggressiveCompounder | None = None


def get_compounder() -> AggressiveCompounder:
    """Get or create the global compounder instance."""
    global _compounder
    if _compounder is None:
        _compounder = AggressiveCompounder()
    return _compounder


def enable_compounding() -> AggressiveCompounder:
    """Enable aggressive compounding globally."""
    compounder = get_compounder()
    compounder.enable()
    return compounder


def disable_compounding() -> AggressiveCompounder:
    """Disable aggressive compounding globally."""
    compounder = get_compounder()
    compounder.disable()
    return compounder


class PyramidCompounder:
    """Conservative pyramid compounding with streak multipliers.

    Features:
    - 30% profit reinvestment (conservative approach)
    - 2x multiplier after 3+ win streak
    - 1.5x multiplier for shorter streaks
    - Designed for steady bankroll growth with reduced risk

    Note: This conservative 30% reinvestment reduces volatility
    while still benefiting from streak multipliers.
    """

    def __init__(self, reinvest_pct: float = 0.30):
        """Initialize the PyramidCompounder.

        Args:
            reinvest_pct: Percentage of profits to reinvest (default: 68%)
        """
        self.reinvest_pct = reinvest_pct
        self.enabled = False
        self.win_streak = 0
        self.total_compounded = 0.0
        self.compound_count = 0

    def enable(self) -> None:
        """Enable pyramid compounding."""
        self.enabled = True
        log.info(
            "Pyramid compounding ENABLED",
            reinvest_pct=f"{self.reinvest_pct:.0%}",
            streak_2x_threshold=3,
        )

    def disable(self) -> None:
        """Disable pyramid compounding."""
        self.enabled = False
        log.info("Pyramid compounding DISABLED")

    def record_result(self, is_win: bool) -> int:
        """Record a trade result and update streak.

        Args:
            is_win: True if trade was profitable

        Returns:
            Current win streak
        """
        if is_win:
            self.win_streak += 1
        else:
            self.win_streak = 0
        return self.win_streak

    def compound(self, pnl: float, balance: float) -> float:
        """Apply pyramid compounding to the balance.

        Uses streak multipliers for aggressive growth:
        - 2.0x multiplier for 3+ win streak
        - 1.5x multiplier for shorter streaks

        Args:
            pnl: Profit/Loss from the trade
            balance: Current trading balance

        Returns:
            New balance after compounding
        """
        if not self.enabled:
            return balance + pnl

        if pnl <= 0:
            # Loss - reset streak and just add PnL
            self.win_streak = 0
            return balance + pnl

        # Record win first, then calculate multiplier based on new streak
        self.win_streak += 1

        # Calculate reinvestment with streak multiplier
        reinvest = pnl * self.reinvest_pct
        streak_multiplier = 2.0 if self.win_streak >= 3 else 1.5
        new_balance = balance + (reinvest * streak_multiplier)

        self.total_compounded += reinvest * streak_multiplier
        self.compound_count += 1

        log.info(
            "Pyramid compounding applied",
            pnl=f"${pnl:.2f}",
            reinvest=f"${reinvest:.2f}",
            streak=self.win_streak,
            multiplier=f"{streak_multiplier:.1f}x",
            new_balance=f"${new_balance:.2f}",
        )

        return new_balance

    def get_status(self) -> dict:
        """Get compounding status for display.

        Returns:
            Dict with compounding statistics
        """
        return {
            "enabled": self.enabled,
            "reinvest_pct": self.reinvest_pct,
            "win_streak": self.win_streak,
            "current_multiplier": 2.0 if self.win_streak >= 3 else 1.5,
            "total_compounded": self.total_compounded,
            "compound_count": self.compound_count,
        }


# Global pyramid compounder instance
_pyramid_compounder: PyramidCompounder | None = None


def get_pyramid_compounder() -> PyramidCompounder:
    """Get or create the global pyramid compounder instance."""
    global _pyramid_compounder
    if _pyramid_compounder is None:
        _pyramid_compounder = PyramidCompounder()
    return _pyramid_compounder


def enable_pyramid_compounding() -> PyramidCompounder:
    """Enable pyramid compounding globally."""
    compounder = get_pyramid_compounder()
    compounder.enable()
    return compounder


def disable_pyramid_compounding() -> PyramidCompounder:
    """Disable pyramid compounding globally."""
    compounder = get_pyramid_compounder()
    compounder.disable()
    return compounder
