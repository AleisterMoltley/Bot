"""PnL and Position Tracking for PolyBot.

Provides realistic profit/loss tracking including:
- Maker/taker fee calculation
- Partial fill handling
- NegRisk accrued funding/interest
- Cost basis tracking per position
- Realized and unrealized PnL calculation
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Any

from polybot.config import get_settings
from polybot.logging_setup import get_logger

log = get_logger(__name__)


class FeeType(str, Enum):
    """Fee types for Polymarket trades."""

    MAKER = "maker"  # Providing liquidity
    TAKER = "taker"  # Taking liquidity


class PositionStatus(str, Enum):
    """Position status."""

    OPEN = "open"
    CLOSED = "closed"
    PARTIAL = "partial"  # Partially closed


@dataclass
class FeeSchedule:
    """Fee schedule for Polymarket as of March 2026.

    Note: Fees have changed over time. Current structure:
    - Maker rebate: 0% (was positive, now neutral)
    - Taker fee: 0.1% of notional
    - NegRisk markets may have additional funding
    """

    maker_fee_bps: int = 0  # 0 bps maker fee (neutral)
    taker_fee_bps: int = 10  # 10 bps = 0.1% taker fee
    negrisk_funding_daily_bps: int = 2  # ~0.02% daily funding rate for NegRisk

    def calc_fee(self, notional: Decimal, fee_type: FeeType) -> Decimal:
        """Calculate fee for a trade.

        Args:
            notional: Trade notional value in USDC
            fee_type: MAKER or TAKER

        Returns:
            Fee amount in USDC (negative for rebates)
        """
        if fee_type == FeeType.MAKER:
            return notional * Decimal(self.maker_fee_bps) / Decimal(10000)
        return notional * Decimal(self.taker_fee_bps) / Decimal(10000)

    def calc_negrisk_funding(
        self, position_value: Decimal, days_held: float
    ) -> Decimal:
        """Calculate accrued funding for NegRisk position.

        Args:
            position_value: Current position value
            days_held: Fractional days held

        Returns:
            Accrued funding cost (positive = cost to holder)
        """
        daily_rate = Decimal(self.negrisk_funding_daily_bps) / Decimal(10000)
        return position_value * daily_rate * Decimal(str(days_held))


@dataclass
class Fill:
    """Represents a single fill (partial or complete) of an order."""

    fill_id: str
    order_id: str
    timestamp: datetime
    price: Decimal
    size: Decimal  # Number of shares
    side: str  # BUY or SELL
    fee_type: FeeType
    fee_amount: Decimal
    is_partial: bool = False
    remaining_size: Decimal | None = None

    @property
    def notional(self) -> Decimal:
        """Calculate notional value of fill."""
        return self.price * self.size

    @property
    def total_cost(self) -> Decimal:
        """Calculate total cost including fees."""
        if self.side == "BUY":
            return self.notional + self.fee_amount
        return (
            self.notional - self.fee_amount
        )  # Selling: you receive notional minus fee


@dataclass
class Position:
    """Represents an open or closed position."""

    position_id: str
    market_id: str
    token_id: str
    market_title: str
    outcome: str  # YES or NO
    opened_at: datetime
    is_negrisk: bool = False
    condition_id: str = ""

    # Position state
    size: Decimal = Decimal("0")
    avg_entry_price: Decimal = Decimal("0")
    cost_basis: Decimal = Decimal("0")
    total_fees_paid: Decimal = Decimal("0")
    accrued_funding: Decimal = Decimal("0")

    # Fill history
    fills: list[Fill] = field(default_factory=list)

    # Current state
    current_price: Decimal = Decimal("0")
    status: PositionStatus = PositionStatus.OPEN
    closed_at: datetime | None = None

    # Realized PnL (from partial/full closes)
    realized_pnl: Decimal = Decimal("0")

    @property
    def market_value(self) -> Decimal:
        """Current market value of position."""
        return self.size * self.current_price

    @property
    def unrealized_pnl(self) -> Decimal:
        """Unrealized profit/loss (excludes fees and funding)."""
        if self.size == 0:
            return Decimal("0")
        return self.market_value - (self.size * self.avg_entry_price)

    @property
    def unrealized_pnl_net(self) -> Decimal:
        """Unrealized PnL including fees and funding."""
        return self.unrealized_pnl - self.total_fees_paid - self.accrued_funding

    @property
    def total_pnl(self) -> Decimal:
        """Total PnL (realized + unrealized net)."""
        return self.realized_pnl + self.unrealized_pnl_net

    @property
    def pnl_pct(self) -> Decimal:
        """PnL as percentage of cost basis."""
        if self.cost_basis == 0:
            return Decimal("0")
        return (self.total_pnl / self.cost_basis) * Decimal("100")

    def add_fill(self, fill: Fill) -> None:
        """Add a fill to this position and update averages."""
        self.fills.append(fill)
        self.total_fees_paid += fill.fee_amount

        if fill.side == "BUY":
            # Buying: increase position
            if self.size == 0:
                self.avg_entry_price = fill.price
            else:
                # Weighted average
                old_notional = self.size * self.avg_entry_price
                new_notional = fill.size * fill.price
                self.avg_entry_price = (old_notional + new_notional) / (
                    self.size + fill.size
                )

            self.size += fill.size
            self.cost_basis += fill.total_cost

        else:  # SELL
            if fill.size >= self.size:
                # Full close
                realized = (
                    fill.notional - (fill.size * self.avg_entry_price) - fill.fee_amount
                )
                self.realized_pnl += realized
                self.size = Decimal("0")
                self.status = PositionStatus.CLOSED
                self.closed_at = fill.timestamp
            else:
                # Partial close
                portion = fill.size / self.size
                realized_cost_basis = self.cost_basis * portion
                realized = fill.notional - realized_cost_basis - fill.fee_amount
                self.realized_pnl += realized
                self.size -= fill.size
                self.cost_basis *= Decimal("1") - portion
                self.status = PositionStatus.PARTIAL

    def update_funding(self, current_time: datetime | None = None) -> Decimal:
        """Update accrued funding for NegRisk position.

        Returns:
            New accrued funding since last update
        """
        if not self.is_negrisk or self.size == 0:
            return Decimal("0")

        settings = get_settings()
        fee_schedule = FeeSchedule(
            negrisk_funding_daily_bps=getattr(settings, "negrisk_funding_daily_bps", 2)
        )

        now = current_time or datetime.now(timezone.utc)
        days_held = (now - self.opened_at).total_seconds() / 86400

        new_funding = fee_schedule.calc_negrisk_funding(self.market_value, days_held)
        delta = new_funding - self.accrued_funding
        self.accrued_funding = new_funding

        return delta


class PnLTracker:
    """Centralized PnL and position tracking.

    Tracks all positions across markets, calculates fees, handles partial fills,
    and computes accurate PnL including NegRisk funding.

    Usage:
        tracker = PnLTracker()
        fill = tracker.record_fill(...)
        pnl = tracker.get_total_pnl()
    """

    def __init__(self) -> None:
        self._positions: dict[str, Position] = {}  # token_id -> Position
        self._fee_schedule = FeeSchedule()
        self._fill_counter = 0

    def get_fee_schedule(self) -> FeeSchedule:
        """Get current fee schedule."""
        return self._fee_schedule

    def update_fee_schedule(
        self,
        maker_bps: int | None = None,
        taker_bps: int | None = None,
        negrisk_funding_bps: int | None = None,
    ) -> None:
        """Update fee schedule (e.g., from API)."""
        if maker_bps is not None:
            self._fee_schedule.maker_fee_bps = maker_bps
        if taker_bps is not None:
            self._fee_schedule.taker_fee_bps = taker_bps
        if negrisk_funding_bps is not None:
            self._fee_schedule.negrisk_funding_daily_bps = negrisk_funding_bps
        log.info(
            "Fee schedule updated",
            maker_bps=self._fee_schedule.maker_fee_bps,
            taker_bps=self._fee_schedule.taker_fee_bps,
            negrisk_funding_bps=self._fee_schedule.negrisk_funding_daily_bps,
        )

    def record_fill(
        self,
        order_id: str,
        token_id: str,
        market_id: str,
        market_title: str,
        outcome: str,
        side: str,
        price: float,
        size: float,
        fee_type: FeeType = FeeType.TAKER,
        is_partial: bool = False,
        remaining_size: float | None = None,
        is_negrisk: bool = False,
        condition_id: str = "",
        timestamp: datetime | None = None,
    ) -> Fill:
        """Record a fill and update position.

        Args:
            order_id: Exchange order ID
            token_id: Token being traded
            market_id: Market condition ID
            market_title: Human-readable market name
            outcome: YES or NO
            side: BUY or SELL
            price: Fill price (0-1 for binary markets)
            size: Number of shares filled
            fee_type: MAKER or TAKER
            is_partial: Whether this is a partial fill
            remaining_size: Remaining unfilled size (for partial fills)
            is_negrisk: Whether this is a NegRisk market
            condition_id: Condition ID for NegRisk markets
            timestamp: Fill timestamp (defaults to now)

        Returns:
            Fill object with computed fees
        """
        ts = timestamp or datetime.now(timezone.utc)
        price_d = Decimal(str(price))
        size_d = Decimal(str(size))

        # Calculate fee
        notional = price_d * size_d
        fee = self._fee_schedule.calc_fee(notional, fee_type)

        # Create fill
        self._fill_counter += 1
        fill = Fill(
            fill_id=f"fill_{self._fill_counter}_{int(time.time())}",
            order_id=order_id,
            timestamp=ts,
            price=price_d,
            size=size_d,
            side=side.upper(),
            fee_type=fee_type,
            fee_amount=fee,
            is_partial=is_partial,
            remaining_size=Decimal(str(remaining_size)) if remaining_size else None,
        )

        # Get or create position
        position = self._positions.get(token_id)
        if position is None:
            position = Position(
                position_id=f"pos_{token_id[:8]}_{int(time.time())}",
                market_id=market_id,
                token_id=token_id,
                market_title=market_title,
                outcome=outcome.upper(),
                opened_at=ts,
                is_negrisk=is_negrisk,
                condition_id=condition_id,
            )
            self._positions[token_id] = position

        # Add fill to position
        position.add_fill(fill)

        log.info(
            "Fill recorded",
            order_id=order_id[:12] if order_id else "none",
            side=side,
            price=f"{price:.4f}",
            size=f"{size:.2f}",
            fee=f"${float(fee):.4f}",
            is_partial=is_partial,
            position_size=f"{float(position.size):.2f}",
        )

        # Clean up closed positions (keep for history but mark)
        if position.status == PositionStatus.CLOSED:
            log.info(
                "Position closed",
                market=market_title[:40],
                realized_pnl=f"${float(position.realized_pnl):.2f}",
                total_fees=f"${float(position.total_fees_paid):.4f}",
            )

        return fill

    def update_price(self, token_id: str, current_price: float) -> None:
        """Update current price for a position."""
        if token_id in self._positions:
            self._positions[token_id].current_price = Decimal(str(current_price))

    def update_all_funding(self) -> Decimal:
        """Update funding for all NegRisk positions.

        Returns:
            Total new funding accrued since last update
        """
        total_delta = Decimal("0")
        for position in self._positions.values():
            if position.status == PositionStatus.OPEN and position.is_negrisk:
                delta = position.update_funding()
                total_delta += delta
        return total_delta

    def get_position(self, token_id: str) -> Position | None:
        """Get position for a token."""
        return self._positions.get(token_id)

    def get_all_positions(self, include_closed: bool = False) -> list[Position]:
        """Get all positions."""
        positions = list(self._positions.values())
        if not include_closed:
            positions = [p for p in positions if p.status != PositionStatus.CLOSED]
        return positions

    def get_total_pnl(self) -> dict[str, Decimal]:
        """Get total PnL summary across all positions.

        Returns:
            Dict with realized, unrealized, fees, funding, and net PnL
        """
        realized = Decimal("0")
        unrealized = Decimal("0")
        fees = Decimal("0")
        funding = Decimal("0")

        for position in self._positions.values():
            realized += position.realized_pnl
            if position.status != PositionStatus.CLOSED:
                unrealized += position.unrealized_pnl
            fees += position.total_fees_paid
            funding += position.accrued_funding

        net = realized + unrealized - fees - funding

        return {
            "realized_pnl": realized,
            "unrealized_pnl": unrealized,
            "total_fees": fees,
            "accrued_funding": funding,
            "net_pnl": net,
        }

    def get_pnl_summary(self) -> str:
        """Get formatted PnL summary string."""
        pnl = self.get_total_pnl()
        open_positions = [
            p for p in self._positions.values() if p.status != PositionStatus.CLOSED
        ]

        return (
            f"📊 **PnL Summary**\n"
            f"  Realized:    ${float(pnl['realized_pnl']):+.2f}\n"
            f"  Unrealized:  ${float(pnl['unrealized_pnl']):+.2f}\n"
            f"  Fees Paid:   ${float(pnl['total_fees']):.4f}\n"
            f"  Funding:     ${float(pnl['accrued_funding']):.4f}\n"
            f"  ─────────────────\n"
            f"  **Net PnL:   ${float(pnl['net_pnl']):+.2f}**\n"
            f"  Open Positions: {len(open_positions)}"
        )

    def to_dict(self) -> dict[str, Any]:
        """Serialize tracker state to dict for persistence."""
        return {
            "positions": {
                token_id: {
                    "position_id": p.position_id,
                    "market_id": p.market_id,
                    "token_id": p.token_id,
                    "market_title": p.market_title,
                    "outcome": p.outcome,
                    "opened_at": p.opened_at.isoformat(),
                    "is_negrisk": p.is_negrisk,
                    "condition_id": p.condition_id,
                    "size": str(p.size),
                    "avg_entry_price": str(p.avg_entry_price),
                    "cost_basis": str(p.cost_basis),
                    "total_fees_paid": str(p.total_fees_paid),
                    "accrued_funding": str(p.accrued_funding),
                    "current_price": str(p.current_price),
                    "status": p.status.value,
                    "closed_at": p.closed_at.isoformat() if p.closed_at else None,
                    "realized_pnl": str(p.realized_pnl),
                }
                for token_id, p in self._positions.items()
            },
            "fee_schedule": {
                "maker_fee_bps": self._fee_schedule.maker_fee_bps,
                "taker_fee_bps": self._fee_schedule.taker_fee_bps,
                "negrisk_funding_daily_bps": self._fee_schedule.negrisk_funding_daily_bps,
            },
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PnLTracker":
        """Restore tracker from serialized dict."""
        tracker = cls()

        # Restore fee schedule
        if "fee_schedule" in data:
            fs = data["fee_schedule"]
            tracker._fee_schedule = FeeSchedule(
                maker_fee_bps=fs.get("maker_fee_bps", 0),
                taker_fee_bps=fs.get("taker_fee_bps", 10),
                negrisk_funding_daily_bps=fs.get("negrisk_funding_daily_bps", 2),
            )

        # Restore positions
        for token_id, p_data in data.get("positions", {}).items():
            position = Position(
                position_id=p_data["position_id"],
                market_id=p_data["market_id"],
                token_id=p_data["token_id"],
                market_title=p_data["market_title"],
                outcome=p_data["outcome"],
                opened_at=datetime.fromisoformat(p_data["opened_at"]),
                is_negrisk=p_data.get("is_negrisk", False),
                condition_id=p_data.get("condition_id", ""),
                size=Decimal(p_data["size"]),
                avg_entry_price=Decimal(p_data["avg_entry_price"]),
                cost_basis=Decimal(p_data["cost_basis"]),
                total_fees_paid=Decimal(p_data["total_fees_paid"]),
                accrued_funding=Decimal(p_data.get("accrued_funding", "0")),
                current_price=Decimal(p_data.get("current_price", "0")),
                status=PositionStatus(p_data.get("status", "open")),
                realized_pnl=Decimal(p_data.get("realized_pnl", "0")),
            )
            if p_data.get("closed_at"):
                position.closed_at = datetime.fromisoformat(p_data["closed_at"])
            tracker._positions[token_id] = position

        return tracker


# Global singleton
_pnl_tracker: PnLTracker | None = None


def get_pnl_tracker() -> PnLTracker:
    """Get or create the global PnLTracker singleton."""
    global _pnl_tracker
    if _pnl_tracker is None:
        _pnl_tracker = PnLTracker()
    return _pnl_tracker


def init_pnl_tracker_from_db() -> PnLTracker:
    """Initialize PnL tracker from database state."""
    from polybot.database import load_config

    tracker = get_pnl_tracker()
    saved_state = load_config("pnl_tracker_state")
    if saved_state:
        try:
            restored = PnLTracker.from_dict(saved_state)
            global _pnl_tracker
            _pnl_tracker = restored
            log.info(
                "PnL tracker restored from DB",
                positions=len(restored._positions),
            )
            return restored
        except Exception as e:
            log.error("Failed to restore PnL tracker", error=str(e))

    return tracker


def save_pnl_tracker_to_db() -> None:
    """Save current PnL tracker state to database."""
    from polybot.database import save_config

    tracker = get_pnl_tracker()
    try:
        save_config("pnl_tracker_state", tracker.to_dict())
        log.debug("PnL tracker state saved")
    except Exception as e:
        log.error("Failed to save PnL tracker", error=str(e))


# Utility functions for common operations
def calc_trade_fee(notional: float, is_maker: bool = False) -> float:
    """Quick fee calculation for a trade.

    Args:
        notional: Trade notional value
        is_maker: True if maker order

    Returns:
        Fee amount in USDC
    """
    tracker = get_pnl_tracker()
    fee_type = FeeType.MAKER if is_maker else FeeType.TAKER
    return float(tracker.get_fee_schedule().calc_fee(Decimal(str(notional)), fee_type))


def estimate_arb_profit(
    yes_price: float,
    no_price: float,
    amount: float,
) -> dict[str, float]:
    """Estimate profit from YES+NO arbitrage including fees.

    Args:
        yes_price: YES token price
        no_price: NO token price
        amount: Total USDC to deploy

    Returns:
        Dict with gross profit, fees, and net profit
    """
    combined = yes_price + no_price
    if combined >= 1.0:
        return {"gross_profit": 0, "fees": 0, "net_profit": 0, "profit_pct": 0}

    # Split amount between YES and NO
    yes_amount = amount * (yes_price / combined)
    no_amount = amount * (no_price / combined)

    # Calculate shares acquired
    yes_shares = yes_amount / yes_price if yes_price > 0 else 0
    no_shares = no_amount / no_price if no_price > 0 else 0

    # Value at resolution (either YES or NO pays $1)
    # Min shares determines the guaranteed payout
    min_shares = min(yes_shares, no_shares)
    payout = min_shares * 1.0

    gross_profit = payout - amount

    # Fees: taker on both sides
    fees = calc_trade_fee(yes_amount) + calc_trade_fee(no_amount)

    net_profit = gross_profit - fees
    profit_pct = (net_profit / amount) * 100 if amount > 0 else 0

    return {
        "gross_profit": gross_profit,
        "fees": fees,
        "net_profit": net_profit,
        "profit_pct": profit_pct,
        "yes_amount": yes_amount,
        "no_amount": no_amount,
        "combined_price": combined,
    }
