"""SQLModel-based database models for PolyBot.

Type-safe ORM layer using SQLModel (Pydantic + SQLAlchemy).
Provides validated models for trades, positions, P&L tracking, and bot state.
"""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Optional

from pydantic import field_validator
from sqlmodel import Field, SQLModel


# =============================================================================
# Enums for type safety
# =============================================================================


class TradeSide(str, Enum):
    """Trade side enumeration."""

    BUY = "BUY"
    SELL = "SELL"


class TradeMode(str, Enum):
    """Trade mode enumeration."""

    SIGNAL = "signal"
    COPY = "copy"
    ARBITRAGE = "arbitrage"
    MANUAL = "manual"


class PositionStatus(str, Enum):
    """Position status enumeration."""

    OPEN = "OPEN"
    CLOSED = "CLOSED"
    LIQUIDATED = "LIQUIDATED"


class ArbStatus(str, Enum):
    """Arbitrage execution status."""

    PENDING = "PENDING"
    EXECUTED = "EXECUTED"
    FAILED = "FAILED"
    PARTIAL = "PARTIAL"


# =============================================================================
# Base timestamp mixin
# =============================================================================


def utc_now() -> datetime:
    """Return current UTC datetime."""
    return datetime.now(timezone.utc)


def utc_now_str() -> str:
    """Return current UTC datetime as ISO string."""
    return utc_now().isoformat()


# =============================================================================
# Trade Model
# =============================================================================


class TradeBase(SQLModel):
    """Base trade model with common fields."""

    mode: str = Field(
        default="signal", description="Trading mode: signal, copy, arbitrage"
    )
    market_id: Optional[str] = Field(default=None, description="Market identifier")
    market_title: Optional[str] = Field(
        default=None, description="Human-readable market title"
    )
    side: str = Field(description="BUY or SELL")
    outcome: Optional[str] = Field(default=None, description="YES or NO outcome")
    price: float = Field(ge=0, le=1, description="Trade price (0-1)")
    size: float = Field(ge=0, description="Position size")
    cost: float = Field(ge=0, description="Total cost in USD")
    order_id: Optional[str] = Field(default=None, description="Exchange order ID")
    strategy: Optional[str] = Field(default=None, description="Strategy name")
    confidence: Optional[float] = Field(
        default=None, ge=0, le=100, description="Signal confidence"
    )
    kelly_size: Optional[float] = Field(
        default=None, ge=0, description="Kelly-sized position"
    )
    profit: float = Field(default=0, description="Realized profit/loss")
    notes: Optional[str] = Field(default=None, description="Additional notes")

    @field_validator("side")
    @classmethod
    def validate_side(cls, v: str) -> str:
        """Ensure side is uppercase."""
        return v.upper()


class Trade(TradeBase, table=True):
    """Trade database model."""

    __tablename__ = "trades"

    id: Optional[int] = Field(default=None, primary_key=True)
    timestamp: str = Field(default_factory=utc_now_str, description="ISO timestamp")


class TradeCreate(TradeBase):
    """Trade creation model (no ID, auto-timestamp)."""

    pass


class TradeRead(TradeBase):
    """Trade read model (includes ID and timestamp)."""

    id: int
    timestamp: str


# =============================================================================
# Position Model
# =============================================================================


class PositionBase(SQLModel):
    """Base position model with common fields."""

    market_id: str = Field(description="Market identifier")
    market_title: Optional[str] = Field(
        default=None, description="Human-readable market title"
    )
    token_id: str = Field(description="Token identifier")
    condition_id: Optional[str] = Field(default=None, description="Condition ID")
    side: str = Field(description="YES or NO")
    outcome: Optional[str] = Field(default=None, description="Outcome prediction")
    size: float = Field(ge=0, description="Position size")
    avg_price: float = Field(ge=0, le=1, description="Average entry price")
    cost_basis: float = Field(ge=0, description="Total cost basis")
    current_price: float = Field(
        default=0, ge=0, le=1, description="Current market price"
    )
    unrealized_pnl: float = Field(default=0, description="Unrealized P&L")
    status: str = Field(default="OPEN", description="Position status")
    realized_pnl: float = Field(default=0, description="Realized P&L when closed")
    my_bought_size: float = Field(default=0, ge=0, description="Total bought size")


class Position(PositionBase, table=True):
    """Position database model."""

    __tablename__ = "positions"

    id: Optional[int] = Field(default=None, primary_key=True)
    opened_at: str = Field(
        default_factory=utc_now_str, description="Position open timestamp"
    )
    closed_at: Optional[str] = Field(
        default=None, description="Position close timestamp"
    )


class PositionCreate(PositionBase):
    """Position creation model."""

    pass


class PositionRead(PositionBase):
    """Position read model with all fields."""

    id: int
    opened_at: str
    closed_at: Optional[str]


# =============================================================================
# Daily P&L Model
# =============================================================================


class DailyPnLBase(SQLModel):
    """Base daily P&L model."""

    realized: float = Field(default=0, description="Realized P&L for the day")
    unrealized: float = Field(default=0, description="Unrealized P&L snapshot")
    total_trades: int = Field(default=0, ge=0, description="Total trades for the day")
    winning_trades: int = Field(default=0, ge=0, description="Winning trades count")
    losing_trades: int = Field(default=0, ge=0, description="Losing trades count")


class DailyPnL(DailyPnLBase, table=True):
    """Daily P&L database model."""

    __tablename__ = "daily_pnl"

    date: str = Field(primary_key=True, description="Date in YYYY-MM-DD format")

    @property
    def winrate(self) -> float:
        """Calculate win rate percentage."""
        if self.total_trades == 0:
            return 0.0
        return (self.winning_trades / self.total_trades) * 100


class DailyPnLCreate(DailyPnLBase):
    """Daily P&L creation model."""

    date: str


class DailyPnLRead(DailyPnLBase):
    """Daily P&L read model."""

    date: str

    @property
    def winrate(self) -> float:
        """Calculate win rate percentage."""
        if self.total_trades == 0:
            return 0.0
        return (self.winning_trades / self.total_trades) * 100


# =============================================================================
# Risk State Model
# =============================================================================


class RiskStateBase(SQLModel):
    """Base risk state model."""

    daily_loss: float = Field(default=0, ge=0, description="Cumulative daily loss")
    consecutive_losses: int = Field(
        default=0, ge=0, description="Consecutive losing trades"
    )
    is_paused: bool = Field(
        default=False, description="Trading paused due to risk limits"
    )
    last_reset_date: Optional[str] = Field(
        default=None, description="Last daily reset date"
    )


class RiskState(RiskStateBase, table=True):
    """Risk state database model (singleton)."""

    __tablename__ = "risk_state"

    id: int = Field(default=1, primary_key=True)
    updated_at: Optional[str] = Field(default=None, description="Last update timestamp")


class RiskStateRead(RiskStateBase):
    """Risk state read model."""

    id: int = 1
    updated_at: Optional[str]


# =============================================================================
# Backtest Results Model
# =============================================================================


class BacktestResultBase(SQLModel):
    """Base backtest result model."""

    crypto_id: Optional[str] = Field(default=None, description="Asset tested")
    data_points: Optional[int] = Field(
        default=None, ge=0, description="Number of data points"
    )
    total_trades: Optional[int] = Field(
        default=None, ge=0, description="Total trades executed"
    )
    winning_trades: Optional[int] = Field(
        default=None, ge=0, description="Winning trades"
    )
    losing_trades: Optional[int] = Field(
        default=None, ge=0, description="Losing trades"
    )
    winrate: Optional[float] = Field(
        default=None, ge=0, le=100, description="Win rate %"
    )
    initial_balance: Optional[float] = Field(
        default=None, ge=0, description="Starting balance"
    )
    final_balance: Optional[float] = Field(default=None, description="Ending balance")
    net_profit: Optional[float] = Field(default=None, description="Net profit/loss")
    max_drawdown: Optional[float] = Field(
        default=None, ge=0, le=100, description="Max drawdown %"
    )
    sharpe_ratio: Optional[float] = Field(default=None, description="Sharpe ratio")
    profit_factor: Optional[float] = Field(
        default=None, ge=0, description="Profit factor"
    )
    optimized_params: Optional[str] = Field(
        default=None, description="JSON optimized params"
    )
    is_walk_forward: bool = Field(
        default=False, description="Walk-forward optimization result"
    )


class BacktestResult(BacktestResultBase, table=True):
    """Backtest result database model."""

    __tablename__ = "backtest_results"

    id: Optional[int] = Field(default=None, primary_key=True)
    run_timestamp: str = Field(default_factory=utc_now_str, description="Run timestamp")


class BacktestResultCreate(BacktestResultBase):
    """Backtest result creation model."""

    pass


class BacktestResultRead(BacktestResultBase):
    """Backtest result read model."""

    id: int
    run_timestamp: str


# =============================================================================
# Arbitrage Execution Model
# =============================================================================


class ArbExecutionBase(SQLModel):
    """Base arbitrage execution model."""

    market: str = Field(description="Market identifier")
    yes_price: Optional[float] = Field(
        default=None, ge=0, le=1, description="YES price"
    )
    no_price: Optional[float] = Field(default=None, ge=0, le=1, description="NO price")
    combined: Optional[float] = Field(
        default=None, description="Combined price (should be < 1)"
    )
    profit: Optional[float] = Field(default=None, description="Expected profit")
    yes_order_id: Optional[str] = Field(default=None, description="YES order ID")
    no_order_id: Optional[str] = Field(default=None, description="NO order ID")
    status: str = Field(default="PENDING", description="Execution status")
    total_cost: float = Field(default=0, ge=0, description="Total cost")
    expected_profit: float = Field(default=0, description="Expected profit")


class ArbExecution(ArbExecutionBase, table=True):
    """Arbitrage execution database model."""

    __tablename__ = "arb_executions"

    id: Optional[int] = Field(default=None, primary_key=True)
    timestamp: str = Field(
        default_factory=utc_now_str, description="Execution timestamp"
    )


class ArbExecutionCreate(ArbExecutionBase):
    """Arbitrage execution creation model."""

    pass


class ArbExecutionRead(ArbExecutionBase):
    """Arbitrage execution read model."""

    id: int
    timestamp: str


# =============================================================================
# Copy Trade Model
# =============================================================================


class CopyTradeBase(SQLModel):
    """Base copy trade model."""

    trader_address: str = Field(description="Address of trader being copied")
    market_id: Optional[str] = Field(default=None, description="Market identifier")
    market_title: Optional[str] = Field(default=None, description="Market title")
    side: str = Field(description="BUY or SELL")
    trader_size: Optional[float] = Field(
        default=None, ge=0, description="Trader's position size"
    )
    trader_price: Optional[float] = Field(
        default=None, ge=0, le=1, description="Trader's price"
    )
    my_size: Optional[float] = Field(
        default=None, ge=0, description="Our position size"
    )
    my_price: Optional[float] = Field(
        default=None, ge=0, le=1, description="Our execution price"
    )
    multiplier_used: Optional[float] = Field(
        default=None, ge=0, description="Size multiplier used"
    )
    tier_used: Optional[str] = Field(default=None, description="Trader tier")
    order_id: Optional[str] = Field(default=None, description="Our order ID")
    status: str = Field(default="EXECUTED", description="Execution status")
    profit: float = Field(default=0, description="Realized profit")


class CopyTrade(CopyTradeBase, table=True):
    """Copy trade database model."""

    __tablename__ = "copy_trades"

    id: Optional[int] = Field(default=None, primary_key=True)
    timestamp: str = Field(default_factory=utc_now_str, description="Copy timestamp")


class CopyTradeCreate(CopyTradeBase):
    """Copy trade creation model."""

    pass


class CopyTradeRead(CopyTradeBase):
    """Copy trade read model."""

    id: int
    timestamp: str


# =============================================================================
# Scanner Alert Model
# =============================================================================


class ScannerAlertBase(SQLModel):
    """Base scanner alert model."""

    market: str = Field(description="Market identifier")
    yes_ask: Optional[float] = Field(
        default=None, ge=0, le=1, description="YES ask price"
    )
    no_ask: Optional[float] = Field(
        default=None, ge=0, le=1, description="NO ask price"
    )
    combined: Optional[float] = Field(default=None, description="Combined price")
    profit: Optional[float] = Field(default=None, description="Potential profit %")
    platform: str = Field(default="polymarket", description="Trading platform")
    days_until_resolution: Optional[float] = Field(
        default=None, ge=0, description="Days to resolve"
    )


class ScannerAlert(ScannerAlertBase, table=True):
    """Scanner alert database model."""

    __tablename__ = "scanner_alerts"

    id: Optional[int] = Field(default=None, primary_key=True)
    timestamp: str = Field(default_factory=utc_now_str, description="Alert timestamp")


class ScannerAlertCreate(ScannerAlertBase):
    """Scanner alert creation model."""

    pass


class ScannerAlertRead(ScannerAlertBase):
    """Scanner alert read model."""

    id: int
    timestamp: str


# =============================================================================
# Bot Config Model
# =============================================================================


class BotConfig(SQLModel, table=True):
    """Bot configuration key-value store."""

    __tablename__ = "bot_config"

    key: str = Field(primary_key=True, description="Config key")
    value: str = Field(description="Config value (JSON or string)")
    updated_at: Optional[str] = Field(default=None, description="Last update timestamp")


class BotConfigRead(SQLModel):
    """Bot config read model."""

    key: str
    value: str
    updated_at: Optional[str]
