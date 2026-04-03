"""SQLite database layer for PolyBot.

Stores trades, positions, P&L, backtest results, and bot state.
Uses SQLModel for type-safe ORM with Pydantic validation.
Also provides raw SQLite access for backward compatibility.
"""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, TypeVar

from sqlmodel import Session, SQLModel, create_engine, select

from polybot.config import get_settings
from polybot.models import (
    ArbExecution,
    ArbExecutionCreate,
    ArbExecutionRead,
    BacktestResult,
    BacktestResultCreate,
    BacktestResultRead,
    CopyTrade,
    CopyTradeCreate,
    CopyTradeRead,
    DailyPnL,
    Position,
    PositionCreate,
    PositionRead,
    RiskState,
    RiskStateRead,
    ScannerAlert,
    ScannerAlertCreate,
    ScannerAlertRead,
    Trade,
    TradeCreate,
    TradeRead,
)

_DB_PATH: Path | None = None
_ENGINE = None

T = TypeVar("T", bound=SQLModel)


def _get_db_path() -> Path:
    """Get database file path."""
    global _DB_PATH
    if _DB_PATH is None:
        _DB_PATH = Path(get_settings().db_path)
    return _DB_PATH


def _get_engine():
    """Get or create SQLModel engine."""
    global _ENGINE
    if _ENGINE is None:
        db_path = _get_db_path()
        db_path.parent.mkdir(parents=True, exist_ok=True)
        _ENGINE = create_engine(f"sqlite:///{db_path}", echo=False)
    return _ENGINE


# =============================================================================
# Raw SQL Schema (for backward compatibility and explicit table creation)
# =============================================================================

SCHEMA = """
CREATE TABLE IF NOT EXISTS trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    mode TEXT NOT NULL DEFAULT 'signal',
    market_id TEXT,
    market_title TEXT,
    side TEXT NOT NULL,
    outcome TEXT,
    price REAL NOT NULL,
    size REAL NOT NULL,
    cost REAL NOT NULL,
    order_id TEXT,
    strategy TEXT,
    confidence REAL,
    kelly_size REAL,
    profit REAL DEFAULT 0,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS positions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    opened_at TEXT NOT NULL,
    market_id TEXT NOT NULL,
    market_title TEXT,
    token_id TEXT NOT NULL,
    condition_id TEXT,
    side TEXT NOT NULL,
    outcome TEXT,
    size REAL NOT NULL,
    avg_price REAL NOT NULL,
    cost_basis REAL NOT NULL,
    current_price REAL DEFAULT 0,
    unrealized_pnl REAL DEFAULT 0,
    status TEXT DEFAULT 'OPEN',
    closed_at TEXT,
    realized_pnl REAL DEFAULT 0,
    my_bought_size REAL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS daily_pnl (
    date TEXT PRIMARY KEY,
    realized REAL DEFAULT 0,
    unrealized REAL DEFAULT 0,
    total_trades INTEGER DEFAULT 0,
    winning_trades INTEGER DEFAULT 0,
    losing_trades INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS backtest_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_timestamp TEXT NOT NULL,
    crypto_id TEXT,
    data_points INTEGER,
    total_trades INTEGER,
    winning_trades INTEGER,
    losing_trades INTEGER,
    winrate REAL,
    initial_balance REAL,
    final_balance REAL,
    net_profit REAL,
    max_drawdown REAL,
    sharpe_ratio REAL,
    profit_factor REAL,
    optimized_params TEXT,
    is_walk_forward INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS arb_executions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    market TEXT NOT NULL,
    yes_price REAL,
    no_price REAL,
    combined REAL,
    profit REAL,
    yes_order_id TEXT,
    no_order_id TEXT,
    status TEXT DEFAULT 'PENDING',
    total_cost REAL DEFAULT 0,
    expected_profit REAL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS copy_trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    trader_address TEXT NOT NULL,
    market_id TEXT,
    market_title TEXT,
    side TEXT NOT NULL,
    trader_size REAL,
    trader_price REAL,
    my_size REAL,
    my_price REAL,
    multiplier_used REAL,
    tier_used TEXT,
    order_id TEXT,
    status TEXT DEFAULT 'EXECUTED',
    profit REAL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS risk_state (
    id INTEGER PRIMARY KEY DEFAULT 1,
    daily_loss REAL DEFAULT 0,
    consecutive_losses INTEGER DEFAULT 0,
    is_paused INTEGER DEFAULT 0,
    last_reset_date TEXT,
    updated_at TEXT
);

CREATE TABLE IF NOT EXISTS scanner_alerts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    market TEXT NOT NULL,
    yes_ask REAL,
    no_ask REAL,
    combined REAL,
    profit REAL,
    platform TEXT DEFAULT 'polymarket',
    days_until_resolution REAL
);

CREATE TABLE IF NOT EXISTS bot_config (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT
);

CREATE TABLE IF NOT EXISTS piggybank_transfers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    profit_usd REAL NOT NULL,
    amount_usd REAL NOT NULL,
    tx_hash TEXT,
    status TEXT DEFAULT 'ok'
);
"""


# ── Persistent connection singleton (avoids open/close overhead) ──
_PERSISTENT_CONN: sqlite3.Connection | None = None


def _get_persistent_conn() -> sqlite3.Connection:
    """Get or create a persistent SQLite connection with WAL mode."""
    global _PERSISTENT_CONN
    if _PERSISTENT_CONN is None:
        db_path = _get_db_path()
        db_path.parent.mkdir(parents=True, exist_ok=True)
        _PERSISTENT_CONN = sqlite3.connect(
            str(db_path), check_same_thread=False
        )
        _PERSISTENT_CONN.row_factory = sqlite3.Row
        _PERSISTENT_CONN.execute("PRAGMA journal_mode=WAL")
        _PERSISTENT_CONN.execute("PRAGMA busy_timeout=5000")
        _PERSISTENT_CONN.execute("PRAGMA synchronous=NORMAL")
        _PERSISTENT_CONN.execute("PRAGMA cache_size=-65536")  # 64MB
        _PERSISTENT_CONN.execute("PRAGMA temp_store=MEMORY")
    return _PERSISTENT_CONN


def init_db() -> None:
    """Initialize database with schema."""
    conn = _get_persistent_conn()
    conn.executescript(SCHEMA)
    _ensure_indexes()


@contextmanager
def get_db():
    """Get a database connection context manager.

    Uses persistent connection with WAL mode for concurrent access.
    """
    conn = _get_persistent_conn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def record_trade(
    *,
    mode: str,
    side: str,
    price: float,
    size: float,
    cost: float,
    market_id: str = "",
    market_title: str = "",
    outcome: str = "",
    order_id: str = "",
    strategy: str = "",
    confidence: float = 0,
    kelly_size: float = 0,
    profit: float = 0,
    notes: str = "",
) -> int:
    """Record a trade and return its ID."""
    ts = datetime.now(timezone.utc).isoformat()
    with get_db() as conn:
        cursor = conn.execute(
            """INSERT INTO trades (
                timestamp, mode, market_id, market_title, side, outcome,
                price, size, cost, order_id, strategy, confidence,
                kelly_size, profit, notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                ts,
                mode,
                market_id,
                market_title,
                side,
                outcome,
                price,
                size,
                cost,
                order_id,
                strategy,
                confidence,
                kelly_size,
                profit,
                notes,
            ),
        )
        return cursor.lastrowid


def get_risk_state() -> dict:
    """Get current risk management state."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    with get_db() as conn:
        row = conn.execute("SELECT * FROM risk_state WHERE id = 1").fetchone()
        if not row:
            conn.execute(
                "INSERT INTO risk_state (id, daily_loss, consecutive_losses, "
                "is_paused, last_reset_date, updated_at) VALUES (1, 0, 0, 0, ?, ?)",
                (today, datetime.now(timezone.utc).isoformat()),
            )
            return {"daily_loss": 0, "consecutive_losses": 0, "is_paused": False}

        state = dict(row)
        # Reset daily loss if new day
        if state.get("last_reset_date") != today:
            conn.execute(
                "UPDATE risk_state SET daily_loss = 0, last_reset_date = ?, "
                "updated_at = ? WHERE id = 1",
                (today, datetime.now(timezone.utc).isoformat()),
            )
            state["daily_loss"] = 0
        state["is_paused"] = bool(state.get("is_paused", 0))
        return state


def update_risk_state(*, daily_loss_delta: float = 0, is_loss: bool = False) -> dict:
    """Update risk state after a trade."""
    settings = get_settings()
    state = get_risk_state()
    now = datetime.now(timezone.utc).isoformat()

    new_daily_loss = state["daily_loss"] + daily_loss_delta
    if is_loss:
        new_consecutive = state["consecutive_losses"] + 1
    else:
        new_consecutive = 0

    is_paused = (
        new_daily_loss >= settings.max_daily_loss
        or new_consecutive >= settings.circuit_breaker_consecutive_losses
    )

    with get_db() as conn:
        conn.execute(
            "UPDATE risk_state SET daily_loss = ?, consecutive_losses = ?, "
            "is_paused = ?, updated_at = ? WHERE id = 1",
            (new_daily_loss, new_consecutive, int(is_paused), now),
        )

    return {
        "daily_loss": new_daily_loss,
        "consecutive_losses": new_consecutive,
        "is_paused": is_paused,
    }


def save_config(key: str, value: Any) -> None:
    """Save a configuration value."""
    now = datetime.now(timezone.utc).isoformat()
    val_str = json.dumps(value) if not isinstance(value, str) else value
    with get_db() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO bot_config (key, value, updated_at) VALUES (?, ?, ?)",
            (key, val_str, now),
        )


def load_config(key: str, default: Any = None) -> Any:
    """Load a configuration value."""
    with get_db() as conn:
        row = conn.execute(
            "SELECT value FROM bot_config WHERE key = ?", (key,)
        ).fetchone()
        if row:
            try:
                return json.loads(row["value"])
            except (json.JSONDecodeError, TypeError):
                return row["value"]
        return default


# =============================================================================
# SQLModel Session Management
# =============================================================================


@contextmanager
def get_session():
    """Get a SQLModel session context manager."""
    engine = _get_engine()
    with Session(engine) as session:
        yield session


def init_sqlmodel_db() -> None:
    """Initialize database using SQLModel (creates tables from models)."""
    engine = _get_engine()
    SQLModel.metadata.create_all(engine)


# =============================================================================
# Type-Safe CRUD Operations using SQLModel
# =============================================================================


def create_trade(trade_data: TradeCreate) -> TradeRead:
    """Create a new trade with type validation."""
    with get_session() as session:
        trade = Trade.model_validate(trade_data)
        session.add(trade)
        session.commit()
        session.refresh(trade)
        return TradeRead.model_validate(trade)


def get_trades(limit: int = 100, mode: str | None = None) -> list[TradeRead]:
    """Get trades with optional filtering."""
    with get_session() as session:
        statement = select(Trade).order_by(Trade.id.desc()).limit(limit)
        if mode:
            statement = statement.where(Trade.mode == mode)
        trades = session.exec(statement).all()
        return [TradeRead.model_validate(t) for t in trades]


def get_trade_by_id(trade_id: int) -> TradeRead | None:
    """Get a specific trade by ID."""
    with get_session() as session:
        trade = session.get(Trade, trade_id)
        return TradeRead.model_validate(trade) if trade else None


def create_position(position_data: PositionCreate) -> PositionRead:
    """Create a new position with type validation."""
    with get_session() as session:
        position = Position.model_validate(position_data)
        session.add(position)
        session.commit()
        session.refresh(position)
        return PositionRead.model_validate(position)


def get_positions(status: str = "OPEN") -> list[PositionRead]:
    """Get positions by status."""
    with get_session() as session:
        statement = (
            select(Position)
            .where(Position.status == status)
            .order_by(Position.id.desc())
        )
        positions = session.exec(statement).all()
        return [PositionRead.model_validate(p) for p in positions]


def close_position(position_id: int, realized_pnl: float) -> PositionRead | None:
    """Close a position and record realized P&L."""
    with get_session() as session:
        position = session.get(Position, position_id)
        if not position:
            return None
        position.status = "CLOSED"
        position.closed_at = datetime.now(timezone.utc).isoformat()
        position.realized_pnl = realized_pnl
        session.commit()
        session.refresh(position)
        return PositionRead.model_validate(position)


def get_or_create_daily_pnl(date: str | None = None) -> DailyPnL:
    """Get or create daily P&L record for a date."""
    if date is None:
        date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    with get_session() as session:
        daily = session.get(DailyPnL, date)
        if not daily:
            daily = DailyPnL(date=date)
            session.add(daily)
            session.commit()
            session.refresh(daily)
        return daily


def update_daily_pnl(
    date: str | None = None,
    *,
    realized_delta: float = 0,
    unrealized: float | None = None,
    is_win: bool | None = None,
) -> DailyPnL:
    """Update daily P&L with trade results."""
    if date is None:
        date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    with get_session() as session:
        daily = session.get(DailyPnL, date)
        if not daily:
            daily = DailyPnL(date=date)
            session.add(daily)

        daily.realized += realized_delta
        if unrealized is not None:
            daily.unrealized = unrealized

        if is_win is not None:
            daily.total_trades += 1
            if is_win:
                daily.winning_trades += 1
            else:
                daily.losing_trades += 1

        session.commit()
        session.refresh(daily)
        return daily


def get_daily_pnl_history(days: int = 30) -> list[DailyPnL]:
    """Get recent daily P&L history."""
    with get_session() as session:
        statement = select(DailyPnL).order_by(DailyPnL.date.desc()).limit(days)
        return list(session.exec(statement).all())


def create_arb_execution(arb_data: ArbExecutionCreate) -> ArbExecutionRead:
    """Create an arbitrage execution record."""
    with get_session() as session:
        arb = ArbExecution.model_validate(arb_data)
        session.add(arb)
        session.commit()
        session.refresh(arb)
        return ArbExecutionRead.model_validate(arb)


def update_arb_execution(
    arb_id: int, status: str, profit: float | None = None
) -> ArbExecutionRead | None:
    """Update arbitrage execution status."""
    with get_session() as session:
        arb = session.get(ArbExecution, arb_id)
        if not arb:
            return None
        arb.status = status
        if profit is not None:
            arb.profit = profit
        session.commit()
        session.refresh(arb)
        return ArbExecutionRead.model_validate(arb)


def create_copy_trade(copy_data: CopyTradeCreate) -> CopyTradeRead:
    """Create a copy trade record."""
    with get_session() as session:
        copy_trade = CopyTrade.model_validate(copy_data)
        session.add(copy_trade)
        session.commit()
        session.refresh(copy_trade)
        return CopyTradeRead.model_validate(copy_trade)


def create_scanner_alert(alert_data: ScannerAlertCreate) -> ScannerAlertRead:
    """Create a scanner alert record."""
    with get_session() as session:
        alert = ScannerAlert.model_validate(alert_data)
        session.add(alert)
        session.commit()
        session.refresh(alert)
        return ScannerAlertRead.model_validate(alert)


def create_backtest_result(result_data: BacktestResultCreate) -> BacktestResultRead:
    """Create a backtest result record."""
    with get_session() as session:
        result = BacktestResult.model_validate(result_data)
        session.add(result)
        session.commit()
        session.refresh(result)
        return BacktestResultRead.model_validate(result)


def get_sqlmodel_risk_state() -> RiskStateRead:
    """Get risk state using SQLModel."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    with get_session() as session:
        risk = session.get(RiskState, 1)
        if not risk:
            risk = RiskState(
                id=1,
                daily_loss=0,
                consecutive_losses=0,
                is_paused=False,
                last_reset_date=today,
                updated_at=datetime.now(timezone.utc).isoformat(),
            )
            session.add(risk)
            session.commit()
            session.refresh(risk)

        # Reset daily loss if new day
        if risk.last_reset_date != today:
            risk.daily_loss = 0
            risk.last_reset_date = today
            risk.updated_at = datetime.now(timezone.utc).isoformat()
            session.commit()
            session.refresh(risk)

        return RiskStateRead.model_validate(risk)


def update_sqlmodel_risk_state(
    *,
    daily_loss_delta: float = 0,
    is_loss: bool = False,
) -> RiskStateRead:
    """Update risk state using SQLModel with type safety."""
    settings = get_settings()
    now = datetime.now(timezone.utc).isoformat()
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    with get_session() as session:
        risk = session.get(RiskState, 1)
        if not risk:
            risk = RiskState(id=1, last_reset_date=today)
            session.add(risk)

        # Reset if new day
        if risk.last_reset_date != today:
            risk.daily_loss = 0
            risk.last_reset_date = today

        risk.daily_loss += daily_loss_delta
        if is_loss:
            risk.consecutive_losses += 1
        else:
            risk.consecutive_losses = 0

        risk.is_paused = (
            risk.daily_loss >= settings.max_daily_loss
            or risk.consecutive_losses >= settings.circuit_breaker_consecutive_losses
        )
        risk.updated_at = now

        session.commit()
        session.refresh(risk)
        return RiskStateRead.model_validate(risk)


# =============================================================================
# Aggregate Queries
# =============================================================================


def get_total_pnl_summary() -> dict[str, Any]:
    """Get total P&L summary across all time."""
    with get_db() as conn:
        row = conn.execute(
            """SELECT
                COALESCE(SUM(realized), 0) as total_realized,
                COALESCE(SUM(unrealized), 0) as total_unrealized,
                COALESCE(SUM(total_trades), 0) as total_trades,
                COALESCE(SUM(winning_trades), 0) as winning_trades,
                COALESCE(SUM(losing_trades), 0) as losing_trades
            FROM daily_pnl"""
        ).fetchone()
        if row:
            data = dict(row)
            total = data.get("total_trades", 0)
            winning = data.get("winning_trades", 0)
            data["overall_winrate"] = (winning / total * 100) if total > 0 else 0
            return data
    return {
        "total_realized": 0,
        "total_unrealized": 0,
        "total_trades": 0,
        "winning_trades": 0,
        "losing_trades": 0,
        "overall_winrate": 0,
    }


def get_position_pnl_summary() -> dict[str, Any]:
    """Get P&L summary from positions."""
    with get_db() as conn:
        open_row = conn.execute(
            """SELECT
                COUNT(*) as open_count,
                COALESCE(SUM(unrealized_pnl), 0) as total_unrealized,
                COALESCE(SUM(cost_basis), 0) as total_cost_basis
            FROM positions WHERE status = 'OPEN'"""
        ).fetchone()

        closed_row = conn.execute(
            """SELECT
                COUNT(*) as closed_count,
                COALESCE(SUM(realized_pnl), 0) as total_realized
            FROM positions WHERE status = 'CLOSED'"""
        ).fetchone()

        return {
            "open_positions": dict(open_row) if open_row else {},
            "closed_positions": dict(closed_row) if closed_row else {},
        }


def _ensure_indexes() -> None:
    """Create performance indexes (idempotent)."""
    conn = _get_persistent_conn()
    conn.execute("CREATE INDEX IF NOT EXISTS idx_piggybank_ts ON piggybank_transfers(timestamp)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_ts ON trades(timestamp)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_positions_status ON positions(status)")
    conn.commit()
