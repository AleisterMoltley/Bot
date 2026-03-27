"""Tests for SQLModel models and database operations."""

import tempfile
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pytest
from pydantic import ValidationError


class MockSettings:
    """Mock settings for testing."""

    db_path = ""
    max_daily_loss = 100.0
    circuit_breaker_consecutive_losses = 5


@pytest.fixture
def temp_db():
    """Create a temporary database for testing."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    mock_settings = MockSettings()
    mock_settings.db_path = db_path

    # Patch settings and reset globals
    with patch("polybot.database.get_settings", return_value=mock_settings):
        import polybot.database as db_module

        db_module._DB_PATH = None
        db_module._ENGINE = None

        # Initialize database
        db_module.init_db()

        yield db_path, mock_settings

    # Cleanup
    Path(db_path).unlink(missing_ok=True)


class TestTradeModels:
    """Test Trade model validation."""

    def test_trade_create_basic(self):
        """Test basic trade creation."""
        from polybot.models import TradeCreate

        trade = TradeCreate(
            mode="signal",
            side="BUY",
            price=0.65,
            size=100,
            cost=65.00,
        )
        assert trade.mode == "signal"
        assert trade.side == "BUY"
        assert trade.price == 0.65
        assert trade.cost == 65.00

    def test_trade_side_uppercase(self):
        """Test that side is automatically uppercased."""
        from polybot.models import TradeCreate

        trade = TradeCreate(mode="signal", side="buy", price=0.5, size=10, cost=5)
        assert trade.side == "BUY"

        trade2 = TradeCreate(mode="signal", side="Sell", price=0.5, size=10, cost=5)
        assert trade2.side == "SELL"

    def test_trade_price_validation(self):
        """Test price must be between 0 and 1."""
        from polybot.models import TradeCreate

        # Valid price
        trade = TradeCreate(mode="signal", side="buy", price=0.0, size=10, cost=0)
        assert trade.price == 0.0

        trade = TradeCreate(mode="signal", side="buy", price=1.0, size=10, cost=10)
        assert trade.price == 1.0

        # Invalid price (> 1)
        with pytest.raises(ValidationError):
            TradeCreate(mode="signal", side="buy", price=1.5, size=10, cost=15)

        # Invalid price (< 0)
        with pytest.raises(ValidationError):
            TradeCreate(mode="signal", side="buy", price=-0.1, size=10, cost=-1)

    def test_trade_size_validation(self):
        """Test size must be non-negative."""
        from polybot.models import TradeCreate

        trade = TradeCreate(mode="signal", side="buy", price=0.5, size=0, cost=0)
        assert trade.size == 0

        with pytest.raises(ValidationError):
            TradeCreate(mode="signal", side="buy", price=0.5, size=-10, cost=-5)

    def test_trade_with_optional_fields(self):
        """Test trade with all optional fields."""
        from polybot.models import TradeCreate

        trade = TradeCreate(
            mode="copy",
            side="sell",
            price=0.75,
            size=50,
            cost=37.50,
            market_id="0x123abc",
            market_title="Will ETH reach $5000?",
            outcome="YES",
            order_id="order-456",
            strategy="momentum",
            confidence=85.5,
            kelly_size=25.0,
            profit=10.50,
            notes="Good entry point",
        )
        assert trade.market_id == "0x123abc"
        assert trade.confidence == 85.5
        assert trade.notes == "Good entry point"


class TestPositionModels:
    """Test Position model validation."""

    def test_position_create(self):
        """Test position creation."""
        from polybot.models import PositionCreate

        position = PositionCreate(
            market_id="0x123",
            token_id="token-456",
            side="YES",
            size=100,
            avg_price=0.55,
            cost_basis=55.00,
        )
        assert position.status == "OPEN"
        assert position.unrealized_pnl == 0

    def test_position_price_validation(self):
        """Test position price constraints."""
        from polybot.models import PositionCreate

        # Valid avg_price
        position = PositionCreate(
            market_id="0x123",
            token_id="token-456",
            side="YES",
            size=100,
            avg_price=0.5,
            cost_basis=50.00,
        )
        assert position.avg_price == 0.5

        # Invalid avg_price
        with pytest.raises(ValidationError):
            PositionCreate(
                market_id="0x123",
                token_id="token-456",
                side="YES",
                size=100,
                avg_price=1.5,
                cost_basis=150.00,
            )


class TestDailyPnLModels:
    """Test DailyPnL model."""

    def test_daily_pnl_winrate(self):
        """Test winrate calculation."""
        from polybot.models import DailyPnLRead

        pnl = DailyPnLRead(
            date="2026-03-18",
            realized=100.0,
            unrealized=50.0,
            total_trades=10,
            winning_trades=7,
            losing_trades=3,
        )
        assert pnl.winrate == 70.0

    def test_daily_pnl_winrate_zero_trades(self):
        """Test winrate with zero trades."""
        from polybot.models import DailyPnLRead

        pnl = DailyPnLRead(
            date="2026-03-18",
            realized=0,
            unrealized=0,
            total_trades=0,
            winning_trades=0,
            losing_trades=0,
        )
        assert pnl.winrate == 0.0


class TestRiskStateModels:
    """Test RiskState model."""

    def test_risk_state_defaults(self):
        """Test risk state default values."""
        from polybot.models import RiskState

        risk = RiskState()
        assert risk.daily_loss == 0
        assert risk.consecutive_losses == 0
        assert risk.is_paused is False


class TestEnums:
    """Test enumeration values."""

    def test_trade_side_enum(self):
        """Test TradeSide enum."""
        from polybot.models import TradeSide

        assert TradeSide.BUY == "BUY"
        assert TradeSide.SELL == "SELL"

    def test_trade_mode_enum(self):
        """Test TradeMode enum."""
        from polybot.models import TradeMode

        assert TradeMode.SIGNAL == "signal"
        assert TradeMode.COPY == "copy"
        assert TradeMode.ARBITRAGE == "arbitrage"

    def test_position_status_enum(self):
        """Test PositionStatus enum."""
        from polybot.models import PositionStatus

        assert PositionStatus.OPEN == "OPEN"
        assert PositionStatus.CLOSED == "CLOSED"

    def test_arb_status_enum(self):
        """Test ArbStatus enum."""
        from polybot.models import ArbStatus

        assert ArbStatus.PENDING == "PENDING"
        assert ArbStatus.EXECUTED == "EXECUTED"
        assert ArbStatus.FAILED == "FAILED"


class TestDatabaseOperations:
    """Test SQLModel database operations."""

    def test_create_trade(self, temp_db):
        """Test creating a trade via SQLModel."""
        db_path, mock_settings = temp_db

        with patch("polybot.database.get_settings", return_value=mock_settings):
            import polybot.database as db_module

            db_module._DB_PATH = None
            db_module._ENGINE = None

            from polybot.database import create_trade
            from polybot.models import TradeCreate

            trade_data = TradeCreate(
                mode="signal",
                side="BUY",
                price=0.65,
                size=100,
                cost=65.00,
                market_title="Test Market",
            )

            result = create_trade(trade_data)
            assert result.id is not None
            assert result.side == "BUY"
            assert result.price == 0.65
            assert result.timestamp is not None

    def test_get_trades(self, temp_db):
        """Test retrieving trades."""
        db_path, mock_settings = temp_db

        with patch("polybot.database.get_settings", return_value=mock_settings):
            import polybot.database as db_module

            db_module._DB_PATH = None
            db_module._ENGINE = None

            from polybot.database import create_trade, get_trades
            from polybot.models import TradeCreate

            # Create some trades
            for i in range(3):
                trade_data = TradeCreate(
                    mode="signal",
                    side="BUY",
                    price=0.5 + i * 0.1,
                    size=10 * (i + 1),
                    cost=5 * (i + 1),
                )
                create_trade(trade_data)

            trades = get_trades(limit=10)
            assert len(trades) == 3

    def test_create_position(self, temp_db):
        """Test creating a position."""
        db_path, mock_settings = temp_db

        with patch("polybot.database.get_settings", return_value=mock_settings):
            import polybot.database as db_module

            db_module._DB_PATH = None
            db_module._ENGINE = None

            from polybot.database import create_position
            from polybot.models import PositionCreate

            position_data = PositionCreate(
                market_id="0x123",
                token_id="token-456",
                side="YES",
                size=100,
                avg_price=0.55,
                cost_basis=55.00,
            )

            result = create_position(position_data)
            assert result.id is not None
            assert result.status == "OPEN"
            assert result.opened_at is not None

    def test_close_position(self, temp_db):
        """Test closing a position."""
        db_path, mock_settings = temp_db

        with patch("polybot.database.get_settings", return_value=mock_settings):
            import polybot.database as db_module

            db_module._DB_PATH = None
            db_module._ENGINE = None

            from polybot.database import close_position, create_position
            from polybot.models import PositionCreate

            position_data = PositionCreate(
                market_id="0x123",
                token_id="token-456",
                side="YES",
                size=100,
                avg_price=0.55,
                cost_basis=55.00,
            )
            position = create_position(position_data)

            closed = close_position(position.id, realized_pnl=15.50)
            assert closed is not None
            assert closed.status == "CLOSED"
            assert closed.realized_pnl == 15.50
            assert closed.closed_at is not None

    def test_daily_pnl_operations(self, temp_db):
        """Test daily P&L operations."""
        db_path, mock_settings = temp_db

        with patch("polybot.database.get_settings", return_value=mock_settings):
            import polybot.database as db_module

            db_module._DB_PATH = None
            db_module._ENGINE = None

            from polybot.database import get_or_create_daily_pnl, update_daily_pnl

            # Get/create today's P&L
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            daily = get_or_create_daily_pnl(today)
            assert daily.date == today
            assert daily.realized == 0

            # Update with a winning trade
            updated = update_daily_pnl(today, realized_delta=25.00, is_win=True)
            assert updated.realized == 25.00
            assert updated.total_trades == 1
            assert updated.winning_trades == 1

            # Update with a losing trade
            updated = update_daily_pnl(today, realized_delta=-10.00, is_win=False)
            assert updated.realized == 15.00
            assert updated.total_trades == 2
            assert updated.losing_trades == 1

    def test_risk_state_operations(self, temp_db):
        """Test risk state operations."""
        db_path, mock_settings = temp_db

        with patch("polybot.database.get_settings", return_value=mock_settings):
            import polybot.database as db_module

            db_module._DB_PATH = None
            db_module._ENGINE = None

            from polybot.database import (
                get_sqlmodel_risk_state,
                update_sqlmodel_risk_state,
            )

            # Get initial state
            risk = get_sqlmodel_risk_state()
            assert risk.daily_loss == 0
            assert risk.consecutive_losses == 0
            assert risk.is_paused is False

            # Update with a loss
            risk = update_sqlmodel_risk_state(daily_loss_delta=20.0, is_loss=True)
            assert risk.daily_loss == 20.0
            assert risk.consecutive_losses == 1

            # Update with another loss
            risk = update_sqlmodel_risk_state(daily_loss_delta=30.0, is_loss=True)
            assert risk.daily_loss == 50.0
            assert risk.consecutive_losses == 2

            # Update with a win (resets consecutive losses)
            risk = update_sqlmodel_risk_state(daily_loss_delta=0, is_loss=False)
            assert risk.consecutive_losses == 0


class TestBackwardCompatibility:
    """Test backward compatibility with raw SQL functions."""

    def test_record_trade_legacy(self, temp_db):
        """Test legacy record_trade function still works."""
        db_path, mock_settings = temp_db

        with patch("polybot.database.get_settings", return_value=mock_settings):
            import polybot.database as db_module

            db_module._DB_PATH = None
            db_module._ENGINE = None

            from polybot.database import record_trade

            trade_id = record_trade(
                mode="signal",
                side="BUY",
                price=0.65,
                size=100,
                cost=65.00,
                market_title="Legacy Trade",
            )
            assert trade_id is not None
            assert trade_id > 0

    def test_get_risk_state_legacy(self, temp_db):
        """Test legacy get_risk_state function."""
        db_path, mock_settings = temp_db

        with patch("polybot.database.get_settings", return_value=mock_settings):
            import polybot.database as db_module

            db_module._DB_PATH = None
            db_module._ENGINE = None

            from polybot.database import get_risk_state

            risk = get_risk_state()
            assert "daily_loss" in risk
            assert "consecutive_losses" in risk
            assert "is_paused" in risk
