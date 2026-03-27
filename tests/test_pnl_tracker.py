#!/usr/bin/env python3
"""Tests for PnL tracker — fee calculation, positions, and partial fills."""

import sys
import os

# Add src directory to path for imports when running directly
sys.path.insert(
    0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src")
)

from decimal import Decimal
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch


def test_fee_schedule_taker():
    """Test taker fee calculation."""
    from polybot.pnl_tracker import FeeSchedule, FeeType

    schedule = FeeSchedule(taker_fee_bps=10)  # 0.1% fee

    # $100 trade should have $0.10 taker fee
    fee = schedule.calc_fee(Decimal("100"), FeeType.TAKER)
    assert fee == Decimal("0.10")

    # $1000 trade should have $1.00 taker fee
    fee2 = schedule.calc_fee(Decimal("1000"), FeeType.TAKER)
    assert fee2 == Decimal("1.00")


def test_fee_schedule_maker():
    """Test maker fee calculation (currently 0)."""
    from polybot.pnl_tracker import FeeSchedule, FeeType

    schedule = FeeSchedule(maker_fee_bps=0)

    # $100 maker trade should have $0 fee
    fee = schedule.calc_fee(Decimal("100"), FeeType.MAKER)
    assert fee == Decimal("0")


def test_fee_schedule_negrisk_funding():
    """Test NegRisk funding calculation."""
    from polybot.pnl_tracker import FeeSchedule

    # 2 bps daily = 0.02% per day
    schedule = FeeSchedule(negrisk_funding_daily_bps=2)

    # $1000 position held for 1 day
    funding = schedule.calc_negrisk_funding(Decimal("1000"), 1.0)
    assert funding == Decimal("0.20")  # 0.02% of 1000

    # $1000 position held for 0.5 days
    funding2 = schedule.calc_negrisk_funding(Decimal("1000"), 0.5)
    assert funding2 == Decimal("0.10")


def test_fill_notional():
    """Test fill notional calculation."""
    from polybot.pnl_tracker import Fill, FeeType

    fill = Fill(
        fill_id="test_fill",
        order_id="test_order",
        timestamp=datetime.now(timezone.utc),
        price=Decimal("0.50"),
        size=Decimal("100"),
        side="BUY",
        fee_type=FeeType.TAKER,
        fee_amount=Decimal("0.05"),
    )

    assert fill.notional == Decimal("50")  # 0.50 * 100
    assert fill.total_cost == Decimal("50.05")  # notional + fee for BUY


def test_fill_sell_total_cost():
    """Test sell fill total cost (notional minus fee)."""
    from polybot.pnl_tracker import Fill, FeeType

    fill = Fill(
        fill_id="test_fill",
        order_id="test_order",
        timestamp=datetime.now(timezone.utc),
        price=Decimal("0.60"),
        size=Decimal("100"),
        side="SELL",
        fee_type=FeeType.TAKER,
        fee_amount=Decimal("0.06"),
    )

    assert fill.notional == Decimal("60")
    assert fill.total_cost == Decimal("59.94")  # notional - fee for SELL


def test_position_buy_fill():
    """Test position with a buy fill."""
    from polybot.pnl_tracker import Position, Fill, FeeType, PositionStatus

    position = Position(
        position_id="test_pos",
        market_id="test_market",
        token_id="test_token",
        market_title="Test Market",
        outcome="YES",
        opened_at=datetime.now(timezone.utc),
    )

    fill = Fill(
        fill_id="fill_1",
        order_id="order_1",
        timestamp=datetime.now(timezone.utc),
        price=Decimal("0.50"),
        size=Decimal("100"),
        side="BUY",
        fee_type=FeeType.TAKER,
        fee_amount=Decimal("0.05"),
    )

    position.add_fill(fill)

    assert position.size == Decimal("100")
    assert position.avg_entry_price == Decimal("0.50")
    assert position.cost_basis == Decimal("50.05")
    assert position.total_fees_paid == Decimal("0.05")
    assert position.status == PositionStatus.OPEN


def test_position_multiple_buys_weighted_average():
    """Test weighted average price with multiple buys."""
    from polybot.pnl_tracker import Position, Fill, FeeType

    position = Position(
        position_id="test_pos",
        market_id="test_market",
        token_id="test_token",
        market_title="Test Market",
        outcome="YES",
        opened_at=datetime.now(timezone.utc),
    )

    # First buy: 100 shares at $0.40
    fill1 = Fill(
        fill_id="fill_1",
        order_id="order_1",
        timestamp=datetime.now(timezone.utc),
        price=Decimal("0.40"),
        size=Decimal("100"),
        side="BUY",
        fee_type=FeeType.TAKER,
        fee_amount=Decimal("0.04"),
    )
    position.add_fill(fill1)

    # Second buy: 100 shares at $0.60
    fill2 = Fill(
        fill_id="fill_2",
        order_id="order_2",
        timestamp=datetime.now(timezone.utc),
        price=Decimal("0.60"),
        size=Decimal("100"),
        side="BUY",
        fee_type=FeeType.TAKER,
        fee_amount=Decimal("0.06"),
    )
    position.add_fill(fill2)

    assert position.size == Decimal("200")
    # Weighted average: (100*0.40 + 100*0.60) / 200 = 0.50
    assert position.avg_entry_price == Decimal("0.50")
    assert position.total_fees_paid == Decimal("0.10")


def test_position_partial_close():
    """Test partial position close."""
    from polybot.pnl_tracker import Position, Fill, FeeType, PositionStatus

    position = Position(
        position_id="test_pos",
        market_id="test_market",
        token_id="test_token",
        market_title="Test Market",
        outcome="YES",
        opened_at=datetime.now(timezone.utc),
    )

    # Buy 100 shares at $0.50 (cost basis = 50 + 0.05 fee = 50.05)
    fill1 = Fill(
        fill_id="fill_1",
        order_id="order_1",
        timestamp=datetime.now(timezone.utc),
        price=Decimal("0.50"),
        size=Decimal("100"),
        side="BUY",
        fee_type=FeeType.TAKER,
        fee_amount=Decimal("0.05"),
    )
    position.add_fill(fill1)

    # Sell 50 shares at $0.60
    fill2 = Fill(
        fill_id="fill_2",
        order_id="order_2",
        timestamp=datetime.now(timezone.utc),
        price=Decimal("0.60"),
        size=Decimal("50"),
        side="SELL",
        fee_type=FeeType.TAKER,
        fee_amount=Decimal("0.03"),
    )
    position.add_fill(fill2)

    assert position.size == Decimal("50")
    assert position.status == PositionStatus.PARTIAL
    # Realized: sell proceeds (30) - portion of cost basis (50.05 * 0.5 = 25.025) - sell fee (0.03)
    # = 30 - 25.025 - 0.03 = 4.945
    assert position.realized_pnl == Decimal("4.945")


def test_position_full_close():
    """Test full position close."""
    from polybot.pnl_tracker import Position, Fill, FeeType, PositionStatus

    position = Position(
        position_id="test_pos",
        market_id="test_market",
        token_id="test_token",
        market_title="Test Market",
        outcome="YES",
        opened_at=datetime.now(timezone.utc),
    )

    # Buy 100 shares at $0.50
    fill1 = Fill(
        fill_id="fill_1",
        order_id="order_1",
        timestamp=datetime.now(timezone.utc),
        price=Decimal("0.50"),
        size=Decimal("100"),
        side="BUY",
        fee_type=FeeType.TAKER,
        fee_amount=Decimal("0.05"),
    )
    position.add_fill(fill1)

    # Sell all 100 shares at $0.70
    fill2 = Fill(
        fill_id="fill_2",
        order_id="order_2",
        timestamp=datetime.now(timezone.utc),
        price=Decimal("0.70"),
        size=Decimal("100"),
        side="SELL",
        fee_type=FeeType.TAKER,
        fee_amount=Decimal("0.07"),
    )
    position.add_fill(fill2)

    assert position.size == Decimal("0")
    assert position.status == PositionStatus.CLOSED
    # Realized: 100 * 0.70 - 100 * 0.50 - 0.07 = 70 - 50 - 0.07 = 19.93
    assert position.realized_pnl == Decimal("19.93")


def test_position_unrealized_pnl():
    """Test unrealized PnL calculation."""
    from polybot.pnl_tracker import Position, Fill, FeeType

    position = Position(
        position_id="test_pos",
        market_id="test_market",
        token_id="test_token",
        market_title="Test Market",
        outcome="YES",
        opened_at=datetime.now(timezone.utc),
    )

    # Buy 100 shares at $0.50
    fill = Fill(
        fill_id="fill_1",
        order_id="order_1",
        timestamp=datetime.now(timezone.utc),
        price=Decimal("0.50"),
        size=Decimal("100"),
        side="BUY",
        fee_type=FeeType.TAKER,
        fee_amount=Decimal("0.05"),
    )
    position.add_fill(fill)

    # Current price is $0.60
    position.current_price = Decimal("0.60")

    # Unrealized: 100 * 0.60 - 100 * 0.50 = 10
    assert position.unrealized_pnl == Decimal("10")

    # Unrealized net: 10 - 0.05 (fees) = 9.95
    assert position.unrealized_pnl_net == Decimal("9.95")


def test_estimate_arb_profit():
    """Test arbitrage profit estimation including fees."""
    from polybot.pnl_tracker import estimate_arb_profit

    # YES at $0.45, NO at $0.45 = combined $0.90
    # Profit = $1 - $0.90 = $0.10 per share
    result = estimate_arb_profit(
        yes_price=0.45,
        no_price=0.45,
        amount=100,
    )

    assert result["combined_price"] == 0.9
    assert result["gross_profit"] > 0
    assert result["fees"] > 0
    assert result["net_profit"] < result["gross_profit"]
    assert result["profit_pct"] > 0


def test_estimate_arb_no_profit():
    """Test arb estimation when no profit exists."""
    from polybot.pnl_tracker import estimate_arb_profit

    # YES at $0.55, NO at $0.50 = combined $1.05 (no arb)
    result = estimate_arb_profit(
        yes_price=0.55,
        no_price=0.50,
        amount=100,
    )

    assert result["gross_profit"] == 0
    assert result["net_profit"] == 0
    assert result["profit_pct"] == 0


def test_pnl_tracker_record_fill():
    """Test PnLTracker fill recording."""
    with patch("polybot.pnl_tracker.get_settings") as mock_settings:
        mock_settings.return_value = MagicMock(
            negrisk_funding_daily_bps=2,
        )

        from polybot.pnl_tracker import PnLTracker, FeeType

        tracker = PnLTracker()

        fill = tracker.record_fill(
            order_id="order_123",
            token_id="token_abc",
            market_id="market_xyz",
            market_title="Test Market",
            outcome="YES",
            side="BUY",
            price=0.50,
            size=100,
            fee_type=FeeType.TAKER,
        )

        assert fill.price == Decimal("0.50")
        assert fill.size == Decimal("100")
        assert fill.fee_amount == Decimal("0.05")  # 0.1% of $50

        position = tracker.get_position("token_abc")
        assert position is not None
        assert position.size == Decimal("100")


def test_pnl_tracker_total_pnl():
    """Test total PnL calculation across positions."""
    with patch("polybot.pnl_tracker.get_settings") as mock_settings:
        mock_settings.return_value = MagicMock(
            negrisk_funding_daily_bps=2,
        )

        from polybot.pnl_tracker import PnLTracker

        tracker = PnLTracker()

        # Position 1: Buy and partial close
        tracker.record_fill(
            order_id="order_1",
            token_id="token_1",
            market_id="market_1",
            market_title="Market 1",
            outcome="YES",
            side="BUY",
            price=0.50,
            size=100,
        )

        tracker.record_fill(
            order_id="order_2",
            token_id="token_1",
            market_id="market_1",
            market_title="Market 1",
            outcome="YES",
            side="SELL",
            price=0.60,
            size=50,
        )

        # Update current price
        tracker.update_price("token_1", 0.55)

        pnl = tracker.get_total_pnl()

        assert pnl["total_fees"] > Decimal("0")
        assert "realized_pnl" in pnl
        assert "unrealized_pnl" in pnl
        assert "net_pnl" in pnl


def test_pnl_tracker_serialization():
    """Test PnLTracker to_dict and from_dict."""
    with patch("polybot.pnl_tracker.get_settings") as mock_settings:
        mock_settings.return_value = MagicMock(
            negrisk_funding_daily_bps=2,
        )

        from polybot.pnl_tracker import PnLTracker

        tracker = PnLTracker()
        tracker.record_fill(
            order_id="order_1",
            token_id="token_1",
            market_id="market_1",
            market_title="Test Market",
            outcome="YES",
            side="BUY",
            price=0.50,
            size=100,
        )

        # Serialize
        data = tracker.to_dict()

        assert "positions" in data
        assert "token_1" in data["positions"]
        assert "fee_schedule" in data

        # Deserialize
        restored = PnLTracker.from_dict(data)

        position = restored.get_position("token_1")
        assert position is not None
        assert position.size == Decimal("100")


if __name__ == "__main__":
    # Run tests when executed directly
    print("Running PnL tracker module tests...")

    test_fee_schedule_taker()
    print("✓ test_fee_schedule_taker passed")

    test_fee_schedule_maker()
    print("✓ test_fee_schedule_maker passed")

    test_fee_schedule_negrisk_funding()
    print("✓ test_fee_schedule_negrisk_funding passed")

    test_fill_notional()
    print("✓ test_fill_notional passed")

    test_fill_sell_total_cost()
    print("✓ test_fill_sell_total_cost passed")

    test_position_buy_fill()
    print("✓ test_position_buy_fill passed")

    test_position_multiple_buys_weighted_average()
    print("✓ test_position_multiple_buys_weighted_average passed")

    test_position_partial_close()
    print("✓ test_position_partial_close passed")

    test_position_full_close()
    print("✓ test_position_full_close passed")

    test_position_unrealized_pnl()
    print("✓ test_position_unrealized_pnl passed")

    test_estimate_arb_profit()
    print("✓ test_estimate_arb_profit passed")

    test_estimate_arb_no_profit()
    print("✓ test_estimate_arb_no_profit passed")

    test_pnl_tracker_record_fill()
    print("✓ test_pnl_tracker_record_fill passed")

    test_pnl_tracker_total_pnl()
    print("✓ test_pnl_tracker_total_pnl passed")

    test_pnl_tracker_serialization()
    print("✓ test_pnl_tracker_serialization passed")

    print("\nAll PnL tracker tests passed! ✓")
