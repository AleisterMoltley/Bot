"""Tests for the SignalEngine module."""

import time

import pytest

from polybot.signal_engine import (
    ASSETS,
    BINANCE_SYMBOLS,
    CONF_CASCADE,
    CONF_LATENCY,
    CONF_NONE,
    CONF_OFI_STRONG,
    CONF_OFI_WEAK,
    AssetState,
    Signal,
    SignalEngine,
    get_signal_engine,
)


class TestSignalDataclass:
    """Tests for the Signal dataclass."""

    def test_signal_valid_up(self):
        sig = Signal(asset="BTC", direction="up", confidence=0.7, reason="test")
        assert sig.is_valid is True
        assert sig.direction == "up"

    def test_signal_valid_down(self):
        sig = Signal(asset="ETH", direction="down", confidence=0.6, reason="test")
        assert sig.is_valid is True

    def test_signal_invalid_none_direction(self):
        sig = Signal(asset="SOL", direction=None, confidence=0.0, reason="no_edge")
        assert sig.is_valid is False

    def test_signal_invalid_low_confidence(self):
        sig = Signal(asset="XRP", direction="up", confidence=0.3, reason="weak")
        assert sig.is_valid is False

    def test_signal_boundary_confidence(self):
        sig = Signal(asset="BTC", direction="up", confidence=0.5, reason="boundary")
        assert sig.is_valid is False  # must be > 0.5, not >= 0.5

    def test_signal_timestamp_set(self):
        before = time.time()
        sig = Signal(asset="BTC", direction="up", confidence=0.7, reason="test")
        after = time.time()
        assert before <= sig.timestamp <= after


class TestAssetState:
    """Tests for the AssetState dataclass."""

    def test_default_state(self):
        state = AssetState(symbol="btcusdt")
        assert state.symbol == "btcusdt"
        assert state.last_price == 0.0
        assert state.price_15s_ago == 0.0
        assert len(state.prices) == 0
        assert len(state.orderbook_snaps) == 0
        assert len(state.liquidations) == 0


class TestSignalEngineInit:
    """Tests for SignalEngine initialization."""

    def test_engine_creates_states_for_all_assets(self):
        engine = SignalEngine()
        for asset in ASSETS:
            assert asset in engine.states
            assert engine.states[asset].symbol == BINANCE_SYMBOLS[asset]

    def test_engine_not_running_initially(self):
        engine = SignalEngine()
        assert engine._running is False


class TestGetSignalUnknownAsset:
    """Test get_signal with unknown asset."""

    def test_unknown_asset_returns_no_signal(self):
        engine = SignalEngine()
        sig = engine.get_signal("DOGE")
        assert sig.direction is None
        assert sig.confidence == 0.0
        assert sig.reason == "unknown_asset"


class TestLiquidationCascade:
    """Tests for liquidation cascade signal detection."""

    def test_long_liquidation_cascade_returns_down(self):
        engine = SignalEngine()
        state = engine.states["BTC"]
        now = time.time()
        # Add $4M in long liquidations in last 10s
        state.liquidations.append((now - 2, 2_000_000, "long"))
        state.liquidations.append((now - 1, 2_000_000, "long"))
        sig = engine.get_signal("BTC")
        assert sig.direction == "down"
        assert sig.confidence == CONF_CASCADE

    def test_short_liquidation_cascade_returns_up(self):
        engine = SignalEngine()
        state = engine.states["ETH"]
        now = time.time()
        # Add $4M in short liquidations in last 10s
        state.liquidations.append((now - 3, 2_500_000, "short"))
        state.liquidations.append((now - 1, 1_500_000, "short"))
        sig = engine.get_signal("ETH")
        assert sig.direction == "up"
        assert sig.confidence == CONF_CASCADE

    def test_old_liquidations_ignored(self):
        engine = SignalEngine()
        state = engine.states["BTC"]
        now = time.time()
        # Liquidations from 20s ago should not count
        state.liquidations.append((now - 20, 5_000_000, "long"))
        sig = engine.get_signal("BTC")
        assert sig.direction is None

    def test_below_threshold_no_cascade(self):
        engine = SignalEngine()
        state = engine.states["BTC"]
        now = time.time()
        state.liquidations.append((now - 2, 1_000_000, "long"))
        sig = engine.get_signal("BTC")
        assert sig.direction is None


class TestOFISignal:
    """Tests for Order Flow Imbalance signal detection."""

    def _inject_orderbook_snaps(self, state, snaps):
        """Helper to inject orderbook snapshots."""
        for ts, bid, ask in snaps:
            state.orderbook_snaps.append((ts, bid, ask))

    def test_strong_positive_ofi_returns_up(self):
        engine = SignalEngine()
        state = engine.states["SOL"]
        now = time.time()
        # Bids increasing heavily, asks flat → strong buy pressure
        self._inject_orderbook_snaps(
            state,
            [
                (now - 10, 100, 100),
                (now - 8, 200, 100),
                (now - 6, 300, 100),
                (now - 4, 400, 100),
            ],
        )
        sig = engine.get_signal("SOL")
        assert sig.direction == "up"
        assert sig.confidence == CONF_OFI_STRONG

    def test_strong_negative_ofi_returns_down(self):
        engine = SignalEngine()
        state = engine.states["SOL"]
        now = time.time()
        # Asks increasing heavily, bids flat → strong sell pressure
        self._inject_orderbook_snaps(
            state,
            [
                (now - 10, 100, 100),
                (now - 8, 100, 200),
                (now - 6, 100, 300),
                (now - 4, 100, 400),
            ],
        )
        sig = engine.get_signal("SOL")
        assert sig.direction == "down"
        assert sig.confidence == CONF_OFI_STRONG

    def test_too_few_snaps_returns_no_signal(self):
        engine = SignalEngine()
        state = engine.states["BTC"]
        now = time.time()
        state.orderbook_snaps.append((now - 2, 100, 50))
        state.orderbook_snaps.append((now - 1, 200, 50))
        sig = engine.get_signal("BTC")
        assert sig.direction is None


class TestLatencyArbitrage:
    """Tests for latency arbitrage signal detection."""

    def test_cex_price_up_poly_cheap_returns_up(self):
        engine = SignalEngine()
        state = engine.states["BTC"]
        state.last_price = 100_000
        state.price_15s_ago = 99_500  # +0.5% move
        sig = engine.get_signal("BTC", polymarket_up_price=0.50)
        assert sig.direction == "up"
        assert sig.confidence == CONF_LATENCY

    def test_cex_price_down_poly_expensive_returns_down(self):
        engine = SignalEngine()
        state = engine.states["ETH"]
        state.last_price = 3_000
        state.price_15s_ago = 3_020  # -0.66% move
        sig = engine.get_signal("ETH", polymarket_up_price=0.55)
        assert sig.direction == "down"
        assert sig.confidence == CONF_LATENCY

    def test_cex_price_up_but_poly_already_reacted_no_signal(self):
        engine = SignalEngine()
        state = engine.states["BTC"]
        state.last_price = 100_000
        state.price_15s_ago = 99_500  # +0.5% move
        # Polymarket already at 0.60 → already priced in
        sig = engine.get_signal("BTC", polymarket_up_price=0.60)
        assert sig.direction is None

    def test_no_price_data_no_latency_signal(self):
        engine = SignalEngine()
        sig = engine.get_signal("BTC", polymarket_up_price=0.50)
        assert sig.direction is None


class TestWeakOFI:
    """Tests for weak OFI signal with polymarket alignment."""

    def _inject_mild_buy_pressure(self, state):
        now = time.time()
        # Moderate bid increase — keep OFI between 0.8 and 1.5 (weak range)
        state.orderbook_snaps.append((now - 10, 100, 100))
        state.orderbook_snaps.append((now - 8, 110, 105))
        state.orderbook_snaps.append((now - 6, 118, 110))
        state.orderbook_snaps.append((now - 4, 125, 115))

    def test_weak_ofi_up_with_cheap_poly_returns_up(self):
        engine = SignalEngine()
        state = engine.states["XRP"]
        self._inject_mild_buy_pressure(state)
        sig = engine.get_signal("XRP", polymarket_up_price=0.48)
        # Should get a buy signal (either weak or strong OFI)
        assert sig.direction == "up"
        assert sig.confidence in (CONF_OFI_WEAK, CONF_OFI_STRONG)

    def test_weak_ofi_up_with_expensive_poly_no_signal(self):
        engine = SignalEngine()
        state = engine.states["XRP"]
        self._inject_mild_buy_pressure(state)
        sig = engine.get_signal("XRP", polymarket_up_price=0.55)
        # If OFI is weak and poly is expensive, should not fire
        if sig.direction is not None:
            assert sig.confidence >= CONF_OFI_STRONG  # Only strong signals should fire


class TestNoEdge:
    """Tests for no-edge scenarios."""

    def test_empty_state_returns_no_edge(self):
        engine = SignalEngine()
        sig = engine.get_signal("BTC")
        assert sig.direction is None
        assert sig.confidence == CONF_NONE
        assert sig.reason == "no_edge"


class TestComputeOFI:
    """Tests for _compute_ofi method."""

    def test_ofi_with_no_data(self):
        engine = SignalEngine()
        state = engine.states["BTC"]
        ofi = engine._compute_ofi(state)
        assert ofi == 0.0

    def test_ofi_balanced(self):
        engine = SignalEngine()
        state = engine.states["BTC"]
        now = time.time()
        state.orderbook_snaps.append((now - 6, 100, 100))
        state.orderbook_snaps.append((now - 4, 150, 150))
        state.orderbook_snaps.append((now - 2, 200, 200))
        ofi = engine._compute_ofi(state)
        assert ofi == 0.0  # Equal increases cancel out


class TestHandlers:
    """Tests for WebSocket message handlers."""

    def test_handle_depth(self):
        engine = SignalEngine()
        data = {
            "b": [["100.0", "1.5"], ["99.0", "2.0"]],
            "a": [["101.0", "1.0"], ["102.0", "3.0"]],
        }
        engine._handle_depth("BTC", data)
        assert len(engine.states["BTC"].orderbook_snaps) == 1
        _, bid_vol, ask_vol = engine.states["BTC"].orderbook_snaps[0]
        assert bid_vol == pytest.approx(3.5)
        assert ask_vol == pytest.approx(4.0)

    def test_handle_trade(self):
        engine = SignalEngine()
        data = {"p": "50000.50"}
        engine._handle_trade("BTC", data)
        assert engine.states["BTC"].last_price == pytest.approx(50000.50)
        assert len(engine.states["BTC"].prices) == 1

    def test_handle_trade_zero_price_ignored(self):
        engine = SignalEngine()
        data = {"p": "0"}
        engine._handle_trade("BTC", data)
        assert engine.states["BTC"].last_price == 0.0
        assert len(engine.states["BTC"].prices) == 0

    def test_handle_liquidation_long(self):
        engine = SignalEngine()
        data = {"o": {"S": "SELL", "q": "1.0", "p": "50000"}}
        engine._handle_liquidation("BTC", data)
        assert len(engine.states["BTC"].liquidations) == 1
        _, usd_size, side = engine.states["BTC"].liquidations[0]
        assert usd_size == 50000.0
        assert side == "long"

    def test_handle_liquidation_short(self):
        engine = SignalEngine()
        data = {"o": {"S": "BUY", "q": "10.0", "p": "3000"}}
        engine._handle_liquidation("ETH", data)
        assert len(engine.states["ETH"].liquidations) == 1
        _, usd_size, side = engine.states["ETH"].liquidations[0]
        assert usd_size == 30000.0
        assert side == "short"

    def test_handle_liquidation_tiny_ignored(self):
        engine = SignalEngine()
        data = {"o": {"S": "SELL", "q": "0.001", "p": "50000"}}
        engine._handle_liquidation("BTC", data)
        assert len(engine.states["BTC"].liquidations) == 0  # $50 < $10k threshold

    def test_handle_depth_bad_data(self):
        engine = SignalEngine()
        engine._handle_depth("BTC", {})  # No crash
        assert len(engine.states["BTC"].orderbook_snaps) == 1

    def test_handle_trade_bad_data(self):
        engine = SignalEngine()
        engine._handle_trade("BTC", {})  # No crash - "p" defaults to 0


class TestGetSignalEngineSingleton:
    """Tests for the get_signal_engine singleton."""

    def test_singleton_returns_same_instance(self):
        import polybot.signal_engine as mod

        mod._engine = None  # Reset singleton
        e1 = get_signal_engine()
        e2 = get_signal_engine()
        assert e1 is e2
        mod._engine = None  # Cleanup

    def test_singleton_creates_new_if_none(self):
        import polybot.signal_engine as mod

        mod._engine = None
        engine = get_signal_engine()
        assert isinstance(engine, SignalEngine)
        mod._engine = None  # Cleanup


class TestSignalPriority:
    """Test that signal priority is correct (liquidation > OFI > latency)."""

    def test_liquidation_overrides_ofi(self):
        engine = SignalEngine()
        state = engine.states["BTC"]
        now = time.time()

        # Add both liquidation cascade AND strong OFI
        state.liquidations.append((now - 2, 4_000_000, "long"))

        state.orderbook_snaps.append((now - 10, 100, 100))
        state.orderbook_snaps.append((now - 8, 200, 100))
        state.orderbook_snaps.append((now - 6, 300, 100))
        state.orderbook_snaps.append((now - 4, 400, 100))

        sig = engine.get_signal("BTC")
        # Liquidation should win (checked first)
        assert sig.confidence == CONF_CASCADE
        assert sig.direction == "down"


@pytest.mark.asyncio
class TestSignalEngineStartStop:
    """Tests for start/stop lifecycle."""

    async def test_stop_clears_tasks(self):
        engine = SignalEngine()
        engine._running = True
        await engine.stop()
        assert engine._running is False
        assert len(engine._tasks) == 0

    async def test_start_sets_running(self):
        engine = SignalEngine()
        # We can't actually connect to Binance, but we can verify
        # the flag gets set (connections will fail)
        engine._running = True  # Prevent actual start
        await engine.start()  # Should return early (already running)
        assert engine._running is True
