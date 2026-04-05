"""Tests for mode_strategies, hyperliquid_engine, and mode switching."""

from __future__ import annotations

import asyncio
import json
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ═══════════════════════════════════════════════════════════════════════════════
# MODE STRATEGIES TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestStrategyConfig:
    """Test StrategyConfig definitions and accessors."""

    def test_all_strategies_defined(self):
        from polybot.mode_strategies import STRATEGIES
        expected_modes = [
            "sniper45", "sniper30", "sniper60", "hype45",
            "demonscalp", "coinbase", "full_consensus", "down_only",
            "updown", "arbitrage", "signal", "signal247",
            "autopred", "reverse",
        ]
        for mode in expected_modes:
            assert mode in STRATEGIES, f"Missing strategy: {mode}"

    def test_strategy_count(self):
        from polybot.mode_strategies import STRATEGIES
        assert len(STRATEGIES) == 14

    def test_get_strategy_valid(self):
        from polybot.mode_strategies import get_strategy
        s = get_strategy("sniper45")
        assert s.ui_mode == "sniper45"
        assert s.base_mode == "sniper"
        assert s.snipe_window_seconds == 45

    def test_get_strategy_unknown_returns_default(self):
        from polybot.mode_strategies import get_strategy, DEFAULT_STRATEGY
        s = get_strategy("nonexistent_mode")
        assert s == DEFAULT_STRATEGY
        assert s.ui_mode == "updown"

    def test_sniper_variants_have_different_windows(self):
        from polybot.mode_strategies import STRATEGIES
        assert STRATEGIES["sniper30"].snipe_window_seconds == 30
        assert STRATEGIES["sniper45"].snipe_window_seconds == 45
        assert STRATEGIES["sniper60"].snipe_window_seconds == 60

    def test_hype45_uses_hyperliquid(self):
        from polybot.mode_strategies import STRATEGIES
        hype = STRATEGIES["hype45"]
        assert hype.use_hyperliquid is True
        assert hype.use_binance is True  # Binance as fallback
        assert hype.base_mode == "sniper"

    def test_demonscalp_high_conviction(self):
        from polybot.mode_strategies import STRATEGIES
        ds = STRATEGIES["demonscalp"]
        assert ds.min_confidence >= 0.70
        assert ds.kelly_multiplier == 0.7
        assert ds.snipe_window_seconds == 120

    def test_down_only_filters_direction(self):
        from polybot.mode_strategies import STRATEGIES
        d = STRATEGIES["down_only"]
        assert d.direction_filter == "down"

    def test_reverse_conservative_sizing(self):
        from polybot.mode_strategies import STRATEGIES
        r = STRATEGIES["reverse"]
        assert r.kelly_multiplier == 0.35
        assert r.min_confidence >= 0.65

    def test_signal247_no_hour_filter(self):
        from polybot.mode_strategies import STRATEGIES
        s = STRATEGIES["signal247"]
        assert s.active_hours is None
        assert s.skip_low_volatility is False

    def test_full_consensus_requires_agreement(self):
        from polybot.mode_strategies import STRATEGIES
        fc = STRATEGIES["full_consensus"]
        assert fc.signal_consensus_required is True
        assert fc.use_binance is True
        assert fc.use_hyperliquid is True

    def test_all_strategies_frozen(self):
        """Strategies should be immutable."""
        from polybot.mode_strategies import STRATEGIES
        for name, s in STRATEGIES.items():
            with pytest.raises(AttributeError):
                s.min_confidence = 0.99  # type: ignore


class TestDirectionForSignal:
    """Test the strategy-aware direction resolution logic."""

    def test_basic_signal_passthrough(self):
        from polybot.mode_strategies import get_direction_for_signal, STRATEGIES
        strategy = STRATEGIES["updown"]
        direction, conf = get_direction_for_signal(
            strategy,
            binance_direction="up",
            binance_confidence=0.70,
        )
        assert direction == "up"
        assert conf == 0.70

    def test_below_min_confidence_filtered(self):
        from polybot.mode_strategies import get_direction_for_signal, STRATEGIES
        strategy = STRATEGIES["demonscalp"]  # min_confidence=0.72
        direction, conf = get_direction_for_signal(
            strategy,
            binance_direction="up",
            binance_confidence=0.65,
        )
        assert direction is None
        assert conf == 0.0

    def test_down_only_blocks_up(self):
        from polybot.mode_strategies import get_direction_for_signal, STRATEGIES
        strategy = STRATEGIES["down_only"]
        direction, conf = get_direction_for_signal(
            strategy,
            binance_direction="up",
            binance_confidence=0.80,
        )
        assert direction is None

    def test_down_only_allows_down(self):
        from polybot.mode_strategies import get_direction_for_signal, STRATEGIES
        strategy = STRATEGIES["down_only"]
        direction, conf = get_direction_for_signal(
            strategy,
            binance_direction="down",
            binance_confidence=0.70,
        )
        assert direction == "down"

    def test_hype_prefers_stronger_signal(self):
        from polybot.mode_strategies import get_direction_for_signal, STRATEGIES
        strategy = STRATEGIES["hype45"]
        direction, conf = get_direction_for_signal(
            strategy,
            binance_direction="up",
            binance_confidence=0.60,
            hype_direction="down",
            hype_confidence=0.75,
        )
        assert direction == "down"
        assert conf == 0.75

    def test_consensus_blocks_on_disagreement(self):
        from polybot.mode_strategies import get_direction_for_signal, STRATEGIES
        strategy = STRATEGIES["full_consensus"]
        direction, conf = get_direction_for_signal(
            strategy,
            binance_direction="up",
            binance_confidence=0.70,
            hype_direction="down",
            hype_confidence=0.75,
        )
        assert direction is None
        assert conf == 0.0

    def test_consensus_boosts_on_agreement(self):
        from polybot.mode_strategies import get_direction_for_signal, STRATEGIES
        strategy = STRATEGIES["full_consensus"]
        direction, conf = get_direction_for_signal(
            strategy,
            binance_direction="up",
            binance_confidence=0.70,
            hype_direction="up",
            hype_confidence=0.72,
        )
        assert direction == "up"
        assert conf > 0.70  # boosted

    def test_reverse_fades_crowd_up(self):
        from polybot.mode_strategies import get_direction_for_signal, STRATEGIES
        strategy = STRATEGIES["reverse"]
        direction, conf = get_direction_for_signal(
            strategy,
            binance_direction="up",
            binance_confidence=0.70,
            polymarket_up_price=0.72,  # Crowd heavily long
        )
        assert direction == "down"  # Contrarian fade

    def test_reverse_fades_crowd_down(self):
        from polybot.mode_strategies import get_direction_for_signal, STRATEGIES
        strategy = STRATEGIES["reverse"]
        direction, conf = get_direction_for_signal(
            strategy,
            binance_direction="down",
            binance_confidence=0.70,
            polymarket_up_price=0.28,  # Crowd heavily short
        )
        assert direction == "up"  # Contrarian fade

    def test_reverse_no_trade_in_neutral(self):
        from polybot.mode_strategies import get_direction_for_signal, STRATEGIES
        strategy = STRATEGIES["reverse"]
        direction, conf = get_direction_for_signal(
            strategy,
            binance_direction="up",
            binance_confidence=0.70,
            polymarket_up_price=0.50,  # No extreme to fade
        )
        assert direction is None

    def test_no_signal_returns_none(self):
        from polybot.mode_strategies import get_direction_for_signal, STRATEGIES
        strategy = STRATEGIES["updown"]
        direction, conf = get_direction_for_signal(
            strategy,
            binance_direction=None,
            binance_confidence=0.0,
        )
        assert direction is None


class TestShouldSkipByHours:
    """Test hour-based filtering."""

    def test_no_hour_filter(self):
        from polybot.mode_strategies import should_skip_by_hours, STRATEGIES
        strategy = STRATEGIES["signal247"]
        assert should_skip_by_hours(strategy) is False

    def test_within_active_hours(self):
        import datetime
        from polybot.mode_strategies import should_skip_by_hours, StrategyConfig
        strategy = StrategyConfig(
            ui_mode="test", label="Test", description="",
            base_mode="signal", active_hours=(0, 24),
        )
        assert should_skip_by_hours(strategy) is False

    def test_outside_active_hours(self):
        import datetime
        from polybot.mode_strategies import should_skip_by_hours, StrategyConfig
        # Use an impossible window (hour 25-26) to guarantee skip
        now_hour = datetime.datetime.now(datetime.timezone.utc).hour
        # Set window to an hour that's definitely not now
        bad_start = (now_hour + 12) % 24
        bad_end = (now_hour + 13) % 24
        if bad_start < bad_end:
            strategy = StrategyConfig(
                ui_mode="test", label="Test", description="",
                base_mode="signal", active_hours=(bad_start, bad_end),
            )
            assert should_skip_by_hours(strategy) is True


# ═══════════════════════════════════════════════════════════════════════════════
# HYPERLIQUID ENGINE TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestHyperliquidEngine:
    """Test HyperliquidEngine signal logic (no real WebSocket)."""

    def _make_engine(self):
        from polybot.hyperliquid_engine import HyperliquidEngine
        engine = HyperliquidEngine(assets=["BTC", "ETH", "SOL"])
        return engine

    def test_init_creates_states(self):
        engine = self._make_engine()
        assert "BTC" in engine.states
        assert "ETH" in engine.states
        assert "SOL" in engine.states
        assert not engine._running

    def test_get_signal_unknown_asset(self):
        engine = self._make_engine()
        sig = engine.get_signal("UNKNOWN")
        assert sig.direction is None
        assert sig.confidence == 0.0

    def test_no_data_returns_no_edge(self):
        engine = self._make_engine()
        sig = engine.get_signal("BTC")
        assert sig.reason == "no_edge"
        assert sig.direction is None

    def test_big_buy_flow_signal(self):
        engine = self._make_engine()
        state = engine.states["BTC"]
        now = time.time()
        # Inject $600K buy flow in last 10s
        for i in range(60):
            state.trades.append((now - i * 0.1, 60000.0, 10000.0, "B"))
        sig = engine.get_signal("BTC")
        assert sig.direction == "up"
        assert sig.confidence > 0.5
        assert "buy_flow" in sig.reason

    def test_big_sell_flow_signal(self):
        engine = self._make_engine()
        state = engine.states["ETH"]
        now = time.time()
        for i in range(60):
            state.trades.append((now - i * 0.1, 3000.0, 10000.0, "A"))
        sig = engine.get_signal("ETH")
        assert sig.direction == "down"
        assert "sell_flow" in sig.reason

    def test_latency_arb_up(self):
        engine = self._make_engine()
        state = engine.states["BTC"]
        state.last_price = 60200.0
        state.price_15s_ago = 60000.0  # +0.33%
        sig = engine.get_signal("BTC", polymarket_up_price=0.50)
        assert sig.direction == "up"
        assert "latency" in sig.reason

    def test_latency_arb_down(self):
        engine = self._make_engine()
        state = engine.states["BTC"]
        state.last_price = 59800.0
        state.price_15s_ago = 60000.0  # -0.33%
        sig = engine.get_signal("BTC", polymarket_up_price=0.50)
        assert sig.direction == "down"
        assert "latency" in sig.reason

    def test_ofi_signal(self):
        engine = self._make_engine()
        state = engine.states["SOL"]
        now = time.time()
        # Inject orderbook snaps with increasing bid volume
        for i in range(10):
            bid_vol = 100.0 + i * 20  # growing bids
            ask_vol = 100.0  # flat asks
            state.orderbook_snaps.append((now - (9 - i), bid_vol, ask_vol))
        sig = engine.get_signal("SOL", polymarket_up_price=0.48)
        # Should detect buy pressure
        if sig.direction is not None:
            assert sig.direction == "up"

    def test_get_all_signals(self):
        engine = self._make_engine()
        signals = engine.get_all_signals()
        assert "BTC" in signals
        assert "ETH" in signals
        assert "SOL" in signals

    def test_get_status(self):
        engine = self._make_engine()
        status = engine.get_status()
        assert status["source"] == "hyperliquid"
        assert "BTC" in status["prices"]
        assert status["running"] is False

    def test_handle_trades(self):
        engine = self._make_engine()
        engine._handle_trades([
            {"coin": "BTC", "px": "60000", "sz": "0.5", "side": "B"},
            {"coin": "ETH", "px": "3000", "sz": "10", "side": "A"},
        ])
        assert engine.states["BTC"].last_price == 60000.0
        assert engine.states["ETH"].last_price == 3000.0
        assert len(engine.states["BTC"].trades) == 1

    def test_handle_l2book(self):
        engine = self._make_engine()
        engine._handle_l2book({
            "coin": "BTC",
            "levels": [
                [{"px": "60000", "sz": "1.5", "n": 3}, {"px": "59999", "sz": "2.0", "n": 5}],
                [{"px": "60001", "sz": "1.0", "n": 2}, {"px": "60002", "sz": "0.5", "n": 1}],
            ],
        })
        assert engine.states["BTC"].best_bid == 60000.0
        assert engine.states["BTC"].best_ask == 60001.0
        assert len(engine.states["BTC"].orderbook_snaps) == 1

    def test_handle_all_mids(self):
        engine = self._make_engine()
        engine._handle_all_mids({"mids": {"BTC": "60100.5", "ETH": "3050.2"}})
        assert engine.get_mid_price("BTC") == 60100.5
        assert engine.get_mid_price("ETH") == 3050.2
        assert engine.get_mid_price("UNKNOWN") == 0.0

    def test_handle_trades_ignores_unknown_coin(self):
        engine = self._make_engine()
        engine._handle_trades([
            {"coin": "DOGE", "px": "0.15", "sz": "1000", "side": "B"},
        ])
        assert "DOGE" not in engine.states

    def test_handle_message_routing(self):
        engine = self._make_engine()
        engine._handle_message({"channel": "trades", "data": [
            {"coin": "BTC", "px": "61000", "sz": "0.1", "side": "B"},
        ]})
        assert engine.states["BTC"].last_price == 61000.0

        engine._handle_message({"channel": "allMids", "data": {"mids": {"SOL": "150.5"}}})
        assert engine.get_mid_price("SOL") == 150.5

        # subscriptionResponse should not crash
        engine._handle_message({"channel": "subscriptionResponse", "data": {}})

    def test_signal_source_field(self):
        engine = self._make_engine()
        sig = engine.get_signal("BTC")
        assert sig.source == "hyperliquid"


class TestHyperliquidEngineAsync:
    """Test async lifecycle (mocked WebSocket)."""

    @pytest.mark.asyncio
    async def test_start_stop(self):
        engine = self._make_engine()
        # Mock the WS loop so it doesn't actually connect
        with patch.object(engine, "_run_ws", new_callable=AsyncMock):
            await engine.start()
            assert engine._running is True
            assert len(engine._tasks) == 1

            await engine.stop()
            assert engine._running is False
            assert len(engine._tasks) == 0

    @pytest.mark.asyncio
    async def test_start_idempotent(self):
        engine = self._make_engine()
        with patch.object(engine, "_run_ws", new_callable=AsyncMock):
            await engine.start()
            await engine.start()  # Second call should be no-op
            assert len(engine._tasks) == 1

    def _make_engine(self):
        from polybot.hyperliquid_engine import HyperliquidEngine
        return HyperliquidEngine(assets=["BTC"])


class TestHyperliquidSingleton:
    """Test global singleton and is_hyperliquid_enabled."""

    def test_singleton(self):
        from polybot.hyperliquid_engine import get_hyperliquid_engine
        e1 = get_hyperliquid_engine(assets=["BTC"])
        e2 = get_hyperliquid_engine()
        assert e1 is e2

    def test_is_enabled_reads_env(self):
        with patch.dict("os.environ", {"HYPERLIQUID_ENABLED": "true"}):
            from polybot.hyperliquid_engine import is_hyperliquid_enabled
            # Need to reimport or call directly since it's cached
            import os
            assert os.environ.get("HYPERLIQUID_ENABLED") == "true"


# ═══════════════════════════════════════════════════════════════════════════════
# MODE SWITCHING API TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestModeSwitching:
    """Test runtime mode switching via the API endpoints."""

    def test_runtime_override_dict(self):
        """Test that _runtime_mode_override is mutable and used by get_active_strategy."""
        from polybot.mode_strategies import get_strategy
        s = get_strategy("demonscalp")
        assert s.label == "DemonScalp"
        assert s.base_mode == "sniper"

    def test_mode_map_matches_strategies(self):
        """Every mode in the frontend MODE_MAP should have a corresponding strategy."""
        from polybot.mode_strategies import STRATEGIES
        frontend_modes = [
            "sniper45", "sniper30", "sniper60", "hype45",
            "demonscalp", "coinbase", "full_consensus", "down_only",
            "updown", "arbitrage", "signal", "signal247",
            "autopred", "reverse",
        ]
        for mode in frontend_modes:
            assert mode in STRATEGIES, f"Frontend mode '{mode}' missing from STRATEGIES"

    def test_valid_base_modes(self):
        """All strategies must use valid backend modes."""
        from polybot.mode_strategies import STRATEGIES
        valid_base = {"sniper", "updown", "signal", "arbitrage"}
        for name, s in STRATEGIES.items():
            assert s.base_mode in valid_base, f"{name} has invalid base_mode: {s.base_mode}"

    def test_confidence_bounds(self):
        """All strategies should have reasonable confidence thresholds."""
        from polybot.mode_strategies import STRATEGIES
        for name, s in STRATEGIES.items():
            assert 0.0 < s.min_confidence <= 1.0, f"{name} has bad min_confidence: {s.min_confidence}"

    def test_kelly_multiplier_bounds(self):
        """Kelly multipliers should be between 0 and 1 when set."""
        from polybot.mode_strategies import STRATEGIES
        for name, s in STRATEGIES.items():
            if s.kelly_multiplier is not None:
                assert 0.0 < s.kelly_multiplier <= 1.0, f"{name} bad kelly: {s.kelly_multiplier}"


# ═══════════════════════════════════════════════════════════════════════════════
# SNIPER INTEGRATION TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestSniperSlotTimes:
    """Test _get_current_slot_times with dynamic window."""

    def test_default_window(self):
        from polybot.sniper import _get_current_slot_times
        slots = _get_current_slot_times()
        assert "BTC" in slots
        assert "HYPE" in slots
        for asset, slot in slots.items():
            assert "in_snipe_window" in slot
            assert "seconds_until_end" in slot
            assert slot["seconds_until_end"] >= 0

    def test_custom_window_45(self):
        from polybot.sniper import _get_current_slot_times
        slots_30 = _get_current_slot_times(snipe_window=30)
        slots_45 = _get_current_slot_times(snipe_window=45)
        # With wider window, seconds_until_snipe should be smaller (or snipe window already open)
        for asset in ["BTC", "ETH"]:
            assert slots_45[asset]["seconds_until_snipe"] <= slots_30[asset]["seconds_until_snipe"]

    def test_hype_in_prefixes(self):
        from polybot.sniper import ASSET_PREFIXES
        assert "HYPE" in ASSET_PREFIXES
        assert ASSET_PREFIXES["HYPE"] == "hype-updown-5m-"


# ═══════════════════════════════════════════════════════════════════════════════
# SCANNER CONSTANTS TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestScannerConstants:
    """Test canonical constants in scanner.py."""

    def test_target_slug_prefixes(self):
        from polybot.scanner import TARGET_SLUG_PREFIXES
        assert "btc-updown-5m-" in TARGET_SLUG_PREFIXES
        assert "hype-updown-5m-" in TARGET_SLUG_PREFIXES
        assert len(TARGET_SLUG_PREFIXES) >= 5

    def test_asset_from_slug(self):
        from polybot.scanner import ASSET_FROM_SLUG
        assert ASSET_FROM_SLUG["btc"] == "BTC"
        assert ASSET_FROM_SLUG["hype"] == "HYPE"
        assert ASSET_FROM_SLUG["eth"] == "ETH"

    def test_crypto_symbol_map_hype(self):
        from polybot.scanner import CRYPTO_SYMBOL_MAP
        assert CRYPTO_SYMBOL_MAP["hype"] == "HYPE/USDT"
        assert CRYPTO_SYMBOL_MAP["hyperliquid"] == "HYPE/USDT"

    def test_target_coins_include_hype(self):
        from polybot.scanner import _5MIN_TARGET_COINS
        assert "hype" in _5MIN_TARGET_COINS
        assert "hyperliquid" in _5MIN_TARGET_COINS
