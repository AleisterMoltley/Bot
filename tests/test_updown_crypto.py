"""Tests for the Up/Down Crypto compounding and whale tracking modules."""


class TestAggressiveCompounder:
    """Test AggressiveCompounder class."""

    def test_compounder_initialization(self):
        """Test compounder initializes with correct defaults."""
        from polybot.compounding import AggressiveCompounder

        compounder = AggressiveCompounder()
        assert compounder.reinvest_pct == 0.68
        assert compounder.full_kelly_ev_threshold == 0.12
        assert compounder.enabled is False
        assert compounder.total_compounded == 0.0

    def test_compounder_enable_disable(self):
        """Test enable/disable functionality."""
        from polybot.compounding import AggressiveCompounder

        compounder = AggressiveCompounder()
        assert compounder.enabled is False

        compounder.enable()
        assert compounder.enabled is True

        compounder.disable()
        assert compounder.enabled is False

    def test_compound_when_disabled(self):
        """Test compounding when disabled returns balance + pnl."""
        from polybot.compounding import AggressiveCompounder

        compounder = AggressiveCompounder()
        # Disabled - should just add pnl to balance
        result = compounder.compound(pnl=100, current_balance=1000)
        assert result == 1100

    def test_compound_when_enabled_profit(self):
        """Test compounding with profit reinvests 68%."""
        from polybot.compounding import AggressiveCompounder

        compounder = AggressiveCompounder()
        compounder.enable()

        # Enabled with profit - should reinvest 68%
        result = compounder.compound(pnl=100, current_balance=1000)
        assert result == 1068  # 1000 + (100 * 0.68)
        assert compounder.total_compounded == 68
        assert compounder.compound_count == 1

    def test_compound_when_enabled_loss(self):
        """Test compounding with loss just subtracts."""
        from polybot.compounding import AggressiveCompounder

        compounder = AggressiveCompounder()
        compounder.enable()

        # Loss - no compounding, just add negative pnl
        result = compounder.compound(pnl=-50, current_balance=1000)
        assert result == 950

    def test_get_status(self):
        """Test get_status returns correct data."""
        from polybot.compounding import AggressiveCompounder

        compounder = AggressiveCompounder()
        compounder.enable()
        compounder.compound(pnl=100, current_balance=1000)

        status = compounder.get_status()
        assert status["enabled"] is True
        assert status["reinvest_pct"] == 0.68
        assert status["total_compounded"] == 68
        assert status["compound_count"] == 1


class TestGlobalCompounder:
    """Test global compounder functions."""

    def test_get_compounder_singleton(self):
        """Test get_compounder returns singleton."""
        from polybot.compounding import get_compounder

        c1 = get_compounder()
        c2 = get_compounder()
        assert c1 is c2

    def test_enable_compounding(self):
        """Test enable_compounding function."""
        from polybot.compounding import enable_compounding, get_compounder

        compounder = enable_compounding()
        assert compounder.enabled is True
        assert get_compounder().enabled is True


class TestUpDownCryptoFilter:
    """Test Up/Down crypto market filter."""

    def test_is_up_down_crypto_market_btc(self):
        """Test filter detects BTC up/down market."""
        from polybot.scanner import MaxProfitScanner

        scanner = MaxProfitScanner()
        market = {"question": "Bitcoin up or down in the next hour?"}
        assert scanner.is_up_down_crypto_market(market) is True

    def test_is_up_down_crypto_market_eth(self):
        """Test filter detects ETH up/down market."""
        from polybot.scanner import MaxProfitScanner

        scanner = MaxProfitScanner()
        market = {"question": "Ethereum up or down by 8pm?"}
        assert scanner.is_up_down_crypto_market(market) is True

    def test_is_up_down_crypto_market_sol(self):
        """Test filter detects SOL up/down market."""
        from polybot.scanner import MaxProfitScanner

        scanner = MaxProfitScanner()
        market = {"question": "SOL up or down today?"}
        assert scanner.is_up_down_crypto_market(market) is True

    def test_is_up_down_crypto_market_xrp_will_hit(self):
        """Test filter detects XRP 'will hit' market."""
        from polybot.scanner import MaxProfitScanner

        scanner = MaxProfitScanner()
        market = {"question": "XRP will hit $1.50 today?"}
        assert scanner.is_up_down_crypto_market(market) is True

    def test_is_up_down_crypto_market_rejects_non_crypto(self):
        """Test filter rejects non-crypto markets."""
        from polybot.scanner import MaxProfitScanner

        scanner = MaxProfitScanner()
        market = {"question": "Will Trump win the election?"}
        assert scanner.is_up_down_crypto_market(market) is False

    def test_is_up_down_crypto_market_rejects_long_duration(self):
        """Test filter rejects markets with duration > 240 minutes."""
        from polybot.scanner import MaxProfitScanner

        scanner = MaxProfitScanner()
        market = {
            "question": "Bitcoin up or down by end of week?",
            "duration": 500,  # > 240 minutes
        }
        assert scanner.is_up_down_crypto_market(market) is False

    def test_is_up_down_crypto_market_rejects_crypto_without_pattern(self):
        """Test filter rejects crypto markets without up/down pattern."""
        from polybot.scanner import MaxProfitScanner

        scanner = MaxProfitScanner()
        market = {"question": "Bitcoin price prediction for tomorrow?"}
        assert scanner.is_up_down_crypto_market(market) is False

    def test_scanner_up_down_only_mode(self):
        """Test scanner with up_down_only mode."""
        from polybot.scanner import MaxProfitScanner

        scanner = MaxProfitScanner(up_down_only=True)
        assert scanner.up_down_only is True


class TestLiquiditySizing:
    """Test liquidity-based position sizing."""

    def test_position_size_with_liquidity(self):
        """Test position sizing respects liquidity limits."""
        from polybot.risk import calculate_position_size_with_liquidity

        # High EV trade with limited liquidity
        size = calculate_position_size_with_liquidity(
            ev=0.15,
            edge=0.15,
            bankroll=10000,
            liquidity=1000,
            use_full_kelly=True,
        )

        # Should be capped at 35% of liquidity = $350
        assert size <= 350

    def test_position_size_full_kelly_requires_high_ev(self):
        """Test Full Kelly requires >12% EV."""
        from polybot.risk import calculate_position_size_with_liquidity

        # Low EV trade with full kelly enabled
        size = calculate_position_size_with_liquidity(
            ev=0.10,  # Below 12%
            edge=0.10,
            bankroll=10000,
            liquidity=100000,
            use_full_kelly=True,
        )

        # Should be 0 since EV < 12%
        assert size == 0.0


class TestWhaleCopyTracker:
    """Test whale copy tracking functionality."""

    def test_tracker_initialization(self):
        """Test tracker initializes with correct defaults."""
        from polybot.whale_tracker import WhaleCopyTracker

        tracker = WhaleCopyTracker()
        assert tracker.min_bet_usd == 5000
        assert tracker.enabled is False
        assert tracker.tracked_bets == []

    def test_tracker_enable_disable(self):
        """Test enable/disable functionality."""
        from polybot.whale_tracker import WhaleCopyTracker

        tracker = WhaleCopyTracker()
        tracker.enable()
        assert tracker.enabled is True

        tracker.disable()
        assert tracker.enabled is False

    def test_is_up_down_crypto_market(self):
        """Test market filter for whale tracker."""
        from polybot.whale_tracker import WhaleCopyTracker

        tracker = WhaleCopyTracker()

        # Should pass - crypto keyword + up/down pattern
        assert tracker.is_up_down_crypto_market("Bitcoin up or down today?") is True
        assert tracker.is_up_down_crypto_market("ETH will hit $4000 today?") is True

        # Should fail - no crypto or no pattern
        assert tracker.is_up_down_crypto_market("Trump election odds") is False
        assert tracker.is_up_down_crypto_market("Bitcoin price prediction") is False

    def test_format_whale_bets_empty(self):
        """Test formatting with no bets."""
        from polybot.whale_tracker import WhaleCopyTracker

        tracker = WhaleCopyTracker()
        result = tracker.format_whale_bets()
        assert "No whale bets" in result

    def test_get_status(self):
        """Test get_status returns correct data."""
        from polybot.whale_tracker import WhaleCopyTracker

        tracker = WhaleCopyTracker()
        tracker.enable()

        status = tracker.get_status()
        assert status["enabled"] is True
        assert status["min_bet_usd"] == 5000
        assert status["tracked_bets"] == 0


class TestPyramidCompounder:
    """Test PyramidCompounder class with streak multipliers."""

    def test_pyramid_initialization(self):
        """Test pyramid compounder initializes with correct defaults."""
        from polybot.compounding import PyramidCompounder

        compounder = PyramidCompounder()
        assert compounder.reinvest_pct == 0.30  # Conservative 30% reinvest
        assert compounder.enabled is False
        assert compounder.win_streak == 0
        assert compounder.total_compounded == 0.0

    def test_pyramid_enable_disable(self):
        """Test enable/disable functionality."""
        from polybot.compounding import PyramidCompounder

        compounder = PyramidCompounder()
        assert compounder.enabled is False

        compounder.enable()
        assert compounder.enabled is True

        compounder.disable()
        assert compounder.enabled is False

    def test_pyramid_compound_when_disabled(self):
        """Test compounding when disabled returns balance + pnl."""
        from polybot.compounding import PyramidCompounder

        compounder = PyramidCompounder()
        result = compounder.compound(pnl=100, balance=1000)
        assert result == 1100

    def test_pyramid_compound_with_loss_resets_streak(self):
        """Test that losses reset the win streak."""
        from polybot.compounding import PyramidCompounder

        compounder = PyramidCompounder()
        compounder.enable()
        compounder.win_streak = 5

        result = compounder.compound(pnl=-50, balance=1000)
        assert result == 950
        assert compounder.win_streak == 0

    def test_pyramid_compound_with_low_streak(self):
        """Test compounding with starting 0-1 win streak uses 1.5x multiplier.

        When starting with streak=0, the first win increments to streak=1,
        which uses 1.5x multiplier. When starting with streak=1, the win
        increments to streak=2, which still uses 1.5x multiplier.
        """
        from polybot.compounding import PyramidCompounder

        compounder = PyramidCompounder()
        compounder.enable()
        compounder.win_streak = 1  # After win, becomes 2 → 1.5x

        result = compounder.compound(pnl=100, balance=1000)
        # Win increments streak to 2, 2 < 3 → 1.5x multiplier
        # 100 * 0.30 * 1.5 = 45
        expected = 1000 + (100 * 0.30 * 1.5)
        assert result == expected
        assert compounder.win_streak == 2

    def test_pyramid_compound_with_high_streak(self):
        """Test compounding with 2+ win streak (becomes 3+) uses 2.0x multiplier."""
        from polybot.compounding import PyramidCompounder

        compounder = PyramidCompounder()
        compounder.enable()
        compounder.win_streak = 2  # After win, becomes 3 → 2.0x

        result = compounder.compound(pnl=100, balance=1000)
        # Win increments streak to 3, 3 >= 3 → 2.0x multiplier
        # 100 * 0.30 * 2.0 = 60
        expected = 1000 + (100 * 0.30 * 2.0)
        assert result == expected
        assert compounder.win_streak == 3

    def test_pyramid_get_status(self):
        """Test get_status returns correct data."""
        from polybot.compounding import PyramidCompounder

        compounder = PyramidCompounder()
        compounder.enable()
        compounder.win_streak = 4

        status = compounder.get_status()
        assert status["enabled"] is True
        assert status["reinvest_pct"] == 0.30  # Conservative 30% reinvest
        assert status["win_streak"] == 4
        assert status["current_multiplier"] == 2.0


class TestUpDownCryptoScanner:
    """Test UpDownCryptoScanner class with ULTIMATE 5-MIN CHASER V2 filter."""

    def test_scanner_initialization(self):
        """Test scanner initializes correctly."""
        from polybot.scanner_updown import UpDownCryptoScanner

        scanner = UpDownCryptoScanner()
        assert scanner.up_down_only is True
        assert scanner.filtered_count == 0

    def test_is_chaser_market_btc(self):
        """Test filter detects BTC 5-minute chaser market with proper volume/liquidity."""
        from polybot.scanner_updown import UpDownCryptoScanner

        scanner = UpDownCryptoScanner()
        # Must have: crypto + "up or down" + 5 min pattern + volume >= 8000 + liquidity >= 3000
        market = {
            "question": "Bitcoin up or down in 5 min?",
            "duration": 300,
            "volume": 10000,
            "liquidity": 5000,
        }
        assert scanner._is_chaser_market(market) is True

    def test_is_chaser_market_eth(self):
        """Test filter detects ETH 5-minute chaser market."""
        from polybot.scanner_updown import UpDownCryptoScanner

        scanner = UpDownCryptoScanner()
        market = {
            "question": "ETH up or down 5-minute prediction",
            "duration": 300,
            "volume": 10000,
            "liquidity": 5000,
        }
        assert scanner._is_chaser_market(market) is True

    def test_is_chaser_market_sol_variant(self):
        """Test filter detects SOL '5 minutes' variant."""
        from polybot.scanner_updown import UpDownCryptoScanner

        scanner = UpDownCryptoScanner()
        market = {
            "question": "Solana up or down 5 minutes",
            "duration": 300,
            "volume": 10000,
            "liquidity": 5000,
        }
        assert scanner._is_chaser_market(market) is True

    def test_is_5_minute_market_xrp_duration_seconds(self):
        """Test filter rejects XRP market (not in CHASER_PATTERN regex)."""
        from polybot.scanner_updown import UpDownCryptoScanner

        scanner = UpDownCryptoScanner()
        market = {"question": "XRP 5 min price", "duration_seconds": 300}
        # XRP is not in CHASER_PATTERN regex (only bitcoin|btc|ethereum|eth|solana|sol)
        assert scanner._is_5_minute_market(market) is False

    def test_is_5_minute_market_rejects_15_min(self):
        """Test filter rejects 15-minute markets (duration > 300)."""
        from polybot.scanner_updown import UpDownCryptoScanner

        scanner = UpDownCryptoScanner()
        market = {
            "question": "Bitcoin up or down 15 min",
            "duration": 900,  # 15 minutes - rejected
            "volume": 10000,
            "liquidity": 5000,
        }
        assert scanner._is_5_minute_market(market) is False

    def test_is_5_minute_market_rejects_30_min(self):
        """Test filter rejects 30-minute markets (duration > 300)."""
        from polybot.scanner_updown import UpDownCryptoScanner

        scanner = UpDownCryptoScanner()
        market = {
            "question": "Bitcoin up or down 30 min",
            "duration": 1800,  # 30 minutes - rejected
            "volume": 10000,
            "liquidity": 5000,
        }
        assert scanner._is_5_minute_market(market) is False

    def test_is_5_minute_market_rejects_60_min(self):
        """Test filter rejects 60-minute (1 hour) markets."""
        from polybot.scanner_updown import UpDownCryptoScanner

        scanner = UpDownCryptoScanner()
        market = {
            "question": "Bitcoin up or down in the next hour?",
            "duration": 3600,  # 60 minutes - rejected (duration > 300)
            "volume": 10000,
            "liquidity": 5000,
        }
        assert scanner._is_5_minute_market(market) is False

    def test_is_5_minute_market_rejects_without_5min_pattern(self):
        """Test filter rejects markets without '5 min' pattern in title."""
        from polybot.scanner_updown import UpDownCryptoScanner

        scanner = UpDownCryptoScanner()
        market = {
            "question": "Bitcoin up or down today?",
            "duration": 300,  # Duration is 5 min, but title lacks 5min pattern
            "volume": 10000,
            "liquidity": 5000,
        }
        assert scanner._is_5_minute_market(market) is False

    def test_is_5_minute_market_rejects_non_crypto(self):
        """Test filter rejects non-crypto markets."""
        from polybot.scanner_updown import UpDownCryptoScanner

        scanner = UpDownCryptoScanner()
        market = {
            "question": "Trump 5 min polls up or down?",
            "duration": 300,
            "volume": 10000,
            "liquidity": 5000,
        }
        assert scanner._is_5_minute_market(market) is False

    def test_is_up_down_crypto_backward_compat(self):
        """Test _is_up_down_crypto calls _is_chaser_market for backward compatibility."""
        from polybot.scanner_updown import UpDownCryptoScanner

        scanner = UpDownCryptoScanner()
        market = {
            "question": "Bitcoin up or down 5 min?",
            "duration": 300,
            "volume": 10000,
            "liquidity": 5000,
        }
        # Both methods should return the same result
        assert scanner._is_up_down_crypto(market) == scanner._is_5_minute_market(market)

    def test_is_chaser_market_rejects_low_volume(self):
        """Test filter rejects markets with volume < 8000."""
        from polybot.scanner_updown import UpDownCryptoScanner

        scanner = UpDownCryptoScanner()
        market = {
            "question": "Bitcoin up or down 5 min?",
            "duration": 300,
            "volume": 5000,  # Below 8000 threshold
            "liquidity": 5000,
        }
        assert scanner._is_chaser_market(market) is False

    def test_is_chaser_market_rejects_low_liquidity(self):
        """Test filter rejects markets with liquidity < 3000."""
        from polybot.scanner_updown import UpDownCryptoScanner

        scanner = UpDownCryptoScanner()
        market = {
            "question": "Bitcoin up or down 5 min?",
            "duration": 300,
            "volume": 10000,
            "liquidity": 2000,  # Below 3000 threshold
        }
        assert scanner._is_chaser_market(market) is False

    def test_get_status(self):
        """Test get_status returns correct 5-minute mode data."""
        from polybot.scanner_updown import UpDownCryptoScanner

        scanner = UpDownCryptoScanner()
        status = scanner.get_status()
        assert status["mode"] == "5-MINUTE EXCLUSIVE Mode"
        # supported_coins now returns target_symbols from config (default: BTC, ETH, SOL)
        assert "BTC" in status["supported_coins"]
        assert "ETH" in status["supported_coins"]
        assert "SOL" in status["supported_coins"]
        assert status["max_duration_minutes"] == 5
        assert status["max_duration_seconds"] == 300
        assert "5 min" in status["five_min_patterns"]
        assert "trend_filter_enabled" in status
        assert "websocket_active" in status


class TestTargetSymbolsConfig:
    """Test target_symbols configuration for scanner whitelist."""

    def test_default_target_symbols(self):
        """Test default target_symbols are BTC, ETH, SOL."""
        from polybot.config import Settings

        settings = Settings()
        assert settings.target_symbols == ["BTC", "ETH", "SOL"]

    def test_parse_target_symbols_from_string(self):
        """Test parsing target_symbols from comma-separated string."""
        from polybot.config import Settings

        settings = Settings(target_symbols="BTC,ETH,SOL,XRP")
        assert "BTC" in settings.target_symbols
        assert "ETH" in settings.target_symbols
        assert "SOL" in settings.target_symbols
        assert "XRP" in settings.target_symbols

    def test_parse_target_symbols_uppercase(self):
        """Test target_symbols are uppercased."""
        from polybot.config import Settings

        settings = Settings(target_symbols="btc,eth,sol")
        assert settings.target_symbols == ["BTC", "ETH", "SOL"]

    def test_scanner_uses_chaser_pattern(self):
        """Test scanner uses CHASER_PATTERN regex for filtering."""
        from polybot.scanner_updown import UpDownCryptoScanner

        scanner = UpDownCryptoScanner()
        # BTC should pass with proper pattern, volume, and liquidity
        btc_market = {
            "question": "Bitcoin up or down 5 min?",
            "duration": 300,
            "volume": 10000,
            "liquidity": 5000,
        }
        assert scanner._is_chaser_market(btc_market) is True

        # XRP not in CHASER_PATTERN regex
        xrp_market = {
            "question": "XRP up or down 5 min?",
            "duration": 300,
            "volume": 10000,
            "liquidity": 5000,
        }
        assert scanner._is_chaser_market(xrp_market) is False

    def test_scanner_rejects_politics(self):
        """Test scanner rejects political markets."""
        from polybot.scanner_updown import UpDownCryptoScanner

        scanner = UpDownCryptoScanner()
        politics_market = {
            "question": "Trump 5 min polls",
            "duration": 300,
            "volume": 10000,
            "liquidity": 5000,
        }
        assert scanner._is_5_minute_market(politics_market) is False


class TestKellyPositionCalculation:
    """Test Kelly position calculation for auto-trade feature."""

    def test_calculate_kelly_position_basic(self):
        """Test basic Kelly position calculation."""
        from polybot.risk import calculate_kelly_position

        # 10% edge, $100 bankroll, Half-Kelly (0.5)
        position = calculate_kelly_position(edge=0.10, bankroll=100.0, kelly_mult=0.5)
        # Expected: 0.10 * 4 * 0.5 * 100 = 20.0 USD
        assert position == 20.0

    def test_calculate_kelly_position_full_kelly(self):
        """Test Full Kelly position calculation."""
        from polybot.risk import calculate_kelly_position

        # 10% edge, $100 bankroll, Full-Kelly (1.0)
        position = calculate_kelly_position(edge=0.10, bankroll=100.0, kelly_mult=1.0)
        # Expected: 0.10 * 4 * 1.0 * 100 = 40.0 USD (but capped at 25% = 25.0)
        assert position == 25.0

    def test_calculate_kelly_position_zero_edge(self):
        """Test Kelly returns 0 for zero edge."""
        from polybot.risk import calculate_kelly_position

        position = calculate_kelly_position(edge=0.0, bankroll=100.0, kelly_mult=0.5)
        assert position == 0.0

    def test_calculate_kelly_position_negative_edge(self):
        """Test Kelly returns 0 for negative edge."""
        from polybot.risk import calculate_kelly_position

        position = calculate_kelly_position(edge=-0.05, bankroll=100.0, kelly_mult=0.5)
        assert position == 0.0

    def test_calculate_kelly_position_zero_bankroll(self):
        """Test Kelly returns 0 for zero bankroll."""
        from polybot.risk import calculate_kelly_position

        position = calculate_kelly_position(edge=0.10, bankroll=0.0, kelly_mult=0.5)
        assert position == 0.0


class TestAutoTradeConfig:
    """Test new auto-trade configuration fields."""

    def test_kelly_multiplier_default(self):
        """Test kelly_multiplier defaults to 0.5 (Half-Kelly)."""
        from polybot.config import Settings

        settings = Settings()
        assert settings.kelly_multiplier == 0.5

    def test_kelly_multiplier_validation(self):
        """Test kelly_multiplier validation (0.1-1.0 range)."""
        import os
        from polybot.config import Settings

        # Use environment variable to override kelly_multiplier
        os.environ["KELLY_MULTIPLIER"] = "1.0"
        try:
            settings = Settings(_env_file=None)
            assert settings.kelly_multiplier == 1.0
        finally:
            del os.environ["KELLY_MULTIPLIER"]

        os.environ["KELLY_MULTIPLIER"] = "0.1"
        try:
            settings = Settings(_env_file=None)
            assert settings.kelly_multiplier == 0.1
        finally:
            del os.environ["KELLY_MULTIPLIER"]

    def test_max_position_usd_default(self):
        """Test max_position_usd defaults to 50.0."""
        from polybot.config import Settings

        settings = Settings()
        assert settings.max_position_usd == 50.0

    def test_min_trade_usd_default(self):
        """Test min_trade_usd defaults to 5.0."""
        from polybot.config import Settings

        settings = Settings()
        assert settings.min_trade_usd == 5.0

    def test_auto_execute_default(self):
        """Test auto_execute defaults to True (production default).

        PATCH 2026: Changed from False to True for production-ready bot.
        """
        from polybot.config import Settings

        settings = Settings()
        assert settings.auto_execute is True  # PATCH 2026: Production default

    def test_execution_slippage_bps_default(self):
        """Test execution_slippage_bps defaults to 30."""
        from polybot.config import Settings

        settings = Settings()
        assert settings.execution_slippage_bps == 30


class TestEdgeEngineV2:
    """Test EdgeEngine v2 volatility-adjusted calculation."""

    def test_5min_volatility_adjusted_edge_basic(self):
        """Test basic volatility-adjusted edge calculation."""
        from polybot.edge_engine import EdgeEngine

        engine = EdgeEngine()
        market = {
            "tokens": [{"outcome": "yes", "price": 0.6}],
            "current_price": 100.0,
            "target_price": 100.0,
            "time_to_expiry_hours": 0.083,  # 5 minutes
            "implied_vol": 0.3,
            "symbol": "BTC",
        }

        edge = engine.get_5min_volatility_adjusted_edge(market, "up")
        # Edge should be non-negative
        assert edge >= 0.0
        assert edge <= 0.25  # Capped at MAX_EDGE_CAP

    def test_5min_volatility_adjusted_edge_direction_down(self):
        """Test volatility-adjusted edge for down direction."""
        from polybot.edge_engine import EdgeEngine

        engine = EdgeEngine()
        market = {
            "tokens": [{"outcome": "yes", "price": 0.4}],
            "current_price": 100.0,
            "target_price": 100.0,
            "time_to_expiry_hours": 0.083,
            "implied_vol": 0.3,
            "symbol": "ETH",
        }

        edge = engine.get_5min_volatility_adjusted_edge(market, "down")
        assert edge >= 0.0
        assert edge <= 0.25

    def test_5min_volatility_adjusted_edge_missing_price(self):
        """Test edge calculation handles missing current_price."""
        from polybot.edge_engine import EdgeEngine

        engine = EdgeEngine()
        market = {
            "tokens": [{"outcome": "yes", "price": 0.5}],
            "current_price": 0,  # Invalid
            "target_price": 100.0,
            "time_to_expiry_hours": 0.083,
            "implied_vol": 0.3,
        }

        edge = engine.get_5min_volatility_adjusted_edge(market, "up")
        assert edge == 0.0  # Should return 0 for invalid price
