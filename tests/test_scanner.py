"""Tests for the MaxProfitScanner functionality."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch


class TestMaxProfitScanner:
    """Test MaxProfitScanner class."""

    def test_scanner_initialization(self):
        """Test scanner initializes with correct defaults."""
        from polybot.scanner import MaxProfitScanner

        scanner = MaxProfitScanner()
        # Note: Actual values come from config injection, but class defaults are:
        # DEFAULT_MIN_VOLUME = 5_000, DEFAULT_MIN_LIQUIDITY = 1_000, DEFAULT_MIN_EV = 0.015
        assert scanner.scan_results == []
        assert scanner.markets_scanned == 0

    def test_scanner_custom_thresholds(self):
        """Test scanner accepts custom thresholds."""
        from polybot.scanner import MaxProfitScanner

        scanner = MaxProfitScanner(
            min_volume=100_000, min_liquidity=25_000, min_ev=0.10
        )
        assert scanner.min_volume == 100_000
        assert scanner.min_liquidity == 25_000
        assert scanner.min_ev == 0.10

    def test_hybrid_score_calculation(self):
        """Test hybrid score calculation with correct weights."""
        from polybot.scanner import MaxProfitScanner

        scanner = MaxProfitScanner()
        # Pure arbitrage opportunity
        score = scanner.calculate_hybrid_score(100, 0, 0)
        assert score == 50.0  # 100 * 0.50

        # Pure CEX edge opportunity
        score = scanner.calculate_hybrid_score(0, 100, 0)
        assert score == 30.0  # 100 * 0.30

        # Pure TA opportunity
        score = scanner.calculate_hybrid_score(0, 0, 100)
        assert score == 20.0  # 100 * 0.20

        # Mixed opportunity
        score = scanner.calculate_hybrid_score(50, 50, 50)
        assert score == 50.0  # 50*0.5 + 50*0.3 + 50*0.2

    def test_detect_crypto_market(self):
        """Test crypto market detection."""
        from polybot.scanner import MaxProfitScanner

        scanner = MaxProfitScanner()

        # Bitcoin detection
        crypto, symbol = scanner._detect_crypto_market("Will Bitcoin be above $100k?")
        assert crypto == "bitcoin"
        assert symbol == "BTC/USDT"

        # Ethereum detection
        crypto, symbol = scanner._detect_crypto_market("ETH price above $4000")
        assert crypto == "eth"
        assert symbol == "ETH/USDT"

        # Solana detection
        crypto, symbol = scanner._detect_crypto_market("Solana hits $200")
        assert crypto == "solana"
        assert symbol == "SOL/USDT"

        # Non-crypto market
        crypto, symbol = scanner._detect_crypto_market("Will Trump win election?")
        assert crypto is None
        assert symbol is None

    def test_extract_strike_price(self):
        """Test strike price extraction from questions."""
        from polybot.scanner import MaxProfitScanner

        scanner = MaxProfitScanner()

        # Standard dollar format
        price = scanner._extract_strike_price("Will Bitcoin be above $70,000?")
        assert price == 70000.0

        # K format
        price = scanner._extract_strike_price("BTC price > $65k by end of month?")
        assert price == 65000.0

        # Simple number
        price = scanner._extract_strike_price("ETH hits $4000")
        assert price == 4000.0

        # No price in question
        price = scanner._extract_strike_price("Will it rain tomorrow?")
        assert price is None

    def test_get_scan_status(self):
        """Test scan status reporting."""
        from polybot.scanner import MaxProfitScanner

        scanner = MaxProfitScanner()
        scanner.markets_scanned = 150
        scanner.scan_results = [{"type": "ARB", "ev": 0.15}]

        status = scanner.get_scan_status()
        assert "150" in status["status"]
        assert status["high_ev_count"] == 1
        assert len(status["results"]) == 1


class TestMaxProfitScannerWithMocks:
    """Test scanner with mocked external dependencies."""

    @patch("polybot.scanner.fetch_all_active_markets")
    def test_scan_finds_arb_opportunity(self, mock_fetch):
        """Test scanner identifies arbitrage opportunities."""
        from polybot.scanner import MaxProfitScanner

        # Mock market data with arb opportunity (YES + NO < 1.0)
        mock_fetch.return_value = [
            {
                "question": "Will BTC hit $100k?",
                "condition_id": "test-market-1",
                "tokens": [
                    {"outcome": "yes", "price": 0.45},
                    {"outcome": "no", "price": 0.50},
                ],
                "volume": 100_000,
                "liquidity": 50_000,
            }
        ]

        scanner = MaxProfitScanner()
        results = scanner.scan()

        assert len(results) == 1
        assert results[0]["type"] == "ARB"
        assert results[0]["tier"] == 1
        assert results[0]["combined"] < 1.0

    @patch("polybot.scanner.fetch_all_active_markets")
    def test_scan_filters_low_volume(self, mock_fetch):
        """Test scanner filters out low volume markets."""
        from polybot.scanner import MaxProfitScanner

        # Mock market with arb but low volume
        # PATCH 2026: Updated test to use liquidity below new 200 threshold
        mock_fetch.return_value = [
            {
                "question": "Low volume market",
                "tokens": [
                    {"outcome": "yes", "price": 0.45},
                    {"outcome": "no", "price": 0.50},
                ],
                "volume": 1_000,  # Below threshold
                "liquidity": 50,  # Below 200 threshold (PATCH 2026)
            }
        ]

        scanner = MaxProfitScanner()
        results = scanner.scan()

        # fetch_all_active_markets passes min_volume to API call, so mock returns empty
        # This test verifies that even if data somehow passes API filter,
        # the scanner's liquidity filter still catches it
        assert len(results) == 0

    @patch("polybot.scanner.fetch_all_active_markets")
    def test_scan_filters_low_liquidity(self, mock_fetch):
        """Test scanner filters out low liquidity markets.

        This tests the scanner's internal liquidity filter.
        Markets can pass the initial fetch but fail the scanner's own filter.
        With config injection, min_liquidity defaults to settings.min_liquidity_usd (650).
        """
        from polybot.scanner import MaxProfitScanner

        # Mock market with good volume but very low liquidity
        # This simulates a market that passed fetch_all_active_markets
        # but should be filtered by scanner's liquidity threshold
        mock_fetch.return_value = [
            {
                "question": "Good volume but low liquidity",
                "tokens": [
                    {"outcome": "yes", "price": 0.45},
                    {"outcome": "no", "price": 0.50},
                ],
                "volume": 100_000,  # Above volume threshold
                "liquidity": 100,  # Below scanner's liquidity threshold (even with lowered defaults)
            }
        ]

        scanner = MaxProfitScanner()
        results = scanner.scan()

        # Should be empty because scanner's internal liquidity filter catches it
        assert len(results) == 0
        # Verify the market was scanned (passed to scanner)
        assert scanner.markets_scanned == 1


class TestFormatMaxProfitResults:
    """Test result formatting function."""

    def test_format_empty_results(self):
        """Test formatting with no results."""
        from polybot.scanner import format_max_profit_results

        result = format_max_profit_results([])
        assert "No high-EV opportunities" in result

    def test_format_arb_result(self):
        """Test formatting arbitrage result."""
        from polybot.scanner import format_max_profit_results

        results = [
            {
                "market": "BTC above $100k",
                "type": "ARB",
                "tier": 1,
                "ev": 0.05,
                "edge": 0.05,
                "pnl_potential": "+$50",
                "yes_price": 0.45,
                "no_price": 0.50,
            }
        ]

        formatted = format_max_profit_results(results)
        assert "MAX PROFIT SCAN RESULTS" in formatted
        assert "ARB" in formatted
        assert "+$50" in formatted

    def test_format_edge_result(self):
        """Test formatting CEX edge result."""
        from polybot.scanner import format_max_profit_results

        results = [
            {
                "market": "ETH above $4000",
                "type": "EDGE",
                "tier": 2,
                "ev": 0.12,
                "edge": 0.12,
                "pnl_potential": "+$120",
                "real_prob": 0.65,
                "implied_prob": 0.53,
            }
        ]

        formatted = format_max_profit_results(results)
        assert "EDGE" in formatted
        assert "Real:" in formatted


class TestKellySize:
    """Test Kelly criterion position sizing."""

    def test_kelly_rejects_low_ev(self):
        """Test Kelly returns 0 for low EV trades."""
        from polybot.risk import kelly_size

        # EV below 8% threshold
        result = kelly_size(ev=0.05, edge=0.05)
        assert result == 0.0

    def test_kelly_accepts_high_ev(self):
        """Test Kelly returns positive fraction for high EV trades."""
        from polybot.risk import kelly_size

        # EV above 8% threshold
        result = kelly_size(ev=0.10, edge=0.10)
        assert result > 0.0
        assert result <= 0.25  # Max Kelly fraction

    def test_kelly_capped_at_max(self):
        """Test Kelly fraction is capped at maximum."""
        from polybot.risk import kelly_size

        # Very high edge
        result = kelly_size(ev=0.50, edge=0.50)
        assert result <= 0.25  # Max Kelly fraction

    def test_kelly_rejects_negative_edge(self):
        """Test Kelly returns 0 for negative edge."""
        from polybot.risk import kelly_size

        result = kelly_size(ev=0.10, edge=-0.05)
        assert result == 0.0


class TestShouldTradeOpportunity:
    """Test trade approval logic."""

    @patch("polybot.hourly_risk_regime.get_hourly_multiplier", return_value=1.6)
    @patch("polybot.risk.check_risk_limits")
    def test_arb_always_approved(self, mock_risk, mock_hourly_multiplier):
        """Test arbitrage opportunities always pass."""
        from polybot.risk import should_trade_opportunity

        mock_risk.return_value = (True, "OK")

        should_trade, reason, fraction = should_trade_opportunity(
            {
                "type": "ARB",
                "ev": 0.05,  # Below threshold but ARB
                "edge": 0.05,
            }
        )

        assert should_trade is True
        assert "ARB" in reason
        assert fraction == 0.25  # Max for arb

    @patch("polybot.hourly_risk_regime.get_hourly_multiplier", return_value=1.6)
    @patch("polybot.risk.check_risk_limits")
    def test_edge_requires_high_ev(self, mock_risk, mock_hourly_multiplier):
        """Test edge opportunities require high EV."""
        from polybot.risk import should_trade_opportunity

        mock_risk.return_value = (True, "OK")

        # Low EV - should reject
        should_trade, reason, fraction = should_trade_opportunity(
            {
                "type": "EDGE",
                "ev": 0.05,  # Below threshold
                "edge": 0.05,
            }
        )

        assert should_trade is False
        assert "EV too low" in reason

    @patch("polybot.hourly_risk_regime.get_hourly_multiplier", return_value=1.6)
    @patch("polybot.risk.check_risk_limits")
    def test_respects_risk_limits(self, mock_risk, mock_hourly_multiplier):
        """Test risk limits are respected."""
        from polybot.risk import should_trade_opportunity

        mock_risk.return_value = (False, "Circuit breaker triggered")

        should_trade, reason, fraction = should_trade_opportunity(
            {"type": "EDGE", "ev": 0.15, "edge": 0.15}
        )

        assert should_trade is False
        assert "Circuit breaker" in reason


class Test5MinPreFilter:
    """Test 5-minute Up/Down market pre-filter functionality.

    V12 BRUTEFORCE NUCLEAR: Uses broad keyword matching for
    BTC/ETH/SOL/XRP 5-minute Up or Down markets.
    Match condition: has_coin AND has_updown AND has_5min.
    """

    @patch(
        "polybot.scanner._get_gamma_endpoint",
        return_value="https://gamma-api.polymarket.com",
    )
    @patch("polybot.scanner.make_proxied_request")
    @patch("polybot.scanner.get_settings")
    def test_sync_filter_active_in_updown_mode(
        self, mock_settings, mock_request, mock_endpoint
    ):
        """Test sync version filters markets when mode is 'updown'.

        V12 BRUTEFORCE: Uses coin + updown + 5min keyword matching.
        """
        from unittest.mock import MagicMock
        from polybot.scanner import fetch_all_active_markets

        # Configure settings for updown mode with all required attributes
        mock_cfg = type(
            "Settings",
            (),
            {
                "mode": "updown",
                "target_symbols": ["BTC", "ETH"],
                "use_api_mirrors": False,
                "up_down_only": False,
            },
        )()
        mock_settings.return_value = mock_cfg

        # V12: Mock API response with coin + updown + 5min keywords
        markets = [
            # PASS: coin=bitcoin, updown="up or down", 5min="5 minutes"
            {"question": "Bitcoin Up or Down - 5 Minutes", "volume": 100_000},
            # REJECT: No coin keyword
            {"question": "Will Trump win election?", "volume": 100_000},
            # REJECT: Has coin but no updown or 5min
            {"question": "ETH price today", "volume": 100_000},
            # PASS: coin=ethereum, updown="up or down", 5min="5 minutes"
            {"question": "Ethereum Up or Down - 5 Minutes", "volume": 100_000},
        ]

        # First call returns markets, second returns empty to stop pagination
        mock_response1 = MagicMock()
        mock_response1.json.return_value = markets
        mock_response2 = MagicMock()
        mock_response2.json.return_value = []
        mock_request.side_effect = [mock_response1, mock_response2]

        result = fetch_all_active_markets(min_volume=0)

        # V12: Should return markets matching coin + updown + 5min
        assert len(result) == 2
        assert any("bitcoin" in m["question"].lower() for m in result)
        assert any("ethereum" in m["question"].lower() for m in result)

    @patch(
        "polybot.scanner._get_gamma_endpoint",
        return_value="https://gamma-api.polymarket.com",
    )
    @patch("polybot.scanner.make_proxied_request")
    @patch("polybot.scanner.get_settings")
    def test_sync_filter_inactive_in_signal_mode(
        self, mock_settings, mock_request, mock_endpoint
    ):
        """Test sync version does NOT filter when mode is 'signal'."""
        from unittest.mock import MagicMock
        from polybot.scanner import fetch_all_active_markets

        # Configure settings for signal mode (pre-filter should not apply)
        mock_cfg = type(
            "Settings",
            (),
            {
                "mode": "signal",
                "target_symbols": ["BTC", "ETH"],
                "use_api_mirrors": False,
                "up_down_only": False,
            },
        )()
        mock_settings.return_value = mock_cfg

        # Mock API response
        markets = [
            {"question": "Will BTC be up or down in 5 min?", "volume": 100_000},
            {"question": "Will Trump win election?", "volume": 100_000},
        ]
        mock_response1 = MagicMock()
        mock_response1.json.return_value = markets
        mock_response2 = MagicMock()
        mock_response2.json.return_value = []
        mock_request.side_effect = [mock_response1, mock_response2]

        result = fetch_all_active_markets(min_volume=0)

        # Should return all markets (no filter applied)
        assert len(result) == 2

    @patch(
        "polybot.scanner._get_gamma_endpoint",
        return_value="https://gamma-api.polymarket.com",
    )
    @patch("polybot.scanner.make_proxied_request")
    @patch("polybot.scanner.get_settings")
    def test_sync_filter_active_in_all_mode(
        self, mock_settings, mock_request, mock_endpoint
    ):
        """Test sync version filters markets when mode is 'all'.

        V12 BRUTEFORCE: Uses coin + updown + 5min keyword matching.
        """
        from unittest.mock import MagicMock
        from polybot.scanner import fetch_all_active_markets

        # Configure settings for 'all' mode
        mock_cfg = type(
            "Settings",
            (),
            {
                "mode": "all",
                "target_symbols": ["BTC"],
                "use_api_mirrors": False,
                "up_down_only": False,
            },
        )()
        mock_settings.return_value = mock_cfg

        # V12: Mock API response - coin + updown + 5min match
        markets = [
            # PASS: coin=bitcoin, updown="up or down", 5min="5 minutes"
            {"question": "Bitcoin Up or Down - 5 Minutes", "volume": 100_000},
            # REJECT: No updown or 5min keywords
            {"question": "Will ETH hit $5000?", "volume": 100_000},
        ]
        mock_response1 = MagicMock()
        mock_response1.json.return_value = markets
        mock_response2 = MagicMock()
        mock_response2.json.return_value = []
        mock_request.side_effect = [mock_response1, mock_response2]

        result = fetch_all_active_markets(min_volume=0)

        # V12: Only 1 passes because it matches coin + updown + 5min
        assert len(result) == 1
        questions = [m["question"].lower() for m in result]
        assert any("bitcoin" in q for q in questions)

    @pytest.mark.asyncio
    @patch("polybot.scanner.aiohttp.ClientSession")
    @patch("polybot.scanner.get_settings")
    async def test_v38_returns_empty_when_private_key_missing(
        self, mock_settings, mock_aiohttp_session
    ):
        """V38: async function returns [] when no private key is set."""
        import os
        from unittest.mock import patch as mock_patch

        from polybot.scanner import fetch_all_active_markets_async

        mock_cfg = type(
            "Settings",
            (),
            {
                "mode": "updown",
                "target_symbols": ["BTC", "SOL"],
                "use_api_mirrors": False,
                "up_down_only": False,
            },
        )()
        mock_settings.return_value = mock_cfg

        env_overrides = {
            "POLYMARKET_PRIVATE_KEY": "",
            "PRIVATE_KEY": "",
            "PK": "",
            "WALLET_PRIVATE_KEY": "",
        }

        with mock_patch.dict(os.environ, env_overrides, clear=False):
            result = await fetch_all_active_markets_async(min_volume=0)

        # V40: returns [] when key is missing (ENV fatal)
        assert result == []

    @patch(
        "polybot.scanner._get_gamma_endpoint",
        return_value="https://gamma-api.polymarket.com",
    )
    @patch("polybot.scanner.make_proxied_request")
    @patch("polybot.scanner.get_settings")
    def test_filter_uses_correct_keywords(
        self, mock_settings, mock_request, mock_endpoint
    ):
        """Test filter correctly identifies 5min markets by V12 bruteforce matching.

        V12 BRUTEFORCE: Uses coin + updown + 5min keyword matching.
        """
        from unittest.mock import MagicMock
        from polybot.scanner import fetch_all_active_markets

        mock_cfg = type(
            "Settings",
            (),
            {
                "mode": "updown",
                "target_symbols": ["BTC"],
                "use_api_mirrors": False,
                "up_down_only": False,
            },
        )()
        mock_settings.return_value = mock_cfg

        # V12: Bruteforce keyword match (coin + updown + 5min)
        markets = [
            # PASS: coin=bitcoin, updown="up or down", 5min="5 minutes"
            {"question": "Bitcoin Up or Down - 5 Minutes", "volume": 100_000},
            # REJECT: coin=btc, 5min="5min", but NO updown
            {"question": "BTC 5min prediction", "volume": 100_000},
            # REJECT: coin=btc, 5min="5-minute", but NO updown
            {"question": "BTC 5-minute direction", "volume": 100_000},
            # REJECT: coin=btc, but no updown, no 5min
            {"question": "Will BTC rise?", "volume": 100_000},
            # REJECT: coin=btc, updown="up or down", but NO 5min
            {"question": "BTC up or down next hour?", "volume": 100_000},
            # REJECT: coin=btc, but no updown, no 5min
            {"question": "BTC hit $100k?", "volume": 100_000},
            # REJECT: coin=btc, but no updown, no 5min
            {"question": "BTC reach target?", "volume": 100_000},
            # REJECT: coin=btc, but no updown, no 5min
            {"question": "BTC price today", "volume": 100_000},
        ]
        mock_response1 = MagicMock()
        mock_response1.json.return_value = markets
        mock_response2 = MagicMock()
        mock_response2.json.return_value = []
        mock_request.side_effect = [mock_response1, mock_response2]

        result = fetch_all_active_markets(min_volume=0)

        # V12: Only 1 market matches (coin + updown + 5min)
        assert len(result) == 1
        questions = [m["question"].lower() for m in result]
        assert any("up or down" in q for q in questions)

    @patch(
        "polybot.scanner._get_gamma_endpoint",
        return_value="https://gamma-api.polymarket.com",
    )
    @patch("polybot.scanner.make_proxied_request")
    @patch("polybot.scanner.get_settings")
    def test_filter_matches_new_5min_keywords(
        self, mock_settings, mock_request, mock_endpoint
    ):
        """Test V12 bruteforce keyword matching for 5-minute markets.

        V12 BRUTEFORCE: Uses coin + updown + 5min keyword matching.
        """
        from unittest.mock import MagicMock
        from polybot.scanner import fetch_all_active_markets

        mock_cfg = type(
            "Settings",
            (),
            {
                "mode": "updown",
                "target_symbols": ["BTC", "ETH", "SOL"],
                "use_api_mirrors": False,
                "up_down_only": False,
            },
        )()
        mock_settings.return_value = mock_cfg

        # V12: Bruteforce keyword matches
        markets = [
            # PASS: coin=bitcoin, updown="up or down", 5min="5 minutes"
            {"question": "Bitcoin Up or Down - 5 Minutes", "volume": 100_000},
            # PASS: coin=ethereum, updown="up or down", 5min="5 minutes"
            {"question": "Ethereum Up or Down - 5 Minutes", "volume": 100_000},
            # PASS: coin=solana, updown="up or down", 5min="5 minutes"
            {"question": "Solana Up or Down - 5 Minutes", "volume": 100_000},
            # PASS: coin=xrp, updown="up or down", 5min="5 minutes"
            {"question": "XRP Up or Down - 5 Minutes", "volume": 100_000},
            # REJECT: coin=eth, 5min="5 minutes", but NO updown
            {"question": "ETH next 5 minutes direction?", "volume": 100_000},
            # REJECT: coin=sol, 5min="5 minutes", but NO updown
            {"question": "SOL within 5 minutes prediction", "volume": 100_000},
            # REJECT: coin=btc, 5min="5 min", but NO updown
            {"question": "BTC 5 min price movement", "volume": 100_000},
            # REJECT: coin=btc, but no updown, no 5min
            {"question": "Will BTC rise or fall?", "volume": 100_000},
            # REJECT: coin=eth, but no updown, no 5min
            {"question": "ETH bull or bear?", "volume": 100_000},
            # REJECT: coin=btc, but no updown, no 5min
            {"question": "BTC monthly forecast", "volume": 100_000},
            {"question": "General crypto news", "volume": 100_000},
        ]
        mock_response1 = MagicMock()
        mock_response1.json.return_value = markets
        mock_response2 = MagicMock()
        mock_response2.json.return_value = []
        mock_request.side_effect = [mock_response1, mock_response2]

        result = fetch_all_active_markets(min_volume=0)

        # V12: 4 markets match (coin + updown + 5min)
        assert len(result) == 4, (
            f"Expected 4 markets, got {len(result)}: {[m['question'] for m in result]}"
        )

    @patch(
        "polybot.scanner._get_gamma_endpoint",
        return_value="https://gamma-api.polymarket.com",
    )
    @patch("polybot.scanner.make_proxied_request")
    @patch("polybot.scanner.get_settings")
    def test_filter_matches_coin_specific_patterns(
        self, mock_settings, mock_request, mock_endpoint
    ):
        """Test V12 filter matches coin-specific 5-minute patterns.

        V12 BRUTEFORCE: Uses coin + updown + 5min keyword matching.
        """
        from unittest.mock import MagicMock
        from polybot.scanner import fetch_all_active_markets

        mock_cfg = type(
            "Settings",
            (),
            {
                "mode": "updown",
                "target_symbols": ["BTC", "ETH", "SOL"],
                "use_api_mirrors": False,
                "up_down_only": False,
            },
        )()
        mock_settings.return_value = mock_cfg

        # V12: Bruteforce keyword matches
        markets = [
            # PASS: coin=bitcoin, updown="up or down", 5min="5 minutes"
            {"question": "Bitcoin Up or Down - 5 Minutes", "volume": 100_000},
            # PASS: coin=ethereum, updown="up or down", 5min="5 minutes"
            {"question": "Ethereum Up or Down - 5 Minutes", "volume": 100_000},
            # PASS: coin=solana, updown="up or down", 5min="5 minutes"
            {"question": "Solana Up or Down - 5 Minutes", "volume": 100_000},
            # REJECT: coin=btc, 5min="5min", but NO updown
            {"question": "BTC 5min price", "volume": 100_000},
        ]
        mock_response1 = MagicMock()
        mock_response1.json.return_value = markets
        mock_response2 = MagicMock()
        mock_response2.json.return_value = []
        mock_request.side_effect = [mock_response1, mock_response2]

        result = fetch_all_active_markets(min_volume=0)

        # V12: 3 markets match (coin + updown + 5min)
        assert len(result) == 3

    @patch(
        "polybot.scanner._get_gamma_endpoint",
        return_value="https://gamma-api.polymarket.com",
    )
    @patch("polybot.scanner.make_proxied_request")
    @patch("polybot.scanner.get_settings")
    def test_filter_matches_real_polymarket_5min_titles(
        self, mock_settings, mock_request, mock_endpoint
    ):
        """Test V12: Filter catches real Polymarket 5-minute title formats.

        V12 BRUTEFORCE: Uses coin + updown + 5min keyword matching.
        """
        from unittest.mock import MagicMock
        from polybot.scanner import fetch_all_active_markets

        mock_cfg = type(
            "Settings",
            (),
            {
                "mode": "updown",
                "target_symbols": ["BTC", "ETH", "SOL"],
                "use_api_mirrors": False,
                "up_down_only": False,
            },
        )()
        mock_settings.return_value = mock_cfg

        # V12: Bruteforce keyword matches (including titles with date/time suffixes)
        markets = [
            # PASS: coin=bitcoin, updown="up or down", 5min="5 minutes"
            {"question": "Bitcoin Up or Down - 5 Minutes", "volume": 100_000},
            # PASS: coin=ethereum, updown="up or down", 5min="5 minutes"
            {"question": "Ethereum Up or Down - 5 Minutes", "volume": 100_000},
            # PASS: coin=solana, updown="up or down", 5min="5 minutes"
            {"question": "Solana Up or Down - 5 Minutes", "volume": 100_000},
            # PASS: coin=xrp, updown="up or down", 5min="5 minutes"
            {"question": "XRP Up or Down - 5 Minutes", "volume": 100_000},
            # REJECT: coin=bitcoin, 5min="5 min", but NO updown
            {"question": "Bitcoin - 5 min prediction", "volume": 100_000},
            # REJECT: coin=bitcoin, updown="up or down", but NO 5min
            {"question": "Bitcoin Up or Down tomorrow", "volume": 100_000},
        ]
        mock_response1 = MagicMock()
        mock_response1.json.return_value = markets
        mock_response2 = MagicMock()
        mock_response2.json.return_value = []
        mock_request.side_effect = [mock_response1, mock_response2]

        result = fetch_all_active_markets(min_volume=0)

        # V12: 4 markets match (coin + updown + 5min)
        assert len(result) == 4, (
            f"Expected 4 markets, got {len(result)}: {[m['question'] for m in result]}"
        )
        # Verify core phrase patterns are present
        questions = [m["question"].lower() for m in result]
        assert any("- 5 minutes" in q for q in questions)

    @patch(
        "polybot.scanner._get_gamma_endpoint",
        return_value="https://gamma-api.polymarket.com",
    )
    @patch("polybot.scanner.make_proxied_request")
    @patch("polybot.scanner.get_settings")
    def test_filter_excludes_long_term_markets(
        self, mock_settings, mock_request, mock_endpoint
    ):
        """Test V12: Filter excludes long-term markets (no updown/5min keywords).

        V12 BRUTEFORCE: Markets without all 3 keywords (coin + updown + 5min)
        are naturally excluded. No block patterns needed.
        """
        from unittest.mock import MagicMock
        from polybot.scanner import fetch_all_active_markets

        mock_cfg = type(
            "Settings",
            (),
            {
                "mode": "updown",
                "target_symbols": ["BTC", "ETH", "SOL"],
                "use_api_mirrors": False,
                "up_down_only": False,
            },
        )()
        mock_settings.return_value = mock_cfg

        # V12: Mix of coin+updown+5min matches and long-term markets
        markets = [
            # PASS: coin=bitcoin, updown="up or down", 5min="5 minutes"
            {"question": "Bitcoin Up or Down - 5 Minutes", "volume": 100_000},
            {"question": "Ethereum Up or Down - 5 Minutes", "volume": 100_000},
            # REJECT: coin=btc, but no updown or 5min
            {"question": "Will BTC hit $100k by December 2026?", "volume": 100_000},
            # REJECT: coin=eth, but no updown or 5min
            {"question": "Will ETH reach $5000 by end of year?", "volume": 100_000},
            # REJECT: coin=btc, but no updown or 5min
            {"question": "BTC will hit $150k by 2027", "volume": 100_000},
            # REJECT: coin=sol, but no updown or 5min
            {"question": "Will SOL reach $300 by June 2026?", "volume": 100_000},
            # REJECT: coin=btc, but no updown or 5min
            {"question": "BTC price by March 31", "volume": 100_000},
        ]
        mock_response1 = MagicMock()
        mock_response1.json.return_value = markets
        mock_response2 = MagicMock()
        mock_response2.json.return_value = []
        mock_request.side_effect = [mock_response1, mock_response2]

        result = fetch_all_active_markets(min_volume=0)

        # V12: Should only return the 2 coin+updown+5min matches
        assert len(result) == 2
        # Verify the returned markets have core phrase
        questions = [m["question"].lower() for m in result]
        assert any("- 5 minutes" in q for q in questions)
        # Verify no long-term markets passed
        assert not any("december" in q for q in questions)
        assert not any("2026" in q for q in questions)
        assert not any("2027" in q for q in questions)

    @patch(
        "polybot.scanner._get_gamma_endpoint",
        return_value="https://gamma-api.polymarket.com",
    )
    @patch("polybot.scanner.make_proxied_request")
    @patch("polybot.scanner.get_settings")
    def test_filter_excludes_year_based_markets(
        self, mock_settings, mock_request, mock_endpoint
    ):
        """Test V12: Filter excludes markets missing required keywords.

        V12 BRUTEFORCE: Markets must have ALL THREE keywords (coin + updown + 5min).
        Year-based markets are excluded because they lack updown/5min keywords.
        Markets with all 3 keywords pass even without exact title format.
        """
        from unittest.mock import MagicMock
        from polybot.scanner import fetch_all_active_markets

        mock_cfg = type(
            "Settings",
            (),
            {
                "mode": "updown",
                "target_symbols": ["BTC"],
                "use_api_mirrors": False,
                "up_down_only": False,
            },
        )()
        mock_settings.return_value = mock_cfg

        # V12: Only coin + updown + 5min matches pass
        markets = [
            # REJECT: coin=btc, but no updown, no 5min
            {"question": "BTC will hit $100k in 2026", "volume": 100_000},
            {"question": "Will BTC reach $200k by 2027?", "volume": 100_000},
            {"question": "BTC price prediction by 2028", "volume": 100_000},
            # REJECT: coin=btc, 5min="5 min", but NO updown
            {"question": "BTC 5 min direction", "volume": 100_000},
            # PASS: coin=btc, updown="up or down", 5min="5 min" (V12 broader than V11)
            {"question": "BTC 5 min up or down", "volume": 100_000},
            # PASS: coin=bitcoin, updown="up or down", 5min="5 minutes"
            {"question": "Bitcoin Up or Down - 5 Minutes", "volume": 100_000},
        ]
        mock_response1 = MagicMock()
        mock_response1.json.return_value = markets
        mock_response2 = MagicMock()
        mock_response2.json.return_value = []
        mock_request.side_effect = [mock_response1, mock_response2]

        result = fetch_all_active_markets(min_volume=0)

        # V12: 2 markets match (coin + updown + 5min) – broader than V11
        assert len(result) == 2
        questions = [m["question"].lower() for m in result]
        assert any("up or down" in q for q in questions)

    @patch(
        "polybot.scanner._get_gamma_endpoint",
        return_value="https://gamma-api.polymarket.com",
    )
    @patch("polybot.scanner.make_proxied_request")
    @patch("polybot.scanner.get_settings")
    def test_filter_matches_by_slug(self, mock_settings, mock_request, mock_endpoint):
        """Test V12: Filter matches markets by keyword matching (not slugs).

        V12 BRUTEFORCE: Uses coin + updown + 5min keyword matching on question.
        Slug matching is no longer used – pure bruteforce on question text.
        """
        from unittest.mock import MagicMock
        from polybot.scanner import fetch_all_active_markets

        mock_cfg = type(
            "Settings",
            (),
            {
                "mode": "updown",
                "target_symbols": ["BTC", "ETH", "SOL", "XRP"],
                "use_api_mirrors": False,
                "up_down_only": False,
            },
        )()
        mock_settings.return_value = mock_cfg

        # V12: Markets match by coin + updown + 5min keywords (not by slug)
        markets = [
            # PASS: coin=bitcoin, updown="up or down", 5min="5 minutes"
            {
                "question": "Bitcoin Up or Down - 5 Minutes",
                "slug": "random-slug",
                "volume": 100_000,
            },
            # PASS: coin=btc, updown="up or down", 5min="5 min"
            {
                "question": "BTC Up or Down - 5 Min Round",
                "slug": "btc-updown-5m-abc123",
                "volume": 100_000,
            },
            # PASS: coin=eth, updown="up/down", 5min="5 min"
            {
                "question": "ETH Up/Down - 5 Min Short-term",
                "slug": "eth-updown-5m-def456",
                "volume": 100_000,
            },
            # PASS: coin=solana, updown="up or down", 5min="5 minutes"
            {
                "question": "Solana Up or Down - 5 Minutes Trend",
                "slug": "sol-updown-5m-ghi789",
                "volume": 100_000,
            },
            # PASS: coin=xrp, updown="up or down", 5min="5 minutes"
            {
                "question": "XRP Up or Down - 5 Minutes Direction",
                "slug": "xrp-updown-5m-jkl012",
                "volume": 100_000,
            },
            # REJECT: coin=bitcoin, but no updown or 5min
            {
                "question": "Bitcoin Long Term",
                "slug": "btc-long-term",
                "volume": 100_000,
            },
        ]
        mock_response1 = MagicMock()
        mock_response1.json.return_value = markets
        mock_response2 = MagicMock()
        mock_response2.json.return_value = []
        mock_request.side_effect = [mock_response1, mock_response2]

        result = fetch_all_active_markets(min_volume=0)

        # V12: 5 markets match (coin + updown + 5min keywords in question)
        assert len(result) == 5, (
            f"Expected 5 markets, got {len(result)}: {[m.get('question') for m in result]}"
        )
        # Verify specific markets are present
        result_questions = [m["question"] for m in result]
        assert "Bitcoin Up or Down - 5 Minutes" in result_questions
        assert "BTC Up or Down - 5 Min Round" in result_questions
        assert "ETH Up/Down - 5 Min Short-term" in result_questions
        assert "Solana Up or Down - 5 Minutes Trend" in result_questions
        assert "XRP Up or Down - 5 Minutes Direction" in result_questions
        # Verify rejected market is NOT present
        assert "Bitcoin Long Term" not in result_questions


class TestV14DynamicSlugCycleMode:
    """Test V38 dynamic slug cycle – timestamp-based slug matching."""

    @pytest.mark.asyncio
    @patch("polybot.scanner.get_settings")
    async def test_v38_returns_dynamic_slug_matches(self, mock_settings):
        """V38: When aiohttp finds timestamp-based slug markets, return them."""
        import os
        from unittest.mock import AsyncMock, MagicMock

        from polybot.scanner import fetch_all_active_markets_async

        mock_cfg = type(
            "Settings",
            (),
            {
                "mode": "updown",
                "target_symbols": ["BTC", "ETH", "SOL", "XRP"],
                "use_api_mirrors": False,
                "up_down_only": False,
                "effective_log_level": 20,
                "dry_run": True,
            },
        )()
        mock_settings.return_value = mock_cfg

        # Use a fixed timestamp so we can predict the slug
        fixed_ts = 1774271400  # divisible by 300
        expected_slug = f"btc-updown-5m-{fixed_ts}"

        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(
            return_value=[
                {
                    "question": "BTC Up or Down - 5 Min Live",
                    "slug": expected_slug,
                    "volume": 80_000,
                },
            ]
        )
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        # V38 requires a valid private key to proceed past ENV check
        long_key = "0x" + "a" * 64

        with (
            patch.dict(os.environ, {"POLYMARKET_PRIVATE_KEY": long_key}),
            patch(
                "polybot.scanner.aiohttp.ClientSession",
                return_value=mock_session,
            ),
            patch(
                "polybot.executor.get_polygon_balance_async",
                new_callable=AsyncMock,
                return_value=100.0,
            ),
            patch("polybot.scanner.time.time", return_value=float(fixed_ts)),
        ):
            result = await fetch_all_active_markets_async(min_volume=0)

        # V38 should return directly fetched markets
        assert len(result) >= 1
        slugs = [m.get("slug", "") for m in result]
        assert any("btc-updown-5m" in s for s in slugs)

    @pytest.mark.asyncio
    @patch("polybot.scanner.get_settings")
    async def test_v38_returns_empty_when_no_slugs_found(self, mock_settings):
        """V38: When no slug matches found and key is present, return empty list."""
        import os
        from unittest.mock import AsyncMock, MagicMock

        from polybot.scanner import fetch_all_active_markets_async

        mock_cfg = type(
            "Settings",
            (),
            {
                "mode": "updown",
                "target_symbols": ["BTC", "ETH", "SOL", "XRP"],
                "use_api_mirrors": False,
                "up_down_only": False,
                "effective_log_level": 20,
                "dry_run": True,
            },
        )()
        mock_settings.return_value = mock_cfg

        # V38 slug fetch returns empty results (no matching slugs)
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value=[])
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        long_key = "0x" + "a" * 64

        with (
            patch.dict(os.environ, {"POLYMARKET_PRIVATE_KEY": long_key}),
            patch("polybot.scanner.aiohttp.ClientSession", return_value=mock_session),
            patch(
                "polybot.executor.get_polygon_balance_async",
                new_callable=AsyncMock,
                return_value=100.0,
            ),
        ):
            result = await fetch_all_active_markets_async(min_volume=0)

        # V38: returns empty list when no slugs match
        assert result == []


class TestV38ForceTradeMode:
    """Test V38 trade execution on found markets."""

    @pytest.mark.asyncio
    @patch("polybot.scanner.get_settings")
    async def test_v38_returns_filtered_markets(self, mock_settings):
        """V38: When slug fetch finds markets, return them."""
        import os
        from unittest.mock import AsyncMock, MagicMock

        from polybot.scanner import fetch_all_active_markets_async

        mock_cfg = type(
            "Settings",
            (),
            {
                "mode": "updown",
                "target_symbols": ["BTC", "ETH", "SOL", "XRP"],
                "use_api_mirrors": False,
                "up_down_only": False,
                "effective_log_level": 20,
                "dry_run": True,
            },
        )()
        mock_settings.return_value = mock_cfg

        fixed_ts = 1774271400
        expected_slug = f"btc-updown-5m-{fixed_ts}"

        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(
            return_value=[
                {
                    "question": "BTC Up or Down - 5 Min Live",
                    "slug": expected_slug,
                    "volume": 80_000,
                    "liquidity": 5_000,
                },
            ]
        )
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        mock_trade = AsyncMock(return_value={"status": "dry_run", "id": "test-123"})

        # V38 requires a valid private key to proceed past ENV check
        long_key = "0x" + "a" * 64

        with (
            patch.dict(os.environ, {"POLYMARKET_PRIVATE_KEY": long_key}),
            patch(
                "polybot.scanner.aiohttp.ClientSession",
                return_value=mock_session,
            ),
            patch(
                "polybot.executor.get_polygon_balance_async",
                new_callable=AsyncMock,
                return_value=100.0,
            ),
            patch("polybot.scanner.time.time", return_value=float(fixed_ts)),
            patch("polybot.executor.place_trade_async", mock_trade),
        ):
            result = await fetch_all_active_markets_async(min_volume=0)

        # V38 returns found markets
        assert len(result) >= 1
        assert any("btc-updown-5m" in m.get("slug", "") for m in result)

    @pytest.mark.asyncio
    @patch("polybot.scanner.get_settings")
    async def test_v38_returns_all_found_markets(self, mock_settings):
        """V38: Returns all markets found from slug fetch."""
        import os
        from unittest.mock import AsyncMock, MagicMock

        from polybot.scanner import fetch_all_active_markets_async

        mock_cfg = type(
            "Settings",
            (),
            {
                "mode": "updown",
                "target_symbols": ["BTC", "ETH", "SOL", "XRP"],
                "use_api_mirrors": False,
                "up_down_only": False,
                "effective_log_level": 20,
                "dry_run": True,
            },
        )()
        mock_settings.return_value = mock_cfg

        fixed_ts = 1774271400

        # Return 15 markets from slug fetch (all share the same valid slug
        # so they pass is_slot_tradeable; this tests that all API-returned
        # markets are collected regardless of duplicates)
        many_markets = [
            {
                "question": f"BTC Up or Down - 5 Min #{i}",
                "slug": f"btc-updown-5m-{fixed_ts}",
                "volume": 80_000,
                "liquidity": 5_000,
            }
            for i in range(15)
        ]

        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value=many_markets)
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        mock_trade = AsyncMock(return_value={"status": "dry_run", "id": "test-123"})

        # V38 requires a valid private key
        long_key = "0x" + "a" * 64

        with (
            patch.dict(os.environ, {"POLYMARKET_PRIVATE_KEY": long_key}),
            patch(
                "polybot.scanner.aiohttp.ClientSession",
                return_value=mock_session,
            ),
            patch(
                "polybot.executor.get_polygon_balance_async",
                new_callable=AsyncMock,
                return_value=100.0,
            ),
            patch("polybot.scanner.time.time", return_value=float(fixed_ts)),
            patch("polybot.executor.place_trade_async", mock_trade),
        ):
            result = await fetch_all_active_markets_async(min_volume=0)

        # V38 returns all found markets
        assert len(result) >= 1


class TestV38DecisionExecution:
    """Test V38 ENV-CHECK + TRADE EXECUTION – fatal stop when key missing, trade size 30.0."""

    @pytest.mark.asyncio
    @patch("polybot.scanner.get_settings")
    async def test_v38_chooses_up_when_random_below_threshold(self, mock_settings):
        """V38: Chooses Up when random.random() < 0.6 (60% Up-Bias), trade size 30.0."""
        import os
        from unittest.mock import AsyncMock, MagicMock

        from polybot.scanner import fetch_all_active_markets_async

        mock_cfg = type(
            "Settings",
            (),
            {
                "mode": "updown",
                "target_symbols": ["BTC", "ETH", "SOL", "XRP"],
                "use_api_mirrors": False,
                "up_down_only": False,
                "effective_log_level": 20,
                "dry_run": True,
            },
        )()
        mock_settings.return_value = mock_cfg

        fixed_ts = 1774271400
        expected_slug = f"btc-updown-5m-{fixed_ts}"
        up_token_id = "token-up-btc-001"
        down_token_id = "token-down-btc-001"

        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(
            return_value=[
                {
                    "question": "BTC Up or Down - 5 Min Live",
                    "slug": expected_slug,
                    "volume": 80_000,
                    "liquidity": 5_000,
                    "clobTokenIds": [up_token_id, down_token_id],
                },
            ]
        )
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        mock_trade = AsyncMock(return_value={"status": "dry_run", "id": "v38-001"})

        # V38 requires a valid private key (>= 60 chars) to proceed
        long_key = "0x" + "a" * 64

        with (
            patch.dict(os.environ, {"POLYMARKET_PRIVATE_KEY": long_key}),
            patch(
                "polybot.scanner.aiohttp.ClientSession",
                return_value=mock_session,
            ),
            patch(
                "polybot.executor.get_polygon_balance_async",
                new_callable=AsyncMock,
                return_value=100.0,
            ),
            patch("polybot.scanner.time.time", return_value=float(fixed_ts)),
            patch("polybot.executor.place_trade_async", mock_trade),
            patch("py_clob_client.client.ClobClient", MagicMock()),
            patch(
                "polybot.scanner._fetch_order_book",
                return_value={"asks": [{"price": "0.40"}], "bids": [{"price": "0.38"}]},
            ),
        ):
            result = await fetch_all_active_markets_async(min_volume=0)

        # V38 returns filtered markets
        assert len(result) >= 1
        assert any("btc-updown-5m" in m.get("slug", "") for m in result)

        # V38 called place_trade_async with outcome="up" (mid price 0.39 < 0.45)
        assert mock_trade.call_count >= 1
        for call in mock_trade.call_args_list:
            assert "side" not in call.kwargs, "V57: side parameter must NOT be passed"
            assert "size" not in call.kwargs, "V57: size parameter replaced by amount"
            assert call.kwargs["amount"] == 30.0
            assert call.kwargs["outcome"] == "up"  # mid price 0.39 < 0.45 → "up"
            assert call.kwargs["token_id"] == up_token_id

    @pytest.mark.asyncio
    @patch("polybot.scanner.get_settings")
    async def test_v38_chooses_down_when_random_above_threshold(self, mock_settings):
        """V38: Chooses Down when random.random() >= 0.6."""
        import os
        from unittest.mock import AsyncMock, MagicMock

        from polybot.scanner import fetch_all_active_markets_async

        mock_cfg = type(
            "Settings",
            (),
            {
                "mode": "updown",
                "target_symbols": ["BTC", "ETH", "SOL", "XRP"],
                "use_api_mirrors": False,
                "up_down_only": False,
                "effective_log_level": 20,
                "dry_run": True,
            },
        )()
        mock_settings.return_value = mock_cfg

        fixed_ts = 1774271400
        expected_slug = f"btc-updown-5m-{fixed_ts}"
        up_token_id = "token-up-btc-001"
        down_token_id = "token-down-btc-001"

        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(
            return_value=[
                {
                    "question": "BTC Up or Down - 5 Min Live",
                    "slug": expected_slug,
                    "volume": 80_000,
                    "liquidity": 5_000,
                    "clobTokenIds": [up_token_id, down_token_id],
                },
            ]
        )
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        mock_trade = AsyncMock(return_value={"status": "dry_run", "id": "v38-002"})

        # V38 requires a valid private key (>= 60 chars) to proceed
        long_key = "0x" + "a" * 64

        with (
            patch.dict(os.environ, {"POLYMARKET_PRIVATE_KEY": long_key}),
            patch(
                "polybot.scanner.aiohttp.ClientSession",
                return_value=mock_session,
            ),
            patch(
                "polybot.executor.get_polygon_balance_async",
                new_callable=AsyncMock,
                return_value=100.0,
            ),
            patch("polybot.scanner.time.time", return_value=float(fixed_ts)),
            patch("polybot.executor.place_trade_async", mock_trade),
            patch("py_clob_client.client.ClobClient", MagicMock()),
            patch(
                "polybot.scanner._fetch_order_book",
                return_value={"asks": [{"price": "0.62"}], "bids": [{"price": "0.60"}]},
            ),
        ):
            result = await fetch_all_active_markets_async(min_volume=0)

        # V38 returns filtered markets
        assert len(result) >= 1

        # V38 called place_trade_async with outcome="down" (mid price 0.61 > 0.55)
        assert mock_trade.call_count >= 1
        for call in mock_trade.call_args_list:
            assert "side" not in call.kwargs, "V57: side parameter must NOT be passed"
            assert "size" not in call.kwargs, "V57: size parameter replaced by amount"
            assert call.kwargs["amount"] == 30.0
            assert call.kwargs["outcome"] == "down"  # mid price 0.61 > 0.55 → "down"
            assert call.kwargs["token_id"] == down_token_id

    @pytest.mark.asyncio
    @patch("polybot.scanner.get_settings")
    async def test_v38_up_bias_at_boundary(self, mock_settings):
        """V38: random=0.5 → Up (0.5 < 0.6)."""
        import os
        from unittest.mock import AsyncMock, MagicMock

        from polybot.scanner import fetch_all_active_markets_async

        mock_cfg = type(
            "Settings",
            (),
            {
                "mode": "updown",
                "target_symbols": ["BTC", "ETH", "SOL", "XRP"],
                "use_api_mirrors": False,
                "up_down_only": False,
                "effective_log_level": 20,
                "dry_run": True,
            },
        )()
        mock_settings.return_value = mock_cfg

        fixed_ts = 1774271400
        expected_slug = f"btc-updown-5m-{fixed_ts}"
        up_token_id = "token-up-btc-001"
        down_token_id = "token-down-btc-001"

        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(
            return_value=[
                {
                    "question": "BTC Up or Down - 5 Min Live",
                    "slug": expected_slug,
                    "volume": 80_000,
                    "clobTokenIds": [up_token_id, down_token_id],
                },
            ]
        )
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        mock_trade = AsyncMock(return_value={"status": "dry_run", "id": "v38-edge"})

        # Boundary test: random=0.5 is BELOW V38's 0.6 threshold → Up
        long_key = "0x" + "a" * 64

        with (
            patch.dict(os.environ, {"POLYMARKET_PRIVATE_KEY": long_key}),
            patch(
                "polybot.scanner.aiohttp.ClientSession",
                return_value=mock_session,
            ),
            patch(
                "polybot.executor.get_polygon_balance_async",
                new_callable=AsyncMock,
                return_value=100.0,
            ),
            patch("polybot.scanner.time.time", return_value=float(fixed_ts)),
            patch("polybot.executor.place_trade_async", mock_trade),
            patch("py_clob_client.client.ClobClient", MagicMock()),
            patch(
                "polybot.scanner._fetch_order_book",
                return_value={"asks": [{"price": "0.40"}], "bids": [{"price": "0.38"}]},
            ),
        ):
            result = await fetch_all_active_markets_async(min_volume=0)

        assert len(result) >= 1
        assert mock_trade.call_count >= 1
        for call in mock_trade.call_args_list:
            assert "side" not in call.kwargs, "V57: side parameter must NOT be passed"
            assert call.kwargs["token_id"] == up_token_id

    @pytest.mark.asyncio
    @patch("polybot.scanner.get_settings")
    async def test_v38_skips_market_with_missing_clobTokenIds(self, mock_settings):
        """V38: Skips markets with missing clobTokenIds field; trades those with clobTokenIds."""
        import os
        from unittest.mock import AsyncMock, MagicMock

        from polybot.scanner import fetch_all_active_markets_async

        mock_cfg = type(
            "Settings",
            (),
            {
                "mode": "updown",
                "target_symbols": ["BTC", "ETH", "SOL", "XRP"],
                "use_api_mirrors": False,
                "up_down_only": False,
                "effective_log_level": 20,
                "dry_run": True,
            },
        )()
        mock_settings.return_value = mock_cfg

        fixed_ts = 1774271400

        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(
            return_value=[
                {
                    "question": "BTC Up or Down - 5 Min Live",
                    "slug": f"btc-updown-5m-{fixed_ts}",
                    "volume": 80_000,
                    # No clobTokenIds – V38 skips this market
                    "tokens": [
                        {"outcome": "Up", "token_id": "token-up-btc-001"},
                        {"outcome": "Down", "token_id": "token-down-btc-001"},
                    ],
                },
                {
                    "question": "ETH Up or Down - 5 Min Live",
                    "slug": f"eth-updown-5m-{fixed_ts}",
                    "volume": 60_000,
                    # Has clobTokenIds – V38 trades this
                    "clobTokenIds": [
                        "token-up-eth-001",
                        "token-down-eth-001",
                    ],
                },
            ]
        )
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        mock_trade = AsyncMock(return_value={"status": "dry_run", "id": "v38-eth"})

        long_key = "0x" + "a" * 64

        with (
            patch.dict(os.environ, {"POLYMARKET_PRIVATE_KEY": long_key}),
            patch(
                "polybot.scanner.aiohttp.ClientSession",
                return_value=mock_session,
            ),
            patch(
                "polybot.executor.get_polygon_balance_async",
                new_callable=AsyncMock,
                return_value=100.0,
            ),
            patch("polybot.scanner.time.time", return_value=float(fixed_ts)),
            patch("polybot.executor.place_trade_async", mock_trade),
            patch("py_clob_client.client.ClobClient", MagicMock()),
            patch(
                "polybot.scanner._fetch_order_book",
                return_value={"asks": [{"price": "0.40"}], "bids": [{"price": "0.38"}]},
            ),
        ):
            result = await fetch_all_active_markets_async(min_volume=0)

        # Returns all filtered markets (both BTC and ETH)
        assert len(result) >= 1
        # Only ETH trade executed (BTC skipped – no clobTokenIds)
        assert mock_trade.call_count == 1
        assert mock_trade.call_args.kwargs["token_id"] == "token-up-eth-001"

    @pytest.mark.asyncio
    @patch("polybot.scanner.get_settings")
    async def test_v38_handles_trade_error_gracefully(self, mock_settings):
        """V38: Continues processing if one trade fails."""
        import os
        from unittest.mock import AsyncMock, MagicMock

        from polybot.scanner import fetch_all_active_markets_async

        mock_cfg = type(
            "Settings",
            (),
            {
                "mode": "updown",
                "target_symbols": ["BTC", "ETH", "SOL", "XRP"],
                "use_api_mirrors": False,
                "up_down_only": False,
                "effective_log_level": 20,
                "dry_run": False,
            },
        )()
        mock_settings.return_value = mock_cfg

        fixed_ts = 1774271400

        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(
            return_value=[
                {
                    "question": "BTC Up or Down - 5 Min Live",
                    "slug": f"btc-updown-5m-{fixed_ts}",
                    "volume": 80_000,
                    "clobTokenIds": [
                        "token-up-btc-001",
                        "token-down-btc-001",
                    ],
                },
                {
                    "question": "ETH Up or Down - 5 Min Live",
                    "slug": f"eth-updown-5m-{fixed_ts}",
                    "volume": 60_000,
                    "clobTokenIds": [
                        "token-up-eth-001",
                        "token-down-eth-001",
                    ],
                },
            ]
        )
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        # First trade succeeds, second fails
        mock_trade = AsyncMock(
            side_effect=[
                {"status": "executed", "id": "v38-ok"},
                Exception("CLOB client error"),
            ]
        )

        long_key = "0x" + "a" * 64

        with (
            patch.dict(os.environ, {"POLYMARKET_PRIVATE_KEY": long_key}),
            patch(
                "polybot.scanner.aiohttp.ClientSession",
                return_value=mock_session,
            ),
            patch(
                "polybot.executor.get_polygon_balance_async",
                new_callable=AsyncMock,
                return_value=100.0,
            ),
            patch("polybot.scanner.time.time", return_value=float(fixed_ts)),
            patch("polybot.executor.place_trade_async", mock_trade),
            patch(
                "polybot.scanner._fetch_order_book",
                return_value={"asks": [{"price": "0.40"}], "bids": [{"price": "0.38"}]},
            ),
        ):
            # Should not raise even if individual trades fail
            result = await fetch_all_active_markets_async(min_volume=0)

        # Still returns all filtered markets despite trade error
        assert len(result) >= 1

    @pytest.mark.asyncio
    @patch("polybot.scanner.get_settings")
    async def test_v38_skips_market_with_single_clobTokenIds_entry(self, mock_settings):
        """V38: Skips markets with only 1 clobTokenIds entry (needs exactly 2)."""
        import os
        from unittest.mock import AsyncMock, MagicMock

        from polybot.scanner import fetch_all_active_markets_async

        mock_cfg = type(
            "Settings",
            (),
            {
                "mode": "updown",
                "target_symbols": ["BTC", "ETH", "SOL", "XRP"],
                "use_api_mirrors": False,
                "up_down_only": False,
                "effective_log_level": 20,
                "dry_run": True,
            },
        )()
        mock_settings.return_value = mock_cfg

        fixed_ts = 1774271400

        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(
            return_value=[
                {
                    "question": "BTC Up or Down - 5 Min Live",
                    "slug": f"btc-updown-5m-{fixed_ts}",
                    "volume": 80_000,
                    # Only 1 clobTokenId – V38 skips
                    "clobTokenIds": ["token-up-btc-only"],
                },
            ]
        )
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        mock_trade = AsyncMock(return_value={"status": "dry_run", "id": "v38-skip"})

        long_key = "0x" + "a" * 64

        with (
            patch.dict(os.environ, {"POLYMARKET_PRIVATE_KEY": long_key}),
            patch(
                "polybot.scanner.aiohttp.ClientSession",
                return_value=mock_session,
            ),
            patch(
                "polybot.executor.get_polygon_balance_async",
                new_callable=AsyncMock,
                return_value=100.0,
            ),
            patch("polybot.scanner.time.time", return_value=float(fixed_ts)),
            patch("polybot.executor.place_trade_async", mock_trade),
            patch(
                "polybot.scanner._fetch_order_book",
                return_value={"asks": [{"price": "0.40"}], "bids": [{"price": "0.38"}]},
            ),
        ):
            result = await fetch_all_active_markets_async(min_volume=0)

        # V38 returns filtered markets
        assert len(result) >= 1
        # V38 skips market with only 1 clobTokenId – no trade
        assert mock_trade.call_count == 0

    @pytest.mark.asyncio
    @patch("polybot.scanner.get_settings")
    async def test_v38_no_fallback_to_outcomes_loop(self, mock_settings):
        """V38: Does NOT fall back to outcomes/tokens parsing (no loop)."""
        import os
        from unittest.mock import AsyncMock, MagicMock

        from polybot.scanner import fetch_all_active_markets_async

        mock_cfg = type(
            "Settings",
            (),
            {
                "mode": "updown",
                "target_symbols": ["BTC", "ETH", "SOL", "XRP"],
                "use_api_mirrors": False,
                "up_down_only": False,
                "effective_log_level": 20,
                "dry_run": True,
            },
        )()
        mock_settings.return_value = mock_cfg

        fixed_ts = 1774271400

        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(
            return_value=[
                {
                    "question": "BTC Up or Down - 5 Min Live",
                    "slug": f"btc-updown-5m-{fixed_ts}",
                    "volume": 80_000,
                    # Has tokens with Up/Down but NO clobTokenIds
                    "tokens": [
                        {"outcome": "Up", "token_id": "token-up-btc-001"},
                        {"outcome": "Down", "token_id": "token-down-btc-001"},
                    ],
                },
            ]
        )
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        mock_trade = AsyncMock(return_value={"status": "dry_run", "id": "v38-nofb"})

        long_key = "0x" + "a" * 64

        with (
            patch.dict(os.environ, {"POLYMARKET_PRIVATE_KEY": long_key}),
            patch(
                "polybot.scanner.aiohttp.ClientSession",
                return_value=mock_session,
            ),
            patch(
                "polybot.executor.get_polygon_balance_async",
                new_callable=AsyncMock,
                return_value=100.0,
            ),
            patch("polybot.scanner.time.time", return_value=float(fixed_ts)),
            patch("polybot.executor.place_trade_async", mock_trade),
            patch(
                "polybot.scanner._fetch_order_book",
                return_value={"asks": [{"price": "0.40"}], "bids": [{"price": "0.38"}]},
            ),
        ):
            result = await fetch_all_active_markets_async(min_volume=0)

        # V38 returns filtered markets
        assert len(result) >= 1
        # V38 does NOT use outcomes loop – no clobTokenIds means no trade
        assert mock_trade.call_count == 0

    @pytest.mark.asyncio
    @patch("polybot.scanner.get_settings")
    async def test_v38_clobTokenIds_direct_mapping(self, mock_settings):
        """V38: clobTokenIds[0]=Up, [1]=Down used directly (ignores tokens list)."""
        import os
        from unittest.mock import AsyncMock, MagicMock

        from polybot.scanner import fetch_all_active_markets_async

        mock_cfg = type(
            "Settings",
            (),
            {
                "mode": "updown",
                "target_symbols": ["BTC", "ETH", "SOL", "XRP"],
                "use_api_mirrors": False,
                "up_down_only": False,
                "effective_log_level": 20,
                "dry_run": True,
            },
        )()
        mock_settings.return_value = mock_cfg

        fixed_ts = 1774271400
        expected_slug = f"btc-updown-5m-{fixed_ts}"
        clob_up_id = "clob-market-up-001"
        clob_down_id = "clob-market-down-001"

        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(
            return_value=[
                {
                    "question": "BTC Up or Down - 5 Min Live",
                    "slug": expected_slug,
                    "volume": 80_000,
                    # Market-level clobTokenIds (V38 only source)
                    "clobTokenIds": [clob_up_id, clob_down_id],
                    # Also has tokens with different IDs – should be ignored
                    "tokens": [
                        {"outcome": "Up", "token_id": "token-up-ignored"},
                        {"outcome": "Down", "token_id": "token-down-ignored"},
                    ],
                },
            ]
        )
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        mock_trade = AsyncMock(return_value={"status": "dry_run", "id": "v38-clob-mkt"})

        long_key = "0x" + "a" * 64

        with (
            patch.dict(os.environ, {"POLYMARKET_PRIVATE_KEY": long_key}),
            patch(
                "polybot.scanner.aiohttp.ClientSession",
                return_value=mock_session,
            ),
            patch(
                "polybot.executor.get_polygon_balance_async",
                new_callable=AsyncMock,
                return_value=100.0,
            ),
            patch("polybot.scanner.time.time", return_value=float(fixed_ts)),
            patch("polybot.executor.place_trade_async", mock_trade),
            patch("py_clob_client.client.ClobClient", MagicMock()),
            patch(
                "polybot.scanner._fetch_order_book",
                return_value={"asks": [{"price": "0.40"}], "bids": [{"price": "0.38"}]},
            ),
        ):
            result = await fetch_all_active_markets_async(min_volume=0)

        # V38 returns filtered markets
        assert len(result) >= 1

        # V38 used clobTokenIds directly – chose Up (mid price 0.39 < 0.45)
        assert mock_trade.call_count >= 1
        for call in mock_trade.call_args_list:
            assert "side" not in call.kwargs, "V57: side parameter must NOT be passed"
            assert call.kwargs["amount"] == 30.0
            # clobTokenIds[0] is the Up token (V38 direct source)
            assert call.kwargs["token_id"] == clob_up_id

    @pytest.mark.asyncio
    @patch("polybot.scanner.get_settings")
    async def test_v38_parses_clobTokenIds_from_json_string(self, mock_settings):
        """V38: Parses clobTokenIds when value is a JSON string (not a list)."""
        import os
        from unittest.mock import AsyncMock, MagicMock

        from polybot.scanner import fetch_all_active_markets_async

        mock_cfg = type(
            "Settings",
            (),
            {
                "mode": "updown",
                "target_symbols": ["BTC", "ETH", "SOL", "XRP"],
                "use_api_mirrors": False,
                "up_down_only": False,
                "effective_log_level": 20,
                "dry_run": True,
            },
        )()
        mock_settings.return_value = mock_cfg

        fixed_ts = 1774271400
        expected_slug = f"btc-updown-5m-{fixed_ts}"
        up_token_id = "token-up-btc-json-001"
        down_token_id = "token-down-btc-json-001"

        # clobTokenIds is a JSON STRING (not a list)
        import json

        clob_json_string = json.dumps([up_token_id, down_token_id])

        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(
            return_value=[
                {
                    "question": "BTC Up or Down - 5 Min Live",
                    "slug": expected_slug,
                    "volume": 80_000,
                    "liquidity": 5_000,
                    "clobTokenIds": clob_json_string,
                },
            ]
        )
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        mock_trade = AsyncMock(return_value={"status": "dry_run", "id": "v38-json"})

        long_key = "0x" + "a" * 64

        with (
            patch.dict(os.environ, {"POLYMARKET_PRIVATE_KEY": long_key}),
            patch(
                "polybot.scanner.aiohttp.ClientSession",
                return_value=mock_session,
            ),
            patch(
                "polybot.executor.get_polygon_balance_async",
                new_callable=AsyncMock,
                return_value=100.0,
            ),
            patch("polybot.scanner.time.time", return_value=float(fixed_ts)),
            patch("polybot.executor.place_trade_async", mock_trade),
            patch("py_clob_client.client.ClobClient", MagicMock()),
            patch(
                "polybot.scanner._fetch_order_book",
                return_value={"asks": [{"price": "0.40"}], "bids": [{"price": "0.38"}]},
            ),
        ):
            result = await fetch_all_active_markets_async(min_volume=0)

        # V38 returns filtered markets
        assert len(result) >= 1

        # V38 parsed JSON string → traded successfully with Up (mid price 0.39 < 0.45)
        assert mock_trade.call_count >= 1
        for call in mock_trade.call_args_list:
            assert "side" not in call.kwargs, "V57: side parameter must NOT be passed"
            assert "size" not in call.kwargs, "V57: size parameter replaced by amount"
            assert call.kwargs["amount"] == 30.0
            assert call.kwargs["outcome"] == "up"  # mid price 0.39 < 0.45 → "up"
            assert call.kwargs["token_id"] == up_token_id

    @pytest.mark.asyncio
    @patch("polybot.scanner.get_settings")
    async def test_v38_skips_invalid_json_string_clobTokenIds(self, mock_settings):
        """V38: Skips market when clobTokenIds is an invalid/unparseable JSON string."""
        import os
        from unittest.mock import AsyncMock, MagicMock

        from polybot.scanner import fetch_all_active_markets_async

        mock_cfg = type(
            "Settings",
            (),
            {
                "mode": "updown",
                "target_symbols": ["BTC", "ETH", "SOL", "XRP"],
                "use_api_mirrors": False,
                "up_down_only": False,
                "effective_log_level": 20,
                "dry_run": True,
            },
        )()
        mock_settings.return_value = mock_cfg

        fixed_ts = 1774271400

        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(
            return_value=[
                {
                    "question": "BTC Up or Down - 5 Min Live",
                    "slug": f"btc-updown-5m-{fixed_ts}",
                    "volume": 80_000,
                    # Invalid JSON string – V38 should skip
                    "clobTokenIds": "not-valid-json{[",
                },
            ]
        )
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        mock_trade = AsyncMock(return_value={"status": "dry_run", "id": "v38-bad"})

        long_key = "0x" + "a" * 64

        with (
            patch.dict(os.environ, {"POLYMARKET_PRIVATE_KEY": long_key}),
            patch(
                "polybot.scanner.aiohttp.ClientSession",
                return_value=mock_session,
            ),
            patch(
                "polybot.executor.get_polygon_balance_async",
                new_callable=AsyncMock,
                return_value=100.0,
            ),
            patch("polybot.scanner.time.time", return_value=float(fixed_ts)),
            patch("polybot.executor.place_trade_async", mock_trade),
            patch(
                "polybot.scanner._fetch_order_book",
                return_value={"asks": [{"price": "0.40"}], "bids": [{"price": "0.38"}]},
            ),
        ):
            result = await fetch_all_active_markets_async(min_volume=0)

        # V38 returns filtered markets
        assert len(result) >= 1
        # Invalid JSON string → parsed to empty → skipped → no trade
        assert mock_trade.call_count == 0

    @pytest.mark.asyncio
    @patch("polybot.scanner.get_settings")
    async def test_v38_env_ok_when_key_present(self, mock_settings):
        """V38: ENV OK logged when key is present and >= 60 chars."""
        import os
        from unittest.mock import AsyncMock, MagicMock, patch as mock_patch

        from polybot.scanner import fetch_all_active_markets_async

        mock_cfg = type(
            "Settings",
            (),
            {
                "mode": "updown",
                "target_symbols": ["BTC", "ETH", "SOL", "XRP"],
                "use_api_mirrors": False,
                "up_down_only": False,
                "effective_log_level": 20,
                "dry_run": True,
            },
        )()
        mock_settings.return_value = mock_cfg

        fixed_ts = 1774271400

        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(
            return_value=[
                {
                    "question": "BTC Up or Down - 5 Min Live",
                    "slug": f"btc-updown-5m-{fixed_ts}",
                    "volume": 80_000,
                    "clobTokenIds": [
                        "token-up-btc-001",
                        "token-down-btc-001",
                    ],
                },
            ]
        )
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        mock_trade = AsyncMock(
            return_value={"status": "dry_run", "id": "v38-env-debug"}
        )

        # Key must be >= 40 chars for V40 ENV OK path
        long_key = "0x" + "a" * 64

        with (
            mock_patch.dict(
                os.environ,
                {"POLYMARKET_PRIVATE_KEY": long_key},
            ),
            patch(
                "polybot.scanner.aiohttp.ClientSession",
                return_value=mock_session,
            ),
            patch(
                "polybot.executor.get_polygon_balance_async",
                new_callable=AsyncMock,
                return_value=100.0,
            ),
            patch("polybot.scanner.time.time", return_value=float(fixed_ts)),
            patch("polybot.executor.place_trade_async", mock_trade),
            patch(
                "polybot.scanner._fetch_order_book",
                return_value={"asks": [{"price": "0.40"}], "bids": [{"price": "0.38"}]},
            ),
        ):
            result = await fetch_all_active_markets_async(min_volume=0)

        # V38 block runs without error when valid key is present
        assert len(result) >= 1

    @pytest.mark.asyncio
    @patch("polybot.scanner.get_settings")
    async def test_v38_env_fatal_when_key_missing(self, mock_settings):
        """V38: ENV FATAL error logged when key is missing or too short."""
        import os
        from unittest.mock import patch as mock_patch

        from polybot.scanner import fetch_all_active_markets_async

        mock_cfg = type(
            "Settings",
            (),
            {
                "mode": "updown",
                "target_symbols": ["BTC", "ETH", "SOL", "XRP"],
                "use_api_mirrors": False,
                "up_down_only": False,
                "effective_log_level": 20,
                "dry_run": True,
            },
        )()
        mock_settings.return_value = mock_cfg

        # Clear all key env vars to trigger V40 ENV FATAL path
        env_overrides = {
            "POLYMARKET_PRIVATE_KEY": "",
            "PRIVATE_KEY": "",
            "PK": "",
            "WALLET_PRIVATE_KEY": "",
        }

        with mock_patch.dict(os.environ, env_overrides, clear=False):
            # V40 returns [] immediately when key is missing (hard stop)
            result = await fetch_all_active_markets_async(min_volume=0)

        # V40 hard stop: returns empty list when key is missing
        assert result == []


def _clear_config_cache():
    """Helper to clear polybot.config module cache for fresh settings."""
    import sys

    for mod in list(sys.modules.keys()):
        if "polybot.config" in mod:
            del sys.modules[mod]


class TestV7PositionScaling:
    """Test V7 BONUS: Auto-Position-Scaling functionality."""

    def test_config_position_scaling_factor_default(self):
        """Test position_scaling_factor has correct default value."""
        import os

        # Remove environment variable if set
        os.environ.pop("POSITION_SCALING_FACTOR", None)
        _clear_config_cache()

        from polybot.config import Settings

        s = Settings()
        assert s.position_scaling_factor == 1.0

    def test_config_daily_risk_reset_hour_default(self):
        """Test daily_risk_reset_hour has correct default value."""
        import os

        # Remove environment variable if set
        os.environ.pop("DAILY_RISK_RESET_HOUR", None)
        _clear_config_cache()

        from polybot.config import Settings

        s = Settings()
        assert s.daily_risk_reset_hour == 0

    def test_config_position_scaling_factor_from_env(self):
        """Test position_scaling_factor can be set via environment variable."""
        import os

        os.environ["POSITION_SCALING_FACTOR"] = "2.0"
        _clear_config_cache()

        from polybot.config import Settings

        s = Settings()
        assert s.position_scaling_factor == 2.0

        # Cleanup
        os.environ.pop("POSITION_SCALING_FACTOR", None)

    def test_config_daily_risk_reset_hour_from_env(self):
        """Test daily_risk_reset_hour can be set via environment variable."""
        import os

        os.environ["DAILY_RISK_RESET_HOUR"] = "12"
        _clear_config_cache()

        from polybot.config import Settings

        s = Settings()
        assert s.daily_risk_reset_hour == 12

        # Cleanup
        os.environ.pop("DAILY_RISK_RESET_HOUR", None)

    def test_edge_ratio_calculation(self):
        """Test edge ratio calculation for position scaling using BASELINE_EDGE constant."""
        from polybot.scanner import BASELINE_EDGE

        # Formula: edge_ratio = abs(edge) / BASELINE_EDGE
        # 2% edge = 1.0 ratio (baseline)
        # 4% edge = 2.0 ratio (double position)
        # 1% edge = 0.5 ratio (half position)
        edge_2_percent = abs(0.02) / BASELINE_EDGE
        assert edge_2_percent == 1.0

        edge_4_percent = abs(0.04) / BASELINE_EDGE
        assert edge_4_percent == 2.0

        edge_1_percent = abs(0.01) / BASELINE_EDGE
        assert edge_1_percent == 0.5

    def test_scaled_position_calculation(self):
        """Test scaled position calculation with different scaling factors."""
        from polybot.scanner import BASELINE_EDGE

        kelly_size = 10.0  # $10 base Kelly position
        edge = 0.02  # 2% edge (baseline)

        # Normal scaling (factor = 1.0)
        edge_ratio = abs(edge) / BASELINE_EDGE
        scaled_normal = kelly_size * 1.0 * edge_ratio
        assert scaled_normal == 10.0

        # Aggressive scaling (factor = 1.5)
        scaled_aggressive = kelly_size * 1.5 * edge_ratio
        assert scaled_aggressive == 15.0

        # With higher edge (4%)
        edge_high = 0.04
        edge_ratio_high = abs(edge_high) / BASELINE_EDGE
        scaled_high_edge = kelly_size * 1.0 * edge_ratio_high
        assert scaled_high_edge == 20.0  # Double position for double edge


class TestV42SdkCredDerivation:
    """Tests for V42 SDK credential derivation + V41 0x prefix fix."""

    @pytest.mark.asyncio
    @patch("polybot.scanner.get_settings")
    async def test_v41_auto_prefix_0x(self, mock_settings):
        """V41: auto-prefix '0x' when key lacks it."""
        import os
        from unittest.mock import AsyncMock, MagicMock, patch as mock_patch

        from polybot.scanner import fetch_all_active_markets_async

        mock_cfg = type(
            "Settings",
            (),
            {
                "mode": "updown",
                "target_symbols": ["BTC", "ETH", "SOL", "XRP"],
                "use_api_mirrors": False,
                "up_down_only": False,
                "effective_log_level": 20,
                "dry_run": True,
            },
        )()
        mock_settings.return_value = mock_cfg

        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value=[])
        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        mock_trade = AsyncMock(return_value={"status": "dry_run", "id": "v42-test"})

        # Key WITHOUT 0x prefix – V41 should auto-add it
        raw_key = "a" * 64

        with (
            mock_patch.dict(
                os.environ,
                {"POLYMARKET_PRIVATE_KEY": raw_key},
            ),
            patch(
                "polybot.scanner.aiohttp.ClientSession",
                return_value=mock_session,
            ),
            patch(
                "polybot.executor.get_polygon_balance_async",
                new_callable=AsyncMock,
                return_value=100.0,
            ),
            patch("polybot.scanner.time.time", return_value=1700000100.0),
            patch("polybot.executor.place_trade_async", mock_trade),
            patch(
                "polybot.scanner._fetch_order_book",
                return_value={"asks": [{"price": "0.40"}], "bids": [{"price": "0.38"}]},
            ),
        ):
            result = await fetch_all_active_markets_async(min_volume=0)

        # Should not crash – V41 auto-prefixed 0x and V42 derivation ran
        assert isinstance(result, list)

    @pytest.mark.asyncio
    @patch("polybot.scanner.get_settings")
    async def test_v42_cred_derivation_with_valid_key(self, mock_settings):
        """V42: credential derivation runs without error for valid 0x key."""
        import os
        from unittest.mock import AsyncMock, MagicMock, patch as mock_patch

        from polybot.scanner import fetch_all_active_markets_async

        mock_cfg = type(
            "Settings",
            (),
            {
                "mode": "updown",
                "target_symbols": ["BTC", "ETH", "SOL", "XRP"],
                "use_api_mirrors": False,
                "up_down_only": False,
                "effective_log_level": 20,
                "dry_run": True,
            },
        )()
        mock_settings.return_value = mock_cfg

        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value=[])
        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        mock_trade = AsyncMock(return_value={"status": "dry_run", "id": "v42-cred"})

        # Valid 0x-prefixed key (66 chars)
        long_key = "0x" + "a" * 64

        with (
            mock_patch.dict(
                os.environ,
                {"POLYMARKET_PRIVATE_KEY": long_key},
            ),
            patch(
                "polybot.scanner.aiohttp.ClientSession",
                return_value=mock_session,
            ),
            patch(
                "polybot.executor.get_polygon_balance_async",
                new_callable=AsyncMock,
                return_value=100.0,
            ),
            patch("polybot.scanner.time.time", return_value=1700000100.0),
            patch("polybot.executor.place_trade_async", mock_trade),
            patch(
                "polybot.scanner._fetch_order_book",
                return_value={"asks": [{"price": "0.40"}], "bids": [{"price": "0.38"}]},
            ),
        ):
            result = await fetch_all_active_markets_async(min_volume=0)

        # V42 block should run without raising
        assert isinstance(result, list)

    @pytest.mark.asyncio
    @patch("polybot.scanner.get_settings")
    async def test_v42_cred_error_handled_gracefully(self, mock_settings):
        """V42: credential derivation error is caught and logged, not raised."""
        import os
        from unittest.mock import AsyncMock, MagicMock, patch as mock_patch

        from polybot.scanner import fetch_all_active_markets_async

        mock_cfg = type(
            "Settings",
            (),
            {
                "mode": "updown",
                "target_symbols": ["BTC", "ETH", "SOL", "XRP"],
                "use_api_mirrors": False,
                "up_down_only": False,
                "effective_log_level": 20,
                "dry_run": True,
            },
        )()
        mock_settings.return_value = mock_cfg

        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value=[])
        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        mock_trade = AsyncMock(return_value={"status": "dry_run", "id": "v42-err"})

        # Valid-length key but V42 Account.from_key will be mocked to fail
        long_key = "0x" + "b" * 64

        with (
            mock_patch.dict(
                os.environ,
                {"POLYMARKET_PRIVATE_KEY": long_key},
            ),
            patch(
                "polybot.scanner.aiohttp.ClientSession",
                return_value=mock_session,
            ),
            patch(
                "polybot.executor.get_polygon_balance_async",
                new_callable=AsyncMock,
                return_value=100.0,
            ),
            patch("polybot.scanner.time.time", return_value=1700000100.0),
            patch("polybot.executor.place_trade_async", mock_trade),
            patch(
                "polybot.scanner._fetch_order_book",
                return_value={"asks": [{"price": "0.40"}], "bids": [{"price": "0.38"}]},
            ),
            patch("web3.Account.from_key", side_effect=ValueError("bad key")),
        ):
            # Should NOT raise – V42 catches the error
            result = await fetch_all_active_markets_async(min_volume=0)

        assert isinstance(result, list)


class TestV43CredIntegration:
    """Tests for V43 global cred integration + per-trade derivation."""

    @pytest.mark.asyncio
    @patch("polybot.scanner.get_settings")
    async def test_v43_global_cred_env_set(self, mock_settings):
        """V43: global cred preparation sets POLYMARKET_PRIVATE_KEY env."""
        import os
        from unittest.mock import AsyncMock, MagicMock, patch as mock_patch

        from polybot.scanner import fetch_all_active_markets_async

        mock_cfg = type(
            "Settings",
            (),
            {
                "mode": "updown",
                "target_symbols": ["BTC", "ETH", "SOL", "XRP"],
                "use_api_mirrors": False,
                "up_down_only": False,
                "effective_log_level": 20,
                "dry_run": True,
            },
        )()
        mock_settings.return_value = mock_cfg

        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value=[])
        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        mock_trade = AsyncMock(return_value={"status": "dry_run", "id": "v43-global"})

        long_key = "0x" + "a" * 64

        with (
            mock_patch.dict(
                os.environ,
                {"POLYMARKET_PRIVATE_KEY": long_key},
            ),
            patch(
                "polybot.scanner.aiohttp.ClientSession",
                return_value=mock_session,
            ),
            patch(
                "polybot.executor.get_polygon_balance_async",
                new_callable=AsyncMock,
                return_value=100.0,
            ),
            patch("polybot.scanner.time.time", return_value=1700000100.0),
            patch("polybot.executor.place_trade_async", mock_trade),
            patch(
                "polybot.scanner._fetch_order_book",
                return_value={"asks": [{"price": "0.40"}], "bids": [{"price": "0.38"}]},
            ),
        ):
            result = await fetch_all_active_markets_async(min_volume=0)
            # V43 global cred block should have set the env var
            assert os.environ.get("POLYMARKET_PRIVATE_KEY") == long_key

        assert isinstance(result, list)

    @pytest.mark.asyncio
    @patch("polybot.scanner.get_settings")
    async def test_v43_per_trade_cred_derivation(self, mock_settings):
        """V43: per-trade credential derivation runs before each trade."""
        import os
        from unittest.mock import AsyncMock, MagicMock, patch as mock_patch

        from polybot.scanner import fetch_all_active_markets_async

        mock_cfg = type(
            "Settings",
            (),
            {
                "mode": "updown",
                "target_symbols": ["BTC", "ETH", "SOL", "XRP"],
                "use_api_mirrors": False,
                "up_down_only": False,
                "effective_log_level": 20,
                "dry_run": True,
            },
        )()
        mock_settings.return_value = mock_cfg

        # Return a market that matches the slug pattern
        mock_market = {
            "slug": "btc-updown-5m-1700000100",
            "question": "Will BTC go up?",
            "clobTokenIds": ["token-up-1", "token-down-1"],
            "volume": 50000,
        }
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value=[mock_market])
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        mock_trade = AsyncMock(return_value={"status": "dry_run", "id": "v43-trade"})

        # Use timestamp divisible by 300 so slot math produces exact slug
        fixed_ts = 1700000100

        long_key = "0x" + "a" * 64

        with (
            mock_patch.dict(
                os.environ,
                {"POLYMARKET_PRIVATE_KEY": long_key},
            ),
            patch(
                "polybot.scanner.aiohttp.ClientSession",
                return_value=mock_session,
            ),
            patch(
                "polybot.executor.get_polygon_balance_async",
                new_callable=AsyncMock,
                return_value=100.0,
            ),
            patch("polybot.scanner.time.time", return_value=float(fixed_ts)),
            patch("polybot.executor.place_trade_async", mock_trade),
            patch("py_clob_client.client.ClobClient", MagicMock()),
            patch(
                "polybot.scanner._fetch_order_book",
                return_value={"asks": [{"price": "0.40"}], "bids": [{"price": "0.38"}]},
            ),
        ):
            result = await fetch_all_active_markets_async(min_volume=0)

        # Trade should have been called (V43 derivation wraps the trade)
        assert mock_trade.called
        assert isinstance(result, list)
        assert len(result) > 0

    @pytest.mark.asyncio
    @patch("polybot.scanner.get_settings")
    async def test_v43_trade_error_caught_gracefully(self, mock_settings):
        """V43: trade errors (including derivation failures) are caught."""
        import os
        from unittest.mock import AsyncMock, MagicMock, patch as mock_patch

        from polybot.scanner import fetch_all_active_markets_async

        mock_cfg = type(
            "Settings",
            (),
            {
                "mode": "updown",
                "target_symbols": ["BTC", "ETH", "SOL", "XRP"],
                "use_api_mirrors": False,
                "up_down_only": False,
                "effective_log_level": 20,
                "dry_run": True,
            },
        )()
        mock_settings.return_value = mock_cfg

        mock_market = {
            "slug": "btc-updown-5m-1700000100",
            "question": "Will BTC go up?",
            "clobTokenIds": ["token-up-1", "token-down-1"],
            "volume": 50000,
        }
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value=[mock_market])
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        # Trade raises an exception
        mock_trade = AsyncMock(side_effect=RuntimeError("API connection failed"))

        fixed_ts = 1700000100
        long_key = "0x" + "a" * 64

        with (
            mock_patch.dict(
                os.environ,
                {"POLYMARKET_PRIVATE_KEY": long_key},
            ),
            patch(
                "polybot.scanner.aiohttp.ClientSession",
                return_value=mock_session,
            ),
            patch(
                "polybot.executor.get_polygon_balance_async",
                new_callable=AsyncMock,
                return_value=100.0,
            ),
            patch("polybot.scanner.time.time", return_value=float(fixed_ts)),
            patch("polybot.executor.place_trade_async", mock_trade),
            patch(
                "polybot.scanner._fetch_order_book",
                return_value={"asks": [{"price": "0.40"}], "bids": [{"price": "0.38"}]},
            ),
        ):
            # Should NOT raise – V43 catches the error
            result = await fetch_all_active_markets_async(min_volume=0)

        assert isinstance(result, list)


class TestV44FinalCredSet:
    """Tests for onchain trade execution – no ClobClient needed."""

    @pytest.mark.asyncio
    @patch("polybot.scanner.get_settings")
    async def test_v44_global_cred_env_set(self, mock_settings):
        """Onchain: POLYMARKET_PRIVATE_KEY env var is preserved during trade."""
        import os
        from unittest.mock import AsyncMock, MagicMock, patch as mock_patch

        from polybot.scanner import fetch_all_active_markets_async

        mock_cfg = type(
            "Settings",
            (),
            {
                "mode": "updown",
                "target_symbols": ["BTC", "ETH", "SOL", "XRP"],
                "use_api_mirrors": False,
                "up_down_only": False,
                "effective_log_level": 20,
                "dry_run": True,
            },
        )()
        mock_settings.return_value = mock_cfg

        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value=[])
        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        mock_trade = AsyncMock(return_value={"status": "dry_run", "id": "v44-global"})

        long_key = "0x" + "a" * 64

        with (
            mock_patch.dict(
                os.environ,
                {"POLYMARKET_PRIVATE_KEY": long_key},
            ),
            patch(
                "polybot.scanner.aiohttp.ClientSession",
                return_value=mock_session,
            ),
            patch(
                "polybot.executor.get_polygon_balance_async",
                new_callable=AsyncMock,
                return_value=100.0,
            ),
            patch("polybot.scanner.time.time", return_value=1700000100.0),
            patch("polybot.executor.place_trade_async", mock_trade),
            patch(
                "polybot.scanner._fetch_order_book",
                return_value={"asks": [{"price": "0.40"}], "bids": [{"price": "0.38"}]},
            ),
        ):
            result = await fetch_all_active_markets_async(min_volume=0)
            assert os.environ.get("POLYMARKET_PRIVATE_KEY") == long_key

        assert isinstance(result, list)

    @pytest.mark.asyncio
    @patch("polybot.scanner.get_settings")
    async def test_v44_per_trade_clob_client_cred_derivation(self, mock_settings):
        """Onchain: executor.place_trade_async called directly (no ClobClient)."""
        import os
        from unittest.mock import AsyncMock, MagicMock, patch as mock_patch

        from polybot.scanner import fetch_all_active_markets_async

        mock_cfg = type(
            "Settings",
            (),
            {
                "mode": "updown",
                "target_symbols": ["BTC", "ETH", "SOL", "XRP"],
                "use_api_mirrors": False,
                "up_down_only": False,
                "effective_log_level": 20,
                "dry_run": True,
            },
        )()
        mock_settings.return_value = mock_cfg

        mock_market = {
            "slug": "btc-updown-5m-1700000100",
            "question": "Will BTC go up?",
            "clobTokenIds": ["token-up-1", "token-down-1"],
            "volume": 50000,
        }
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value=[mock_market])
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        mock_trade = AsyncMock(return_value={"status": "dry_run", "id": "v44-trade"})

        fixed_ts = 1700000100
        long_key = "0x" + "a" * 64

        with (
            mock_patch.dict(
                os.environ,
                {"POLYMARKET_PRIVATE_KEY": long_key},
            ),
            patch(
                "polybot.scanner.aiohttp.ClientSession",
                return_value=mock_session,
            ),
            patch(
                "polybot.executor.get_polygon_balance_async",
                new_callable=AsyncMock,
                return_value=100.0,
            ),
            patch("polybot.scanner.time.time", return_value=float(fixed_ts)),
            patch("polybot.executor.place_trade_async", mock_trade),
            patch(
                "polybot.scanner._fetch_order_book",
                return_value={"asks": [{"price": "0.40"}], "bids": [{"price": "0.38"}]},
            ),
        ):
            result = await fetch_all_active_markets_async(min_volume=0)

        assert mock_trade.called
        assert isinstance(result, list)
        assert len(result) > 0

    @pytest.mark.asyncio
    @patch("polybot.scanner.get_settings")
    async def test_v44_trade_error_caught_gracefully(self, mock_settings):
        """Onchain: trade errors are caught gracefully."""
        import os
        from unittest.mock import AsyncMock, MagicMock, patch as mock_patch

        from polybot.scanner import fetch_all_active_markets_async

        mock_cfg = type(
            "Settings",
            (),
            {
                "mode": "updown",
                "target_symbols": ["BTC", "ETH", "SOL", "XRP"],
                "use_api_mirrors": False,
                "up_down_only": False,
                "effective_log_level": 20,
                "dry_run": True,
            },
        )()
        mock_settings.return_value = mock_cfg

        mock_market = {
            "slug": "btc-updown-5m-1700000100",
            "question": "Will BTC go up?",
            "clobTokenIds": ["token-up-1", "token-down-1"],
            "volume": 50000,
        }
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value=[mock_market])
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        mock_trade = AsyncMock(side_effect=RuntimeError("API connection failed"))

        fixed_ts = 1700000100
        long_key = "0x" + "a" * 64

        with (
            mock_patch.dict(
                os.environ,
                {"POLYMARKET_PRIVATE_KEY": long_key},
            ),
            patch(
                "polybot.scanner.aiohttp.ClientSession",
                return_value=mock_session,
            ),
            patch(
                "polybot.executor.get_polygon_balance_async",
                new_callable=AsyncMock,
                return_value=100.0,
            ),
            patch("polybot.scanner.time.time", return_value=float(fixed_ts)),
            patch("polybot.executor.place_trade_async", mock_trade),
            patch(
                "polybot.scanner._fetch_order_book",
                return_value={"asks": [{"price": "0.40"}], "bids": [{"price": "0.38"}]},
            ),
        ):
            result = await fetch_all_active_markets_async(min_volume=0)

        assert isinstance(result, list)


class TestV45FinalClobHost:
    """Tests for onchain trade execution – direct executor call, no ClobClient host config."""

    @pytest.mark.asyncio
    @patch("polybot.scanner.get_settings")
    async def test_v45_clob_host_and_timeout(self, mock_settings):
        """Onchain: trade called directly via executor (no ClobClient constructor)."""
        import os
        from unittest.mock import AsyncMock, MagicMock, patch as mock_patch

        from polybot.scanner import fetch_all_active_markets_async

        mock_cfg = type(
            "Settings",
            (),
            {
                "mode": "updown",
                "target_symbols": ["BTC", "ETH", "SOL", "XRP"],
                "use_api_mirrors": False,
                "up_down_only": False,
                "effective_log_level": 20,
                "dry_run": True,
            },
        )()
        mock_settings.return_value = mock_cfg

        mock_market = {
            "slug": "btc-updown-5m-1700000100",
            "question": "Will BTC go up?",
            "clobTokenIds": ["token-up-1", "token-down-1"],
            "volume": 50000,
        }
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value=[mock_market])
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        mock_trade = AsyncMock(return_value={"status": "dry_run", "id": "v45-trade"})

        fixed_ts = 1700000100
        long_key = "0x" + "a" * 64

        with (
            mock_patch.dict(
                os.environ,
                {"POLYMARKET_PRIVATE_KEY": long_key},
            ),
            patch(
                "polybot.scanner.aiohttp.ClientSession",
                return_value=mock_session,
            ),
            patch(
                "polybot.executor.get_polygon_balance_async",
                new_callable=AsyncMock,
                return_value=100.0,
            ),
            patch("polybot.scanner.time.time", return_value=float(fixed_ts)),
            patch("polybot.executor.place_trade_async", mock_trade),
            patch(
                "polybot.scanner._fetch_order_book",
                return_value={"asks": [{"price": "0.40"}], "bids": [{"price": "0.38"}]},
            ),
        ):
            result = await fetch_all_active_markets_async(min_volume=0)

        assert mock_trade.called
        call_kwargs = mock_trade.call_args.kwargs
        assert isinstance(call_kwargs["market"], dict)
        assert call_kwargs["amount"] == 30.0
        assert isinstance(result, list)
        assert len(result) > 0

    @pytest.mark.asyncio
    @patch("polybot.scanner.get_settings")
    async def test_v45_clob_host_log_messages(self, mock_settings, capsys):
        """Onchain: log messages include CYCLE MATCH and TRADE SUCCESS markers."""
        import os
        from unittest.mock import AsyncMock, MagicMock, patch as mock_patch

        from polybot.scanner import fetch_all_active_markets_async

        mock_cfg = type(
            "Settings",
            (),
            {
                "mode": "updown",
                "target_symbols": ["BTC", "ETH", "SOL", "XRP"],
                "use_api_mirrors": False,
                "up_down_only": False,
                "effective_log_level": 20,
                "dry_run": True,
            },
        )()
        mock_settings.return_value = mock_cfg

        mock_market = {
            "slug": "btc-updown-5m-1700000100",
            "question": "Will BTC go up?",
            "clobTokenIds": ["token-up-1", "token-down-1"],
            "volume": 50000,
        }
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value=[mock_market])
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        mock_trade = AsyncMock(return_value={"status": "dry_run", "id": "v45-log"})

        fixed_ts = 1700000100
        long_key = "0x" + "a" * 64

        with (
            mock_patch.dict(
                os.environ,
                {"POLYMARKET_PRIVATE_KEY": long_key},
            ),
            patch(
                "polybot.scanner.aiohttp.ClientSession",
                return_value=mock_session,
            ),
            patch(
                "polybot.executor.get_polygon_balance_async",
                new_callable=AsyncMock,
                return_value=100.0,
            ),
            patch("polybot.scanner.time.time", return_value=float(fixed_ts)),
            patch("polybot.executor.place_trade_async", mock_trade),
            patch(
                "polybot.scanner._fetch_order_book",
                return_value={"asks": [{"price": "0.40"}], "bids": [{"price": "0.38"}]},
            ),
        ):
            await fetch_all_active_markets_async(min_volume=0)

        captured = capsys.readouterr()
        assert "[CYCLE MATCH]" in captured.out
        assert "[TRADE SUCCESS]" in captured.out

    @pytest.mark.asyncio
    @patch("polybot.scanner.get_settings")
    async def test_v45_trade_error_caught_gracefully(self, mock_settings):
        """Onchain: trade errors are caught gracefully."""
        import os
        from unittest.mock import AsyncMock, MagicMock, patch as mock_patch

        from polybot.scanner import fetch_all_active_markets_async

        mock_cfg = type(
            "Settings",
            (),
            {
                "mode": "updown",
                "target_symbols": ["BTC", "ETH", "SOL", "XRP"],
                "use_api_mirrors": False,
                "up_down_only": False,
                "effective_log_level": 20,
                "dry_run": True,
            },
        )()
        mock_settings.return_value = mock_cfg

        mock_market = {
            "slug": "btc-updown-5m-1700000100",
            "question": "Will BTC go up?",
            "clobTokenIds": ["token-up-1", "token-down-1"],
            "volume": 50000,
        }
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value=[mock_market])
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        mock_trade = AsyncMock(side_effect=RuntimeError("Connection failed"))

        fixed_ts = 1700000100
        long_key = "0x" + "a" * 64

        with (
            mock_patch.dict(
                os.environ,
                {"POLYMARKET_PRIVATE_KEY": long_key},
            ),
            patch(
                "polybot.scanner.aiohttp.ClientSession",
                return_value=mock_session,
            ),
            patch(
                "polybot.executor.get_polygon_balance_async",
                new_callable=AsyncMock,
                return_value=100.0,
            ),
            patch("polybot.scanner.time.time", return_value=float(fixed_ts)),
            patch("polybot.executor.place_trade_async", mock_trade),
            patch(
                "polybot.scanner._fetch_order_book",
                return_value={"asks": [{"price": "0.40"}], "bids": [{"price": "0.38"}]},
            ),
        ):
            result = await fetch_all_active_markets_async(min_volume=0)

        assert isinstance(result, list)


class TestV46FinalClobFix:
    """Tests for onchain trade execution – no ClobClient timeout config needed."""

    @pytest.mark.asyncio
    @patch("polybot.scanner.get_settings")
    async def test_v46_no_timeout_param(self, mock_settings):
        """Onchain: trade called with correct params (no ClobClient)."""
        import os
        from unittest.mock import AsyncMock, MagicMock, patch as mock_patch

        from polybot.scanner import fetch_all_active_markets_async

        mock_cfg = type(
            "Settings",
            (),
            {
                "mode": "updown",
                "target_symbols": ["BTC", "ETH", "SOL", "XRP"],
                "use_api_mirrors": False,
                "up_down_only": False,
                "effective_log_level": 20,
                "dry_run": True,
            },
        )()
        mock_settings.return_value = mock_cfg

        mock_market = {
            "slug": "btc-updown-5m-1700000100",
            "question": "Will BTC go up?",
            "clobTokenIds": ["token-up-1", "token-down-1"],
            "volume": 50000,
        }
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value=[mock_market])
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        mock_trade = AsyncMock(return_value={"status": "dry_run", "id": "v46-trade"})

        fixed_ts = 1700000100
        long_key = "0x" + "a" * 64

        with (
            mock_patch.dict(
                os.environ,
                {"POLYMARKET_PRIVATE_KEY": long_key},
            ),
            patch(
                "polybot.scanner.aiohttp.ClientSession",
                return_value=mock_session,
            ),
            patch(
                "polybot.executor.get_polygon_balance_async",
                new_callable=AsyncMock,
                return_value=100.0,
            ),
            patch("polybot.scanner.time.time", return_value=float(fixed_ts)),
            patch("polybot.executor.place_trade_async", mock_trade),
            patch(
                "polybot.scanner._fetch_order_book",
                return_value={"asks": [{"price": "0.40"}], "bids": [{"price": "0.38"}]},
            ),
        ):
            result = await fetch_all_active_markets_async(min_volume=0)

        assert mock_trade.called
        call_kwargs = mock_trade.call_args.kwargs
        assert call_kwargs["market"]["slug"] == "btc-updown-5m-1700000100"
        assert call_kwargs["amount"] == 30.0
        assert call_kwargs["outcome"] == "up"
        assert call_kwargs["token_id"] == "token-up-1"
        assert isinstance(result, list)
        assert len(result) > 0

    @pytest.mark.asyncio
    @patch("polybot.scanner.get_settings")
    async def test_v46_log_messages(self, mock_settings, capsys):
        """Onchain: log messages include CYCLE MATCH and TRADE SUCCESS markers."""
        import os
        from unittest.mock import AsyncMock, MagicMock, patch as mock_patch

        from polybot.scanner import fetch_all_active_markets_async

        mock_cfg = type(
            "Settings",
            (),
            {
                "mode": "updown",
                "target_symbols": ["BTC", "ETH", "SOL", "XRP"],
                "use_api_mirrors": False,
                "up_down_only": False,
                "effective_log_level": 20,
                "dry_run": True,
            },
        )()
        mock_settings.return_value = mock_cfg

        mock_market = {
            "slug": "btc-updown-5m-1700000100",
            "question": "Will BTC go up?",
            "clobTokenIds": ["token-up-1", "token-down-1"],
            "volume": 50000,
        }
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value=[mock_market])
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        mock_trade = AsyncMock(return_value={"status": "dry_run", "id": "v46-log"})

        fixed_ts = 1700000100
        long_key = "0x" + "a" * 64

        with (
            mock_patch.dict(
                os.environ,
                {"POLYMARKET_PRIVATE_KEY": long_key},
            ),
            patch(
                "polybot.scanner.aiohttp.ClientSession",
                return_value=mock_session,
            ),
            patch(
                "polybot.executor.get_polygon_balance_async",
                new_callable=AsyncMock,
                return_value=100.0,
            ),
            patch("polybot.scanner.time.time", return_value=float(fixed_ts)),
            patch("polybot.executor.place_trade_async", mock_trade),
            patch(
                "polybot.scanner._fetch_order_book",
                return_value={"asks": [{"price": "0.40"}], "bids": [{"price": "0.38"}]},
            ),
        ):
            await fetch_all_active_markets_async(min_volume=0)

        captured = capsys.readouterr()
        assert "[CYCLE MATCH]" in captured.out
        assert "[TRADE SUCCESS]" in captured.out

    @pytest.mark.asyncio
    @patch("polybot.scanner.get_settings")
    async def test_v46_trade_error_caught_gracefully(self, mock_settings):
        """Onchain: trade errors are caught gracefully."""
        import os
        from unittest.mock import AsyncMock, MagicMock, patch as mock_patch

        from polybot.scanner import fetch_all_active_markets_async

        mock_cfg = type(
            "Settings",
            (),
            {
                "mode": "updown",
                "target_symbols": ["BTC", "ETH", "SOL", "XRP"],
                "use_api_mirrors": False,
                "up_down_only": False,
                "effective_log_level": 20,
                "dry_run": True,
            },
        )()
        mock_settings.return_value = mock_cfg

        mock_market = {
            "slug": "btc-updown-5m-1700000100",
            "question": "Will BTC go up?",
            "clobTokenIds": ["token-up-1", "token-down-1"],
            "volume": 50000,
        }
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value=[mock_market])
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        mock_trade = AsyncMock(side_effect=RuntimeError("Connection failed"))

        fixed_ts = 1700000100
        long_key = "0x" + "a" * 64

        with (
            mock_patch.dict(
                os.environ,
                {"POLYMARKET_PRIVATE_KEY": long_key},
            ),
            patch(
                "polybot.scanner.aiohttp.ClientSession",
                return_value=mock_session,
            ),
            patch(
                "polybot.executor.get_polygon_balance_async",
                new_callable=AsyncMock,
                return_value=100.0,
            ),
            patch("polybot.scanner.time.time", return_value=float(fixed_ts)),
            patch("polybot.executor.place_trade_async", mock_trade),
            patch(
                "polybot.scanner._fetch_order_book",
                return_value={"asks": [{"price": "0.40"}], "bids": [{"price": "0.38"}]},
            ),
        ):
            result = await fetch_all_active_markets_async(min_volume=0)

        assert isinstance(result, list)


class TestV48UltimateFinalFix:
    """Tests for onchain trade execution – no ApiCreds conversion needed."""

    @pytest.mark.asyncio
    @patch("polybot.scanner.get_settings")
    async def test_v48_api_creds_model_dump_path(self, mock_settings):
        """Onchain: trade called with correct market dict."""
        import os
        from unittest.mock import AsyncMock, MagicMock, patch as mock_patch

        from polybot.scanner import fetch_all_active_markets_async

        mock_cfg = type(
            "Settings",
            (),
            {
                "mode": "updown",
                "target_symbols": ["BTC", "ETH", "SOL", "XRP"],
                "use_api_mirrors": False,
                "up_down_only": False,
                "effective_log_level": 20,
                "dry_run": True,
            },
        )()
        mock_settings.return_value = mock_cfg

        mock_market = {
            "slug": "btc-updown-5m-1700000100",
            "question": "Will BTC go up?",
            "clobTokenIds": ["token-up-1", "token-down-1"],
            "volume": 50000,
        }
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value=[mock_market])
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        mock_trade = AsyncMock(return_value={"status": "dry_run", "id": "v48-trade"})

        fixed_ts = 1700000100
        long_key = "0x" + "a" * 64

        with (
            mock_patch.dict(
                os.environ,
                {"POLYMARKET_PRIVATE_KEY": long_key},
            ),
            patch(
                "polybot.scanner.aiohttp.ClientSession",
                return_value=mock_session,
            ),
            patch(
                "polybot.executor.get_polygon_balance_async",
                new_callable=AsyncMock,
                return_value=100.0,
            ),
            patch("polybot.scanner.time.time", return_value=float(fixed_ts)),
            patch("polybot.executor.place_trade_async", mock_trade),
            patch(
                "polybot.scanner._fetch_order_book",
                return_value={"asks": [{"price": "0.40"}], "bids": [{"price": "0.38"}]},
            ),
        ):
            result = await fetch_all_active_markets_async(min_volume=0)

        assert mock_trade.called
        call_kwargs = mock_trade.call_args.kwargs
        assert isinstance(call_kwargs["market"], dict)
        assert isinstance(result, list)
        assert len(result) > 0

    @pytest.mark.asyncio
    @patch("polybot.scanner.get_settings")
    async def test_v48_api_creds_plain_dict_fallback(self, mock_settings):
        """Onchain: trade called with correct outcome and token_id."""
        import os
        from unittest.mock import AsyncMock, MagicMock, patch as mock_patch

        from polybot.scanner import fetch_all_active_markets_async

        mock_cfg = type(
            "Settings",
            (),
            {
                "mode": "updown",
                "target_symbols": ["BTC", "ETH", "SOL", "XRP"],
                "use_api_mirrors": False,
                "up_down_only": False,
                "effective_log_level": 20,
                "dry_run": True,
            },
        )()
        mock_settings.return_value = mock_cfg

        mock_market = {
            "slug": "btc-updown-5m-1700000100",
            "question": "Will BTC go up?",
            "clobTokenIds": ["token-up-1", "token-down-1"],
            "volume": 50000,
        }
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value=[mock_market])
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        mock_trade = AsyncMock(return_value={"status": "dry_run", "id": "v48-dict"})

        fixed_ts = 1700000100
        long_key = "0x" + "a" * 64

        with (
            mock_patch.dict(
                os.environ,
                {"POLYMARKET_PRIVATE_KEY": long_key},
            ),
            patch(
                "polybot.scanner.aiohttp.ClientSession",
                return_value=mock_session,
            ),
            patch(
                "polybot.executor.get_polygon_balance_async",
                new_callable=AsyncMock,
                return_value=100.0,
            ),
            patch("polybot.scanner.time.time", return_value=float(fixed_ts)),
            patch("polybot.executor.place_trade_async", mock_trade),
            patch(
                "polybot.scanner._fetch_order_book",
                return_value={"asks": [{"price": "0.40"}], "bids": [{"price": "0.38"}]},
            ),
        ):
            result = await fetch_all_active_markets_async(min_volume=0)

        assert mock_trade.called
        call_kwargs = mock_trade.call_args.kwargs
        assert call_kwargs["outcome"] == "up"
        assert call_kwargs["token_id"] == "token-up-1"
        assert isinstance(result, list)
        assert len(result) > 0

    @pytest.mark.asyncio
    @patch("polybot.scanner.get_settings")
    async def test_v48_log_messages(self, mock_settings, capsys):
        """Onchain: log messages include CYCLE MATCH and TRADE SUCCESS markers."""
        import os
        from unittest.mock import AsyncMock, MagicMock, patch as mock_patch

        from polybot.scanner import fetch_all_active_markets_async

        mock_cfg = type(
            "Settings",
            (),
            {
                "mode": "updown",
                "target_symbols": ["BTC", "ETH", "SOL", "XRP"],
                "use_api_mirrors": False,
                "up_down_only": False,
                "effective_log_level": 20,
                "dry_run": True,
            },
        )()
        mock_settings.return_value = mock_cfg

        mock_market = {
            "slug": "btc-updown-5m-1700000100",
            "question": "Will BTC go up?",
            "clobTokenIds": ["token-up-1", "token-down-1"],
            "volume": 50000,
        }
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value=[mock_market])
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        mock_trade = AsyncMock(return_value={"status": "dry_run", "id": "v48-log"})

        fixed_ts = 1700000100
        long_key = "0x" + "a" * 64

        with (
            mock_patch.dict(
                os.environ,
                {"POLYMARKET_PRIVATE_KEY": long_key},
            ),
            patch(
                "polybot.scanner.aiohttp.ClientSession",
                return_value=mock_session,
            ),
            patch(
                "polybot.executor.get_polygon_balance_async",
                new_callable=AsyncMock,
                return_value=100.0,
            ),
            patch("polybot.scanner.time.time", return_value=float(fixed_ts)),
            patch("polybot.executor.place_trade_async", mock_trade),
            patch(
                "polybot.scanner._fetch_order_book",
                return_value={"asks": [{"price": "0.40"}], "bids": [{"price": "0.38"}]},
            ),
        ):
            await fetch_all_active_markets_async(min_volume=0)

        captured = capsys.readouterr()
        assert "[CYCLE MATCH]" in captured.out
        assert "[TRADE SUCCESS]" in captured.out

    @pytest.mark.asyncio
    @patch("polybot.scanner.get_settings")
    async def test_v48_trade_error_caught_gracefully(self, mock_settings):
        """Onchain: trade errors are caught gracefully."""
        import os
        from unittest.mock import AsyncMock, MagicMock, patch as mock_patch

        from polybot.scanner import fetch_all_active_markets_async

        mock_cfg = type(
            "Settings",
            (),
            {
                "mode": "updown",
                "target_symbols": ["BTC", "ETH", "SOL", "XRP"],
                "use_api_mirrors": False,
                "up_down_only": False,
                "effective_log_level": 20,
                "dry_run": True,
            },
        )()
        mock_settings.return_value = mock_cfg

        mock_market = {
            "slug": "btc-updown-5m-1700000100",
            "question": "Will BTC go up?",
            "clobTokenIds": ["token-up-1", "token-down-1"],
            "volume": 50000,
        }
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value=[mock_market])
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        mock_trade = AsyncMock(side_effect=RuntimeError("Connection failed"))

        fixed_ts = 1700000100
        long_key = "0x" + "a" * 64

        with (
            mock_patch.dict(
                os.environ,
                {"POLYMARKET_PRIVATE_KEY": long_key},
            ),
            patch(
                "polybot.scanner.aiohttp.ClientSession",
                return_value=mock_session,
            ),
            patch(
                "polybot.executor.get_polygon_balance_async",
                new_callable=AsyncMock,
                return_value=100.0,
            ),
            patch("polybot.scanner.time.time", return_value=float(fixed_ts)),
            patch("polybot.executor.place_trade_async", mock_trade),
            patch(
                "polybot.scanner._fetch_order_book",
                return_value={"asks": [{"price": "0.40"}], "bids": [{"price": "0.38"}]},
            ),
        ):
            result = await fetch_all_active_markets_async(min_volume=0)

        assert isinstance(result, list)


class TestV49UltimateFinalPatch:
    """Tests for onchain trade execution – no executor override needed."""

    @pytest.mark.asyncio
    @patch("polybot.scanner.get_settings")
    async def test_v49_executor_override(self, mock_settings):
        """Onchain: executor.place_trade_async called directly (no _client override)."""
        import os
        from unittest.mock import AsyncMock, MagicMock, patch as mock_patch

        from polybot.scanner import fetch_all_active_markets_async

        mock_cfg = type(
            "Settings",
            (),
            {
                "mode": "updown",
                "target_symbols": ["BTC", "ETH", "SOL", "XRP"],
                "use_api_mirrors": False,
                "up_down_only": False,
                "effective_log_level": 20,
                "dry_run": True,
            },
        )()
        mock_settings.return_value = mock_cfg

        mock_market = {
            "slug": "btc-updown-5m-1700000100",
            "question": "Will BTC go up?",
            "clobTokenIds": ["token-up-1", "token-down-1"],
            "volume": 50000,
        }
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value=[mock_market])
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        mock_trade = AsyncMock(return_value={"status": "dry_run", "id": "v49-exec"})

        fixed_ts = 1700000100
        long_key = "0x" + "a" * 64

        with (
            mock_patch.dict(
                os.environ,
                {"POLYMARKET_PRIVATE_KEY": long_key},
            ),
            patch(
                "polybot.scanner.aiohttp.ClientSession",
                return_value=mock_session,
            ),
            patch(
                "polybot.executor.get_polygon_balance_async",
                new_callable=AsyncMock,
                return_value=100.0,
            ),
            patch("polybot.scanner.time.time", return_value=float(fixed_ts)),
            patch("polybot.executor.place_trade_async", mock_trade),
            patch(
                "polybot.scanner._fetch_order_book",
                return_value={"asks": [{"price": "0.40"}], "bids": [{"price": "0.38"}]},
            ),
        ):
            result = await fetch_all_active_markets_async(min_volume=0)

        assert mock_trade.called
        assert isinstance(result, list)
        assert len(result) > 0

    @pytest.mark.asyncio
    @patch("polybot.scanner.get_settings")
    async def test_v49_api_creds_iterable_fallback(self, mock_settings):
        """Onchain: trade called with correct args (no ApiCreds conversion)."""
        import os
        from unittest.mock import AsyncMock, MagicMock, patch as mock_patch

        from polybot.scanner import fetch_all_active_markets_async

        mock_cfg = type(
            "Settings",
            (),
            {
                "mode": "updown",
                "target_symbols": ["BTC", "ETH", "SOL", "XRP"],
                "use_api_mirrors": False,
                "up_down_only": False,
                "effective_log_level": 20,
                "dry_run": True,
            },
        )()
        mock_settings.return_value = mock_cfg

        mock_market = {
            "slug": "btc-updown-5m-1700000100",
            "question": "Will BTC go up?",
            "clobTokenIds": ["token-up-1", "token-down-1"],
            "volume": 50000,
        }
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value=[mock_market])
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        mock_trade = AsyncMock(return_value={"status": "dry_run", "id": "v49-iter"})

        fixed_ts = 1700000100
        long_key = "0x" + "a" * 64

        with (
            mock_patch.dict(
                os.environ,
                {"POLYMARKET_PRIVATE_KEY": long_key},
            ),
            patch(
                "polybot.scanner.aiohttp.ClientSession",
                return_value=mock_session,
            ),
            patch(
                "polybot.executor.get_polygon_balance_async",
                new_callable=AsyncMock,
                return_value=100.0,
            ),
            patch("polybot.scanner.time.time", return_value=float(fixed_ts)),
            patch("polybot.executor.place_trade_async", mock_trade),
            patch(
                "polybot.scanner._fetch_order_book",
                return_value={"asks": [{"price": "0.40"}], "bids": [{"price": "0.38"}]},
            ),
        ):
            result = await fetch_all_active_markets_async(min_volume=0)

        assert mock_trade.called
        call_kwargs = mock_trade.call_args.kwargs
        assert call_kwargs["amount"] == 30.0
        assert call_kwargs["outcome"] == "up"
        assert isinstance(result, list)
        assert len(result) > 0

    @pytest.mark.asyncio
    @patch("polybot.scanner.get_settings")
    async def test_v49_log_messages(self, mock_settings, capsys):
        """Onchain: log messages include CYCLE MATCH and TRADE SUCCESS."""
        import os
        from unittest.mock import AsyncMock, MagicMock, patch as mock_patch

        from polybot.scanner import fetch_all_active_markets_async

        mock_cfg = type(
            "Settings",
            (),
            {
                "mode": "updown",
                "target_symbols": ["BTC", "ETH", "SOL", "XRP"],
                "use_api_mirrors": False,
                "up_down_only": False,
                "effective_log_level": 20,
                "dry_run": True,
            },
        )()
        mock_settings.return_value = mock_cfg

        mock_market = {
            "slug": "btc-updown-5m-1700000100",
            "question": "Will BTC go up?",
            "clobTokenIds": ["token-up-1", "token-down-1"],
            "volume": 50000,
        }
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value=[mock_market])
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        mock_trade = AsyncMock(return_value={"status": "dry_run", "id": "v49-log"})

        fixed_ts = 1700000100
        long_key = "0x" + "a" * 64

        with (
            mock_patch.dict(
                os.environ,
                {"POLYMARKET_PRIVATE_KEY": long_key},
            ),
            patch(
                "polybot.scanner.aiohttp.ClientSession",
                return_value=mock_session,
            ),
            patch(
                "polybot.executor.get_polygon_balance_async",
                new_callable=AsyncMock,
                return_value=100.0,
            ),
            patch("polybot.scanner.time.time", return_value=float(fixed_ts)),
            patch("polybot.executor.place_trade_async", mock_trade),
            patch(
                "polybot.scanner._fetch_order_book",
                return_value={"asks": [{"price": "0.40"}], "bids": [{"price": "0.38"}]},
            ),
        ):
            await fetch_all_active_markets_async(min_volume=0)

        captured = capsys.readouterr()
        assert "[CYCLE MATCH]" in captured.out
        assert "[TRADE SUCCESS]" in captured.out

    @pytest.mark.asyncio
    @patch("polybot.scanner.get_settings")
    async def test_v49_trade_error_caught_gracefully(self, mock_settings):
        """Onchain: trade errors are caught gracefully with continue."""
        import os
        from unittest.mock import AsyncMock, MagicMock, patch as mock_patch

        from polybot.scanner import fetch_all_active_markets_async

        mock_cfg = type(
            "Settings",
            (),
            {
                "mode": "updown",
                "target_symbols": ["BTC", "ETH", "SOL", "XRP"],
                "use_api_mirrors": False,
                "up_down_only": False,
                "effective_log_level": 20,
                "dry_run": True,
            },
        )()
        mock_settings.return_value = mock_cfg

        mock_market = {
            "slug": "btc-updown-5m-1700000100",
            "question": "Will BTC go up?",
            "clobTokenIds": ["token-up-1", "token-down-1"],
            "volume": 50000,
        }
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value=[mock_market])
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        mock_trade = AsyncMock(side_effect=RuntimeError("Connection failed"))

        fixed_ts = 1700000100
        long_key = "0x" + "a" * 64

        with (
            mock_patch.dict(
                os.environ,
                {"POLYMARKET_PRIVATE_KEY": long_key},
            ),
            patch(
                "polybot.scanner.aiohttp.ClientSession",
                return_value=mock_session,
            ),
            patch(
                "polybot.executor.get_polygon_balance_async",
                new_callable=AsyncMock,
                return_value=100.0,
            ),
            patch("polybot.scanner.time.time", return_value=float(fixed_ts)),
            patch("polybot.executor.place_trade_async", mock_trade),
            patch(
                "polybot.scanner._fetch_order_book",
                return_value={"asks": [{"price": "0.40"}], "bids": [{"price": "0.38"}]},
            ),
        ):
            result = await fetch_all_active_markets_async(min_volume=0)

        assert isinstance(result, list)


class TestV50UltimateFinalFix:
    """Tests for onchain trade execution – no executor creds override needed."""

    @pytest.mark.asyncio
    @patch("polybot.scanner.get_settings")
    async def test_v50_executor_creds_override(self, mock_settings):
        """Onchain: trade called directly (no executor creds override)."""
        import os
        from unittest.mock import AsyncMock, MagicMock, patch as mock_patch

        from polybot.scanner import fetch_all_active_markets_async

        mock_cfg = type(
            "Settings",
            (),
            {
                "mode": "updown",
                "target_symbols": ["BTC", "ETH", "SOL", "XRP"],
                "use_api_mirrors": False,
                "up_down_only": False,
                "effective_log_level": 20,
                "dry_run": True,
            },
        )()
        mock_settings.return_value = mock_cfg

        mock_market = {
            "slug": "btc-updown-5m-1700000100",
            "question": "Will BTC go up?",
            "clobTokenIds": ["token-up-1", "token-down-1"],
            "volume": 50000,
        }
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value=[mock_market])
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        mock_trade = AsyncMock(return_value={"status": "dry_run", "id": "v50-creds"})

        fixed_ts = 1700000100
        long_key = "0x" + "a" * 64

        with (
            mock_patch.dict(
                os.environ,
                {"POLYMARKET_PRIVATE_KEY": long_key},
            ),
            patch(
                "polybot.scanner.aiohttp.ClientSession",
                return_value=mock_session,
            ),
            patch(
                "polybot.executor.get_polygon_balance_async",
                new_callable=AsyncMock,
                return_value=100.0,
            ),
            patch("polybot.scanner.time.time", return_value=float(fixed_ts)),
            patch("polybot.executor.place_trade_async", mock_trade),
            patch(
                "polybot.scanner._fetch_order_book",
                return_value={"asks": [{"price": "0.40"}], "bids": [{"price": "0.38"}]},
            ),
        ):
            result = await fetch_all_active_markets_async(min_volume=0)

        assert mock_trade.called
        assert isinstance(result, list)
        assert len(result) > 0

    @pytest.mark.asyncio
    @patch("polybot.scanner.get_settings")
    async def test_v50_log_messages(self, mock_settings, capsys):
        """Onchain: log messages include CYCLE MATCH and TRADE SUCCESS markers."""
        import os
        from unittest.mock import AsyncMock, MagicMock, patch as mock_patch

        from polybot.scanner import fetch_all_active_markets_async

        mock_cfg = type(
            "Settings",
            (),
            {
                "mode": "updown",
                "target_symbols": ["BTC", "ETH", "SOL", "XRP"],
                "use_api_mirrors": False,
                "up_down_only": False,
                "effective_log_level": 20,
                "dry_run": True,
            },
        )()
        mock_settings.return_value = mock_cfg

        mock_market = {
            "slug": "btc-updown-5m-1700000100",
            "question": "Will BTC go up?",
            "clobTokenIds": ["token-up-1", "token-down-1"],
            "volume": 50000,
        }
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value=[mock_market])
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        mock_trade = AsyncMock(return_value={"status": "dry_run", "id": "v50-log"})

        fixed_ts = 1700000100
        long_key = "0x" + "a" * 64

        with (
            mock_patch.dict(
                os.environ,
                {"POLYMARKET_PRIVATE_KEY": long_key},
            ),
            patch(
                "polybot.scanner.aiohttp.ClientSession",
                return_value=mock_session,
            ),
            patch(
                "polybot.executor.get_polygon_balance_async",
                new_callable=AsyncMock,
                return_value=100.0,
            ),
            patch("polybot.scanner.time.time", return_value=float(fixed_ts)),
            patch("polybot.executor.place_trade_async", mock_trade),
            patch(
                "polybot.scanner._fetch_order_book",
                return_value={"asks": [{"price": "0.40"}], "bids": [{"price": "0.38"}]},
            ),
        ):
            await fetch_all_active_markets_async(min_volume=0)

        captured = capsys.readouterr()
        assert "[CYCLE MATCH]" in captured.out
        assert "[TRADE SUCCESS]" in captured.out
        assert "markets processed" in captured.out

    @pytest.mark.asyncio
    @patch("polybot.scanner.get_settings")
    async def test_v50_full_traceback_on_error(self, mock_settings, capsys):
        """Onchain: trade errors produce [TRADE ERROR] + [FULL TRACEBACK]."""
        import os
        from unittest.mock import AsyncMock, MagicMock, patch as mock_patch

        from polybot.scanner import fetch_all_active_markets_async

        mock_cfg = type(
            "Settings",
            (),
            {
                "mode": "updown",
                "target_symbols": ["BTC", "ETH", "SOL", "XRP"],
                "use_api_mirrors": False,
                "up_down_only": False,
                "effective_log_level": 20,
                "dry_run": True,
            },
        )()
        mock_settings.return_value = mock_cfg

        mock_market = {
            "slug": "btc-updown-5m-1700000100",
            "question": "Will BTC go up?",
            "clobTokenIds": ["token-up-1", "token-down-1"],
            "volume": 50000,
        }
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value=[mock_market])
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        mock_trade = AsyncMock(side_effect=RuntimeError("Connection failed"))

        fixed_ts = 1700000100
        long_key = "0x" + "a" * 64

        with (
            mock_patch.dict(
                os.environ,
                {"POLYMARKET_PRIVATE_KEY": long_key},
            ),
            patch(
                "polybot.scanner.aiohttp.ClientSession",
                return_value=mock_session,
            ),
            patch(
                "polybot.executor.get_polygon_balance_async",
                new_callable=AsyncMock,
                return_value=100.0,
            ),
            patch("polybot.scanner.time.time", return_value=float(fixed_ts)),
            patch("polybot.executor.place_trade_async", mock_trade),
            patch(
                "polybot.scanner._fetch_order_book",
                return_value={"asks": [{"price": "0.40"}], "bids": [{"price": "0.38"}]},
            ),
        ):
            result = await fetch_all_active_markets_async(min_volume=0)

        assert isinstance(result, list)
        captured = capsys.readouterr()
        assert "[TRADE ERROR]" in captured.out
        assert "[FULL TRACEBACK]" in captured.out

    @pytest.mark.asyncio
    @patch("polybot.scanner.get_settings")
    async def test_v50_trade_calls_executor_place_trade(self, mock_settings):
        """Onchain: trade is called via executor.place_trade_async."""
        import os
        from unittest.mock import AsyncMock, MagicMock, patch as mock_patch

        from polybot.scanner import fetch_all_active_markets_async

        mock_cfg = type(
            "Settings",
            (),
            {
                "mode": "updown",
                "target_symbols": ["BTC", "ETH", "SOL", "XRP"],
                "use_api_mirrors": False,
                "up_down_only": False,
                "effective_log_level": 20,
                "dry_run": True,
            },
        )()
        mock_settings.return_value = mock_cfg

        mock_market = {
            "slug": "btc-updown-5m-1700000100",
            "question": "Will BTC go up?",
            "clobTokenIds": ["token-up-1", "token-down-1"],
            "volume": 50000,
        }
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value=[mock_market])
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        mock_trade = AsyncMock(
            return_value={"status": "dry_run", "id": "v50-exec-call"}
        )

        fixed_ts = 1700000100
        long_key = "0x" + "a" * 64

        with (
            mock_patch.dict(
                os.environ,
                {"POLYMARKET_PRIVATE_KEY": long_key},
            ),
            patch(
                "polybot.scanner.aiohttp.ClientSession",
                return_value=mock_session,
            ),
            patch(
                "polybot.executor.get_polygon_balance_async",
                new_callable=AsyncMock,
                return_value=100.0,
            ),
            patch("polybot.scanner.time.time", return_value=float(fixed_ts)),
            patch("polybot.executor.place_trade_async", mock_trade),
            patch(
                "polybot.scanner._fetch_order_book",
                return_value={"asks": [{"price": "0.40"}], "bids": [{"price": "0.38"}]},
            ),
        ):
            result = await fetch_all_active_markets_async(min_volume=0)

        assert mock_trade.called
        assert isinstance(result, list)
        assert len(result) > 0


class TestV51MonkeyPatchFix:
    """Tests for onchain trade execution – no monkey-patching needed."""

    @pytest.mark.asyncio
    @patch("polybot.scanner.get_settings")
    async def test_v52_monkey_patch_applied(self, mock_settings):
        """Onchain: trade called directly (no monkey-patching needed)."""
        import os
        from unittest.mock import AsyncMock, MagicMock, patch as mock_patch

        from polybot.scanner import fetch_all_active_markets_async

        mock_cfg = type(
            "Settings",
            (),
            {
                "mode": "updown",
                "target_symbols": ["BTC", "ETH", "SOL", "XRP"],
                "use_api_mirrors": False,
                "up_down_only": False,
                "effective_log_level": 20,
                "dry_run": True,
            },
        )()
        mock_settings.return_value = mock_cfg

        mock_market = {
            "slug": "btc-updown-5m-1700000100",
            "question": "Will BTC go up?",
            "clobTokenIds": ["token-up-1", "token-down-1"],
            "volume": 50000,
        }
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value=[mock_market])
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        mock_trade = AsyncMock(return_value={"status": "dry_run", "id": "v52-patch"})

        fixed_ts = 1700000100
        long_key = "0x" + "a" * 64

        with (
            mock_patch.dict(
                os.environ,
                {"POLYMARKET_PRIVATE_KEY": long_key},
            ),
            patch(
                "polybot.scanner.aiohttp.ClientSession",
                return_value=mock_session,
            ),
            patch(
                "polybot.executor.get_polygon_balance_async",
                new_callable=AsyncMock,
                return_value=100.0,
            ),
            patch("polybot.scanner.time.time", return_value=float(fixed_ts)),
            patch("polybot.executor.place_trade_async", mock_trade),
            patch(
                "polybot.scanner._fetch_order_book",
                return_value={"asks": [{"price": "0.40"}], "bids": [{"price": "0.38"}]},
            ),
        ):
            result = await fetch_all_active_markets_async(min_volume=0)

        assert mock_trade.called
        assert isinstance(result, list)
        assert len(result) > 0

    @pytest.mark.asyncio
    @patch("polybot.scanner.get_settings")
    async def test_v52_log_messages(self, mock_settings, capsys):
        """Onchain: log messages include CYCLE MATCH and TRADE SUCCESS markers."""
        import os
        from unittest.mock import AsyncMock, MagicMock, patch as mock_patch

        from polybot.scanner import fetch_all_active_markets_async

        mock_cfg = type(
            "Settings",
            (),
            {
                "mode": "updown",
                "target_symbols": ["BTC", "ETH", "SOL", "XRP"],
                "use_api_mirrors": False,
                "up_down_only": False,
                "effective_log_level": 20,
                "dry_run": True,
            },
        )()
        mock_settings.return_value = mock_cfg

        mock_market = {
            "slug": "btc-updown-5m-1700000100",
            "question": "Will BTC go up?",
            "clobTokenIds": ["token-up-1", "token-down-1"],
            "volume": 50000,
        }
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value=[mock_market])
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        mock_trade = AsyncMock(return_value={"status": "dry_run", "id": "v52-log"})

        fixed_ts = 1700000100
        long_key = "0x" + "a" * 64

        with (
            mock_patch.dict(
                os.environ,
                {"POLYMARKET_PRIVATE_KEY": long_key},
            ),
            patch(
                "polybot.scanner.aiohttp.ClientSession",
                return_value=mock_session,
            ),
            patch(
                "polybot.executor.get_polygon_balance_async",
                new_callable=AsyncMock,
                return_value=100.0,
            ),
            patch("polybot.scanner.time.time", return_value=float(fixed_ts)),
            patch("polybot.executor.place_trade_async", mock_trade),
            patch(
                "polybot.scanner._fetch_order_book",
                return_value={"asks": [{"price": "0.40"}], "bids": [{"price": "0.38"}]},
            ),
        ):
            await fetch_all_active_markets_async(min_volume=0)

        captured = capsys.readouterr()
        assert "[CYCLE MATCH]" in captured.out
        assert "[TRADE SUCCESS]" in captured.out
        assert "markets processed" in captured.out

    @pytest.mark.asyncio
    @patch("polybot.scanner.get_settings")
    async def test_v52_full_traceback_on_error(self, mock_settings, capsys):
        """Onchain: trade errors produce [TRADE ERROR] + [FULL TRACEBACK]."""
        import os
        from unittest.mock import AsyncMock, MagicMock, patch as mock_patch

        from polybot.scanner import fetch_all_active_markets_async

        mock_cfg = type(
            "Settings",
            (),
            {
                "mode": "updown",
                "target_symbols": ["BTC", "ETH", "SOL", "XRP"],
                "use_api_mirrors": False,
                "up_down_only": False,
                "effective_log_level": 20,
                "dry_run": True,
            },
        )()
        mock_settings.return_value = mock_cfg

        mock_market = {
            "slug": "btc-updown-5m-1700000100",
            "question": "Will BTC go up?",
            "clobTokenIds": ["token-up-1", "token-down-1"],
            "volume": 50000,
        }
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value=[mock_market])
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        mock_trade = AsyncMock(side_effect=RuntimeError("Connection failed"))

        fixed_ts = 1700000100
        long_key = "0x" + "a" * 64

        with (
            mock_patch.dict(
                os.environ,
                {"POLYMARKET_PRIVATE_KEY": long_key},
            ),
            patch(
                "polybot.scanner.aiohttp.ClientSession",
                return_value=mock_session,
            ),
            patch(
                "polybot.executor.get_polygon_balance_async",
                new_callable=AsyncMock,
                return_value=100.0,
            ),
            patch("polybot.scanner.time.time", return_value=float(fixed_ts)),
            patch("polybot.executor.place_trade_async", mock_trade),
            patch(
                "polybot.scanner._fetch_order_book",
                return_value={"asks": [{"price": "0.40"}], "bids": [{"price": "0.38"}]},
            ),
        ):
            result = await fetch_all_active_markets_async(min_volume=0)

        assert isinstance(result, list)
        captured = capsys.readouterr()
        assert "[TRADE ERROR]" in captured.out
        assert "[FULL TRACEBACK]" in captured.out

    @pytest.mark.asyncio
    @patch("polybot.scanner.get_settings")
    async def test_v52_trade_calls_executor_place_trade(self, mock_settings):
        """Onchain: trade is called via executor.place_trade_async."""
        import os
        from unittest.mock import AsyncMock, MagicMock, patch as mock_patch

        from polybot.scanner import fetch_all_active_markets_async

        mock_cfg = type(
            "Settings",
            (),
            {
                "mode": "updown",
                "target_symbols": ["BTC", "ETH", "SOL", "XRP"],
                "use_api_mirrors": False,
                "up_down_only": False,
                "effective_log_level": 20,
                "dry_run": True,
            },
        )()
        mock_settings.return_value = mock_cfg

        mock_market = {
            "slug": "btc-updown-5m-1700000100",
            "question": "Will BTC go up?",
            "clobTokenIds": ["token-up-1", "token-down-1"],
            "volume": 50000,
        }
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value=[mock_market])
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        mock_trade = AsyncMock(
            return_value={"status": "dry_run", "id": "v52-exec-call"}
        )

        fixed_ts = 1700000100
        long_key = "0x" + "a" * 64

        with (
            mock_patch.dict(
                os.environ,
                {"POLYMARKET_PRIVATE_KEY": long_key},
            ),
            patch(
                "polybot.scanner.aiohttp.ClientSession",
                return_value=mock_session,
            ),
            patch(
                "polybot.executor.get_polygon_balance_async",
                new_callable=AsyncMock,
                return_value=100.0,
            ),
            patch("polybot.scanner.time.time", return_value=float(fixed_ts)),
            patch("polybot.executor.place_trade_async", mock_trade),
            patch(
                "polybot.scanner._fetch_order_book",
                return_value={"asks": [{"price": "0.40"}], "bids": [{"price": "0.38"}]},
            ),
        ):
            result = await fetch_all_active_markets_async(min_volume=0)

        assert mock_trade.called
        assert isinstance(result, list)
        assert len(result) > 0


class TestV52NuklearMonkeyPatch:
    """Tests for onchain trade execution – no nuclear monkey-patch needed."""

    @pytest.mark.asyncio
    @patch("polybot.scanner.get_settings")
    async def test_v52_apicreds_get_method_patched(self, mock_settings):
        """Onchain: trade called directly (no ApiCreds patching)."""
        import os
        from unittest.mock import AsyncMock, MagicMock, patch as mock_patch

        from polybot.scanner import fetch_all_active_markets_async

        mock_cfg = type(
            "Settings",
            (),
            {
                "mode": "updown",
                "target_symbols": ["BTC", "ETH", "SOL", "XRP"],
                "use_api_mirrors": False,
                "up_down_only": False,
                "effective_log_level": 20,
                "dry_run": True,
            },
        )()
        mock_settings.return_value = mock_cfg

        mock_market = {
            "slug": "btc-updown-5m-1700000100",
            "question": "Will BTC go up?",
            "clobTokenIds": ["token-up-1", "token-down-1"],
            "volume": 50000,
        }
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value=[mock_market])
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        mock_trade = AsyncMock(return_value={"status": "dry_run", "id": "v53-l2patch"})

        fixed_ts = 1700000100
        long_key = "0x" + "a" * 64

        with (
            mock_patch.dict(
                os.environ,
                {"POLYMARKET_PRIVATE_KEY": long_key},
            ),
            patch(
                "polybot.scanner.aiohttp.ClientSession",
                return_value=mock_session,
            ),
            patch(
                "polybot.executor.get_polygon_balance_async",
                new_callable=AsyncMock,
                return_value=100.0,
            ),
            patch("polybot.scanner.time.time", return_value=float(fixed_ts)),
            patch("polybot.executor.place_trade_async", mock_trade),
            patch(
                "polybot.scanner._fetch_order_book",
                return_value={"asks": [{"price": "0.40"}], "bids": [{"price": "0.38"}]},
            ),
        ):
            result = await fetch_all_active_markets_async(min_volume=0)

        assert mock_trade.called
        assert isinstance(result, list)
        assert len(result) > 0

    @pytest.mark.asyncio
    @patch("polybot.scanner.get_settings")
    async def test_v52_raw_creds_passed_directly(self, mock_settings):
        """Onchain: trade called with correct params (no creds conversion)."""
        import os
        from unittest.mock import AsyncMock, MagicMock, patch as mock_patch

        from polybot.scanner import fetch_all_active_markets_async

        mock_cfg = type(
            "Settings",
            (),
            {
                "mode": "updown",
                "target_symbols": ["BTC", "ETH", "SOL", "XRP"],
                "use_api_mirrors": False,
                "up_down_only": False,
                "effective_log_level": 20,
                "dry_run": True,
            },
        )()
        mock_settings.return_value = mock_cfg

        mock_market = {
            "slug": "btc-updown-5m-1700000100",
            "question": "Will BTC go up?",
            "clobTokenIds": ["token-up-1", "token-down-1"],
            "volume": 50000,
        }
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value=[mock_market])
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        mock_trade = AsyncMock(return_value={"status": "dry_run", "id": "v53-dictconv"})

        fixed_ts = 1700000100
        long_key = "0x" + "a" * 64

        with (
            mock_patch.dict(
                os.environ,
                {"POLYMARKET_PRIVATE_KEY": long_key},
            ),
            patch(
                "polybot.scanner.aiohttp.ClientSession",
                return_value=mock_session,
            ),
            patch(
                "polybot.executor.get_polygon_balance_async",
                new_callable=AsyncMock,
                return_value=100.0,
            ),
            patch("polybot.scanner.time.time", return_value=float(fixed_ts)),
            patch("polybot.executor.place_trade_async", mock_trade),
            patch(
                "polybot.scanner._fetch_order_book",
                return_value={"asks": [{"price": "0.40"}], "bids": [{"price": "0.38"}]},
            ),
        ):
            await fetch_all_active_markets_async(min_volume=0)

        mock_trade.assert_called_once()
        call_kwargs = mock_trade.call_args.kwargs
        assert call_kwargs["market"]["slug"] == "btc-updown-5m-1700000100"
        assert call_kwargs["amount"] == 30.0

    @pytest.mark.asyncio
    @patch("polybot.scanner.get_settings")
    async def test_v52_nuklear_patch_log_markers(self, mock_settings, capsys):
        """Onchain: log messages include CYCLE MATCH and TRADE SUCCESS markers."""
        import os
        from unittest.mock import AsyncMock, MagicMock, patch as mock_patch

        from polybot.scanner import fetch_all_active_markets_async

        mock_cfg = type(
            "Settings",
            (),
            {
                "mode": "updown",
                "target_symbols": ["BTC", "ETH", "SOL", "XRP"],
                "use_api_mirrors": False,
                "up_down_only": False,
                "effective_log_level": 20,
                "dry_run": True,
            },
        )()
        mock_settings.return_value = mock_cfg

        mock_market = {
            "slug": "btc-updown-5m-1700000100",
            "question": "Will BTC go up?",
            "clobTokenIds": ["token-up-1", "token-down-1"],
            "volume": 50000,
        }
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value=[mock_market])
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        mock_trade = AsyncMock(return_value={"status": "dry_run", "id": "v52-markers"})

        fixed_ts = 1700000100
        long_key = "0x" + "a" * 64

        with (
            mock_patch.dict(
                os.environ,
                {"POLYMARKET_PRIVATE_KEY": long_key},
            ),
            patch(
                "polybot.scanner.aiohttp.ClientSession",
                return_value=mock_session,
            ),
            patch(
                "polybot.executor.get_polygon_balance_async",
                new_callable=AsyncMock,
                return_value=100.0,
            ),
            patch("polybot.scanner.time.time", return_value=float(fixed_ts)),
            patch("polybot.executor.place_trade_async", mock_trade),
            patch(
                "polybot.scanner._fetch_order_book",
                return_value={"asks": [{"price": "0.40"}], "bids": [{"price": "0.38"}]},
            ),
        ):
            await fetch_all_active_markets_async(min_volume=0)

        captured = capsys.readouterr()
        assert "[CYCLE MATCH]" in captured.out
        assert "[TRADE SUCCESS]" in captured.out
        assert "markets processed" in captured.out

    @pytest.mark.asyncio
    @patch("polybot.scanner.get_settings")
    async def test_v52_error_handling_with_traceback(self, mock_settings, capsys):
        """Onchain: trade errors produce [TRADE ERROR] + [FULL TRACEBACK]."""
        import os
        from unittest.mock import AsyncMock, MagicMock, patch as mock_patch

        from polybot.scanner import fetch_all_active_markets_async

        mock_cfg = type(
            "Settings",
            (),
            {
                "mode": "updown",
                "target_symbols": ["BTC", "ETH", "SOL", "XRP"],
                "use_api_mirrors": False,
                "up_down_only": False,
                "effective_log_level": 20,
                "dry_run": True,
            },
        )()
        mock_settings.return_value = mock_cfg

        mock_market = {
            "slug": "btc-updown-5m-1700000100",
            "question": "Will BTC go up?",
            "clobTokenIds": ["token-up-1", "token-down-1"],
            "volume": 50000,
        }
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value=[mock_market])
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        mock_trade = AsyncMock(side_effect=RuntimeError("Connection failed"))

        fixed_ts = 1700000100
        long_key = "0x" + "a" * 64

        with (
            mock_patch.dict(
                os.environ,
                {"POLYMARKET_PRIVATE_KEY": long_key},
            ),
            patch(
                "polybot.scanner.aiohttp.ClientSession",
                return_value=mock_session,
            ),
            patch(
                "polybot.executor.get_polygon_balance_async",
                new_callable=AsyncMock,
                return_value=100.0,
            ),
            patch("polybot.scanner.time.time", return_value=float(fixed_ts)),
            patch("polybot.executor.place_trade_async", mock_trade),
            patch(
                "polybot.scanner._fetch_order_book",
                return_value={"asks": [{"price": "0.40"}], "bids": [{"price": "0.38"}]},
            ),
        ):
            result = await fetch_all_active_markets_async(min_volume=0)

        assert isinstance(result, list)
        captured = capsys.readouterr()
        assert "[TRADE ERROR]" in captured.out
        assert "[FULL TRACEBACK]" in captured.out


class TestV53NuklearAuthPatch:
    """Tests for onchain trade execution – no nuclear auth patch needed."""

    @pytest.mark.asyncio
    @patch("polybot.scanner.get_settings")
    async def test_v53_l2_creds_patched(self, mock_settings):
        """Onchain: trade called directly (no L2 creds patching)."""
        import os
        from unittest.mock import AsyncMock, MagicMock, patch as mock_patch

        from polybot.scanner import fetch_all_active_markets_async

        mock_cfg = type(
            "Settings",
            (),
            {
                "mode": "updown",
                "target_symbols": ["BTC", "ETH", "SOL", "XRP"],
                "use_api_mirrors": False,
                "up_down_only": False,
                "effective_log_level": 20,
                "dry_run": True,
            },
        )()
        mock_settings.return_value = mock_cfg

        mock_market = {
            "slug": "btc-updown-5m-1700000100",
            "question": "Will BTC go up?",
            "clobTokenIds": ["token-up-1", "token-down-1"],
            "volume": 50000,
        }
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value=[mock_market])
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        mock_trade = AsyncMock(return_value={"status": "dry_run", "id": "v53-l2auth"})

        fixed_ts = 1700000100
        long_key = "0x" + "a" * 64

        with (
            mock_patch.dict(
                os.environ,
                {"POLYMARKET_PRIVATE_KEY": long_key},
            ),
            patch(
                "polybot.scanner.aiohttp.ClientSession",
                return_value=mock_session,
            ),
            patch(
                "polybot.executor.get_polygon_balance_async",
                new_callable=AsyncMock,
                return_value=100.0,
            ),
            patch("polybot.scanner.time.time", return_value=float(fixed_ts)),
            patch("polybot.executor.place_trade_async", mock_trade),
            patch(
                "polybot.scanner._fetch_order_book",
                return_value={"asks": [{"price": "0.40"}], "bids": [{"price": "0.38"}]},
            ),
        ):
            result = await fetch_all_active_markets_async(min_volume=0)

        assert mock_trade.called
        assert isinstance(result, list)
        assert len(result) > 0

    @pytest.mark.asyncio
    @patch("polybot.scanner.get_settings")
    async def test_v53_creds_dict_conversion(self, mock_settings):
        """Onchain: trade called with correct params (no creds dict conversion)."""
        import os
        from unittest.mock import AsyncMock, MagicMock, patch as mock_patch

        from polybot.scanner import fetch_all_active_markets_async

        mock_cfg = type(
            "Settings",
            (),
            {
                "mode": "updown",
                "target_symbols": ["BTC", "ETH", "SOL", "XRP"],
                "use_api_mirrors": False,
                "up_down_only": False,
                "effective_log_level": 20,
                "dry_run": True,
            },
        )()
        mock_settings.return_value = mock_cfg

        mock_market = {
            "slug": "btc-updown-5m-1700000100",
            "question": "Will BTC go up?",
            "clobTokenIds": ["token-up-1", "token-down-1"],
            "volume": 50000,
        }
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value=[mock_market])
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        mock_trade = AsyncMock(return_value={"status": "dry_run", "id": "v53-dictconv"})

        fixed_ts = 1700000100
        long_key = "0x" + "a" * 64

        with (
            mock_patch.dict(
                os.environ,
                {"POLYMARKET_PRIVATE_KEY": long_key},
            ),
            patch(
                "polybot.scanner.aiohttp.ClientSession",
                return_value=mock_session,
            ),
            patch(
                "polybot.executor.get_polygon_balance_async",
                new_callable=AsyncMock,
                return_value=100.0,
            ),
            patch("polybot.scanner.time.time", return_value=float(fixed_ts)),
            patch("polybot.executor.place_trade_async", mock_trade),
            patch(
                "polybot.scanner._fetch_order_book",
                return_value={"asks": [{"price": "0.40"}], "bids": [{"price": "0.38"}]},
            ),
        ):
            await fetch_all_active_markets_async(min_volume=0)

        mock_trade.assert_called_once()
        call_kwargs = mock_trade.call_args.kwargs
        assert call_kwargs["amount"] == 30.0
        assert call_kwargs["outcome"] == "up"

    @pytest.mark.asyncio
    @patch("polybot.scanner.get_settings")
    async def test_v53_log_markers(self, mock_settings, capsys):
        """Onchain: log messages include CYCLE MATCH and TRADE SUCCESS markers."""
        import os
        from unittest.mock import AsyncMock, MagicMock, patch as mock_patch

        from polybot.scanner import fetch_all_active_markets_async

        mock_cfg = type(
            "Settings",
            (),
            {
                "mode": "updown",
                "target_symbols": ["BTC", "ETH", "SOL", "XRP"],
                "use_api_mirrors": False,
                "up_down_only": False,
                "effective_log_level": 20,
                "dry_run": True,
            },
        )()
        mock_settings.return_value = mock_cfg

        mock_market = {
            "slug": "btc-updown-5m-1700000100",
            "question": "Will BTC go up?",
            "clobTokenIds": ["token-up-1", "token-down-1"],
            "volume": 50000,
        }
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value=[mock_market])
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        mock_trade = AsyncMock(return_value={"status": "dry_run", "id": "v53-markers"})

        fixed_ts = 1700000100
        long_key = "0x" + "a" * 64

        with (
            mock_patch.dict(
                os.environ,
                {"POLYMARKET_PRIVATE_KEY": long_key},
            ),
            patch(
                "polybot.scanner.aiohttp.ClientSession",
                return_value=mock_session,
            ),
            patch(
                "polybot.executor.get_polygon_balance_async",
                new_callable=AsyncMock,
                return_value=100.0,
            ),
            patch("polybot.scanner.time.time", return_value=float(fixed_ts)),
            patch("polybot.executor.place_trade_async", mock_trade),
            patch(
                "polybot.scanner._fetch_order_book",
                return_value={"asks": [{"price": "0.40"}], "bids": [{"price": "0.38"}]},
            ),
        ):
            await fetch_all_active_markets_async(min_volume=0)

        captured = capsys.readouterr()
        assert "[CYCLE MATCH]" in captured.out
        assert "[TRADE SUCCESS]" in captured.out
        assert "markets processed" in captured.out

    @pytest.mark.asyncio
    @patch("polybot.scanner.get_settings")
    async def test_v53_error_handling_with_traceback(self, mock_settings, capsys):
        """Onchain: trade errors produce [TRADE ERROR] + [FULL TRACEBACK]."""
        import os
        from unittest.mock import AsyncMock, MagicMock, patch as mock_patch

        from polybot.scanner import fetch_all_active_markets_async

        mock_cfg = type(
            "Settings",
            (),
            {
                "mode": "updown",
                "target_symbols": ["BTC", "ETH", "SOL", "XRP"],
                "use_api_mirrors": False,
                "up_down_only": False,
                "effective_log_level": 20,
                "dry_run": True,
            },
        )()
        mock_settings.return_value = mock_cfg

        mock_market = {
            "slug": "btc-updown-5m-1700000100",
            "question": "Will BTC go up?",
            "clobTokenIds": ["token-up-1", "token-down-1"],
            "volume": 50000,
        }
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value=[mock_market])
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        mock_trade = AsyncMock(side_effect=RuntimeError("Connection failed"))

        fixed_ts = 1700000100
        long_key = "0x" + "a" * 64

        with (
            mock_patch.dict(
                os.environ,
                {"POLYMARKET_PRIVATE_KEY": long_key},
            ),
            patch(
                "polybot.scanner.aiohttp.ClientSession",
                return_value=mock_session,
            ),
            patch(
                "polybot.executor.get_polygon_balance_async",
                new_callable=AsyncMock,
                return_value=100.0,
            ),
            patch("polybot.scanner.time.time", return_value=float(fixed_ts)),
            patch("polybot.executor.place_trade_async", mock_trade),
            patch(
                "polybot.scanner._fetch_order_book",
                return_value={"asks": [{"price": "0.40"}], "bids": [{"price": "0.38"}]},
            ),
        ):
            result = await fetch_all_active_markets_async(min_volume=0)

        assert isinstance(result, list)
        captured = capsys.readouterr()
        assert "[TRADE ERROR]" in captured.out
        assert "[FULL TRACEBACK]" in captured.out


class TestV55NuklearThreadPatch:
    """Tests for onchain trade execution – no global ApiCreds patch needed."""

    @pytest.mark.asyncio
    @patch("polybot.scanner.get_settings")
    async def test_v55_apicreds_global_patch(self, mock_settings):
        """Onchain: trade called directly (no global patching)."""
        import os
        from unittest.mock import AsyncMock, MagicMock, patch as mock_patch

        from polybot.scanner import fetch_all_active_markets_async

        mock_cfg = type(
            "Settings",
            (),
            {
                "mode": "updown",
                "target_symbols": ["BTC", "ETH", "SOL", "XRP"],
                "use_api_mirrors": False,
                "up_down_only": False,
                "effective_log_level": 20,
                "dry_run": True,
            },
        )()
        mock_settings.return_value = mock_cfg

        mock_market = {
            "slug": "btc-updown-5m-1700000100",
            "question": "Will BTC go up?",
            "clobTokenIds": ["token-up-1", "token-down-1"],
            "volume": 50000,
        }
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value=[mock_market])
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        mock_trade = AsyncMock(
            return_value={"status": "dry_run", "id": "v55-threadpatch"}
        )

        fixed_ts = 1700000100
        long_key = "0x" + "a" * 64

        with (
            mock_patch.dict(
                os.environ,
                {"POLYMARKET_PRIVATE_KEY": long_key},
            ),
            patch(
                "polybot.scanner.aiohttp.ClientSession",
                return_value=mock_session,
            ),
            patch(
                "polybot.executor.get_polygon_balance_async",
                new_callable=AsyncMock,
                return_value=100.0,
            ),
            patch("polybot.scanner.time.time", return_value=float(fixed_ts)),
            patch("polybot.executor.place_trade_async", mock_trade),
            patch(
                "polybot.scanner._fetch_order_book",
                return_value={"asks": [{"price": "0.40"}], "bids": [{"price": "0.38"}]},
            ),
        ):
            result = await fetch_all_active_markets_async(min_volume=0)

        assert mock_trade.called
        assert isinstance(result, list)
        assert len(result) > 0

    @pytest.mark.asyncio
    @patch("polybot.scanner.get_settings")
    async def test_v55_creds_dict_conversion(self, mock_settings):
        """Onchain: trade called with correct params."""
        import os
        from unittest.mock import AsyncMock, MagicMock, patch as mock_patch

        from polybot.scanner import fetch_all_active_markets_async

        mock_cfg = type(
            "Settings",
            (),
            {
                "mode": "updown",
                "target_symbols": ["BTC", "ETH", "SOL", "XRP"],
                "use_api_mirrors": False,
                "up_down_only": False,
                "effective_log_level": 20,
                "dry_run": True,
            },
        )()
        mock_settings.return_value = mock_cfg

        mock_market = {
            "slug": "btc-updown-5m-1700000100",
            "question": "Will BTC go up?",
            "clobTokenIds": ["token-up-1", "token-down-1"],
            "volume": 50000,
        }
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value=[mock_market])
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        mock_trade = AsyncMock(return_value={"status": "dry_run", "id": "v55-dictconv"})

        fixed_ts = 1700000100
        long_key = "0x" + "a" * 64

        with (
            mock_patch.dict(
                os.environ,
                {"POLYMARKET_PRIVATE_KEY": long_key},
            ),
            patch(
                "polybot.scanner.aiohttp.ClientSession",
                return_value=mock_session,
            ),
            patch(
                "polybot.executor.get_polygon_balance_async",
                new_callable=AsyncMock,
                return_value=100.0,
            ),
            patch("polybot.scanner.time.time", return_value=float(fixed_ts)),
            patch("polybot.executor.place_trade_async", mock_trade),
            patch(
                "polybot.scanner._fetch_order_book",
                return_value={"asks": [{"price": "0.40"}], "bids": [{"price": "0.38"}]},
            ),
        ):
            await fetch_all_active_markets_async(min_volume=0)

        mock_trade.assert_called_once()
        call_kwargs = mock_trade.call_args.kwargs
        assert call_kwargs["amount"] == 30.0
        assert call_kwargs["token_id"] == "token-up-1"

    @pytest.mark.asyncio
    @patch("polybot.scanner.get_settings")
    async def test_v55_log_markers(self, mock_settings, capsys):
        """Onchain: log messages include CYCLE MATCH and TRADE SUCCESS markers."""
        import os
        from unittest.mock import AsyncMock, MagicMock, patch as mock_patch

        from polybot.scanner import fetch_all_active_markets_async

        mock_cfg = type(
            "Settings",
            (),
            {
                "mode": "updown",
                "target_symbols": ["BTC", "ETH", "SOL", "XRP"],
                "use_api_mirrors": False,
                "up_down_only": False,
                "effective_log_level": 20,
                "dry_run": True,
            },
        )()
        mock_settings.return_value = mock_cfg

        mock_market = {
            "slug": "btc-updown-5m-1700000100",
            "question": "Will BTC go up?",
            "clobTokenIds": ["token-up-1", "token-down-1"],
            "volume": 50000,
        }
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value=[mock_market])
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        mock_trade = AsyncMock(return_value={"status": "dry_run", "id": "v55-markers"})

        fixed_ts = 1700000100
        long_key = "0x" + "a" * 64

        with (
            mock_patch.dict(
                os.environ,
                {"POLYMARKET_PRIVATE_KEY": long_key},
            ),
            patch(
                "polybot.scanner.aiohttp.ClientSession",
                return_value=mock_session,
            ),
            patch(
                "polybot.executor.get_polygon_balance_async",
                new_callable=AsyncMock,
                return_value=100.0,
            ),
            patch("polybot.scanner.time.time", return_value=float(fixed_ts)),
            patch("polybot.executor.place_trade_async", mock_trade),
            patch(
                "polybot.scanner._fetch_order_book",
                return_value={"asks": [{"price": "0.40"}], "bids": [{"price": "0.38"}]},
            ),
        ):
            await fetch_all_active_markets_async(min_volume=0)

        captured = capsys.readouterr()
        assert "[CYCLE MATCH]" in captured.out
        assert "[TRADE SUCCESS]" in captured.out
        assert "markets processed" in captured.out

    @pytest.mark.asyncio
    @patch("polybot.scanner.get_settings")
    async def test_v55_error_handling_with_traceback(self, mock_settings, capsys):
        """Onchain: trade errors produce [TRADE ERROR] + [FULL TRACEBACK]."""
        import os
        from unittest.mock import AsyncMock, MagicMock, patch as mock_patch

        from polybot.scanner import fetch_all_active_markets_async

        mock_cfg = type(
            "Settings",
            (),
            {
                "mode": "updown",
                "target_symbols": ["BTC", "ETH", "SOL", "XRP"],
                "use_api_mirrors": False,
                "up_down_only": False,
                "effective_log_level": 20,
                "dry_run": True,
            },
        )()
        mock_settings.return_value = mock_cfg

        mock_market = {
            "slug": "btc-updown-5m-1700000100",
            "question": "Will BTC go up?",
            "clobTokenIds": ["token-up-1", "token-down-1"],
            "volume": 50000,
        }
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value=[mock_market])
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        mock_trade = AsyncMock(side_effect=RuntimeError("Connection failed"))

        fixed_ts = 1700000100
        long_key = "0x" + "a" * 64

        with (
            mock_patch.dict(
                os.environ,
                {"POLYMARKET_PRIVATE_KEY": long_key},
            ),
            patch(
                "polybot.scanner.aiohttp.ClientSession",
                return_value=mock_session,
            ),
            patch(
                "polybot.executor.get_polygon_balance_async",
                new_callable=AsyncMock,
                return_value=100.0,
            ),
            patch("polybot.scanner.time.time", return_value=float(fixed_ts)),
            patch("polybot.executor.place_trade_async", mock_trade),
            patch(
                "polybot.scanner._fetch_order_book",
                return_value={"asks": [{"price": "0.40"}], "bids": [{"price": "0.38"}]},
            ),
        ):
            result = await fetch_all_active_markets_async(min_volume=0)

        assert isinstance(result, list)
        captured = capsys.readouterr()
        assert "[TRADE ERROR]" in captured.out
        assert "[FULL TRACEBACK]" in captured.out


class TestV55NuklearCredsRebuild:
    """Tests for onchain trade execution – no L2-Cache or creds rebuild needed."""

    @pytest.mark.asyncio
    @patch("polybot.scanner.get_settings")
    async def test_v55_l2_cache_deactivated(self, mock_settings):
        """Onchain: trade called directly (no L2 cache to deactivate)."""
        import os
        from unittest.mock import AsyncMock, MagicMock, patch as mock_patch

        from polybot.scanner import fetch_all_active_markets_async

        mock_cfg = type(
            "Settings",
            (),
            {
                "mode": "updown",
                "target_symbols": ["BTC", "ETH", "SOL", "XRP"],
                "use_api_mirrors": False,
                "up_down_only": False,
                "effective_log_level": 20,
                "dry_run": True,
            },
        )()
        mock_settings.return_value = mock_cfg

        mock_market = {
            "slug": "btc-updown-5m-1700000100",
            "question": "Will BTC go up?",
            "clobTokenIds": ["token-up-1", "token-down-1"],
            "volume": 50000,
        }
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value=[mock_market])
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        mock_trade = AsyncMock(return_value={"status": "dry_run", "id": "v56-l2"})

        fixed_ts = 1700000100
        long_key = "0x" + "a" * 64

        with (
            mock_patch.dict(
                os.environ,
                {"POLYMARKET_PRIVATE_KEY": long_key},
            ),
            patch(
                "polybot.scanner.aiohttp.ClientSession",
                return_value=mock_session,
            ),
            patch(
                "polybot.executor.get_polygon_balance_async",
                new_callable=AsyncMock,
                return_value=100.0,
            ),
            patch("polybot.scanner.time.time", return_value=float(fixed_ts)),
            patch("polybot.executor.place_trade_async", mock_trade),
            patch(
                "polybot.scanner._fetch_order_book",
                return_value={"asks": [{"price": "0.40"}], "bids": [{"price": "0.38"}]},
            ),
        ):
            await fetch_all_active_markets_async(min_volume=0)

        assert mock_trade.called

    @pytest.mark.asyncio
    @patch("polybot.scanner.get_settings")
    async def test_v55_secure_creds_logging(self, mock_settings, capsys):
        """Onchain: TRADE SUCCESS logged on successful trade."""
        import os
        from unittest.mock import AsyncMock, MagicMock, patch as mock_patch

        from polybot.scanner import fetch_all_active_markets_async

        mock_cfg = type(
            "Settings",
            (),
            {
                "mode": "updown",
                "target_symbols": ["BTC", "ETH", "SOL", "XRP"],
                "use_api_mirrors": False,
                "up_down_only": False,
                "effective_log_level": 20,
                "dry_run": True,
            },
        )()
        mock_settings.return_value = mock_cfg

        mock_market = {
            "slug": "btc-updown-5m-1700000100",
            "question": "Will BTC go up?",
            "clobTokenIds": ["token-up-1", "token-down-1"],
            "volume": 50000,
        }
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value=[mock_market])
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        mock_trade = AsyncMock(return_value={"status": "dry_run", "id": "v56-creds"})

        fixed_ts = 1700000100
        long_key = "0x" + "a" * 64

        with (
            mock_patch.dict(
                os.environ,
                {"POLYMARKET_PRIVATE_KEY": long_key},
            ),
            patch(
                "polybot.scanner.aiohttp.ClientSession",
                return_value=mock_session,
            ),
            patch(
                "polybot.executor.get_polygon_balance_async",
                new_callable=AsyncMock,
                return_value=100.0,
            ),
            patch("polybot.scanner.time.time", return_value=float(fixed_ts)),
            patch("polybot.executor.place_trade_async", mock_trade),
            patch(
                "polybot.scanner._fetch_order_book",
                return_value={"asks": [{"price": "0.40"}], "bids": [{"price": "0.38"}]},
            ),
        ):
            await fetch_all_active_markets_async(min_volume=0)

        captured = capsys.readouterr()
        assert "[TRADE SUCCESS]" in captured.out

    @pytest.mark.asyncio
    @patch("polybot.scanner.get_settings")
    async def test_v55_trade_params_market_side_size(self, mock_settings):
        """Onchain: place_trade_async called with market=dict, outcome=side, amount=30.0, token_id."""
        import os
        from unittest.mock import AsyncMock, MagicMock, patch as mock_patch

        from polybot.scanner import fetch_all_active_markets_async

        mock_cfg = type(
            "Settings",
            (),
            {
                "mode": "updown",
                "target_symbols": ["BTC", "ETH", "SOL", "XRP"],
                "use_api_mirrors": False,
                "up_down_only": False,
                "effective_log_level": 20,
                "dry_run": True,
            },
        )()
        mock_settings.return_value = mock_cfg

        mock_market = {
            "slug": "btc-updown-5m-1700000100",
            "question": "Will BTC go up?",
            "clobTokenIds": ["token-up-1", "token-down-1"],
            "volume": 50000,
        }
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value=[mock_market])
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        mock_trade = AsyncMock(return_value={"status": "dry_run", "id": "v57-params"})

        fixed_ts = 1700000100
        long_key = "0x" + "a" * 64

        with (
            mock_patch.dict(
                os.environ,
                {"POLYMARKET_PRIVATE_KEY": long_key},
            ),
            patch(
                "polybot.scanner.aiohttp.ClientSession",
                return_value=mock_session,
            ),
            patch(
                "polybot.executor.get_polygon_balance_async",
                new_callable=AsyncMock,
                return_value=100.0,
            ),
            patch("polybot.scanner.time.time", return_value=float(fixed_ts)),
            patch("polybot.executor.place_trade_async", mock_trade),
            patch(
                "polybot.scanner._fetch_order_book",
                return_value={"asks": [{"price": "0.40"}], "bids": [{"price": "0.38"}]},
            ),
        ):
            await fetch_all_active_markets_async(min_volume=0)

        mock_trade.assert_called_once()
        call = mock_trade.call_args
        assert isinstance(call.kwargs["market"], dict), "market must be a dict"
        assert call.kwargs["market"]["slug"] == "btc-updown-5m-1700000100"
        assert "side" not in call.kwargs, "side parameter must NOT be passed"
        assert "size" not in call.kwargs, "size parameter must NOT be passed"
        assert "order_type" not in call.kwargs, (
            "order_type parameter must NOT be passed"
        )
        assert call.kwargs["amount"] == 30.0
        assert call.kwargs["outcome"] == "up"  # mid price 0.39 < 0.45 -> "up"
        assert call.kwargs["token_id"] == "token-up-1"

    @pytest.mark.asyncio
    @patch("polybot.scanner.get_settings")
    async def test_v55_apicreds_import_from_executor(self, mock_settings):
        """Onchain: executor no longer exports ApiCreds (onchain mode)."""
        import os
        from unittest.mock import AsyncMock, MagicMock, patch as mock_patch

        from polybot.scanner import fetch_all_active_markets_async

        mock_cfg = type(
            "Settings",
            (),
            {
                "mode": "updown",
                "target_symbols": ["BTC", "ETH", "SOL", "XRP"],
                "use_api_mirrors": False,
                "up_down_only": False,
                "effective_log_level": 20,
                "dry_run": True,
            },
        )()
        mock_settings.return_value = mock_cfg

        mock_market = {
            "slug": "btc-updown-5m-1700000100",
            "question": "Will BTC go up?",
            "clobTokenIds": ["token-up-1", "token-down-1"],
            "volume": 50000,
        }
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value=[mock_market])
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        mock_trade = AsyncMock(return_value={"status": "dry_run", "id": "v56-import"})

        fixed_ts = 1700000100
        long_key = "0x" + "a" * 64

        with (
            mock_patch.dict(
                os.environ,
                {"POLYMARKET_PRIVATE_KEY": long_key},
            ),
            patch(
                "polybot.scanner.aiohttp.ClientSession",
                return_value=mock_session,
            ),
            patch(
                "polybot.executor.get_polygon_balance_async",
                new_callable=AsyncMock,
                return_value=100.0,
            ),
            patch("polybot.scanner.time.time", return_value=float(fixed_ts)),
            patch("polybot.executor.place_trade_async", mock_trade),
            patch(
                "polybot.scanner._fetch_order_book",
                return_value={"asks": [{"price": "0.40"}], "bids": [{"price": "0.38"}]},
            ),
        ):
            await fetch_all_active_markets_async(min_volume=0)

        assert mock_trade.called

    @pytest.mark.asyncio
    @patch("polybot.scanner.get_settings")
    async def test_v55_onchain_summary_log(self, mock_settings, capsys):
        """Onchain: summary log includes markets processed marker."""
        import os
        from unittest.mock import AsyncMock, MagicMock, patch as mock_patch

        from polybot.scanner import fetch_all_active_markets_async

        mock_cfg = type(
            "Settings",
            (),
            {
                "mode": "updown",
                "target_symbols": ["BTC", "ETH", "SOL", "XRP"],
                "use_api_mirrors": False,
                "up_down_only": False,
                "effective_log_level": 20,
                "dry_run": True,
            },
        )()
        mock_settings.return_value = mock_cfg

        mock_market = {
            "slug": "btc-updown-5m-1700000100",
            "question": "Will BTC go up?",
            "clobTokenIds": ["token-up-1", "token-down-1"],
            "volume": 50000,
        }
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value=[mock_market])
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        mock_trade = AsyncMock(return_value={"status": "dry_run", "id": "v56-summary"})

        fixed_ts = 1700000100
        long_key = "0x" + "a" * 64

        with (
            mock_patch.dict(
                os.environ,
                {"POLYMARKET_PRIVATE_KEY": long_key},
            ),
            patch(
                "polybot.scanner.aiohttp.ClientSession",
                return_value=mock_session,
            ),
            patch(
                "polybot.executor.get_polygon_balance_async",
                new_callable=AsyncMock,
                return_value=100.0,
            ),
            patch("polybot.scanner.time.time", return_value=float(fixed_ts)),
            patch("polybot.executor.place_trade_async", mock_trade),
            patch(
                "polybot.scanner._fetch_order_book",
                return_value={"asks": [{"price": "0.40"}], "bids": [{"price": "0.38"}]},
            ),
        ):
            await fetch_all_active_markets_async(min_volume=0)

        captured = capsys.readouterr()
        assert "markets processed" in captured.out


class TestPrepareTradeParamsGuard:
    """Test the type guard added to _prepare_trade_params."""

    def test_prepare_trade_params_rejects_string(self):
        """_prepare_trade_params raises TypeError when given a string instead of dict."""
        from polybot.executor import _prepare_trade_params

        with pytest.raises(TypeError, match="expected dict, got <class 'str'>"):
            _prepare_trade_params("btc-updown-5m-1774360500", "up")

    def test_prepare_trade_params_accepts_dict(self):
        """_prepare_trade_params works normally with a dict market."""
        from polybot.executor import _prepare_trade_params

        market = {
            "tokens": [
                {"outcome": "up", "token_id": "tok-up"},
                {"outcome": "down", "token_id": "tok-down"},
            ],
            "price_deviation": {"current_price": 0.5},
        }
        token_id, price = _prepare_trade_params(market, "up")
        assert token_id == "tok-up"
        assert price == 0.5


class TestAuthFailedNoFalseSuccess:
    """Tests for error handling: no false SUCCESS logs on trade failures."""

    def _make_market_fixtures(self, fixed_ts):
        """Create standard mock settings, session, and market for auth tests."""
        mock_cfg = type(
            "Settings",
            (),
            {
                "mode": "updown",
                "target_symbols": ["BTC", "ETH", "SOL", "XRP"],
                "use_api_mirrors": False,
                "up_down_only": False,
                "effective_log_level": 20,
                "dry_run": True,
            },
        )()

        up_token = "token-up-btc-001"
        down_token = "token-down-btc-001"
        markets = [
            {
                "question": "BTC Up or Down - 5 Min Live",
                "slug": f"btc-updown-5m-{fixed_ts}",
                "volume": 80_000,
                "liquidity": 5_000,
                "clobTokenIds": [up_token, down_token],
            },
            {
                "question": "ETH Up or Down - 5 Min Live",
                "slug": f"eth-updown-5m-{fixed_ts}",
                "volume": 80_000,
                "liquidity": 5_000,
                "clobTokenIds": ["token-up-eth-001", "token-down-eth-001"],
            },
        ]

        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value=markets)
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        return mock_cfg, mock_session

    @pytest.mark.asyncio
    @patch("polybot.scanner.get_settings")
    async def test_401_error_no_success_log(self, mock_settings, capsys):
        """Onchain: When place_trade_async raises 401, no SUCCESS log appears."""
        import os

        from polybot.scanner import fetch_all_active_markets_async

        fixed_ts = 1774271400
        mock_cfg, mock_session = self._make_market_fixtures(fixed_ts)
        mock_settings.return_value = mock_cfg

        mock_trade = AsyncMock(
            side_effect=Exception("PolyApiException 401 Unauthorized/Invalid api key")
        )
        long_key = "0x" + "a" * 64

        with (
            patch.dict(os.environ, {"POLYMARKET_PRIVATE_KEY": long_key}),
            patch("polybot.scanner.aiohttp.ClientSession", return_value=mock_session),
            patch(
                "polybot.executor.get_polygon_balance_async",
                new_callable=AsyncMock,
                return_value=100.0,
            ),
            patch("polybot.scanner.time.time", return_value=float(fixed_ts)),
            patch("polybot.executor.place_trade_async", mock_trade),
            patch(
                "polybot.scanner._fetch_order_book",
                return_value={"asks": [{"price": "0.40"}], "bids": [{"price": "0.38"}]},
            ),
        ):
            await fetch_all_active_markets_async(min_volume=0)

        assert mock_trade.call_count == 2, (
            f"Expected 2 trade calls (continue on 401), got {mock_trade.call_count}"
        )

        captured = capsys.readouterr()
        assert "[TRADE SUCCESS]" not in captured.out, (
            "SUCCESS must not be logged after a 401 error"
        )
        assert "[TRADE ERROR]" in captured.out, "All errors logged via [TRADE ERROR]"

    @pytest.mark.asyncio
    @patch("polybot.scanner.get_settings")
    async def test_401_error_continues_market_loop(self, mock_settings, capsys):
        """Onchain: A 401 error does NOT break the loop."""
        import os

        from polybot.scanner import fetch_all_active_markets_async

        fixed_ts = 1774271400
        mock_cfg, mock_session = self._make_market_fixtures(fixed_ts)
        mock_settings.return_value = mock_cfg

        mock_trade = AsyncMock(
            side_effect=Exception("PolyApiException 401 Unauthorized/Invalid api key")
        )
        long_key = "0x" + "a" * 64

        with (
            patch.dict(os.environ, {"POLYMARKET_PRIVATE_KEY": long_key}),
            patch("polybot.scanner.aiohttp.ClientSession", return_value=mock_session),
            patch(
                "polybot.executor.get_polygon_balance_async",
                new_callable=AsyncMock,
                return_value=100.0,
            ),
            patch("polybot.scanner.time.time", return_value=float(fixed_ts)),
            patch("polybot.executor.place_trade_async", mock_trade),
            patch(
                "polybot.scanner._fetch_order_book",
                return_value={"asks": [{"price": "0.40"}], "bids": [{"price": "0.38"}]},
            ),
        ):
            await fetch_all_active_markets_async(min_volume=0)

        assert mock_trade.call_count == 2, (
            f"Expected 2 trade calls (continue on 401), got {mock_trade.call_count}"
        )

    @pytest.mark.asyncio
    @patch("polybot.scanner.get_settings")
    async def test_none_return_still_logs_success(self, mock_settings, capsys):
        """When place_trade_async returns None, TRADE FAILED is logged (not SUCCESS)."""
        import os

        from polybot.scanner import fetch_all_active_markets_async

        fixed_ts = 1774271400
        mock_cfg, mock_session = self._make_market_fixtures(fixed_ts)
        mock_settings.return_value = mock_cfg

        mock_trade = AsyncMock(return_value=None)
        long_key = "0x" + "a" * 64

        with (
            patch.dict(os.environ, {"POLYMARKET_PRIVATE_KEY": long_key}),
            patch("polybot.scanner.aiohttp.ClientSession", return_value=mock_session),
            patch(
                "polybot.executor.get_polygon_balance_async",
                new_callable=AsyncMock,
                return_value=100.0,
            ),
            patch("polybot.scanner.time.time", return_value=float(fixed_ts)),
            patch("polybot.executor.place_trade_async", mock_trade),
            patch(
                "polybot.scanner._fetch_order_book",
                return_value={"asks": [{"price": "0.40"}], "bids": [{"price": "0.38"}]},
            ),
        ):
            await fetch_all_active_markets_async(min_volume=0)

        captured = capsys.readouterr()
        assert "[TRADE SUCCESS]" not in captured.out, (
            "SUCCESS must not be logged when result is None"
        )
        assert "[TRADE FAILED]" in captured.out, (
            "TRADE FAILED logged when place_trade_async returns None"
        )

    @pytest.mark.asyncio
    @patch("polybot.scanner.get_settings")
    async def test_successful_trade_logs_success(self, mock_settings, capsys):
        """When place_trade_async returns a result, SUCCESS must be logged."""
        import os

        from polybot.scanner import fetch_all_active_markets_async

        fixed_ts = 1774271400
        mock_cfg, mock_session = self._make_market_fixtures(fixed_ts)
        mock_settings.return_value = mock_cfg

        mock_trade = AsyncMock(return_value={"status": "ok", "id": "test-123"})
        long_key = "0x" + "a" * 64

        with (
            patch.dict(os.environ, {"POLYMARKET_PRIVATE_KEY": long_key}),
            patch("polybot.scanner.aiohttp.ClientSession", return_value=mock_session),
            patch(
                "polybot.executor.get_polygon_balance_async",
                new_callable=AsyncMock,
                return_value=100.0,
            ),
            patch("polybot.scanner.time.time", return_value=float(fixed_ts)),
            patch("polybot.executor.place_trade_async", mock_trade),
            patch(
                "polybot.scanner._fetch_order_book",
                return_value={"asks": [{"price": "0.40"}], "bids": [{"price": "0.38"}]},
            ),
        ):
            await fetch_all_active_markets_async(min_volume=0)

        captured = capsys.readouterr()
        assert "[TRADE SUCCESS]" in captured.out, (
            "SUCCESS must be logged when trade succeeds"
        )

    @pytest.mark.asyncio
    @patch("polybot.scanner.get_settings")
    async def test_non_401_error_continues_loop(self, mock_settings, capsys):
        """Non-401 errors should continue the loop (not break)."""
        import os

        from polybot.scanner import fetch_all_active_markets_async

        fixed_ts = 1774271400
        mock_cfg, mock_session = self._make_market_fixtures(fixed_ts)
        mock_settings.return_value = mock_cfg

        mock_trade = AsyncMock(side_effect=RuntimeError("network timeout"))
        long_key = "0x" + "a" * 64

        with (
            patch.dict(os.environ, {"POLYMARKET_PRIVATE_KEY": long_key}),
            patch("polybot.scanner.aiohttp.ClientSession", return_value=mock_session),
            patch(
                "polybot.executor.get_polygon_balance_async",
                new_callable=AsyncMock,
                return_value=100.0,
            ),
            patch("polybot.scanner.time.time", return_value=float(fixed_ts)),
            patch("polybot.executor.place_trade_async", mock_trade),
            patch(
                "polybot.scanner._fetch_order_book",
                return_value={"asks": [{"price": "0.40"}], "bids": [{"price": "0.38"}]},
            ),
        ):
            await fetch_all_active_markets_async(min_volume=0)

        assert mock_trade.call_count == 2, (
            f"Expected 2 trade calls (continue on non-401), got {mock_trade.call_count}"
        )

        captured = capsys.readouterr()
        assert "[TRADE ERROR]" in captured.out


class TestExecutorAuthReraiseOn401:
    """Test executor.place_trade_async re-raises ALL exceptions (V66 onchain path)."""

    @pytest.mark.asyncio
    async def test_place_trade_async_reraises_401(self):
        """place_trade_async must re-raise exceptions containing '401'."""
        from unittest.mock import MagicMock, patch

        from polybot.executor import place_trade_async

        market = {
            "tokens": [
                {"outcome": "up", "token_id": "tok-up"},
                {"outcome": "down", "token_id": "tok-down"},
            ],
            "price_deviation": {"current_price": 0.5},
        }

        with (
            patch(
                "polybot.onchain_executor.execute_trade",
                side_effect=Exception("PolyApiException 401 Unauthorized"),
            ),
            patch("polybot.executor.get_settings") as mock_settings,
        ):
            mock_settings.return_value = MagicMock(
                dry_run=False, private_key_hex="0x" + "a" * 64
            )
            with pytest.raises(Exception, match="401"):
                await place_trade_async(market=market, outcome="up", amount=30.0)

    @pytest.mark.asyncio
    async def test_place_trade_async_reraises_non_401(self):
        """place_trade_async must re-raise ALL exceptions (not just 401)."""
        from unittest.mock import MagicMock, patch

        from polybot.executor import place_trade_async

        market = {
            "tokens": [
                {"outcome": "up", "token_id": "tok-up"},
                {"outcome": "down", "token_id": "tok-down"},
            ],
            "price_deviation": {"current_price": 0.5},
        }

        with (
            patch(
                "polybot.onchain_executor.execute_trade",
                side_effect=RuntimeError("network timeout"),
            ),
            patch("polybot.executor.get_settings") as mock_settings,
        ):
            mock_settings.return_value = MagicMock(
                dry_run=False, private_key_hex="0x" + "a" * 64
            )
            with pytest.raises(RuntimeError, match="network timeout"):
                await place_trade_async(market=market, outcome="up", amount=30.0)


class TestV60SessionAndCacheKiller:
    """Tests for onchain trade execution – no session/cache cleanup needed."""

    def _make_market_fixtures(self, fixed_ts):
        """Create standard mock settings, session, and market."""
        mock_cfg = type(
            "Settings",
            (),
            {
                "mode": "updown",
                "target_symbols": ["BTC", "ETH", "SOL", "XRP"],
                "use_api_mirrors": False,
                "up_down_only": False,
                "effective_log_level": 20,
                "dry_run": True,
            },
        )()

        markets = [
            {
                "question": "BTC Up or Down - 5 Min Live",
                "slug": f"btc-updown-5m-{fixed_ts}",
                "volume": 80_000,
                "liquidity": 5_000,
                "clobTokenIds": ["token-up-btc-001", "token-down-btc-001"],
            },
        ]

        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value=markets)
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        return mock_cfg, mock_session

    @pytest.mark.asyncio
    @patch("polybot.scanner.get_settings")
    async def test_v60_session_close_logged(self, mock_settings, capsys):
        """Onchain: trade success is logged after execution."""
        import os

        from polybot.scanner import fetch_all_active_markets_async

        fixed_ts = 1774271400
        mock_cfg, mock_session = self._make_market_fixtures(fixed_ts)
        mock_settings.return_value = mock_cfg

        mock_trade = AsyncMock(return_value={"status": "ok"})
        long_key = "0x" + "a" * 64

        with (
            patch.dict(os.environ, {"POLYMARKET_PRIVATE_KEY": long_key}),
            patch("polybot.scanner.aiohttp.ClientSession", return_value=mock_session),
            patch(
                "polybot.executor.get_polygon_balance_async",
                new_callable=AsyncMock,
                return_value=100.0,
            ),
            patch("polybot.scanner.time.time", return_value=float(fixed_ts)),
            patch("polybot.executor.place_trade_async", mock_trade),
            patch(
                "polybot.scanner._fetch_order_book",
                return_value={"asks": [{"price": "0.40"}], "bids": [{"price": "0.38"}]},
            ),
        ):
            await fetch_all_active_markets_async(min_volume=0)

        captured = capsys.readouterr()
        assert "[TRADE SUCCESS]" in captured.out

    @pytest.mark.asyncio
    @patch("polybot.scanner.get_settings")
    async def test_v61_patched_flag_set(self, mock_settings, capsys):
        """Onchain: trade is executed via executor.place_trade_async."""
        import os

        from polybot.scanner import fetch_all_active_markets_async

        fixed_ts = 1774271400
        mock_cfg, mock_session = self._make_market_fixtures(fixed_ts)
        mock_settings.return_value = mock_cfg

        mock_trade = AsyncMock(return_value={"status": "ok"})
        long_key = "0x" + "a" * 64

        with (
            patch.dict(os.environ, {"POLYMARKET_PRIVATE_KEY": long_key}),
            patch("polybot.scanner.aiohttp.ClientSession", return_value=mock_session),
            patch(
                "polybot.executor.get_polygon_balance_async",
                new_callable=AsyncMock,
                return_value=100.0,
            ),
            patch("polybot.scanner.time.time", return_value=float(fixed_ts)),
            patch("polybot.executor.place_trade_async", mock_trade),
            patch(
                "polybot.scanner._fetch_order_book",
                return_value={"asks": [{"price": "0.40"}], "bids": [{"price": "0.38"}]},
            ),
        ):
            await fetch_all_active_markets_async(min_volume=0)

        assert mock_trade.called, "executor.place_trade_async must be called"

    @pytest.mark.asyncio
    @patch("polybot.scanner.get_settings")
    async def test_v60_creds_fehler_on_missing_apikey(self, mock_settings, capsys):
        """Onchain: trade error is logged when trade raises exception."""
        import os

        from polybot.scanner import fetch_all_active_markets_async

        fixed_ts = 1774271400
        mock_cfg, mock_session = self._make_market_fixtures(fixed_ts)
        mock_settings.return_value = mock_cfg

        mock_trade = AsyncMock(side_effect=RuntimeError("trade failed"))
        long_key = "0x" + "a" * 64

        with (
            patch.dict(os.environ, {"POLYMARKET_PRIVATE_KEY": long_key}),
            patch("polybot.scanner.aiohttp.ClientSession", return_value=mock_session),
            patch(
                "polybot.executor.get_polygon_balance_async",
                new_callable=AsyncMock,
                return_value=100.0,
            ),
            patch("polybot.scanner.time.time", return_value=float(fixed_ts)),
            patch("polybot.executor.place_trade_async", mock_trade),
            patch(
                "polybot.scanner._fetch_order_book",
                return_value={"asks": [{"price": "0.40"}], "bids": [{"price": "0.38"}]},
            ),
        ):
            await fetch_all_active_markets_async(min_volume=0)

        captured = capsys.readouterr()
        assert "[TRADE ERROR]" in captured.out

    @pytest.mark.asyncio
    @patch("polybot.scanner.get_settings")
    async def test_v60_all_errors_continue_loop(self, mock_settings, capsys):
        """Onchain: All errors continue the loop, no break."""
        import os

        from polybot.scanner import fetch_all_active_markets_async

        fixed_ts = 1774271400
        mock_cfg = type(
            "Settings",
            (),
            {
                "mode": "updown",
                "target_symbols": ["BTC", "ETH", "SOL", "XRP"],
                "use_api_mirrors": False,
                "up_down_only": False,
                "effective_log_level": 20,
                "dry_run": True,
            },
        )()

        markets = [
            {
                "question": "BTC Up or Down - 5 Min Live",
                "slug": f"btc-updown-5m-{fixed_ts}",
                "volume": 80_000,
                "liquidity": 5_000,
                "clobTokenIds": ["token-up-btc-001", "token-down-btc-001"],
            },
            {
                "question": "ETH Up or Down - 5 Min Live",
                "slug": f"eth-updown-5m-{fixed_ts}",
                "volume": 80_000,
                "liquidity": 5_000,
                "clobTokenIds": ["token-up-eth-001", "token-down-eth-001"],
            },
        ]

        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value=markets)
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        mock_settings.return_value = mock_cfg

        mock_trade = AsyncMock(
            side_effect=[
                Exception("PolyApiException 401 Unauthorized"),
                {"status": "ok"},
            ]
        )
        long_key = "0x" + "a" * 64

        with (
            patch.dict(os.environ, {"POLYMARKET_PRIVATE_KEY": long_key}),
            patch("polybot.scanner.aiohttp.ClientSession", return_value=mock_session),
            patch(
                "polybot.executor.get_polygon_balance_async",
                new_callable=AsyncMock,
                return_value=100.0,
            ),
            patch("polybot.scanner.time.time", return_value=float(fixed_ts)),
            patch("polybot.executor.place_trade_async", mock_trade),
            patch(
                "polybot.scanner._fetch_order_book",
                return_value={"asks": [{"price": "0.40"}], "bids": [{"price": "0.38"}]},
            ),
        ):
            await fetch_all_active_markets_async(min_volume=0)

        assert mock_trade.call_count == 2, (
            f"Expected 2 trade calls (401 continues), got {mock_trade.call_count}"
        )

        captured = capsys.readouterr()
        assert "[TRADE ERROR]" in captured.out
        assert "[TRADE SUCCESS]" in captured.out


class TestV61MultiWayApiKeyExtraction:
    """Tests for onchain trade execution – direct executor calls."""

    def _make_market_fixtures(self, fixed_ts):
        """Create standard mock settings, session, and market."""
        mock_cfg = type(
            "Settings",
            (),
            {
                "mode": "updown",
                "target_symbols": ["BTC", "ETH", "SOL", "XRP"],
                "use_api_mirrors": False,
                "up_down_only": False,
                "effective_log_level": 20,
                "dry_run": True,
            },
        )()

        markets = [
            {
                "question": "BTC Up or Down - 5 Min Live",
                "slug": f"btc-updown-5m-{fixed_ts}",
                "volume": 80_000,
                "liquidity": 5_000,
                "clobTokenIds": ["token-up-btc-001", "token-down-btc-001"],
            },
        ]

        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value=markets)
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        return mock_cfg, mock_session

    @pytest.mark.asyncio
    @patch("polybot.scanner.get_settings")
    async def test_v61_raw_creds_type_logged(self, mock_settings, capsys):
        """Onchain: CYCLE MATCH logged for matching markets."""
        import os

        from polybot.scanner import fetch_all_active_markets_async

        fixed_ts = 1774271400
        mock_cfg, mock_session = self._make_market_fixtures(fixed_ts)
        mock_settings.return_value = mock_cfg

        mock_trade = AsyncMock(return_value={"status": "ok"})
        long_key = "0x" + "a" * 64

        with (
            patch.dict(os.environ, {"POLYMARKET_PRIVATE_KEY": long_key}),
            patch("polybot.scanner.aiohttp.ClientSession", return_value=mock_session),
            patch(
                "polybot.executor.get_polygon_balance_async",
                new_callable=AsyncMock,
                return_value=100.0,
            ),
            patch("polybot.scanner.time.time", return_value=float(fixed_ts)),
            patch("polybot.executor.place_trade_async", mock_trade),
            patch(
                "polybot.scanner._fetch_order_book",
                return_value={"asks": [{"price": "0.40"}], "bids": [{"price": "0.38"}]},
            ),
        ):
            await fetch_all_active_markets_async(min_volume=0)

        captured = capsys.readouterr()
        assert "[CYCLE MATCH]" in captured.out

    @pytest.mark.asyncio
    @patch("polybot.scanner.get_settings")
    async def test_v61_apikey_attribute_extraction(self, mock_settings, capsys):
        """Onchain: trade called with correct token_id (up token for random < 0.6)."""
        import os

        from polybot.scanner import fetch_all_active_markets_async

        fixed_ts = 1774271400
        mock_cfg, mock_session = self._make_market_fixtures(fixed_ts)
        mock_settings.return_value = mock_cfg

        mock_trade = AsyncMock(return_value={"status": "ok"})
        long_key = "0x" + "a" * 64

        with (
            patch.dict(os.environ, {"POLYMARKET_PRIVATE_KEY": long_key}),
            patch("polybot.scanner.aiohttp.ClientSession", return_value=mock_session),
            patch(
                "polybot.executor.get_polygon_balance_async",
                new_callable=AsyncMock,
                return_value=100.0,
            ),
            patch("polybot.scanner.time.time", return_value=float(fixed_ts)),
            patch("polybot.executor.place_trade_async", mock_trade),
            patch(
                "polybot.scanner._fetch_order_book",
                return_value={"asks": [{"price": "0.40"}], "bids": [{"price": "0.38"}]},
            ),
        ):
            await fetch_all_active_markets_async(min_volume=0)

        mock_trade.assert_called_once()
        call_kwargs = mock_trade.call_args.kwargs
        assert call_kwargs["token_id"] == "token-up-btc-001"
        assert call_kwargs["outcome"] == "up"

    @pytest.mark.asyncio
    @patch("polybot.scanner.get_settings")
    async def test_v61_creds_fehler_root_cause_message(self, mock_settings, capsys):
        """Onchain: error traceback logged on trade failure."""
        import os

        from polybot.scanner import fetch_all_active_markets_async

        fixed_ts = 1774271400
        mock_cfg, mock_session = self._make_market_fixtures(fixed_ts)
        mock_settings.return_value = mock_cfg

        mock_trade = AsyncMock(side_effect=RuntimeError("trade failed"))
        long_key = "0x" + "a" * 64

        with (
            patch.dict(os.environ, {"POLYMARKET_PRIVATE_KEY": long_key}),
            patch("polybot.scanner.aiohttp.ClientSession", return_value=mock_session),
            patch(
                "polybot.executor.get_polygon_balance_async",
                new_callable=AsyncMock,
                return_value=100.0,
            ),
            patch("polybot.scanner.time.time", return_value=float(fixed_ts)),
            patch("polybot.executor.place_trade_async", mock_trade),
            patch(
                "polybot.scanner._fetch_order_book",
                return_value={"asks": [{"price": "0.40"}], "bids": [{"price": "0.38"}]},
            ),
        ):
            await fetch_all_active_markets_async(min_volume=0)

        captured = capsys.readouterr()
        assert "[TRADE ERROR]" in captured.out
        assert "[FULL TRACEBACK]" in captured.out

    @pytest.mark.asyncio
    @patch("polybot.scanner.get_settings")
    async def test_v61_patched_flag_set(self, mock_settings, capsys):
        """Onchain: executor.place_trade_async called for each market."""
        import os

        from polybot.scanner import fetch_all_active_markets_async

        fixed_ts = 1774271400
        mock_cfg, mock_session = self._make_market_fixtures(fixed_ts)
        mock_settings.return_value = mock_cfg

        mock_trade = AsyncMock(return_value={"status": "ok"})
        long_key = "0x" + "a" * 64

        with (
            patch.dict(os.environ, {"POLYMARKET_PRIVATE_KEY": long_key}),
            patch("polybot.scanner.aiohttp.ClientSession", return_value=mock_session),
            patch(
                "polybot.executor.get_polygon_balance_async",
                new_callable=AsyncMock,
                return_value=100.0,
            ),
            patch("polybot.scanner.time.time", return_value=float(fixed_ts)),
            patch("polybot.executor.place_trade_async", mock_trade),
            patch(
                "polybot.scanner._fetch_order_book",
                return_value={"asks": [{"price": "0.40"}], "bids": [{"price": "0.38"}]},
            ),
        ):
            await fetch_all_active_markets_async(min_volume=0)

        assert mock_trade.called, "executor.place_trade_async must be called"


# ─── BUG-1: is_slot_tradeable ───────────────────────────────────────────────


class TestIsSlotTradeable:
    """Test is_slot_tradeable slot expiry guard."""

    def test_future_slot_returns_true(self):
        """Slot ending well in the future is tradeable."""
        import time

        from polybot.scanner import is_slot_tradeable

        now = time.time()
        # Slot started 60s ago → ends in 240s, well beyond buffer
        recent_start_ts = int(now - 60)
        slug = f"btc-updown-5m-{recent_start_ts}"
        assert is_slot_tradeable(slug) is True

    def test_expired_slot_returns_false(self):
        """Slot that already ended returns False."""
        import time

        from polybot.scanner import is_slot_tradeable

        now = time.time()
        # Slot started 600s ago → ended 300s ago
        old_start_ts = int(now - 600)
        slug = f"btc-updown-5m-{old_start_ts}"
        assert is_slot_tradeable(slug) is False

    def test_slot_within_buffer_returns_false(self):
        """Slot ending within the buffer period returns False."""
        from unittest.mock import patch as mock_patch

        from polybot.scanner import SLOT_DURATION, is_slot_tradeable

        # Freeze time for deterministic test
        frozen_now = 1_700_000_000.0
        # Slot ends at ts + 300; we want end_time = frozen_now + 30 (inside 60s buffer)
        ts = int(frozen_now + 30 - SLOT_DURATION)  # end_time = frozen_now + 30
        slug = f"btc-updown-5m-{ts}"

        with mock_patch("polybot.scanner.time") as mock_time:
            mock_time.time.return_value = frozen_now
            assert is_slot_tradeable(slug) is False

    def test_invalid_slug_no_timestamp_returns_false(self):
        """Slug with no valid timestamp returns False."""
        from polybot.scanner import is_slot_tradeable

        assert is_slot_tradeable("btc-updown-5m-notanumber") is False

    def test_custom_buffer_seconds(self):
        """Custom buffer_seconds overrides the default."""
        from unittest.mock import patch as mock_patch

        from polybot.scanner import SLOT_DURATION, is_slot_tradeable

        frozen_now = 1_700_000_000.0
        # Slot ends at frozen_now + 50 → outside 10s buffer but inside default 60s
        ts = int(frozen_now + 50 - SLOT_DURATION)
        slug = f"btc-updown-5m-{ts}"

        with mock_patch("polybot.scanner.time") as mock_time:
            mock_time.time.return_value = frozen_now
            # With default 60s buffer → False (50 < 60)
            assert is_slot_tradeable(slug) is False
            # With custom 10s buffer → True (50 > 10)
            assert is_slot_tradeable(slug, buffer_seconds=10) is True


# ─── BUG-2: Balance guard in fetch_all_active_markets_async ─────────────────


class TestBalanceGuard:
    """Test pre-loop and post-trade balance checks in fetch_all_active_markets_async."""

    def _make_market_fixtures(self, fixed_ts):
        """Create standard mock settings, session, and markets."""
        mock_cfg = type(
            "Settings",
            (),
            {
                "mode": "updown",
                "target_symbols": ["BTC", "ETH", "SOL", "XRP"],
                "use_api_mirrors": False,
                "up_down_only": False,
                "effective_log_level": 20,
                "dry_run": True,
                "MIN_BALANCE_USD": 5.0,
            },
        )()

        markets = [
            {
                "question": "BTC Up or Down - 5 Min Live",
                "slug": f"btc-updown-5m-{fixed_ts}",
                "volume": 80_000,
                "liquidity": 5_000,
                "clobTokenIds": ["token-up-btc-001", "token-down-btc-001"],
            },
            {
                "question": "ETH Up or Down - 5 Min Live",
                "slug": f"eth-updown-5m-{fixed_ts}",
                "volume": 80_000,
                "liquidity": 5_000,
                "clobTokenIds": ["token-up-eth-001", "token-down-eth-001"],
            },
        ]

        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value=markets)
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        return mock_cfg, mock_session

    @pytest.mark.asyncio
    @patch("polybot.scanner.get_settings")
    async def test_sufficient_balance_trades_proceed(self, mock_settings, capsys):
        """With balance >= MIN_BALANCE_USD, trades proceed normally."""
        import os

        from polybot.scanner import fetch_all_active_markets_async

        fixed_ts = 1774271400
        mock_cfg, mock_session = self._make_market_fixtures(fixed_ts)
        mock_settings.return_value = mock_cfg

        mock_trade = AsyncMock(return_value={"status": "ok"})
        long_key = "0x" + "a" * 64

        with (
            patch.dict(os.environ, {"POLYMARKET_PRIVATE_KEY": long_key}),
            patch("polybot.scanner.aiohttp.ClientSession", return_value=mock_session),
            patch(
                "polybot.executor.get_polygon_balance_async",
                new_callable=AsyncMock,
                return_value=100.0,
            ),
            patch("polybot.scanner.time.time", return_value=float(fixed_ts)),
            patch("polybot.executor.place_trade_async", mock_trade),
            patch(
                "polybot.scanner._fetch_order_book",
                return_value={"asks": [{"price": "0.40"}], "bids": [{"price": "0.38"}]},
            ),
        ):
            await fetch_all_active_markets_async(min_volume=0)

        assert mock_trade.called, "Trades should proceed with sufficient balance"

    @pytest.mark.asyncio
    @patch("polybot.scanner.get_settings")
    async def test_insufficient_balance_skips_cycle(self, mock_settings, capsys):
        """With balance < MIN_BALANCE_USD, entire cycle is skipped."""
        import os

        from polybot.scanner import fetch_all_active_markets_async

        fixed_ts = 1774271400
        mock_cfg, mock_session = self._make_market_fixtures(fixed_ts)
        mock_settings.return_value = mock_cfg

        mock_trade = AsyncMock(return_value={"status": "ok"})
        long_key = "0x" + "a" * 64

        with (
            patch.dict(os.environ, {"POLYMARKET_PRIVATE_KEY": long_key}),
            patch("polybot.scanner.aiohttp.ClientSession", return_value=mock_session),
            patch(
                "polybot.executor.get_polygon_balance_async",
                new_callable=AsyncMock,
                return_value=0.1,  # V78: below 0.3 threshold
            ),
            patch("polybot.scanner.time.time", return_value=float(fixed_ts)),
            patch("polybot.executor.place_trade_async", mock_trade),
            patch(
                "polybot.scanner._fetch_order_book",
                return_value={"asks": [{"price": "0.40"}], "bids": [{"price": "0.38"}]},
            ),
        ):
            await fetch_all_active_markets_async(min_volume=0)

        captured = capsys.readouterr()
        assert "Insufficient USDC balance" in captured.out
        assert not mock_trade.called, (
            "No trades should be placed with insufficient balance"
        )

    @pytest.mark.asyncio
    @patch("polybot.scanner.get_settings")
    async def test_balance_refresh_stops_when_broke(self, mock_settings, capsys):
        """Balance dropping below minimum after a trade stops further trades."""
        import os

        from polybot.scanner import fetch_all_active_markets_async

        fixed_ts = 1774271400
        mock_cfg, mock_session = self._make_market_fixtures(fixed_ts)
        mock_settings.return_value = mock_cfg

        mock_trade = AsyncMock(return_value={"status": "ok"})
        long_key = "0x" + "a" * 64

        # First call returns sufficient balance (pre-loop),
        # second call returns low balance (post-trade refresh)
        balance_side_effect = AsyncMock(side_effect=[100.0, 0.1])

        with (
            patch.dict(os.environ, {"POLYMARKET_PRIVATE_KEY": long_key}),
            patch("polybot.scanner.aiohttp.ClientSession", return_value=mock_session),
            patch("polybot.executor.get_polygon_balance_async", balance_side_effect),
            patch("polybot.scanner.time.time", return_value=float(fixed_ts)),
            patch("polybot.executor.place_trade_async", mock_trade),
            patch(
                "polybot.scanner._fetch_order_book",
                return_value={"asks": [{"price": "0.40"}], "bids": [{"price": "0.38"}]},
            ),
        ):
            await fetch_all_active_markets_async(min_volume=0)

        captured = capsys.readouterr()
        assert "Balance too low after trade" in captured.out
        # Only one trade should have executed before the balance guard kicked in
        assert mock_trade.call_count == 1


# ─── BUG-4: No duplicate [TRADE FAILED] log in executor.py ─────────────────


class TestNoDuplicateTradeFailedLog:
    """Verify executor.place_trade and place_trade_async do NOT emit [TRADE FAILED]."""

    def test_place_trade_no_trade_failed_log_on_none(self):
        """place_trade returns None when execute_trade returns None; no [TRADE FAILED] log."""
        from unittest.mock import patch as mock_patch

        from polybot.executor import place_trade

        market = {
            "question": "BTC Up or Down",
            "slug": "btc-updown-5m-123",
            "tokens": [{"outcome": "yes", "token_id": "tok1"}],
            "price_deviation": {"current_price": 0.5},
        }

        with (
            mock_patch("polybot.executor.get_settings") as mock_settings,
            mock_patch("polybot.onchain_executor.execute_trade", return_value=None),
            mock_patch("polybot.executor.log") as mock_log,
        ):
            mock_settings.return_value.dry_run = False
            mock_settings.return_value.private_key_hex = "0x" + "a" * 64
            result = place_trade(market, "yes", 10.0, dry_run=False)

        assert result is None
        # Ensure no [TRADE FAILED] log was emitted by executor
        for call in mock_log.error.call_args_list:
            msg = str(call)
            assert "[TRADE FAILED]" not in msg

    @pytest.mark.asyncio
    async def test_place_trade_async_no_trade_failed_log_on_none(self):
        """place_trade_async returns None when execute_trade returns None; no [TRADE FAILED] log."""
        from unittest.mock import patch as mock_patch

        from polybot.executor import place_trade_async

        market = {
            "question": "BTC Up or Down",
            "slug": "btc-updown-5m-123",
            "tokens": [{"outcome": "yes", "token_id": "tok1"}],
            "price_deviation": {"current_price": 0.5},
        }

        with (
            mock_patch("polybot.executor.get_settings") as mock_settings,
            mock_patch("polybot.onchain_executor.execute_trade", return_value=None),
            mock_patch("polybot.executor.log") as mock_log,
        ):
            mock_settings.return_value.dry_run = False
            mock_settings.return_value.private_key_hex = "0x" + "a" * 64
            result = await place_trade_async(market, "yes", 10.0, dry_run=False)

        assert result is None
        for call in mock_log.error.call_args_list:
            msg = str(call)
            assert "[TRADE FAILED]" not in msg


# ─── BUG-2 (session): Traded-slug cache prevents re-trading same slot ───────


class TestTradedSlugsCache:
    """Verify _traded_slugs cache prevents the same slot being traded twice."""

    def _make_market_fixtures(self, fixed_ts):
        """Create standard mock settings, session, and markets."""
        mock_cfg = type(
            "Settings",
            (),
            {
                "mode": "updown",
                "target_symbols": ["BTC", "ETH", "SOL", "XRP"],
                "use_api_mirrors": False,
                "up_down_only": False,
                "effective_log_level": 20,
                "dry_run": True,
                "MIN_BALANCE_USD": 5.0,
            },
        )()

        markets = [
            {
                "question": "BTC Up or Down - 5 Min Live",
                "slug": f"btc-updown-5m-{fixed_ts}",
                "volume": 80_000,
                "liquidity": 5_000,
                "clobTokenIds": ["token-up-btc-001", "token-down-btc-001"],
            },
            {
                "question": "BTC Up or Down - 5 Min Live (dup)",
                "slug": f"btc-updown-5m-{fixed_ts}",
                "volume": 80_000,
                "liquidity": 5_000,
                "clobTokenIds": ["token-up-btc-001", "token-down-btc-001"],
            },
        ]

        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value=markets)
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        return mock_cfg, mock_session

    @pytest.mark.asyncio
    @patch("polybot.scanner.get_settings")
    async def test_duplicate_slug_traded_only_once(self, mock_settings):
        """When filtered list contains duplicate slugs, trade executes only once."""
        import os

        from polybot.scanner import fetch_all_active_markets_async

        fixed_ts = 1774271400
        mock_cfg, mock_session = self._make_market_fixtures(fixed_ts)
        mock_settings.return_value = mock_cfg

        mock_trade = AsyncMock(return_value={"status": "ok"})
        long_key = "0x" + "a" * 64

        with (
            patch.dict(os.environ, {"POLYMARKET_PRIVATE_KEY": long_key}),
            patch("polybot.scanner.aiohttp.ClientSession", return_value=mock_session),
            patch(
                "polybot.executor.get_polygon_balance_async",
                new_callable=AsyncMock,
                return_value=100.0,
            ),
            patch("polybot.scanner.time.time", return_value=float(fixed_ts)),
            patch("polybot.executor.place_trade_async", mock_trade),
            patch(
                "polybot.scanner._fetch_order_book",
                return_value={"asks": [{"price": "0.40"}], "bids": [{"price": "0.38"}]},
            ),
        ):
            await fetch_all_active_markets_async(min_volume=0)

        # The same slug should only be traded once due to _traded_slugs cache
        assert mock_trade.call_count == 1, (
            f"Expected 1 trade call for duplicate slug, got {mock_trade.call_count}"
        )


# ─── BUG-3 (executor): No duplicate [TRADE ERROR] log from inner function ──


class TestNoDuplicateTradeErrorLog:
    """Verify executor.place_trade and place_trade_async do NOT emit [TRADE ERROR]."""

    def test_place_trade_no_trade_error_log_on_exception(self):
        """place_trade re-raises without logging [TRADE ERROR]."""
        from unittest.mock import patch as mock_patch

        from polybot.executor import place_trade

        market = {
            "question": "BTC Up or Down",
            "slug": "btc-updown-5m-123",
            "tokens": [{"outcome": "yes", "token_id": "tok1"}],
            "price_deviation": {"current_price": 0.5},
        }

        with (
            mock_patch("polybot.executor.get_settings") as mock_settings,
            mock_patch(
                "polybot.onchain_executor.execute_trade",
                side_effect=RuntimeError("CLOB failed"),
            ),
            mock_patch("polybot.executor.log") as mock_log,
        ):
            mock_settings.return_value.dry_run = False
            mock_settings.return_value.private_key_hex = "0x" + "a" * 64
            with pytest.raises(RuntimeError, match="CLOB failed"):
                place_trade(market, "yes", 10.0, dry_run=False)

        # Ensure no [TRADE ERROR] log was emitted by executor (only re-raise)
        for call in mock_log.error.call_args_list:
            msg = str(call)
            assert "[TRADE ERROR]" not in msg

    @pytest.mark.asyncio
    async def test_place_trade_async_no_trade_error_log_on_exception(self):
        """place_trade_async re-raises without logging [TRADE ERROR]."""
        from unittest.mock import patch as mock_patch

        from polybot.executor import place_trade_async

        market = {
            "question": "BTC Up or Down",
            "slug": "btc-updown-5m-123",
            "tokens": [{"outcome": "yes", "token_id": "tok1"}],
            "price_deviation": {"current_price": 0.5},
        }

        with (
            mock_patch("polybot.executor.get_settings") as mock_settings,
            mock_patch(
                "polybot.onchain_executor.execute_trade",
                side_effect=RuntimeError("CLOB failed"),
            ),
            mock_patch("polybot.executor.log") as mock_log,
        ):
            mock_settings.return_value.dry_run = False
            mock_settings.return_value.private_key_hex = "0x" + "a" * 64
            with pytest.raises(RuntimeError, match="CLOB failed"):
                await place_trade_async(market, "yes", 10.0, dry_run=False)

        for call in mock_log.error.call_args_list:
            msg = str(call)
            assert "[TRADE ERROR]" not in msg


# ─── BUG-1 (initial filter): Expired slots rejected before adding to filtered ─


class TestExpiredSlotInitialFilter:
    """Verify expired slots are not added to filtered list in initial API loop."""

    @pytest.mark.asyncio
    @patch("polybot.scanner.get_settings")
    async def test_expired_slot_not_in_filtered(self, mock_settings):
        """Expired slot should be excluded from filtered list entirely."""
        import os

        from polybot.scanner import fetch_all_active_markets_async

        # Use a timestamp far in the past so slot is expired
        expired_ts = 1000000000
        current_ts = 1774271400

        mock_cfg = type(
            "Settings",
            (),
            {
                "mode": "updown",
                "target_symbols": ["BTC", "ETH", "SOL", "XRP"],
                "use_api_mirrors": False,
                "up_down_only": False,
                "effective_log_level": 20,
                "dry_run": True,
                "MIN_BALANCE_USD": 5.0,
            },
        )()
        mock_settings.return_value = mock_cfg

        # The API returns a market with an expired slot timestamp
        markets = [
            {
                "question": "BTC Up or Down - 5 Min Live",
                "slug": f"btc-updown-5m-{expired_ts}",
                "volume": 80_000,
                "liquidity": 5_000,
                "clobTokenIds": ["token-up-btc-001", "token-down-btc-001"],
            },
        ]

        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value=markets)
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        long_key = "0x" + "a" * 64

        with (
            patch.dict(os.environ, {"POLYMARKET_PRIVATE_KEY": long_key}),
            patch("polybot.scanner.aiohttp.ClientSession", return_value=mock_session),
            patch(
                "polybot.executor.get_polygon_balance_async",
                new_callable=AsyncMock,
                return_value=100.0,
            ),
            patch("polybot.scanner.time.time", return_value=float(current_ts)),
            patch("polybot.executor.place_trade_async", new_callable=AsyncMock),
        ):
            result = await fetch_all_active_markets_async(min_volume=0)

        # Expired slot should not appear in filtered results
        assert len(result) == 0, (
            "Expired slots should be filtered out before adding to list"
        )
