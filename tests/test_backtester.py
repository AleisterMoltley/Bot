"""Tests for the Backtester module."""

from unittest.mock import patch, MagicMock


class TestBacktester:
    """Test Backtester class functionality."""

    def test_backtester_initialization(self):
        """Test backtester initializes with correct defaults."""
        from polybot.backtester import Backtester

        bt = Backtester()
        assert bt.trade_size == 100.0
        assert bt.min_ev == 0.08
        assert bt._last_result is None

    def test_backtester_custom_params(self):
        """Test backtester accepts custom parameters."""
        from polybot.backtester import Backtester

        bt = Backtester(trade_size=200.0, min_ev=0.10)
        assert bt.trade_size == 200.0
        assert bt.min_ev == 0.10

    def test_simulate_trade_skip_no_value(self):
        """Test trade simulation skips when no clear value."""
        from polybot.backtester import Backtester

        bt = Backtester()

        # Market with prices near 0.5 (no value)
        market = {
            "question": "Test market?",
            "tokens": [
                {"outcome": "yes", "price": 0.50},
                {"outcome": "no", "price": 0.50},
            ],
        }

        result = bt._simulate_trade(market)
        assert result.get("skip") is True

    def test_simulate_trade_yes_value(self):
        """Test trade simulation identifies YES value opportunity."""
        from polybot.backtester import Backtester

        bt = Backtester(min_ev=0.05)  # Lower threshold for test

        # Market with underpriced YES
        market = {
            "question": "Test market?",
            "resolution": "yes",
            "end_date": "2024-01-01",
            "tokens": [
                {"outcome": "yes", "price": 0.30},
                {"outcome": "no", "price": 0.70},
            ],
        }

        result = bt._simulate_trade(market)
        assert result.get("skip") is False
        assert result.get("bet_side") == "yes"
        assert result.get("won") is True
        assert result.get("pnl") > 0

    def test_simulate_trade_loss(self):
        """Test trade simulation calculates loss correctly."""
        from polybot.backtester import Backtester

        bt = Backtester(min_ev=0.05)

        # Market where we bet YES but NO wins
        market = {
            "question": "Test market?",
            "resolution": "no",
            "end_date": "2024-01-01",
            "tokens": [
                {"outcome": "yes", "price": 0.30},
                {"outcome": "no", "price": 0.70},
            ],
        }

        result = bt._simulate_trade(market)
        assert result.get("skip") is False
        assert result.get("bet_side") == "yes"
        assert result.get("won") is False
        assert result.get("pnl") < 0

    @patch("polybot.backtester.fetch_historical_markets")
    def test_fetch_historical_markets(self, mock_fetch):
        """Test fetching historical markets from API."""
        from polybot.backtester import fetch_historical_markets

        mock_fetch.return_value = [
            {"question": "Market 1", "resolved": True},
            {"question": "Market 2", "resolved": True},
        ]

        markets = fetch_historical_markets(limit=100)

        # Since we mocked the function itself, verify the mock was called
        mock_fetch.assert_called_once_with(limit=100)
        assert len(markets) == 2

    @patch("polybot.backtester.requests.get")
    def test_fetch_historical_markets_real(self, mock_get):
        """Test actual fetch_historical_markets function."""
        from polybot.backtester import fetch_historical_markets

        mock_response = MagicMock()
        mock_response.json.return_value = [
            {"question": "Market 1", "resolved": True},
            {"question": "Market 2", "resolved": True},
        ]
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        markets = fetch_historical_markets(limit=100)

        assert len(markets) == 2
        mock_get.assert_called_once()

    @patch("polybot.backtester.requests.get")
    def test_fetch_historical_markets_error(self, mock_get):
        """Test handling of API errors."""
        from polybot.backtester import fetch_historical_markets

        mock_get.side_effect = Exception("Network error")

        markets = fetch_historical_markets()

        assert markets == []

    @patch("polybot.backtester.fetch_historical_markets")
    def test_run_backtest_no_data(self, mock_fetch):
        """Test backtest with no historical data."""
        from polybot.backtester import Backtester

        mock_fetch.return_value = []

        bt = Backtester()
        result = bt.run_backtest(days=30)

        assert result.total_trades == 0
        assert result.total_pnl == 0.0
        assert result.winrate == 0.0

    @patch("polybot.backtester.fetch_historical_markets")
    def test_run_backtest_with_data(self, mock_fetch):
        """Test backtest with mock market data."""
        from polybot.backtester import Backtester

        mock_fetch.return_value = [
            # Winning trade
            {
                "question": "Win market?",
                "resolution": "yes",
                "end_date": "2024-01-01",
                "tokens": [
                    {"outcome": "yes", "price": 0.30},
                    {"outcome": "no", "price": 0.70},
                ],
            },
            # Losing trade
            {
                "question": "Lose market?",
                "resolution": "no",
                "end_date": "2024-01-02",
                "tokens": [
                    {"outcome": "yes", "price": 0.30},
                    {"outcome": "no", "price": 0.70},
                ],
            },
        ]

        bt = Backtester(min_ev=0.05)
        result = bt.run_backtest(days=10)

        assert result.total_trades == 2
        assert result.winning_trades == 1
        assert result.losing_trades == 1
        assert result.winrate == 50.0
        assert len(result.equity_curve) == 2

    def test_get_last_result(self):
        """Test retrieving last backtest result."""
        from polybot.backtester import Backtester, BacktestResult

        bt = Backtester()
        assert bt.get_last_result() is None

        # Manually set a result
        bt._last_result = BacktestResult(total_pnl=100.0)
        result = bt.get_last_result()
        assert result is not None
        assert result.total_pnl == 100.0


class TestBacktestResult:
    """Test BacktestResult dataclass."""

    def test_backtest_result_defaults(self):
        """Test BacktestResult has correct defaults."""
        from polybot.backtester import BacktestResult

        result = BacktestResult()
        assert result.total_pnl == 0.0
        assert result.winrate == 0.0
        assert result.profit_factor == 0.0
        assert result.sharpe == 0.0
        assert result.equity_curve == []

    def test_backtest_result_to_dict(self):
        """Test BacktestResult serialization."""
        from polybot.backtester import BacktestResult

        result = BacktestResult(
            total_pnl=150.50,
            winrate=55.5,
            profit_factor=1.75,
            sharpe=1.25,
            total_trades=20,
            winning_trades=11,
            losing_trades=9,
        )

        data = result.to_dict()
        assert data["total_pnl"] == 150.5
        assert data["winrate"] == 55.5
        assert data["profit_factor"] == 1.75
        assert data["sharpe"] == 1.25
        assert data["total_trades"] == 20


class TestFormatBacktestReport:
    """Test report formatting function."""

    def test_format_positive_report(self):
        """Test formatting positive backtest results."""
        from polybot.backtester import format_backtest_report, BacktestResult

        result = BacktestResult(
            total_pnl=500.0,
            winrate=60.0,
            profit_factor=2.0,
            sharpe=1.5,
            total_trades=50,
            winning_trades=30,
            losing_trades=20,
        )

        report = format_backtest_report(result)
        assert "BACKTEST REPORT" in report
        assert "$500.00" in report
        assert "60.0%" in report
        assert "2.00" in report

    def test_format_negative_report(self):
        """Test formatting negative backtest results."""
        from polybot.backtester import format_backtest_report, BacktestResult

        result = BacktestResult(
            total_pnl=-200.0,
            winrate=40.0,
            profit_factor=0.5,
            sharpe=-0.5,
        )

        report = format_backtest_report(result)
        assert "-$200.00" in report or "$-200.00" in report

    def test_format_from_dict(self):
        """Test formatting from dictionary input."""
        from polybot.backtester import format_backtest_report

        data = {
            "total_pnl": 100.0,
            "winrate": 55.0,
            "profit_factor": 1.5,
            "sharpe": 1.0,
            "total_trades": 10,
            "winning_trades": 6,
            "losing_trades": 4,
            "max_drawdown": 50.0,
            "run_timestamp": "2024-01-01T00:00:00",
        }

        report = format_backtest_report(data)
        assert "BACKTEST REPORT" in report
        assert "$100.00" in report


class TestEdgeBacktester:
    """Test EdgeBacktester class functionality."""

    def test_edge_backtester_initialization(self):
        """Test EdgeBacktester initializes correctly."""
        from polybot.backtester import EdgeBacktester

        bt = EdgeBacktester()
        assert bt.edge_engine is not None
        assert bt.settings is not None
        assert bt.position_size == 1000  # Default position size

    def test_edge_backtester_custom_position_size(self):
        """Test EdgeBacktester with custom position size."""
        from polybot.backtester import EdgeBacktester

        bt = EdgeBacktester(position_size=500)
        assert bt.position_size == 500

    def test_edge_backtester_get_yes_price_at_close(self):
        """Test yes price extraction from market data."""
        from polybot.backtester import EdgeBacktester

        bt = EdgeBacktester()

        # Test with yes_price_at_close field
        market1 = {"yes_price_at_close": 0.65}
        assert bt._get_yes_price_at_close(market1) == 0.65

        # Test with tokens array
        market2 = {
            "tokens": [
                {"outcome": "yes", "price": 0.55},
                {"outcome": "no", "price": 0.45},
            ]
        }
        assert bt._get_yes_price_at_close(market2) == 0.55

        # Test fallback to 0.5
        market3 = {}
        assert bt._get_yes_price_at_close(market3) == 0.5

    def test_edge_backtester_constants(self):
        """Test EdgeBacktester class constants."""
        from polybot.backtester import EdgeBacktester

        assert EdgeBacktester.DEFAULT_POSITION_SIZE == 1000
        assert EdgeBacktester.MAX_MARKETS_TO_FETCH == 2000


class TestPortfolioManager:
    """Test PortfolioManager class."""

    def test_portfolio_manager_initialization(self):
        """Test portfolio manager initializes correctly."""
        from polybot.portfolio_manager import PortfolioManager

        pm = PortfolioManager(bankroll=5000.0)
        assert pm.bankroll == 5000.0
        assert len(pm._positions) == 0

    def test_classify_market(self):
        """Test market classification."""
        from polybot.portfolio_manager import PortfolioManager

        pm = PortfolioManager()

        assert pm.classify_market("Will Bitcoin hit $100k?") == "crypto"
        assert pm.classify_market("Trump election chances") == "politics"
        assert pm.classify_market("Super Bowl winner") == "sports"
        assert pm.classify_market("Random market question") == "other"

    def test_can_add_position_within_limits(self):
        """Test position approval always succeeds (FORCED EXECUTION v5)."""
        from polybot.portfolio_manager import PortfolioManager

        pm = PortfolioManager(bankroll=1000.0)

        # Any position size should be allowed
        allowed, reason = pm.can_add_position("BTC market", 100.0)
        assert allowed is True
        assert reason == "FORCED_OK"

    def test_can_add_position_exceeds_limits(self):
        """Test position is ALWAYS approved (FORCED EXECUTION v5)."""
        from polybot.portfolio_manager import PortfolioManager

        pm = PortfolioManager(bankroll=1000.0)

        # FORCED EXECUTION v5: Even large positions are now allowed
        allowed, reason = pm.can_add_position("BTC market", 500.0)
        assert allowed is True
        assert reason == "FORCED_OK"

    def test_add_position(self):
        """Test adding a position."""
        from polybot.portfolio_manager import PortfolioManager

        pm = PortfolioManager(bankroll=1000.0)

        position = pm.add_position(
            market_id="test-123",
            market_name="Will BTC hit $100k?",
            size_usd=50.0,
            entry_price=0.45,
            side="yes",
        )

        assert position is not None
        assert position.category == "crypto"
        assert position.size_usd == 50.0
        assert len(pm._positions) == 1

    def test_category_exposure_limit(self):
        """Test category exposure check always passes (FORCED EXECUTION v5)."""
        from polybot.portfolio_manager import PortfolioManager

        pm = PortfolioManager(bankroll=1000.0)

        # Add 3 positions at 9% each = 27% total crypto exposure
        pm.add_position("id1", "BTC market 1", 90.0, 0.5, "yes")
        pm.add_position("id2", "ETH market 1", 90.0, 0.5, "yes")
        pm.add_position("id3", "SOL market 1", 90.0, 0.5, "yes")

        # FORCED EXECUTION v5: Position is always allowed
        allowed, reason = pm.can_add_position("DOGE market", 50.0, "crypto")
        assert allowed is True
        assert reason == "FORCED_OK"


class TestVolatilityRegimeDetector:
    """Test VolatilityRegimeDetector class."""

    def test_detector_initialization(self):
        """Test volatility detector initializes correctly."""
        from polybot.volatility_regime import VolatilityRegimeDetector

        detector = VolatilityRegimeDetector()
        assert len(detector._price_history) == 0

    def test_add_price_data(self):
        """Test adding price data."""
        from polybot.volatility_regime import VolatilityRegimeDetector

        detector = VolatilityRegimeDetector()
        detector.add_price_data(0.5)
        detector.add_price_data(0.52)

        assert len(detector._price_history) == 2

    def test_volatility_score_default(self):
        """Test default volatility score with insufficient data."""
        from polybot.volatility_regime import VolatilityRegimeDetector

        detector = VolatilityRegimeDetector()
        score = detector.calculate_volatility_score()

        assert score == 50.0  # Default

    def test_volatility_regime_classification(self):
        """Test regime classification."""
        from polybot.volatility_regime import VolatilityRegimeDetector, VolatilityRegime

        detector = VolatilityRegimeDetector()

        assert detector.get_regime(25) == VolatilityRegime.LOW
        assert detector.get_regime(50) == VolatilityRegime.NORMAL
        assert detector.get_regime(80) == VolatilityRegime.HIGH

    def test_kelly_multiplier(self):
        """Test Kelly multiplier for different regimes."""
        from polybot.volatility_regime import VolatilityRegimeDetector, VolatilityRegime

        detector = VolatilityRegimeDetector()

        assert detector.get_kelly_multiplier(VolatilityRegime.LOW) == 1.2
        assert detector.get_kelly_multiplier(VolatilityRegime.NORMAL) == 1.0
        assert detector.get_kelly_multiplier(VolatilityRegime.HIGH) == 0.6


class TestExecutionLogger:
    """Test ExecutionLogger class."""

    def test_logger_initialization(self):
        """Test execution logger initializes correctly."""
        from polybot.execution_logger import ExecutionLogger

        logger = ExecutionLogger()
        assert len(logger._records) == 0
        assert logger._trade_counter == 0

    def test_log_execution(self):
        """Test logging an execution."""
        from polybot.execution_logger import ExecutionLogger

        logger = ExecutionLogger()

        record = logger.log_execution(
            market_id="test-123",
            market_name="Test Market",
            side="yes",
            direction="buy",
            size_usd=100.0,
            expected_price=0.50,
            actual_price=0.51,
            execution_time_ms=150.0,
        )

        assert record is not None
        assert abs(record.slippage_pct - 2.0) < 0.001  # (0.51 - 0.50) / 0.50 * 100
        assert len(logger._records) == 1

    def test_execution_stats(self):
        """Test execution statistics calculation."""
        from polybot.execution_logger import ExecutionLogger

        logger = ExecutionLogger()

        # Log multiple executions
        logger.log_execution("id1", "Market 1", "yes", "buy", 100.0, 0.50, 0.51, 100.0)
        logger.log_execution("id2", "Market 2", "no", "sell", 200.0, 0.60, 0.59, 200.0)

        stats = logger.get_stats()

        assert stats.total_trades == 2
        assert stats.total_volume_usd == 300.0
        assert stats.successful_trades == 2

    def test_recent_executions(self):
        """Test getting recent executions."""
        from polybot.execution_logger import ExecutionLogger

        logger = ExecutionLogger()

        logger.log_execution("id1", "Market 1", "yes", "buy", 100.0, 0.50, 0.50, 100.0)

        recent = logger.get_recent_executions(limit=10)

        assert len(recent) == 1
        assert recent[0]["market_id"] == "id1"
