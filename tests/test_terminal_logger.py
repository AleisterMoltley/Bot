"""Tests for terminal logger functionality."""

from __future__ import annotations

import tempfile
from pathlib import Path

from polybot.terminal_logger import (
    LogCategory,
    categorize_log_line,
    tail_file,
)
from polybot.logging_setup import (
    get_log_dir,
    get_log_file_path,
    DEFAULT_LOG_DIR,
)


class TestLogCategory:
    """Tests for LogCategory class."""

    def test_log_category_constants_exist(self):
        """Verify all expected category constants exist."""
        assert hasattr(LogCategory, "SCAN")
        assert hasattr(LogCategory, "TRADE")
        assert hasattr(LogCategory, "API")
        assert hasattr(LogCategory, "ERROR")
        assert hasattr(LogCategory, "WARNING")
        assert hasattr(LogCategory, "SIGNAL")
        assert hasattr(LogCategory, "COPY")
        assert hasattr(LogCategory, "ARB")
        assert hasattr(LogCategory, "RISK")
        assert hasattr(LogCategory, "WALLET")
        assert hasattr(LogCategory, "DEBUG")
        assert hasattr(LogCategory, "INFO")
        assert hasattr(LogCategory, "LOG")

    def test_log_category_is_tuple(self):
        """Verify categories are (prefix, style) tuples."""
        assert isinstance(LogCategory.SCAN, tuple)
        assert len(LogCategory.SCAN) == 2
        assert "[SCAN]" in LogCategory.SCAN[0]


class TestCategorizeLogLine:
    """Tests for categorize_log_line function."""

    def test_scan_keywords(self):
        """Test scan-related keywords are categorized correctly."""
        lines = [
            "Scanning markets for opportunities",
            "QUERY to polymarket API",
            "Fetch latest price data",
            "Odds updated for market",
        ]
        for line in lines:
            prefix, style = categorize_log_line(line)
            assert "[SCAN]" in prefix

    def test_trade_keywords(self):
        """Test trade-related keywords are categorized correctly."""
        lines = [
            "Executing trade order",
            "BUY 100 USDC",
            "SELL position closed",
            "Order filled successfully",
        ]
        for line in lines:
            prefix, style = categorize_log_line(line)
            assert "[TRADE]" in prefix

    def test_error_keywords(self):
        """Test error-related keywords are categorized correctly."""
        lines = [
            "ERROR: Connection timeout",
            "Exception raised in handler",
            "Network connection failed",
            "Traceback (most recent call last):",
        ]
        for line in lines:
            prefix, style = categorize_log_line(line)
            assert "[ERROR]" in prefix

    def test_signal_keywords(self):
        """Test signal-related keywords are categorized correctly."""
        lines = [
            "Signal confidence: 75%",
            "RSI indicator showing overbought",
            "MACD crossover detected",
        ]
        for line in lines:
            prefix, style = categorize_log_line(line)
            assert "[SIGNAL]" in prefix

    def test_arb_keywords(self):
        """Test arbitrage-related keywords are categorized correctly."""
        lines = [
            "Arbitrage opportunity found",
            "ARB spread: 2.5%",
        ]
        for line in lines:
            prefix, style = categorize_log_line(line)
            assert "[ARB]" in prefix

    def test_risk_keywords(self):
        """Test risk-related keywords are categorized correctly."""
        lines = [
            "Risk limit exceeded",
            "Circuit breaker triggered",
            "Kelly criterion suggests 0.25",
        ]
        for line in lines:
            prefix, style = categorize_log_line(line)
            assert "[RISK]" in prefix

    def test_wallet_keywords(self):
        """Test wallet-related keywords are categorized correctly."""
        lines = [
            "Wallet balance: 100 USDC",
            "Polygon transaction confirmed",
            "Solana swap completed",
        ]
        for line in lines:
            prefix, style = categorize_log_line(line)
            assert "[WALLET]" in prefix

    def test_generic_log(self):
        """Test unrecognized lines get generic LOG category."""
        prefix, style = categorize_log_line("Some generic message without keywords")
        assert "[LOG]" in prefix

    def test_case_insensitive(self):
        """Test keyword matching is case insensitive."""
        prefix1, _ = categorize_log_line("SCAN")
        prefix2, _ = categorize_log_line("scan")
        prefix3, _ = categorize_log_line("Scan")
        assert prefix1 == prefix2 == prefix3


class TestTailFile:
    """Tests for tail_file function."""

    def test_read_existing_file(self):
        """Test reading existing file content."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".log", delete=False) as f:
            f.write("Line 1\n")
            f.write("Line 2\n")
            f.write("Line 3\n")
            log_path = Path(f.name)

        try:
            lines = list(tail_file(log_path, follow=False))
            assert len(lines) == 3
            assert lines[0] == "Line 1"
            assert lines[1] == "Line 2"
            assert lines[2] == "Line 3"
        finally:
            log_path.unlink()

    def test_empty_file(self):
        """Test reading empty file."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".log", delete=False) as f:
            log_path = Path(f.name)

        try:
            lines = list(tail_file(log_path, follow=False))
            assert lines == []
        finally:
            log_path.unlink()


class TestLoggingSetup:
    """Tests for logging_setup module."""

    def test_get_log_dir_default(self, monkeypatch):
        """Test default log directory."""
        monkeypatch.delenv("POLYBOT_LOG_DIR", raising=False)
        log_dir = get_log_dir()
        assert log_dir == DEFAULT_LOG_DIR

    def test_get_log_dir_from_env(self, monkeypatch):
        """Test custom log directory from environment."""
        monkeypatch.setenv("POLYBOT_LOG_DIR", "/custom/logs")
        log_dir = get_log_dir()
        assert log_dir == Path("/custom/logs")

    def test_get_log_file_path(self, monkeypatch):
        """Test log file path construction."""
        monkeypatch.delenv("POLYBOT_LOG_DIR", raising=False)
        log_file = get_log_file_path()
        assert log_file.name == "polybot.log"
        assert log_file.parent == DEFAULT_LOG_DIR
