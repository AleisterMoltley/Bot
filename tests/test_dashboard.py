"""Tests for the FastAPI dashboard."""

import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest


@pytest.fixture
def temp_db():
    """Create a temporary database for testing."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    # Initialize the database with schema
    conn = sqlite3.connect(db_path)
    conn.executescript("""
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

        CREATE TABLE IF NOT EXISTS risk_state (
            id INTEGER PRIMARY KEY DEFAULT 1,
            daily_loss REAL DEFAULT 0,
            consecutive_losses INTEGER DEFAULT 0,
            is_paused INTEGER DEFAULT 0,
            last_reset_date TEXT,
            updated_at TEXT
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
    """)

    # Insert some test data
    conn.execute(
        "INSERT INTO risk_state (id, daily_loss, consecutive_losses, is_paused) "
        "VALUES (1, 25.50, 2, 0)"
    )
    conn.execute(
        "INSERT INTO trades (timestamp, mode, side, price, size, cost, profit, market_title) "
        "VALUES ('2026-03-18T12:00:00', 'signal', 'BUY', 0.65, 100, 65.00, 10.50, 'Test Market')"
    )
    conn.execute(
        "INSERT INTO positions (opened_at, market_id, market_title, token_id, side, size, "
        "avg_price, cost_basis, current_price, unrealized_pnl, status) "
        "VALUES ('2026-03-18T11:00:00', 'mkt1', 'Test Position', 'tok1', 'YES', 50, "
        "0.55, 27.50, 0.60, 2.50, 'OPEN')"
    )
    conn.commit()
    conn.close()

    yield db_path

    # Cleanup
    Path(db_path).unlink(missing_ok=True)


class MockSettings:
    """Mock settings for testing."""

    db_path = ""
    dashboard_port = 8080


class TestDashboardHelpers:
    """Test dashboard helper functions."""

    def test_get_risk_state(self, temp_db):
        """Test _get_risk_state returns correct data."""
        mock_settings = MockSettings()
        mock_settings.db_path = temp_db

        with patch("polybot.dashboard.get_settings", return_value=mock_settings):
            with patch("polybot.database.get_settings", return_value=mock_settings):
                from polybot.dashboard import _get_risk_state

                # Clear the cached db path
                import polybot.database as db_module

                db_module._DB_PATH = None

                risk = _get_risk_state()
                assert risk["daily_loss"] == 25.50
                assert risk["consecutive_losses"] == 2
                assert risk["is_paused"] == 0

    def test_get_today_pnl_no_data(self, temp_db):
        """Test _get_today_pnl returns zeros when no data."""
        mock_settings = MockSettings()
        mock_settings.db_path = temp_db

        with patch("polybot.dashboard.get_settings", return_value=mock_settings):
            with patch("polybot.database.get_settings", return_value=mock_settings):
                from polybot.dashboard import _get_today_pnl

                import polybot.database as db_module

                db_module._DB_PATH = None

                pnl = _get_today_pnl()
                assert pnl["realized"] == 0
                assert pnl["total_trades"] == 0
                assert pnl["winrate"] == 0

    def test_get_total_pnl_no_data(self, temp_db):
        """Test _get_total_pnl returns zeros when no data."""
        mock_settings = MockSettings()
        mock_settings.db_path = temp_db

        with patch("polybot.dashboard.get_settings", return_value=mock_settings):
            with patch("polybot.database.get_settings", return_value=mock_settings):
                from polybot.dashboard import _get_total_pnl

                import polybot.database as db_module

                db_module._DB_PATH = None

                pnl = _get_total_pnl()
                assert pnl["total_realized"] == 0
                assert pnl["total_trades"] == 0
                assert pnl["overall_winrate"] == 0


class TestDashboardTemplates:
    """Test that dashboard templates exist and are valid."""

    def test_templates_exist(self):
        """Test that all required templates exist."""
        templates_dir = Path(__file__).parent.parent / "src" / "polybot" / "templates"
        required_templates = [
            "base.html",
            "index.html",
            "trades.html",
            "positions.html",
            "pnl.html",
        ]

        for template in required_templates:
            template_path = templates_dir / template
            assert template_path.exists(), f"Template {template} not found"

    def test_templates_have_jinja_syntax(self):
        """Test that templates contain Jinja2 syntax."""
        templates_dir = Path(__file__).parent.parent / "src" / "polybot" / "templates"

        base_content = (templates_dir / "base.html").read_text()
        assert "{% block content %}" in base_content
        assert "{% block title %}" in base_content

        index_content = (templates_dir / "index.html").read_text()
        assert "{% extends" in index_content
        assert "{% for trade in trades %}" in index_content
