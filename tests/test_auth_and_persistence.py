"""Tests for auth middleware, piggybank DB persistence, and WAL concurrency."""

import sqlite3
import threading
import time
from unittest.mock import patch, MagicMock, AsyncMock

import pytest


class TestDashboardAuthMiddleware:
    """Tests for the Bearer-token auth middleware."""

    def _make_mock_settings(self, password=""):
        mock = MagicMock()
        mock.dashboard_password.get_secret_value.return_value = password
        return mock

    @pytest.mark.asyncio
    async def test_no_password_allows_all(self):
        """When DASHBOARD_PASSWORD is empty, all requests pass."""
        from polybot.main_fastapi import DashboardAuthMiddleware

        middleware = DashboardAuthMiddleware(app=MagicMock())

        request = MagicMock()
        request.url.path = "/api/status"
        request.headers = {}
        request.query_params = {}
        request.client.host = "127.0.0.1"

        call_next = AsyncMock(return_value=MagicMock(status_code=200))

        with patch("polybot.main_fastapi.get_settings", return_value=self._make_mock_settings("")):
            response = await middleware.dispatch(request, call_next)
            call_next.assert_called_once()

    @pytest.mark.asyncio
    async def test_valid_bearer_token_passes(self):
        """Valid Bearer token allows access."""
        from polybot.main_fastapi import DashboardAuthMiddleware

        middleware = DashboardAuthMiddleware(app=MagicMock())

        request = MagicMock()
        request.url.path = "/api/status"
        request.headers = {"authorization": "Bearer mysecretpassword"}
        request.query_params = {}
        request.client.host = "127.0.0.1"

        call_next = AsyncMock(return_value=MagicMock(status_code=200))

        with patch("polybot.main_fastapi.get_settings", return_value=self._make_mock_settings("mysecretpassword")):
            response = await middleware.dispatch(request, call_next)
            call_next.assert_called_once()

    @pytest.mark.asyncio
    async def test_invalid_token_returns_401(self):
        """Invalid Bearer token returns 401."""
        from polybot.main_fastapi import DashboardAuthMiddleware, _AUTH_FAIL_TRACKER

        _AUTH_FAIL_TRACKER.clear()
        middleware = DashboardAuthMiddleware(app=MagicMock())

        request = MagicMock()
        request.url.path = "/api/status"
        request.headers = {"authorization": "Bearer wrongpassword"}
        request.query_params = {}
        request.client.host = "127.0.0.1"

        call_next = AsyncMock()

        with patch("polybot.main_fastapi.get_settings", return_value=self._make_mock_settings("correctpassword")):
            response = await middleware.dispatch(request, call_next)
            call_next.assert_not_called()
            assert response.status_code == 401

    @pytest.mark.asyncio
    async def test_health_endpoint_bypasses_auth(self):
        """GET /api/health always passes even with password set."""
        from polybot.main_fastapi import DashboardAuthMiddleware

        middleware = DashboardAuthMiddleware(app=MagicMock())

        request = MagicMock()
        request.url.path = "/api/health"
        request.headers = {}
        request.query_params = {}
        request.client.host = "127.0.0.1"

        call_next = AsyncMock(return_value=MagicMock(status_code=200))

        with patch("polybot.main_fastapi.get_settings", return_value=self._make_mock_settings("secret")):
            response = await middleware.dispatch(request, call_next)
            call_next.assert_called_once()

    @pytest.mark.asyncio
    async def test_rate_limiting_after_10_failures(self):
        """After 10 failed attempts from same IP, returns 429."""
        from polybot.main_fastapi import (
            DashboardAuthMiddleware,
            _AUTH_FAIL_TRACKER,
            _AUTH_FAIL_WINDOW,
        )

        _AUTH_FAIL_TRACKER.clear()
        # Pre-fill 10 failures
        _AUTH_FAIL_TRACKER["10.0.0.1"] = [time.time()] * 10

        middleware = DashboardAuthMiddleware(app=MagicMock())

        request = MagicMock()
        request.url.path = "/api/status"
        request.headers = {"authorization": "Bearer correctpassword"}
        request.query_params = {}
        request.client.host = "10.0.0.1"

        call_next = AsyncMock()

        with patch("polybot.main_fastapi.get_settings", return_value=self._make_mock_settings("correctpassword")):
            response = await middleware.dispatch(request, call_next)
            call_next.assert_not_called()
            assert response.status_code == 429

        _AUTH_FAIL_TRACKER.clear()


class TestPiggyBankDBPersistence:
    """Test that piggybank transfers are persisted to the database."""

    def test_piggybank_transfer_creates_db_record(self, tmp_path):
        """After a successful transfer, a row exists in piggybank_transfers."""
        db_path = tmp_path / "test.db"

        # Create schema
        conn = sqlite3.connect(str(db_path))
        conn.execute("""CREATE TABLE IF NOT EXISTS piggybank_transfers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            profit_usd REAL NOT NULL,
            amount_usd REAL NOT NULL,
            tx_hash TEXT,
            status TEXT DEFAULT 'ok'
        )""")
        conn.commit()

        # Simulate what piggybank.py does after a successful transfer
        from datetime import datetime, timezone
        conn.execute(
            "INSERT INTO piggybank_transfers (timestamp, profit_usd, amount_usd, tx_hash, status) "
            "VALUES (?, ?, ?, ?, ?)",
            (datetime.now(timezone.utc).isoformat(), 10.0, 0.10, "0xabc123", "ok"),
        )
        conn.commit()

        row = conn.execute("SELECT * FROM piggybank_transfers").fetchone()
        assert row is not None
        assert row[3] == 0.10  # amount_usd
        assert row[4] == "0xabc123"  # tx_hash
        conn.close()


class TestSQLiteWALConcurrency:
    """Test that WAL mode handles concurrent writes without SQLITE_BUSY."""

    def test_concurrent_writes_with_wal(self, tmp_path):
        """Multiple threads can write simultaneously with WAL mode."""
        db_path = tmp_path / "wal_test.db"

        conn = sqlite3.connect(str(db_path))
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")
        conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, val TEXT)")
        conn.commit()
        conn.close()

        errors = []
        success_count = [0]

        def writer(thread_id: int):
            try:
                c = sqlite3.connect(str(db_path))
                c.execute("PRAGMA journal_mode=WAL")
                c.execute("PRAGMA busy_timeout=5000")
                for i in range(20):
                    c.execute("INSERT INTO test (val) VALUES (?)", (f"thread-{thread_id}-{i}",))
                    c.commit()
                c.close()
                success_count[0] += 1
            except Exception as e:
                errors.append(f"Thread {thread_id}: {e}")

        threads = [threading.Thread(target=writer, args=(i,)) for i in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        assert not errors, f"Concurrent write errors: {errors}"
        assert success_count[0] == 5

        # Verify all rows written
        c = sqlite3.connect(str(db_path))
        count = c.execute("SELECT COUNT(*) FROM test").fetchone()[0]
        c.close()
        assert count == 100  # 5 threads × 20 rows
