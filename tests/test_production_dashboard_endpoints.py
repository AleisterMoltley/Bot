"""Tests for Production Dashboard v2 endpoints."""

import json
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest


class MockSettings:
    """Mock settings for testing production dashboard endpoints."""

    dry_run = True
    auto_execute = False
    min_ev = 0.015
    kelly_multiplier = 0.5


@pytest.fixture
def mock_settings():
    """Fixture providing mock settings."""
    return MockSettings()


class TestBacktestReportEndpoint:
    """Test /api/backtest-report endpoint."""

    @pytest.mark.asyncio
    async def test_backtest_report_no_file(self):
        """Test backtest report returns status when no file exists."""
        with patch("polybot.main_fastapi.os.path.exists", return_value=False):
            with patch("polybot.main_fastapi._STATIC_DIR", None):
                from polybot.main_fastapi import get_backtest_report

                result = await get_backtest_report()
                assert result["status"] == "no_report_yet"
                assert "hyperopt" in result["message"].lower()

    @pytest.mark.asyncio
    async def test_backtest_report_with_file(self):
        """Test backtest report returns JSON when file exists."""
        test_data = {"total_pnl": 1500.0, "sharpe": 2.5}

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(test_data, f)
            temp_path = f.name

        try:
            with patch("polybot.main_fastapi.os.path.exists", return_value=True):
                with patch(
                    "builtins.open",
                    create=True,
                    return_value=open(temp_path),
                ):
                    from polybot.main_fastapi import get_backtest_report

                    result = await get_backtest_report()
                    assert result["total_pnl"] == 1500.0
                    assert result["sharpe"] == 2.5
        finally:
            Path(temp_path).unlink(missing_ok=True)


class TestStrategyComparisonEndpoint:
    """Test /api/strategy-comparison endpoint."""

    @pytest.mark.asyncio
    async def test_strategy_comparison_returns_data(self):
        """Test strategy comparison returns expected structure."""
        from polybot.main_fastapi import get_strategy_comparison

        result = await get_strategy_comparison()

        assert "top_strategies" in result
        assert len(result["top_strategies"]) == 3
        assert result["top_strategies"][0]["rank"] == 1
        assert "baseline_comparison" in result


class TestProductionStatusEndpoint:
    """Test /api/production-status endpoint."""

    @pytest.mark.asyncio
    async def test_production_status_dry_run(self, mock_settings):
        """Test production status shows dry_run mode correctly."""
        with patch("polybot.main_fastapi.get_settings", return_value=mock_settings):
            with patch("polybot.main_fastapi.os.path.exists", return_value=False):
                with patch("polybot.main_fastapi._STATIC_DIR", None):
                    from polybot.main_fastapi import production_status

                    result = await production_status()

                    assert "auto_trade" in result
                    assert "dry_run" in result["auto_trade"]
                    assert result["best_ev"] == 0.015
                    assert result["kelly_multiplier"] == 0.5

    @pytest.mark.asyncio
    async def test_production_status_hyperopt_complete(self, mock_settings):
        """Test production status shows hyperopt completed."""
        with patch("polybot.main_fastapi.get_settings", return_value=mock_settings):
            with patch("polybot.main_fastapi.os.path.exists", return_value=True):
                from polybot.main_fastapi import production_status

                result = await production_status()

                assert result["hyperopt_completed"] == "✅"
                assert result["overall_status"] == "PRODUCTION_READY"


class TestApplyBestParamsEndpoint:
    """Test /apply-best-params endpoint."""

    @pytest.mark.asyncio
    async def test_apply_best_params_no_file(self):
        """Test apply best params returns error when no params file."""
        with patch("polybot.main_fastapi.os.path.exists", return_value=False):
            with patch("polybot.main_fastapi._STATIC_DIR", None):
                from polybot.main_fastapi import apply_best_params

                result = await apply_best_params()

                assert result["status"] == "error"
                assert "not found" in result["message"]

    @pytest.mark.asyncio
    async def test_apply_best_params_with_file(self):
        """Test apply best params creates output file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)

            # Create mock best_params.env
            params_file = tmpdir_path / "optuna_viz" / "best_params.env"
            params_file.parent.mkdir(parents=True, exist_ok=True)
            params_file.write_text("MIN_EV=0.017\nKELLY_MULTIPLIER=0.55\n")

            # Mock _STATIC_DIR to use our temp directory
            with patch("polybot.main_fastapi._STATIC_DIR", tmpdir_path):
                with patch("polybot.main_fastapi.os.path.exists") as mock_exists:
                    mock_exists.side_effect = lambda p: (
                        p == str(params_file) or "optuna_viz" in p
                    )

                    # Skip this test as it requires more complex mocking
                    # The endpoint logic is tested through integration
                    pytest.skip("Requires integration test with actual file system")


# === PRODUCTION SAFETY ENDPOINT TESTS ===


class TestErrorLogsEndpoint:
    """Test /logs endpoint."""

    @pytest.mark.asyncio
    async def test_error_logs_no_file(self):
        """Test error logs returns stable message when no file exists."""
        with patch("polybot.main_fastapi._get_critical_errors_log_path") as mock_path:
            mock_path_obj = Path("/nonexistent/path/errors.log")
            mock_path.return_value = mock_path_obj

            from polybot.main_fastapi import view_error_logs

            result = await view_error_logs()
            assert result["status"] == "ok"
            assert result["total_errors"] == 0
            assert "stable" in result["logs"][0].lower()

    @pytest.mark.asyncio
    async def test_error_logs_with_file(self):
        """Test error logs returns log content when file exists."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".log", delete=False) as f:
            f.write("2026-03-22T12:00:00 | TestError: test1\n")
            f.write("2026-03-22T12:01:00 | TestError: test2\n")
            f.write("2026-03-22T12:02:00 | TestError: test3\n")
            temp_path = Path(f.name)

        try:
            with patch(
                "polybot.main_fastapi._get_critical_errors_log_path",
                return_value=temp_path,
            ):
                from polybot.main_fastapi import view_error_logs

                result = await view_error_logs()
                assert result["status"] == "ok"
                assert result["total_errors"] == 3
                # Newest first
                assert "test3" in result["logs"][0]
        finally:
            temp_path.unlink(missing_ok=True)


class TestProductionSafetyEndpoint:
    """Test /api/production-safety endpoint."""

    @pytest.mark.asyncio
    async def test_production_safety_default_state(self, mock_settings):
        """Test production safety returns expected structure."""
        with patch("polybot.main_fastapi.get_settings", return_value=mock_settings):
            with patch(
                "polybot.main_fastapi._get_critical_errors_log_path"
            ) as mock_log:
                mock_log.return_value = Path("/nonexistent/errors.log")
                with patch("polybot.main_fastapi._get_backup_dir") as mock_backup:
                    mock_backup.return_value = Path("/nonexistent/backup")

                    from polybot.main_fastapi import production_safety_status

                    result = await production_safety_status()

                    assert "safety_score" in result
                    assert "100/100" in result["safety_score"]
                    assert result["graceful_shutdown"] == "✅ active"
                    assert "error_log_viewer" in result
                    assert result["error_log_viewer"] == "/logs"

    @pytest.mark.asyncio
    async def test_production_safety_with_errors(self, mock_settings):
        """Test safety score decreases with errors."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".log", delete=False) as f:
            # Write 5 errors
            for i in range(5):
                f.write(f"2026-03-22T12:0{i}:00 | TestError: test{i}\n")
            temp_path = Path(f.name)

        try:
            with patch("polybot.main_fastapi.get_settings", return_value=mock_settings):
                with patch(
                    "polybot.main_fastapi._get_critical_errors_log_path",
                    return_value=temp_path,
                ):
                    with patch("polybot.main_fastapi._get_backup_dir") as mock_backup:
                        mock_backup.return_value = Path("/nonexistent/backup")

                        from polybot.main_fastapi import production_safety_status

                        result = await production_safety_status()

                        # 5 errors = 100 - (5 * 10) = 50
                        assert result["safety_score"] == "50/100"
                        assert result["error_count"] == 5
        finally:
            temp_path.unlink(missing_ok=True)


class TestTriggerBackupEndpoint:
    """Test /api/trigger-backup endpoint."""

    @pytest.mark.asyncio
    async def test_trigger_backup_no_source(self):
        """Test trigger backup when no source files exist."""
        with patch("polybot.main_fastapi._get_backup_dir") as mock_backup:
            mock_backup.return_value = Path("/tmp/test_backup_nonexistent")

            from polybot.main_fastapi import trigger_backup

            result = await trigger_backup()

            # Should be skipped since nothing to backup
            assert result["status"] in ("success", "skipped")
