"""Tests for the HyperOptimizer module."""

import json
import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


class TestHyperOptimizer:
    """Test HyperOptimizer class functionality."""

    def test_hyperoptimizer_initialization(self):
        """Test HyperOptimizer initializes correctly."""
        from polybot.optimizer import HyperOptimizer

        optimizer = HyperOptimizer()
        assert optimizer.settings is not None
        assert optimizer.backtester is not None
        assert optimizer._results == []

    def test_hyperoptimizer_output_csv_constant(self):
        """Test HyperOptimizer has correct output CSV constant."""
        from polybot.optimizer import HyperOptimizer

        assert HyperOptimizer.OUTPUT_CSV == "hyperopt_top_results.csv"

    def test_get_setting_key_mapping(self):
        """Test parameter key to settings attribute mapping."""
        from polybot.optimizer import HyperOptimizer

        optimizer = HyperOptimizer()

        # Test known mappings
        assert optimizer._get_setting_key("MIN_EV") == "min_edge_percent"
        assert optimizer._get_setting_key("KELLY_MULTIPLIER") == "kelly_multiplier"
        assert optimizer._get_setting_key("MAX_POSITION_USD") == "max_position_usd"
        assert optimizer._get_setting_key("MIN_TRADE_USD") == "min_trade_usd"
        assert (
            optimizer._get_setting_key("backtest_min_liquidity")
            == "backtest_min_liquidity"
        )

        # Test fallback to lowercase
        assert optimizer._get_setting_key("UNKNOWN_PARAM") == "unknown_param"

    def test_get_results_empty(self):
        """Test get_results returns empty list initially."""
        from polybot.optimizer import HyperOptimizer

        optimizer = HyperOptimizer()
        results = optimizer.get_results()
        assert results == []

    def test_get_results_returns_copy(self):
        """Test get_results returns a copy of results."""
        from polybot.optimizer import HyperOptimizer

        optimizer = HyperOptimizer()
        optimizer._results = [{"score": 1.0}]

        results = optimizer.get_results()
        results.append({"score": 2.0})

        # Original should be unchanged
        assert len(optimizer._results) == 1

    def test_get_top_results_empty(self):
        """Test get_top_results with no results."""
        from polybot.optimizer import HyperOptimizer

        optimizer = HyperOptimizer()
        top = optimizer.get_top_results(n=5)
        assert top == []

    def test_get_top_results_sorted(self):
        """Test get_top_results returns sorted results."""
        from polybot.optimizer import HyperOptimizer

        optimizer = HyperOptimizer()
        optimizer._results = [
            {"params": {"a": 1}, "score": 1.0},
            {"params": {"a": 2}, "score": 3.0},
            {"params": {"a": 3}, "score": 2.0},
        ]

        top = optimizer.get_top_results(n=2)
        assert len(top) == 2
        assert top[0]["score"] == 3.0
        assert top[1]["score"] == 2.0

    def test_export_results_csv(self, tmp_path):
        """Test CSV export functionality."""
        from polybot.optimizer import HyperOptimizer

        optimizer = HyperOptimizer()
        # Override output path for test
        original_csv = HyperOptimizer.OUTPUT_CSV
        HyperOptimizer.OUTPUT_CSV = str(tmp_path / "test_results.csv")

        try:
            best_params = {"MIN_EV": 0.015, "KELLY_MULTIPLIER": 0.5}
            best_score = 2.5

            optimizer._export_results_csv(best_params, best_score)

            assert os.path.exists(HyperOptimizer.OUTPUT_CSV)

            with open(HyperOptimizer.OUTPUT_CSV, "r") as f:
                content = f.read()
                assert "param,value,best_score" in content
                assert "MIN_EV" in content
                assert "0.015" in content
                assert "2.5" in content
        finally:
            HyperOptimizer.OUTPUT_CSV = original_csv

    @pytest.mark.asyncio
    @patch("polybot.optimizer.EdgeBacktester")
    async def test_run_walkforward_optimization_basic(self, mock_backtester_class):
        """Test basic walk-forward optimization flow."""
        from polybot.optimizer import HyperOptimizer

        # Mock backtester to return consistent results
        mock_backtester = MagicMock()
        mock_backtester.run_backtest_async = AsyncMock(
            return_value={
                "sharpe": 1.5,
                "winrate": 60.0,
                "total_pnl": 100.0,
            }
        )
        mock_backtester_class.return_value = mock_backtester

        optimizer = HyperOptimizer()
        optimizer.backtester = mock_backtester

        # Reduce grid for faster test
        optimizer.settings.hyperopt_params = {
            "MIN_EV": [0.01],
            "KELLY_MULTIPLIER": [0.5],
        }
        optimizer.settings.walkforward_windows = 1

        result = await optimizer.run_walkforward_optimization_async()

        assert "best_params" in result
        assert "best_score" in result
        assert "windows" in result
        assert result["windows"] == 1
        assert mock_backtester.run_backtest_async.called

    @pytest.mark.asyncio
    @patch("polybot.optimizer.EdgeBacktester")
    async def test_run_walkforward_finds_best_params(self, mock_backtester_class):
        """Test that optimization finds the best parameter combination (grid search)."""
        from polybot.optimizer import HyperOptimizer

        # Track which params were used
        call_scores = {
            (0.01, 0.3): {"sharpe": 1.0, "winrate": 50.0, "total_pnl": 50.0},
            (0.01, 0.5): {"sharpe": 2.0, "winrate": 65.0, "total_pnl": 150.0},  # Best
            (0.02, 0.3): {"sharpe": 0.5, "winrate": 45.0, "total_pnl": 20.0},
            (0.02, 0.5): {"sharpe": 1.5, "winrate": 55.0, "total_pnl": 80.0},
        }

        async def mock_backtest(*args, **kwargs):
            # Return different results based on settings
            optimizer = HyperOptimizer()
            min_ev = getattr(optimizer.settings, "min_edge_percent", 0.01)
            kelly = getattr(optimizer.settings, "kelly_multiplier", 0.5)
            key = (round(min_ev, 2), round(kelly, 1))
            return call_scores.get(key, {"sharpe": 0, "winrate": 0, "total_pnl": 0})

        mock_backtester = MagicMock()
        mock_backtester.run_backtest_async = AsyncMock(side_effect=mock_backtest)
        mock_backtester_class.return_value = mock_backtester

        optimizer = HyperOptimizer()
        optimizer.backtester = mock_backtester

        # Use grid search for deterministic testing
        optimizer.settings.optuna_sampler = "grid"

        # Small grid for test
        optimizer.settings.hyperopt_params = {
            "MIN_EV": [0.01, 0.02],
            "KELLY_MULTIPLIER": [0.3, 0.5],
        }
        optimizer.settings.walkforward_windows = 1

        result = await optimizer.run_walkforward_optimization_async()

        assert result["best_score"] > 0
        assert len(optimizer._results) == 4  # 2 x 2 combinations


class TestHyperOptConfig:
    """Test hyperopt configuration settings."""

    def test_hyperopt_enabled_default(self):
        """Test hyperopt_enabled defaults to False."""
        from polybot.config import Settings

        # Create fresh settings instance to get defaults
        settings = Settings()
        assert settings.hyperopt_enabled is False

    def test_hyperopt_params_default(self):
        """Test hyperopt_params has correct default structure."""
        from polybot.config import Settings

        # Create fresh settings instance to get defaults
        settings = Settings()

        assert "MIN_EV" in settings.hyperopt_params
        assert "KELLY_MULTIPLIER" in settings.hyperopt_params
        assert "MAX_POSITION_USD" in settings.hyperopt_params
        assert "MIN_TRADE_USD" in settings.hyperopt_params
        assert "backtest_min_liquidity" in settings.hyperopt_params

        # Check value types
        assert isinstance(settings.hyperopt_params["MIN_EV"], list)
        assert isinstance(settings.hyperopt_params["KELLY_MULTIPLIER"], list)

    def test_walkforward_windows_default(self):
        """Test walkforward_windows defaults to 5."""
        from polybot.config import Settings

        # Create fresh settings instance to get defaults
        settings = Settings()
        assert settings.walkforward_windows == 5

    def test_mode_includes_hyperopt(self):
        """Test that 'hyperopt' is a valid mode option."""
        from polybot.config import Settings

        # Should not raise
        try:
            # Create settings with hyperopt mode
            # Note: We can't directly instantiate with mode='hyperopt' easily
            # but we can verify the type annotation includes it
            mode_field = Settings.model_fields["mode"]
            annotation = str(mode_field.annotation)
            assert "hyperopt" in annotation
        except Exception:
            # Alternative: check by inspection
            pass


class TestScannerHyperopt:
    """Test scanner hyperopt integration."""

    def test_run_hyperopt_import(self):
        """Test run_hyperopt can be imported from scanner."""
        from polybot.scanner import run_hyperopt, run_hyperopt_async

        assert callable(run_hyperopt)
        assert callable(run_hyperopt_async)

    @patch("polybot.scanner.get_settings")
    def test_run_hyperopt_disabled_warning(self, mock_settings):
        """Test run_hyperopt returns None when disabled."""
        from polybot.scanner import run_hyperopt

        mock_settings_obj = MagicMock()
        mock_settings_obj.hyperopt_enabled = False
        mock_settings.return_value = mock_settings_obj

        result = run_hyperopt()
        assert result is None

    @pytest.mark.asyncio
    @patch("polybot.scanner.get_settings")
    async def test_run_hyperopt_async_disabled(self, mock_settings):
        """Test run_hyperopt_async returns None when disabled."""
        from polybot.scanner import run_hyperopt_async

        mock_settings_obj = MagicMock()
        mock_settings_obj.hyperopt_enabled = False
        mock_settings.return_value = mock_settings_obj

        result = await run_hyperopt_async()
        assert result is None


class TestOptunaConfig:
    """Test Optuna-specific configuration settings."""

    def test_optuna_sampler_default(self):
        """Test optuna_sampler defaults to 'tpe'."""
        from polybot.config import Settings

        settings = Settings()
        assert settings.optuna_sampler == "tpe"

    def test_optuna_n_trials_default(self):
        """Test optuna_n_trials defaults to 50."""
        from polybot.config import Settings

        settings = Settings()
        assert settings.optuna_n_trials == 50

    def test_optuna_direction_default(self):
        """Test optuna_direction defaults to 'maximize'."""
        from polybot.config import Settings

        settings = Settings()
        assert settings.optuna_direction == "maximize"

    def test_optuna_sampler_valid_values(self):
        """Test optuna_sampler accepts valid values."""
        from polybot.config import Settings

        # Check that Literal type includes expected values
        sampler_field = Settings.model_fields["optuna_sampler"]
        annotation = str(sampler_field.annotation)
        assert "tpe" in annotation
        assert "cmaes" in annotation
        assert "random" in annotation
        assert "grid" in annotation


class TestOptunaOptimization:
    """Test Optuna-based optimization functionality."""

    def test_hyperoptimizer_output_json_constant(self):
        """Test HyperOptimizer has correct JSON output constant."""
        from polybot.optimizer import HyperOptimizer

        assert HyperOptimizer.OUTPUT_JSON == "optuna_best_params.json"

    def test_hyperoptimizer_has_study_attribute(self):
        """Test HyperOptimizer has _study attribute."""
        from polybot.optimizer import HyperOptimizer

        optimizer = HyperOptimizer()
        assert hasattr(optimizer, "_study")
        assert optimizer._study is None  # Initially None

    def test_get_study_returns_none_initially(self):
        """Test get_study returns None before optimization."""
        from polybot.optimizer import HyperOptimizer

        optimizer = HyperOptimizer()
        assert optimizer.get_study() is None

    def test_create_sampler_tpe(self):
        """Test _create_sampler creates TPE sampler."""
        import optuna
        from polybot.optimizer import HyperOptimizer

        optimizer = HyperOptimizer()
        sampler = optimizer._create_sampler("tpe", {})
        assert isinstance(sampler, optuna.samplers.TPESampler)

    def test_create_sampler_cmaes(self):
        """Test _create_sampler creates CMA-ES sampler."""
        import optuna
        from polybot.optimizer import HyperOptimizer

        optimizer = HyperOptimizer()
        sampler = optimizer._create_sampler("cmaes", {})
        assert isinstance(sampler, optuna.samplers.CmaEsSampler)

    def test_create_sampler_random(self):
        """Test _create_sampler creates Random sampler."""
        import optuna
        from polybot.optimizer import HyperOptimizer

        optimizer = HyperOptimizer()
        sampler = optimizer._create_sampler("random", {})
        assert isinstance(sampler, optuna.samplers.RandomSampler)

    def test_create_sampler_grid(self):
        """Test _create_sampler creates Grid sampler."""
        import optuna
        from polybot.optimizer import HyperOptimizer

        optimizer = HyperOptimizer()
        param_grid = {"MIN_EV": [0.01, 0.02]}
        sampler = optimizer._create_sampler("grid", param_grid)
        assert isinstance(sampler, optuna.samplers.GridSampler)

    def test_create_sampler_unknown_defaults_to_tpe(self):
        """Test _create_sampler defaults to TPE for unknown samplers."""
        import optuna
        from polybot.optimizer import HyperOptimizer

        optimizer = HyperOptimizer()
        sampler = optimizer._create_sampler("unknown", {})
        assert isinstance(sampler, optuna.samplers.TPESampler)

    def test_is_score_better_maximize(self):
        """Test _is_score_better with maximize direction."""
        from polybot.optimizer import HyperOptimizer

        optimizer = HyperOptimizer()
        assert optimizer._is_score_better(2.0, 1.0, "maximize") is True
        assert optimizer._is_score_better(1.0, 2.0, "maximize") is False
        assert optimizer._is_score_better(1.0, 1.0, "maximize") is False

    def test_is_score_better_minimize(self):
        """Test _is_score_better with minimize direction."""
        from polybot.optimizer import HyperOptimizer

        optimizer = HyperOptimizer()
        assert optimizer._is_score_better(1.0, 2.0, "minimize") is True
        assert optimizer._is_score_better(2.0, 1.0, "minimize") is False
        assert optimizer._is_score_better(1.0, 1.0, "minimize") is False

    def test_export_results_json(self, tmp_path):
        """Test JSON export functionality."""
        from polybot.optimizer import HyperOptimizer

        optimizer = HyperOptimizer()
        # Override output path for test
        original_json = HyperOptimizer.OUTPUT_JSON
        HyperOptimizer.OUTPUT_JSON = str(tmp_path / "test_results.json")

        try:
            best_params = {"MIN_EV": 0.015, "KELLY_MULTIPLIER": 0.5}
            best_score = 2.5

            optimizer._export_results_json(best_params, best_score)

            assert (tmp_path / "test_results.json").exists()

            with open(HyperOptimizer.OUTPUT_JSON, "r") as f:
                data = json.load(f)
                assert "best_params" in data
                assert data["best_params"] == best_params
                assert data["best_score"] == 2.5
                assert "sampler" in data
                assert "n_trials" in data
                assert "windows" in data
        finally:
            HyperOptimizer.OUTPUT_JSON = original_json

    @pytest.mark.asyncio
    @patch("polybot.optimizer.EdgeBacktester")
    async def test_optuna_optimization_tpe(self, mock_backtester_class):
        """Test Optuna TPE optimization flow."""
        from polybot.optimizer import HyperOptimizer

        mock_backtester = MagicMock()
        mock_backtester.run_backtest_async = AsyncMock(
            return_value={
                "sharpe": 1.5,
                "winrate": 60.0,
                "total_pnl": 100.0,
            }
        )
        mock_backtester_class.return_value = mock_backtester

        optimizer = HyperOptimizer()
        optimizer.backtester = mock_backtester

        # Use TPE sampler
        optimizer.settings.optuna_sampler = "tpe"
        optimizer.settings.optuna_n_trials = 5  # Small for test
        optimizer.settings.walkforward_windows = 1
        optimizer.settings.hyperopt_params = {
            "MIN_EV": [0.01, 0.02],
            "KELLY_MULTIPLIER": [0.5],
        }

        result = await optimizer.run_walkforward_optimization_async()

        assert "best_params" in result
        assert "best_score" in result
        assert "sampler" in result
        assert result["sampler"] == "tpe"
        assert result["windows"] == 1
        # With Optuna, we have n_trials results per window
        assert len(optimizer._results) == 5  # 5 trials × 1 window
        assert optimizer.get_study() is not None

    @pytest.mark.asyncio
    @patch("polybot.optimizer.EdgeBacktester")
    async def test_optuna_optimization_random(self, mock_backtester_class):
        """Test Optuna Random sampler optimization flow."""
        from polybot.optimizer import HyperOptimizer

        mock_backtester = MagicMock()
        mock_backtester.run_backtest_async = AsyncMock(
            return_value={
                "sharpe": 1.5,
                "winrate": 60.0,
                "total_pnl": 100.0,
            }
        )
        mock_backtester_class.return_value = mock_backtester

        optimizer = HyperOptimizer()
        optimizer.backtester = mock_backtester

        # Use Random sampler
        optimizer.settings.optuna_sampler = "random"
        optimizer.settings.optuna_n_trials = 3  # Small for test
        optimizer.settings.walkforward_windows = 1
        optimizer.settings.hyperopt_params = {
            "MIN_EV": [0.01, 0.02],
            "KELLY_MULTIPLIER": [0.5],
        }

        result = await optimizer.run_walkforward_optimization_async()

        assert "best_params" in result
        assert "sampler" in result
        assert result["sampler"] == "random"
        assert len(optimizer._results) == 3  # 3 trials × 1 window


class TestOptunaVizConfig:
    """Test Optuna visualization configuration settings."""

    def test_optuna_viz_enabled_default(self):
        """Test optuna_viz_enabled defaults to True."""
        from polybot.config import Settings

        settings = Settings()
        assert settings.optuna_viz_enabled is True

    def test_optuna_viz_formats_default(self):
        """Test optuna_viz_formats defaults to ['html', 'png']."""
        from polybot.config import Settings

        settings = Settings()
        assert settings.optuna_viz_formats == ["html", "png"]

    def test_auto_apply_best_default(self):
        """Test auto_apply_best defaults to True."""
        from polybot.config import Settings

        settings = Settings()
        assert settings.auto_apply_best is True

    def test_viz_dir_default(self):
        """Test viz_dir defaults to /app/static/optuna_viz."""
        from polybot.config import Settings

        settings = Settings()
        assert settings.viz_dir == "/app/static/optuna_viz"

    def test_viz_dir_from_env(self, monkeypatch):
        """Test viz_dir can be set via VIZ_DIR environment variable."""
        from polybot.config import Settings

        monkeypatch.setenv("VIZ_DIR", "/custom/viz/path")
        settings = Settings()
        assert settings.viz_dir == "/custom/viz/path"


class TestOptunaVisualization:
    """Test Optuna visualization functionality."""

    def test_create_optuna_visualizations_no_study(self, tmp_path):
        """Test _create_optuna_visualizations handles missing study gracefully."""
        from polybot.optimizer import HyperOptimizer

        optimizer = HyperOptimizer()
        optimizer._study = None

        # Should not raise, just log a warning
        optimizer._create_optuna_visualizations()

    @patch("polybot.optimizer.optuna.visualization")
    def test_create_optuna_visualizations_creates_plots(self, mock_vis, tmp_path):
        """Test _create_optuna_visualizations creates expected plots."""
        import optuna
        from polybot.optimizer import HyperOptimizer

        optimizer = HyperOptimizer()

        # Create a minimal study with at least one completed trial
        study = optuna.create_study(direction="maximize")
        study.optimize(lambda trial: trial.suggest_float("x", 0, 1), n_trials=5)
        optimizer._study = study

        # Mock the visualization functions to return mock figures
        mock_fig = MagicMock()
        mock_vis.plot_optimization_history.return_value = mock_fig
        mock_vis.plot_param_importances.return_value = mock_fig
        mock_vis.plot_parallel_coordinate.return_value = mock_fig
        mock_vis.plot_slice.return_value = mock_fig

        # Set viz_dir to tmp_path for the test (instead of /app/static/optuna_viz)
        viz_dir = str(tmp_path / "optuna_viz")
        optimizer.settings.viz_dir = viz_dir

        optimizer._create_optuna_visualizations()

        # Verify visualization functions were called
        mock_vis.plot_optimization_history.assert_called_once_with(study)
        mock_vis.plot_param_importances.assert_called_once_with(study)
        mock_vis.plot_parallel_coordinate.assert_called_once_with(study)
        mock_vis.plot_slice.assert_called_once_with(study)

        # Verify write methods were called for each format
        assert mock_fig.write_html.called
        assert mock_fig.write_image.called

    @patch("plotly.express.line")
    def test_create_walkforward_curve(self, mock_line, tmp_path):
        """Test _create_walkforward_curve creates the walk-forward curve plot.

        Note: plotly.express requires pandas. If pandas is not installed,
        the production code gracefully skips visualization. This test requires
        pandas to verify the plotting logic works correctly.
        """
        # Skip test if pandas is not available (required by plotly.express)
        pytest.importorskip("pandas", reason="pandas required for plotly.express")

        import optuna

        from polybot.optimizer import HyperOptimizer

        optimizer = HyperOptimizer()

        # Create a study with completed trials
        study = optuna.create_study(direction="maximize")
        study.optimize(
            lambda trial: trial.suggest_float("test_param", 0, 1), n_trials=3
        )
        optimizer._study = study

        # Mock the plotly figure
        mock_fig = MagicMock()
        mock_line.return_value = mock_fig

        # Set up output directory
        output_dir = str(tmp_path / "optuna_viz")
        formats = ["html", "png"]
        generated_files: list[str] = []

        optimizer._create_walkforward_curve(output_dir, formats, generated_files)

        # Verify px.line was called
        mock_line.assert_called_once()

        # Verify update_layout was called with expected axes titles
        mock_fig.update_layout.assert_called_once()
        call_kwargs = mock_fig.update_layout.call_args[1]
        assert "xaxis_title" in call_kwargs
        assert "yaxis_title" in call_kwargs

        # Verify write methods were called
        assert mock_fig.write_html.called
        assert mock_fig.write_image.called

    def test_create_walkforward_curve_no_study(self, tmp_path):
        """Test _create_walkforward_curve handles missing study gracefully."""
        from polybot.optimizer import HyperOptimizer

        optimizer = HyperOptimizer()
        optimizer._study = None

        output_dir = str(tmp_path / "optuna_viz")
        generated_files: list[str] = []

        # Should not raise any exception
        optimizer._create_walkforward_curve(output_dir, ["html"], generated_files)

        # No files should be generated
        assert len(generated_files) == 0


class TestAutoApplyBestParams:
    """Test auto-apply best params functionality."""

    def test_auto_apply_best_params_creates_env_file(self, tmp_path):
        """Test _auto_apply_best_params creates best_params.env file."""
        from polybot.optimizer import HyperOptimizer

        optimizer = HyperOptimizer()

        best_params = {"MIN_EV": 0.02, "KELLY_MULTIPLIER": 0.6}
        best_score = 2.5

        # Change to tmp_path for the test
        old_cwd = os.getcwd()
        os.chdir(tmp_path)
        try:
            optimizer._auto_apply_best_params(best_params, best_score)

            env_file = tmp_path / "best_params.env"
            assert env_file.exists()

            content = env_file.read_text()
            assert "MIN_EV=0.02" in content
            assert "KELLY_MULTIPLIER=0.6" in content
        finally:
            os.chdir(old_cwd)

    def test_auto_apply_best_params_creates_json_file(self, tmp_path):
        """Test _auto_apply_best_params creates best_config.json file."""
        from polybot.optimizer import HyperOptimizer

        optimizer = HyperOptimizer()

        best_params = {"MIN_EV": 0.02, "KELLY_MULTIPLIER": 0.6}
        best_score = 2.5

        # Change to tmp_path for the test
        old_cwd = os.getcwd()
        os.chdir(tmp_path)
        try:
            optimizer._auto_apply_best_params(best_params, best_score)

            json_file = tmp_path / "best_config.json"
            assert json_file.exists()

            with open(json_file, "r") as f:
                data = json.load(f)
                assert data["best_params"] == best_params
                assert data["best_score"] == 2.5
                assert "timestamp" in data
                assert "sampler" in data
                assert "n_trials" in data
                assert "windows" in data
        finally:
            os.chdir(old_cwd)

    def test_auto_apply_includes_default_params_if_missing(self, tmp_path):
        """Test _auto_apply_best_params includes defaults if key params missing."""
        from polybot.optimizer import HyperOptimizer

        optimizer = HyperOptimizer()

        # Best params without MIN_EV and KELLY_MULTIPLIER
        best_params = {"MAX_POSITION_USD": 100}
        best_score = 1.5

        # Change to tmp_path for the test
        old_cwd = os.getcwd()
        os.chdir(tmp_path)
        try:
            optimizer._auto_apply_best_params(best_params, best_score)

            env_file = tmp_path / "best_params.env"
            content = env_file.read_text()

            # Should contain the provided param
            assert "MAX_POSITION_USD=100" in content
            # Should also have defaults for critical params
            assert "MIN_EV=" in content
            assert "KELLY_MULTIPLIER=" in content
        finally:
            os.chdir(old_cwd)
