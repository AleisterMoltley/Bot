"""Walk-Forward Hyperparameter Optimizer für Edge-Strategien.

Supports both Optuna-based optimization (TPE, CMA-ES, Random) and traditional
Grid-Search. Uses rolling Walk-Forward windows for validation.
Nutzt den EdgeBacktester für Backtests. Speichert Top-Results + CSV/JSON-Export.
Unterstützt Plotly Visualisierungen und Auto-Apply der besten Parameter.
"""

from __future__ import annotations

import csv
import json
import os
from datetime import datetime
from itertools import product
from typing import Any

import optuna

from polybot.config import get_settings
from polybot.backtester import EdgeBacktester
from polybot.logging_setup import get_logger

logger = get_logger(__name__)

# Suppress Optuna's verbose logging (use our own structured logs)
optuna.logging.set_verbosity(optuna.logging.WARNING)


class HyperOptimizer:
    """Walk-Forward Hyperparameter Optimizer.

    Supports multiple optimization algorithms via Optuna:
    - TPE (Tree-structured Parzen Estimator): Bayesian optimization (recommended)
    - CMA-ES: Evolutionary strategy for continuous parameters
    - Random: Random search baseline
    - Grid: Traditional grid search (original behavior)

    Features:
    - Grid search over MIN_EV, KELLY_MULTIPLIER, MAX_POSITION_USD, etc.
    - Walk-forward validation with rolling windows
    - Composite scoring: Sharpe × Winrate + PNL contribution
    - CSV and JSON export of top results
    """

    # Output files for results
    OUTPUT_CSV = "hyperopt_top_results.csv"
    OUTPUT_JSON = "optuna_best_params.json"

    def __init__(self) -> None:
        """Initialize HyperOptimizer with settings and backtester."""
        self.settings = get_settings()
        self.backtester = EdgeBacktester()
        self._results: list[dict[str, Any]] = []
        self._study: optuna.Study | None = None

    async def run_walkforward_optimization_async(self) -> dict[str, Any]:
        """Run walk-forward optimization with Optuna or grid search.

        Uses the configured sampler (TPE, CMA-ES, Random, or Grid) to find
        optimal parameters across multiple time windows.

        Returns:
            Dict with best_params, best_score, and windows count
        """
        sampler_type = self.settings.optuna_sampler

        # For grid search, use the original implementation for backward compatibility
        if sampler_type == "grid":
            return await self._run_grid_search_async()

        # Use Optuna for TPE, CMA-ES, or Random sampling
        return await self._run_optuna_optimization_async()

    async def _run_optuna_optimization_async(self) -> dict[str, Any]:
        """Run Optuna-based optimization with TPE, CMA-ES, or Random sampler.

        Returns:
            Dict with best_params, best_score, and windows count
        """
        sampler_type = self.settings.optuna_sampler
        param_grid = self.settings.hyperopt_params
        windows = self.settings.walkforward_windows
        base_days = self.settings.backtest_days
        n_trials = self.settings.optuna_n_trials
        direction = self.settings.optuna_direction

        # Create the appropriate sampler
        sampler = self._create_sampler(sampler_type, param_grid)

        # Create Optuna study
        self._study = optuna.create_study(
            direction=direction,
            sampler=sampler,
            study_name="polybot_hyperopt",
        )

        best_score = float("-inf") if direction == "maximize" else float("inf")
        best_params: dict[str, Any] = {}
        param_names = list(param_grid.keys())

        logger.info(
            "OPTUNA HYPEROPT START",
            sampler=sampler_type,
            n_trials=n_trials,
            windows=windows,
            direction=direction,
            param_names=param_names,
        )

        # Walk-Forward: iterate through rolling windows
        for window_idx in range(windows):
            window_days = base_days + window_idx * 7
            # Track window-specific best for accurate logging
            window_best_params: dict[str, Any] = {}
            window_best_score = (
                float("-inf") if direction == "maximize" else float("inf")
            )

            logger.info(
                f"OPTUNA Window {window_idx + 1}/{windows}",
                days=window_days,
                sampler=sampler_type,
            )

            # Define objective function for this window
            async def objective_async(trial: optuna.Trial) -> float:
                # Sample parameters using Optuna
                params_dict = {}
                for param_name in param_names:
                    values = param_grid[param_name]
                    params_dict[param_name] = trial.suggest_categorical(
                        param_name, values
                    )

                # Apply parameters temporarily to settings
                original_values = {}
                for param_key, param_value in params_dict.items():
                    setting_key = self._get_setting_key(param_key)
                    if setting_key and hasattr(self.settings, setting_key):
                        original_values[setting_key] = getattr(
                            self.settings, setting_key
                        )
                        setattr(self.settings, setting_key, param_value)

                try:
                    # Run backtest with current parameters
                    results = await self.backtester.run_backtest_async(
                        days=window_days,
                        min_liquidity=params_dict.get(
                            "backtest_min_liquidity",
                            self.settings.backtest_min_liquidity,
                        ),
                    )

                    # Calculate composite score
                    sharpe = results.get("sharpe", 0)
                    winrate = results.get("winrate", 0)
                    total_pnl = results.get("total_pnl", 0)
                    score = sharpe * (winrate / 100) + (total_pnl / 1000)

                    # Track result
                    self._results.append(
                        {
                            "window": window_idx + 1,
                            "params": params_dict.copy(),
                            "sharpe": sharpe,
                            "winrate": winrate,
                            "total_pnl": total_pnl,
                            "score": round(score, 4),
                            "trial": trial.number,
                        }
                    )

                    return score

                finally:
                    # Restore original settings
                    for setting_key, original_value in original_values.items():
                        setattr(self.settings, setting_key, original_value)

            # Run trials for this window
            # Note: Optuna's optimize() is synchronous, so we use a wrapper
            for trial_idx in range(n_trials):
                trial = self._study.ask()
                try:
                    score = await objective_async(trial)
                    self._study.tell(trial, score)

                    # Update window-specific best
                    if self._is_score_better(score, window_best_score, direction):
                        window_best_score = score
                        window_best_params = {
                            param_name: trial.params[param_name]
                            for param_name in param_names
                        }

                    # Update global best if improved
                    if self._is_score_better(score, best_score, direction):
                        best_score = score
                        best_params = {
                            param_name: trial.params[param_name]
                            for param_name in param_names
                        }
                        logger.info(
                            f"NEW BEST @ Window {window_idx + 1} Trial {trial_idx + 1}",
                            params=best_params,
                            score=f"{score:.4f}",
                        )
                except Exception as e:
                    self._study.tell(trial, state=optuna.trial.TrialState.FAIL)
                    logger.warning(f"Trial {trial_idx + 1} failed: {e}")

            # Log window summary (use window-specific best, not global study best)
            logger.info(
                f"Window {window_idx + 1} BEST",
                params=window_best_params,
                score=f"{window_best_score:.3f}",
            )

        # Export results
        self._export_results_csv(best_params, best_score)
        self._export_results_json(best_params, best_score)

        # === PLOTLY VISUALISIERUNGEN ===
        if self.settings.optuna_viz_enabled and self._study is not None:
            self._create_optuna_visualizations()

        # === AUTO-APPLY BEST PARAMS ===
        if self.settings.auto_apply_best:
            self._auto_apply_best_params(best_params, best_score)

        logger.info(
            "OPTUNA OPTIMIZATION COMPLETE",
            best_params=best_params,
            best_score=round(best_score, 4),
            total_evaluations=len(self._results),
        )

        return {
            "best_params": best_params,
            "best_score": round(best_score, 4),
            "windows": windows,
            "total_evaluations": len(self._results),
            "sampler": self.settings.optuna_sampler,
        }

    def _create_sampler(
        self, sampler_type: str, param_grid: dict[str, list]
    ) -> optuna.samplers.BaseSampler:
        """Create the appropriate Optuna sampler.

        Args:
            sampler_type: One of 'tpe', 'cmaes', 'random', 'grid'
            param_grid: Parameter grid for GridSampler

        Returns:
            Configured Optuna sampler
        """
        if sampler_type == "tpe":
            return optuna.samplers.TPESampler(seed=42)
        elif sampler_type == "cmaes":
            return optuna.samplers.CmaEsSampler(seed=42)
        elif sampler_type == "random":
            return optuna.samplers.RandomSampler(seed=42)
        elif sampler_type == "grid":
            return optuna.samplers.GridSampler(param_grid)
        else:
            logger.warning(f"Unknown sampler '{sampler_type}', defaulting to TPE")
            return optuna.samplers.TPESampler(seed=42)

    def _is_score_better(self, score: float, best_score: float, direction: str) -> bool:
        """Check if score is better than best_score based on direction.

        Args:
            score: Current score to evaluate
            best_score: Best score so far
            direction: 'maximize' or 'minimize'

        Returns:
            True if score is better than best_score
        """
        if direction == "maximize":
            return score > best_score
        return score < best_score

    async def _run_grid_search_async(self) -> dict[str, Any]:
        """Run traditional grid search optimization (original behavior).

        Returns:
            Dict with best_params, best_score, and windows count
        """
        param_grid = self.settings.hyperopt_params
        windows = self.settings.walkforward_windows
        base_days = self.settings.backtest_days

        best_score = float("-inf")
        best_params: dict[str, Any] = {}

        # Generate all parameter combinations
        param_names = list(param_grid.keys())
        param_values = [param_grid[k] for k in param_names]
        param_combos = list(product(*param_values))

        logger.info(
            "HYPEROPT START",
            total_combinations=len(param_combos),
            windows=windows,
            param_names=param_names,
        )

        # Walk-Forward: iterate through rolling windows
        for window_idx in range(windows):
            # Calculate days for this window (rolling forward)
            window_days = base_days + window_idx * 7

            logger.info(f"HYPEROPT Window {window_idx + 1}/{windows}", days=window_days)

            for combo in param_combos:
                params_dict = dict(zip(param_names, combo))

                # Apply parameters temporarily to settings
                original_values = {}
                for param_key, param_value in params_dict.items():
                    # Handle different parameter name mappings
                    setting_key = self._get_setting_key(param_key)
                    if setting_key and hasattr(self.settings, setting_key):
                        original_values[setting_key] = getattr(
                            self.settings, setting_key
                        )
                        setattr(self.settings, setting_key, param_value)

                try:
                    # Run backtest with current parameters
                    results = await self.backtester.run_backtest_async(
                        days=window_days,
                        min_liquidity=params_dict.get(
                            "backtest_min_liquidity",
                            self.settings.backtest_min_liquidity,
                        ),
                    )

                    # Calculate composite score
                    # Score = Sharpe × (Winrate / 100) + (PNL / 1000)
                    # This balances risk-adjusted returns with absolute performance
                    sharpe = results.get("sharpe", 0)
                    winrate = results.get("winrate", 0)
                    total_pnl = results.get("total_pnl", 0)

                    score = sharpe * (winrate / 100) + (total_pnl / 1000)

                    # Track result
                    self._results.append(
                        {
                            "window": window_idx + 1,
                            "params": params_dict.copy(),
                            "sharpe": sharpe,
                            "winrate": winrate,
                            "total_pnl": total_pnl,
                            "score": round(score, 4),
                        }
                    )

                    if score > best_score:
                        best_score = score
                        best_params = params_dict.copy()
                        logger.info(
                            f"NEW BEST @ Window {window_idx + 1}",
                            params=params_dict,
                            sharpe=f"{sharpe:.2f}",
                            winrate=f"{winrate:.1f}%",
                            score=f"{score:.4f}",
                        )

                finally:
                    # Restore original settings
                    for setting_key, original_value in original_values.items():
                        setattr(self.settings, setting_key, original_value)

        # Export top results to CSV
        self._export_results_csv(best_params, best_score)

        logger.info(
            "HYPEROPT COMPLETE",
            best_params=best_params,
            best_score=round(best_score, 4),
            total_evaluations=len(self._results),
        )

        return {
            "best_params": best_params,
            "best_score": round(best_score, 4),
            "windows": windows,
            "total_evaluations": len(self._results),
        }

    def _get_setting_key(self, param_key: str) -> str | None:
        """Map parameter key to settings attribute name.

        Args:
            param_key: Key from hyperopt_params dict

        Returns:
            Corresponding settings attribute name, or None if not found
        """
        # Direct mappings (case-insensitive)
        mappings = {
            "MIN_EV": "min_edge_percent",  # Maps to min_edge as percentage
            "KELLY_MULTIPLIER": "kelly_multiplier",
            "MAX_POSITION_USD": "max_position_usd",
            "MIN_TRADE_USD": "min_trade_usd",
            "backtest_min_liquidity": "backtest_min_liquidity",
        }
        return mappings.get(param_key, param_key.lower())

    def _export_results_csv(
        self, best_params: dict[str, Any], best_score: float
    ) -> None:
        """Export hyperopt results to CSV file.

        Args:
            best_params: Best parameter combination found
            best_score: Score achieved by best params
        """
        try:
            with open(self.OUTPUT_CSV, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["param", "value", "best_score"])
                for param_key, param_value in best_params.items():
                    writer.writerow([param_key, param_value, round(best_score, 4)])

            logger.info(f"Hyperopt results exported to {self.OUTPUT_CSV}")
        except Exception as e:
            logger.error(f"Failed to export hyperopt results: {e}")

    def _export_results_json(
        self, best_params: dict[str, Any], best_score: float
    ) -> None:
        """Export hyperopt results to JSON file (Optuna format).

        Args:
            best_params: Best parameter combination found
            best_score: Score achieved by best params
        """
        try:
            result_data = {
                "best_params": best_params,
                "best_score": round(best_score, 4),
                "sampler": self.settings.optuna_sampler,
                "n_trials": self.settings.optuna_n_trials,
                "windows": self.settings.walkforward_windows,
                "total_evaluations": len(self._results),
            }
            with open(self.OUTPUT_JSON, "w") as f:
                json.dump(result_data, f, indent=2)

            logger.info(f"Optuna results exported to {self.OUTPUT_JSON}")
        except Exception as e:
            logger.error(f"Failed to export JSON results: {e}")

    def get_results(self) -> list[dict[str, Any]]:
        """Get all evaluation results.

        Returns:
            List of dicts with window, params, sharpe, winrate, total_pnl, score
        """
        return self._results.copy()

    def get_top_results(self, n: int = 10) -> list[dict[str, Any]]:
        """Get top N results by score.

        Args:
            n: Number of top results to return

        Returns:
            List of top N results sorted by score descending
        """
        sorted_results = sorted(
            self._results, key=lambda x: x.get("score", 0), reverse=True
        )
        return sorted_results[:n]

    def get_study(self) -> optuna.Study | None:
        """Get the Optuna study object (if using Optuna optimization).

        Returns:
            The Optuna study, or None if not using Optuna
        """
        return self._study

    def _create_optuna_visualizations(self) -> None:
        """Create Plotly visualizations for Optuna study results.

        Generates interactive HTML and/or static PNG plots:
        - optimization_history: Score evolution across trials
        - param_importances: Which parameters impact the score most
        - parallel_coordinate: Multi-dimensional parameter relationships
        - slice: Individual parameter effect on score
        - walkforward_curve: Best score over trials (Walk-Forward curve)
        """
        if self._study is None:
            logger.warning("No Optuna study available for visualization")
            return

        try:
            import optuna.visualization as vis

            # Use configurable output directory (persistent on Railway)
            output_dir = self.settings.viz_dir
            os.makedirs(output_dir, exist_ok=True)

            # Define plot generators - each can fail independently
            plot_generators = {
                "optimization_history": lambda: vis.plot_optimization_history(
                    self._study
                ),
                "parallel_coordinate": lambda: vis.plot_parallel_coordinate(
                    self._study
                ),
                "slice": lambda: vis.plot_slice(self._study),
                # param_importances requires sklearn, may fail if not installed
                "param_importances": lambda: vis.plot_param_importances(self._study),
            }

            formats = self.settings.optuna_viz_formats
            generated_files: list[str] = []
            failed_plots: list[str] = []

            for name, generator in plot_generators.items():
                try:
                    fig = generator()
                    for fmt in formats:
                        filepath = f"{output_dir}/{name}.{fmt}"
                        try:
                            if fmt == "html":
                                fig.write_html(filepath)
                                generated_files.append(filepath)
                            elif fmt == "png":
                                fig.write_image(filepath)
                                generated_files.append(filepath)
                        except Exception as e:
                            logger.warning(f"Failed to write {filepath}: {e}")
                except ImportError as e:
                    # Some plots require optional dependencies like sklearn
                    failed_plots.append(f"{name} (missing dependency)")
                    logger.debug(f"Skipping {name} plot: {e}")
                except Exception as e:
                    failed_plots.append(name)
                    logger.warning(f"Failed to generate {name} plot: {e}")

            # === Walk-Forward Optimization Curve ===
            self._create_walkforward_curve(output_dir, formats, generated_files)

            if generated_files:
                logger.info(
                    "OPTUNA PLOTS ERSTELLT",
                    folder=output_dir,
                    files=[f.split("/")[-1] for f in generated_files],
                    formats=formats,
                )
            if failed_plots:
                logger.debug("Some plots skipped", skipped=failed_plots)

        except ImportError as e:
            logger.warning(f"Plotly visualization dependencies not available: {e}")
        except Exception as e:
            logger.error(f"Failed to create Optuna visualizations: {e}")

    def _create_walkforward_curve(
        self,
        output_dir: str,
        formats: list[str],
        generated_files: list[str],
    ) -> None:
        """Create Walk-Forward Optimization Curve visualization.

        Shows the best score evolution over trials as a line chart with
        markers and spline interpolation. Useful for understanding
        optimization convergence.

        Args:
            output_dir: Directory to save plots
            formats: List of output formats (html, png)
            generated_files: List to append generated file paths to
        """
        if self._study is None:
            return

        try:
            import plotly.express as px

            # Get trials dataframe from Optuna study
            df = self._study.trials_dataframe()

            if df.empty or "number" not in df.columns or "value" not in df.columns:
                logger.debug("No trial data available for walk-forward curve")
                return

            # Filter to completed trials only
            df_complete = df[df["state"] == "COMPLETE"].copy()
            if df_complete.empty:
                logger.debug("No completed trials for walk-forward curve")
                return

            # Create line plot with markers and spline interpolation
            walkforward_fig = px.line(
                df_complete,
                x="number",
                y="value",
                title="Walk-Forward Optimization Curve – Best Score over Trials",
                markers=True,
                line_shape="spline",
            )
            walkforward_fig.update_layout(
                xaxis_title="Trial Number",
                yaxis_title="Score (Sharpe × Winrate + PNL)",
                hovermode="x unified",
            )

            # Save in all configured formats
            for fmt in formats:
                filepath = f"{output_dir}/walkforward_curve.{fmt}"
                try:
                    if fmt == "html":
                        walkforward_fig.write_html(filepath)
                        generated_files.append(filepath)
                    elif fmt == "png":
                        walkforward_fig.write_image(filepath)
                        generated_files.append(filepath)
                except Exception as e:
                    logger.warning(f"Failed to write {filepath}: {e}")

            logger.info(
                "WALK-FORWARD CURVE ERSTELLT",
                path=f"{output_dir}/walkforward_curve.html",
            )

        except ImportError as e:
            logger.debug(f"Plotly express not available for walk-forward curve: {e}")
        except Exception as e:
            logger.warning(f"Failed to create walk-forward curve: {e}")

    def _auto_apply_best_params(
        self, best_params: dict[str, Any], best_score: float
    ) -> None:
        """Auto-apply best parameters by generating env and JSON config files.

        Creates:
        - best_params.env: Environment variables for Railway/Docker deployment
        - best_config.json: JSON config with metadata

        Args:
            best_params: Best parameter combination found
            best_score: Score achieved by best params
        """
        try:
            # Write best_params.env file for easy Railway deployment
            env_file = "best_params.env"
            with open(env_file, "w") as f:
                for param_key, param_value in best_params.items():
                    f.write(f"{param_key.upper()}={param_value}\n")
                # Ensure critical params are included with defaults if not present
                if "MIN_EV" not in best_params:
                    f.write(f"MIN_EV={best_params.get('MIN_EV', 0.015)}\n")
                if "KELLY_MULTIPLIER" not in best_params:
                    f.write(
                        f"KELLY_MULTIPLIER={best_params.get('KELLY_MULTIPLIER', 0.5)}\n"
                    )

            # Write best_config.json file with full metadata
            json_file = "best_config.json"
            config_data = {
                "best_params": best_params,
                "best_score": round(best_score, 4),
                "timestamp": datetime.now().isoformat(),
                "sampler": self.settings.optuna_sampler,
                "n_trials": self.settings.optuna_n_trials,
                "windows": self.settings.walkforward_windows,
            }
            with open(json_file, "w") as f:
                json.dump(config_data, f, indent=2)

            logger.info(
                "AUTO-APPLY DATEIEN ERSTELLT",
                env_file=env_file,
                json_file=json_file,
            )

        except Exception as e:
            logger.error(f"Failed to create auto-apply files: {e}")
