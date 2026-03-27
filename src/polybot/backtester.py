"""Backtester: Historical market replay for strategy validation.

Fetches resolved markets from Polymarket's Gamma API and simulates
trading decisions to calculate realistic performance metrics.

Features:
- Replay resolved markets for historical analysis
- Win rate and profit factor calculation
- Equity curve generation
- Sharpe ratio approximation
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import requests

from polybot.logging_setup import get_logger

log = get_logger(__name__)


def fetch_historical_markets(limit: int = 500) -> list[dict]:
    """Fetch resolved markets from Gamma API.

    Args:
        limit: Maximum number of markets to fetch

    Returns:
        List of resolved market data dictionaries
    """
    try:
        url = f"https://gamma-api.polymarket.com/markets?resolved=true&limit={limit}"
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        markets = response.json()
        log.info("Fetched historical markets", count=len(markets))
        return markets
    except Exception as e:
        log.error("Failed to fetch historical markets", error=str(e))
        return []


@dataclass
class BacktestResult:
    """Results from a backtest run."""

    total_pnl: float = 0.0
    winrate: float = 0.0
    profit_factor: float = 0.0
    sharpe: float = 0.0
    total_trades: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    max_drawdown: float = 0.0
    equity_curve: list[dict] = field(default_factory=list)
    run_timestamp: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "total_pnl": round(self.total_pnl, 2),
            "winrate": round(self.winrate, 1),
            "profit_factor": round(self.profit_factor, 2),
            "sharpe": round(self.sharpe, 2),
            "total_trades": self.total_trades,
            "winning_trades": self.winning_trades,
            "losing_trades": self.losing_trades,
            "max_drawdown": round(self.max_drawdown, 2),
            "equity_curve": self.equity_curve,
            "run_timestamp": self.run_timestamp,
        }


class Backtester:
    """Historical backtesting engine for Polymarket strategies.

    Uses resolved markets from Gamma API to simulate trading decisions
    and calculate realistic performance metrics.
    """

    # Default backtest parameters
    DEFAULT_DAYS = 7  # Fast 7-day backtest (reduced from 180)
    DEFAULT_TRADE_SIZE = 100.0  # $100 per trade
    MIN_EV_THRESHOLD = 0.08  # 8% minimum EV

    def __init__(
        self,
        trade_size: float | None = None,
        min_ev: float | None = None,
    ):
        """Initialize the backtester.

        Args:
            trade_size: Amount per trade in USD (default: $100)
            min_ev: Minimum EV threshold for trade entry (default: 8%)
        """
        self.trade_size = trade_size or self.DEFAULT_TRADE_SIZE
        self.min_ev = min_ev or self.MIN_EV_THRESHOLD
        self._last_result: BacktestResult | None = None

    def _simulate_trade(self, market: dict) -> dict[str, Any]:
        """Simulate a trade on a resolved market.

        Args:
            market: Market data with resolution info

        Returns:
            Trade result dict with pnl and metadata
        """
        # Extract market data
        question = market.get("question", "Unknown")[:50]
        resolution = market.get("resolution", market.get("outcome", ""))
        end_date = market.get("end_date", market.get("resolutionTime", ""))

        # Get token prices (YES/NO)
        tokens = market.get("tokens", [])
        yes_price = 0.5
        no_price = 0.5

        for token in tokens:
            outcome = str(token.get("outcome", "")).lower()
            price = float(token.get("price", 0.5))
            if outcome == "yes":
                yes_price = price
            elif outcome == "no":
                no_price = price

        # Simple strategy: bet on the cheaper side if under 0.4
        # This represents a basic mean-reversion/value strategy
        if yes_price < 0.4:
            bet_side = "yes"
            entry_price = yes_price
        elif no_price < 0.4:
            bet_side = "no"
            entry_price = no_price
        else:
            # No clear value, skip
            return {"skip": True}

        # Calculate EV (simplified)
        ev = (0.5 - entry_price) / 0.5  # Distance from fair value
        if ev < self.min_ev:
            return {"skip": True}

        # Determine if trade won
        resolution_lower = str(resolution).lower()
        won = (bet_side == "yes" and resolution_lower in ["yes", "true", "1"]) or (
            bet_side == "no" and resolution_lower in ["no", "false", "0"]
        )

        # Calculate P&L
        if won:
            # Win: receive $1 per share, paid entry_price
            shares = self.trade_size / entry_price
            pnl = shares * (1.0 - entry_price)
        else:
            # Loss: lose entire stake
            pnl = -self.trade_size

        return {
            "skip": False,
            "market": question,
            "bet_side": bet_side,
            "entry_price": entry_price,
            "resolution": resolution,
            "won": won,
            "pnl": pnl,
            "ev": ev,
            "date": end_date,
        }

    def run_backtest(self, days: int | None = None) -> BacktestResult:
        """Run backtest on historical resolved markets.

        Args:
            days: Number of days of history to analyze (default: 7)

        Returns:
            BacktestResult with performance metrics
        """
        days = days or self.DEFAULT_DAYS

        log.info("Starting backtest", days=days, trade_size=self.trade_size)

        result = BacktestResult(
            run_timestamp=datetime.now(timezone.utc).isoformat(),
        )

        # Fetch resolved markets using module-level function
        markets = fetch_historical_markets(limit=min(days * 3, 1000))
        if not markets:
            log.warning("No historical markets available for backtest")
            return result

        # Track equity curve
        cumulative_pnl = 0.0
        peak_pnl = 0.0
        max_drawdown = 0.0
        total_profit = 0.0
        total_loss = 0.0

        # Process markets (limit to specified days worth)
        for market in markets[:days]:
            trade = self._simulate_trade(market)

            if trade.get("skip"):
                continue

            pnl = trade["pnl"]
            cumulative_pnl += pnl
            result.total_trades += 1

            if pnl > 0:
                result.winning_trades += 1
                total_profit += pnl
            else:
                result.losing_trades += 1
                total_loss += abs(pnl)

            # Track drawdown
            peak_pnl = max(peak_pnl, cumulative_pnl)
            current_dd = peak_pnl - cumulative_pnl
            max_drawdown = max(max_drawdown, current_dd)

            # Add to equity curve
            result.equity_curve.append(
                {
                    "date": trade.get("date", ""),
                    "pnl": round(cumulative_pnl, 2),
                    "trade_pnl": round(pnl, 2),
                    "market": trade.get("market", ""),
                }
            )

        # Calculate final metrics
        result.total_pnl = cumulative_pnl
        result.max_drawdown = max_drawdown

        if result.total_trades > 0:
            result.winrate = (result.winning_trades / result.total_trades) * 100

        if total_loss > 0:
            result.profit_factor = total_profit / total_loss
        elif total_profit > 0:
            result.profit_factor = float("inf")

        # Simplified Sharpe ratio approximation
        # Using average trade return / std deviation estimate
        if result.total_trades > 1:
            avg_return = cumulative_pnl / result.total_trades / self.trade_size
            # Approximate volatility based on win rate and typical trade size
            vol_estimate = 0.5  # Binary outcome volatility
            result.sharpe = (avg_return / vol_estimate) * (252**0.5)  # Annualized
            result.sharpe = max(-5, min(5, result.sharpe))  # Clamp to reasonable range

        self._last_result = result

        log.info(
            "Backtest complete",
            total_trades=result.total_trades,
            winrate=f"{result.winrate:.1f}%",
            total_pnl=f"${result.total_pnl:.2f}",
            profit_factor=f"{result.profit_factor:.2f}",
            sharpe=f"{result.sharpe:.2f}",
        )

        return result

    def get_last_result(self) -> BacktestResult | None:
        """Get the most recent backtest result."""
        return self._last_result


def format_backtest_report(result: BacktestResult | dict) -> str:
    """Format backtest results for display (CLI/Dashboard).

    Args:
        result: BacktestResult or dict with backtest data

    Returns:
        Formatted string for display
    """
    if isinstance(result, BacktestResult):
        data = result.to_dict()
    else:
        data = result

    pnl = data.get("total_pnl", 0)
    pnl_emoji = "📈" if pnl > 0 else "📉" if pnl < 0 else "➖"

    wr = data.get("winrate", 0)
    wr_emoji = "✅" if wr >= 55 else "📊" if wr >= 50 else "⚠️"

    pf = data.get("profit_factor", 0)
    pf_str = f"{pf:.2f}" if isinstance(pf, (int, float)) and pf != float("inf") else "∞"

    sharpe = data.get("sharpe", 0)
    sharpe_emoji = "🌟" if sharpe >= 1.5 else "✅" if sharpe >= 1.0 else "⚠️"

    return f"""
🚀 **BACKTEST REPORT**

{pnl_emoji} **Total P&L:** ${pnl:,.2f}

📊 **Performance Metrics:**
{wr_emoji} Winrate: {wr:.1f}% ({data.get("winning_trades", 0)}W / {data.get("losing_trades", 0)}L)
📈 Profit Factor: {pf_str}
{sharpe_emoji} Sharpe Ratio: {sharpe:.2f}
📉 Max Drawdown: ${data.get("max_drawdown", 0):,.2f}

📝 **Summary:**
Total Trades: {data.get("total_trades", 0)}
Timestamp: {data.get("run_timestamp", "N/A")[:19]}
"""


# =============================================================================
# EdgeBacktester: Historical test for EdgeEngine (5min Up/Down)
# =============================================================================


class EdgeBacktester:
    """Edge Backtester v1 – historical test for EdgeEngine (5min Up/Down).

    Fetches closed markets from Gamma API, simulates trades with real Edge + Outcome.
    Outputs Winrate, Profit Factor, PNL, and CSV.
    """

    # Default configuration constants
    DEFAULT_POSITION_SIZE = 1000  # $1k position simulated
    MAX_MARKETS_TO_FETCH = 2000  # Maximum markets to fetch from API

    def __init__(self, position_size: float | None = None):
        """Initialize the EdgeBacktester with EdgeEngine and settings.

        Args:
            position_size: Position size in USD for simulated trades (default: $1000)
        """
        from polybot.config import get_settings
        from polybot.edge_engine import get_edge_engine

        self.edge_engine = get_edge_engine()
        self.settings = get_settings()
        self.position_size = position_size or self.DEFAULT_POSITION_SIZE

    async def run_backtest_async(
        self, days: int = 30, min_liquidity: float = 200.0
    ) -> dict:
        """Run full backtest and save results.

        Args:
            days: Number of days back for backtest (closed markets)
            min_liquidity: Minimum liquidity filter for markets

        Returns:
            Dict with backtest results (total_trades, winrate, total_pnl, profit_factor, csv)
        """
        import csv
        import math
        from datetime import timedelta

        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        all_closed = await self._fetch_closed_markets(cutoff)

        trades = []
        for market in all_closed:
            # Skip unresolved markets - we can only backtest resolved outcomes
            if not market.get("resolved", False):
                continue

            liquidity = float(market.get("liquidity", 0))
            if liquidity < min_liquidity:
                continue

            edge = self.edge_engine.get_liquidity_adjusted_edge(market, liquidity)
            if edge < 0.01:
                continue

            # Simulate outcome using resolved market data
            yes_price = self._get_yes_price_at_close(market)

            # For resolved markets, outcome is stored in resolution field
            resolution = str(market.get("resolution", "")).lower()
            actual_outcome = 1 if resolution in ("yes", "true", "1") else 0

            # Simulate P&L: assume we bet on YES at the closing price
            # P&L = (outcome - entry_price) * position_size
            pnl = (actual_outcome - yes_price) * self.position_size

            # Apply commission
            commission_rate = self.settings.backtest_commission_bps / 10000
            pnl -= abs(self.position_size) * commission_rate

            trades.append(
                {
                    "timestamp": market.get("closed_at", market.get("end_date", "")),
                    "question": market.get("question", "")[:80],
                    "edge": round(edge, 4),
                    "pnl": round(pnl, 2),
                    "win": pnl > 0,
                }
            )

        # Calculate Metrics
        total_trades = len(trades)
        wins = sum(1 for t in trades if t["win"])
        winrate = wins / total_trades * 100 if total_trades else 0
        total_pnl = sum(t["pnl"] for t in trades)

        # Calculate profit factor (total profits / total losses)
        total_profits = sum(t["pnl"] for t in trades if t["pnl"] > 0)
        total_losses = abs(sum(t["pnl"] for t in trades if t["pnl"] < 0))
        profit_factor = (total_profits / total_losses) if total_losses > 0 else 0.0

        # Calculate Sharpe ratio (annualized) if we have enough trades
        sharpe_ratio = 0.0
        if len(trades) >= 2:
            pnl_values = [t["pnl"] for t in trades]
            mean_pnl = sum(pnl_values) / len(pnl_values)
            variance = sum((p - mean_pnl) ** 2 for p in pnl_values) / (
                len(pnl_values) - 1
            )
            std_dev = math.sqrt(variance) if variance > 0 else 0
            if std_dev > 0:
                # Annualize assuming ~252 trading days
                sharpe_ratio = (mean_pnl / std_dev) * math.sqrt(252)
                sharpe_ratio = max(
                    -10, min(10, sharpe_ratio)
                )  # Clamp to reasonable range

        # CSV Export
        csv_path = self.settings.backtest_output_csv
        with open(csv_path, "w", newline="") as f:
            writer = csv.DictWriter(
                f, fieldnames=["timestamp", "question", "edge", "pnl", "win"]
            )
            writer.writeheader()
            writer.writerows(trades)

        log.info(
            f"BACKTEST ERGEBNISSE → {total_trades} Trades | "
            f"Winrate {winrate:.1f}% | Total PNL ${total_pnl:.2f} | "
            f"Profit Factor {profit_factor:.2f} | Sharpe {sharpe_ratio:.2f}"
        )

        return {
            "total_trades": total_trades,
            "winrate": round(winrate, 1),
            "total_pnl": round(total_pnl, 2),
            "profit_factor": round(profit_factor, 2),
            "sharpe": round(sharpe_ratio, 2),
            "csv": csv_path,
        }

    def _get_yes_price_at_close(self, market: dict) -> float:
        """Extract YES token price at close from market data."""
        # Try different price field names
        if "yes_price_at_close" in market:
            return float(market["yes_price_at_close"])

        # Try tokens array
        for token in market.get("tokens", []):
            if token.get("outcome", "").lower() == "yes":
                p = token.get("price")
                if p is not None:
                    return float(p)

        # Fallback to 0.5
        return 0.5

    async def _fetch_closed_markets(self, cutoff: str) -> list[dict]:
        """Fetch closed 5min Up/Down markets (only BTC/ETH/SOL).

        Args:
            cutoff: ISO timestamp cutoff for filtering

        Returns:
            List of closed market dicts
        """
        from polybot.proxy import make_proxied_request_async

        markets: list[dict] = []
        offset = 0
        target_symbols = ["btc", "eth", "sol"]
        keywords_5min = ["5 min", "5min", "5-minute"]

        while offset < self.MAX_MARKETS_TO_FETCH:
            try:
                data = await make_proxied_request_async(
                    "https://gamma-api.polymarket.com/markets",
                    params={
                        "closed": "true",
                        "limit": "100",
                        "offset": str(offset),
                        "order": "closed_at:desc",
                    },
                )

                if not data or not isinstance(data, list):
                    break

                # Filter for 5min BTC/ETH/SOL markets
                batch = []
                for m in data:
                    question = m.get("question", "").lower()
                    has_symbol = any(s in question for s in target_symbols)
                    has_5min = any(kw in question for kw in keywords_5min)
                    if has_symbol and has_5min:
                        batch.append(m)

                markets.extend(batch)

                if len(data) < 100:
                    break
                offset += 100

            except Exception as e:
                log.error("Error fetching closed markets", offset=offset, error=str(e))
                break

        log.info(f"Fetched {len(markets)} closed 5min crypto markets for backtest")
        return markets
