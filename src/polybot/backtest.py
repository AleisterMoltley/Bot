"""Walk-forward backtest engine.

Splits historical data into training/test windows, optimizes signal
parameters per window, and reports realistic performance metrics.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone

import numpy as np
from pycoingecko import CoinGeckoAPI

from polybot.config import get_settings
from polybot.signals import compute_signal
from polybot.logging_setup import get_logger

log = get_logger(__name__)
cg = CoinGeckoAPI()

BACKTEST_DAYS = 30
INITIAL_BALANCE = 1000.0
TRADE_SIZE_PCT = 0.05
HOLDING_PERIOD = 12  # candles (~1 hour at 5min)
SHARPE_ANNUALIZATION = 93.6


@dataclass
class BacktestResult:
    crypto_id: str = "bitcoin"
    data_points: int = 0
    total_trades: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    initial_balance: float = INITIAL_BALANCE
    final_balance: float = INITIAL_BALANCE
    net_profit: float = 0
    max_drawdown: float = 0
    sharpe_ratio: float = 0
    profit_factor: float = 0
    winrate: float = 0
    optimized_params: dict = field(default_factory=dict)
    is_walk_forward: bool = False
    run_timestamp: str = ""
    equity_curve: list[float] = field(default_factory=list)
    trades: list[dict] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "crypto_id": self.crypto_id,
            "data_points": self.data_points,
            "total_trades": self.total_trades,
            "winning_trades": self.winning_trades,
            "losing_trades": self.losing_trades,
            "initial_balance": self.initial_balance,
            "final_balance": self.final_balance,
            "net_profit": self.net_profit,
            "max_drawdown": self.max_drawdown,
            "sharpe_ratio": self.sharpe_ratio,
            "profit_factor": self.profit_factor,
            "winrate": self.winrate,
            "optimized_params": self.optimized_params,
            "is_walk_forward": self.is_walk_forward,
            "run_timestamp": self.run_timestamp,
        }


def fetch_ohlc(crypto_id: str = "bitcoin", days: int = BACKTEST_DAYS) -> list:
    """Fetch OHLC data from CoinGecko."""
    try:
        data = cg.get_coin_ohlc_by_id(id=crypto_id, vs_currency="usd", days=str(days))
        return data
    except Exception as e:
        log.error("Failed to fetch OHLC data", error=str(e))
        return []


def _optimize_params(closes: list[float], volumes: list[float] | None) -> dict:
    """Brute-force optimize signal parameters on training data."""
    best_pf = 0
    best_params = {
        "short_window": 5,
        "long_window": 20,
        "rsi_overbought": 70,
        "rsi_oversold": 30,
    }

    for sw in range(3, 12, 2):
        for lw in range(15, 30, 5):
            if sw >= lw:
                continue
            settings = get_settings()
            settings_copy = settings.model_copy()
            settings_copy.short_window = sw
            settings_copy.long_window = lw

            wins = losses = 0
            for i in range(
                lw + HOLDING_PERIOD, len(closes) - HOLDING_PERIOD, HOLDING_PERIOD
            ):
                window = closes[:i]
                sig = compute_signal(
                    window, volumes[:i] if volumes else None, settings_copy
                )
                if sig.direction == "hold" or sig.confidence < 50:
                    continue
                future = (
                    closes[i + HOLDING_PERIOD]
                    if i + HOLDING_PERIOD < len(closes)
                    else closes[-1]
                )
                current = closes[i]
                if sig.direction == "up" and future > current:
                    wins += 1
                elif sig.direction == "down" and future < current:
                    wins += 1
                else:
                    losses += 1

            pf = wins / max(losses, 1)
            if pf > best_pf:
                best_pf = pf
                best_params = {"short_window": sw, "long_window": lw}

    return best_params


def run_backtest(
    crypto_id: str = "bitcoin", days: int = BACKTEST_DAYS
) -> BacktestResult | None:
    """Run walk-forward backtest.

    Returns BacktestResult or None if insufficient data.
    """
    result = BacktestResult(
        crypto_id=crypto_id,
        run_timestamp=datetime.now(timezone.utc).isoformat(),
    )

    ohlc = fetch_ohlc(crypto_id, days)
    if len(ohlc) < 100:
        log.warning("Insufficient data for backtest", points=len(ohlc))
        return None

    closes = [c[4] for c in ohlc]
    volumes = [c[5] if len(c) > 5 else 0 for c in ohlc]
    result.data_points = len(closes)

    # Walk-forward: use first 70% for training, last 30% for testing
    split = int(len(closes) * 0.7)
    train_closes = closes[:split]
    train_volumes = volumes[:split] if volumes else None

    params = _optimize_params(train_closes, train_volumes)
    result.optimized_params = params
    result.is_walk_forward = True

    # Apply optimized params
    settings = get_settings()
    settings_copy = settings.model_copy()
    for k, v in params.items():
        if hasattr(settings_copy, k):
            setattr(settings_copy, k, v)

    # Run simulation on test data
    balance = INITIAL_BALANCE
    peak_balance = balance
    max_dd = 0
    returns = []
    total_profit = 0
    total_loss = 0
    equity = [balance]

    step = max(1, HOLDING_PERIOD // 2)
    i = split

    while i < len(closes) - HOLDING_PERIOD:
        window = closes[:i]
        vol_window = volumes[:i] if volumes else None

        sig = compute_signal(window, vol_window, settings_copy)
        if sig.direction == "hold" or sig.confidence < settings_copy.min_confidence:
            i += step
            continue

        trade_size = balance * TRADE_SIZE_PCT
        if trade_size < 1:
            break

        entry = closes[i]
        exit_price = closes[i + HOLDING_PERIOD]
        pct_change = (exit_price - entry) / entry

        if sig.direction == "up":
            profit = trade_size * pct_change
        else:
            profit = trade_size * (-pct_change)

        balance += profit
        if profit > 0:
            result.winning_trades += 1
            total_profit += profit
        else:
            result.losing_trades += 1
            total_loss += abs(profit)

        returns.append(profit / trade_size)
        result.total_trades += 1
        equity.append(balance)

        peak_balance = max(peak_balance, balance)
        dd = (peak_balance - balance) / peak_balance * 100 if peak_balance > 0 else 0
        max_dd = max(max_dd, dd)

        result.trades.append(
            {
                "entry_price": entry,
                "exit_price": exit_price,
                "direction": sig.direction,
                "confidence": sig.confidence,
                "profit": round(profit, 2),
                "balance_after": round(balance, 2),
            }
        )

        i += HOLDING_PERIOD + step

    result.final_balance = balance
    result.net_profit = balance - INITIAL_BALANCE
    result.max_drawdown = max_dd
    result.equity_curve = equity
    result.profit_factor = total_profit / total_loss if total_loss > 0 else float("inf")
    result.winrate = (
        result.winning_trades / result.total_trades * 100
        if result.total_trades > 0
        else 0
    )

    if len(returns) > 1:
        mean_r = np.mean(returns)
        std_r = np.std(returns)
        if std_r > 0:
            result.sharpe_ratio = (mean_r / std_r) * SHARPE_ANNUALIZATION

    log.info(
        "Backtest complete",
        trades=result.total_trades,
        winrate=f"{result.winrate:.1f}%",
        net_profit=f"${result.net_profit:.2f}",
        max_dd=f"{result.max_drawdown:.1f}%",
        sharpe=f"{result.sharpe_ratio:.2f}",
    )

    return result


def format_backtest_results(result: BacktestResult | dict) -> str:
    """Format backtest results for display."""
    d = result.to_dict() if isinstance(result, BacktestResult) else result

    wr = d.get("winrate", 0)
    wr_emoji = "✅" if wr >= 55 else "📊" if wr >= 50 else "⚠️"
    sr = d.get("sharpe_ratio", 0)
    sr_emoji = "🌟" if sr >= 1.5 else "✅" if sr >= 1.0 else "⚠️"
    dd = d.get("max_drawdown", 0)
    dd_emoji = "✅" if dd <= 10 else "📊" if dd <= 20 else "⚠️"

    pf = d.get("profit_factor", 0)
    pf_str = f"{pf:.2f}" if isinstance(pf, (int, float)) and pf != float("inf") else "∞"

    return f"""
📈 **Walk-Forward Backtest Results**

**Asset:** {d.get("crypto_id", "bitcoin").upper()}
**Data Points:** {d.get("data_points", 0):,}

💰 **Performance:**
Initial: ${d.get("initial_balance", 0):,.2f}
Final: ${d.get("final_balance", 0):,.2f}
Net Profit: ${d.get("net_profit", 0):,.2f}

📊 **Metrics:**
{wr_emoji} Winrate: {wr:.1f}% ({d.get("winning_trades", 0)}W / {d.get("losing_trades", 0)}L)
📈 Profit Factor: {pf_str}
{dd_emoji} Max Drawdown: {dd:.1f}%
{sr_emoji} Sharpe Ratio: {sr:.2f}
Total Trades: {d.get("total_trades", 0)}
"""
