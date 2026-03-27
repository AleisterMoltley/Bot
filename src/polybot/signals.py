"""Multi-signal trading engine.

Combines MA Crossover, RSI, MACD, Momentum, and Volume Momentum
into a weighted confidence score with directional prediction.

Also provides smart timing utilities for 5-minute markets.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from polybot.config import get_settings

if TYPE_CHECKING:
    from polybot.config import Settings


@dataclass
class SignalResult:
    direction: str  # "up" | "down" | "hold"
    confidence: float  # 0-100
    components: dict[str, Any]


# ══════════════════════════════════════════════════════════════════════════════
# Smart Timing Functions for 5-Minute Markets
# ══════════════════════════════════════════════════════════════════════════════


def get_seconds_until_market_close(market_end_time: datetime | None = None) -> int:
    """Calculate seconds remaining until the current 5-minute market closes.

    For 5-minute markets, the close times are at :00, :05, :10, :15, etc.

    Args:
        market_end_time: Optional specific market end time. If None,
                        calculates based on current 5-minute window.

    Returns:
        Seconds until market close (0-300 for 5-min markets)
    """
    now = datetime.now(timezone.utc)

    if market_end_time is not None:
        # Use provided market end time
        delta = market_end_time - now
        return max(0, int(delta.total_seconds()))

    # Calculate next 5-minute boundary
    current_minute = now.minute
    current_second = now.second

    # Find minutes until next 5-minute mark
    minutes_past = current_minute % 5
    minutes_remaining = (5 - minutes_past) % 5

    if minutes_remaining == 0:
        # We're on a 5-minute mark
        if current_second == 0:
            # Exactly at the close - return full 5 minutes for next market
            seconds_remaining = 300
        else:
            # Past the close by some seconds - time until next close
            seconds_remaining = 300 - current_second
    else:
        seconds_remaining = (minutes_remaining * 60) - current_second

    return max(0, seconds_remaining)


def get_smart_scan_interval(seconds_to_close: int | None = None) -> int:
    """Get the optimal scan interval based on time to market close.

    Implements dynamic timing:
    - <2 min to close: HIGH_FREQUENCY_INTERVAL (15s)
    - 2-5 min to close: 25s
    - >5 min to close: NORMAL_INTERVAL (45s)

    Args:
        seconds_to_close: Seconds until market close. If None, calculates.

    Returns:
        Recommended scan interval in seconds
    """
    settings = get_settings()

    # If smart scan disabled, use fixed scan interval
    if not settings.smart_scan_enabled:
        return settings.scan_interval_seconds

    if seconds_to_close is None:
        seconds_to_close = get_seconds_until_market_close()

    if seconds_to_close < 120:
        # Last 2 minutes: maximum frequency
        return settings.high_frequency_interval
    elif seconds_to_close < 300:
        # 2-5 minutes: medium frequency
        return 25
    else:
        # >5 minutes: normal frequency
        return settings.normal_interval


def should_skip_trade_near_close(
    seconds_to_close: int,
    spread_pct: float,
    max_spread_pct: float = 0.5,
) -> tuple[bool, str]:
    """Check if trade should be skipped due to timing/spread conditions.

    Rejects trades in the last minute if spread is too high.

    Args:
        seconds_to_close: Seconds until market close
        spread_pct: Current bid-ask spread as percentage
        max_spread_pct: Maximum allowed spread in final minute

    Returns:
        (should_skip: bool, reason: str)
    """
    if seconds_to_close < 60 and spread_pct > max_spread_pct:
        return (
            True,
            f"Skip: High spread ({spread_pct:.2f}%) in final minute",
        )
    return False, "OK"


def get_market_timing_info() -> dict:
    """Get current market timing information for dashboard.

    Returns:
        Dictionary with timing info for display
    """
    seconds_to_close = get_seconds_until_market_close()
    interval = get_smart_scan_interval(seconds_to_close)

    return {
        "seconds_to_close": seconds_to_close,
        "recommended_interval": interval,
        "phase": (
            "final"
            if seconds_to_close < 120
            else "active"
            if seconds_to_close < 300
            else "early"
        ),
        "timestamp": time.time(),
    }


def calculate_ema(prices: list[float], period: int) -> list[float]:
    if len(prices) < period:
        return []
    multiplier = 2 / (period + 1)
    ema = [sum(prices[:period]) / period]
    for price in prices[period:]:
        ema.append((price - ema[-1]) * multiplier + ema[-1])
    return ema


def calculate_rsi(closes: list[float], period: int = 14) -> float:
    if len(closes) < period + 1:
        return 50.0
    changes = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    recent_gains = [max(0, c) for c in changes[-period:]]
    recent_losses = [max(0, -c) for c in changes[-period:]]
    avg_gain = sum(recent_gains) / period
    avg_loss = sum(recent_losses) / period
    if avg_loss == 0:
        return 100.0 if avg_gain > 0 else 50.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def calculate_macd(
    closes: list[float],
    fast: int = 12,
    slow: int = 26,
    signal: int = 9,
) -> dict:
    if len(closes) < slow + signal:
        return {"macd_line": 0, "signal_line": 0, "histogram": 0, "valid": False}
    fast_ema = calculate_ema(closes, fast)
    slow_ema = calculate_ema(closes, slow)
    if not fast_ema or not slow_ema:
        return {"macd_line": 0, "signal_line": 0, "histogram": 0, "valid": False}
    offset = slow - fast
    macd_values = [f - s for f, s in zip(fast_ema[offset:], slow_ema)]
    if len(macd_values) < signal:
        return {"macd_line": 0, "signal_line": 0, "histogram": 0, "valid": False}
    signal_ema = calculate_ema(macd_values, signal)
    if not signal_ema:
        return {
            "macd_line": macd_values[-1],
            "signal_line": 0,
            "histogram": 0,
            "valid": False,
        }
    ml = macd_values[-1]
    sl = signal_ema[-1]
    return {"macd_line": ml, "signal_line": sl, "histogram": ml - sl, "valid": True}


def _ma_signal(closes: list[float], short_w: int, long_w: int) -> dict:
    if len(closes) < long_w:
        return {"direction": "neutral", "strength": 0}
    ma_short = sum(closes[-short_w:]) / short_w
    ma_long = sum(closes[-long_w:]) / long_w
    if ma_long == 0:
        return {"direction": "neutral", "strength": 0}
    pct_diff = ((ma_short - ma_long) / ma_long) * 100
    strength = min(100, abs(pct_diff) * 50)
    return {"direction": "up" if ma_short > ma_long else "down", "strength": strength}


def _rsi_signal(closes: list[float], overbought: int, oversold: int) -> dict:
    rsi = calculate_rsi(closes)
    if rsi <= oversold:
        return {
            "direction": "up",
            "strength": ((oversold - rsi) / oversold) * 100,
            "rsi": rsi,
        }
    if rsi >= overbought:
        return {
            "direction": "down",
            "strength": ((rsi - overbought) / (100 - overbought)) * 100,
            "rsi": rsi,
        }
    return {
        "direction": "neutral",
        "strength": min(30, abs(rsi - 50) / 20 * 100),
        "rsi": rsi,
    }


def _macd_signal(closes: list[float], fast: int, slow: int, sig: int) -> dict:
    macd = calculate_macd(closes, fast, slow, sig)
    if not macd["valid"] or not closes:
        return {"direction": "neutral", "strength": 0}
    histogram = macd["histogram"]
    price = closes[-1] if closes else 1
    strength = min(100, abs(histogram) / price * 10000) if price else 0
    return {
        "direction": "up" if histogram > 0 else "down",
        "strength": strength,
        **macd,
    }


def _momentum_signal(closes: list[float], lookback: int = 10) -> dict:
    if len(closes) < lookback + 1:
        return {"direction": "neutral", "strength": 0}
    current = closes[-1]
    past = closes[-lookback - 1]
    if past == 0:
        return {"direction": "neutral", "strength": 0}
    pct_change = ((current - past) / past) * 100
    strength = min(100, abs(pct_change) * 20)
    return {"direction": "up" if pct_change > 0 else "down", "strength": strength}


def _volume_momentum_signal(
    closes: list[float], volumes: list[float] | None = None
) -> dict:
    """Volume-weighted momentum: bonus when volume and price both rise."""
    if not volumes or len(volumes) < 5 or len(closes) < 5:
        return {"direction": "neutral", "strength": 0}
    recent_vol = sum(volumes[-3:]) / 3
    older_vol = sum(volumes[-6:-3]) / 3 if len(volumes) >= 6 else recent_vol
    price_up = closes[-1] > closes[-3]
    vol_up = recent_vol > older_vol * 1.1
    if price_up and vol_up:
        return {"direction": "up", "strength": 75}
    if not price_up and vol_up:
        return {"direction": "down", "strength": 75}
    return {"direction": "neutral", "strength": 25}


def compute_signal(
    closes: list[float],
    volumes: list[float] | None = None,
    settings: Settings | None = None,
) -> SignalResult:
    """Compute the weighted multi-signal prediction.

    Returns direction (up/down/hold) and confidence (0-100).
    """
    if settings is None:
        settings = get_settings()

    weights = settings.signal_weights
    total_weight = sum(weights.values())

    ma = _ma_signal(closes, settings.short_window, settings.long_window)
    rsi = _rsi_signal(closes, settings.rsi_overbought, settings.rsi_oversold)
    macd = _macd_signal(
        closes, settings.macd_fast, settings.macd_slow, settings.macd_signal
    )
    mom = _momentum_signal(closes)
    vol = _volume_momentum_signal(closes, volumes)

    signals = {
        "ma_crossover": ma,
        "rsi": rsi,
        "macd": macd,
        "momentum": mom,
        "volume_momentum": vol,
    }

    # Weighted vote
    up_score = 0.0
    down_score = 0.0
    for name, sig in signals.items():
        w = weights.get(name, 0) / total_weight
        strength = sig["strength"] / 100.0
        if sig["direction"] == "up":
            up_score += w * strength
        elif sig["direction"] == "down":
            down_score += w * strength

    if up_score > down_score:
        direction = "up"
        confidence = up_score * 100
    elif down_score > up_score:
        direction = "down"
        confidence = down_score * 100
    else:
        direction = "hold"
        confidence = 0

    confidence = min(100, max(0, confidence))

    if confidence < 30:
        direction = "hold"

    return SignalResult(
        direction=direction,
        confidence=confidence,
        components=signals,
    )
