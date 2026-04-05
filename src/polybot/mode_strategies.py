"""
mode_strategies.py — Strategy definitions for all trading modes.

Each mode is a combination of:
- base_mode: Which backend loop to run (sniper, updown, signal, arbitrage)
- timing: When to enter (snipe window, scan interval)
- direction_filter: Which side(s) to trade (up, down, both)
- signal_sources: Which engines to use (binance, hyperliquid, both)
- confidence_min: Minimum confidence threshold
- assets: Which coins to trade
- risk_overrides: Kelly multiplier, max position, etc.

The mode selector UI (/modes) sends a POST to /api/mode with a ui_mode key.
This module resolves that key into a full StrategyConfig that the scanner/sniper
reads at runtime to adjust behavior.

Usage:
    from polybot.mode_strategies import get_active_strategy, STRATEGIES
    strategy = get_active_strategy()
    if strategy.direction_filter == "down":
        # only trade DOWN side
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal, Optional


@dataclass(frozen=True)
class StrategyConfig:
    """Immutable strategy configuration for a trading mode."""

    # Identity
    ui_mode: str
    label: str
    description: str

    # Execution
    base_mode: Literal["sniper", "updown", "signal", "arbitrage"]
    snipe_window_seconds: int = 30  # How many seconds before slot end to enter
    scan_interval_override: Optional[int] = None  # Override scan interval (seconds)

    # Direction
    direction_filter: Literal["both", "up", "down"] = "both"

    # Signal sources
    use_binance: bool = True
    use_hyperliquid: bool = False
    signal_consensus_required: bool = False  # True = need all sources to agree

    # Confidence
    min_confidence: float = 0.60
    min_ev: Optional[float] = None  # Override MIN_EV from config

    # Risk overrides (None = use config defaults)
    kelly_multiplier: Optional[float] = None
    max_position_usd: Optional[float] = None

    # Assets (None = use config default target_symbols)
    assets: Optional[list[str]] = None

    # Timing
    active_hours: Optional[tuple[int, int]] = None  # (start_hour, end_hour) UTC
    skip_low_volatility: bool = True


# ═══════════════════════════════════════════════════════════════════════════════
# STRATEGY DEFINITIONS — One per UI mode
# ═══════════════════════════════════════════════════════════════════════════════

STRATEGIES: dict[str, StrategyConfig] = {

    # ── Sniper Variants ──────────────────────────────────────────────────────

    "sniper45": StrategyConfig(
        ui_mode="sniper45",
        label="Sniper 45",
        description="Last 45s of each 5-min slot. Sweet spot: 70-85¢ prices.",
        base_mode="sniper",
        snipe_window_seconds=45,
        min_confidence=0.58,
    ),

    "sniper30": StrategyConfig(
        ui_mode="sniper30",
        label="Sniper 30",
        description="Last 30s, aggressive. Higher confidence required.",
        base_mode="sniper",
        snipe_window_seconds=30,
        min_confidence=0.65,
    ),

    "sniper60": StrategyConfig(
        ui_mode="sniper60",
        label="Sniper 60",
        description="Last 60s, wider window. Lower entry prices, more risk.",
        base_mode="sniper",
        snipe_window_seconds=60,
        min_confidence=0.55,
    ),

    # ── Hyperliquid ──────────────────────────────────────────────────────────

    "hype45": StrategyConfig(
        ui_mode="hype45",
        label="HYPE 45",
        description="Sniper 45 with Hyperliquid as primary signal source.",
        base_mode="sniper",
        snipe_window_seconds=45,
        use_hyperliquid=True,
        use_binance=True,  # Binance as fallback
        min_confidence=0.58,
    ),

    # ── DemonScalp ───────────────────────────────────────────────────────────

    "demonscalp": StrategyConfig(
        ui_mode="demonscalp",
        label="DemonScalp",
        description="15-minute scalps. High conviction signals only.",
        base_mode="sniper",
        snipe_window_seconds=120,  # Enter 2 min before 5-min slot ends
        min_confidence=0.72,
        kelly_multiplier=0.7,  # More aggressive sizing on high-conviction
        skip_low_volatility=True,
        scan_interval_override=8,  # Faster scanning
    ),

    # ── Coinbase (Macro Trend) ───────────────────────────────────────────────

    "coinbase": StrategyConfig(
        ui_mode="coinbase",
        label="Coinbase",
        description="Macro trend following. Trades in direction of 1h trend.",
        base_mode="signal",
        min_confidence=0.60,
        min_ev=0.015,  # Higher EV threshold — wait for clear trend
        kelly_multiplier=0.4,  # Conservative sizing
        skip_low_volatility=True,
        scan_interval_override=20,  # Slower — macro view
    ),

    # ── Consensus Modes ──────────────────────────────────────────────────────

    "full_consensus": StrategyConfig(
        ui_mode="full_consensus",
        label="Full Consensus",
        description="All signal sources must agree on direction.",
        base_mode="signal",
        use_binance=True,
        use_hyperliquid=True,
        signal_consensus_required=True,
        min_confidence=0.65,
        min_ev=0.012,
    ),

    "down_only": StrategyConfig(
        ui_mode="down_only",
        label="Down Only",
        description="Only trade DOWN side during bearish consensus windows.",
        base_mode="updown",
        direction_filter="down",
        min_confidence=0.60,
        active_hours=(13, 21),  # UTC 13-21 = historically more bearish for crypto
    ),

    # ── Standard Modes ───────────────────────────────────────────────────────

    "updown": StrategyConfig(
        ui_mode="updown",
        label="Up/Down",
        description="Both sides by schedule. Default production mode.",
        base_mode="updown",
        direction_filter="both",
        min_confidence=0.55,
    ),

    "arbitrage": StrategyConfig(
        ui_mode="arbitrage",
        label="Arbitrage",
        description="Mispriced market edges. CEX vs Polymarket delta.",
        base_mode="arbitrage",
        min_confidence=0.70,
        min_ev=0.02,  # Need clear mispricing
        kelly_multiplier=0.6,
    ),

    # ── Signal Variants ──────────────────────────────────────────────────────

    "signal": StrategyConfig(
        ui_mode="signal",
        label="Signals",
        description="Filtered signal trades. Binance OFI/liquidation only.",
        base_mode="signal",
        min_confidence=0.62,
        min_ev=0.01,
    ),

    "signal247": StrategyConfig(
        ui_mode="signal247",
        label="Sig 24/7",
        description="Signals around the clock. No time restrictions.",
        base_mode="signal",
        min_confidence=0.60,
        skip_low_volatility=False,
        active_hours=None,  # No hour filtering
        scan_interval_override=10,
    ),

    # ── Advanced Modes ───────────────────────────────────────────────────────

    "autopred": StrategyConfig(
        ui_mode="autopred",
        label="AutoPred",
        description="Predictive model signals. EdgeEngine + TA indicators weighted.",
        base_mode="signal",
        min_confidence=0.58,
        min_ev=0.008,
        kelly_multiplier=0.5,
        use_binance=True,
        use_hyperliquid=True,
    ),

    "reverse": StrategyConfig(
        ui_mode="reverse",
        label="Reverse",
        description="Contrarian consensus. Fade the crowd when confidence is extreme.",
        base_mode="signal",
        min_confidence=0.68,  # High threshold — only fade clear extremes
        min_ev=0.015,
        kelly_multiplier=0.35,  # Conservative — contrarian is risky
    ),
}

# Default fallback
DEFAULT_STRATEGY = STRATEGIES["updown"]


def get_strategy(ui_mode: str) -> StrategyConfig:
    """Get strategy config by UI mode key."""
    return STRATEGIES.get(ui_mode, DEFAULT_STRATEGY)


def get_active_strategy() -> StrategyConfig:
    """Get the currently active strategy from runtime mode override."""
    try:
        from polybot.main_fastapi import _runtime_mode_override
        ui_mode = _runtime_mode_override.get("ui_mode", "updown")
        return get_strategy(ui_mode)
    except (ImportError, AttributeError):
        return DEFAULT_STRATEGY


def get_direction_for_signal(
    strategy: StrategyConfig,
    binance_direction: Optional[str],
    binance_confidence: float,
    hype_direction: Optional[str] = None,
    hype_confidence: float = 0.0,
    polymarket_up_price: float = 0.5,
) -> tuple[Optional[str], float]:
    """Apply strategy logic to determine final trade direction and confidence.

    Handles:
    - Direction filtering (down_only, up_only)
    - Consensus requirement (all sources agree)
    - Reverse/contrarian mode (flip the signal)
    - Confidence merging from multiple sources

    Returns:
        (direction, confidence) — direction is None if filtered out.
    """
    # ── Pick primary signal ──
    primary_dir = binance_direction
    primary_conf = binance_confidence

    # If Hyperliquid is enabled and has a stronger signal, prefer it
    if strategy.use_hyperliquid and hype_direction and hype_confidence > primary_conf:
        primary_dir = hype_direction
        primary_conf = hype_confidence

    # ── Consensus check ──
    if strategy.signal_consensus_required:
        if strategy.use_binance and strategy.use_hyperliquid:
            if binance_direction != hype_direction:
                return None, 0.0  # Disagree → no trade
            # Both agree → boost confidence
            primary_conf = min(0.95, (binance_confidence + hype_confidence) / 2 + 0.05)

    if primary_dir is None:
        return None, 0.0

    # ── Reverse/contrarian mode ──
    if strategy.ui_mode == "reverse":
        # Only flip when Polymarket crowd is strongly positioned
        if polymarket_up_price > 0.65:
            primary_dir = "down"  # Fade the UP crowd
            primary_conf = min(0.80, primary_conf + 0.05)
        elif polymarket_up_price < 0.35:
            primary_dir = "up"  # Fade the DOWN crowd
            primary_conf = min(0.80, primary_conf + 0.05)
        else:
            return None, 0.0  # No extreme to fade

    # ── Direction filter ──
    if strategy.direction_filter == "down" and primary_dir != "down":
        return None, 0.0
    if strategy.direction_filter == "up" and primary_dir != "up":
        return None, 0.0

    # ── Confidence threshold ──
    if primary_conf < strategy.min_confidence:
        return None, 0.0

    return primary_dir, primary_conf


def should_skip_by_hours(strategy: StrategyConfig) -> bool:
    """Check if current UTC hour is outside the strategy's active window."""
    if strategy.active_hours is None:
        return False
    import datetime
    hour = datetime.datetime.now(datetime.timezone.utc).hour
    start, end = strategy.active_hours
    if start <= end:
        return not (start <= hour < end)
    else:
        # Wraps midnight (e.g., 22-6)
        return not (hour >= start or hour < end)
