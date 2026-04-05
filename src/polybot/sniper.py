"""Sniper Mode — Trade in the last 30 seconds of 5-minute markets.

Strategy:
1. Detect when a new 5-min slot starts (btc-updown-5m-{timestamp})
2. Wait until T+4:30 (30 seconds before expiry)
3. At T+4:30: Read CEX price trend over the last 4 minutes
4. Determine which side is winning (UP or DOWN)
5. Buy the winning side — price is cheap because outcome is near-certain
6. Market resolves at T+5:00 — collect payout

Why this works:
- At T+4:30, the CEX price has already moved in the 5-min window
- You know with ~75-85% certainty which way the market will resolve
- Polymarket prices lag CEX by 5-30 seconds
- Combined with SignalEngine OFI/liquidation data = even higher confidence

Activate: Set SNIPER_MODE=true in Railway environment variables.
"""

from __future__ import annotations

import asyncio
import time
import json
from typing import Optional

import aiohttp

from polybot.config import get_settings
from polybot.logging_setup import get_logger
from polybot.signal_engine import get_signal_engine

log = get_logger(__name__)

# Sniper timing
SLOT_DURATION = 300  # 5 minutes
SNIPE_WINDOW_BEFORE_END = 30  # Enter 30s before market closes
SNIPE_SCAN_SECONDS = 5  # Spend 5s analyzing, then execute
MIN_CONFIDENCE_TO_SNIPE = 0.60  # Base minimum confidence

# Asset prefixes for slug construction
ASSET_PREFIXES = {
    "BTC": "btc-updown-5m-",
    "ETH": "eth-updown-5m-",
    "SOL": "sol-updown-5m-",
    "XRP": "xrp-updown-5m-",
    "HYPE": "hype-updown-5m-",
}

GAMMA_API = "https://gamma-api.polymarket.com"


def _is_hype_mode() -> bool:
    """Check if Hyperliquid mode is active (runtime override or env)."""
    import os
    if os.environ.get("HYPERLIQUID_ENABLED", "").lower() in ("true", "1", "yes"):
        return True
    # Check runtime mode override from main_fastapi
    try:
        from polybot.main_fastapi import _runtime_mode_override
        return _runtime_mode_override.get("sub") == "hype"
    except (ImportError, AttributeError):
        return False


def _extract_yes_price(market: dict) -> float:
    """Extract YES/UP price from a Polymarket market dict."""
    yes_price = 0.5
    for token in market.get("tokens", []):
        if token.get("outcome", "").lower() in ("yes", "up"):
            p = token.get("price")
            if p is not None:
                yes_price = float(p)
                break
    return yes_price


def _get_current_slot_times(snipe_window: int = SNIPE_WINDOW_BEFORE_END) -> dict[str, dict]:
    """Calculate current and next slot start/end for each asset.

    Args:
        snipe_window: Seconds before slot end to consider "snipe window" open.

    Returns dict of asset -> {start, end, slug, seconds_until_snipe, seconds_until_end}
    """
    now = int(time.time())
    current_slot_start = (now // SLOT_DURATION) * SLOT_DURATION
    next_slot_start = current_slot_start + SLOT_DURATION

    result = {}
    for asset, prefix in ASSET_PREFIXES.items():
        slot_end = current_slot_start + SLOT_DURATION
        seconds_until_end = slot_end - now
        seconds_until_snipe = seconds_until_end - snipe_window

        result[asset] = {
            "start": current_slot_start,
            "end": slot_end,
            "slug": f"{prefix}{current_slot_start}",
            "next_slug": f"{prefix}{next_slot_start}",
            "seconds_until_end": seconds_until_end,
            "seconds_until_snipe": max(0, seconds_until_snipe),
            "in_snipe_window": seconds_until_snipe <= 0 and seconds_until_end > 5,
        }
    return result


async def _fetch_market_by_slug(slug: str) -> Optional[dict]:
    """Fetch market data from Gamma API by slug."""
    url = f"{GAMMA_API}/markets?slug={slug}&closed=false&limit=1"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    markets = data if isinstance(data, list) else data.get("data", [])
                    for m in markets:
                        if slug in m.get("slug", "").lower():
                            return m
    except Exception as e:
        log.debug("Failed to fetch market", slug=slug, error=str(e))
    return None


def _analyze_direction(asset: str, market: dict) -> tuple[str, float]:
    """Analyze which direction will win using SignalEngine + price data.

    Uses Hyperliquid engine when HYPE mode is active, otherwise Binance.
    Returns (direction, confidence) where direction is "up" or "down".
    """
    # Check if Hyperliquid mode is active (runtime override or env)
    use_hype = _is_hype_mode()

    if use_hype:
        from polybot.hyperliquid_engine import get_hyperliquid_engine
        hype = get_hyperliquid_engine()
        # Get YES price from market
        yes_price = _extract_yes_price(market)
        signal = hype.get_signal(asset, polymarket_up_price=yes_price)
        if signal.is_valid and signal.confidence >= MIN_CONFIDENCE_TO_SNIPE:
            log.info(
                "🎯 SNIPER [HYPE] signal",
                asset=asset, direction=signal.direction,
                confidence=f"{signal.confidence:.3f}",
                reason=signal.reason,
            )
            return signal.direction, signal.confidence
        # Fall through to Binance engine as backup

    engine = get_signal_engine()

    # Get YES price from market
    yes_price = 0.5
    for token in market.get("tokens", []):
        if token.get("outcome", "").lower() in ("yes", "up"):
            p = token.get("price")
            if p is not None:
                yes_price = float(p)
                break

    # Primary signal: SignalEngine (OFI, liquidations, latency arb)
    signal = engine.get_signal(asset, polymarket_up_price=yes_price)

    if signal.is_valid and signal.confidence >= MIN_CONFIDENCE_TO_SNIPE:
        log.info(
            "🎯 SNIPER signal from engine",
            asset=asset, direction=signal.direction,
            confidence=f"{signal.confidence:.3f}",
            reason=signal.reason,
        )
        return signal.direction, signal.confidence

    # Fallback: Use current market prices as indicator
    # In the last 30s, Polymarket prices usually reflect reality
    # YES price > 0.55 = market thinks UP, < 0.45 = DOWN
    if yes_price > 0.58:
        conf = min(0.85, 0.5 + (yes_price - 0.5) * 1.5)
        return "up", conf
    elif yes_price < 0.42:
        conf = min(0.85, 0.5 + (0.5 - yes_price) * 1.5)
        return "down", conf

    # Secondary: Check CEX price movement over the slot
    state = engine.states.get(asset)
    if state and state.last_price > 0 and state.price_15s_ago > 0:
        # Use full-slot price delta (not just 15s)
        # Look at the oldest price we have in the buffer
        if state.prices:
            oldest_price = state.prices[0][1]  # First (oldest) in deque
            delta = (state.last_price - oldest_price) / oldest_price
            if abs(delta) > 0.001:  # > 0.1% move
                direction = "up" if delta > 0 else "down"
                conf = min(0.80, 0.55 + abs(delta) * 50)
                log.info(
                    "🎯 SNIPER CEX price delta",
                    asset=asset, direction=direction,
                    delta=f"{delta*100:.3f}%",
                    confidence=f"{conf:.3f}",
                )
                return direction, conf

    # Not confident enough
    return "up" if yes_price <= 0.5 else "down", 0.50


async def _execute_snipe(
    asset: str,
    market: dict,
    direction: str,
    confidence: float,
) -> Optional[dict]:
    """Execute the snipe trade with volatility-aware sizing."""
    from polybot.executor import calc_kelly_size, place_trade_async
    from polybot.risk_manager import get_risk_manager
    from polybot.executor import get_polygon_balance_async

    rm = get_risk_manager()

    # Pre-trade risk check
    can_trade, reason = rm.check_can_trade()
    if not can_trade:
        log.warning("🎯 SNIPER blocked by risk", reason=reason)
        return None

    balance = await get_polygon_balance_async()
    if balance < get_settings().min_balance_usd:
        log.warning("🎯 SNIPER insufficient balance", balance=f"${balance:.2f}")
        return None

    # Kelly sizing with sniper confidence
    kelly = calc_kelly_size(
        confidence=confidence,
        balance=balance,
        base_trade_amount=get_settings().min_trade_usd,
    )

    amount = kelly["size"]

    # ── Volatility schedule: adjust sizing ──
    from polybot.volatility_schedule import is_volatility_schedule_enabled, get_current_regime
    if is_volatility_schedule_enabled():
        regime = get_current_regime()
        amount = round(amount * regime.intensity, 2)
        # Cap at max position
        amount = min(amount, get_settings().max_position_usd)
        if regime.is_hot:
            log.info("🔥 VOL BOOST",
                     window=regime.active_window,
                     multiplier=f"{regime.intensity:.1f}x",
                     sized_amount=f"${amount:.2f}")

    outcome = "up" if direction == "up" else "down"

    # Map direction to correct token
    tokens = market.get("tokens", [])
    token_id = None
    for t in tokens:
        t_outcome = t.get("outcome", "").lower()
        if direction == "up" and t_outcome in ("yes", "up"):
            token_id = t.get("token_id")
        elif direction == "down" and t_outcome in ("no", "down"):
            token_id = t.get("token_id")

    if not token_id:
        # Fallback: first token = UP, second = DOWN
        clob_raw = market.get("clobTokenIds", [])
        if isinstance(clob_raw, str):
            try:
                clob_ids = json.loads(clob_raw)
            except (json.JSONDecodeError, ValueError):
                clob_ids = []
        else:
            clob_ids = clob_raw
        if len(clob_ids) >= 2:
            token_id = clob_ids[0] if direction == "up" else clob_ids[1]

    if not token_id:
        log.error("🎯 SNIPER no token_id found", asset=asset, direction=direction)
        return None

    log.info(
        "🎯 SNIPER EXECUTING",
        asset=asset,
        direction=direction,
        confidence=f"{confidence:.3f}",
        amount=f"${amount:.2f}",
        kelly_edge=f"{kelly['edge']:.2f}%",
        slug=market.get("slug", ""),
    )

    result = await place_trade_async(
        market=market,
        outcome=outcome,
        amount=amount,
        token_id=token_id,
    )

    if result:
        log.info("🎯 SNIPER TRADE PLACED", result=str(result)[:200])
    else:
        log.warning("🎯 SNIPER trade failed")

    return result


async def run_sniper_loop():
    """Main sniper loop — runs continuously.

    Cycle:
    1. Calculate slot timing for all assets
    2. Wait until snipe window (T+4:30)
    3. Fetch market data + analyze direction
    4. Execute if confident
    5. Wait for next slot
    """
    settings = get_settings()
    targets = settings.target_symbols
    engine = get_signal_engine()
    sniped_slugs: set[str] = set()

    # Load active strategy for snipe window / confidence overrides
    try:
        from polybot.mode_strategies import get_active_strategy
        strategy = get_active_strategy()
        dynamic_window = strategy.snipe_window_seconds
        dynamic_min_conf = strategy.min_confidence
        log.info(
            "🎯 SNIPER MODE ACTIVE (strategy=%s, window=%ds, min_conf=%.2f)",
            strategy.label, dynamic_window, dynamic_min_conf, targets=targets,
        )
    except (ImportError, AttributeError):
        dynamic_window = SNIPE_WINDOW_BEFORE_END
        dynamic_min_conf = MIN_CONFIDENCE_TO_SNIPE
        log.info("🎯 SNIPER MODE ACTIVE", targets=targets)

    # Start signal engine(s) for real-time data
    try:
        await engine.start()
        # Also start Hyperliquid engine if in HYPE mode
        if _is_hype_mode():
            from polybot.hyperliquid_engine import get_hyperliquid_engine
            hype = get_hyperliquid_engine(assets=targets)
            await hype.start()
            log.info("🎯 HYPERLIQUID ENGINE started alongside Binance")
        # Wait for data warmup
        await asyncio.sleep(5)
    except Exception as e:
        log.warning("Signal engine start failed: %s", e)

    while True:
        try:
            # ── Volatility schedule check ──
            from polybot.volatility_schedule import (
                is_volatility_schedule_enabled, get_current_regime, should_skip_trade
            )
            vol_enabled = is_volatility_schedule_enabled()

            if vol_enabled and should_skip_trade():
                # Aggressive mode: skip quiet hours entirely
                await asyncio.sleep(30)
                continue

            # Dynamic confidence threshold based on volatility regime
            effective_min_confidence = dynamic_min_conf
            vol_regime = None
            if vol_enabled:
                vol_regime = get_current_regime()
                effective_min_confidence = max(
                    0.50,
                    dynamic_min_conf + vol_regime.confidence_offset,
                )

            slots = _get_current_slot_times(snipe_window=dynamic_window)

            for asset in targets:
                if asset not in slots:
                    continue

                slot = slots[asset]
                slug = slot["slug"]

                # Already sniped this slot?
                if slug in sniped_slugs:
                    continue

                if slot["in_snipe_window"]:
                    if vol_regime and vol_regime.is_hot:
                        log.info(
                            "🎯🔥 SNIPE WINDOW + VOL BOOST",
                            asset=asset, window=vol_regime.active_window,
                            intensity=f"{vol_regime.intensity:.1f}x",
                            min_conf=f"{effective_min_confidence:.2f}",
                        )
                    else:
                        log.info(
                            "🎯 SNIPE WINDOW OPEN",
                            asset=asset, slug=slug,
                            seconds_left=slot["seconds_until_end"],
                        )

                    # Fetch market
                    market = await _fetch_market_by_slug(slug)
                    if not market:
                        log.debug("Market not found", slug=slug)
                        continue

                    # Analyze direction
                    direction, confidence = _analyze_direction(asset, market)

                    if confidence < effective_min_confidence:
                        log.info(
                            "🎯 SNIPER SKIP — low confidence",
                            asset=asset, confidence=f"{confidence:.3f}",
                            min_required=f"{effective_min_confidence:.2f}",
                        )
                        sniped_slugs.add(slug)
                        continue

                    # Execute!
                    result = await _execute_snipe(asset, market, direction, confidence)
                    sniped_slugs.add(slug)

                    if result:
                        log.info("🎯 SNIPE COMPLETE", asset=asset, direction=direction)

            # Clean old slugs (keep last 100)
            if len(sniped_slugs) > 100:
                sniped_slugs.clear()

            # Sleep 2s between checks
            await asyncio.sleep(2)

        except asyncio.CancelledError:
            log.info("🎯 SNIPER stopping")
            break
        except Exception as e:
            log.error("🎯 SNIPER error: %s", e, exc_info=True)
            await asyncio.sleep(5)


# Convenience for starting from main
def is_sniper_mode() -> bool:
    """Check if sniper mode is enabled via env var."""
    import os
    return os.getenv("SNIPER_MODE", "").lower() in ("true", "1", "yes")
