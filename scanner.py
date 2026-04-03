"""Market scanner: discovers and trades mispriced 5-min Up/Down crypto markets.

Fetches active markets from the Polymarket Gamma API, filters for 5-minute
Up/Down BTC/ETH/SOL/XRP markets, computes edge via EdgeEngine + SignalEngine,
and executes trades through a single unified pipeline with Kelly position sizing.

v2.0 rewrite (April 2026) — audit fixes:
- SINGLE trade execution path (unified_trade_cycle_async) with Kelly sizing
- fetch_markets_async is now pure fetching, no side effects
- _traded_slugs persisted to disk (survives restarts)
- Deduplicated keyword lists
- Fixed MaxProfitScanner sort order (tier 1 = highest priority)
- Fixed _get_volume truthy check (0.0 is valid)
- Fixed datetime.utcnow() → datetime.now(timezone.utc)
- Removed ~500 lines of dead keyword lists and commented-out code
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import aiohttp
import requests

from polybot import config
from polybot.config import get_settings
from polybot.proxy import get_proxy_manager, make_proxied_request
from polybot.logging_setup import get_logger
from polybot.onchain_executor import _fetch_order_book
from polybot.signal_engine import get_signal_engine

logger = get_logger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
#  Constants
# ═══════════════════════════════════════════════════════════════════════════════

GAMMA_API = "https://gamma-api.polymarket.com"
SLOT_DURATION = 300  # 5-minute slots
SLOT_BUFFER_SECONDS = 90

# Baseline edge for position scaling (2% edge = 1.0x)
BASELINE_EDGE = 0.02

# Single source of truth for target coins
TARGET_COINS: list[str] = ["bitcoin", "btc", "ethereum", "eth", "solana", "sol", "xrp"]

# Slug prefixes for direct API lookup
TARGET_SLUG_PREFIXES: list[str] = [
    "btc-updown-5m-",
    "eth-updown-5m-",
    "sol-updown-5m-",
    "xrp-updown-5m-",
]

# Asset slug → ticker mapping
ASSET_FROM_SLUG: dict[str, str] = {
    "btc": "BTC",
    "eth": "ETH",
    "sol": "SOL",
    "xrp": "XRP",
}

# CEX symbol mappings
CRYPTO_SYMBOL_MAP: dict[str, str] = {
    "bitcoin": "BTC/USDT",
    "btc": "BTC/USDT",
    "ethereum": "ETH/USDT",
    "eth": "ETH/USDT",
    "solana": "SOL/USDT",
    "sol": "SOL/USDT",
    "xrp": "XRP/USDT",
    "dogecoin": "DOGE/USDT",
    "doge": "DOGE/USDT",
}

CATEGORY_KEYWORDS: dict[str, list[str]] = {
    "crypto": ["bitcoin", "btc", "ethereum", "eth", "crypto", "solana", "sol",
               "xrp", "dogecoin", "doge", "token", "defi", "blockchain"],
    "politics": ["president", "election", "congress", "senate", "trump", "biden",
                 "democrat", "republican", "vote", "governor", "political"],
    "sports": ["nfl", "nba", "mlb", "nhl", "soccer", "football", "basketball",
               "baseball", "tennis", "golf", "mma", "ufc", "super bowl"],
    "economics": ["fed", "interest rate", "inflation", "gdp", "unemployment",
                  "stock", "s&p", "nasdaq", "recession", "treasury", "fomc"],
    "tech": ["ai", "artificial intelligence", "openai", "google", "apple",
             "microsoft", "meta", "tesla", "ipo", "startup"],
    "world": ["war", "ukraine", "russia", "china", "nato", "military",
              "summit", "treaty", "international"],
}


# ═══════════════════════════════════════════════════════════════════════════════
#  Traded-slug persistence (survives restarts)
# ═══════════════════════════════════════════════════════════════════════════════

_TRADED_SLUGS_PATH = Path(os.getenv("TRADED_SLUGS_PATH", "/tmp/polybot_traded_slugs.json"))


def _load_traded_slugs() -> set[str]:
    """Load traded slugs from disk. Prunes entries older than 1 hour."""
    try:
        if _TRADED_SLUGS_PATH.exists():
            data = json.loads(_TRADED_SLUGS_PATH.read_text())
            cutoff = time.time() - 3600  # 1h TTL
            return {
                slug
                for slug, ts in data.items()
                if ts > cutoff
            }
    except Exception as e:
        logger.warning("Failed to load traded slugs", error=str(e))
    return set()


def _save_traded_slugs(slugs: dict[str, float]) -> None:
    """Persist traded slugs to disk."""
    try:
        _TRADED_SLUGS_PATH.write_text(json.dumps(slugs))
    except Exception as e:
        logger.warning("Failed to save traded slugs", error=str(e))


# slug → timestamp when traded
_traded_slugs: dict[str, float] = {}


def _init_traded_slugs() -> None:
    global _traded_slugs
    loaded = _load_traded_slugs()
    _traded_slugs = {slug: time.time() for slug in loaded}


def _mark_slug_traded(slug: str) -> None:
    _traded_slugs[slug] = time.time()
    _save_traded_slugs(_traded_slugs)


def _is_slug_traded(slug: str) -> bool:
    if slug in _traded_slugs:
        # Prune expired
        if _traded_slugs[slug] < time.time() - 3600:
            del _traded_slugs[slug]
            return False
        return True
    return False


# Initialize on import
_init_traded_slugs()


# ═══════════════════════════════════════════════════════════════════════════════
#  Slot / filter helpers
# ═══════════════════════════════════════════════════════════════════════════════


def is_slot_tradeable(slug: str, buffer_seconds: int = SLOT_BUFFER_SECONDS) -> bool:
    """Return True only if the slot end time is still far enough in the future."""
    try:
        ts = int(slug.split("-")[-1])
        end_time = ts + SLOT_DURATION
        return end_time > time.time() + buffer_seconds
    except (ValueError, IndexError):
        return False


def _get_gamma_endpoint() -> str:
    settings = get_settings()
    if settings.use_api_mirrors:
        return get_proxy_manager().get_mirror_endpoint("gamma")
    return GAMMA_API


def _get_volume(market: dict) -> float:
    """Extract market volume. Returns 0.0 for missing data (not falsy-skip)."""
    for key in ("volume", "volumeNum", "volume24hr"):
        v = market.get(key)
        if v is not None:
            return float(v)
    tokens = market.get("tokens", [])
    return sum(float(t.get("volume", 0)) for t in tokens)


def _get_yes_price(market: dict) -> float | None:
    for token in market.get("tokens", []):
        if token.get("outcome", "").lower() == "yes":
            p = token.get("price")
            if p is not None:
                return float(p)
    return None


def _get_no_price(market: dict) -> float | None:
    for token in market.get("tokens", []):
        if token.get("outcome", "").lower() == "no":
            p = token.get("price")
            if p is not None:
                return float(p)
    return None


def categorize_market(question: str) -> str:
    q = question.lower()
    for category, keywords in CATEGORY_KEYWORDS.items():
        for kw in keywords:
            if len(kw) <= 3:
                if re.search(r"\b" + re.escape(kw) + r"\b", q):
                    return category
            elif kw in q:
                return category
    return "other"


# ═══════════════════════════════════════════════════════════════════════════════
#  5-Minute Market Filter (single, clean version)
# ═══════════════════════════════════════════════════════════════════════════════


def _should_apply_5min_prefilter(settings) -> bool:
    mode = getattr(settings, "mode", "")
    up_down_only = getattr(settings, "up_down_only", True)
    return mode in ("updown", "all") or up_down_only


def _filter_5min_markets(markets: list[dict]) -> list[dict]:
    """Filter markets to only 5-minute Up/Down crypto markets.

    A market passes if: has_coin AND has_updown AND has_5min
    Uses TARGET_COINS as single source of truth (no duplicate lists).
    """
    filtered = []
    total = len(markets)

    for m in markets:
        q = m.get("question", "").lower()

        has_coin = any(c in q for c in TARGET_COINS)
        has_updown = "up or down" in q or "up/down" in q
        has_5min = (
            "5 minutes" in q
            or "5 min" in q
            or "5-minute" in q
            or "5m" in q
            or "- 5 " in q
        )

        if has_coin and has_updown and has_5min:
            filtered.append(m)
            logger.debug(
                "[5MIN MATCH]",
                slug=m.get("slug", "")[:80],
                title=q[:120],
            )

    logger.info(
        f"5MIN FILTER: {total} → {len(filtered)} markets",
    )
    return filtered


# ═══════════════════════════════════════════════════════════════════════════════
#  Market Fetching (PURE — no trade side effects)
# ═══════════════════════════════════════════════════════════════════════════════


def fetch_all_active_markets(min_volume: float = 10_000) -> list[dict]:
    """Fetch all active Polymarket markets above volume threshold (sync).

    This is a PURE data-fetching function. It does NOT execute trades.
    """
    all_markets: list[dict] = []
    offset = 0
    limit = 100
    base_url = _get_gamma_endpoint()

    while offset < 5000:
        try:
            resp = make_proxied_request(
                f"{base_url}/markets",
                params={
                    "active": "true",
                    "closed": "false",
                    "limit": limit,
                    "offset": offset,
                },
                timeout=30,
                max_retries=3,
            )
            markets = resp.json()
            if not markets:
                break
            all_markets.extend(markets)
            offset += limit
        except requests.RequestException as e:
            logger.error("Error fetching markets", offset=offset, error=str(e))
            break

    settings = get_settings()
    if _should_apply_5min_prefilter(settings):
        original_count = len(all_markets)
        all_markets = _filter_5min_markets(all_markets)
        logger.info(
            f"[5MIN FILTER] sync: {original_count} → {len(all_markets)}",
        )

    return [m for m in all_markets if _get_volume(m) >= min_volume]


async def fetch_5min_markets_async() -> list[dict]:
    """Fetch active 5-minute updown markets via direct slug lookup (async).

    This is a PURE data-fetching function. It does NOT execute trades.
    Returns list of tradeable market dicts.
    """
    current_ts = int(time.time())
    slot = (current_ts // 300) * 300
    filtered: list[dict] = []

    async with aiohttp.ClientSession() as session:
        for prefix in TARGET_SLUG_PREFIXES:
            for offset in [0, -300, 300, 600, -600]:
                ts = slot + offset
                full_slug = f"{prefix}{ts}"
                url = (
                    f"{GAMMA_API}/markets"
                    f"?slug={full_slug}&closed=false&limit=5"
                )
                try:
                    timeout = aiohttp.ClientTimeout(total=8)
                    async with session.get(url, timeout=timeout) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            markets = (
                                data.get("data", [])
                                if isinstance(data, dict)
                                else data
                            )
                            for m in markets:
                                slug = m.get("slug", "").lower()
                                if full_slug in slug and is_slot_tradeable(slug):
                                    filtered.append(m)
                                    logger.debug("[SLOT MATCH]", slug=slug)
                except Exception as e:
                    logger.debug(
                        "Slug lookup failed",
                        slug=full_slug,
                        error=str(e),
                    )

    logger.info(f"[FETCH] {len(filtered)} tradeable 5min markets found")
    return filtered


# ═══════════════════════════════════════════════════════════════════════════════
#  Unified Trade Cycle (SINGLE execution path)
# ═══════════════════════════════════════════════════════════════════════════════


async def unified_trade_cycle_async() -> list[dict]:
    """Single unified trade cycle: fetch → signal → size → execute.

    This is the ONE trade execution path. All trades go through here.
    Uses Kelly position sizing from config, respects dry_run, daily limits.

    Returns list of trade result dicts.
    """
    from polybot import executor
    from polybot.risk import calculate_kelly_position

    settings = get_settings()
    results: list[dict] = []

    # ── Validate private key ──
    pk = os.getenv("POLYMARKET_PRIVATE_KEY")
    if not pk or len(pk) < 40:
        logger.critical("[STARTUP] POLYMARKET_PRIVATE_KEY missing or too short")
        return []

    if not pk.startswith("0x"):
        pk = "0x" + pk
        os.environ["POLYMARKET_PRIVATE_KEY"] = pk

    logger.info("[STARTUP] Private key validated")

    # ── Safety: check flags ──
    if not settings.auto_execute:
        logger.debug("Auto-execute disabled (AUTO_EXECUTE=false)")
        return []

    logger.info(
        "TRADE CYCLE START",
        mode="DRY RUN" if settings.dry_run else "LIVE",
        max_position=settings.max_position_usd,
        kelly_mult=settings.kelly_multiplier,
    )

    # ── Start signal engine ──
    engine = None
    try:
        engine = get_signal_engine()
        if not engine._running:
            asyncio.create_task(engine.start())
            await asyncio.sleep(2.0)
            logger.info("[SIGNAL ENGINE] Started")
    except Exception as exc:
        logger.warning("[SIGNAL ENGINE] Failed to start: %s", exc)

    # ── Pre-cycle balance check ──
    balance = await executor.get_polygon_balance_async()
    if balance < config.MIN_BALANCE_USD:
        logger.warning(
            "Insufficient USDC balance",
            balance=balance,
            min_required=config.MIN_BALANCE_USD,
        )
        return []

    # ── Fetch markets ──
    markets = await fetch_5min_markets_async()
    if not markets:
        logger.info("[CYCLE] No tradeable markets found")
        return []

    # ── Trade each market ──
    for m in markets:
        slug = m.get("slug", "")

        # Validate slug is one of our targets
        if not any(coin in slug for coin in ASSET_FROM_SLUG):
            continue

        # Slot expiry guard
        if not is_slot_tradeable(slug):
            logger.debug("Skipping expired slot", slug=slug)
            continue

        # Duplicate guard (persistent)
        if _is_slug_traded(slug):
            logger.debug("Already traded this slot", slug=slug)
            continue

        # ── Parse CLOB token IDs ──
        clob_raw = m.get("clobTokenIds") or []
        if isinstance(clob_raw, str):
            try:
                clob_ids = json.loads(clob_raw)
            except (json.JSONDecodeError, ValueError):
                clob_ids = []
        else:
            clob_ids = clob_raw

        if len(clob_ids) != 2:
            logger.debug("Invalid clobTokenIds", slug=slug, ids=clob_ids)
            continue

        # ── Determine direction via orderbook + signal engine ──
        try:
            book = _fetch_order_book(clob_ids[0])
            asks = book.get("asks", [])
            bids = book.get("bids", [])
            best_ask = float(asks[0]["price"]) if asks else 0.5
            best_bid = float(bids[0]["price"]) if bids else 0.5
            up_price = (best_ask + best_bid) / 2

            # Skip resolved/expired
            if up_price <= 0.02 or up_price >= 0.98:
                logger.debug("Resolved/expired market", slug=slug, price=up_price)
                continue

            # Determine asset
            asset = next(
                (v for k, v in ASSET_FROM_SLUG.items() if k in slug), None
            )

            # Try signal engine first
            side = None
            signal_confidence = 0.5
            if asset and engine is not None:
                try:
                    signal = engine.get_signal(asset, polymarket_up_price=up_price)
                    if signal and signal.is_valid:
                        side = signal.direction
                        signal_confidence = signal.confidence
                        logger.info(
                            "[SIGNAL] %s → %s (conf=%.2f) | PM_UP=$%.3f",
                            asset, side.upper(), signal.confidence, up_price,
                        )
                except Exception as sig_err:
                    logger.debug("[SIGNAL] Error: %s", sig_err)

            # Price-based fallback (only outside ambiguous zone)
            if side is None:
                if up_price < 0.45:
                    side = "up"
                elif up_price > 0.55:
                    side = "down"
                else:
                    # Ambiguous zone — no signal, no edge, skip.
                    try:
                        from polybot.calibration_lookup import get_calibration
                        _cal = get_calibration()
                        price_cents = int(round(up_price * 100))
                        if _cal.is_bad_price_zone(price_cents):
                            logger.info(
                                "[CALIBRATION] Bad price zone → SKIP | price=%d¢",
                                price_cents,
                            )
                    except Exception:
                        pass
                    logger.info(
                        "[SKIP] Ambiguous zone (45-55¢) | price=%.3f slug=%s",
                        up_price, slug,
                    )
                    continue

        except Exception as e:
            logger.warning(
                "[SKIP] Orderbook error → skip | slug=%s error=%s",
                slug, str(e)[:200],
            )
            continue

        # ── Kelly position sizing ──
        # Compute edge using the EdgeEngine (not the naive linear model)
        from polybot.edge_engine import get_edge_engine
        edge_engine = get_edge_engine()
        edge = edge_engine.get_real_edge(m)

        if edge <= 0:
            logger.debug("No edge after EdgeEngine calc", slug=slug)
            continue

        kelly_size = calculate_kelly_position(
            edge=edge,
            bankroll=balance,
            kelly_mult=settings.kelly_multiplier,
        )

        if kelly_size <= 0:
            logger.debug("Kelly size zero", slug=slug, edge=edge)
            continue

        # Scale by edge quality
        edge_ratio = edge / BASELINE_EDGE
        scaled_position = kelly_size * settings.position_scaling_factor * edge_ratio

        # Apply limits
        position_usd = max(scaled_position, settings.min_trade_usd)
        position_usd = min(position_usd, settings.max_position_usd)

        # Adaptive scaling (if enabled)
        if settings.adaptive_scaling and edge > settings.min_ev:
            position_usd = position_usd * (1 + (edge * 2))
            position_usd = max(position_usd, settings.min_trade_usd)
            position_usd = min(position_usd, settings.max_position_usd)

        # Don't exceed current balance
        position_usd = min(position_usd, balance * 0.95)

        token_id = clob_ids[0] if side == "up" else clob_ids[1]

        # ── Log decision ──
        risk_pct = (position_usd / balance * 100) if balance > 0 else 0
        trade_mode = "DRY RUN" if settings.dry_run else "LIVE"
        logger.info(
            f"🚀 {trade_mode} TRADE",
            slug=slug[:60],
            side=side.upper(),
            edge=round(edge, 4),
            kelly=round(kelly_size, 2),
            position=round(position_usd, 2),
            balance=round(balance, 2),
            risk_pct=round(risk_pct, 2),
        )

        # ── Execute ──
        try:
            result = await executor.place_trade_async(
                market=m,
                outcome=side,
                amount=position_usd,
                token_id=token_id,
                dry_run=settings.dry_run,
            )

            if result:
                logger.info(f"[TRADE OK] {slug} ${position_usd:.2f} {side.upper()}")
                _mark_slug_traded(slug)
                results.append({
                    "market": m.get("question", "")[:60],
                    "slug": slug,
                    "side": side.upper(),
                    "position_usd": round(position_usd, 2),
                    "edge": round(edge, 4),
                    "status": "executed",
                })
            else:
                logger.error(f"[TRADE FAILED] {slug} result=None")
                results.append({
                    "market": m.get("question", "")[:60],
                    "slug": slug,
                    "status": "failed",
                })

        except Exception as e:
            logger.error(f"[TRADE ERROR] {slug}: {type(e).__name__} - {str(e)[:300]}")
            results.append({
                "market": m.get("question", "")[:60],
                "slug": slug,
                "status": "error",
                "error": str(e)[:200],
            })

        # ── Refresh balance, stop if broke ──
        balance = await executor.get_polygon_balance_async()
        if balance < config.MIN_BALANCE_USD:
            logger.warning("Balance too low, stopping cycle", balance=balance)
            break

    # ── Write journal + summary ──
    _write_trade_journal(results, settings)
    _write_daily_summary(results)

    logger.info(
        "TRADE CYCLE COMPLETE",
        markets_found=len(markets),
        trades=len(results),
        executed=len([r for r in results if r.get("status") == "executed"]),
    )
    return results


# ═══════════════════════════════════════════════════════════════════════════════
#  Journal / Summary Writers
# ═══════════════════════════════════════════════════════════════════════════════


def _write_trade_journal(results: list[dict], settings) -> None:
    if not results:
        return
    journal_entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "trades": results,
    }
    try:
        path = getattr(settings, "trade_journal_path", "/tmp/polybot_journal.jsonl")
        with open(path, "a") as f:
            f.write(json.dumps(journal_entry) + "\n")
    except Exception as e:
        logger.warning("Journal write failed", error=str(e))


def _write_daily_summary(results: list[dict]) -> None:
    settings = get_settings()
    executed = len([r for r in results if r.get("status") == "executed"])
    best_edge = max((r.get("edge", 0) for r in results), default=0)
    line = (
        f"{datetime.now(timezone.utc).isoformat()} | "
        f"Trades: {executed} | "
        f"Best Edge: {best_edge:.4f}\n"
    )
    try:
        path = getattr(settings, "daily_summary_txt", "/tmp/polybot_daily.txt")
        with open(path, "a") as f:
            f.write(line)
    except Exception as e:
        logger.warning("Daily summary write failed", error=str(e))


# ═══════════════════════════════════════════════════════════════════════════════
#  Market Analysis (scanning without trading)
# ═══════════════════════════════════════════════════════════════════════════════


def calculate_price_deviation(market: dict) -> dict[str, Any]:
    current_price = _get_yes_price(market)
    if current_price is None:
        return {"current_price": None, "deviation_pct": 0, "direction": "unknown"}

    historical_mean = 0.5
    prices = market.get("priceHistory", [])
    if prices:
        vals = []
        for entry in prices:
            if isinstance(entry, dict):
                v = entry.get("price") or entry.get("yes")
                if v:
                    vals.append(float(v))
            elif isinstance(entry, (int, float)):
                vals.append(float(entry))
        if vals:
            historical_mean = sum(vals) / len(vals)

    deviation = current_price - historical_mean
    deviation_pct = (deviation / historical_mean) * 100 if historical_mean > 0 else 0
    return {
        "current_price": current_price,
        "historical_mean": historical_mean,
        "deviation_pct": deviation_pct,
        "direction": "underpriced" if deviation < 0 else "overpriced",
    }


def calculate_arb_spread(market: dict) -> dict[str, Any]:
    yes_price = _get_yes_price(market)
    no_price = _get_no_price(market)
    if yes_price is None or no_price is None:
        return {"spread": 0, "profit_pct": 0, "has_arb": False}
    combined = yes_price + no_price
    spread = 1.0 - combined
    profit_pct = spread * 100
    return {
        "yes_price": yes_price,
        "no_price": no_price,
        "combined": combined,
        "spread": spread,
        "profit_pct": profit_pct,
        "has_arb": spread > 0.005,
    }


def scan_all_markets(min_volume: float = 10_000) -> list[dict]:
    """Scan markets with category and deviation/arb data (sync)."""
    markets = fetch_all_active_markets(min_volume)
    return [
        {
            **m,
            "category": categorize_market(m.get("question", "")),
            "price_deviation": calculate_price_deviation(m),
            "arb_spread": calculate_arb_spread(m),
        }
        for m in markets
    ]


def get_top_mispriced_markets(
    count: int = 8,
    min_volume: float = 10_000,
    min_deviation_pct: float = 10.0,
    prioritize_politics: bool = False,
) -> list[dict]:
    markets = scan_all_markets(min_volume)
    mispriced = [
        m
        for m in markets
        if abs(m["price_deviation"].get("deviation_pct", 0)) >= min_deviation_pct
        and m["price_deviation"].get("current_price") is not None
    ]

    def sort_key(m: dict) -> tuple:
        dev = abs(m["price_deviation"].get("deviation_pct", 0))
        is_priority = (
            prioritize_politics and m.get("category") == "politics" and dev > 8.0
        )
        return (not is_priority, -dev)

    mispriced.sort(key=sort_key)
    return mispriced[:count]


def get_arb_opportunities(
    min_profit_pct: float = 0.5, min_volume: float = 10_000
) -> list[dict]:
    markets = scan_all_markets(min_volume)
    arbs = [
        m
        for m in markets
        if m["arb_spread"]["has_arb"]
        and m["arb_spread"]["profit_pct"] >= min_profit_pct
    ]
    arbs.sort(key=lambda m: -m["arb_spread"]["profit_pct"])
    return arbs


# ═══════════════════════════════════════════════════════════════════════════════
#  MaxProfitScanner (analysis-only, uses EdgeEngine)
# ═══════════════════════════════════════════════════════════════════════════════


class MaxProfitScanner:
    """Scanner for high-EV opportunities. Analysis only — does not trade.

    For trade execution, use unified_trade_cycle_async().
    """

    DEFAULT_ARB_THRESHOLD = 0.98
    WEIGHT_ARB = 0.50
    WEIGHT_CEX_EDGE = 0.30
    WEIGHT_TA = 0.20

    def __init__(
        self,
        min_volume: float | None = None,
        min_liquidity: float | None = None,
        min_ev: float | None = None,
        up_down_only: bool = False,
    ):
        self.up_down_only = up_down_only
        settings = get_settings()

        self.min_volume = min_volume or (settings.min_liquidity_usd * 5)
        self.min_liquidity = min_liquidity or settings.min_liquidity_usd
        self.min_ev = min_ev or (settings.min_edge_percent / 100)

        self.scan_results: list[dict] = []
        self.markets_scanned = 0
        self.last_scan_time: datetime | None = None

    def _calculate_hybrid_score(
        self, arb_score: float, cex_edge_score: float, ta_score: float
    ) -> float:
        return (
            arb_score * self.WEIGHT_ARB
            + cex_edge_score * self.WEIGHT_CEX_EDGE
            + ta_score * self.WEIGHT_TA
        )

    def _evaluate_markets(self, markets: list[dict]) -> list[dict]:
        """Core scan logic shared between sync and async paths."""
        from polybot.edge_engine import get_edge_engine
        edge_engine = get_edge_engine()

        self.markets_scanned = len(markets)
        opportunities: list[dict] = []

        for market in markets:
            # Liquidity filter
            liquidity = float(
                market.get("liquidity", 0) or market.get("volumeNum", 0) or 0
            )
            if liquidity < self.min_liquidity:
                continue

            question = market.get("question", "")[:80]
            market_id = market.get("condition_id") or market.get("id", "")

            # Tier 1: Arbitrage (risk-free)
            arb_data = calculate_arb_spread(market)
            if (
                arb_data.get("has_arb")
                and arb_data.get("combined", 1.0) < self.DEFAULT_ARB_THRESHOLD
            ):
                profit_pct = arb_data.get("profit_pct", 0)
                opportunities.append({
                    "market_id": market_id,
                    "market": question,
                    "type": "ARB",
                    "tier": 1,
                    "ev": round(profit_pct / 100, 4),
                    "edge": round(profit_pct / 100, 4),
                    "profit_pct": round(profit_pct, 2),
                    "yes_price": arb_data.get("yes_price"),
                    "no_price": arb_data.get("no_price"),
                    "combined": arb_data.get("combined"),
                    "volume": _get_volume(market),
                    "liquidity": liquidity,
                    "hybrid_score": self._calculate_hybrid_score(100, 0, 0),
                    "raw_market": market,
                })
                continue

            # Tier 2: EdgeEngine-based edge
            edge = edge_engine.get_real_edge(market)
            if edge > self.min_ev:
                cex_edge_score = min(100, edge * 200)
                opportunities.append({
                    "market_id": market_id,
                    "market": question,
                    "type": "EDGE",
                    "tier": 2,
                    "ev": round(edge, 4),
                    "edge": round(edge, 4),
                    "profit_pct": round(edge * 100, 2),
                    "volume": _get_volume(market),
                    "liquidity": liquidity,
                    "hybrid_score": self._calculate_hybrid_score(0, cex_edge_score, 0),
                    "raw_market": market,
                })

        # Sort: tier ascending (1=best), then hybrid_score descending
        opportunities.sort(
            key=lambda x: (x.get("tier", 99), -x.get("hybrid_score", 0))
        )
        return opportunities

    def scan(self, limit: int = 5) -> list[dict]:
        """Scan for high-EV opportunities (sync). Does NOT trade."""
        self.last_scan_time = datetime.now(timezone.utc)
        markets = fetch_all_active_markets(min_volume=self.min_volume)
        if self.up_down_only:
            markets = _filter_5min_markets(markets)
        opps = self._evaluate_markets(markets)
        self.scan_results = opps[:limit]
        logger.info(
            "MaxProfit scan complete",
            scanned=self.markets_scanned,
            found=len(opps),
            returned=len(self.scan_results),
        )
        return self.scan_results

    async def scan_async(self, limit: int = 5) -> list[dict]:
        """Scan for high-EV opportunities (async). Does NOT trade."""
        self.last_scan_time = datetime.now(timezone.utc)
        markets = await fetch_5min_markets_async()
        opps = self._evaluate_markets(markets)
        self.scan_results = opps[:limit]
        logger.info(
            "MaxProfit async scan complete",
            scanned=self.markets_scanned,
            found=len(opps),
            returned=len(self.scan_results),
        )
        return self.scan_results

    def get_scan_status(self) -> dict[str, Any]:
        return {
            "status": f"Scanned {self.markets_scanned} markets",
            "high_ev_count": len(self.scan_results),
            "last_scan": self.last_scan_time.isoformat() if self.last_scan_time else None,
            "results": self.scan_results,
        }


# ═══════════════════════════════════════════════════════════════════════════════
#  Formatting
# ═══════════════════════════════════════════════════════════════════════════════


def format_scan_results(markets: list[dict]) -> str:
    if not markets:
        return "No mispriced markets found."
    lines = ["🔎 **Top Mispriced Markets**\n"]
    for i, m in enumerate(markets, 1):
        q = m.get("question", "Unknown")[:60]
        d = m.get("price_deviation", {})
        dev_pct = d.get("deviation_pct", 0)
        direction = d.get("direction", "unknown")
        emoji = "📉" if direction == "underpriced" else "📈"
        lines.append(f"**{i}. {q}...**")
        lines.append(f"   {emoji} {direction.upper()} by {abs(dev_pct):.1f}%")
        lines.append(f"   Category: {m.get('category', 'other').capitalize()}")
        lines.append("")
    return "\n".join(lines)


def format_max_profit_results(results: list[dict]) -> str:
    if not results:
        return "🔍 No high-EV opportunities found."
    lines = ["🚀 **MAX PROFIT SCAN RESULTS**\n"]
    for i, r in enumerate(results, 1):
        tier_emoji = "💰" if r.get("tier") == 1 else "📊"
        type_label = r.get("type", "UNKNOWN")
        market = r.get("market", "Unknown")[:55]
        edge = r.get("edge", 0) * 100
        lines.append(f"**{i}. {tier_emoji} [{type_label}]** {market}...")
        lines.append(f"   Edge: {edge:.1f}%")
        if type_label == "ARB":
            yes_p = r.get("yes_price", 0)
            no_p = r.get("no_price", 0)
            lines.append(f"   YES: ${yes_p:.3f} + NO: ${no_p:.3f} = ${yes_p + no_p:.3f}")
        lines.append("")
    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════════════════
#  Backtest / Hyperopt CLI entry points
# ═══════════════════════════════════════════════════════════════════════════════


def run_edge_backtest() -> dict | None:
    """CLI: python -m polybot.scanner --backtest"""
    async def _run():
        from polybot.backtester import EdgeBacktester
        settings = get_settings()
        backtester = EdgeBacktester()
        results = await backtester.run_backtest_async(
            days=settings.backtest_days, min_liquidity=settings.backtest_min_liquidity
        )
        logger.info(
            "BACKTEST COMPLETE",
            total_trades=results["total_trades"],
            winrate=results["winrate"],
            total_pnl=results["total_pnl"],
        )
        return results
    return asyncio.run(_run())


def run_hyperopt() -> dict | None:
    """CLI: python -m polybot.scanner --hyperopt"""
    async def _run():
        from polybot.optimizer import HyperOptimizer
        settings = get_settings()
        if not settings.hyperopt_enabled:
            logger.warning("HYPEROPT DISABLED")
            return None
        optimizer = HyperOptimizer()
        results = await optimizer.run_walkforward_optimization_async()
        logger.info(
            "HYPEROPT COMPLETE",
            best_params=results["best_params"],
            best_score=results["best_score"],
        )
        return results
    return asyncio.run(_run())


async def run_hyperopt_async() -> dict | None:
    """Async entry point for hyperparameter optimization."""
    from polybot.optimizer import HyperOptimizer
    settings = get_settings()
    if not settings.hyperopt_enabled:
        logger.warning("HYPEROPT DISABLED")
        return None
    optimizer = HyperOptimizer()
    return await optimizer.run_walkforward_optimization_async()


# ═══════════════════════════════════════════════════════════════════════════════
#  Backward compatibility aliases
# ═══════════════════════════════════════════════════════════════════════════════

# Old callers that used fetch_all_active_markets_async for trading
# should now call unified_trade_cycle_async instead.
# This alias fetches markets WITHOUT trading (safe).
fetch_all_active_markets_async = fetch_5min_markets_async

# Old callers
scan_all_markets_async = scan_all_markets
get_top_mispriced_markets_async = get_top_mispriced_markets
get_arb_opportunities_async = get_arb_opportunities
