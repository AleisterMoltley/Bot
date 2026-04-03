"""Market scanner: discovers mispriced markets from Polymarket Gamma API.

Fetches all active markets, categorizes them, and identifies price deviations.
Used by both signal mode (find mispriced prediction markets) and arbitrage mode
(find YES+NO < $1 opportunities).

Now includes MaxProfitScanner with:
- Tier-1: Arbitrage-first detection (risk-free opportunities)
- Tier-2: CEX Edge (Binance real price vs Polymarket implied odds)
- Tier-3: Liquidity + Volume Filter ($50k+ volume, $10k+ liquidity)
- EV (Expected Value) calculation
- Hybrid scoring system

Uses async aiohttp for non-blocking API calls (March 2026).
Falls back to sync requests for CLI commands.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import random

import re
import time
from datetime import datetime, timezone
from typing import Any

import aiohttp
import requests

from polybot import config
from polybot.config import get_settings
from polybot.proxy import (
    get_proxy_manager,
    make_proxied_request,
)
from polybot.logging_setup import get_logger
from polybot.onchain_executor import _fetch_order_book
from polybot.signal_engine import get_signal_engine

logger = get_logger(__name__)


# ── Slot expiry guard (BUG-1 fix) ────────────────────────────────────────────
SLOT_DURATION = 300  # 5-minute slots
SLOT_BUFFER_SECONDS = 90  # default safety buffer

# ── Traded-slug session cache (BUG-2 fix) ────────────────────────────────────
_traded_slugs: set[str] = set()


def is_slot_tradeable(slug: str, buffer_seconds: int = SLOT_BUFFER_SECONDS) -> bool:
    """Return True only if the slot end time is still in the future.

    Slugs follow the format ``btc-updown-5m-<unix_timestamp>``.
    The trailing number is the market **start** time; each slot runs
    for 5 minutes, so ``end_time = timestamp + 300``.
    """
    try:
        ts = int(slug.split("-")[-1])
        end_time = ts + SLOT_DURATION
        return end_time > time.time() + buffer_seconds
    except (ValueError, IndexError):
        return False


# Default endpoint (may be overridden by proxy manager mirrors)
GAMMA_API = "https://gamma-api.polymarket.com"

# V7: Baseline edge for position scaling (2% edge = 1.0 scaling ratio)
# Used in auto-position-scaling formula: scaled_position = kelly_size * factor * (edge / BASELINE_EDGE)
BASELINE_EDGE = 0.02

# Crypto symbol mappings for CEX price lookups
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
    "crypto": [
        "bitcoin",
        "btc",
        "ethereum",
        "eth",
        "crypto",
        "solana",
        "sol",
        "xrp",
        "dogecoin",
        "doge",
        "token",
        "defi",
        "blockchain",
    ],
    "politics": [
        "president",
        "election",
        "congress",
        "senate",
        "trump",
        "biden",
        "democrat",
        "republican",
        "vote",
        "governor",
        "political",
    ],
    "sports": [
        "nfl",
        "nba",
        "mlb",
        "nhl",
        "soccer",
        "football",
        "basketball",
        "baseball",
        "tennis",
        "golf",
        "mma",
        "ufc",
        "super bowl",
    ],
    "economics": [
        "fed",
        "interest rate",
        "inflation",
        "gdp",
        "unemployment",
        "stock",
        "s&p",
        "nasdaq",
        "recession",
        "treasury",
        "fomc",
    ],
    "tech": [
        "ai",
        "artificial intelligence",
        "openai",
        "google",
        "apple",
        "microsoft",
        "meta",
        "tesla",
        "ipo",
        "startup",
    ],
    "world": [
        "war",
        "ukraine",
        "russia",
        "china",
        "nato",
        "military",
        "summit",
        "treaty",
        "international",
    ],
}

# ═══════════════════════════════════════════════════════════════════════════════
# 5MIN SUPER-FILTER V10.7 FINAL – DYNAMISCHER TITEL-MATCH (Datum/Uhrzeit ignoriert)
# ═══════════════════════════════════════════════════════════════════════════════
#
# ZWECK (Purpose):
#   - Exakt deine Märkte: "Bitcoin Up or Down - 5 Minutes" + beliebiges Datum/Uhrzeit danach
#   - Nur BTC/ETH/SOL/XRP – nichts anderes wird jemals gehandelt
#   - Filter läuft DIREKT nach dem API-Fetch und VOR der teuren EV-Berechnung
#     → spart CPU und API-Rate-Limits
#
# GOAL: Filter ~2395 Polymarket markets down to 0-4 exact 5-minute Up/Down
#       crypto markets (BTC/ETH/SOL/XRP). This runs BEFORE expensive EV calculations.
#
# V10.7 FINAL (DYNAMIC TITLE-MATCH):
#   - DYNAMIC MATCH logic: has_coin AND (has_core OR has_slug) AND NOT block_patterns
#   - Target coins: bitcoin, btc, ethereum, eth, solana, sol, xrp
#   - Core phrases: "up or down - 5 minutes", "up or down - 5 min",
#     "up or down 5 minutes", "up/down - 5 minutes"
#   - Slug patterns: "updown-5m", "5m-updown", "updown5m", "5-minute"
#   - Block patterns: FDV, market cap, one day after, airdrop, by june, 2026,
#     presidential, world cup
#   - Debug: logs up to 100 coin/up-or-down/5-minutes markets for visibility
#
# HOW IT WORKS:
#   A market passes if: has_coin AND (has_core OR has_slug) AND NOT block_patterns
#   - has_coin: Question contains any target coin name (bitcoin, btc, ethereum, etc.)
#   - has_core: Question contains any core phrase (up or down - 5 minutes, etc.)
#   - has_slug: Slug contains any slug pattern (updown-5m, 5m-updown, etc.)
#   - block_patterns: Filters out FDV/Launch/Airdrop/Elections/Sports events
#
# DEBUG OUTPUT:
#   - Logs ALL markets that contain coin keywords, "up or down", or "5 minutes" (up to 100)
#   - Shows exact question + slug for each potential market
#   - Helps identify new market formats
#
# ACTIVATION:
#   - MODE=updown (default) or MODE=all
#   - UP_DOWN_ONLY=true (default)
#   - See config.py: PRODUCTION DEFAULTS section
#
# PERFORMANCE:
#   - Reduziert Scan-Volumen von ~2395 auf 0–4 deiner exakten 5min Märkte
#   - Logging auf DEBUG für volle Transparenz
#   - Debug: Bis zu 100 potenzielle Märkte werden geloggt
#   - Typical log output: "5MIN SUPER-FILTER V10.7: 2395 → 4 deiner exakten Märkte"
#
# ═══════════════════════════════════════════════════════════════════════════════

# ─── Separate keyword categories for V10.1 clarity ───────────────────────────────
# Diese Listen werden unten zu FIVE_MIN_KEYWORDS kombiniert für den eigentlichen Filter.
# Getrennte Listen erleichtern Wartung und dokumentieren die Absicht.
# These lists are combined into FIVE_MIN_KEYWORDS below for the actual filter.
# Keeping them separate makes maintenance easier and documents the intent.

_5MIN_TIME_KEYWORDS: list[str] = [
    # Standard 5-minute patterns
    "5 min",
    "5min",
    "5mins",
    "5 mins",
    "5-minute",
    "5 minute",
    "five minute",
    "five min",
    # V9.5: Extended real Polymarket title patterns
    "- 5 min",
    " - 5min",
    "5 min.",
    "5min ",
    "5 min ",  # with trailing space for better matching
    # Time-phrase patterns (common Polymarket formats)
    "in 5 minutes",
    "next 5 minutes",
    "within 5 minutes",
    "over the next 5 minutes",
    # V9.6: Extended patterns for broader matching
    "in 5 mins",
    # Combined time+context patterns
    "5 min price",
    "5min price",
    "5 minute price",
    "5-minute price",
    "direction in 5 min",
    "5 min direction",
    "5-minute direction",
    "will go up or down in 5",
    # V10.1: ET time mentions for specific Polymarket market windows
    "8:05pm",
    "8:10pm",
    "7:50pm",
    "7:55pm",
    " et",  # ET time zone (with leading space to avoid matching "bet", "better", etc.)
]

_5MIN_DIRECTION_KEYWORDS: list[str] = [
    # Up/Down patterns (V10.1: EXACT MATCH - only directional keywords)
    "up or down",
    "Up or Down",
    "up/down",
    # V9.5: Bull/Bear patterns
    "bull or bear",
]

# V10.5: Block patterns for FDV/Launch/Airdrop/Elections/Sports events
_5MIN_BLOCK_PATTERNS: list[str] = [
    "fdv",
    "market cap",
    "one day after",
    "airdrop",
    "by june",
    "2026",
    "presidential",
    "world cup",
]

# V10.2 FINAL: Coin keywords for flexible matching
_5MIN_COIN_KEYWORDS: list[str] = [
    "bitcoin",
    "btc",
    "ethereum",
    "eth",
    "solana",
    "sol",
    "xrp",
]

# V10.3 FINAL: Time keywords for flexible matching
_5MIN_TIME_MATCH_KEYWORDS: list[str] = [
    "5 minutes",
    "5 min",
    "5-minute",
    "5min",
    "- 5 min",
    "5 mins",
    "in 5 minutes",
    "next 5 minutes",
    "5 Minutes",  # Capital M variant
]

# V10.3 FINAL: Direction keywords for flexible matching
_5MIN_DIRECTION_MATCH_KEYWORDS: list[str] = [
    "up or down",
    "up/down",
    "bull or bear",
    "go up",
    "go down",
]

# V10.3 FINAL: Allowed slug patterns (substring match)
# NOTE: "5m" is intentionally broad but protected by the coin keyword requirement.
# A market only passes if it ALSO has a coin keyword (btc/eth/sol/xrp).
_5MIN_ALLOWED_SLUGS: list[str] = [
    "updown-5m",
    "btc-updown",
    "eth-updown",
    "sol-updown",
    "xrp-updown",
    "5m",  # V10.3: Added broad 5m slug match (protected by coin check)
]

# V10.2 FINAL: Exact title patterns for 5-minute markets (kept for backward compat)
# Only match these exact 4 market titles (case-insensitive)
_5MIN_EXACT_TITLES: list[str] = [
    "bitcoin up or down - 5 minutes",
    "ethereum up or down - 5 minutes",
    "solana up or down - 5 minutes",
    "xrp up or down - 5 minutes",
]

# V10.6: Exact slugs for 5-minute markets (from Polymarket URLs)
# Trailing dash ensures we match the exact slug prefix (e.g., btc-updown-5m-*)
_5MIN_EXACT_SLUGS: list[str] = [
    "btc-updown-5m-",
    "eth-updown-5m-",
    "sol-updown-5m-",
    "xrp-updown-5m-",
]

# V10.9: Dynamic matching constants – target coins, updown phrases, time keywords, slug patterns
# Used by _filter_5min_markets() for dynamic title-match (ignoring date/time suffixes)
_5MIN_TARGET_COINS: list[str] = [
    "bitcoin",
    "btc",
    "ethereum",
    "eth",
    "solana",
    "sol",
    "xrp",
]
_5MIN_UPDOWN_PHRASES: list[str] = [
    "up or down",
    "up/down",
]
_5MIN_V109_TIME_KEYWORDS: list[str] = [
    "5 minutes",
    "5 min",
    "5-minute",
    "5min",
    "5 mins",
    "5m",
    "- 5 min",
]
_5MIN_SLUG_PATTERNS: list[str] = [
    "updown-5m",
    "5m-updown",
    "btc-updown-5m",
    "eth-updown-5m",
    "sol-updown-5m",
    "xrp-updown-5m",
    "5-minute",
]
# V10.9: Keep core phrases for backward compat (used by FIVE_MIN_KEYWORDS)
_5MIN_CORE_PHRASES: list[str] = [
    "up or down - 5 minutes",
    "up or down - 5 min",
    "up or down 5 minutes",
    "up/down - 5 minutes",
]

_5MIN_COIN_SPECIFIC_KEYWORDS: list[str] = [
    # Coin-specific time patterns (common on Polymarket)
    "btc 5 min",
    "eth 5 min",
    "sol 5 min",
    "bitcoin 5 min",
    "ethereum 5 min",
    "solana 5 min",
]

_5MIN_LEGACY_KEYWORDS: list[str] = [
    # V10.1: Legacy keywords removed - exact match filter only uses specific patterns
    # Previously: "will", "hit", "reach" - too broad and caught non-5min markets
]

# ─── Long-term exclusion keywords (V10.2) ────────────────────────────────────────
# V10.2: This list is now replaced by _5MIN_BLOCK_PATTERNS above which is more
# comprehensive and includes FDV/Airdrop/Elections/Sports exclusions.
# Keeping this for backward compatibility with any code that references it.
_5MIN_LONG_TERM_EXCLUDE: list[str] = _5MIN_BLOCK_PATTERNS

# V10.5: Message constant for no real 5min opportunities found
# _NO_REAL_5MIN_MESSAGE = (
#     "TEST: Keine deiner 4 Märkte aktuell erkannt (Polymarket hat aktuell keine)"
# )

# ─── Combined keyword list for the actual filter ────────────────────────────────
# A market matches if ANY of these keywords appear in the question text.
# The symbol check (BTC/ETH/SOL) is done separately via target_symbols config.

FIVE_MIN_KEYWORDS: list[str] = (
    _5MIN_TIME_KEYWORDS
    + _5MIN_DIRECTION_KEYWORDS
    + _5MIN_COIN_SPECIFIC_KEYWORDS
    + _5MIN_LEGACY_KEYWORDS
)


def _should_apply_5min_prefilter(settings) -> bool:
    """Check if 5-minute market pre-filter should be applied.

    5MIN SUPER-FILTER V12: This determines whether to activate the pre-filter
    that reduces ~2395 markets to 0-4 exact 5-minute Up/Down crypto markets.

    Activation conditions (any of these):
        - MODE=updown (default production mode)
        - MODE=all (full market scan with 5min filter)
        - UP_DOWN_ONLY=true (explicit filter enable)

    Returns:
        True if filter should be applied, False otherwise.
    """
    mode = getattr(settings, "mode", "")
    # V4: Direct attribute access since up_down_only is now a proper Field
    up_down_only = settings.up_down_only if hasattr(settings, "up_down_only") else True
    return mode in ("updown", "all") or up_down_only


def _filter_5min_markets(
    markets: list[dict], target_symbols: list[str], log_rejections: bool = False
) -> list[dict]:
    """Filter markets to only include 5-minute Up/Down crypto markets.

    5MIN SUPER-FILTER V12 BRUTEFORCE – NUCLEAR

    Zweck:
        - Ignoriert alles außer "up or down" + "5" + BTC/ETH/SOL/XRP
        - Loggt ALLES, damit wir endlich sehen, ob die Märkte überhaupt im API-Feed sind
        - Kein Volume-Limit, kein Block-Pattern – rein keyword-basiert

    This filter runs DIRECTLY after API fetch and BEFORE expensive EV calculations.
    It typically reduces ~2395 markets to 0-4 actual tradeable 5-minute markets.

    V12 BRUTEFORCE NUCLEAR:
        - Bruteforce keyword match: has_coin AND has_updown AND has_5min
        - Coins: bitcoin, btc, ethereum, eth, solana, sol, xrp
        - Updown: "up or down", "up/down"
        - 5min: "5 minutes", "5 min", "5-minute", "5m", "- 5 "
        - No block patterns – pure keyword matching only
        - No volume limit

    Filter Logic:
        A market passes if:
            has_coin AND has_updown AND has_5min
        - has_coin: Question contains any coin keyword
        - has_updown: Question contains "up or down" or "up/down"
        - has_5min: Question contains any 5-minute time keyword

    Args:
        markets: List of market dictionaries from Gamma API
        target_symbols: List of crypto symbols to filter for (e.g., ["BTC", "ETH", "SOL"])
        log_rejections: If True, log rejection details at DEBUG level

    Returns:
        Filtered list containing only markets matching coin + updown + 5min keywords.

    Example Log Output:
        5MIN SUPER-FILTER V12 BRUTEFORCE: 2395 → 4 deiner exakten Märkte
    """
    # ===================================================================
    # V12 BRUTEFORCE MODE – NUCLEAR
    # Ignoriert alles außer "up or down" + "5" + BTC/ETH/SOL/XRP
    # Loggt ALLES, damit wir endlich sehen, ob die Märkte überhaupt im API-Feed sind
    # ===================================================================
    coins = ["bitcoin", "btc", "ethereum", "eth", "solana", "sol", "xrp"]
    filtered = []
    total_fetched = len(markets)
    raw_debug: list[str] = []

    for m in markets:
        q = m.get("question", "").lower()
        slug = m.get("slug", "").lower()

        # Bruteforce Match – extrem breit
        has_coin = any(c in q for c in coins)
        has_updown = "up or down" in q or "up/down" in q
        has_5min = (
            "5 minutes" in q
            or "5 min" in q
            or "5-minute" in q
            or "5m" in q
            or "- 5 " in q
        )

        # Market sample for debugging
        if len(raw_debug) < 200:
            raw_debug.append(q)

        if has_coin and has_updown and has_5min:
            filtered.append(m)
            logger.info(
                "[V12 BRUTEFORCE MATCH]",
                slug=slug,
                title=q[:220],
                volume=m.get("volume", 0),
            )

    logger.info(
        f"5MIN SUPER-FILTER V12 BRUTEFORCE: {total_fetched} → {len(filtered)}"
        " deiner exakten Märkte"
    )
    logger.info(f"TOTAL MARKETS GELADEN: {total_fetched} (max 5000 von API)")

    if raw_debug:
        logger.debug("Raw markets sample (first 20): %s", raw_debug[:20])

    if len(filtered) > 0:
        logger.info(
            f"✅ TEST: {len(filtered)} DEINER 5-MIN MÄRKTE LIVE GEFUNDEN!"
            " TRADE AKTIVIERT!"
        )
    else:
        # logger.warning(
        #     "❌ TEST: Keine deiner 4 Märkte gefunden"
        #     " – sie sind entweder nicht aktiv oder nicht im API-Feed"
        # )
        pass

    min_ev = 0.001  # noqa: F841  # V12: EV threshold for future use

    return [m for m in filtered if _get_volume(m) >= 0]


def _get_gamma_endpoint() -> str:
    """Get the current Gamma API endpoint (may be mirror)."""
    settings = get_settings()
    if settings.use_api_mirrors:
        return get_proxy_manager().get_mirror_endpoint("gamma")
    return GAMMA_API


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


def fetch_all_active_markets(min_volume: float = 10_000) -> list[dict]:
    """Fetch all active Polymarket markets above volume threshold (sync version).

    Uses proxy manager for geo-bypass and automatic mirror failover.
    For async operations, use fetch_all_active_markets_async().
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

    # ═══════════════════════════════════════════════════════════════════════════
    # 5MIN SUPER-FILTER V11 (sync version) – EXAKTE TITEL + FULL SCAN
    # ═══════════════════════════════════════════════════════════════════════════
    # Reduziert ~2395 Märkte auf 0-4 exakte 5min BTC/ETH/SOL/XRP Up or Down Märkte.
    # Reduces ~2395 markets to 0-4 exact 5min BTC/ETH/SOL/XRP Up or Down markets.
    # V11 FINAL: Exakte Titel-Suche + Full Scan (min_volume=0)
    # Blocks FDV/Launch/Airdrop/Elections/Sports events
    # Market sample logging (debug level only)
    # Runs BEFORE expensive EV calculations for performance.
    # Activated by: MODE=updown (default) or UP_DOWN_ONLY=true
    # ═══════════════════════════════════════════════════════════════════════════
    settings = get_settings()
    if _should_apply_5min_prefilter(settings):
        original_count = len(all_markets)
        # Enable DEBUG rejection logging if effective_log_level <= DEBUG
        effective_level = getattr(settings, "effective_log_level", logging.INFO)
        log_rejections = effective_level <= logging.DEBUG
        all_markets = _filter_5min_markets(
            all_markets, settings.target_symbols, log_rejections=log_rejections
        )
        logger.info(
            f"[5MIN FILTER] V11 (sync): {original_count} → {len(all_markets)} markets | "
            f"Mode={settings.mode} | Symbols={settings.target_symbols}"
        )

    return [m for m in all_markets if _get_volume(m) >= min_volume]


async def fetch_all_active_markets_async(min_volume: float = 10_000) -> list[dict]:
    """Fetch active 5-minute updown markets and execute trades via ClobClient."""
    from polybot import executor

    _TRADE_AMOUNT_USD = 30.0

    # Validate private key is available
    pk = os.getenv("POLYMARKET_PRIVATE_KEY")
    if not pk or len(pk) < 40:
        logger.critical("[STARTUP] POLYMARKET_PRIVATE_KEY missing or too short")
        return []

    # Ensure 0x prefix
    if not pk.startswith("0x"):
        pk = "0x" + pk
        os.environ["POLYMARKET_PRIVATE_KEY"] = pk

    logger.info(f"[STARTUP] Private key OK (length={len(pk)})")

    # Start CEX signal engine (connects to Binance WebSockets)
    try:
        engine = get_signal_engine()
        if not engine._running:
            asyncio.create_task(engine.start())
            await asyncio.sleep(2.0)  # Brief warmup before first trade
            logger.info("[SIGNAL ENGINE] Started and warming up")
    except Exception as exc:
        logger.warning(
            "[SIGNAL ENGINE] Failed to start: %s — falling back to price-based", exc
        )
        engine = None

    # === TIMESTAMP SLOT MATH ===
    target_prefixes = [
        "btc-updown-5m-",
        "eth-updown-5m-",
        "sol-updown-5m-",
        "xrp-updown-5m-",
    ]

    current_ts = int(time.time())
    slot = (current_ts // 300) * 300

    filtered: list[dict] = []

    async with aiohttp.ClientSession() as session:
        for prefix in target_prefixes:
            for offset in [0, -300, 300, 600, -600]:
                ts = slot + offset
                full_slug = f"{prefix}{ts}"
                url = (
                    f"https://gamma-api.polymarket.com/markets"
                    f"?slug={full_slug}&closed=false&limit=5"
                )
                try:
                    timeout = aiohttp.ClientTimeout(total=8)
                    async with session.get(url, timeout=timeout) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            markets = (
                                data.get("data", []) if isinstance(data, dict) else data
                            )
                            for m in markets:
                                slug = m.get("slug", "").lower()
                                if full_slug in slug:
                                    if not is_slot_tradeable(slug):
                                        logger.debug("Skipping expired slot", slug=slug)
                                        continue
                                    filtered.append(m)
                                    logger.info(
                                        "[CYCLE MATCH]",
                                        slug=slug,
                                        title=m.get("question", "")[:180],
                                    )
                except Exception:
                    pass

    # === BUG-2 FIX: Pre-loop balance guard ===
    balance = await executor.get_polygon_balance_async()
    if balance < config.MIN_BALANCE_USD:
        logger.warning(
            "Insufficient USDC balance, skipping entire cycle",
            balance=balance,
            min_required=config.MIN_BALANCE_USD,
        )
        return filtered

    # === TRADE EXECUTION (ClobClient with L2 credentials) ===
    for m in filtered:
        slug = m.get("slug", "")
        if not any(
            coin in slug
            for coin in [
                "btc-updown-5m",
                "eth-updown-5m",
                "sol-updown-5m",
                "xrp-updown-5m",
            ]
        ):
            continue

        # === BUG-1 FIX: Skip expired slots ===
        if not is_slot_tradeable(slug):
            ts = int(slug.split("-")[-1]) if slug.split("-")[-1].isdigit() else 0
            end_time = ts + SLOT_DURATION
            logger.debug("Skipping expired slot", slug=slug, end_time=end_time)
            continue

        # === BUG-2 FIX: Skip already-traded slugs ===
        if slug in _traded_slugs:
            logger.debug("Slug already traded this session, skipping", slug=slug)
            continue

        logger.info(f"[CYCLE MATCH] {slug}")

        clob_raw = m.get("clobTokenIds") or []
        if isinstance(clob_raw, str):
            try:
                clob_ids = json.loads(clob_raw)
            except (json.JSONDecodeError, ValueError):
                clob_ids = []
        else:
            clob_ids = clob_raw

        if len(clob_ids) != 2:
            continue

        try:
            # Use orderbook price to determine direction.
            # If best ask (UP token) is below 0.45, market implies DOWN is more likely → buy UP (contrarian edge).
            # If best ask (UP token) is above 0.55, market implies UP is more likely → buy DOWN (fade the crowd).
            # Between 0.45-0.55 we have no edge — randomize.
            try:
                book = _fetch_order_book(clob_ids[0])
                asks = book.get("asks", [])
                bids = book.get("bids", [])
                best_ask = float(asks[0]["price"]) if asks else 0.5
                best_bid = float(bids[0]["price"]) if bids else 0.5
                up_price = (best_ask + best_bid) / 2

                # Skip resolved/expired markets
                if up_price <= 0.02 or up_price >= 0.98:
                    logger.debug(
                        "Skipping resolved/expired market token %s (price=%.4f)",
                        clob_ids[0],
                        up_price,
                    )
                    continue

                # === CEX SIGNAL ENGINE ===
                # Determine asset from slug (btc-updown-5m-... → BTC)
                asset_map = {"btc": "BTC", "eth": "ETH", "sol": "SOL", "xrp": "XRP"}
                asset = next((v for k, v in asset_map.items() if k in slug), None)

                signal = None
                if asset and engine is not None:
                    try:
                        signal = engine.get_signal(asset, polymarket_up_price=up_price)
                    except Exception as sig_err:
                        logger.debug(
                            "[SIGNAL ENGINE] Error getting signal: %s", sig_err
                        )

                if signal and signal.is_valid:
                    side = signal.direction
                    logger.info(
                        "[SIGNAL ENGINE] %s → %s (conf=%.2f) reason=%s | PM_UP=$%.3f",
                        asset,
                        side.upper(),
                        signal.confidence,
                        signal.reason,
                        up_price,
                    )
                elif up_price < 0.45:
                    side = "up"  # Price-based fallback
                elif up_price > 0.55:
                    side = "down"  # Price-based fallback
                else:
                    side = "up" if random.random() < 0.5 else "down"  # No edge → 50/50

            except Exception:
                side = "up" if random.random() < 0.5 else "down"
            token_id = clob_ids[0] if side == "up" else clob_ids[1]

            result = await executor.place_trade_async(
                market=m,
                outcome=side,
                amount=_TRADE_AMOUNT_USD,
                token_id=token_id,
            )
            if result:
                logger.info(
                    f"[TRADE SUCCESS] {slug} amount={_TRADE_AMOUNT_USD} outcome={side.capitalize()}"
                )
                _traded_slugs.add(slug)
            else:
                logger.error(f"[TRADE FAILED] {slug} result was None")

        except Exception as e:
            import traceback

            logger.error(f"[TRADE ERROR] {slug}: {type(e).__name__} - {str(e)[:400]}")
            logger.error(f"[FULL TRACEBACK]\n{traceback.format_exc()}")
            continue

        # === BUG-2 FIX: Refresh balance after each trade, stop when broke ===
        balance = await executor.get_polygon_balance_async()
        if balance < config.MIN_BALANCE_USD:
            logger.warning(
                "Balance too low after trade, stopping cycle",
                balance=balance,
                min_required=config.MIN_BALANCE_USD,
            )
            break

    logger.info(f"✅ {len(filtered)} markets processed")
    return filtered


def _get_volume(market: dict) -> float:
    for key in ("volume", "volumeNum", "volume24hr"):
        v = market.get(key)
        if v:
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


def calculate_price_deviation(market: dict) -> dict[str, Any]:
    """Calculate how much current YES price deviates from 0.5 baseline."""
    current_price = _get_yes_price(market)
    if current_price is None:
        return {"current_price": None, "deviation_pct": 0, "direction": "unknown"}
    # Use 0.5 as neutral baseline
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
    """Calculate YES+NO spread for arbitrage opportunities."""
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
        "has_arb": spread > 0.005,  # > 0.5% profit after fees
    }


def scan_all_markets(min_volume: float = 10_000) -> list[dict]:
    """Scan markets with category and deviation/arb data (sync version)."""
    markets = fetch_all_active_markets(min_volume)
    scanned = []
    for m in markets:
        question = m.get("question", "")
        scanned.append(
            {
                **m,
                "category": categorize_market(question),
                "price_deviation": calculate_price_deviation(m),
                "arb_spread": calculate_arb_spread(m),
            }
        )
    return scanned


async def scan_all_markets_async(min_volume: float = 10_000) -> list[dict]:
    """Scan markets with category and deviation/arb data (async version)."""
    markets = await fetch_all_active_markets_async(min_volume)
    scanned = []
    for m in markets:
        question = m.get("question", "")
        scanned.append(
            {
                **m,
                "category": categorize_market(question),
                "price_deviation": calculate_price_deviation(m),
                "arb_spread": calculate_arb_spread(m),
            }
        )
    return scanned


def get_top_mispriced_markets(
    count: int = 8,
    min_volume: float = 10_000,
    min_deviation_pct: float = 10.0,
    prioritize_politics: bool = False,
) -> list[dict]:
    """Get top mispriced markets sorted by deviation (sync version)."""
    markets = scan_all_markets(min_volume)
    return _filter_mispriced(markets, count, min_deviation_pct, prioritize_politics)


async def get_top_mispriced_markets_async(
    count: int = 8,
    min_volume: float = 10_000,
    min_deviation_pct: float = 10.0,
    prioritize_politics: bool = False,
) -> list[dict]:
    """Get top mispriced markets sorted by deviation (async version)."""
    markets = await scan_all_markets_async(min_volume)
    return _filter_mispriced(markets, count, min_deviation_pct, prioritize_politics)


def _filter_mispriced(
    markets: list[dict],
    count: int,
    min_deviation_pct: float,
    prioritize_politics: bool,
) -> list[dict]:
    """Filter and sort mispriced markets (shared logic)."""
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
    min_profit_pct: float = 0.5,
    min_volume: float = 10_000,
) -> list[dict]:
    """Get markets where YES+NO < $1 (arbitrage opportunities) - sync version."""
    markets = scan_all_markets(min_volume)
    return _filter_arbs(markets, min_profit_pct)


async def get_arb_opportunities_async(
    min_profit_pct: float = 0.5,
    min_volume: float = 10_000,
) -> list[dict]:
    """Get markets where YES+NO < $1 (arbitrage opportunities) - async version."""
    markets = await scan_all_markets_async(min_volume)
    return _filter_arbs(markets, min_profit_pct)


def _filter_arbs(markets: list[dict], min_profit_pct: float) -> list[dict]:
    """Filter arbitrage opportunities (shared logic)."""
    arbs = [
        m
        for m in markets
        if m["arb_spread"]["has_arb"]
        and m["arb_spread"]["profit_pct"] >= min_profit_pct
    ]
    arbs.sort(key=lambda m: -m["arb_spread"]["profit_pct"])
    return arbs


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


# ============================================================================
# MaxProfitScanner: Hybrid Scanner for Maximum Profit
# ============================================================================


class MaxProfitScanner:
    """Advanced scanner prioritizing arbitrage and CEX-edge opportunities.

    Tier-1: Arbitrage-First (risk-free → 100% priority)
    Tier-2: CEX-Edge (Binance real price vs Polymarket implied odds)
    Tier-3: Liquidity + Volume Filter ($50k+ volume, $10k+ liquidity)

    Includes:
    - EV (Expected Value) calculation
    - Hybrid scoring (Arbitrage 50% + CEX Edge 30% + TA 20%)
    - Live scan status for dashboard integration
    - Up/Down Crypto filter for short-duration markets
    """

    # Scanner configuration
    DEFAULT_MIN_VOLUME = 5_000  # $5k minimum volume
    DEFAULT_MIN_LIQUIDITY = 1_000  # $1k minimum liquidity
    DEFAULT_MIN_EV = 0.015  # 1.5% Expected Value threshold
    DEFAULT_ARB_THRESHOLD = 0.98  # YES + NO < $0.98 for arbitrage

    # Volume is typically 5x liquidity (markets need trading activity)
    VOLUME_TO_LIQUIDITY_RATIO = 5

    # Up/Down Crypto filter settings
    UP_DOWN_MAX_DURATION_MINUTES = 240  # Max 4 hours for up/down markets

    # Hybrid scoring weights
    WEIGHT_ARB = 0.50  # 50% weight for arbitrage opportunities
    WEIGHT_CEX_EDGE = 0.30  # 30% weight for CEX edge
    WEIGHT_TA = 0.20  # 20% weight for technical analysis

    def __init__(
        self,
        min_volume: float | None = None,
        min_liquidity: float | None = None,
        min_ev: float | None = None,
        up_down_only: bool = False,
    ):
        """Initialize the MaxProfitScanner.

        Args:
            min_volume: Minimum market volume filter (from config)
            min_liquidity: Minimum market liquidity filter (from config)
            min_ev: Minimum Expected Value threshold (from config)
            up_down_only: Filter for Up/Down crypto markets only
        """
        self.up_down_only = up_down_only

        # Config injection: always use settings (no hardcoded defaults)
        from polybot.config import get_settings

        settings = get_settings()

        # Use explicit values if provided, otherwise fall back to config only
        if min_volume is not None:
            self.min_volume = min_volume
        else:
            # Always use config - no hardcoded fallback
            self.min_volume = (
                settings.min_liquidity_usd * self.VOLUME_TO_LIQUIDITY_RATIO
            )

        if min_liquidity is not None:
            self.min_liquidity = min_liquidity
        else:
            # Always use config - no hardcoded fallback
            self.min_liquidity = settings.min_liquidity_usd

        if min_ev is not None:
            self.min_ev = min_ev
        else:
            # Always use config - no hardcoded fallback
            self.min_ev = settings.min_edge_percent / 100

        self.scan_results: list[dict] = []
        self.markets_scanned = 0
        self.last_scan_time: datetime | None = None
        self._exchange = None

    @property
    def exchange(self):
        """Lazy-load the CCXT Binance exchange instance."""
        if self._exchange is None:
            try:
                import ccxt

                self._exchange = ccxt.binance({"enableRateLimit": True})
            except ImportError:
                logger.warning("ccxt not installed, CEX edge calculation disabled")
                self._exchange = None
            except Exception as e:
                logger.error("Failed to initialize Binance exchange", error=str(e))
                self._exchange = None
        return self._exchange

    def get_cex_price(self, symbol: str) -> float | None:
        """Get mid-price from Binance for a trading pair.

        Args:
            symbol: Trading pair symbol (e.g., "BTC/USDT")

        Returns:
            Mid-price (bid + ask) / 2 or None if unavailable
        """
        if not self.exchange:
            return None
        try:
            ticker = self.exchange.fetch_ticker(symbol)
            bid = ticker.get("bid", 0) or 0
            ask = ticker.get("ask", 0) or 0
            if bid > 0 and ask > 0:
                return (bid + ask) / 2
            return ticker.get("last")
        except Exception as e:
            logger.debug("Failed to fetch CEX price", symbol=symbol, error=str(e))
            return None

    def _detect_crypto_market(self, question: str) -> tuple[str | None, str | None]:
        """Detect if market is a crypto price prediction market.

        Args:
            question: Market question text

        Returns:
            Tuple of (crypto_name, cex_symbol) or (None, None)
        """
        q_lower = question.lower()
        for crypto, symbol in CRYPTO_SYMBOL_MAP.items():
            if crypto in q_lower:
                return crypto, symbol
        return None, None

    def is_up_down_crypto_market(self, market: dict) -> bool:
        """Check if market is a short-duration Up/Down crypto market.

        Filters for:
        - Bitcoin, XRP, Solana, Ethereum up/down markets
        - Short duration (max 4 hours)
        - Must contain "up or down" or "will hit" patterns

        Args:
            market: Market data dictionary

        Returns:
            True if market qualifies as Up/Down crypto market
        """
        question = market.get("question", "").lower()

        # Check for crypto keywords
        crypto_keywords = [
            "bitcoin up or down",
            "xrp up or down",
            "solana up or down",
            "ethereum up or down",
            "btc",
            "xrp",
            "sol",
            "eth",
        ]
        has_crypto = any(kw in question for kw in crypto_keywords)
        if not has_crypto:
            return False

        # Check for up/down or price hit patterns
        up_down_patterns = ["up or down", "will hit", "above or below"]
        has_pattern = any(pattern in question for pattern in up_down_patterns)
        if not has_pattern:
            return False

        # Check duration (if available) - max 4 hours (240 minutes)
        duration = market.get("duration", 0)
        if duration and duration > self.UP_DOWN_MAX_DURATION_MINUTES:
            return False

        return True

    def _extract_strike_price(self, question: str) -> float | None:
        """Extract strike price from market question.

        Examples:
        - "Will Bitcoin be above $70,000 on March 31?" → 70000
        - "BTC price > $65k?" → 65000
        """
        q_lower = question.lower()

        # Pattern for $65k format (with k suffix)
        k_match = re.search(r"\$([0-9,]+(?:\.[0-9]+)?)\s*k", q_lower)
        if k_match:
            price_str = k_match.group(1).replace(",", "")
            return float(price_str) * 1000

        # Pattern for $70,000 format (standard dollar)
        dollar_match = re.search(r"\$([0-9,]+(?:\.[0-9]+)?)", q_lower)
        if dollar_match:
            price_str = dollar_match.group(1).replace(",", "")
            return float(price_str)

        # Pattern for 70000 USD format
        usd_match = re.search(r"([0-9,]+(?:\.[0-9]+)?)\s*(?:usd|dollars?)", q_lower)
        if usd_match:
            price_str = usd_match.group(1).replace(",", "")
            return float(price_str)

        return None

    def calculate_ev(self, market: dict) -> dict[str, Any]:
        """Calculate Expected Value for a market.

        The EV calculation uses:
        - Edge = real_probability - market_implied_probability
        - EV = edge × liquidity_factor (normalized, max 1.0)

        The liquidity factor (liquidity / 1000, capped at 1.0) is used to:
        1. Scale EV to a comparable range (0-1) regardless of market size
        2. Favor higher liquidity markets where trades can be executed
        3. Provide a conservative estimate that doesn't overweight large markets

        Args:
            market: Market data dictionary

        Returns:
            Dict with ev, edge, real_prob, implied_prob, and confidence
        """
        result = {
            "ev": 0.0,
            "edge": 0.0,
            "real_prob": None,
            "implied_prob": None,
            "confidence": 0.0,
            "type": "unknown",
        }

        question = market.get("question", "")
        yes_price = _get_yes_price(market)
        if yes_price is None:
            return result

        result["implied_prob"] = yes_price

        # Detect crypto market
        crypto_name, cex_symbol = self._detect_crypto_market(question)
        if not crypto_name or not cex_symbol:
            return result

        # Get strike price from question
        strike_price = self._extract_strike_price(question)
        if not strike_price:
            return result

        # Get real price from CEX
        cex_price = self.get_cex_price(cex_symbol)
        if not cex_price:
            return result

        # Determine if market is "up" or "down" prediction
        q_lower = question.lower()
        is_above_market = any(
            word in q_lower for word in ["above", "over", "higher", "exceed", ">", "up"]
        )
        is_below_market = any(
            word in q_lower for word in ["below", "under", "lower", "<", "down"]
        )

        # Calculate real probability based on current price vs strike
        # Simple model: probability based on distance from strike
        price_ratio = cex_price / strike_price
        if is_above_market:
            # If current price > strike, higher probability of YES
            if cex_price > strike_price:
                real_prob = min(0.95, 0.5 + (price_ratio - 1) * 2)
            else:
                real_prob = max(0.05, 0.5 - (1 - price_ratio) * 2)
        elif is_below_market:
            # If current price < strike, higher probability of YES
            if cex_price < strike_price:
                real_prob = min(0.95, 0.5 + (1 - price_ratio) * 2)
            else:
                real_prob = max(0.05, 0.5 - (price_ratio - 1) * 2)
        else:
            # Unknown direction, use neutral
            return result

        # Calculate edge (difference between real and implied probability)
        edge = real_prob - yes_price

        # Calculate EV using liquidity factor for normalization
        # liquidity_factor = min(liquidity / 1000, 1.0) normalizes EV to 0-1 range
        # and ensures higher liquidity markets are weighted appropriately
        liquidity = float(market.get("liquidity", 0) or market.get("volumeNum", 0) or 0)
        liquidity_factor = min(liquidity / 1000, 1.0)
        ev = edge * liquidity_factor

        result.update(
            {
                "ev": round(ev, 4),
                "edge": round(edge, 4),
                "real_prob": round(real_prob, 4),
                "implied_prob": round(yes_price, 4),
                "cex_price": cex_price,
                "strike_price": strike_price,
                "confidence": min(100, abs(edge) * 200),
                "type": "cex_edge",
            }
        )

        return result

    def calculate_hybrid_score(
        self, arb_score: float, cex_edge_score: float, ta_score: float
    ) -> float:
        """Calculate hybrid score using weighted components.

        Args:
            arb_score: Arbitrage opportunity score (0-100)
            cex_edge_score: CEX edge score (0-100)
            ta_score: Technical analysis score (0-100)

        Returns:
            Weighted hybrid score (0-100)
        """
        return (
            arb_score * self.WEIGHT_ARB
            + cex_edge_score * self.WEIGHT_CEX_EDGE
            + ta_score * self.WEIGHT_TA
        )

    def scan(self, limit: int = 5) -> list[dict]:
        """Scan markets for high-EV opportunities (sync version).

        Prioritizes:
        1. Arbitrage opportunities (YES + NO < $0.98)
        2. CEX-Edge opportunities (real prob vs market implied)
        3. High liquidity + volume markets

        Args:
            limit: Maximum number of results to return

        Returns:
            List of high-EV market opportunities
        """
        logger.info(
            "Starting MaxProfit scan",
            min_volume=self.min_volume,
            min_liquidity=self.min_liquidity,
            min_ev=self.min_ev,
            up_down_only=self.up_down_only,
        )
        self.last_scan_time = datetime.now()

        # Fetch markets with basic volume filter
        markets = fetch_all_active_markets(min_volume=self.min_volume)
        self.markets_scanned = len(markets)

        high_ev_opportunities: list[dict] = []

        for market in markets:
            # Apply Up/Down crypto filter if enabled
            if self.up_down_only and not self.is_up_down_crypto_market(market):
                continue

            # Apply liquidity filter
            liquidity = float(
                market.get("liquidity", 0) or market.get("volumeNum", 0) or 0
            )
            if liquidity < self.min_liquidity:
                continue

            question = market.get("question", "")[:80]
            market_id = market.get("condition_id") or market.get("id", "")

            # Tier 1: Check for arbitrage opportunity
            arb_data = calculate_arb_spread(market)
            if (
                arb_data.get("has_arb")
                and arb_data.get("combined", 1.0) < self.DEFAULT_ARB_THRESHOLD
            ):
                profit_pct = arb_data.get("profit_pct", 0)
                high_ev_opportunities.append(
                    {
                        "market_id": market_id,
                        "market": question,
                        "type": "ARB",
                        "tier": 1,
                        "ev": round(profit_pct / 100, 4),
                        "edge": round(profit_pct / 100, 4),
                        "profit_pct": round(profit_pct, 2),
                        "pnl_potential": f"+${int(profit_pct * 10)}",
                        "yes_price": arb_data.get("yes_price"),
                        "no_price": arb_data.get("no_price"),
                        "combined": arb_data.get("combined"),
                        "volume": _get_volume(market),
                        "liquidity": liquidity,
                        "hybrid_score": self.calculate_hybrid_score(100, 0, 0),
                        "raw_market": market,
                    }
                )
                continue  # Arb opportunities get priority, skip other checks

            # Tier 2: Calculate CEX Edge
            ev_data = self.calculate_ev(market)
            if (
                ev_data.get("ev", 0) > self.min_ev
                or ev_data.get("edge", 0) > self.min_ev
            ):
                edge = ev_data.get("edge", 0)
                cex_edge_score = min(100, abs(edge) * 200)
                high_ev_opportunities.append(
                    {
                        "market_id": market_id,
                        "market": question,
                        "type": "EDGE",
                        "tier": 2,
                        "ev": ev_data.get("ev", 0),
                        "edge": edge,
                        "profit_pct": round(edge * 100, 2),
                        "pnl_potential": f"+${int(abs(edge) * 1000)}",
                        "real_prob": ev_data.get("real_prob"),
                        "implied_prob": ev_data.get("implied_prob"),
                        "cex_price": ev_data.get("cex_price"),
                        "strike_price": ev_data.get("strike_price"),
                        "volume": _get_volume(market),
                        "liquidity": liquidity,
                        "hybrid_score": self.calculate_hybrid_score(
                            0, cex_edge_score, 0
                        ),
                        "raw_market": market,
                    }
                )

        # Sort by hybrid score (highest first)
        high_ev_opportunities.sort(
            key=lambda x: (-x.get("tier", 3), -x.get("hybrid_score", 0))
        )

        # Store results and return top N
        self.scan_results = high_ev_opportunities[:limit]

        # ===================================================================
        # V9.1: STRICTER 5MIN FILTER + CLEAR LOGGING
        # ===================================================================
        # Logs whether real 5-minute opportunities with EV > min_ev were found.
        # Helps diagnose if filter is too strict or market is too efficient.
        # ===================================================================
        if len(high_ev_opportunities) == 0:
            # logger.warning(
            #     "[5MIN SCAN] NO REAL 5MIN OPPORTUNITY FOUND",
            #     message=_NO_REAL_5MIN_MESSAGE,
            #     markets_scanned=self.markets_scanned,
            #     filtered_5min=len(markets),
            #     min_ev_used=self.min_ev,
            # )
            pass
        else:
            logger.info(
                "[5MIN SCAN] REAL 5MIN OPPORTUNITIES FOUND",
                count=len(high_ev_opportunities),
                min_ev_used=self.min_ev,
            )

        logger.info(
            "MaxProfit scan complete",
            markets_scanned=self.markets_scanned,
            high_ev_found=len(high_ev_opportunities),
            results_returned=len(self.scan_results),
        )

        return self.scan_results

    async def scan_async(self, limit: int = 5) -> list[dict]:
        """Scan markets for high-EV opportunities (async version).

        Same logic as scan() but uses async market fetching.

        Args:
            limit: Maximum number of results to return

        Returns:
            List of high-EV market opportunities
        """
        logger.info(
            "Starting async MaxProfit scan",
            min_volume=self.min_volume,
            min_liquidity=self.min_liquidity,
            min_ev=self.min_ev,
            up_down_only=self.up_down_only,
        )
        self.last_scan_time = datetime.now()

        # Fetch markets with basic volume filter (async)
        markets = await fetch_all_active_markets_async(min_volume=self.min_volume)
        self.markets_scanned = len(markets)

        high_ev_opportunities: list[dict] = []

        for market in markets:
            # Apply Up/Down crypto filter if enabled
            if self.up_down_only and not self.is_up_down_crypto_market(market):
                continue

            # Apply liquidity filter
            liquidity = float(
                market.get("liquidity", 0) or market.get("volumeNum", 0) or 0
            )
            if liquidity < self.min_liquidity:
                continue

            question = market.get("question", "")[:80]
            market_id = market.get("condition_id") or market.get("id", "")

            # Tier 1: Check for arbitrage opportunity
            arb_data = calculate_arb_spread(market)
            if (
                arb_data.get("has_arb")
                and arb_data.get("combined", 1.0) < self.DEFAULT_ARB_THRESHOLD
            ):
                profit_pct = arb_data.get("profit_pct", 0)
                high_ev_opportunities.append(
                    {
                        "market_id": market_id,
                        "market": question,
                        "type": "ARB",
                        "tier": 1,
                        "ev": round(profit_pct / 100, 4),
                        "edge": round(profit_pct / 100, 4),
                        "profit_pct": round(profit_pct, 2),
                        "pnl_potential": f"+${int(profit_pct * 10)}",
                        "yes_price": arb_data.get("yes_price"),
                        "no_price": arb_data.get("no_price"),
                        "combined": arb_data.get("combined"),
                        "volume": _get_volume(market),
                        "liquidity": liquidity,
                        "hybrid_score": self.calculate_hybrid_score(100, 0, 0),
                        "raw_market": market,
                    }
                )
                continue

            # Tier 2: Calculate CEX Edge
            ev_data = self.calculate_ev(market)
            if (
                ev_data.get("ev", 0) > self.min_ev
                or ev_data.get("edge", 0) > self.min_ev
            ):
                edge = ev_data.get("edge", 0)
                cex_edge_score = min(100, abs(edge) * 200)
                high_ev_opportunities.append(
                    {
                        "market_id": market_id,
                        "market": question,
                        "type": "EDGE",
                        "tier": 2,
                        "ev": ev_data.get("ev", 0),
                        "edge": edge,
                        "profit_pct": round(edge * 100, 2),
                        "pnl_potential": f"+${int(abs(edge) * 1000)}",
                        "real_prob": ev_data.get("real_prob"),
                        "implied_prob": ev_data.get("implied_prob"),
                        "cex_price": ev_data.get("cex_price"),
                        "strike_price": ev_data.get("strike_price"),
                        "volume": _get_volume(market),
                        "liquidity": liquidity,
                        "hybrid_score": self.calculate_hybrid_score(
                            0, cex_edge_score, 0
                        ),
                        "raw_market": market,
                    }
                )

        # Sort by tier and hybrid score
        high_ev_opportunities.sort(
            key=lambda x: (-x.get("tier", 3), -x.get("hybrid_score", 0))
        )

        # Store results and return top N
        self.scan_results = high_ev_opportunities[:limit]

        # ===================================================================
        # V9.1: STRICTER 5MIN FILTER + CLEAR LOGGING
        # ===================================================================
        # Logs whether real 5-minute opportunities with EV > min_ev were found.
        # Helps diagnose if filter is too strict or market is too efficient.
        # ===================================================================
        if len(high_ev_opportunities) == 0:
            # logger.warning(
            #     "[5MIN SCAN] NO REAL 5MIN OPPORTUNITY FOUND",
            #     message=_NO_REAL_5MIN_MESSAGE,
            #     markets_scanned=self.markets_scanned,
            #     filtered_5min=len(markets),
            #     min_ev_used=self.min_ev,
            # )
            pass
        else:
            logger.info(
                "[5MIN SCAN] REAL 5MIN OPPORTUNITIES FOUND",
                count=len(high_ev_opportunities),
                min_ev_used=self.min_ev,
            )

        logger.info(
            "Async MaxProfit scan complete",
            markets_scanned=self.markets_scanned,
            high_ev_found=len(high_ev_opportunities),
            results_returned=len(self.scan_results),
        )

        return self.scan_results

    def get_scan_status(self) -> dict[str, Any]:
        """Get current scan status for dashboard.

        Returns:
            Dict with scan status, results count, and last scan time
        """
        return {
            "status": f"Scanned {self.markets_scanned} markets",
            "high_ev_count": len(self.scan_results),
            "last_scan": self.last_scan_time.isoformat()
            if self.last_scan_time
            else None,
            "results": self.scan_results,
        }


def format_max_profit_results(results: list[dict]) -> str:
    """Format MaxProfitScanner results for display.

    Args:
        results: List of high-EV opportunities

    Returns:
        Formatted string for CLI/Dashboard display
    """
    if not results:
        return "🔍 No high-EV opportunities found at this time."

    lines = ["🚀 **MAX PROFIT SCAN RESULTS**\n"]

    for i, r in enumerate(results, 1):
        tier_emoji = (
            "💰" if r.get("tier") == 1 else "📊" if r.get("tier") == 2 else "📈"
        )
        type_label = r.get("type", "UNKNOWN")
        market = r.get("market", "Unknown market")[:55]
        pnl = r.get("pnl_potential", "+$0")
        edge = r.get("edge", 0) * 100

        lines.append(f"**{i}. {tier_emoji} [{type_label}]** {market}...")
        lines.append(f"   Edge: {edge:.1f}% | Potential: {pnl}")

        if type_label == "ARB":
            yes_p = r.get("yes_price", 0)
            no_p = r.get("no_price", 0)
            lines.append(
                f"   YES: ${yes_p:.3f} + NO: ${no_p:.3f} = ${yes_p + no_p:.3f}"
            )
        elif type_label == "EDGE":
            real = r.get("real_prob", 0)
            implied = r.get("implied_prob", 0)
            if real and implied:
                lines.append(
                    f"   Real: {real * 100:.1f}% vs Market: {implied * 100:.1f}%"
                )

        lines.append("")

    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════════════════
#  🤖 AUTO-TRADE EXECUTION (Kelly Position Sizing)
# ═══════════════════════════════════════════════════════════════════════════════


async def execute_auto_trades_async(
    opportunities: list[dict],
    max_trades: int = 3,
) -> list[dict]:
    """Execute auto-trades for high-EV opportunities using Kelly position sizing.

    Only executes when auto_execute=True AND dry_run=False in settings.
    Uses Kelly Criterion for position sizing with configurable multiplier.

    Args:
        opportunities: List of high-EV market opportunities from scanner
        max_trades: Maximum trades to execute per scan cycle (default 3)

    Returns:
        List of execution results (trades placed or skipped)
    """
    from polybot.config import get_settings
    from polybot.risk import calculate_kelly_position
    from polybot.risk_manager import get_risk_manager

    settings = get_settings()
    results: list[dict] = []
    risk_mgr = get_risk_manager()

    # ── Risk Manager gate: stop if circuit breaker active ──
    can_trade, risk_reason = risk_mgr.check_can_trade()
    if not can_trade:
        logger.warning(
            "Auto-trade BLOCKED by risk manager",
            reason=risk_reason,
        )
        return results

    # ===================================================================
    # V9 FINAL: STARTUP VALIDATION
    # Log key settings at function entry for debugging/monitoring
    # ===================================================================
    logger.info(
        "V9 STARTUP VALIDATION",
        mode=settings.mode,
        dry_run=settings.dry_run,
        min_ev=settings.min_ev,
        max_position=settings.max_position_usd,
        adaptive_scaling=settings.adaptive_scaling,
    )

    # Safety check: Only execute when both flags are enabled
    if not settings.auto_execute:
        logger.debug("Auto-execute disabled (AUTO_EXECUTE=false)")
        return results

    if settings.dry_run:
        logger.info(
            "Auto-trade in DRY RUN mode — trades will be simulated",
            auto_execute=settings.auto_execute,
            dry_run=settings.dry_run,
        )

    # Import executor for trade placement
    from polybot.executor import place_trade_async

    for market_opp in opportunities[:max_trades]:
        try:
            raw_market = market_opp.get("raw_market", {})
            edge = market_opp.get("edge", 0)
            ev = market_opp.get("ev", 0)
            liquidity = market_opp.get("liquidity", 0)
            question = market_opp.get("market", "")[:60]

            # Get balance for Kelly calculation
            # Default fallback used when balance_usd not provided (sizing reference only)
            DEFAULT_BANKROLL_USD = 100.0
            bankroll = raw_market.get("balance_usd")
            if bankroll is None:
                bankroll = DEFAULT_BANKROLL_USD
                logger.debug(
                    "Using default bankroll for Kelly sizing",
                    default=DEFAULT_BANKROLL_USD,
                    market=question,
                )

            # Check if EV meets minimum threshold
            if ev < settings.min_edge_percent / 100:
                log_rejection(
                    raw_market,
                    f"EV too low for auto-trade ({ev:.2%} < {settings.min_edge_percent / 100:.2%})",
                    edge,
                    ev,
                    liquidity,
                )
                continue

            # ── Risk Manager: liquidity check ──
            liq_ok, liq_reason = risk_mgr.check_liquidity(liquidity)
            if not liq_ok:
                log_rejection(raw_market, liq_reason, edge, ev, liquidity)
                continue

            # ── Risk Manager: re-check can_trade (may have been paused by previous trade) ──
            can_trade, risk_reason = risk_mgr.check_can_trade()
            if not can_trade:
                logger.warning("Auto-trade stopped mid-cycle", reason=risk_reason)
                break

            # Calculate Kelly position size
            kelly_size = calculate_kelly_position(
                edge=abs(edge),
                bankroll=bankroll,
                kelly_mult=settings.kelly_multiplier,
            )

            # Skip trade if Kelly returns 0 (no edge or negative edge)
            if kelly_size <= 0:
                log_rejection(
                    raw_market,
                    "Kelly position size is zero (no edge)",
                    edge,
                    ev,
                    liquidity,
                )
                continue

            # ===================================================================
            # V7 BONUS: Auto-Position-Scaling
            # Scale position based on edge quality: better edge → larger position
            # Formula: scaled_position = kelly_size * scaling_factor * (edge / BASELINE_EDGE)
            # ===================================================================
            edge_ratio = abs(edge) / BASELINE_EDGE  # 2% edge = 1.0 ratio (baseline)
            scaled_position = kelly_size * settings.position_scaling_factor * edge_ratio

            # Apply position limits (min and max) only when Kelly > 0
            position_usd = max(scaled_position, settings.min_trade_usd)
            position_usd = min(position_usd, settings.max_position_usd)

            # ===================================================================
            # V9 FINAL: ADAPTIVE RISK SCALING
            # Slightly increase position size when edge quality is above threshold
            # Formula: position_usd = position_usd * (1 + (edge * 2))
            # ===================================================================
            if settings.adaptive_scaling and ev > settings.min_ev:
                position_usd = position_usd * (1 + (abs(edge) * 2))
                # Re-apply both limits after scaling for safety
                position_usd = max(position_usd, settings.min_trade_usd)
                position_usd = min(position_usd, settings.max_position_usd)

            # Determine trade side (YES if edge positive, NO if negative)
            side = "YES" if edge > 0 else "NO"

            # ===================================================================
            # V6 BONUS: ULTIMATIVER TRADE-DECISION-LOG (maximale Transparenz)
            # Zeigt exakt: Balance, Risk %, Confidence, Kelly-Size, Final Position
            # ===================================================================
            trade_mode = "DRY RUN" if settings.dry_run else "REAL"
            current_balance = bankroll  # Use the bankroll we already have
            risk_percent = (
                (position_usd / current_balance) * 100 if current_balance > 0 else 0
            )
            # Note: daily_risk_used tracking not yet implemented - placeholder for future extension
            daily_risk_used = 0.0

            logger.info(
                f"🚀 EXECUTING {trade_mode} TRADE – V7 ULTIMATE DECISION LOG",
                market=question[:80],
                edge=round(edge, 4),
                ev=round(ev, 4),
                current_balance=round(current_balance, 2),
                kelly_size=round(kelly_size, 2),
                scaled_position=round(scaled_position, 2),
                position_usd=round(position_usd, 2),
                scaling_factor=settings.position_scaling_factor,
                edge_ratio=round(edge_ratio, 2),
                risk_percent=round(risk_percent, 2),
                daily_risk_used_placeholder=round(daily_risk_used, 2),
                max_daily_risk=settings.max_daily_risk_usd,
                confidence="HIGH" if abs(edge) > 0.03 else "MEDIUM",
                side=side,
                min_ev_threshold=settings.min_ev,
                auto_execute=settings.auto_execute,
                dry_run=settings.dry_run,
                aggressive_mode=settings.aggressive_mode,
            )

            # Execute trade
            trade_result = await place_trade_async(
                market=raw_market,
                outcome=side.lower(),
                amount=position_usd,
                dry_run=settings.dry_run,
            )

            results.append(
                {
                    "market": question,
                    "side": side,
                    "position_usd": round(position_usd, 2),
                    "edge": round(edge, 4),
                    "ev": round(ev, 4),
                    "result": trade_result,
                    "status": "executed" if trade_result else "failed",
                }
            )

        except Exception as e:
            logger.error(
                "Auto-trade execution failed",
                market=market_opp.get("market", "")[:60],
                error=str(e),
            )
            results.append(
                {
                    "market": market_opp.get("market", "")[:60],
                    "status": "error",
                    "error": str(e),
                }
            )

    if results:
        logger.info(
            "Auto-trade execution complete",
            trades_attempted=len(results),
            successful=[r for r in results if r.get("status") == "executed"],
        )

    # ===================================================================
    # V7 BONUS: Scan Summary Log
    # Provides overview of scan results with daily risk reset status
    # ===================================================================
    successful_trades = len([r for r in results if r.get("status") == "executed"])
    failed_trades = len([r for r in results if r.get("status") in ("failed", "error")])
    best_ev = max([opp.get("ev", 0) for opp in opportunities], default=0)
    # Check if daily reset has occurred: current hour >= reset hour means reset is done for today
    current_hour = datetime.utcnow().hour
    daily_reset_status = (
        "done" if current_hour >= settings.daily_risk_reset_hour else "pending"
    )

    logger.info(
        "📊 SCAN SUMMARY V7",
        opportunities_found=len(opportunities),
        trades_executed=successful_trades,
        trades_failed=failed_trades,
        best_ev=round(best_ev, 4),
        scaling_factor=settings.position_scaling_factor,
        daily_risk_reset=daily_reset_status,
        daily_risk_reset_hour=settings.daily_risk_reset_hour,
    )

    # === TEST LOG: Zeigt klar, ob der Scan überhaupt Opportunities findet ===
    if len(opportunities) == 0:
        logger.warning(
            "TEST: NO OPPORTUNITY FOUND AFTER SCAN",
            message="Filter hat Märkte gefunden, aber KEINE mit EV > 0.005. Entweder Markt zu effizient oder Filter noch zu streng.",
            min_ev_used=settings.min_ev,
        )
    else:
        logger.info("TEST: OPPORTUNITIES FOUND", count=len(opportunities))

    # ===================================================================
    # V8 BONUS: INTERNAL TRADE JOURNAL + STARTUP VALIDATION
    # ===================================================================
    # 100% intern – keine externen Dienste
    # Schreibt täglich eine JSON-Zusammenfassung + prüft Config bei Start
    # ===================================================================
    if len(opportunities) > 0:
        journal_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "scan_summary": {
                "opportunities_found": len(opportunities),
                "trades_executed": successful_trades,
                "trades_failed": failed_trades,
                "best_ev": round(best_ev, 4),
            },
        }
        try:
            with open(settings.trade_journal_path, "a") as f:
                f.write(json.dumps(journal_entry) + "\n")
            logger.info("V8 INTERNAL JOURNAL UPDATED", path=settings.trade_journal_path)
        except Exception as e:
            logger.warning("V8 JOURNAL WRITE FAILED", error=str(e))

    # ===================================================================
    # V9 FINAL: TÄGLICHER TEXT-LOG
    # Writes a simple text summary line for monitoring
    # 100% internal – nur lokale .txt Datei
    # ===================================================================
    summary_line = (
        f"{datetime.now(timezone.utc).isoformat()} | "
        f"Opportunities: {len(opportunities)} | "
        f"Best EV: {best_ev:.4f} | "
        f"Trades: {successful_trades}\n"
    )
    try:
        with open(settings.daily_summary_txt, "a") as f:
            f.write(summary_line)
        logger.info("V9 DAILY SUMMARY TXT UPDATED", path=settings.daily_summary_txt)
    except Exception as e:
        logger.warning("V9 TXT WRITE FAILED", error=str(e))

    return results


# ═══════════════════════════════════════════════════════════════════════════════
#  🔍 DEBUG HELPER: Detailed Rejection Logging
# ═══════════════════════════════════════════════════════════════════════════════


def log_rejection(
    market: dict,
    reason: str,
    edge: float = 0.0,
    ev: float = 0.0,
    liquidity: float = 0.0,
) -> None:
    """Log market rejection details when DEBUG level is enabled.

    Helper function for detailed reject-logging in edge_engine & scanner.
    Only logs when log_level is DEBUG to avoid production log spam.

    Args:
        market: Market dictionary containing at least a 'question' key
        reason: Human-readable rejection reason
        edge: Calculated edge percentage (default 0.0)
        ev: Expected value (default 0.0)
        liquidity: Market liquidity in USD (default 0.0)
    """
    settings = get_settings()
    effective_level = getattr(settings, "effective_log_level", logging.INFO)
    if effective_level <= logging.DEBUG:
        logger.debug(
            "MARKET REJECTED",
            question=market.get("question", "")[:80],
            edge=round(edge, 4),
            ev=round(ev, 4),
            liquidity=round(liquidity, 0),
            reason=reason,
        )


# ═══════════════════════════════════════════════════════════════════════════════
#  🧪 BACKTEST MODE: Historical Edge Testing
# ═══════════════════════════════════════════════════════════════════════════════


def run_edge_backtest() -> dict | None:
    """Sync entry point for CLI: python -m polybot.scanner --backtest

    Triggers the EdgeBacktester to run historical tests on closed 5min Up/Down markets.

    Returns:
        Backtest results dict or None if backtest fails
    """
    import asyncio

    async def _run():
        from polybot.backtester import EdgeBacktester

        settings = get_settings()
        backtester = EdgeBacktester()
        results = await backtester.run_backtest_async(
            days=settings.backtest_days, min_liquidity=settings.backtest_min_liquidity
        )
        logger.info(
            "BACKTEST ABGESCHLOSSEN",
            total_trades=results["total_trades"],
            winrate=results["winrate"],
            total_pnl=results["total_pnl"],
        )
        return results

    return asyncio.run(_run())


# ═══════════════════════════════════════════════════════════════════════════════
#  🔬 HYPEROPT MODE: Walk-Forward Parameter Optimization
# ═══════════════════════════════════════════════════════════════════════════════


def run_hyperopt() -> dict | None:
    """Sync entry point for CLI: python -m polybot.scanner --hyperopt

    Triggers the HyperOptimizer to run walk-forward grid search optimization.

    Returns:
        Hyperopt results dict with best_params or None if optimization fails
    """
    import asyncio

    async def _run():
        from polybot.optimizer import HyperOptimizer

        settings = get_settings()
        if not settings.hyperopt_enabled:
            logger.warning(
                "HYPEROPT DISABLED - Set HYPEROPT_ENABLED=true to enable optimization"
            )
            return None

        optimizer = HyperOptimizer()
        results = await optimizer.run_walkforward_optimization_async()
        logger.info(
            "HYPEROPT ABGESCHLOSSEN",
            best_params=results["best_params"],
            best_score=results["best_score"],
            windows=results["windows"],
        )
        return results

    return asyncio.run(_run())


async def run_hyperopt_async() -> dict | None:
    """Async entry point for hyperparameter optimization.

    Called from async context (e.g., FastAPI, main async loop).

    Returns:
        Hyperopt results dict with best_params or None if optimization fails
    """
    from polybot.optimizer import HyperOptimizer

    settings = get_settings()
    if not settings.hyperopt_enabled:
        logger.warning(
            "HYPEROPT DISABLED - Set HYPEROPT_ENABLED=true to enable optimization"
        )
        return None

    optimizer = HyperOptimizer()
    results = await optimizer.run_walkforward_optimization_async()
    logger.info(
        "HYPEROPT ABGESCHLOSSEN",
        best_params=results["best_params"],
        best_score=results["best_score"],
        windows=results["windows"],
    )
    return results
