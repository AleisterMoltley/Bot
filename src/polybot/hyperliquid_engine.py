"""
hyperliquid_engine.py — Hyperliquid-based trading signal engine.

Connects to Hyperliquid WebSocket (wss://api.hyperliquid.xyz/ws) for:
1. Trade flow: Real-time trades for price tracking & momentum detection
2. Order book depth (L2): Bid/ask imbalance for OFI signals
3. AllMids: Cross-asset mid prices for correlation signals

This provides an alternative/complementary data source to Binance,
especially useful for assets with high Hyperliquid volume.

Activated via HYPE 45 mode in the mode selector, or by setting
HYPERLIQUID_ENABLED=true in env.
"""

from __future__ import annotations

import asyncio
import json
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Literal, Optional

import aiohttp

from polybot.logging_setup import get_logger

log = get_logger(__name__)

# ── Hyperliquid WebSocket ────────────────────────────────────────────────
HYPE_WS_URL = "wss://api.hyperliquid.xyz/ws"

# Supported assets — using Hyperliquid's perp coin names
HYPE_ASSETS = ["BTC", "ETH", "SOL", "XRP", "HYPE"]

# Hyperliquid uses plain coin names for perps (e.g. "BTC", "ETH", "SOL")
# No symbol mapping needed — Hyperliquid convention matches our asset names

# ── Signal thresholds (tuned for Hyperliquid's DEX liquidity) ──────────
# Hyperliquid has lower volume than Binance, so thresholds are adjusted
OFI_STRONG_THRESHOLD = 1.3       # slightly lower than Binance (1.5)
OFI_WEAK_THRESHOLD = 0.7         # slightly lower than Binance (0.8)
LATENCY_ARB_DELTA = 0.003        # 0.30% — Hyperliquid is faster than Polymarket
LATENCY_ARB_POLY_MAX = 0.57
LATENCY_ARB_POLY_MIN = 0.43
TRADE_FLOW_THRESHOLD_USD = 500_000  # $500K in 10s = big flow (no liquidation feed)

# Confidence levels
CONF_BIG_FLOW = 0.78
CONF_OFI_STRONG = 0.65
CONF_OFI_WEAK = 0.55
CONF_LATENCY = 0.68
CONF_NONE = 0.0


@dataclass
class HypeSignal:
    """A trading signal derived from Hyperliquid data."""

    asset: str
    direction: Literal["up", "down"] | None
    confidence: float
    reason: str
    source: str = "hyperliquid"
    timestamp: float = field(default_factory=time.time)

    @property
    def is_valid(self) -> bool:
        return self.direction is not None and self.confidence > 0.5


@dataclass
class HypeAssetState:
    """Rolling state for one asset on Hyperliquid."""

    coin: str  # Hyperliquid coin name

    # Trade history: (timestamp, price, size_usd, side)
    # side: "B" = buy, "A" = sell (Hyperliquid convention)
    trades: deque = field(default_factory=lambda: deque(maxlen=500))

    # Order book snapshots: (timestamp, bid_vol, ask_vol)
    orderbook_snaps: deque = field(default_factory=lambda: deque(maxlen=60))

    # Price history: (timestamp, price)
    prices: deque = field(default_factory=lambda: deque(maxlen=240))

    # Last known state
    last_price: float = 0.0
    price_15s_ago: float = 0.0
    price_updated_at: float = 0.0

    # Best bid/ask
    best_bid: float = 0.0
    best_ask: float = 0.0


class HyperliquidEngine:
    """
    Real-time signal engine using Hyperliquid WebSocket data.

    Usage:
        engine = HyperliquidEngine()
        await engine.start()
        signal = engine.get_signal("BTC", polymarket_up_price=0.52)

    Subscribes to:
    - trades: per-coin trade stream for price + flow tracking
    - l2Book: order book snapshots for OFI calculation
    - allMids: cross-asset mid prices (used for correlation)
    """

    def __init__(self, assets: list[str] | None = None) -> None:
        self.assets = assets or HYPE_ASSETS
        self.states: dict[str, HypeAssetState] = {
            asset: HypeAssetState(coin=asset) for asset in self.assets
        }
        self._tasks: list[asyncio.Task] = []
        self._running = False
        self._all_mids: dict[str, float] = {}  # coin -> mid price

    async def start(self) -> None:
        """Start Hyperliquid WebSocket connections."""
        if self._running:
            return
        self._running = True
        log.info("[HYPE ENGINE] Starting — connecting to Hyperliquid WebSocket")

        # Single connection handles all subscriptions
        self._tasks.append(
            asyncio.create_task(
                self._run_ws(),
                name="hype_ws_main",
            )
        )

        log.info("[HYPE ENGINE] WebSocket task started for %s", self.assets)

    async def stop(self) -> None:
        """Cancel WebSocket tasks."""
        self._running = False
        for task in self._tasks:
            task.cancel()
        self._tasks.clear()
        log.info("[HYPE ENGINE] Stopped")

    def get_signal(
        self,
        asset: str,
        polymarket_up_price: float = 0.5,
    ) -> HypeSignal:
        """
        Compute trading signal for an asset using Hyperliquid data.

        Args:
            asset: "BTC", "ETH", "SOL", "XRP", or "HYPE"
            polymarket_up_price: Current UP token price on Polymarket (0-1)

        Returns:
            HypeSignal with direction/confidence, or direction=None if no edge.
        """
        if asset not in self.states:
            return HypeSignal(
                asset=asset, direction=None, confidence=0.0, reason="unknown_asset"
            )

        state = self.states[asset]
        now = time.time()

        # === SIGNAL 1: Large Trade Flow (substitute for liquidation cascade) ===
        # Hyperliquid doesn't have a separate liquidation feed,
        # but large aggressive trades serve a similar purpose
        flow_window = 10.0
        buy_flow_usd = sum(
            size for ts, _, size, side in state.trades
            if now - ts <= flow_window and side == "B"
        )
        sell_flow_usd = sum(
            size for ts, _, size, side in state.trades
            if now - ts <= flow_window and side == "A"
        )

        net_flow = buy_flow_usd - sell_flow_usd

        if buy_flow_usd >= TRADE_FLOW_THRESHOLD_USD and net_flow > 0:
            log.info(
                "[HYPE ENGINE] %s BIG BUY FLOW $%.0fK (net +$%.0fK) → UP (conf=%.2f)",
                asset,
                buy_flow_usd / 1000,
                net_flow / 1000,
                CONF_BIG_FLOW,
            )
            return HypeSignal(
                asset=asset,
                direction="up",
                confidence=CONF_BIG_FLOW,
                reason=f"hype_buy_flow_${buy_flow_usd / 1e6:.1f}M",
            )

        if sell_flow_usd >= TRADE_FLOW_THRESHOLD_USD and net_flow < 0:
            log.info(
                "[HYPE ENGINE] %s BIG SELL FLOW $%.0fK (net -$%.0fK) → DOWN (conf=%.2f)",
                asset,
                sell_flow_usd / 1000,
                abs(net_flow) / 1000,
                CONF_BIG_FLOW,
            )
            return HypeSignal(
                asset=asset,
                direction="down",
                confidence=CONF_BIG_FLOW,
                reason=f"hype_sell_flow_${sell_flow_usd / 1e6:.1f}M",
            )

        # === SIGNAL 2: Order Flow Imbalance ===
        ofi = self._compute_ofi(state, window=30.0)

        if ofi >= OFI_STRONG_THRESHOLD:
            log.info(
                "[HYPE ENGINE] %s STRONG OFI=%.2f → UP (conf=%.2f)",
                asset, ofi, CONF_OFI_STRONG,
            )
            return HypeSignal(
                asset=asset,
                direction="up",
                confidence=CONF_OFI_STRONG,
                reason=f"hype_ofi_strong_{ofi:.2f}",
            )

        if ofi <= -OFI_STRONG_THRESHOLD:
            log.info(
                "[HYPE ENGINE] %s STRONG OFI=%.2f → DOWN (conf=%.2f)",
                asset, ofi, CONF_OFI_STRONG,
            )
            return HypeSignal(
                asset=asset,
                direction="down",
                confidence=CONF_OFI_STRONG,
                reason=f"hype_ofi_strong_{ofi:.2f}",
            )

        # === SIGNAL 3: Latency Arbitrage (Hype → Polymarket) ===
        if state.last_price > 0 and state.price_15s_ago > 0:
            price_delta = (state.last_price - state.price_15s_ago) / state.price_15s_ago

            if (
                price_delta >= LATENCY_ARB_DELTA
                and polymarket_up_price < LATENCY_ARB_POLY_MAX
            ):
                log.info(
                    "[HYPE ENGINE] %s LATENCY ARB: HYPE +%.3f%% | PM_UP=$%.3f → UP (conf=%.2f)",
                    asset, price_delta * 100, polymarket_up_price, CONF_LATENCY,
                )
                return HypeSignal(
                    asset=asset,
                    direction="up",
                    confidence=CONF_LATENCY,
                    reason=f"hype_latency_arb_+{price_delta * 100:.2f}%",
                )

            if (
                price_delta <= -LATENCY_ARB_DELTA
                and polymarket_up_price > LATENCY_ARB_POLY_MIN
            ):
                log.info(
                    "[HYPE ENGINE] %s LATENCY ARB: HYPE %.3f%% | PM_UP=$%.3f → DOWN (conf=%.2f)",
                    asset, price_delta * 100, polymarket_up_price, CONF_LATENCY,
                )
                return HypeSignal(
                    asset=asset,
                    direction="down",
                    confidence=CONF_LATENCY,
                    reason=f"hype_latency_arb_{price_delta * 100:.2f}%",
                )

        # === WEAK OFI ===
        if ofi >= OFI_WEAK_THRESHOLD and polymarket_up_price < 0.52:
            return HypeSignal(
                asset=asset,
                direction="up",
                confidence=CONF_OFI_WEAK,
                reason=f"hype_ofi_weak_{ofi:.2f}",
            )
        if ofi <= -OFI_WEAK_THRESHOLD and polymarket_up_price > 0.48:
            return HypeSignal(
                asset=asset,
                direction="down",
                confidence=CONF_OFI_WEAK,
                reason=f"hype_ofi_weak_{ofi:.2f}",
            )

        return HypeSignal(
            asset=asset,
            direction=None,
            confidence=CONF_NONE,
            reason="no_edge",
        )

    def get_all_signals(self, polymarket_prices: dict[str, float] | None = None) -> dict[str, HypeSignal]:
        """Get signals for all tracked assets."""
        prices = polymarket_prices or {}
        return {
            asset: self.get_signal(asset, prices.get(asset, 0.5))
            for asset in self.assets
        }

    def get_mid_price(self, coin: str) -> float:
        """Get latest mid price from allMids subscription."""
        return self._all_mids.get(coin, 0.0)

    def get_status(self) -> dict:
        """Return engine status for dashboard."""
        return {
            "running": self._running,
            "source": "hyperliquid",
            "ws_url": HYPE_WS_URL,
            "assets": self.assets,
            "prices": {
                asset: {
                    "last": s.last_price,
                    "15s_ago": s.price_15s_ago,
                    "trades_1m": sum(1 for ts, *_ in s.trades if time.time() - ts <= 60),
                    "bid": s.best_bid,
                    "ask": s.best_ask,
                }
                for asset, s in self.states.items()
            },
        }

    # ── Internal: OFI calculation ────────────────────────────────────────

    def _compute_ofi(self, state: HypeAssetState, window: float = 30.0) -> float:
        """Compute Order Flow Imbalance from L2 book snapshots."""
        now = time.time()
        snaps = [
            (ts, bid, ask)
            for ts, bid, ask in state.orderbook_snaps
            if now - ts <= window
        ]

        if len(snaps) < 3:
            return 0.0

        bid_increases = 0.0
        ask_increases = 0.0
        total = 0.0

        for i in range(1, len(snaps)):
            _, prev_bid, prev_ask = snaps[i - 1]
            _, curr_bid, curr_ask = snaps[i]

            bid_delta = max(0, curr_bid - prev_bid)
            ask_delta = max(0, curr_ask - prev_ask)

            bid_increases += bid_delta
            ask_increases += ask_delta
            total += bid_delta + ask_delta

        if total == 0:
            return 0.0

        return (bid_increases - ask_increases) / total * 10

    # ── WebSocket connection ─────────────────────────────────────────────

    async def _run_ws(self, reconnect_delay: float = 5.0) -> None:
        """Main WebSocket loop — subscribe to all feeds on a single connection."""
        while self._running:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(
                        HYPE_WS_URL,
                        heartbeat=30,
                        timeout=aiohttp.ClientTimeout(total=None, connect=10),
                    ) as ws:
                        log.info("[HYPE ENGINE] WebSocket connected")

                        # Subscribe to feeds
                        await self._subscribe_all(ws)

                        async for msg in ws:
                            if not self._running:
                                return
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                try:
                                    data = json.loads(msg.data)
                                    self._handle_message(data)
                                except json.JSONDecodeError:
                                    pass
                            elif msg.type in (
                                aiohttp.WSMsgType.ERROR,
                                aiohttp.WSMsgType.CLOSED,
                            ):
                                break

            except asyncio.CancelledError:
                return
            except Exception as exc:
                log.debug(
                    "[HYPE ENGINE] WebSocket disconnected: %s — reconnecting in %.0fs",
                    exc, reconnect_delay,
                )

            if self._running:
                await asyncio.sleep(reconnect_delay)

    async def _subscribe_all(self, ws) -> None:
        """Send subscription messages for all feeds."""
        # Subscribe to trades for each asset
        for asset in self.assets:
            await ws.send_json({
                "method": "subscribe",
                "subscription": {"type": "trades", "coin": asset},
            })
            await asyncio.sleep(0.1)

        # Subscribe to L2 book for each asset
        for asset in self.assets:
            await ws.send_json({
                "method": "subscribe",
                "subscription": {"type": "l2Book", "coin": asset},
            })
            await asyncio.sleep(0.1)

        # Subscribe to allMids for cross-asset view
        await ws.send_json({
            "method": "subscribe",
            "subscription": {"type": "allMids"},
        })

        log.info("[HYPE ENGINE] Subscribed to trades + l2Book for %s + allMids", self.assets)

    def _handle_message(self, data: dict) -> None:
        """Route incoming WebSocket messages to appropriate handlers."""
        channel = data.get("channel")

        if channel == "trades":
            self._handle_trades(data.get("data", []))
        elif channel == "l2Book":
            self._handle_l2book(data.get("data", {}))
        elif channel == "allMids":
            self._handle_all_mids(data.get("data", {}))
        elif channel == "subscriptionResponse":
            pass  # ack, ignore
        elif channel == "bbo":
            self._handle_bbo(data.get("data", {}))

    def _handle_trades(self, trades: list) -> None:
        """Process trade stream messages."""
        for trade in trades:
            try:
                coin = trade.get("coin", "")
                if coin not in self.states:
                    continue

                state = self.states[coin]
                price = float(trade.get("px", 0))
                size = float(trade.get("sz", 0))
                side = trade.get("side", "")  # "B" or "A"
                now = time.time()

                if price <= 0:
                    continue

                usd_size = price * size
                state.trades.append((now, price, usd_size, side))
                state.prices.append((now, price))

                # Update 15s-ago price
                cutoff = now - 15.0
                old = [(ts, p) for ts, p in state.prices if ts <= cutoff]
                if old:
                    state.price_15s_ago = old[-1][1]
                elif state.price_15s_ago == 0:
                    state.price_15s_ago = price

                state.last_price = price
                state.price_updated_at = now

            except Exception:
                pass

    def _handle_l2book(self, data: dict) -> None:
        """Process L2 order book snapshots."""
        try:
            coin = data.get("coin", "")
            if coin not in self.states:
                return

            state = self.states[coin]
            levels = data.get("levels", [[], []])

            if len(levels) < 2:
                return

            bids = levels[0]  # [{px, sz, n}, ...]
            asks = levels[1]

            # Sum top 10 levels by size
            bid_vol = sum(float(lvl.get("sz", 0)) for lvl in bids[:10])
            ask_vol = sum(float(lvl.get("sz", 0)) for lvl in asks[:10])

            state.orderbook_snaps.append((time.time(), bid_vol, ask_vol))

            # Track best bid/ask
            if bids:
                state.best_bid = float(bids[0].get("px", 0))
            if asks:
                state.best_ask = float(asks[0].get("px", 0))

        except Exception:
            pass

    def _handle_bbo(self, data: dict) -> None:
        """Process best bid/offer updates."""
        try:
            coin = data.get("coin", "")
            if coin not in self.states:
                return

            state = self.states[coin]
            bbo = data.get("bbo", [None, None])

            if bbo[0]:
                state.best_bid = float(bbo[0].get("px", 0))
            if bbo[1]:
                state.best_ask = float(bbo[1].get("px", 0))

        except Exception:
            pass

    def _handle_all_mids(self, data: dict) -> None:
        """Process allMids broadcast — gives mid prices for all coins."""
        try:
            mids = data.get("mids", {})
            for coin, mid_str in mids.items():
                self._all_mids[coin] = float(mid_str)
        except Exception:
            pass


# ── Global singleton ─────────────────────────────────────────────────────

_hype_engine: HyperliquidEngine | None = None


def get_hyperliquid_engine(assets: list[str] | None = None) -> HyperliquidEngine:
    """Get or create the global Hyperliquid engine."""
    global _hype_engine
    if _hype_engine is None:
        _hype_engine = HyperliquidEngine(assets=assets)
    return _hype_engine


def is_hyperliquid_enabled() -> bool:
    """Check if Hyperliquid engine should be active."""
    try:
        from polybot.config import get_settings
        return get_settings().hyperliquid_enabled
    except Exception:
        import os
        return os.environ.get("HYPERLIQUID_ENABLED", "").lower() in ("true", "1", "yes")
