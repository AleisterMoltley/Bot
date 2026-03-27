"""
signal_engine.py — CEX-based trading signal engine.

Connects to Binance WebSocket feeds to detect:
1. Order Flow Imbalance (OFI): hidden buy/sell pressure before price moves
2. Liquidation Cascades: forced moves with 80%+ directional probability
3. Latency Arbitrage: CEX price moved but Polymarket hasn't caught up yet

This gives the bot a 15-90 second edge over Polymarket crowd pricing.
"""

from __future__ import annotations

import asyncio
import json
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Literal

import aiohttp

from polybot.logging_setup import get_logger

log = get_logger(__name__)

# Supported assets
ASSETS = ["BTC", "ETH", "SOL", "XRP"]

# Binance symbol mapping
BINANCE_SYMBOLS = {
    "BTC": "btcusdt",
    "ETH": "ethusdt",
    "SOL": "solusdt",
    "XRP": "xrpusdt",
}

# Signal thresholds
LIQUIDATION_CASCADE_USD = 3_000_000  # $3M liquidations in 10s = cascade
OFI_STRONG_THRESHOLD = 1.5  # OFI score for strong signal
OFI_WEAK_THRESHOLD = 0.8  # OFI score for weak signal
LATENCY_ARB_DELTA = 0.0025  # 0.25% price move on CEX
LATENCY_ARB_POLY_MAX = 0.57  # Polymarket price must be < this (not yet reacted)
LATENCY_ARB_POLY_MIN = 0.43  # Polymarket price must be > this

# Confidence levels
CONF_CASCADE = 0.82
CONF_OFI_STRONG = 0.67
CONF_OFI_WEAK = 0.57
CONF_LATENCY = 0.70
CONF_NONE = 0.0


@dataclass
class Signal:
    """A trading signal with direction and confidence."""

    asset: str  # BTC, ETH, SOL, XRP
    direction: Literal["up", "down"] | None
    confidence: float  # 0.0 - 1.0
    reason: str  # Human readable reason
    timestamp: float = field(default_factory=time.time)

    @property
    def is_valid(self) -> bool:
        return self.direction is not None and self.confidence > 0.5


@dataclass
class AssetState:
    """Rolling state for one asset."""

    symbol: str

    # Price history (timestamp, price) — last 120 seconds
    prices: deque = field(default_factory=lambda: deque(maxlen=240))

    # Order book snapshots (timestamp, bid_vol, ask_vol)
    orderbook_snaps: deque = field(default_factory=lambda: deque(maxlen=60))

    # Liquidations (timestamp, usd_size, side: "long"/"short")
    liquidations: deque = field(default_factory=lambda: deque(maxlen=100))

    # Last known price
    last_price: float = 0.0
    price_15s_ago: float = 0.0
    price_updated_at: float = 0.0


class SignalEngine:
    """
    Real-time signal engine using Binance WebSocket data.

    Usage:
        engine = SignalEngine()
        await engine.start()
        signal = engine.get_signal("BTC", polymarket_up_price=0.52)
    """

    def __init__(self) -> None:
        self.states: dict[str, AssetState] = {
            asset: AssetState(symbol=BINANCE_SYMBOLS[asset]) for asset in ASSETS
        }
        self._tasks: list[asyncio.Task] = []
        self._running = False

    async def start(self) -> None:
        """Start all WebSocket connections."""
        if self._running:
            return
        self._running = True
        log.info("[SIGNAL ENGINE] Starting — connecting to Binance WebSockets")

        for asset in ASSETS:
            symbol = BINANCE_SYMBOLS[asset]
            # Depth feed for OFI
            self._tasks.append(
                asyncio.create_task(
                    self._connect_depth(asset, symbol),
                    name=f"depth_{asset}",
                )
            )
            # Trade feed for price tracking
            self._tasks.append(
                asyncio.create_task(
                    self._connect_trades(asset, symbol),
                    name=f"trades_{asset}",
                )
            )
            # Liquidation feed (futures)
            self._tasks.append(
                asyncio.create_task(
                    self._connect_liquidations(asset, symbol),
                    name=f"liq_{asset}",
                )
            )
            # Stagger connections 200ms apart to avoid rate limits
            await asyncio.sleep(0.2)

        log.info("[SIGNAL ENGINE] All feeds started")

    async def stop(self) -> None:
        """Cancel all WebSocket tasks."""
        self._running = False
        for task in self._tasks:
            task.cancel()
        self._tasks.clear()
        log.info("[SIGNAL ENGINE] Stopped")

    def get_signal(
        self,
        asset: str,
        polymarket_up_price: float = 0.5,
    ) -> Signal:
        """
        Compute trading signal for an asset.

        Args:
            asset: "BTC", "ETH", "SOL", or "XRP"
            polymarket_up_price: Current UP token price on Polymarket (0-1)

        Returns:
            Signal with direction and confidence, or Signal with direction=None
            if no edge detected.
        """
        if asset not in self.states:
            return Signal(
                asset=asset, direction=None, confidence=0.0, reason="unknown_asset"
            )

        state = self.states[asset]
        now = time.time()

        # === SIGNAL 1: Liquidation Cascade ===
        # Sum liquidations in last 10 seconds
        liq_window = 10.0
        long_liq_usd = sum(
            size
            for ts, size, side in state.liquidations
            if now - ts <= liq_window and side == "long"
        )
        short_liq_usd = sum(
            size
            for ts, size, side in state.liquidations
            if now - ts <= liq_window and side == "short"
        )

        if long_liq_usd >= LIQUIDATION_CASCADE_USD:
            # Long liquidations → price drops → buy DOWN
            log.info(
                "[SIGNAL ENGINE] %s LIQUIDATION CASCADE (LONG) $%.0f → DOWN (conf=%.2f)",
                asset,
                long_liq_usd,
                CONF_CASCADE,
            )
            return Signal(
                asset=asset,
                direction="down",
                confidence=CONF_CASCADE,
                reason=f"long_liquidation_cascade_${long_liq_usd / 1e6:.1f}M",
            )

        if short_liq_usd >= LIQUIDATION_CASCADE_USD:
            # Short liquidations → price rises → buy UP
            log.info(
                "[SIGNAL ENGINE] %s LIQUIDATION CASCADE (SHORT) $%.0f → UP (conf=%.2f)",
                asset,
                short_liq_usd,
                CONF_CASCADE,
            )
            return Signal(
                asset=asset,
                direction="up",
                confidence=CONF_CASCADE,
                reason=f"short_liquidation_cascade_${short_liq_usd / 1e6:.1f}M",
            )

        # === SIGNAL 2: Order Flow Imbalance (OFI) ===
        ofi = self._compute_ofi(state, window=30.0)

        if ofi >= OFI_STRONG_THRESHOLD:
            log.info(
                "[SIGNAL ENGINE] %s STRONG OFI=%.2f → UP (conf=%.2f)",
                asset,
                ofi,
                CONF_OFI_STRONG,
            )
            return Signal(
                asset=asset,
                direction="up",
                confidence=CONF_OFI_STRONG,
                reason=f"ofi_strong_{ofi:.2f}",
            )

        if ofi <= -OFI_STRONG_THRESHOLD:
            log.info(
                "[SIGNAL ENGINE] %s STRONG OFI=%.2f → DOWN (conf=%.2f)",
                asset,
                ofi,
                CONF_OFI_STRONG,
            )
            return Signal(
                asset=asset,
                direction="down",
                confidence=CONF_OFI_STRONG,
                reason=f"ofi_strong_{ofi:.2f}",
            )

        # === SIGNAL 3: Latency Arbitrage ===
        # CEX price moved but Polymarket hasn't reacted yet
        if state.last_price > 0 and state.price_15s_ago > 0:
            price_delta = (state.last_price - state.price_15s_ago) / state.price_15s_ago

            if (
                price_delta >= LATENCY_ARB_DELTA
                and polymarket_up_price < LATENCY_ARB_POLY_MAX
            ):
                # CEX rose, Polymarket UP still cheap → buy UP
                log.info(
                    "[SIGNAL ENGINE] %s LATENCY ARB: CEX +%.3f%% | PM_UP=$%.3f → UP (conf=%.2f)",
                    asset,
                    price_delta * 100,
                    polymarket_up_price,
                    CONF_LATENCY,
                )
                return Signal(
                    asset=asset,
                    direction="up",
                    confidence=CONF_LATENCY,
                    reason=f"latency_arb_cex+{price_delta * 100:.2f}%",
                )

            if (
                price_delta <= -LATENCY_ARB_DELTA
                and polymarket_up_price > LATENCY_ARB_POLY_MIN
            ):
                # CEX fell, Polymarket DOWN still cheap → buy DOWN
                log.info(
                    "[SIGNAL ENGINE] %s LATENCY ARB: CEX %.3f%% | PM_UP=$%.3f → DOWN (conf=%.2f)",
                    asset,
                    price_delta * 100,
                    polymarket_up_price,
                    CONF_LATENCY,
                )
                return Signal(
                    asset=asset,
                    direction="down",
                    confidence=CONF_LATENCY,
                    reason=f"latency_arb_cex{price_delta * 100:.2f}%",
                )

        # === WEAK OFI: only if polymarket price aligns ===
        if ofi >= OFI_WEAK_THRESHOLD and polymarket_up_price < 0.52:
            return Signal(
                asset=asset,
                direction="up",
                confidence=CONF_OFI_WEAK,
                reason=f"ofi_weak_{ofi:.2f}",
            )
        if ofi <= -OFI_WEAK_THRESHOLD and polymarket_up_price > 0.48:
            return Signal(
                asset=asset,
                direction="down",
                confidence=CONF_OFI_WEAK,
                reason=f"ofi_weak_{ofi:.2f}",
            )

        # No signal
        return Signal(
            asset=asset,
            direction=None,
            confidence=CONF_NONE,
            reason="no_edge",
        )

    def _compute_ofi(self, state: AssetState, window: float = 30.0) -> float:
        """
        Compute Order Flow Imbalance score for last N seconds.

        OFI = (bid_volume_increase - ask_volume_increase) / total_volume
        Positive = buying pressure, Negative = selling pressure
        """
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

        return (bid_increases - ask_increases) / total * 10  # scale to readable number

    # ── WebSocket connections ────────────────────────────────────────────────

    async def _connect_depth(self, asset: str, symbol: str) -> None:
        """Connect to Binance depth WebSocket for OFI calculation."""
        url = f"wss://stream.binance.com:9443/ws/{symbol}@depth@500ms"
        await self._ws_loop(asset, url, self._handle_depth, feed_name="depth")

    async def _connect_trades(self, asset: str, symbol: str) -> None:
        """Connect to Binance trade WebSocket for price tracking."""
        url = f"wss://stream.binance.com:9443/ws/{symbol}@aggTrade"
        await self._ws_loop(asset, url, self._handle_trade, feed_name="trade")

    async def _connect_liquidations(self, asset: str, symbol: str) -> None:
        """Connect to Binance futures liquidation WebSocket."""
        url = f"wss://fstream.binance.com/ws/{symbol}@forceOrder"
        await self._ws_loop(
            asset, url, self._handle_liquidation, feed_name="liquidation"
        )

    async def _ws_loop(
        self,
        asset: str,
        url: str,
        handler,
        feed_name: str,
        reconnect_delay: float = 5.0,
    ) -> None:
        """WebSocket connection loop with auto-reconnect."""
        first_tick_skipped = False  # Layer 4: skip first (stale) tick

        while self._running:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(
                        url,
                        heartbeat=30,
                        timeout=aiohttp.ClientTimeout(total=None, connect=10),
                    ) as ws:
                        log.debug("[SIGNAL ENGINE] %s %s connected", asset, feed_name)
                        first_tick_skipped = False

                        async for msg in ws:
                            if not self._running:
                                return
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                data = json.loads(msg.data)

                                # Layer 4: skip first tick (stale cache)
                                if not first_tick_skipped:
                                    first_tick_skipped = True
                                    continue

                                handler(asset, data)

                            elif msg.type in (
                                aiohttp.WSMsgType.ERROR,
                                aiohttp.WSMsgType.CLOSED,
                            ):
                                break

            except asyncio.CancelledError:
                return
            except Exception as exc:
                log.debug(
                    "[SIGNAL ENGINE] %s %s disconnected: %s — reconnecting in %.0fs",
                    asset,
                    feed_name,
                    exc,
                    reconnect_delay,
                )

            if self._running:
                await asyncio.sleep(reconnect_delay)

    def _handle_depth(self, asset: str, data: dict) -> None:
        """Process order book depth update."""
        state = self.states[asset]
        try:
            bids = data.get("b", [])
            asks = data.get("a", [])

            # Sum top 10 levels
            bid_vol = sum(float(qty) for _, qty in bids[:10])
            ask_vol = sum(float(qty) for _, qty in asks[:10])

            state.orderbook_snaps.append((time.time(), bid_vol, ask_vol))
        except Exception:
            pass

    def _handle_trade(self, asset: str, data: dict) -> None:
        """Process trade update for price tracking."""
        state = self.states[asset]
        try:
            price = float(data.get("p", 0))
            if price <= 0:
                return

            now = time.time()
            state.prices.append((now, price))

            # Update 15s ago price
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

    def _handle_liquidation(self, asset: str, data: dict) -> None:
        """Process liquidation event."""
        state = self.states[asset]
        try:
            order = data.get("o", {})
            side = order.get("S", "")  # BUY = short liq, SELL = long liq
            qty = float(order.get("q", 0))
            price = float(order.get("p", 0))
            usd_size = qty * price

            if usd_size < 10_000:  # ignore tiny liquidations
                return

            liq_side = "long" if side == "SELL" else "short"
            state.liquidations.append((time.time(), usd_size, liq_side))

            if usd_size > 500_000:
                log.info(
                    "[SIGNAL ENGINE] %s LIQUIDATION $%.0fK %s",
                    asset,
                    usd_size / 1000,
                    liq_side.upper(),
                )
        except Exception:
            pass


# Global singleton — started once at bot startup
_engine: SignalEngine | None = None


def get_signal_engine() -> SignalEngine:
    """Get or create the global signal engine."""
    global _engine
    if _engine is None:
        _engine = SignalEngine()
    return _engine
