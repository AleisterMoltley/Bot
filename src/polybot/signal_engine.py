"""
signal_engine.py — CEX-based trading signal engine.

Connects to Binance WebSocket feeds to detect:
1. Order Flow Imbalance (OFI): hidden buy/sell pressure before price moves
2. Liquidation Cascades: forced moves with 80%+ directional probability
3. Latency Arbitrage: CEX price moved but Polymarket hasn't caught up yet

OFI uses Binance FUTURES depth (more reactive, thinner book = faster signals).
True delta-OFI: tracks bid/ask size changes at best levels, not total volume.

Latency Arb uses adaptive time windows (5s/10s/15s/30s) with confidence
scaled by how quickly Polymarket has historically lagged CEX price moves.

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
OFI_STRONG_THRESHOLD = 0.35   # True delta-OFI ratio for strong signal (futures book)
OFI_WEAK_THRESHOLD = 0.18     # True delta-OFI ratio for weak signal
LATENCY_ARB_DELTA = 0.0025    # 0.25% price move on CEX
LATENCY_ARB_POLY_MAX = 0.57   # Polymarket price must be < this (not yet reacted)
LATENCY_ARB_POLY_MIN = 0.43   # Polymarket price must be > this

# Adaptive latency windows (seconds): shortest first for fastest signal
LATENCY_WINDOWS = [5.0, 10.0, 15.0, 30.0]
# Confidence scaling per window: faster reaction = higher confidence
LATENCY_CONF_BY_WINDOW = {5.0: 0.78, 10.0: 0.72, 15.0: 0.70, 30.0: 0.62}

# Confidence levels
CONF_CASCADE = 0.82
CONF_OFI_STRONG = 0.67
CONF_OFI_WEAK = 0.57
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
class FuturesOrderBookLevel:
    """Snapshot of best bid/ask from futures book."""
    timestamp: float
    best_bid_price: float
    best_bid_qty: float
    best_ask_price: float
    best_ask_qty: float


@dataclass
class AssetState:
    """Rolling state for one asset."""

    symbol: str

    # Price history (timestamp, price) — last 120 seconds
    prices: deque = field(default_factory=lambda: deque(maxlen=240))

    # Futures order book levels for true delta-OFI
    # Each entry: FuturesOrderBookLevel
    futures_book_snaps: deque = field(default_factory=lambda: deque(maxlen=120))

    # Legacy spot orderbook snapshots (kept for fallback)
    orderbook_snaps: deque = field(default_factory=lambda: deque(maxlen=60))

    # Liquidations (timestamp, usd_size, side: "long"/"short")
    liquidations: deque = field(default_factory=lambda: deque(maxlen=100))

    # Last known price
    last_price: float = 0.0
    # Price history keyed by window: {window_seconds: price_at_that_time}
    price_history_by_window: dict = field(default_factory=dict)
    price_updated_at: float = 0.0

    # Rolling price buffer for adaptive windows (timestamp, price)
    price_ring: deque = field(default_factory=lambda: deque(maxlen=500))


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
            # Futures depth feed for true delta-OFI (more reactive than spot)
            self._tasks.append(
                asyncio.create_task(
                    self._connect_futures_depth(asset, symbol),
                    name=f"futures_depth_{asset}",
                )
            )
            # Trade feed for price tracking (spot aggTrade, high frequency)
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
            log.info(
                "[SIGNAL ENGINE] %s LIQUIDATION CASCADE (LONG) $%.0f → DOWN (conf=%.2f)",
                asset, long_liq_usd, CONF_CASCADE,
            )
            return Signal(
                asset=asset,
                direction="down",
                confidence=CONF_CASCADE,
                reason=f"long_liquidation_cascade_${long_liq_usd / 1e6:.1f}M",
            )

        if short_liq_usd >= LIQUIDATION_CASCADE_USD:
            log.info(
                "[SIGNAL ENGINE] %s LIQUIDATION CASCADE (SHORT) $%.0f → UP (conf=%.2f)",
                asset, short_liq_usd, CONF_CASCADE,
            )
            return Signal(
                asset=asset,
                direction="up",
                confidence=CONF_CASCADE,
                reason=f"short_liquidation_cascade_${short_liq_usd / 1e6:.1f}M",
            )

        # === SIGNAL 2: True Delta-OFI (Futures book) ===
        ofi, ofi_source = self._compute_delta_ofi(state, window=20.0)

        if ofi >= OFI_STRONG_THRESHOLD:
            log.info(
                "[SIGNAL ENGINE] %s STRONG OFI=%.3f [%s] → UP (conf=%.2f)",
                asset, ofi, ofi_source, CONF_OFI_STRONG,
            )
            return Signal(
                asset=asset,
                direction="up",
                confidence=CONF_OFI_STRONG,
                reason=f"ofi_strong_{ofi:.3f}_{ofi_source}",
            )

        if ofi <= -OFI_STRONG_THRESHOLD:
            log.info(
                "[SIGNAL ENGINE] %s STRONG OFI=%.3f [%s] → DOWN (conf=%.2f)",
                asset, ofi, ofi_source, CONF_OFI_STRONG,
            )
            return Signal(
                asset=asset,
                direction="down",
                confidence=CONF_OFI_STRONG,
                reason=f"ofi_strong_{ofi:.3f}_{ofi_source}",
            )

        # === SIGNAL 3: Adaptive Latency Arbitrage ===
        # Try shortest window first (highest confidence), fall back to longer windows
        latency_signal = self._compute_latency_arb(state, polymarket_up_price, now)
        if latency_signal is not None:
            return latency_signal

        # === WEAK OFI: only if polymarket price aligns ===
        if ofi >= OFI_WEAK_THRESHOLD and polymarket_up_price < 0.52:
            return Signal(
                asset=asset,
                direction="up",
                confidence=CONF_OFI_WEAK,
                reason=f"ofi_weak_{ofi:.3f}_{ofi_source}",
            )
        if ofi <= -OFI_WEAK_THRESHOLD and polymarket_up_price > 0.48:
            return Signal(
                asset=asset,
                direction="down",
                confidence=CONF_OFI_WEAK,
                reason=f"ofi_weak_{ofi:.3f}_{ofi_source}",
            )

        return Signal(
            asset=asset,
            direction=None,
            confidence=CONF_NONE,
            reason="no_edge",
        )

    def _compute_delta_ofi(self, state: AssetState, window: float = 20.0) -> tuple[float, str]:
        """
        Compute true Delta Order Flow Imbalance from futures book.

        True OFI = sum of signed changes at best bid/ask:
          - bid_qty increases: +pressure (buyers aggressive)
          - ask_qty decreases: +pressure (sellers pulled = buyers taking)
          - bid_qty decreases: -pressure (buyers cancelled)
          - ask_qty increases: -pressure (sellers added)

        Normalised to [-1, +1].

        Returns (ofi_score, source_label).
        """
        now = time.time()

        # Prefer futures book snaps
        snaps = [
            s for s in state.futures_book_snaps
            if now - s.timestamp <= window
        ]

        if len(snaps) >= 3:
            buy_pressure = 0.0
            sell_pressure = 0.0

            for i in range(1, len(snaps)):
                prev = snaps[i - 1]
                curr = snaps[i]

                # Bid side: increase = buy pressure, decrease = selling into bids
                bid_delta = curr.best_bid_qty - prev.best_bid_qty
                # Ask side: decrease = demand absorbing asks, increase = supply added
                ask_delta = prev.best_ask_qty - curr.best_ask_qty  # note: inverted

                signed_flow = bid_delta + ask_delta
                if signed_flow > 0:
                    buy_pressure += signed_flow
                else:
                    sell_pressure += abs(signed_flow)

            total = buy_pressure + sell_pressure
            if total > 0:
                ofi = (buy_pressure - sell_pressure) / total
                return ofi, "futures_delta"

        # Fallback: spot volume-based OFI (legacy)
        spot_snaps = [
            (ts, bid, ask)
            for ts, bid, ask in state.orderbook_snaps
            if now - ts <= window
        ]
        if len(spot_snaps) >= 3:
            bid_increases = 0.0
            ask_increases = 0.0
            total = 0.0
            for i in range(1, len(spot_snaps)):
                _, prev_bid, prev_ask = spot_snaps[i - 1]
                _, curr_bid, curr_ask = spot_snaps[i]
                bid_delta = max(0, curr_bid - prev_bid)
                ask_delta = max(0, curr_ask - prev_ask)
                bid_increases += bid_delta
                ask_increases += ask_delta
                total += bid_delta + ask_delta
            if total > 0:
                # Scale to [-1, 1] range (previously was *10)
                raw = (bid_increases - ask_increases) / total
                return raw, "spot_volume"

        return 0.0, "no_data"

    def _compute_latency_arb(
        self,
        state: AssetState,
        polymarket_up_price: float,
        now: float,
    ) -> Signal | None:
        """
        Adaptive latency arbitrage: try multiple lookback windows.

        Shorter windows = higher confidence (PM reacts slowly).
        Returns the first (most confident) signal found, or None.
        """
        if state.last_price <= 0 or len(state.price_ring) < 10:
            return None

        asset = state.symbol.replace("usdt", "").upper()

        for window in LATENCY_WINDOWS:
            cutoff = now - window
            old_prices = [(ts, p) for ts, p in state.price_ring if ts <= cutoff]
            if not old_prices:
                continue

            ref_price = old_prices[-1][1]
            price_delta = (state.last_price - ref_price) / ref_price

            conf = LATENCY_CONF_BY_WINDOW[window]

            if (
                price_delta >= LATENCY_ARB_DELTA
                and polymarket_up_price < LATENCY_ARB_POLY_MAX
            ):
                log.info(
                    "[SIGNAL ENGINE] %s LATENCY ARB (%ds): CEX +%.3f%% | PM_UP=$%.3f → UP (conf=%.2f)",
                    asset, int(window), price_delta * 100, polymarket_up_price, conf,
                )
                return Signal(
                    asset=asset,
                    direction="up",
                    confidence=conf,
                    reason=f"latency_arb_{int(window)}s_cex+{price_delta * 100:.2f}%",
                )

            if (
                price_delta <= -LATENCY_ARB_DELTA
                and polymarket_up_price > LATENCY_ARB_POLY_MIN
            ):
                log.info(
                    "[SIGNAL ENGINE] %s LATENCY ARB (%ds): CEX %.3f%% | PM_UP=$%.3f → DOWN (conf=%.2f)",
                    asset, int(window), price_delta * 100, polymarket_up_price, conf,
                )
                return Signal(
                    asset=asset,
                    direction="down",
                    confidence=conf,
                    reason=f"latency_arb_{int(window)}s_cex{price_delta * 100:.2f}%",
                )

        return None

    # ── WebSocket connections ────────────────────────────────────────────────

    async def _connect_futures_depth(self, asset: str, symbol: str) -> None:
        """Connect to Binance FUTURES depth WebSocket for true delta-OFI."""
        # Futures book is thinner and more reactive than spot — better OFI signal
        url = f"wss://fstream.binance.com/ws/{symbol}@bookTicker"
        await self._ws_loop(asset, url, self._handle_futures_depth, feed_name="futures_depth")

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
        first_tick_skipped = False

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

                                # Skip first tick (stale cache)
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
                    asset, feed_name, exc, reconnect_delay,
                )

            if self._running:
                await asyncio.sleep(reconnect_delay)

    def _handle_futures_depth(self, asset: str, data: dict) -> None:
        """Process futures bookTicker update for true delta-OFI."""
        state = self.states[asset]
        try:
            # bookTicker format: {"b": "best_bid_price", "B": "best_bid_qty",
            #                      "a": "best_ask_price", "A": "best_ask_qty"}
            best_bid_price = float(data.get("b", 0))
            best_bid_qty = float(data.get("B", 0))
            best_ask_price = float(data.get("a", 0))
            best_ask_qty = float(data.get("A", 0))

            if best_bid_price <= 0 or best_ask_price <= 0:
                return

            snap = FuturesOrderBookLevel(
                timestamp=time.time(),
                best_bid_price=best_bid_price,
                best_bid_qty=best_bid_qty,
                best_ask_price=best_ask_price,
                best_ask_qty=best_ask_qty,
            )
            state.futures_book_snaps.append(snap)
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
            state.price_ring.append((now, price))
            state.last_price = price
            state.price_updated_at = now

        except Exception:
            pass

    def _handle_depth(self, asset: str, data: dict) -> None:
        """Process spot order book depth update (legacy fallback for OFI)."""
        state = self.states[asset]
        try:
            bids = data.get("b", [])
            asks = data.get("a", [])
            bid_vol = sum(float(qty) for _, qty in bids[:10])
            ask_vol = sum(float(qty) for _, qty in asks[:10])
            state.orderbook_snaps.append((time.time(), bid_vol, ask_vol))
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

            if usd_size < 10_000:
                return

            liq_side = "long" if side == "SELL" else "short"
            state.liquidations.append((time.time(), usd_size, liq_side))

            if usd_size > 500_000:
                log.info(
                    "[SIGNAL ENGINE] %s LIQUIDATION $%.0fK %s",
                    asset, usd_size / 1000, liq_side.upper(),
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

