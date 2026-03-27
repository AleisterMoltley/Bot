"""ULTIMATE 5-MIN CHASER V2 + LIVE WEBSOCKET MONITOR (2026 Edition)

Der stärkste Modus ever: Pro Coin immer current + next Markt + instant switch bei Resolution.
+ Live Crypto Prices via RTDS für perfekten Trend-Filter.
"""

from __future__ import annotations
import re
import asyncio
import threading
import json
from typing import Any
import websockets
from polybot.config import get_settings
from polybot.scanner import MaxProfitScanner
from polybot.logging_setup import get_logger
from polybot.simple_trend_filter import (
    is_trend_filter_enabled,
    linear_regression_slope,
    std_dev,
    MIN_SLOPE_THRESHOLD,
    MAX_VOLATILITY_THRESHOLD,
)

log = get_logger(__name__)

# Duration and filter constants
MAX_DURATION_SECONDS = 300
MIN_VOLUME_THRESHOLD = 8000
MIN_LIQUIDITY_THRESHOLD = 3000
MAX_PRICE_HISTORY = 10

# WebSocket configuration
POLYMARKET_WS_URI = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

# Ultra-robuster Regex + Liquidity
CHASER_PATTERN = re.compile(
    r"(?i)(bitcoin|btc|ethereum|eth|solana|sol).*(up or down|up/down|up & down).*?(5\s*min|5-minute|5 minutes|5m)",
    re.IGNORECASE,
)


class WebSocketMonitor:
    """Live WS für instant resolution + RTDS Live Prices."""

    def __init__(self):
        self.resolved_markets = set()
        self.live_prices = {}  # coin -> list of last MAX_PRICE_HISTORY prices
        self.running = False
        self.thread = None

    async def _ws_handler(self):
        while self.running:
            try:
                async with websockets.connect(POLYMARKET_WS_URI) as ws:
                    await ws.send(
                        json.dumps({"type": "subscribe", "channel": "market"})
                    )
                    log.info("🚀 WebSocket connected – live monitoring active")
                    while self.running:
                        try:
                            msg = await ws.recv()
                            data = json.loads(msg)
                            if data.get("type") == "market_resolved":
                                market_id = data.get("market_id")
                                self.resolved_markets.add(market_id)
                                log.info(
                                    "✅ MARKET RESOLVED (instant switch!)",
                                    market_id=market_id,
                                )
                            # RTDS Preis-Update (für Trend-Filter)
                            if "price" in data and "token" in data:
                                coin = data["token"].lower()[:3]
                                if coin in ["btc", "eth", "sol"]:
                                    price = float(data["price"])
                                    if coin not in self.live_prices:
                                        self.live_prices[coin] = []
                                    self.live_prices[coin].append(price)
                                    if len(self.live_prices[coin]) > MAX_PRICE_HISTORY:
                                        self.live_prices[coin].pop(0)
                        except websockets.exceptions.ConnectionClosed as e:
                            log.warning(
                                "WebSocket connection closed",
                                code=e.code,
                                reason=e.reason,
                            )
                            break
                        except json.JSONDecodeError as e:
                            log.warning("Invalid JSON from WebSocket", error=str(e))
                        except Exception as e:
                            log.warning(
                                "WS message error",
                                error=str(e),
                                error_type=type(e).__name__,
                            )
            except websockets.exceptions.InvalidURI as e:
                log.error("Invalid WebSocket URI", uri=POLYMARKET_WS_URI, error=str(e))
                break
            except (OSError, ConnectionRefusedError) as e:
                log.warning("WebSocket connection failed, retrying...", error=str(e))
            except Exception as e:
                log.warning(
                    "WebSocket error, reconnecting...",
                    error=str(e),
                    error_type=type(e).__name__,
                )
            if self.running:
                await asyncio.sleep(2)

    def start(self):
        if self.running:
            return
        self.running = True
        self.thread = threading.Thread(
            target=lambda: asyncio.run(self._ws_handler()), daemon=True
        )
        self.thread.start()
        log.info("WebSocket Monitor gestartet")

    def stop(self):
        """Stop the WebSocket monitor gracefully."""
        self.running = False
        log.info("WebSocket Monitor stopping")

    def is_resolved(self, market_id: str) -> bool:
        return market_id in self.resolved_markets

    def get_last_10_prices(self, coin: str) -> list[float]:
        return self.live_prices.get(coin.lower()[:3], [])


ws_monitor = WebSocketMonitor()


class UpDownCryptoScanner(MaxProfitScanner):
    """ULTIMATE SMART 5-MIN CHASER V2 mit LIVE WEBSOCKET."""

    def __init__(self, min_volume=None, min_liquidity=None, min_ev=None):
        super().__init__(
            min_volume=min_volume,
            min_liquidity=min_liquidity,
            min_ev=min_ev,
            up_down_only=True,
        )
        self.last_chosen = {}
        self.filtered_count = 0
        ws_monitor.start()  # Auto-Start

    def scan(self) -> list[dict[str, Any]]:
        all_markets = super().scan_markets() or self._fetch_all_markets()
        candidates = [m for m in all_markets if self._is_chaser_market(m)]

        chaser_results = []
        targets = get_settings().target_symbols

        for symbol in targets:
            coin_markets = [m for m in candidates if self._matches_symbol(m, symbol)]
            coin_markets.sort(key=lambda m: m.get("start_date") or "", reverse=True)

            current = coin_markets[0] if coin_markets else None
            next_one = coin_markets[1] if len(coin_markets) > 1 else None

            # INSTANT SWITCH dank WebSocket
            if symbol in self.last_chosen and ws_monitor.is_resolved(
                self.last_chosen[symbol]
            ):
                log.info("🔄 AUTO-SWITCH durch WebSocket Resolution", coin=symbol)
                self.last_chosen[symbol] = current.get("id") if current else None

            best = current or next_one
            if best:
                best["chaser_coin"] = symbol
                best["is_current"] = current is not None
                chaser_results.append(best)

        # Live-Preise für besseren Trend-Filter
        final = []
        for m in chaser_results:
            coin = m["chaser_coin"]
            prices = ws_monitor.get_last_10_prices(coin) or self._get_last_10_prices(m)
            if is_trend_filter_enabled() and prices:
                slope = linear_regression_slope(prices)
                vol = std_dev(prices)
                if abs(slope) < MIN_SLOPE_THRESHOLD or vol > MAX_VOLATILITY_THRESHOLD:
                    continue
                m["trend_slope"] = slope
                m["trend_volatility"] = vol
            final.append(m)

        self.filtered_count = len(final)
        sorted_final = sorted(final, key=lambda m: m.get("ev", 0), reverse=True)

        log.info(
            "🔥 ULTIMATE CHASER + WS COMPLETE",
            coins=targets,
            tracked=len(sorted_final),
            live_ws_active=ws_monitor.running,
        )

        return sorted_final[: len(targets) * 2]

    def _is_chaser_market(self, market: dict) -> bool:
        if not CHASER_PATTERN.search(market.get("question", "")):
            return False
        if (
            market.get("volume", 0) < MIN_VOLUME_THRESHOLD
            or market.get("liquidity", 0) < MIN_LIQUIDITY_THRESHOLD
        ):
            return False
        duration = market.get("duration_seconds") or market.get("duration") or 0
        return duration <= MAX_DURATION_SECONDS

    def _matches_symbol(self, market: dict, symbol: str) -> bool:
        return symbol.lower() in market.get("question", "").lower()

    def _fetch_all_markets(self) -> list[dict]:
        """Fetch all markets from the API.

        Returns:
            List of market dictionaries
        """
        try:
            from polybot.proxy import make_proxied_request

            resp = make_proxied_request(
                "https://gamma-api.polymarket.com/markets",
                params={"limit": 500, "active": True},
                timeout=30,
                max_retries=3,
            )
            data = resp.json()
            return data if isinstance(data, list) else []
        except Exception as e:
            log.error("Failed to fetch markets", error=str(e))
            return []

    def _get_last_10_prices(self, market: dict) -> list[float]:
        """Get the last 10 prices for a market for trend analysis.

        Extracts price history from various fields in the market data dictionary.
        Falls back through multiple data sources in order of preference.

        Args:
            market: Market data dictionary

        Returns:
            List of last 10 prices, or empty list if unavailable
        """
        # Try to get prices from market data (if available in response)
        prices = market.get("price_history", [])
        if prices and len(prices) >= 2:
            return prices[-10:] if len(prices) >= 10 else prices

        # Try to get from tokens/outcomes pricing
        tokens = market.get("tokens", [])
        if tokens:
            # Use YES token prices if available
            for token in tokens:
                if token.get("outcome") == "Yes":
                    history = token.get("price_history", [])
                    if history:
                        return history[-10:] if len(history) >= 10 else history
                    # Fallback to single price point
                    price = token.get("price")
                    if price is not None:
                        return [float(price)]

        # Try to construct from best_bid/best_ask history
        bid_history = market.get("bid_history", [])
        ask_history = market.get("ask_history", [])
        if bid_history and ask_history:
            # Use midpoint prices
            min_len = min(len(bid_history), len(ask_history))
            if min_len >= 2:
                midpoints = [
                    (bid_history[i] + ask_history[i]) / 2 for i in range(min_len)
                ]
                return midpoints[-10:] if len(midpoints) >= 10 else midpoints

        # Fallback: use current price as single data point (no trend analysis possible)
        current_price = market.get("yes_price") or market.get("price")
        if current_price is not None:
            return [float(current_price)]

        return []

    # Backward compatibility methods
    def _is_5_minute_market(self, market: dict) -> bool:
        """Backward compatibility: alias for _is_chaser_market."""
        return self._is_chaser_market(market)

    def _is_up_down_crypto(self, market: dict) -> bool:
        """Backward compatibility: alias for _is_chaser_market."""
        return self._is_chaser_market(market)

    def get_status(self) -> dict[str, Any]:
        """Get scanner status for display.

        Returns:
            Dict with scanner statistics
        """
        return {
            "mode": "5-MINUTE EXCLUSIVE Mode",
            "mode_description": "Only 5-Minute Events - Maximum Turnover",
            "supported_coins": get_settings().target_symbols,
            "five_min_patterns": ["5 min", "5-minute", "5 minutes", "5m"],
            "max_duration_seconds": MAX_DURATION_SECONDS,
            "max_duration_minutes": MAX_DURATION_SECONDS // 60,
            "last_scan_markets": self.filtered_count,
            "min_ev": self.min_ev,
            "trend_filter_enabled": is_trend_filter_enabled(),
            "websocket_active": ws_monitor.running,
        }


# Global scanner instance
_updown_scanner: UpDownCryptoScanner | None = None


def get_updown_scanner() -> UpDownCryptoScanner:
    """Get or create the global Up/Down scanner instance."""
    global _updown_scanner
    if _updown_scanner is None:
        _updown_scanner = UpDownCryptoScanner()
    return _updown_scanner


def scan_updown_markets() -> list[dict]:
    """Convenience function to scan for Up/Down crypto markets."""
    return get_updown_scanner().scan()
