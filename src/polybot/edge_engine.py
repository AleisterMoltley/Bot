"""Edge Engine: CEX-backed edge calculation for 5-min Up/Down markets.

Edge is only real when it comes from an external reference price that the
Polymarket crowd hasn't yet priced in. This module computes edge as:

    edge = |P_cex_implied - P_polymarket|

where P_cex_implied is the probability that price will be UP at expiry,
estimated from the CEX price momentum scaled by realised volatility.

The old deviation*4 approach is intentionally removed — it was tautological.
Returns 0.0 when SignalEngine has no data (safe fallback).
"""

from __future__ import annotations

import math
import time

from polybot.logging_setup import get_logger

logger = get_logger(__name__)


def _get_yes_price(market: dict) -> float | None:
    """Extract YES/UP token price from market data."""
    for token in market.get("tokens", []):
        outcome = token.get("outcome", "").lower()
        if outcome in ("yes", "up"):
            p = token.get("price")
            if p is not None:
                return float(p)
    return None


def _normal_cdf(z: float) -> float:
    """Standard normal CDF using math.erf."""
    return 0.5 * (1.0 + math.erf(z / math.sqrt(2.0)))


class EdgeEngine:
    """CEX-backed edge calculator for 5-min binary crypto markets.

    Edge = |implied_prob_from_cex - polymarket_price|

    CEX-implied probability uses log-normal model:
        P(UP) = Phi( drift / (sigma * sqrt(T)) )
    where drift = 30s CEX return, sigma = annualised asset vol, T = time remaining.
    """

    MAX_EDGE_CAP = 0.20
    MIN_EDGE_TO_TRADE = 0.04

    ASSET_VOL: dict[str, float] = {
        "BTC": 1.20, "ETH": 1.40, "SOL": 1.80, "XRP": 1.60,
    }
    DEFAULT_VOL = 1.50

    def get_real_edge(self, market: dict) -> float:
        """Return edge in [0, MAX_EDGE_CAP]. Returns 0.0 when no CEX data."""
        try:
            from polybot.signal_engine import get_signal_engine
            engine = get_signal_engine()
            if not engine._running:
                return 0.0

            question = market.get("question", "").lower()
            asset = next((a.upper() for a in ("btc", "eth", "sol", "xrp") if a in question), None)
            if asset is None:
                return 0.0

            state = engine.states.get(asset)
            if state is None or state.last_price <= 0:
                return 0.0

            poly_up_price = _get_yes_price(market)
            if poly_up_price is None:
                return 0.0

            time_remaining_min = self._get_time_remaining_min(market)
            if time_remaining_min <= 0.1:
                return 0.0

            now = time.time()
            old_prices = [(ts, p) for ts, p in state.price_ring if now - ts >= 30.0]
            if not old_prices:
                return 0.0

            ref_price = old_prices[-1][1]
            cex_return = (state.last_price - ref_price) / ref_price

            sigma_annual = self.ASSET_VOL.get(asset, self.DEFAULT_VOL)
            T_years = time_remaining_min / (365 * 24 * 60)
            sigma_T = sigma_annual * math.sqrt(T_years)
            if sigma_T <= 0:
                return 0.0

            z = cex_return / sigma_T
            cex_implied_prob = _normal_cdf(z)
            raw_edge = abs(cex_implied_prob - poly_up_price)
            edge = min(raw_edge, self.MAX_EDGE_CAP)

            logger.debug(
                "EdgeEngine",
                asset=asset,
                cex_return_pct=round(cex_return * 100, 3),
                z=round(z, 3),
                cex_implied=round(cex_implied_prob, 4),
                poly_price=round(poly_up_price, 4),
                edge=round(edge, 4),
                t_min=round(time_remaining_min, 1),
            )
            return edge if edge >= self.MIN_EDGE_TO_TRADE else 0.0

        except Exception as e:
            logger.debug("EdgeEngine failed", error=str(e))
            return 0.0

    def get_direction(self, market: dict) -> str | None:
        """Return 'up', 'down', or None based on CEX vs Polymarket pricing."""
        try:
            from polybot.signal_engine import get_signal_engine
            engine = get_signal_engine()
            if not engine._running:
                return None

            question = market.get("question", "").lower()
            asset = next((a.upper() for a in ("btc", "eth", "sol", "xrp") if a in question), None)
            if asset is None:
                return None

            state = engine.states.get(asset)
            if state is None or state.last_price <= 0:
                return None

            poly_up_price = _get_yes_price(market)
            if poly_up_price is None:
                return None

            now = time.time()
            old_prices = [(ts, p) for ts, p in state.price_ring if now - ts >= 30.0]
            if not old_prices:
                return None

            cex_return = (state.last_price - old_prices[-1][1]) / old_prices[-1][1]
            t_min = self._get_time_remaining_min(market)
            sigma_annual = self.ASSET_VOL.get(asset, self.DEFAULT_VOL)
            T_years = max(t_min, 0.1) / (365 * 24 * 60)
            sigma_T = sigma_annual * math.sqrt(T_years)
            z = cex_return / sigma_T if sigma_T > 0 else 0.0
            cex_implied_prob = _normal_cdf(z)

            if cex_implied_prob > poly_up_price + self.MIN_EDGE_TO_TRADE:
                return "up"
            elif cex_implied_prob < poly_up_price - self.MIN_EDGE_TO_TRADE:
                return "down"
            return None
        except Exception:
            return None

    def _get_time_remaining_min(self, market: dict) -> float:
        end_date_str = market.get("endDate") or market.get("end_date_iso")
        if end_date_str:
            try:
                from datetime import datetime, timezone
                end_dt = datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
                return max(0.0, (end_dt - datetime.now(timezone.utc)).total_seconds() / 60)
            except Exception:
                pass
        slug = market.get("slug", "")
        parts = slug.split("-")
        if parts and parts[-1].isdigit():
            expiry_ts = int(parts[-1]) + 300
            return max(0.0, (expiry_ts - time.time()) / 60)
        return 2.5

    def get_liquidity_adjusted_edge(self, market: dict, liquidity: float = 10000) -> float:
        base = self.get_real_edge(market)
        if base == 0.0 or liquidity >= 10000:
            return base
        return base * (liquidity / 10000)


_edge_engine: EdgeEngine | None = None


def get_edge_engine() -> EdgeEngine:
    global _edge_engine
    if _edge_engine is None:
        _edge_engine = EdgeEngine()
    return _edge_engine
