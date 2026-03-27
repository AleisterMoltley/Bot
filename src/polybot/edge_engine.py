"""Edge Engine: Real edge calculation via deviation-based pricing.

Provides real edge calculation for crypto price prediction markets by:
- Measuring deviation of YES price from 0.5 baseline
- Simple formula: edge = deviation * 4 (capped at 25%)
- Volatility-adjusted probability for 5min Up/Down markets (v2)

This simple approach works well for short-term Up/Down markets where
prices deviating from 0.5 indicate potential mispricing opportunities.
"""

from __future__ import annotations

import math

from polybot.logging_setup import get_logger

logger = get_logger(__name__)


def _get_yes_price(market: dict) -> float | None:
    """Extract YES token price from market data."""
    for token in market.get("tokens", []):
        if token.get("outcome", "").lower() == "yes":
            p = token.get("price")
            if p is not None:
                return float(p)
    return None


class EdgeEngine:
    """Calculate real trading edge using deviation from 0.5 baseline.

    For short-term crypto prediction markets (Up/Down), edge is calculated by:
    1. Getting current YES price from market data
    2. Calculating deviation from 0.5 baseline
    3. Edge = deviation * 4, capped at 25%

    Examples:
    - 50/50 (price=0.5): deviation=0.0 → edge=0% (no trade)
    - 55/45 (price=0.55): deviation=0.05 → edge=20%
    - 60/40+ (price≥0.6): deviation≥0.1 → edge=25% (capped)

    Provides:
    - Simple deviation-based edge calculation
    - Works without external CEX data
    - Conservative fallback (no trade) when data unavailable
    """

    # Maximum edge cap to avoid outliers
    MAX_EDGE_CAP = 0.25  # 25%

    # Edge scaling factor (no discount for live trading)
    EDGE_SCALE_FACTOR = 1.0

    def __init__(self):
        """Initialize the EdgeEngine with lazy CEX connection."""
        self._cex = None

    @property
    def cex(self):
        """Lazy-load the CCXT Binance exchange instance."""
        if self._cex is None:
            try:
                import ccxt

                self._cex = ccxt.binance({"enableRateLimit": True})
                logger.info("EdgeEngine: Binance exchange initialized")
            except ImportError:
                logger.warning("ccxt not installed, edge calculation disabled")
                self._cex = None
            except Exception as e:
                logger.error("Failed to initialize Binance exchange", error=str(e))
                self._cex = None
        return self._cex

    def get_real_edge(self, market: dict) -> float:
        """Calculate real edge using simple deviation from 0.5 baseline.

        For short-term prediction markets, edge is calculated by:
        1. Getting current YES price from market
        2. Calculating deviation from 0.5 baseline
        3. Edge = deviation * 4, capped at MAX_EDGE_CAP (25%)

        Examples:
        - 50/50 market: edge = 0%
        - 55/45 market: edge = 20%
        - 60/40+ market: edge = 25% (capped)

        Args:
            market: Market data dict with tokens containing price info

        Returns:
            Real edge as a float (0.0 to MAX_EDGE_CAP).
            Returns 0.0 if calculation fails (conservative - no trade).
        """
        try:
            # Get current YES price from market
            yes_price = _get_yes_price(market) or 0.5

            # Calculate deviation from 0.5 baseline
            deviation = abs(yes_price - 0.5)

            # Raw edge: deviation * 4 (50/50 = 0, 70/30 = 0.8 edge)
            raw_edge = deviation * 4.0

            # Apply scaling factor (EDGE_SCALE_FACTOR = 1.0)
            edge = raw_edge * self.EDGE_SCALE_FACTOR

            # Cap edge at maximum (25%)
            final_edge = min(self.MAX_EDGE_CAP, edge)

            # ===================================================================
            # BONUS PATCH V5: Ultra-detailed edge logging (for debugging)
            # Shows: yes_price, implied_prob, deviation, volatility, final_edge
            # ===================================================================
            time_to_expiry_hours = market.get(
                "time_to_expiry_hours", 0.083
            )  # Default: 5 minutes (0.083 hours)
            volatility = market.get("implied_vol", 0.35)  # fallback 35%

            # Calculate z_score for detailed logging
            z_score = 0.0
            if volatility > 0 and time_to_expiry_hours > 0:
                z_score = deviation / (
                    volatility * math.sqrt(time_to_expiry_hours / 24)
                )

            logger.debug(
                "EDGE CALCULATION V5 (detailed)",
                symbol=market.get("question", "")[:50],
                yes_price=round(yes_price, 4),
                implied_prob=round(
                    yes_price, 4
                ),  # YES price = market's implied probability
                deviation=round(deviation, 4),
                raw_edge=round(raw_edge, 4),
                final_edge=round(final_edge, 4),
                z_score=round(z_score, 3),
                volatility=round(volatility, 3),
                time_to_expiry_hours=round(time_to_expiry_hours, 3),
            )

            return final_edge

        except Exception as e:
            logger.debug("Edge calculation failed, returning zero", error=str(e))
            return 0.0

    def get_liquidity_adjusted_edge(
        self, market: dict, liquidity: float = 10000
    ) -> float:
        """Calculate edge with liquidity depth adjustment.

        Reduces edge for low-liquidity markets to account for slippage.

        Args:
            market: Market data dict (same as get_real_edge)
            liquidity: Market liquidity in USD

        Returns:
            Liquidity-adjusted edge value (0.0 if no edge or low liquidity)
        """
        base_edge = self.get_real_edge(market)

        if base_edge == 0.0:
            return 0.0

        # Apply liquidity discount for thin markets
        # Full edge for $10k+ liquidity, scaled down for less
        min_liquidity = 10000
        if liquidity < min_liquidity:
            liquidity_factor = liquidity / min_liquidity
            adjusted_edge = base_edge * liquidity_factor
            logger.debug(
                "Liquidity-adjusted edge",
                base_edge=round(base_edge, 4),
                liquidity=round(liquidity, 2),
                liquidity_factor=round(liquidity_factor, 2),
                adjusted_edge=round(adjusted_edge, 4),
            )
            return adjusted_edge

        return base_edge

    def get_5min_volatility_adjusted_edge(
        self, market: dict, direction: str = "up"
    ) -> float:
        """Calculate edge using volatility-adjusted probability for 5min markets.

        Uses z-score based probability estimation for short-term crypto Up/Down
        markets. Better suited for 5-minute duration markets where volatility
        matters more than simple price deviation.

        Args:
            market: Market data dict with price info and optional volatility
            direction: Trade direction ("up" or "down")

        Returns:
            Volatility-adjusted edge (0.0 to MAX_EDGE_CAP)
        """
        try:
            # Get current YES price (implied probability)
            yes_price = _get_yes_price(market) or 0.5
            implied_prob = yes_price

            # Get market parameters (with sensible defaults for 5min markets)
            current_price = market.get("current_price", 1.0)
            target_price = market.get("target_price", current_price)
            time_to_expiry_hours = market.get("time_to_expiry_hours", 0.083)  # 5min
            volatility = market.get("implied_vol", 0.3)  # fallback to 30% vol

            if current_price <= 0:
                return 0.0

            # Calculate z-score for price distance
            distance = (target_price - current_price) / current_price
            # Scale annualized volatility to time horizon (hours / 24 = days)
            # Using sqrt(T) for volatility time-scaling per Brownian motion model
            time_scaled_vol = volatility * math.sqrt(time_to_expiry_hours / 24)

            if time_scaled_vol <= 0:
                return 0.0

            z_score = distance / time_scaled_vol

            # Normal CDF approximation using error function (erf)
            # Standard formula: Φ(z) ≈ 0.5 + 0.5 * erf(z / sqrt(2))
            # Using 0.4 coefficient and 1.5 divisor for slightly conservative estimate
            # that accounts for fat-tailed crypto distributions (vs normal)
            if direction.lower() == "up":
                estimated_prob = 0.5 + 0.4 * math.erf(z_score / 1.5)
            else:
                estimated_prob = 0.5 - 0.4 * math.erf(z_score / 1.5)

            # Clamp probability to reasonable range
            estimated_prob = max(0.01, min(0.99, estimated_prob))

            # Calculate edge: difference between estimated and implied probability
            raw_edge = abs(estimated_prob - implied_prob)
            edge = raw_edge * self.EDGE_SCALE_FACTOR
            final_edge = min(self.MAX_EDGE_CAP, edge)

            logger.debug(
                "EdgeEngine v2 (5min volatility-adjusted)",
                symbol=market.get("symbol", "UNKNOWN"),
                direction=direction,
                estimated_prob=round(estimated_prob, 4),
                implied_prob=round(implied_prob, 4),
                z_score=round(z_score, 3),
                volatility=round(volatility, 3),
                final_edge=round(final_edge, 4),
            )

            return final_edge

        except Exception as e:
            logger.debug(
                "5min volatility-adjusted edge calculation failed", error=str(e)
            )
            return 0.0


# Singleton instance for convenience
_edge_engine: EdgeEngine | None = None


def get_edge_engine() -> EdgeEngine:
    """Get the global EdgeEngine singleton instance."""
    global _edge_engine
    if _edge_engine is None:
        _edge_engine = EdgeEngine()
    return _edge_engine
