"""Edge Engine v5: CEX-implied probability + SignalEngine + Empirical Calibration.

Changes from v4:
- Singleton now respects parameter changes (recreates on new aggressiveness)
- VolatilityMonitor integrated into EdgeEngine (accessible via .vol_monitor)
- datetime.utcnow() replaced with datetime.now(timezone.utc) everywhere
- get_5min_volatility_adjusted_edge removed (was just a redirect)
- Cleaner structure, no dead code
"""

from __future__ import annotations

import math
from datetime import datetime, timezone

from polybot.logging_setup import get_logger

logger = get_logger(__name__)

# Prediction market price-space volatility for the mean-reversion fallback.
# Typical 5-min price swing magnitudes on Polymarket UP/DOWN markets.
# Calibrated: 2¢ deviation → ~2.8% edge, 5¢ → ~7% (after discount).
PM_PRICE_VOL: dict[str, float] = {
    "BTC": 0.10,
    "ETH": 0.12,
    "SOL": 0.15,
    "XRP": 0.14,
    "DOGE": 0.16,
}
DEFAULT_PM_VOL = 0.12

EDGE_DISCOUNT = 0.6
MAX_EDGE = 0.20
MIN_EDGE = 0.005


# ─── Helpers ──────────────────────────────────────────────────────────────────


def _get_yes_price(market: dict) -> float | None:
    for token in market.get("tokens", []):
        if token.get("outcome", "").lower() == "yes":
            p = token.get("price")
            if p is not None:
                return float(p)
    return None


def _normal_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _detect_asset(market: dict) -> str:
    q = (market.get("question", "") + " " + market.get("slug", "")).upper()
    name_map = {"BITCOIN": "BTC", "ETHEREUM": "ETH", "SOLANA": "SOL"}
    for asset in ["BTC", "BITCOIN", "ETH", "ETHEREUM", "SOL", "SOLANA", "XRP", "DOGE"]:
        if asset in q:
            return name_map.get(asset, asset)
    return "BTC"


def _get_current_et_hour() -> int:
    try:
        import pytz
        return datetime.now(pytz.timezone("US/Eastern")).hour
    except ImportError:
        try:
            from zoneinfo import ZoneInfo
            return datetime.now(ZoneInfo("US/Eastern")).hour
        except ImportError:
            return (datetime.now(timezone.utc).hour - 5) % 24


# ─── VolatilityMonitor ────────────────────────────────────────────────────────


class VolatilityMonitor:
    """Tracks observed 5-min price swings and compares to PM_PRICE_VOL.

    Alerts when hardcoded volatility constants drift >2x from observed reality.
    """

    DRIFT_THRESHOLD = 2.0
    MIN_SAMPLES = 20
    MAX_WINDOW = 200

    def __init__(self) -> None:
        self._observations: dict[str, list[float]] = {}

    def record_price_swing(
        self, asset: str, yes_price_before: float, yes_price_after: float
    ) -> None:
        swing = abs(yes_price_after - yes_price_before)
        obs = self._observations.setdefault(asset, [])
        obs.append(swing)
        if len(obs) > self.MAX_WINDOW:
            self._observations[asset] = obs[-self.MAX_WINDOW :]

    def get_observed_vol(self, asset: str) -> float | None:
        obs = self._observations.get(asset, [])
        if len(obs) < self.MIN_SAMPLES:
            return None
        return sum(obs) / len(obs)

    def check_health(self) -> dict[str, dict]:
        report: dict[str, dict] = {}
        for asset, hardcoded in PM_PRICE_VOL.items():
            observed = self.get_observed_vol(asset)
            samples = len(self._observations.get(asset, []))
            if observed is None:
                report[asset] = {
                    "hardcoded": hardcoded,
                    "observed": None,
                    "ratio": None,
                    "healthy": True,
                    "samples": samples,
                }
                continue
            ratio = observed / hardcoded if hardcoded > 0 else float("inf")
            healthy = (1 / self.DRIFT_THRESHOLD) <= ratio <= self.DRIFT_THRESHOLD
            if not healthy:
                logger.warning(
                    "PM_PRICE_VOL drift detected",
                    asset=asset,
                    hardcoded=f"{hardcoded:.3f}",
                    observed=f"{observed:.3f}",
                    ratio=f"{ratio:.2f}",
                )
            report[asset] = {
                "hardcoded": hardcoded,
                "observed": round(observed, 4),
                "ratio": round(ratio, 2),
                "healthy": healthy,
                "samples": samples,
            }
        return report


# ─── EdgeEngine ───────────────────────────────────────────────────────────────


class EdgeEngine:
    """CEX model + signal engine + empirical calibration for 5-min Up/Down markets."""

    def __init__(self, calibration_aggressiveness: float = 0.5) -> None:
        self._signal_engine = None
        self._calibration = None
        self._cal_aggressiveness = calibration_aggressiveness
        self.vol_monitor = VolatilityMonitor()

    @property
    def signal_engine(self):
        if self._signal_engine is None:
            try:
                from polybot.signal_engine import get_signal_engine
                self._signal_engine = get_signal_engine()
            except Exception:
                self._signal_engine = None
        return self._signal_engine

    @property
    def calibration(self):
        if self._calibration is None:
            try:
                from polybot.calibration_lookup import get_calibration
                self._calibration = get_calibration(
                    aggressiveness=self._cal_aggressiveness
                )
            except Exception as e:
                logger.warning(
                    "CalibrationLookup unavailable, v3 fallback", error=str(e)
                )
                self._calibration = None
        return self._calibration

    def get_real_edge(self, market: dict, is_maker: bool = False) -> float:
        try:
            yes_price = _get_yes_price(market) or 0.5
            price_cents = int(round(yes_price * 100))
            asset = _detect_asset(market)
            pm_vol = PM_PRICE_VOL.get(asset, DEFAULT_PM_VOL)

            if self.calibration and self.calibration.is_bad_price_zone(price_cents):
                logger.debug(
                    "EDGE v5: longshot trap zone",
                    price_cents=price_cents,
                    asset=asset,
                )

            model_prob = 0.5
            direction = "up"

            if self.signal_engine:
                signal = self.signal_engine.get_signal(
                    asset, polymarket_up_price=yes_price
                )
                if signal.is_valid:
                    model_prob = (
                        signal.confidence
                        if signal.direction == "up"
                        else (1.0 - signal.confidence)
                    )
                    direction = signal.direction
                else:
                    # Mean-reversion fallback using PM price-space volatility
                    deviation = yes_price - 0.5
                    if pm_vol > 0:
                        z = -deviation / (pm_vol * 3)
                        model_prob = _normal_cdf(z)
                    model_prob = max(0.05, min(0.95, model_prob))
                    direction = "up" if model_prob > 0.5 else "down"

            raw_edge = abs(model_prob - yes_price)
            edge = raw_edge * EDGE_DISCOUNT

            # Empirical calibration
            if self.calibration and edge > 0:
                hour_et = _get_current_et_hour()
                cal = self.calibration.get_calibrated_signal(
                    raw_edge=edge,
                    direction=direction,
                    market_price_cents=price_cents,
                    hour_et=hour_et,
                    is_maker=is_maker,
                )
                edge = cal.adjusted_edge

            if edge < MIN_EDGE:
                return 0.0

            return min(MAX_EDGE, edge)

        except Exception as e:
            logger.debug("Edge calc failed", error=str(e))
            return 0.0

    def get_direction(self, market: dict) -> tuple[str, float]:
        """Get recommended direction and confidence.

        Applies lightweight NO-side bias for the ambiguous zone (45-55¢).
        Callers that use get_real_edge get bias via CalibratedSignal already.
        """
        yes_price = _get_yes_price(market) or 0.5
        asset = _detect_asset(market)

        if self.signal_engine:
            signal = self.signal_engine.get_signal(
                asset, polymarket_up_price=yes_price
            )
            if signal.is_valid:
                return signal.direction, signal.confidence

        if yes_price > 0.55:
            return "down", 0.5 + (yes_price - 0.5) * 0.3
        elif yes_price < 0.45:
            return "up", 0.5 + (0.5 - yes_price) * 0.3

        # Ambiguous zone: lightweight NO-side bias from calibration
        if self.calibration:
            price_cents = int(round(yes_price * 100))
            no_adv = self.calibration.get_no_side_advantage(price_cents)
            if no_adv > 0.003:
                return "down", 0.5 + no_adv * self.calibration.aggressiveness

        return "up" if yes_price <= 0.5 else "down", 0.5

    def get_liquidity_adjusted_edge(
        self, market: dict, liquidity: float = 10000, is_maker: bool = False
    ) -> float:
        base = self.get_real_edge(market, is_maker=is_maker)
        if base == 0.0:
            return 0.0
        if liquidity < 5000:
            return base * (liquidity / 5000)
        return base

    def get_calibration_report(self, market: dict) -> dict:
        """Detailed calibration analysis for dashboard/logging."""
        yes_price = _get_yes_price(market) or 0.5
        price_cents = int(round(yes_price * 100))
        hour_et = _get_current_et_hour()
        report: dict = {
            "price_cents": price_cents,
            "hour_et": hour_et,
            "v5_enabled": self.calibration is not None,
            "vol_health": self.vol_monitor.check_health(),
        }
        if self.calibration:
            c = self.calibration
            report.update({
                "cal_dev_pp": round(c.get_calibration_deviation(price_cents) * 100, 2),
                "maker_adv_pp": round(c.get_maker_advantage(price_cents) * 100, 2),
                "no_bias_pp": round(c.get_no_side_advantage(price_cents) * 100, 2),
                "hourly_excess_pp": round(c.get_hourly_excess(hour_et) * 100, 2),
                "hourly_mult": round(c.get_hourly_modifier(hour_et), 2),
                "bad_zone": c.is_bad_price_zone(price_cents),
                "aggressiveness": c.aggressiveness,
            })
        return report


# ─── Singleton ────────────────────────────────────────────────────────────────

_edge_engine: EdgeEngine | None = None
_edge_engine_aggressiveness: float | None = None


def get_edge_engine(calibration_aggressiveness: float = 0.5) -> EdgeEngine:
    """Get or create the EdgeEngine singleton.

    FIX: Recreates the instance if calibration_aggressiveness has changed
    (previously ignored parameter changes after first creation).
    """
    global _edge_engine, _edge_engine_aggressiveness
    if (
        _edge_engine is None
        or _edge_engine_aggressiveness != calibration_aggressiveness
    ):
        _edge_engine = EdgeEngine(calibration_aggressiveness=calibration_aggressiveness)
        _edge_engine_aggressiveness = calibration_aggressiveness
    return _edge_engine


def get_vol_monitor() -> VolatilityMonitor:
    """Get the VolatilityMonitor from the current EdgeEngine singleton."""
    return get_edge_engine().vol_monitor
