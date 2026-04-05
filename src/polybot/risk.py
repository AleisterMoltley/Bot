"""Risk management: daily loss limits, circuit breaker, position caps, Kelly sizing.

Includes volatility-adjusted Kelly sizing for adaptive position management.
"""

from __future__ import annotations

from polybot.config import get_settings
from polybot.database import get_risk_state, update_risk_state
from polybot.logging_setup import get_logger

log = get_logger(__name__)

# EV threshold for Kelly sizing
MIN_EV_FOR_KELLY = 0.08  # 8% minimum Expected Value

# Maximum Kelly fraction (25% cap)
MAX_KELLY_FRACTION = 0.25

# Volatility adjustment enabled by default
VOLATILITY_ADJUST_ENABLED = True


def check_risk_limits() -> tuple[bool, str]:
    """Check if trading is allowed under current risk limits.

    Returns (allowed: bool, reason: str).
    """
    settings = get_settings()
    state = get_risk_state()

    if state.get("is_paused"):
        if state["daily_loss"] >= settings.max_daily_loss:
            return (
                False,
                f"Daily loss limit reached (${state['daily_loss']:.2f} >= ${settings.max_daily_loss:.2f})",
            )
        if state["consecutive_losses"] >= settings.circuit_breaker_consecutive_losses:
            return (
                False,
                f"Circuit breaker: {state['consecutive_losses']} consecutive losses",
            )
        return False, "Trading paused by risk manager"

    return True, "OK"


def record_trade_result(profit: float) -> dict:
    """Record a trade result and update risk state.

    Returns updated risk state.
    """
    is_loss = profit < 0
    loss_delta = abs(profit) if is_loss else 0

    state = update_risk_state(daily_loss_delta=loss_delta, is_loss=is_loss)

    if state["is_paused"]:
        log.warning(
            "Risk limits triggered — trading paused",
            daily_loss=f"${state['daily_loss']:.2f}",
            consecutive_losses=state["consecutive_losses"],
        )
    return state


def reset_circuit_breaker() -> None:
    """Manually reset the circuit breaker."""
    from polybot.database import get_db
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc).isoformat()
    with get_db() as conn:
        conn.execute(
            "UPDATE risk_state SET consecutive_losses = 0, is_paused = 0, updated_at = ? WHERE id = 1",
            (now,),
        )
    log.info("Circuit breaker reset")


def kelly_size(ev: float, edge: float, bankroll: float | None = None) -> float:
    """Calculate optimal position size using Kelly Criterion.

    Only sizes trades with EV > 8% to filter noise.

    Args:
        ev: Expected Value of the opportunity (0.0 to 1.0)
        edge: Edge over market (real_prob - implied_prob)
        bankroll: Optional bankroll amount (not used, kept for API compat)

    Returns:
        Recommended position size as fraction of bankroll (0.0 to MAX_KELLY_FRACTION)
        Returns 0.0 if EV is below threshold or edge is non-positive
    """
    if ev < MIN_EV_FOR_KELLY or edge <= 0:
        return 0.0

    half_kelly = edge / (1 - edge) if edge < 1 else 1.0
    fraction = min(max(half_kelly, 0.0), MAX_KELLY_FRACTION)

    return fraction


def calculate_position_size(
    ev: float,
    edge: float,
    bankroll: float,
    min_trade_usd: float | None = None,
    max_trade_usd: float | None = None,
) -> float:
    """Calculate actual USD position size for a trade.

    Args:
        ev: Expected Value of the opportunity
        edge: Edge over market
        bankroll: Current bankroll in USD
        min_trade_usd: Minimum trade size (uses settings if not provided)
        max_trade_usd: Maximum trade size (uses settings if not provided)

    Returns:
        Position size in USD (0.0 if trade should be skipped)
    """
    settings = get_settings()

    # Get Kelly fraction
    fraction = kelly_size(ev, edge, bankroll)
    if fraction == 0.0:
        return 0.0

    # Calculate position in USD
    position_usd = bankroll * fraction

    # Apply min/max constraints
    min_size = min_trade_usd or settings.kelly_min_trade_usd
    max_size = max_trade_usd or settings.max_order_size_usd

    # If calculated size is below minimum, skip the trade
    if position_usd < min_size:
        log.debug(
            "Position size below minimum",
            calculated=round(position_usd, 2),
            minimum=min_size,
        )
        return 0.0

    # Cap at maximum
    final_size = min(position_usd, max_size)

    log.info(
        "Position size calculated",
        bankroll=round(bankroll, 2),
        kelly_fraction=round(fraction, 4),
        raw_size=round(position_usd, 2),
        final_size=round(final_size, 2),
    )

    return final_size


# Full Kelly threshold for aggressive compounding
FULL_KELLY_EV_THRESHOLD = 0.12  # 12% EV threshold for full Kelly
MAX_LIQUIDITY_FRACTION = 0.35  # Max 35% of market liquidity


def calculate_position_size_with_liquidity(
    ev: float,
    edge: float,
    bankroll: float,
    liquidity: float,
    use_full_kelly: bool = False,
) -> float:
    """Calculate position size with liquidity depth consideration.

    For high-EV (>12%) opportunities, uses Full Kelly when enabled.
    Always respects 35% max of market liquidity to avoid slippage.

    Args:
        ev: Expected Value of the opportunity
        edge: Edge over market
        bankroll: Current bankroll in USD
        liquidity: Market liquidity in USD
        use_full_kelly: Enable Full Kelly for >12% EV

    Returns:
        Position size in USD (0.0 if trade should be skipped)
    """
    settings = get_settings()

    # Skip low-EV opportunities
    if ev < FULL_KELLY_EV_THRESHOLD and use_full_kelly:
        log.debug(
            "EV below Full Kelly threshold",
            ev=round(ev, 4),
            threshold=FULL_KELLY_EV_THRESHOLD,
        )
        return 0.0

    # Use standard Kelly for normal EV
    if not use_full_kelly or ev < FULL_KELLY_EV_THRESHOLD:
        return calculate_position_size(ev, edge, bankroll)

    # Full Kelly calculation for high-EV opportunities
    # f* = edge / variance, where variance ≈ 0.5 for binary outcomes
    kelly_fraction = min(1.0, edge / 0.5)

    # Calculate position in USD
    position_usd = bankroll * kelly_fraction

    # Cap at 35% of market liquidity to avoid market impact
    max_liquidity_size = liquidity * MAX_LIQUIDITY_FRACTION
    position_usd = min(position_usd, max_liquidity_size)

    # Apply min/max constraints
    min_size = settings.kelly_min_trade_usd
    max_size = settings.max_order_size_usd

    if position_usd < min_size:
        log.debug(
            "Full Kelly position below minimum",
            calculated=round(position_usd, 2),
            minimum=min_size,
        )
        return 0.0

    final_size = min(position_usd, max_size)

    log.info(
        "Full Kelly position with liquidity sizing",
        ev=round(ev, 4),
        kelly_fraction=round(kelly_fraction, 4),
        liquidity=round(liquidity, 2),
        max_liquidity_size=round(max_liquidity_size, 2),
        final_size=round(final_size, 2),
    )

    return final_size


def should_trade_opportunity(opportunity: dict) -> tuple[bool, str, float]:
    """Evaluate if an opportunity should be traded based on risk rules.

    Args:
        opportunity: Dict with 'ev', 'edge', and optionally 'type' keys

    Returns:
        Tuple of (should_trade: bool, reason: str, suggested_size_fraction: float)
    """
    # Check hourly risk regime first - block trading during inactive hours
    try:
        from polybot.hourly_risk_regime import get_hourly_multiplier

        if get_hourly_multiplier() == 0.0:
            return False, "Hourly risk regime: trading inactive", 0.0
    except ImportError:
        pass  # Hourly regime module not available

    # Check risk limits
    allowed, reason = check_risk_limits()
    if not allowed:
        return False, reason, 0.0

    ev = opportunity.get("ev", 0)
    edge = opportunity.get("edge", 0)
    opp_type = opportunity.get("type", "UNKNOWN")

    # Arbitrage opportunities always pass (risk-free)
    if opp_type == "ARB":
        log.info("Arbitrage opportunity approved (risk-free)")
        return True, "ARB: Risk-free opportunity", 0.25  # Max Kelly for arb

    # Calculate Kelly fraction
    fraction = kelly_size(ev, edge)
    if fraction == 0.0:
        return False, f"EV too low ({ev:.2%} < {MIN_EV_FOR_KELLY:.0%})", 0.0

    return True, f"High-EV trade approved (EV: {ev:.2%}, Edge: {edge:.2%})", fraction


def calculate_kelly_position(
    edge: float,
    bankroll: float,
    kelly_mult: float = 0.5,
) -> float:
    """Calculate Kelly position size for auto-trade execution.

    Simplified Kelly formula optimized for 5min Up/Down markets with
    configurable Kelly multiplier (Half-Kelly default for safety).

    Args:
        edge: Trading edge (0.0 to 1.0, e.g., 0.10 = 10% edge)
        bankroll: Current bankroll in USD
        kelly_mult: Kelly multiplier (0.5=Half-Kelly safer, 1.0=Full-Kelly)

    Returns:
        Position size in USD (will be clamped by caller to min/max limits)
    """
    if edge <= 0 or bankroll <= 0:
        return 0.0

    # Kelly formula: f* = edge / variance
    # For binary outcomes, variance = p*(1-p) ≈ 0.25 at fair odds (p=0.5)
    # Therefore: f* = edge / 0.25 = edge * 4
    # This 4.0 multiplier is BINARY_VARIANCE_RECIPROCAL = 1 / 0.25
    BINARY_VARIANCE_RECIPROCAL = 4.0
    kelly_fraction = edge * BINARY_VARIANCE_RECIPROCAL

    # Apply Kelly multiplier (Half-Kelly = 0.5 for safety)
    kelly_fraction *= kelly_mult

    # Cap at 25% of bankroll regardless of edge
    kelly_fraction = min(kelly_fraction, MAX_KELLY_FRACTION)

    position_usd = bankroll * kelly_fraction

    log.debug(
        "Kelly position calculated (auto-trade)",
        edge=round(edge, 4),
        kelly_mult=kelly_mult,
        kelly_fraction=round(kelly_fraction, 4),
        bankroll=round(bankroll, 2),
        position_usd=round(position_usd, 2),
    )

    return position_usd


def calculate_bucketed_kelly(
    edge: float,
    bankroll: float,
    base_kelly: float = 0.5,
    max_position: float = 60.0,
    min_position: float = 5.0,
) -> float:
    """Edge-bucketed Kelly — scales sizing based on edge strength.

    Instead of a flat Kelly multiplier, this uses edge buckets to be
    conservative on weak edges and aggressive on strong ones.

    Edge buckets:
        < 1.5%  → 0.20× (very conservative — weak edge)
        1.5–3%  → 0.50× (normal)
        3–5%    → 0.80× (aggressive — strong edge)
        > 5%    → 1.10× (very aggressive — rare, capped)

    Args:
        edge: Trading edge (0.0-1.0 scale, e.g., 0.03 = 3%)
        bankroll: Current bankroll in USD
        base_kelly: Base Kelly fraction (default 0.5 = Half-Kelly)
        max_position: Hard cap per trade in USD
        min_position: Minimum trade size in USD

    Returns:
        Position size in USD, capped to [min_position, max_position].
    """
    if edge <= 0 or bankroll <= 0:
        return 0.0

    # Edge bucketing
    if edge < 0.015:
        multiplier = 0.20
    elif edge < 0.030:
        multiplier = 0.50
    elif edge < 0.050:
        multiplier = 0.80
    else:
        multiplier = 1.10

    # Kelly formula: f* = edge * 4 (binary outcome, variance = 0.25)
    BINARY_VARIANCE_RECIPROCAL = 4.0
    kelly_fraction = edge * BINARY_VARIANCE_RECIPROCAL * multiplier * base_kelly

    # Cap at 25% of bankroll
    kelly_fraction = min(kelly_fraction, MAX_KELLY_FRACTION)

    position_usd = bankroll * kelly_fraction

    # Hard caps
    position_usd = min(position_usd, max_position)
    position_usd = max(position_usd, min_position) if position_usd > 0 else 0.0

    log.debug(
        "Bucketed Kelly",
        edge=round(edge, 4),
        bucket_mult=multiplier,
        kelly_frac=round(kelly_fraction, 4),
        position=round(position_usd, 2),
    )

    return position_usd
