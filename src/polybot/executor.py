"""Order execution via onchain_executor on Polygon.

V66+: Fixed Kelly sizing (confidence is 0-1, not 0-100).
Integrated risk manager for pre-trade checks and performance-based sizing.
"""

from __future__ import annotations

import asyncio
import os

from polybot.config import get_settings
from polybot.logging_setup import get_logger

log = get_logger(__name__)

_client_lock = asyncio.Lock()


def invalidate_l2_cache() -> None:
    """Invalidate cached L2 credentials from environment."""
    for key in ["POLY_API_KEY", "POLY_API_SECRET", "POLY_API_PASSPHRASE"]:
        os.environ.pop(key, None)
    log.info("L2 credential cache invalidated")


def get_clob_client():
    """Create a ClobClient with L2 API credentials from environment."""
    try:
        from py_clob_client.client import ClobClient
        from py_clob_client.clob_types import ApiCreds

        api_key = os.environ.get("POLY_API_KEY")
        api_secret = os.environ.get("POLY_API_SECRET")
        api_passphrase = os.environ.get("POLY_API_PASSPHRASE")
        pk = get_settings().private_key_hex

        if not all([api_key, api_secret, api_passphrase, pk]):
            log.warning("get_clob_client: missing L2 credentials or private key")
            return None

        return ClobClient(
            "https://clob.polymarket.com",
            key=pk,
            chain_id=137,
            creds=ApiCreds(
                api_key=api_key,
                api_secret=api_secret,
                api_passphrase=api_passphrase,
            ),
        )
    except Exception as e:
        log.error(f"get_clob_client failed: {e}")
        return None


def calc_kelly_size(
    confidence: float,
    balance: float,
    base_trade_amount: float,
) -> dict:
    """Calculate Kelly criterion position size.

    FIXED: confidence is 0.0-1.0 (not 0-100).

    Kelly formula: f* = (p*b - q) / b
    where p = win probability, q = 1-p, b = win/loss ratio

    Then apply:
    - Half-Kelly multiplier (from config)
    - Risk manager sizing factor (performance-based)
    - Hard caps (max position, max % of balance)

    Args:
        confidence: Win probability (0.0 to 1.0)
        balance: Current USDC balance
        base_trade_amount: Base trade size from config

    Returns:
        dict with 'size', 'edge', 'kelly_fraction'
    """
    settings = get_settings()
    win_pct = settings.kelly_avg_win_pct    # avg win size (e.g. 0.07 = 7%)
    loss_pct = settings.kelly_avg_loss_pct  # avg loss size (e.g. 0.04 = 4%)
    max_frac = settings.kelly_max_fraction  # max kelly fraction (e.g. 0.25)
    min_trade = settings.kelly_min_trade_usd
    kelly_mult = settings.kelly_multiplier  # half-kelly = 0.5
    slippage = 0.005

    # Confidence is 0-1 — do NOT divide by 100
    # Clamp to valid range
    win_prob = max(0.01, min(0.99, confidence))
    lose_prob = 1.0 - win_prob

    # Expected edge per trade
    edge = (win_prob * win_pct) - (lose_prob * loss_pct) - slippage

    if edge <= 0:
        return {"size": min_trade, "edge": round(edge * 100, 2), "kelly_fraction": 0.0}

    # Kelly fraction: f* = edge / win_pct (simplified)
    kelly_f = edge / win_pct if win_pct > 0 else 0
    kelly_f = min(kelly_f, max_frac)

    # Apply Kelly multiplier (half-Kelly default)
    kelly_f *= kelly_mult

    # Apply risk manager performance factor
    try:
        from polybot.risk_manager import get_risk_manager
        sizing_factor = get_risk_manager().get_sizing_factor()
        kelly_f *= sizing_factor
    except Exception:
        pass

    # Calculate position size
    size = balance * kelly_f
    size = max(size, min_trade)
    size = min(size, settings.max_position_usd)  # hard cap from config
    size = min(size, balance * (settings.max_position_size_pct / 100))  # % cap

    log.debug(
        "Kelly sizing",
        confidence=f"{win_prob:.3f}",
        edge=f"{edge*100:.2f}%",
        kelly_f=f"{kelly_f:.4f}",
        size=f"${size:.2f}",
        balance=f"${balance:.2f}",
    )

    return {
        "size": round(size, 2),
        "edge": round(edge * 100, 2),
        "kelly_fraction": round(kelly_f, 4),
    }


def _prepare_trade_params(market: dict, outcome: str) -> tuple[str | None, float]:
    """Extract token_id and price from market data."""
    if not isinstance(market, dict):
        raise TypeError(f"expected dict, got {type(market)}: {market!r}")
    tokens = market.get("tokens", [])
    token_id = None
    for t in tokens:
        if t.get("outcome", "").lower() == outcome.lower():
            token_id = t.get("token_id")
            break

    price_dev = market.get("price_deviation", {})
    current_price = price_dev.get("current_price", 0.5)
    if outcome.lower() == "no":
        current_price = 1.0 - current_price

    return token_id, current_price


def _build_clob_client():
    """Build ClobClient with L2 API credentials."""
    from py_clob_client.client import ClobClient
    from py_clob_client.clob_types import ApiCreds
    from polybot.credentials_manager import get_cached_creds, get_or_create_l2_creds

    pk = get_settings().private_key_hex
    if not pk:
        raise ValueError("No private key — trading disabled")

    creds = get_cached_creds()
    if not creds:
        log.warning("[EXECUTOR] Creds not ready — bootstrapping...")
        creds = get_or_create_l2_creds()

    return ClobClient(
        "https://clob.polymarket.com",
        key=pk, chain_id=137,
        creds=ApiCreds(api_key=creds["api_key"], api_secret=creds["api_secret"],
                       api_passphrase=creds["api_passphrase"]),
    )


def _outcome_to_side(outcome: str) -> str:
    return "BUY" if outcome.lower() in ("yes", "up") else "SELL"


def place_trade(market: dict, outcome: str, amount: float, dry_run: bool = None) -> dict | None:
    """Place a trade on Polymarket (sync) via onchain_executor."""
    dry_run = dry_run if dry_run is not None else get_settings().dry_run

    token_id, current_price = _prepare_trade_params(market, outcome)
    if not token_id:
        log.error("No token_id for outcome", outcome=outcome)
        return None

    if dry_run:
        log.info("🧪 DRY RUN", amount=amount, outcome=outcome,
                 market=market.get("question", "")[:60])
        return {"status": "dry_run", "amount": amount, "outcome": outcome}

    slug = market.get("slug", "unknown")
    log.info(f"[ONCHAIN] {slug}")
    log.info("🚀 EXECUTING", amount=amount, outcome=outcome,
             market=market.get("question", "")[:60])

    from polybot.onchain_executor import execute_trade as onchain_execute

    pk = get_settings().private_key_hex
    result = onchain_execute(
        private_key=pk, token_id=token_id, amount_usdc=amount,
        side=_outcome_to_side(outcome), current_price=current_price,
        market=market,
    )

    # Track execution result in risk manager
    try:
        from polybot.risk_manager import get_risk_manager
        rm = get_risk_manager()
        if result:
            rm.record_position_opened()
            log.info(f"[TRADE OK] {slug} ${amount} {outcome}")
        else:
            rm.record_execution_failure()
            log.warning(f"[TRADE FAILED] {slug} ${amount} {outcome}")
    except Exception:
        if result:
            log.info(f"[TRADE OK] {slug} ${amount} {outcome}")

    return result


async def place_trade_async(
    market: dict, outcome: str, amount: float,
    dry_run: bool = None, token_id: str | None = None,
) -> dict | None:
    """Place a trade on Polymarket (async) via onchain_executor."""
    _settings = get_settings()
    dry_run = dry_run if dry_run is not None else _settings.dry_run

    if getattr(_settings, "redeem_only", False) is True:
        log.info("🛑 REDEEM_ONLY — trade blocked")
        return {"status": "skipped", "reason": "redeem_only"}

    resolved_token_id, current_price = _prepare_trade_params(market, outcome)
    if token_id is not None:
        resolved_token_id = token_id
    token_id = resolved_token_id

    if not token_id:
        log.error("No token_id for outcome", outcome=outcome)
        return None

    if dry_run:
        log.info("🧪 DRY RUN", amount=amount, outcome=outcome,
                 market=market.get("question", "")[:60])
        return {"status": "dry_run", "amount": amount, "outcome": outcome}

    slug = market.get("slug", "unknown")
    log.info(f"[ONCHAIN] {slug}")
    log.info("🚀 EXECUTING", amount=amount, outcome=outcome,
             market=market.get("question", "")[:60])

    from polybot.onchain_executor import execute_trade as onchain_execute
    pk = get_settings().private_key_hex

    result = await asyncio.to_thread(
        onchain_execute, private_key=pk, token_id=token_id,
        amount_usdc=amount, side=_outcome_to_side(outcome),
        current_price=current_price, market=market,
    )

    # Track execution result in risk manager
    try:
        from polybot.risk_manager import get_risk_manager
        rm = get_risk_manager()
        if result:
            rm.record_position_opened()
            log.info(f"[TRADE OK] {slug} ${amount} {outcome}")
        else:
            rm.record_execution_failure()
            log.warning(f"[TRADE FAILED] {slug} ${amount} {outcome}")
    except Exception:
        if result:
            log.info(f"[TRADE OK] {slug} ${amount} {outcome}")

    return result


def get_polygon_balance(client=None) -> float:
    """Get USDC balance on Polygon (sync)."""
    try:
        pk = get_settings().private_key_hex
        if not pk:
            return 0.0
        from polybot.onchain_executor import get_usdc_balance
        return get_usdc_balance(pk)
    except Exception:
        return 0.0


async def get_polygon_balance_async(client=None) -> float:
    """Get USDC balance on Polygon (async)."""
    try:
        pk = get_settings().private_key_hex
        if not pk:
            return 0.0
        from polybot.onchain_executor import get_usdc_balance
        return await asyncio.to_thread(get_usdc_balance, pk)
    except Exception:
        return 0.0
