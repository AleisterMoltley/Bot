"""Order execution via onchain_executor on Polygon.

V66: Trade placement delegated entirely to onchain_executor.execute_trade().
No more py_clob_client MarketOrderArgs – all trades go through the CLOB
REST taker flow in onchain_executor.py.

Includes Kelly criterion position sizing.
Now supports async operations for non-blocking trading (March 2026).
"""

from __future__ import annotations

import asyncio
import os

from polybot.config import get_settings
from polybot.logging_setup import get_logger

log = get_logger(__name__)

_client_lock = asyncio.Lock()


def invalidate_l2_cache() -> None:
    """Invalidate cached L2 credentials so they are re-derived on next use."""
    import os

    if os.path.exists("/tmp/polymarket_creds.json"):
        os.remove("/tmp/polymarket_creds.json")
    for key in ["POLY_API_KEY", "POLY_API_SECRET", "POLY_API_PASSPHRASE"]:
        os.environ.pop(key, None)
    log.info("L2 credential cache invalidated")


def get_clob_client():
    """Create a ClobClient with L2 API credentials from environment.

    Returns ClobClient instance or None if credentials are not available.
    """
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

    Returns dict with 'size' (USD) and 'edge' (%).
    """
    settings = get_settings()
    win_pct = settings.kelly_avg_win_pct
    loss_pct = settings.kelly_avg_loss_pct
    max_frac = settings.kelly_max_fraction
    min_trade = settings.kelly_min_trade_usd
    slippage = 0.005

    win_prob = confidence / 100.0
    lose_prob = 1.0 - win_prob
    edge = (win_prob * win_pct) - (lose_prob * loss_pct) - slippage

    if edge <= 0:
        return {"size": min_trade, "edge": edge * 100, "kelly_fraction": 0}

    kelly_f = edge / win_pct if win_pct > 0 else 0
    kelly_f = min(kelly_f, max_frac)

    size = balance * kelly_f
    size = max(size, min_trade)
    size = min(size, base_trade_amount * 3)  # cap at 3x base
    size = min(size, balance * 0.25)  # never more than 25% of balance

    return {
        "size": round(size, 2),
        "edge": round(edge * 100, 2),
        "kelly_fraction": round(kelly_f, 4),
    }


def _prepare_trade_params(market: dict, outcome: str) -> tuple[str | None, float]:
    """Extract token_id and price from market data (shared logic)."""
    if not isinstance(market, dict):
        raise TypeError(
            f"_prepare_trade_params: expected dict, got {type(market)}: {market!r}"
        )
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
    """Build a ClobClient with L2 API credentials for trade execution."""
    from py_clob_client.client import ClobClient
    from py_clob_client.clob_types import ApiCreds

    from polybot.credentials_manager import get_cached_creds, get_or_create_l2_creds

    pk = get_settings().private_key_hex
    if not pk:
        raise ValueError("No private key configured — trading disabled")

    creds = get_cached_creds()
    if not creds:
        # Bootstrap on-demand if startup didn't run in time
        log.warning("[EXECUTOR] Creds not ready — bootstrapping now...")
        creds = get_or_create_l2_creds()

    return ClobClient(
        "https://clob.polymarket.com",
        key=pk,
        chain_id=137,
        creds=ApiCreds(
            api_key=creds["api_key"],
            api_secret=creds["api_secret"],
            api_passphrase=creds["api_passphrase"],
        ),
    )


def _outcome_to_side(outcome: str) -> str:
    """Map outcome labels to BUY/SELL for onchain_executor."""
    return "BUY" if outcome.lower() in ("yes", "up") else "SELL"


def place_trade(
    market: dict,
    outcome: str,
    amount: float,
    dry_run: bool = None,
) -> dict | None:
    """Place a trade on Polymarket (sync version) via onchain_executor.

    Args:
        market: Market dict from scanner (must have 'tokens' with token_ids)
        outcome: 'yes' or 'no' (or 'up'/'down' for 5min markets)
        amount: USDC amount
        dry_run: If None, uses config.dry_run setting (env DRY_RUN override)

    Returns:
        Order result dict or None
    """
    dry_run = dry_run if dry_run is not None else get_settings().dry_run
    token_id, current_price = _prepare_trade_params(market, outcome)

    if not token_id:
        log.error("No token_id found for outcome", outcome=outcome)
        return None

    if dry_run:
        log.info(
            "🧪 DRY RUN TRADE (simulated)",
            amount=amount,
            outcome=outcome,
            market=market.get("question", "")[:60],
        )
        return {"status": "dry_run", "amount": amount, "outcome": outcome}

    slug = market.get("slug", "unknown")
    log.info(f"[ONCHAIN CYCLE MATCH] {slug}")
    log.info(
        "🚀 LIVE TRADE EXECUTING",
        amount=amount,
        outcome=outcome,
        market=market.get("question", "")[:60],
    )

    from polybot.onchain_executor import execute_trade as onchain_execute

    pk = get_settings().private_key_hex
    side = _outcome_to_side(outcome)

    result = onchain_execute(
        private_key=pk,
        token_id=token_id,
        amount_usdc=amount,
        side=side,
        current_price=current_price,
        market=market,  # V88: Pass market for position tracking
    )

    if result:
        log.info(
            f"[ONCHAIN TRADE SUCCESS] {slug} amount={amount} outcome={outcome}",
        )
    return result


async def place_trade_async(
    market: dict,
    outcome: str,
    amount: float,
    dry_run: bool = None,
    token_id: str | None = None,
) -> dict | None:
    """Place a trade on Polymarket (async version) via onchain_executor.

    Uses asyncio.to_thread for blocking onchain_executor calls.

    Args:
        market: Market dict from scanner (must have 'tokens' with token_ids)
        outcome: 'yes' or 'no' (or 'up'/'down' for 5min markets)
        amount: USDC amount
        dry_run: If None, uses config.dry_run setting (env DRY_RUN override)
        token_id: Optional pre-resolved token_id. Skips _prepare_trade_params
                  lookup when provided (used by V21 for direct Up-token resolution).

    Returns:
        Order result dict or None
    """
    dry_run = dry_run if dry_run is not None else get_settings().dry_run
    # Always call _prepare_trade_params for current_price; override token_id
    # only when caller provides a pre-resolved one (V21 Up-token path).
    resolved_token_id, current_price = _prepare_trade_params(market, outcome)
    if token_id is not None:
        resolved_token_id = token_id
    token_id = resolved_token_id

    if not token_id:
        log.error("No token_id found for outcome", outcome=outcome)
        return None

    if dry_run:
        log.info(
            "🧪 DRY RUN TRADE (simulated)",
            amount=amount,
            outcome=outcome,
            market=market.get("question", "")[:60],
        )
        return {"status": "dry_run", "amount": amount, "outcome": outcome}

    slug = market.get("slug", "unknown")
    log.info(f"[ONCHAIN CYCLE MATCH] {slug}")
    log.info(
        "🚀 LIVE TRADE EXECUTING",
        amount=amount,
        outcome=outcome,
        market=market.get("question", "")[:60],
    )

    from polybot.onchain_executor import execute_trade as onchain_execute

    pk = get_settings().private_key_hex
    side = _outcome_to_side(outcome)

    result = await asyncio.to_thread(
        onchain_execute,
        private_key=pk,
        token_id=token_id,
        amount_usdc=amount,
        side=side,
        current_price=current_price,
        market=market,  # V88: Pass market for position tracking
    )

    if result:
        log.info(
            f"[ONCHAIN TRADE SUCCESS] {slug} amount={amount} outcome={outcome}",
        )
    return result


def get_polygon_balance(client=None) -> float:
    """Get USDC balance on Polygon via web3 (sync version)."""
    try:
        settings = get_settings()
        pk = settings.private_key_hex
        if not pk:
            return 0.0
        from polybot.onchain_executor import get_usdc_balance

        return get_usdc_balance(pk)
    except Exception:
        return 0.0


async def get_polygon_balance_async(client=None) -> float:
    """Get USDC balance on Polygon via web3 (async version)."""
    try:
        settings = get_settings()
        pk = settings.private_key_hex
        if not pk:
            return 0.0
        from polybot.onchain_executor import get_usdc_balance

        return await asyncio.to_thread(get_usdc_balance, pk)
    except Exception:
        return 0.0
