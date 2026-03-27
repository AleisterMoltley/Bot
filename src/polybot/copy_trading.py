"""Copy trading engine.

Monitors trader wallets via Polymarket Data API, replicates trades
with proportional sizing, tiered multipliers, and position tracking.

Now uses proxy manager for geo-bypass and PnL tracker for accurate tracking.
"""

from __future__ import annotations


from polybot.config import get_settings
from polybot.database import record_trade
from polybot.executor import get_clob_client, get_polygon_balance
from polybot.pnl_tracker import FeeType, get_pnl_tracker, save_pnl_tracker_to_db
from polybot.proxy import get_proxy_manager, make_proxied_request
from polybot.logging_setup import get_logger

log = get_logger(__name__)

# Default endpoints (may be overridden by proxy manager mirrors)
DATA_API = "https://data-api.polymarket.com"
GAMMA_API = "https://gamma-api.polymarket.com"


def _get_data_endpoint() -> str:
    """Get the current Data API endpoint (may be mirror)."""
    settings = get_settings()
    if settings.use_api_mirrors:
        return get_proxy_manager().get_mirror_endpoint("data")
    return DATA_API


def fetch_trader_activity(address: str, limit: int = 100) -> list[dict]:
    """Fetch recent activity for a trader address using proxy."""
    try:
        base_url = _get_data_endpoint()
        resp = make_proxied_request(
            f"{base_url}/activity",
            params={"user": address, "limit": limit},
            timeout=30,
            max_retries=3,
        )
        return resp.json()
    except Exception as e:
        log.error("Failed to fetch trader activity", trader=address[:10], error=str(e))
        return []


def fetch_trader_positions(address: str) -> list[dict]:
    """Fetch current positions for a trader using proxy."""
    try:
        base_url = _get_data_endpoint()
        resp = make_proxied_request(
            f"{base_url}/positions",
            params={"user": address},
            timeout=30,
            max_retries=3,
        )
        return resp.json()
    except Exception as e:
        log.error("Failed to fetch positions", trader=address[:10], error=str(e))
        return []


def get_trader_balance(address: str) -> float:
    """Estimate trader's USDC balance from their positions."""
    positions = fetch_trader_positions(address)
    total = sum(
        float(p.get("currentValue", 0)) + float(p.get("cashBalance", 0))
        for p in positions
    )
    return max(total, 1.0)


def get_tiered_multiplier(trader_order_size: float) -> tuple[float, str]:
    """Get the multiplier for a given trader order size based on tiers.

    Returns (multiplier, tier_description).
    """
    settings = get_settings()
    tiers = settings.parse_tiered_multipliers()

    if not tiers:
        return settings.trade_multiplier, "flat"

    for tier in tiers:
        if tier["min"] <= trader_order_size < tier["max"]:
            return tier["multiplier"], f"{tier['min']}-{tier['max']}"

    # Fallback to last tier if no match
    if tiers:
        last = tiers[-1]
        return last["multiplier"], f"{last['min']}+"

    return settings.trade_multiplier, "default"


def calculate_copy_size(
    trader_order_usd: float,
    trader_balance: float,
    my_balance: float,
) -> dict:
    """Calculate the copy trade size with tiered multipliers.

    Returns dict with 'size', 'multiplier', 'tier', 'skipped', 'reason'.
    """
    settings = get_settings()

    # Base calculation
    if settings.copy_strategy == "PERCENTAGE":
        ratio = my_balance / trader_balance if trader_balance > 0 else 0
        base_size = trader_order_usd * ratio * (settings.copy_size / 100.0)
    elif settings.copy_strategy == "FIXED":
        base_size = settings.copy_size
    else:
        base_size = (
            trader_order_usd * (my_balance / trader_balance)
            if trader_balance > 0
            else 0
        )

    # Apply tiered multiplier
    multiplier, tier = get_tiered_multiplier(trader_order_usd)
    final_size = base_size * multiplier

    # Apply limits
    if final_size < settings.min_order_size_usd:
        return {
            "size": 0,
            "multiplier": multiplier,
            "tier": tier,
            "skipped": True,
            "reason": f"Below minimum ${settings.min_order_size_usd}",
        }

    final_size = min(final_size, settings.max_order_size_usd)
    final_size = min(
        final_size, my_balance * 0.95
    )  # Never use more than 95% of balance

    return {
        "size": round(final_size, 2),
        "multiplier": multiplier,
        "tier": tier,
        "skipped": False,
        "reason": "OK",
    }


def copy_trade(
    trader_address: str,
    trade: dict,
    dry_run: bool = True,
) -> dict | None:
    """Copy a single trade from a trader.

    Args:
        trader_address: The trader's wallet address
        trade: Trade activity dict from Polymarket API
        dry_run: If True, only log

    Returns:
        Result dict or None
    """
    side = trade.get("side", "").upper()
    if side not in ("BUY", "SELL"):
        return None

    trader_size = float(trade.get("usdcSize", 0) or trade.get("size", 0))
    if trader_size <= 0:
        return None

    my_balance = get_polygon_balance()
    trader_balance = get_trader_balance(trader_address)

    calc = calculate_copy_size(trader_size, trader_balance, my_balance)

    if calc["skipped"]:
        log.info(
            "Skipping copy trade",
            reason=calc["reason"],
            trader_size=f"${trader_size:.2f}",
            multiplier=calc["multiplier"],
        )
        return None

    token_id = trade.get("asset", "") or trade.get("token_id", "")
    price = float(trade.get("price", 0))

    log.info(
        "Copying trade",
        side=side,
        trader=trader_address[:10],
        trader_size=f"${trader_size:.2f}",
        my_size=f"${calc['size']:.2f}",
        multiplier=f"{calc['multiplier']}x",
        tier=calc["tier"],
    )

    if dry_run:
        log.info("DRY RUN — would execute copy trade")
        record_trade(
            mode="copy",
            side=side,
            price=price,
            size=calc["size"],
            cost=calc["size"],
            market_title=trade.get("title", ""),
            notes=f"DRY RUN copy from {trader_address[:10]}",
        )
        return {"dry_run": True, **calc}

    client = get_clob_client()
    if not client:
        log.error("No CLOB client for copy trade")
        return None

    try:
        from py_clob_client.clob_types import OrderArgs

        size_tokens = calc["size"] / price if price > 0 else calc["size"]
        order_args = OrderArgs(
            token_id=token_id,
            price=round(price, 2),
            size=round(size_tokens, 2),
            side=side,
        )
        result = client.create_and_post_order(order_args)

        # Get order ID from result
        order_id = result.get("orderID", "") if isinstance(result, dict) else ""

        # Record in database
        record_trade(
            mode="copy",
            side=side,
            price=price,
            size=calc["size"],
            cost=calc["size"],
            market_title=trade.get("title", ""),
            order_id=order_id,
            notes=f"Copy from {trader_address[:10]}, {calc['multiplier']}x tier {calc['tier']}",
        )

        # Track in PnL tracker for accurate fee and position tracking
        settings = get_settings()
        if settings.pnl_tracking_enabled:
            pnl_tracker = get_pnl_tracker()
            pnl_tracker.record_fill(
                order_id=order_id,
                token_id=token_id,
                market_id=trade.get("market_id", ""),
                market_title=trade.get("title", ""),
                outcome=trade.get("outcome", "YES"),
                side=side,
                price=price,
                size=size_tokens,
                fee_type=FeeType.TAKER,  # Copy trades are typically taker
                is_partial=False,
                is_negrisk=trade.get("is_negrisk", False),
                condition_id=trade.get("condition_id", ""),
            )
            save_pnl_tracker_to_db()

        log.info("Copy trade executed", result=str(result)[:100])
        return {"result": result, **calc}

    except Exception as e:
        log.error("Copy trade failed", error=str(e))
        return None
