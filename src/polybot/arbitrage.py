"""Arbitrage engine: buy YES+NO when combined < $1.

Scans for price inefficiencies and executes both sides simultaneously.
Now uses PnL tracker for accurate fee calculation and profit tracking.
"""

from __future__ import annotations

from polybot.config import get_settings
from polybot.database import record_trade
from polybot.executor import get_clob_client
from polybot.pnl_tracker import (
    FeeType,
    estimate_arb_profit,
    get_pnl_tracker,
    save_pnl_tracker_to_db,
)
from polybot.scanner import get_arb_opportunities
from polybot.logging_setup import get_logger

log = get_logger(__name__)


def find_opportunities() -> list[dict]:
    """Find current arbitrage opportunities."""
    settings = get_settings()
    return get_arb_opportunities(
        min_profit_pct=settings.arb_min_profit_threshold * 100,
        min_volume=settings.arb_min_liquidity_usd,
    )


def execute_arb(market: dict, amount: float, dry_run: bool = True) -> dict | None:
    """Execute an arbitrage trade: buy YES + NO simultaneously.

    Args:
        market: Market dict with arb_spread data
        amount: USDC to allocate (split between YES and NO)
        dry_run: If True, only log
    """
    arb = market.get("arb_spread", {})
    yes_price = arb.get("yes_price", 0)
    no_price = arb.get("no_price", 0)
    combined = arb.get("combined", 1.0)
    profit_pct = arb.get("profit_pct", 0)

    if combined >= 1.0:
        log.warning("No arb opportunity", combined=combined)
        return None

    # Calculate profit including fees
    profit_estimate = estimate_arb_profit(yes_price, no_price, amount)
    net_profit_pct = profit_estimate["profit_pct"]

    # Skip if fees eat all the profit
    if net_profit_pct <= 0:
        log.warning(
            "Arb not profitable after fees",
            gross_profit_pct=f"{profit_pct:.2f}%",
            fees=f"${profit_estimate['fees']:.4f}",
            net_profit_pct=f"{net_profit_pct:.2f}%",
        )
        return None

    # Split amount proportionally
    yes_amount = profit_estimate["yes_amount"]
    no_amount = profit_estimate["no_amount"]

    question = market.get("question", "")[:60]
    log.info(
        "Arb opportunity (fee-adjusted)",
        market=question,
        yes_price=f"{yes_price:.4f}",
        no_price=f"{no_price:.4f}",
        combined=f"{combined:.4f}",
        gross_profit_pct=f"{profit_pct:.2f}%",
        fees=f"${profit_estimate['fees']:.4f}",
        net_profit_pct=f"{net_profit_pct:.2f}%",
        amount=f"${amount:.2f}",
    )

    if dry_run:
        log.info("DRY RUN — would execute arb trade")
        record_trade(
            mode="arb",
            side="BUY",
            price=combined,
            size=amount,
            cost=amount,
            market_title=question,
            notes=f"DRY RUN arb: YES@{yes_price:.4f} + NO@{no_price:.4f} = {combined:.4f}, net profit {net_profit_pct:.2f}%",
        )
        return {
            "dry_run": True,
            "profit_pct": profit_pct,
            "net_profit_pct": net_profit_pct,
            "fees": profit_estimate["fees"],
        }

    client = get_clob_client()
    if not client:
        log.error("No CLOB client for arb trade")
        return None

    tokens = market.get("tokens", [])
    yes_token = no_token = None
    for t in tokens:
        outcome = t.get("outcome", "").lower()
        if outcome == "yes":
            yes_token = t.get("token_id")
        elif outcome == "no":
            no_token = t.get("token_id")

    if not yes_token or not no_token:
        log.error("Missing token IDs for arb")
        return None

    try:
        from py_clob_client.clob_types import OrderArgs

        yes_size = yes_amount / yes_price if yes_price > 0 else 0
        no_size = no_amount / no_price if no_price > 0 else 0

        # Execute both orders
        yes_args = OrderArgs(
            token_id=yes_token,
            price=round(yes_price, 2),
            size=round(yes_size, 2),
            side="BUY",
        )
        no_args = OrderArgs(
            token_id=no_token,
            price=round(no_price, 2),
            size=round(no_size, 2),
            side="BUY",
        )

        yes_result = client.create_and_post_order(yes_args)
        no_result = client.create_and_post_order(no_args)

        # Get order IDs
        yes_order_id = (
            yes_result.get("orderID", "") if isinstance(yes_result, dict) else ""
        )
        no_order_id = (
            no_result.get("orderID", "") if isinstance(no_result, dict) else ""
        )

        record_trade(
            mode="arb",
            side="BUY",
            price=combined,
            size=amount,
            cost=amount,
            market_title=question,
            notes=f"ARB: YES@{yes_price:.4f} + NO@{no_price:.4f}, fees ${profit_estimate['fees']:.4f}",
        )

        # Track in PnL tracker
        settings = get_settings()
        if settings.pnl_tracking_enabled:
            pnl_tracker = get_pnl_tracker()
            market_id = market.get("condition_id", "") or market.get("id", "")
            is_negrisk = market.get("enableOrderBook", False) or market.get(
                "is_negrisk", False
            )

            # Record YES fill
            pnl_tracker.record_fill(
                order_id=yes_order_id,
                token_id=yes_token,
                market_id=market_id,
                market_title=question,
                outcome="YES",
                side="BUY",
                price=yes_price,
                size=yes_size,
                fee_type=FeeType.TAKER,
                is_negrisk=is_negrisk,
            )

            # Record NO fill
            pnl_tracker.record_fill(
                order_id=no_order_id,
                token_id=no_token,
                market_id=market_id,
                market_title=question,
                outcome="NO",
                side="BUY",
                price=no_price,
                size=no_size,
                fee_type=FeeType.TAKER,
                is_negrisk=is_negrisk,
            )

            save_pnl_tracker_to_db()

        log.info("Arb trade executed", yes=str(yes_result)[:80], no=str(no_result)[:80])
        return {
            "yes_result": yes_result,
            "no_result": no_result,
            "profit_pct": profit_pct,
            "net_profit_pct": net_profit_pct,
            "fees": profit_estimate["fees"],
        }

    except Exception as e:
        log.error("Arb trade failed", error=str(e))
        return None
