"""Solana to Polygon USDC bridge for auto-funding.

Checks Polygon balance and bridges USDC from Solana when below threshold.
Supports automatic SOL → USDC swapping via Jupiter DEX.
"""

from __future__ import annotations

from polybot.config import get_settings
from polybot.logging_setup import get_logger

log = get_logger(__name__)

# Solana USDC token address (public, not a secret)
SOLANA_USDC_MINT = (
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"  # pragma: allowlist secret
)
BRIDGE_URL = "https://bridge.polymarket.com/deposit"


def check_and_fund(polygon_balance: float, dry_run: bool = True) -> dict | None:
    """Check Polygon balance and bridge from Solana if needed.

    This function:
    1. Checks if Polygon USDC balance is below threshold
    2. If below threshold, checks Solana wallet for SOL/USDC
    3. If SOL is available and auto_swap_sol is enabled, swaps SOL to USDC
    4. Bridges USDC from Solana to Polygon

    Returns bridge result or None if not needed.
    """
    settings = get_settings()

    if not settings.solana_private_key.get_secret_value():
        return None

    if polygon_balance >= settings.min_poly_balance_usdc:
        return None

    log.info(
        "Polygon balance below threshold — checking Solana wallet",
        balance=f"${polygon_balance:.2f}",
        threshold=f"${settings.min_poly_balance_usdc:.2f}",
    )

    try:
        from polybot.solana_bridge import (
            get_wallet_info,
            check_and_swap,
        )

        # Get Solana wallet info
        wallet_info = get_wallet_info()
        if not wallet_info:
            log.warning("Could not get Solana wallet info")
            return None

        log.info(
            "Solana wallet status",
            address=wallet_info.address[:10] + "...",
            sol_balance=f"{wallet_info.sol_balance:.4f} SOL",
            usdc_balance=f"${wallet_info.usdc_balance:.2f} USDC",
        )

        result = {
            "solana_wallet": wallet_info.address,
            "sol_balance": float(wallet_info.sol_balance),
            "usdc_balance": float(wallet_info.usdc_balance),
        }

        # Check if we need to swap SOL to USDC
        if (
            settings.auto_swap_sol
            and wallet_info.sol_balance >= settings.sol_swap_threshold
        ):
            log.info(
                "Auto-swapping SOL to USDC",
                sol_available=f"{wallet_info.sol_balance:.4f}",
                threshold=f"{settings.sol_swap_threshold:.4f}",
            )
            swap_result = check_and_swap(dry_run=dry_run)
            if swap_result:
                result["swap"] = swap_result

                # Refresh wallet info after swap
                if not dry_run:
                    wallet_info = get_wallet_info()
                    if wallet_info:
                        result["usdc_balance_after_swap"] = float(
                            wallet_info.usdc_balance
                        )

        # Check if we have USDC to bridge
        usdc_to_bridge = wallet_info.usdc_balance if wallet_info else 0
        swap_data = result.get("swap")
        if swap_data:
            # If we just swapped, estimate the new USDC amount
            for op in swap_data.get("operations", []):
                if op.get("type") == "swap" and op.get("result", {}).get(
                    "output_amount"
                ):
                    usdc_to_bridge = op["result"]["output_amount"]
                    break

        if usdc_to_bridge < 1:
            log.info(
                "Not enough USDC on Solana to bridge", usdc=f"${usdc_to_bridge:.2f}"
            )
            result["bridge_skipped"] = "Insufficient USDC"
            return result

        # Bridge USDC to Polygon
        if dry_run:
            log.info(
                "DRY RUN — would bridge USDC from Solana to Polygon",
                amount=f"${min(usdc_to_bridge, settings.bridge_fund_amount):.2f}",
            )
            result["bridge"] = {
                "dry_run": True,
                "amount": min(usdc_to_bridge, settings.bridge_fund_amount),
            }
            return result

        # Initiate actual bridge
        from polybot.solana_bridge import bridge_usdc_to_polygon

        import asyncio
        from decimal import Decimal

        bridge_amount = min(
            Decimal(str(usdc_to_bridge)), Decimal(str(settings.bridge_fund_amount))
        )
        polygon_address = settings.wallet_address
        if not polygon_address and settings.private_key_hex:
            from web3 import Web3

            w3 = Web3()
            polygon_address = w3.eth.account.from_key(settings.private_key_hex).address

        if polygon_address:
            bridge_result = asyncio.run(
                bridge_usdc_to_polygon(bridge_amount, polygon_address, dry_run=dry_run)
            )
            result["bridge"] = {
                "success": bridge_result.success,
                "amount": float(bridge_result.amount_bridged),
                "target": bridge_result.target_address,
                "tx_signature": bridge_result.tx_signature,
                "error": bridge_result.error,
            }
        else:
            result["bridge"] = {"error": "No Polygon address configured"}

        return result

    except ImportError as e:
        log.error("Solana dependencies not installed", error=str(e))
        return None
    except Exception as e:
        log.error("Funding operation failed", error=str(e))
        return None


def get_solana_status() -> dict | None:
    """Get current Solana wallet status for display.

    Returns dict with wallet info or None if not available.
    """
    settings = get_settings()

    if not settings.solana_private_key.get_secret_value():
        return None

    try:
        from polybot.solana_bridge import get_wallet_info

        wallet_info = get_wallet_info()
        if not wallet_info:
            return None

        return {
            "address": wallet_info.address,
            "sol_balance": float(wallet_info.sol_balance),
            "usdc_balance": float(wallet_info.usdc_balance),
            "auto_swap_enabled": settings.auto_swap_sol,
            "swap_threshold": settings.sol_swap_threshold,
        }
    except Exception:
        return None
