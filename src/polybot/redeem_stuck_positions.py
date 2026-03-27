#!/usr/bin/env python3
"""Redeem stuck/resolved positions from Polymarket.

This script queries the Polymarket Data API for redeemable positions
and calls the CTF contract's redeemPositions function to convert
winning outcome tokens back into USDC.

Uses the same contract addresses and patterns as onchain_executor.py
for consistency.

Usage:
    # Via environment variable:
    POLYMARKET_PRIVATE_KEY=0x... python -m polybot.redeem_stuck_positions

    # Or use PolyBot's config system (recommended):
    python -m polybot.redeem_stuck_positions
"""

from __future__ import annotations

import sys
import time

import requests
from web3 import Web3

from polybot.config import get_settings
from polybot.logging_setup import get_logger

# Use the same constants as onchain_executor.py
from polybot.onchain_executor import (
    CTF_ABI,
    CTF_ADDRESS,
    USDC_ABI,
    USDC_ADDRESS,
)

log = get_logger(__name__)

# Polymarket Data API base URL
DATA_API = "https://data-api.polymarket.com"


def fetch_redeemable_positions(wallet: str, limit: int = 100) -> list[dict]:
    """Fetch all positions with redeemable=true via Polymarket Data API.

    Args:
        wallet: The wallet address to query positions for.
        limit: Maximum positions to fetch (default: 100). If API returns
               exactly this many, there may be more positions available.

    Returns:
        List of position dicts that are redeemable.
    """
    url = f"{DATA_API}/positions"
    params = {
        "user": wallet,
        "sizeThreshold": "0.01",
        "limit": limit,
        "redeemable": "true",
    }
    try:
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        positions = resp.json()
        log.info("✅ Data API: %d positions loaded", len(positions))
        # Warn if limit reached - may have more positions
        if len(positions) >= limit:
            log.warning(
                "⚠️  Limit of %d reached. There may be more positions. "
                "Run again after redeeming these.",
                limit,
            )
        return positions
    except Exception as e:
        log.warning("❌ Data API error: %s", e)
        # Fallback: fetch all positions and filter locally
        try:
            params_all = {"user": wallet, "sizeThreshold": "0.01", "limit": limit}
            resp2 = requests.get(url, params=params_all, timeout=15)
            resp2.raise_for_status()
            all_pos = resp2.json()
            redeemable = [p for p in all_pos if p.get("redeemable") is True]
            log.info(
                "✅ Fallback: %d redeemable of %d total", len(redeemable), len(all_pos)
            )
            return redeemable
        except Exception as e2:
            log.error("❌ Fallback also failed: %s", e2)
            return []


def redeem_position(w3: Web3, account, ctf, position: dict) -> bool:
    """Execute redeemPositions for a single position.

    Args:
        w3: Web3 instance connected to Polygon.
        account: The account object with signing key.
        ctf: The CTF contract instance.
        position: Position dict from Data API.

    Returns:
        True if redemption succeeded, False otherwise.
    """
    condition_id = position.get("conditionId") or position.get("condition_id")
    outcome_index = position.get("outcomeIndex", 0)
    title = position.get("title", "?")[:60]
    outcome = position.get("outcome", "?")
    size = position.get("size", 0)
    cur_price = position.get("curPrice", 0)

    if not condition_id:
        log.warning("  ⚠️  No conditionId for '%s' — skipping", title)
        return False

    # IndexSet: outcomeIndex 0 → indexSet=1 (bit 0), outcomeIndex 1 → indexSet=2 (bit 1)
    # Polymarket Up/Down: outcome 0 = Up/Yes → indexSet [1], outcome 1 = Down/No → indexSet [2]
    index_set = [1 << outcome_index]  # 2^outcomeIndex

    log.info("\n  📋 Redeem: '%s'", title)
    log.info(
        "     Outcome: %s (index=%d, indexSet=%s)", outcome, outcome_index, index_set
    )
    log.info("     Size: %.4f tokens @ $%.4f", float(size), float(cur_price))
    log.info("     ConditionId: %s...", condition_id[:20])

    # Parent collection ID is always 0x0 (32 bytes)
    parent_collection_id = "0x" + "0" * 64

    try:
        nonce = w3.eth.get_transaction_count(account.address, "pending")
        gas_price = w3.eth.gas_price

        tx = ctf.functions.redeemPositions(
            Web3.to_checksum_address(USDC_ADDRESS),
            parent_collection_id,
            condition_id,
            index_set,
        ).build_transaction(
            {
                "from": account.address,
                "gas": 300_000,
                "gasPrice": int(gas_price * 1.2),  # 20% tip
                "nonce": nonce,
                "chainId": 137,
            }
        )

        signed = w3.eth.account.sign_transaction(tx, account.key)
        tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
        log.info("     🚀 TX submitted: %s", tx_hash.hex())

        receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
        if receipt["status"] == 1:
            log.info("     ✅ REDEEMED! Block %d", receipt["blockNumber"])
            return True
        else:
            log.error("     ❌ TX reverted! Hash: %s", tx_hash.hex())
            return False

    except Exception as exc:
        exc_str = str(exc).lower()
        if "already known" in exc_str:
            log.info("     ℹ️  TX already in mempool — waiting...")
            time.sleep(15)
            return True
        elif "nonce too low" in exc_str:
            log.info("     ℹ️  Nonce conflict — next position")
            return False
        else:
            log.error("     ❌ Error: %s", exc)
            return False


def main() -> None:
    """Main entry point for redeeming stuck positions."""
    print("=" * 60)
    print("  POLYMARKET REDEEM STUCK POSITIONS")
    print("=" * 60)

    # Load settings via PolyBot's config system
    settings = get_settings()

    # Get private key from settings
    pk = settings.private_key_hex
    if not pk:
        log.error("❌ POLYMARKET_PRIVATE_KEY not set!")
        log.error("   Set env: export POLYMARKET_PRIVATE_KEY=0x...")
        sys.exit(1)

    # Ensure 0x prefix
    if not pk.startswith("0x"):
        pk = "0x" + pk

    # Connect to Polygon RPC
    log.info("\n🔌 Connecting to Polygon RPC...")
    w3 = Web3(Web3.HTTPProvider(settings.polygon_rpc_url))
    if not w3.is_connected():
        log.error("❌ RPC connection failed!")
        sys.exit(1)
    log.info("✅ Polygon connected (Chain ID: %d)", w3.eth.chain_id)

    # Load account from private key
    account = w3.eth.account.from_key(pk)
    wallet = account.address
    log.info("👛 Wallet: %s", wallet)

    # Check USDC balance
    usdc = w3.eth.contract(address=Web3.to_checksum_address(USDC_ADDRESS), abi=USDC_ABI)
    balance = usdc.functions.balanceOf(account.address).call() / 1e6
    log.info("💰 USDC Balance: $%.6f", balance)

    # Create CTF contract instance
    ctf = w3.eth.contract(address=Web3.to_checksum_address(CTF_ADDRESS), abi=CTF_ABI)

    # Fetch redeemable positions
    log.info("\n🔍 Fetching redeemable positions for %s...", wallet)
    positions = fetch_redeemable_positions(wallet)

    if not positions:
        log.info("\n✅ No redeemable positions found.")
        log.info("   Either already redeemed, or positions are still open.")
        return

    log.info("\n🎯 %d redeemable position(s) found:\n", len(positions))
    for i, p in enumerate(positions, 1):
        title = p.get("title", "?")[:55]
        outcome = p.get("outcome", "?")
        size = float(p.get("size", 0))
        cur_price = float(p.get("curPrice", 0))
        log.info("  %d. %s", i, title)
        log.info(
            "     Outcome: %s | Size: %.4f | Price: $%.4f", outcome, size, cur_price
        )

    log.info("\n⚡ Starting redeem process...")
    success = 0
    failed = 0

    for i, pos in enumerate(positions, 1):
        log.info("\n[%d/%d]", i, len(positions))
        ok = redeem_position(w3, account, ctf, pos)
        if ok:
            success += 1
        else:
            failed += 1
        # Short pause between transactions
        if i < len(positions):
            time.sleep(3)

    # Final balance check
    balance_after = usdc.functions.balanceOf(account.address).call() / 1e6
    gained = balance_after - balance

    print(f"\n{'=' * 60}")
    print("  SUMMARY")
    print(f"{'=' * 60}")
    print(f"  ✅ Successful: {success}")
    print(f"  ❌ Failed: {failed}")
    print(f"  💰 Balance before: ${balance:.6f}")
    print(f"  💰 Balance after: ${balance_after:.6f}")
    if gained > 0:
        print(f"  🎉 Gained: +${gained:.6f} USDC")
    print("  ℹ️  Note: Gas fees paid in MATIC are not reflected above")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
