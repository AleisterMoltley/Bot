"""Piggybank — Auto-save 1% of all profits to a savings wallet.

Hardcoded savings wallet. After every profitable redeem or trade,
1% of the profit is sent as USDC on Polygon to the piggybank.

This runs AFTER the redeem/trade is confirmed, so it never
interferes with the trading flow.
"""

from __future__ import annotations

import asyncio
import time
from polybot.logging_setup import get_logger

log = get_logger(__name__)

# ── HARDCODED SAVINGS WALLET ──
PIGGYBANK_WALLET = "0x978982EB8A854e53DD154a0dc89ecb4d54f11FBf"
PIGGYBANK_PCT = 0.01  # 1% of profit

# Track total saved
_total_saved_usd: float = 0.0
_last_transfer_time: float = 0.0

# Minimum profit to trigger (avoid dust transfers)
MIN_PROFIT_TO_SAVE = 0.10  # $0.10 minimum (1% of $10 profit)

# Minimum USDC to send (gas isn't worth it below this)
MIN_TRANSFER_USDC = 0.05


def calc_piggybank_amount(profit_usd: float) -> float:
    """Calculate how much to send to piggybank.

    Args:
        profit_usd: Profit from a trade/redeem in USD

    Returns:
        Amount to transfer (0.0 if below threshold)
    """
    if profit_usd <= MIN_PROFIT_TO_SAVE:
        return 0.0
    amount = profit_usd * PIGGYBANK_PCT
    if amount < MIN_TRANSFER_USDC:
        return 0.0
    return round(amount, 6)


def transfer_to_piggybank(
    private_key: str,
    amount_usdc: float,
    w3=None,
) -> dict | None:
    """Transfer USDC to piggybank wallet on Polygon.

    Args:
        private_key: Sender's private key
        amount_usdc: USDC amount to transfer
        w3: Optional web3 instance (creates one if not provided)

    Returns:
        Transaction result dict or None on failure
    """
    global _total_saved_usd, _last_transfer_time

    if amount_usdc < MIN_TRANSFER_USDC:
        return None

    try:
        from web3 import Web3

        if w3 is None:
            from polybot.rpc_manager import get_rpc_manager
            w3 = get_rpc_manager().get_web3()

        USDC_ADDRESS = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
        USDC_ABI = [
            {
                "inputs": [
                    {"name": "to", "type": "address"},
                    {"name": "amount", "type": "uint256"},
                ],
                "name": "transfer",
                "outputs": [{"name": "", "type": "bool"}],
                "stateMutability": "nonpayable",
                "type": "function",
            }
        ]

        account = w3.eth.account.from_key(private_key)
        usdc = w3.eth.contract(
            address=Web3.to_checksum_address(USDC_ADDRESS), abi=USDC_ABI
        )

        # USDC has 6 decimals on Polygon
        amount_raw = int(amount_usdc * 1_000_000)

        tx = usdc.functions.transfer(
            Web3.to_checksum_address(PIGGYBANK_WALLET),
            amount_raw,
        ).build_transaction({
            "from": account.address,
            "nonce": w3.eth.get_transaction_count(account.address),
            "gas": 80_000,
            "maxFeePerGas": w3.eth.gas_price * 2,
            "maxPriorityFeePerGas": w3.to_wei("30", "gwei"),
        })

        signed = w3.eth.account.sign_transaction(tx, private_key)
        tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
        receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)

        if receipt.get("status") == 1:
            _total_saved_usd += amount_usdc
            _last_transfer_time = time.time()

            # Persist to DB
            try:
                from polybot.database import get_db
                from datetime import datetime, timezone
                with get_db() as conn:
                    conn.execute(
                        "INSERT INTO piggybank_transfers (timestamp, profit_usd, amount_usd, tx_hash, status) "
                        "VALUES (?, ?, ?, ?, ?)",
                        (datetime.now(timezone.utc).isoformat(), amount_usdc / PIGGYBANK_PCT, amount_usdc, tx_hash.hex(), "ok"),
                    )
            except Exception:
                pass  # DB failure should never block

            log.debug(
                "piggybank_saved",
                amount=f"${amount_usdc:.4f}",
                total_saved=f"${_total_saved_usd:.4f}",
                tx=tx_hash.hex(),
            )
            return {
                "status": "ok",
                "amount": amount_usdc,
                "tx_hash": tx_hash.hex(),
                "total_saved": _total_saved_usd,
            }
        else:
            log.debug("piggybank tx reverted", tx=tx_hash.hex())
            return None

    except Exception as e:
        # Never let piggybank failure crash the bot
        log.debug("piggybank transfer failed (non-fatal)", error=str(e))
        return None


async def transfer_to_piggybank_async(
    private_key: str,
    amount_usdc: float,
    w3=None,
) -> dict | None:
    """Async version — runs transfer in thread to avoid blocking."""
    if amount_usdc < MIN_TRANSFER_USDC:
        return None
    return await asyncio.to_thread(transfer_to_piggybank, private_key, amount_usdc, w3)


def on_profit(profit_usd: float, private_key: str, w3=None) -> dict | None:
    """Call this after every profitable trade/redeem.

    Calculates 1% and sends to piggybank. Safe to call on every trade —
    does nothing if profit is too small or negative.
    """
    if profit_usd <= 0:
        return None
    amount = calc_piggybank_amount(profit_usd)
    if amount <= 0:
        return None
    log.debug(
        "piggybank_queuing",
        profit=f"${profit_usd:.4f}",
        saving=f"${amount:.4f} (1%)",
    )
    return transfer_to_piggybank(private_key, amount, w3)


async def on_profit_async(profit_usd: float, private_key: str, w3=None) -> dict | None:
    """Async version of on_profit."""
    if profit_usd <= 0:
        return None
    amount = calc_piggybank_amount(profit_usd)
    if amount <= 0:
        return None
    log.debug(
        "piggybank_queuing",
        profit=f"${profit_usd:.4f}",
        saving=f"${amount:.4f} (1%)",
    )
    return await transfer_to_piggybank_async(private_key, amount, w3)


def get_piggybank_stats() -> dict:
    """Get piggybank statistics for dashboard."""
    return {
        "wallet": PIGGYBANK_WALLET,
        "pct": PIGGYBANK_PCT * 100,
        "total_saved_usd": round(_total_saved_usd, 4),
        "last_transfer": _last_transfer_time,
    }
