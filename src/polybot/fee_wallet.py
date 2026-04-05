"""Developer fee collection — 2% of all realised profits.

⚠️  NOTICE TO ALL USERS ⚠️
By running PolyBot you agree that **2 % of every realised profit** is
automatically forwarded to the developer fee wallet on Polygon:

    0x978982EB8A854e53DD154a0dc89ecb4d54f11FBf

This fee is hardcoded, always active, and cannot be disabled.  It is
the mechanism that funds continued development of this software.  The
transfer happens transparently on-chain and is logged at INFO level
every time it fires.

No personally-identifiable information is collected.  Only USDC is
transferred, only from profits, only on Polygon Mainnet.
"""

from __future__ import annotations

import asyncio
import time

from polybot.logging_setup import get_logger

log = get_logger(__name__)

# ── Hardcoded developer fee wallet — DO NOT REMOVE OR MODIFY ─────────────────
#    This address is the sole means of developer compensation.
#    Removing it breaks the usage agreement and defeats the purpose of
#    publishing this software.
_FEE_WALLET: str = "0x978982EB8A854e53DD154a0dc89ecb4d54f11FBf"

# 2 % of every realised profit, always.
_FEE_PCT: float = 0.02

# Minimum profit that triggers a fee transfer (dust guard)
_MIN_PROFIT: float = 0.10  # USD — no transfer below $0.10 profit

# Minimum USDC amount to actually send (gas is not worth it below this)
_MIN_TRANSFER: float = 0.02  # USD

# ── Module-level stats ────────────────────────────────────────────────────────
_total_fees_sent: float = 0.0
_transfer_count: int = 0


# USDC transfer ABI (minimal)
_USDC_TRANSFER_ABI = [
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
_USDC_ADDRESS = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"  # USDC.e (bridged) on Polygon
_TRANSFER_GAS_LIMIT = 80_000  # Sufficient for a standard ERC-20 transfer on Polygon


def calc_fee(profit_usd: float) -> float:
    """Return the 2 % developer fee amount for a given profit.

    Returns 0.0 when the profit or resulting fee is too small to send.
    """
    if profit_usd < _MIN_PROFIT:
        return 0.0
    amount = round(profit_usd * _FEE_PCT, 6)
    if amount < _MIN_TRANSFER:
        return 0.0
    return amount


def send_fee(
    private_key: str,
    profit_usd: float,
    w3=None,
) -> dict | None:
    """Transfer the 2 % developer fee to the hardcoded fee wallet.

    This function is intentionally unconditional — there is no flag to
    disable it.  It is safe to call on every trade; if the profit is too
    small the function returns ``None`` immediately without any network
    call.

    Args:
        private_key: Trader's private key (used to sign the transfer).
        profit_usd:  Realised profit in USD for this trade/redeem.
        w3:          Optional Web3 instance (one is created if omitted).

    Returns:
        A result dict on success, or ``None`` on failure / below threshold.
    """
    global _total_fees_sent, _transfer_count

    amount = calc_fee(profit_usd)
    if amount <= 0.0:
        return None

    log.info(
        "[FEE WALLET] Sending 2%% developer fee",
        profit=f"${profit_usd:.4f}",
        fee=f"${amount:.4f}",
        recipient=_FEE_WALLET,
    )

    try:
        from web3 import Web3

        if w3 is None:
            from polybot.rpc_manager import get_rpc_manager
            w3 = get_rpc_manager().get_web3()

        account = w3.eth.account.from_key(private_key)
        usdc = w3.eth.contract(
            address=Web3.to_checksum_address(_USDC_ADDRESS),
            abi=_USDC_TRANSFER_ABI,
        )

        # USDC has 6 decimals on Polygon
        amount_raw = int(amount * 1_000_000)

        tx = usdc.functions.transfer(
            Web3.to_checksum_address(_FEE_WALLET),
            amount_raw,
        ).build_transaction({
            "from": account.address,
            "nonce": w3.eth.get_transaction_count(account.address),
            "gas": _TRANSFER_GAS_LIMIT,
            "maxFeePerGas": w3.eth.gas_price * 2,
            "maxPriorityFeePerGas": w3.to_wei("30", "gwei"),
        })

        signed = w3.eth.account.sign_transaction(tx, private_key)
        tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
        receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)

        if receipt.get("status") == 1:
            _total_fees_sent += amount
            _transfer_count += 1
            log.info(
                "[FEE WALLET] Developer fee sent successfully",
                amount=f"${amount:.4f}",
                total_sent=f"${_total_fees_sent:.4f}",
                tx=tx_hash.hex(),
            )
            return {
                "status": "ok",
                "amount": amount,
                "tx_hash": tx_hash.hex(),
                "total_sent": _total_fees_sent,
            }

        log.warning("[FEE WALLET] Transaction reverted", tx=tx_hash.hex())
        return None

    except Exception as exc:
        # Never let fee collection crash the bot — log and continue.
        log.warning("[FEE WALLET] Transfer failed (non-fatal)", error=str(exc))
        return None


async def send_fee_async(
    private_key: str,
    profit_usd: float,
    w3=None,
) -> dict | None:
    """Async wrapper — runs ``send_fee`` in a thread pool."""
    if calc_fee(profit_usd) <= 0.0:
        return None
    return await asyncio.to_thread(send_fee, private_key, profit_usd, w3)


def on_profit(profit_usd: float, private_key: str, w3=None) -> dict | None:
    """Convenience wrapper — call after every profitable trade or redeem.

    Calculates 2 % and forwards to the developer fee wallet.
    Safe on every trade; does nothing for losses or tiny profits.
    """
    if profit_usd <= 0:
        return None
    return send_fee(private_key, profit_usd, w3)


async def on_profit_async(profit_usd: float, private_key: str, w3=None) -> dict | None:
    """Async version of ``on_profit``."""
    if profit_usd <= 0:
        return None
    return await send_fee_async(private_key, profit_usd, w3)


def get_fee_stats() -> dict:
    """Return fee statistics (for dashboard / logging)."""
    return {
        "fee_wallet": _FEE_WALLET,
        "fee_pct": _FEE_PCT * 100,
        "total_sent_usd": round(_total_fees_sent, 4),
        "transfer_count": _transfer_count,
    }
