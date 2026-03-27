"""Startup checks module for PolyBot.

Performs essential startup validations including:
- RPC connection verification with Alchemy priority
- USDC allowance check and auto-approve if needed
- Wallet balance verification

These checks run automatically at app startup to ensure the bot is ready to trade.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from web3.exceptions import ContractLogicError

from polybot.config import get_settings
from polybot.rpc_manager import PolygonRpcManager

logger = logging.getLogger(__name__)

# Minimum allowance threshold in USDC (raw units, 6 decimals)
MIN_ALLOWANCE_RAW = 10_000 * 10**6  # 10,000 USDC

# Max uint256 for unlimited approval
MAX_UINT256 = 2**256 - 1

# ERC20 ABI for approve and allowance functions
ERC20_ABI = [
    {
        "constant": True,
        "inputs": [{"name": "_owner", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"name": "balance", "type": "uint256"}],
        "type": "function",
    },
    {
        "constant": True,
        "inputs": [
            {"name": "_owner", "type": "address"},
            {"name": "_spender", "type": "address"},
        ],
        "name": "allowance",
        "outputs": [{"name": "", "type": "uint256"}],
        "type": "function",
    },
    {
        "inputs": [
            {"name": "spender", "type": "address"},
            {"name": "amount", "type": "uint256"},
        ],
        "name": "approve",
        "outputs": [{"name": "", "type": "bool"}],
        "stateMutability": "nonpayable",
        "type": "function",
    },
]


class AllowanceStatus:
    """Status of USDC allowance check and approval."""

    def __init__(self) -> None:
        self.checked = False
        self.allowance_usdc: float = 0.0
        self.balance_usdc: float = 0.0
        self.needs_approval = False
        self.approval_attempted = False
        self.approval_success = False
        self.approval_tx_hash: str | None = None
        self.error: str | None = None
        self.warnings: list[str] = []

    def to_dict(self) -> dict[str, Any]:
        """Convert status to dictionary for API response."""
        return {
            "checked": self.checked,
            "allowance_usdc": self.allowance_usdc,
            "balance_usdc": self.balance_usdc,
            "needs_approval": self.needs_approval,
            "approval_attempted": self.approval_attempted,
            "approval_success": self.approval_success,
            "approval_tx_hash": self.approval_tx_hash,
            "error": self.error,
            "warnings": self.warnings,
            "status": self._get_status_message(),
        }

    def _get_status_message(self) -> str:
        """Get human-readable status message."""
        if self.error:
            return f"⚠️ Fehler: {self.error}"
        if not self.checked:
            return "⏳ Prüfung läuft..."
        if self.approval_success:
            return "✅ Auto-Approve erfolgreich"
        if self.needs_approval and not self.approval_attempted:
            return "⚠️ Manuell freigeben nötig"
        if self.needs_approval and self.approval_attempted:
            return "❌ Auto-Approve fehlgeschlagen"
        # Use settings threshold for consistency
        settings = get_settings()
        if self.allowance_usdc >= settings.min_allowance_usdc:
            return f"✅ {self.allowance_usdc:,.0f} USDC freigegeben"
        return f"⚠️ Low allowance: ${self.allowance_usdc:.2f}"


# Global allowance status
_allowance_status: AllowanceStatus | None = None


def get_allowance_status() -> AllowanceStatus:
    """Get the global allowance status singleton."""
    global _allowance_status
    if _allowance_status is None:
        _allowance_status = AllowanceStatus()
    return _allowance_status


async def check_allowance(rpc_manager: PolygonRpcManager) -> AllowanceStatus:
    """Check current USDC allowance for CTF Exchange.

    Args:
        rpc_manager: RPC manager instance for Web3 calls

    Returns:
        AllowanceStatus with current allowance information
    """
    status = get_allowance_status()
    settings = get_settings()

    wallet = settings.wallet_address
    if not wallet:
        status.error = "WALLET_ADDRESS nicht konfiguriert"
        status.warnings.append("⚠️ WALLET_ADDRESS nicht gesetzt")
        return status

    try:
        w3 = await rpc_manager.get_best_provider(timeout=5.0)

        # Get USDC contract
        usdc_address = settings.usdc_address
        usdc_contract = w3.eth.contract(
            address=w3.to_checksum_address(usdc_address),
            abi=ERC20_ABI,
        )

        # Check balance
        balance_raw = await usdc_contract.functions.balanceOf(
            w3.to_checksum_address(wallet)
        ).call()
        status.balance_usdc = balance_raw / 1e6

        # Check allowance for CTF Exchange (Polymarket Router)
        ctf_exchange = settings.ctf_exchange
        allowance_raw = await usdc_contract.functions.allowance(
            w3.to_checksum_address(wallet),
            w3.to_checksum_address(ctf_exchange),
        ).call()
        status.allowance_usdc = allowance_raw / 1e6

        # Determine if approval is needed
        min_allowance = settings.min_allowance_usdc
        status.needs_approval = status.allowance_usdc < min_allowance

        status.checked = True

        if status.allowance_usdc < status.balance_usdc:
            status.warnings.append(
                f"⚠️ Low allowance: ${status.allowance_usdc:.2f} (balance: ${status.balance_usdc:.2f})"
            )

        logger.info(
            f"💰 Allowance: ${status.allowance_usdc:,.2f} | "
            f"Balance: ${status.balance_usdc:,.2f} | "
            f"Needs approval: {status.needs_approval}"
        )

    except ContractLogicError as e:
        status.error = f"Contract error: {e}"
        logger.error(f"❌ Allowance check contract error: {e}")
    except Exception as e:
        status.error = str(e)
        logger.error(f"❌ Allowance check failed: {e}")

    return status


async def ensure_allowance_and_approve(
    rpc_manager: PolygonRpcManager,
    max_retries: int = 3,
    retry_delay: float = 5.0,
) -> AllowanceStatus:
    """Check allowance and auto-approve if needed.

    Performs the following:
    1. Check current USDC allowance for CTF Exchange
    2. If allowance < 10,000 USDC and AUTO_APPROVE=true, approve unlimited
    3. Retry up to 3 times with 5s backoff on failure

    Args:
        rpc_manager: RPC manager instance for Web3 calls
        max_retries: Maximum number of approval attempts
        retry_delay: Delay between retries in seconds

    Returns:
        AllowanceStatus with approval result
    """
    settings = get_settings()
    status = await check_allowance(rpc_manager)

    if status.error:
        return status

    if not status.needs_approval:
        logger.info(f"✅ Allowance OK: ${status.allowance_usdc:,.2f} USDC freigegeben")
        return status

    # Check if auto-approve is enabled
    if not settings.auto_approve:
        logger.warning(
            "⚠️ Allowance zu niedrig und AUTO_APPROVE=false - Manuell freigeben nötig! "
            "Führe 'python -m polybot.approve_usdc' aus oder aktiviere AUTO_APPROVE=true"
        )
        return status

    # Check if we have a private key for signing
    pk = settings.private_key_hex
    if not pk:
        status.error = "POLYGON_PRIVATE_KEY nicht konfiguriert für Auto-Approve"
        logger.error("❌ " + status.error)
        return status

    wallet = settings.wallet_address
    if not wallet:
        status.error = "WALLET_ADDRESS nicht konfiguriert"
        return status

    # Attempt auto-approval with retries
    logger.info(
        f"🔄 Auto-Approve wird gestartet (Allowance: ${status.allowance_usdc:.2f}, "
        f"Minimum: ${settings.min_allowance_usdc:,.0f})..."
    )

    status.approval_attempted = True

    for attempt in range(1, max_retries + 1):
        try:
            w3 = await rpc_manager.get_best_provider(timeout=5.0)

            # Get USDC contract
            usdc_address = settings.usdc_address
            usdc_contract = w3.eth.contract(
                address=w3.to_checksum_address(usdc_address),
                abi=ERC20_ABI,
            )

            # Build approval transaction
            ctf_exchange = settings.ctf_exchange
            nonce = await w3.eth.get_transaction_count(w3.to_checksum_address(wallet))
            gas_price = await w3.eth.gas_price

            # Use slightly higher gas price for faster confirmation
            gas_price_boosted = int(gas_price * 1.1)

            tx = await usdc_contract.functions.approve(
                w3.to_checksum_address(ctf_exchange),
                MAX_UINT256,
            ).build_transaction(
                {
                    "from": w3.to_checksum_address(wallet),
                    "nonce": nonce,
                    "gas": 100_000,
                    "gasPrice": gas_price_boosted,
                    "chainId": settings.chain_id,
                }
            )

            # Sign and send transaction
            signed_tx = w3.eth.account.sign_transaction(tx, pk)
            tx_hash = await w3.eth.send_raw_transaction(signed_tx.raw_transaction)
            status.approval_tx_hash = tx_hash.hex()

            logger.info(f"📤 Approval TX gesendet: {status.approval_tx_hash}")

            # Wait for confirmation
            receipt = await w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)

            if receipt["status"] == 1:
                status.approval_success = True
                status.needs_approval = False
                logger.info(
                    f"✅ Auto-Approve erfolgreich! TX: {status.approval_tx_hash}"
                )
                return status
            else:
                raise Exception("Transaction failed (status=0)")

        except Exception as e:
            logger.warning(
                f"⚠️ Auto-Approve Versuch {attempt}/{max_retries} fehlgeschlagen: {e}"
            )
            if attempt < max_retries:
                logger.info(f"🔄 Retry in {retry_delay}s...")
                await asyncio.sleep(retry_delay)
            else:
                status.error = (
                    f"Auto-Approve nach {max_retries} Versuchen fehlgeschlagen: {e}"
                )
                logger.error(f"❌ {status.error}")

    return status


async def run_startup_checks(rpc_manager: PolygonRpcManager) -> dict[str, Any]:
    """Run all startup checks.

    Performs:
    1. RPC connection test with Alchemy priority (test Alchemy first if configured)
    2. Block number verification
    3. Allowance check and auto-approve if needed

    Args:
        rpc_manager: RPC manager instance

    Returns:
        Dict with check results
    """
    results: dict[str, Any] = {
        "rpc_connected": False,
        "block_number": None,
        "alchemy_status": rpc_manager.get_alchemy_status(),
        "allowance_status": None,
        "errors": [],
        "warnings": [],
    }

    settings = get_settings()

    # 1. Test Alchemy connection specifically if configured
    if rpc_manager.alchemy_configured:
        success, block_number, error_msg = await rpc_manager.test_alchemy_connection(
            timeout=5.0
        )
        if success:
            results["rpc_connected"] = True
            results["block_number"] = block_number
            results["alchemy_status"] = rpc_manager.get_alchemy_status()
            logger.info(f"✅ Alchemy connected at block #{block_number}")
        else:
            # Alchemy configured but connection failed - show clear error
            results["errors"].append(
                error_msg or "Alchemy Connection failed – Key prüfen!"
            )
            results["alchemy_status"] = rpc_manager.get_alchemy_status()
            logger.error(f"❌ {error_msg}")
            # Don't fallback to other RPCs when Alchemy is configured but fails
            # This ensures the user sees the error and fixes their Alchemy key
            return results
    else:
        # No Alchemy configured - use regular RPC connection
        try:
            w3 = await rpc_manager.get_best_provider(timeout=5.0)
            block_number = await w3.eth.block_number
            results["rpc_connected"] = True
            results["block_number"] = block_number

            # Update connection status
            await rpc_manager.update_connection_status(block_number)
            results["alchemy_status"] = rpc_manager.get_alchemy_status()

            logger.info(f"✅ RPC connected at block #{block_number}")

        except Exception as e:
            results["errors"].append(f"RPC connection failed: {e}")
            logger.error(f"❌ RPC connection failed: {e}")
            return results

    # 2. Check and auto-approve allowance if needed
    if settings.wallet_address:
        try:
            allowance_status = await ensure_allowance_and_approve(rpc_manager)
            results["allowance_status"] = allowance_status.to_dict()
            results["warnings"].extend(allowance_status.warnings)
            if allowance_status.error:
                results["errors"].append(allowance_status.error)
        except Exception as e:
            results["errors"].append(f"Allowance check failed: {e}")
            logger.error(f"❌ Allowance check failed: {e}")
    else:
        results["warnings"].append("WALLET_ADDRESS nicht konfiguriert")

    return results
