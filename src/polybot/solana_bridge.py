"""Solana integration for PolyBot.

Provides functionality to:
- Monitor Solana wallet for incoming SOL deposits
- Swap SOL to USDC via Jupiter DEX aggregator
- Bridge USDC from Solana to Polygon via Wormhole

This enables automatic funding of Polymarket trades with Solana.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from decimal import Decimal

from polybot.config import get_settings
from polybot.logging_setup import get_logger

log = get_logger(__name__)

# Solana constants (public addresses, not secrets)
SOLANA_USDC_MINT = (
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"  # pragma: allowlist secret
)
WRAPPED_SOL_MINT = (
    "So11111111111111111111111111111111111111112"  # pragma: allowlist secret
)
LAMPORTS_PER_SOL = 1_000_000_000
USDC_DECIMALS = 6

# Jupiter DEX aggregator API (v6)
JUPITER_QUOTE_API = "https://quote-api.jup.ag/v6/quote"
JUPITER_SWAP_API = "https://quote-api.jup.ag/v6/swap"

# Wormhole bridge contracts (public addresses)
WORMHOLE_BRIDGE_ADDRESS = (
    "worm2ZoG2kUd4vFXhvjh93UUH596ayRfgQ2MgjNMTth"  # pragma: allowlist secret
)
WORMHOLE_TOKEN_BRIDGE = (
    "wormDTUJ6AWPNvk59vGQbDvGJmqbDTdgWgAqcLBCgUb"  # pragma: allowlist secret
)

# Polygon USDC for bridge target
POLYGON_USDC = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
POLYGON_CHAIN_ID = 5  # Wormhole chain ID for Polygon

# SOL reserved for transaction fees (minimum to keep in wallet)
TRANSACTION_FEE_RESERVE_SOL = Decimal("0.01")


@dataclass
class SolanaWalletInfo:
    """Information about a Solana wallet."""

    address: str
    sol_balance: Decimal = Decimal("0")
    usdc_balance: Decimal = Decimal("0")
    last_updated: float = field(default_factory=time.time)


@dataclass
class SwapQuote:
    """Quote for a SOL to USDC swap."""

    input_amount: Decimal
    output_amount: Decimal
    price_impact: Decimal
    route_info: dict = field(default_factory=dict)
    quote_response: dict = field(default_factory=dict)


@dataclass
class BridgeResult:
    """Result of a bridge operation."""

    success: bool
    tx_signature: str = ""
    amount_bridged: Decimal = Decimal("0")
    target_address: str = ""
    error: str = ""


def _get_solana_client():
    """Get Solana RPC client."""
    settings = get_settings()
    try:
        from solana.rpc.api import Client as SolanaClient

        return SolanaClient(settings.solana_rpc_url)
    except ImportError:
        log.error("Solana SDK not installed. Run: pip install solana solders")
        return None


def _get_keypair():
    """Get Solana keypair from settings."""
    settings = get_settings()
    secret = settings.solana_private_key.get_secret_value()
    if not secret:
        return None
    try:
        from solders.keypair import Keypair
        import base58

        return Keypair.from_bytes(base58.b58decode(secret))
    except Exception as e:
        log.error("Failed to load Solana keypair", error=str(e))
        return None


def get_solana_address() -> str | None:
    """Get the public address of the configured Solana wallet."""
    keypair = _get_keypair()
    if keypair:
        return str(keypair.pubkey())
    return None


def get_wallet_info() -> SolanaWalletInfo | None:
    """Get current wallet balances (SOL and USDC).

    Returns:
        SolanaWalletInfo with current balances, or None if unavailable.
    """
    client = _get_solana_client()
    keypair = _get_keypair()

    if not client or not keypair:
        return None

    try:
        from solders.pubkey import Pubkey

        pubkey = keypair.pubkey()
        address = str(pubkey)

        # Get SOL balance
        sol_resp = client.get_balance(pubkey)
        sol_lamports = sol_resp.value if hasattr(sol_resp, "value") else 0
        sol_balance = Decimal(str(sol_lamports)) / Decimal(str(LAMPORTS_PER_SOL))

        # Get USDC balance (SPL token)
        usdc_balance = Decimal("0")
        try:
            usdc_mint = Pubkey.from_string(SOLANA_USDC_MINT)
            # Find associated token account
            from spl.token.constants import (
                ASSOCIATED_TOKEN_PROGRAM_ID,
                TOKEN_PROGRAM_ID,
            )

            ata = Pubkey.find_program_address(
                [bytes(pubkey), bytes(TOKEN_PROGRAM_ID), bytes(usdc_mint)],
                ASSOCIATED_TOKEN_PROGRAM_ID,
            )[0]

            token_resp = client.get_token_account_balance(ata)
            if hasattr(token_resp, "value") and token_resp.value:
                usdc_balance = Decimal(token_resp.value.amount) / Decimal(
                    10**USDC_DECIMALS
                )
        except Exception:
            # No USDC token account or zero balance
            pass

        return SolanaWalletInfo(
            address=address,
            sol_balance=sol_balance,
            usdc_balance=usdc_balance,
            last_updated=time.time(),
        )
    except Exception as e:
        log.error("Failed to get wallet info", error=str(e))
        return None


async def get_swap_quote(
    sol_amount: Decimal, slippage_bps: int = 50
) -> SwapQuote | None:
    """Get a quote for swapping SOL to USDC via Jupiter.

    Args:
        sol_amount: Amount of SOL to swap
        slippage_bps: Slippage tolerance in basis points (50 = 0.5%)

    Returns:
        SwapQuote with output amount and route info, or None if failed.
    """
    try:
        import httpx

        # Convert SOL to lamports
        lamports = int(sol_amount * LAMPORTS_PER_SOL)

        params = {
            "inputMint": WRAPPED_SOL_MINT,
            "outputMint": SOLANA_USDC_MINT,
            "amount": str(lamports),
            "slippageBps": slippage_bps,
        }

        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(JUPITER_QUOTE_API, params=params)
            resp.raise_for_status()
            quote_data = resp.json()

        if not quote_data:
            log.warning("Empty quote response from Jupiter")
            return None

        out_amount = Decimal(quote_data.get("outAmount", 0)) / Decimal(
            10**USDC_DECIMALS
        )
        price_impact = Decimal(str(quote_data.get("priceImpactPct", 0)))

        return SwapQuote(
            input_amount=sol_amount,
            output_amount=out_amount,
            price_impact=price_impact,
            route_info=quote_data.get("routePlan", []),
            quote_response=quote_data,
        )
    except Exception as e:
        log.error("Failed to get swap quote", error=str(e))
        return None


async def execute_swap(quote: SwapQuote, dry_run: bool = True) -> dict | None:
    """Execute a SOL to USDC swap via Jupiter.

    Args:
        quote: The swap quote to execute
        dry_run: If True, only simulate the swap

    Returns:
        Transaction result dict or None if failed.
    """
    keypair = _get_keypair()
    if not keypair:
        log.error("No Solana keypair configured")
        return None

    if dry_run:
        log.info(
            "DRY RUN — would swap SOL to USDC",
            sol_amount=f"{quote.input_amount:.4f}",
            usdc_amount=f"${quote.output_amount:.2f}",
            price_impact=f"{quote.price_impact:.4f}%",
        )
        return {
            "dry_run": True,
            "input_amount": float(quote.input_amount),
            "output_amount": float(quote.output_amount),
        }

    try:
        import httpx
        from solders.transaction import VersionedTransaction
        import base64

        pubkey_str = str(keypair.pubkey())

        # Get swap transaction from Jupiter
        swap_request = {
            "quoteResponse": quote.quote_response,
            "userPublicKey": pubkey_str,
            "wrapAndUnwrapSol": True,
            "dynamicComputeUnitLimit": True,
            "prioritizationFeeLamports": "auto",
        }

        async with httpx.AsyncClient(timeout=60) as client:
            resp = await client.post(JUPITER_SWAP_API, json=swap_request)
            resp.raise_for_status()
            swap_data = resp.json()

        swap_tx_data = swap_data.get("swapTransaction")
        if not swap_tx_data:
            log.error("No swap transaction in response")
            return None

        # Decode and sign transaction
        tx_bytes = base64.b64decode(swap_tx_data)
        tx = VersionedTransaction.from_bytes(tx_bytes)

        # Sign the transaction
        signed_tx = VersionedTransaction(tx.message, [keypair])

        # Submit transaction
        sol_client = _get_solana_client()
        if not sol_client:
            return None

        # Serialize and send
        tx_opts = {"skip_preflight": False, "max_retries": 3}
        result = sol_client.send_transaction(signed_tx, opts=tx_opts)

        if hasattr(result, "value"):
            sig = str(result.value)
            log.info(
                "Swap transaction submitted",
                signature=sig,
                sol_amount=f"{quote.input_amount:.4f}",
                usdc_expected=f"${quote.output_amount:.2f}",
            )
            return {
                "success": True,
                "signature": sig,
                "input_amount": float(quote.input_amount),
                "output_amount": float(quote.output_amount),
            }
        else:
            log.error("Failed to submit swap transaction", result=str(result))
            return None

    except Exception as e:
        log.error("Swap execution failed", error=str(e))
        return None


async def bridge_usdc_to_polygon(
    usdc_amount: Decimal,
    polygon_address: str,
    dry_run: bool = True,
) -> BridgeResult:
    """Bridge USDC from Solana to Polygon via Wormhole.

    Args:
        usdc_amount: Amount of USDC to bridge
        polygon_address: Target wallet address on Polygon
        dry_run: If True, only simulate the bridge

    Returns:
        BridgeResult with status and transaction info.
    """
    if dry_run:
        log.info(
            "DRY RUN — would bridge USDC to Polygon",
            usdc_amount=f"${usdc_amount:.2f}",
            target=polygon_address[:10] + "...",
        )
        return BridgeResult(
            success=True,
            amount_bridged=usdc_amount,
            target_address=polygon_address,
            error="dry_run",
        )

    keypair = _get_keypair()
    if not keypair:
        return BridgeResult(success=False, error="No Solana keypair configured")

    try:
        # Wormhole bridge implementation
        # Note: Full Wormhole integration requires the wormhole-sdk
        # This is a simplified implementation using direct RPC calls

        sol_client = _get_solana_client()
        if not sol_client:
            return BridgeResult(success=False, error="No Solana client")

        log.info(
            "Initiating Wormhole bridge",
            amount=f"${usdc_amount:.2f}",
            target_chain="Polygon",
        )

        # For a production implementation, we would:
        # 1. Approve USDC spending by Wormhole token bridge
        # 2. Call transfer_tokens on Wormhole token bridge
        # 3. Wait for VAA (Verifiable Action Approval)
        # 4. Submit VAA on Polygon to complete bridge

        # Simplified: Log that full implementation is pending
        log.warning(
            "Full Wormhole bridge requires additional integration. "
            "For now, please manually bridge USDC via portal.wormhole.com"
        )

        return BridgeResult(
            success=False,
            amount_bridged=usdc_amount,
            target_address=polygon_address,
            error="full_wormhole_integration_pending",
        )

    except Exception as e:
        log.error("Bridge operation failed", error=str(e))
        return BridgeResult(success=False, error=str(e))


async def auto_swap_and_bridge(
    min_sol_amount: Decimal | None = None,
    dry_run: bool = True,
) -> dict:
    """Automatically swap SOL to USDC and bridge to Polygon.

    This is the main entry point for the auto-funding flow:
    1. Check SOL balance
    2. If above threshold, swap SOL to USDC
    3. Bridge USDC to Polygon wallet

    Args:
        min_sol_amount: Minimum SOL to trigger swap (uses config if None)
        dry_run: If True, only simulate operations

    Returns:
        Dict with status and operation details.
    """
    settings = get_settings()

    if min_sol_amount is None:
        min_sol_amount = Decimal(str(settings.sol_swap_threshold))

    # Check wallet
    wallet = get_wallet_info()
    if not wallet:
        return {"success": False, "error": "Could not get wallet info"}

    log.info(
        "Checking Solana wallet for auto-funding",
        address=wallet.address[:10] + "...",
        sol_balance=f"{wallet.sol_balance:.4f} SOL",
        usdc_balance=f"${wallet.usdc_balance:.2f}",
    )

    result = {
        "wallet_address": wallet.address,
        "initial_sol": float(wallet.sol_balance),
        "initial_usdc": float(wallet.usdc_balance),
        "operations": [],
    }

    # Check if we should swap SOL
    if not settings.auto_swap_sol:
        log.info("Auto-swap disabled in settings")
        result["swap_disabled"] = True
        return result

    if wallet.sol_balance < min_sol_amount:
        log.info(
            "SOL balance below swap threshold",
            balance=f"{wallet.sol_balance:.4f}",
            threshold=f"{min_sol_amount:.4f}",
        )
        result["below_threshold"] = True
        return result

    # Calculate swap amount (keep some SOL for transaction fees)
    sol_to_swap = wallet.sol_balance - TRANSACTION_FEE_RESERVE_SOL

    if sol_to_swap <= Decimal("0"):
        log.info("Not enough SOL after reserving fees")
        result["insufficient_after_fees"] = True
        return result

    # Get swap quote
    quote = await get_swap_quote(sol_to_swap)
    if not quote:
        result["error"] = "Could not get swap quote"
        return result

    log.info(
        "Swap quote received",
        sol_amount=f"{quote.input_amount:.4f}",
        usdc_amount=f"${quote.output_amount:.2f}",
        price_impact=f"{quote.price_impact:.4f}%",
    )

    # Check price impact
    if quote.price_impact > Decimal("1"):
        log.warning(
            "Price impact too high, skipping swap",
            impact=f"{quote.price_impact:.2f}%",
        )
        result["price_impact_too_high"] = float(quote.price_impact)
        return result

    # Execute swap
    swap_result = await execute_swap(quote, dry_run=dry_run)
    if swap_result:
        result["operations"].append({"type": "swap", "result": swap_result})

        # If swap succeeded and not dry run, bridge to Polygon
        if swap_result.get("success") and not dry_run:
            polygon_address = settings.wallet_address
            if not polygon_address and settings.private_key_hex:
                # Derive address from private key
                from web3 import Web3

                w3 = Web3()
                polygon_address = w3.eth.account.from_key(
                    settings.private_key_hex
                ).address

            if polygon_address:
                bridge_result = await bridge_usdc_to_polygon(
                    Decimal(str(swap_result["output_amount"])),
                    polygon_address,
                    dry_run=dry_run,
                )
                result["operations"].append(
                    {
                        "type": "bridge",
                        "result": {
                            "success": bridge_result.success,
                            "amount": float(bridge_result.amount_bridged),
                            "target": bridge_result.target_address,
                            "error": bridge_result.error,
                        },
                    }
                )
            else:
                result["bridge_skipped"] = "No Polygon address configured"
    else:
        result["error"] = "Swap failed"

    result["success"] = len(result.get("operations", [])) > 0
    return result


def check_and_swap(dry_run: bool = True) -> dict | None:
    """Synchronous wrapper for auto_swap_and_bridge.

    For use in the main bot loop.
    """
    try:
        return asyncio.run(auto_swap_and_bridge(dry_run=dry_run))
    except Exception as e:
        log.error("Auto swap and bridge failed", error=str(e))
        return None


# Singleton manager
_bridge_manager: "SolanaBridgeManager | None" = None


class SolanaBridgeManager:
    """Manager for Solana bridge operations.

    Provides monitoring, caching, and rate limiting for bridge operations.
    """

    def __init__(self):
        self._last_check: float = 0
        self._check_interval: int = 60  # seconds
        self._last_wallet_info: SolanaWalletInfo | None = None
        self._pending_operations: list = []

    def get_cached_wallet_info(self, max_age: int = 30) -> SolanaWalletInfo | None:
        """Get wallet info from cache or refresh if stale."""
        now = time.time()
        if (
            self._last_wallet_info
            and (now - self._last_wallet_info.last_updated) < max_age
        ):
            return self._last_wallet_info

        self._last_wallet_info = get_wallet_info()
        return self._last_wallet_info

    def should_check(self) -> bool:
        """Check if we should run another funding check."""
        return (time.time() - self._last_check) >= self._check_interval

    def run_check(self, dry_run: bool = True) -> dict | None:
        """Run a funding check if interval has passed."""
        if not self.should_check():
            return None

        self._last_check = time.time()
        return check_and_swap(dry_run=dry_run)


def get_bridge_manager() -> SolanaBridgeManager:
    """Get the singleton bridge manager instance."""
    global _bridge_manager
    if _bridge_manager is None:
        _bridge_manager = SolanaBridgeManager()
    return _bridge_manager


def format_wallet_info(wallet: SolanaWalletInfo | None) -> str:
    """Format wallet info for display."""
    if not wallet:
        return "❌ Solana wallet not configured or unavailable"

    return (
        f"🌐 **Solana Wallet**\n\n"
        f"Address: `{wallet.address[:8]}...{wallet.address[-6:]}`\n"
        f"SOL: {wallet.sol_balance:.4f}\n"
        f"USDC: ${wallet.usdc_balance:.2f}"
    )
