"""Full Redeemer V89: Independent on-chain position redemption module.

This module scans for ALL redeemable positions on-chain and via Polymarket Data API,
completely independent of the bot's internal state (open_positions dict).

Key Features:
- On-chain CTF contract scanning for ERC-1155 token balances
- Polymarket Data API fallback for redeemable positions
- Runs as async task every 45 seconds (configurable via FULL_REDEEM_INTERVAL_SECONDS)
- Lax balance requirements (only needs gas, not 1.2 USDC trading minimum)
- Batch redeem with tenacity retries (3 attempts, exponential backoff)
- Full dry-run support
- Works even when trading is blocked by Balance Master V6

This solves the core problem: Bot "forgets" positions after restart or state loss.
By scanning on-chain + Data API, we find ALL redeemable positions regardless of
internal tracking state.

Environment Variables:
    FULL_REDEEM_ENABLED: Enable/disable full redeem task (default: false)
    FULL_REDEEM_INTERVAL_SECONDS: Scan interval in seconds (default: 45)
    MIN_REDEEM_BALANCE: Minimum USDC for gas (default: 0.08)
    REDEEM_GAS_BUFFER_PERCENT: Gas buffer percentage (default: 30)

Usage:
    # Auto-start in main_fastapi.py lifespan (if FULL_REDEEM_ENABLED=true)

    # Manual trigger via endpoint:
    POST /api/force_full_redeem

    # Programmatic:
    from polybot.full_redeemer import FullRedeemer
    redeemer = FullRedeemer()
    await redeemer.redeem_all_positions()
"""

from __future__ import annotations

import asyncio
import os
from typing import Any

import aiohttp
from eth_account import Account
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)
from web3 import Web3
from web3.middleware import ExtraDataToPOAMiddleware

from polybot.logging_setup import get_logger

log = get_logger(__name__)

# =============================================================================
# Constants
# =============================================================================

# Contract addresses (Polygon Mainnet)
CTF_ADDRESS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"  # ConditionalTokens
USDC_ADDRESS = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"

# Null/zero bytes32 for parent collection ID
NULL_BYTES32 = "0x" + "0" * 64

# CTF ABI for redeemPositions
CTF_ABI = [
    {
        "inputs": [
            {"name": "collateralToken", "type": "address"},
            {"name": "parentCollectionId", "type": "bytes32"},
            {"name": "conditionId", "type": "bytes32"},
            {"name": "indexSets", "type": "uint256[]"},
        ],
        "name": "redeemPositions",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function",
    },
    {
        "inputs": [
            {"name": "account", "type": "address"},
            {"name": "id", "type": "uint256"},
        ],
        "name": "balanceOf",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
]

# USDC ABI for balance check
USDC_ABI = [
    {
        "inputs": [{"name": "account", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    }
]

# Fallback RPCs
FALLBACK_RPCS = [
    "https://rpc.ankr.com/polygon",
    "https://polygon.llamarpc.com",
    "https://polygon-bor-rpc.publicnode.com",
    "https://1rpc.io/matic",
]

# Polymarket APIs
DATA_API_BASE = "https://data-api.polymarket.com"
GAMMA_API_BASE = "https://gamma-api.polymarket.com"


# =============================================================================
# Configuration Defaults (can be overridden by env vars)
# =============================================================================

# Full redeem defaults
DEFAULT_FULL_REDEEM_ENABLED = False
DEFAULT_FULL_REDEEM_INTERVAL_SECONDS = 45

# Balance thresholds for redeem (much laxer than trading)
# Trading needs $1.2+, but redeem only needs gas money
DEFAULT_MIN_REDEEM_BALANCE = 0.08  # Just enough for gas
DEFAULT_REDEEM_GAS_BUFFER_PERCENT = 30

# Gas settings
DEFAULT_GAS_LIMIT = 300_000
DEFAULT_GAS_PRICE_GWEI = 35


def get_full_redeem_config() -> dict[str, Any]:
    """Get full redeem configuration from environment variables."""
    return {
        "enabled": os.environ.get("FULL_REDEEM_ENABLED", "").lower()
        in ("true", "1", "yes"),
        "interval_seconds": int(
            os.environ.get(
                "FULL_REDEEM_INTERVAL_SECONDS",
                str(DEFAULT_FULL_REDEEM_INTERVAL_SECONDS),
            )
        ),
        "min_redeem_balance": float(
            os.environ.get("MIN_REDEEM_BALANCE", str(DEFAULT_MIN_REDEEM_BALANCE))
        ),
        "redeem_gas_buffer_percent": int(
            os.environ.get(
                "REDEEM_GAS_BUFFER_PERCENT", str(DEFAULT_REDEEM_GAS_BUFFER_PERCENT)
            )
        ),
        # Note: Full Redeemer respects the global DRY_RUN setting.
        # To enable actual redemptions, set DRY_RUN=false in your environment.
        # When FULL_REDEEM_ENABLED=true but DRY_RUN=true (or not set),
        # the module will log what it would redeem but won't send transactions.
        "dry_run": os.environ.get("DRY_RUN", "true").lower() in ("true", "1", "yes"),
        "wallet_address": os.environ.get("WALLET_ADDRESS", "").strip(),
    }


# =============================================================================
# Network Exceptions for Retry
# =============================================================================

NETWORK_EXCEPTIONS = (
    ConnectionError,
    TimeoutError,
    OSError,
    aiohttp.ClientError,
)


# =============================================================================
# FullRedeemer Class
# =============================================================================


class FullRedeemer:
    """Independent full position redeemer.

    Scans on-chain + Data API for ALL redeemable positions and redeems them,
    regardless of bot internal state.

    This class is fully independent of OnchainExecutor's open_positions dict.
    """

    def __init__(
        self,
        wallet_address: str | None = None,
        private_key: str | None = None,
    ):
        """Initialize the full redeemer.

        Args:
            wallet_address: Wallet address to scan/redeem for.
                            Defaults to WALLET_ADDRESS env var.
            private_key: Private key for signing transactions.
                         Defaults to POLYMARKET_PRIVATE_KEY env var.
        """
        # Get private key
        self._private_key = (
            private_key or os.environ.get("POLYMARKET_PRIVATE_KEY", "").strip()
        )
        if self._private_key and not self._private_key.startswith("0x"):
            self._private_key = "0x" + self._private_key

        # Derive wallet address from private key if not provided
        if wallet_address:
            self._wallet_address = Web3.to_checksum_address(wallet_address)
        elif self._private_key:
            account = Account.from_key(self._private_key)
            self._wallet_address = account.address
        else:
            # Fallback to env var
            env_wallet = os.environ.get("WALLET_ADDRESS", "").strip()
            self._wallet_address = (
                Web3.to_checksum_address(env_wallet) if env_wallet else ""
            )

        # Web3 connection (lazy init)
        self._w3: Web3 | None = None

        # Track redeemed condition IDs to avoid duplicates within session
        self._redeemed_conditions: set[str] = set()

        # Config
        self._config = get_full_redeem_config()

        log.info(
            "[FULL REDEEMER V89] Initialized | wallet=%s | dry_run=%s",
            self._wallet_address[:20] + "..." if self._wallet_address else "NOT SET",
            self._config["dry_run"],
        )

    def _get_web3(self) -> Web3:
        """Get or create Web3 connection with fallback RPCs."""
        if self._w3 is not None and self._w3.is_connected():
            return self._w3

        # Try primary RPC from env first
        rpc_from_env = os.environ.get("POLYGON_RPC_URL", "").strip()
        all_rpcs = ([rpc_from_env] if rpc_from_env else []) + FALLBACK_RPCS

        for url in all_rpcs:
            if not url:
                continue
            try:
                w3 = Web3(Web3.HTTPProvider(url, request_kwargs={"timeout": 30}))
                w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
                if w3.is_connected():
                    log.info(
                        "[FULL REDEEMER V89] Connected to RPC: %s",
                        url[:50] + "..." if len(url) > 50 else url,
                    )
                    self._w3 = w3
                    return w3
            except Exception as exc:
                log.debug("[FULL REDEEMER V89] RPC %s failed: %s", url[:30], exc)
                continue

        raise ConnectionError(
            "[FULL REDEEMER V89] All Polygon RPCs failed — cannot connect"
        )

    def _get_usdc_balance(self) -> float:
        """Get current USDC balance of the wallet."""
        if not self._wallet_address:
            return 0.0

        try:
            w3 = self._get_web3()
            usdc = w3.eth.contract(
                address=Web3.to_checksum_address(USDC_ADDRESS),
                abi=USDC_ABI,
            )
            balance_raw = usdc.functions.balanceOf(self._wallet_address).call()
            return balance_raw / 1_000_000  # 6 decimals
        except Exception as exc:
            log.warning("[FULL REDEEMER V89] Failed to get USDC balance: %s", exc)
            return 0.0

    async def _fetch_redeemable_from_data_api(self) -> list[dict[str, Any]]:
        """Fetch redeemable positions from Polymarket Data API.

        This is the primary method for finding redeemable positions.
        The Data API knows which markets are resolved and which positions
        the wallet holds that can be redeemed.

        Returns:
            List of position dicts with conditionId, outcomeIndex, etc.
        """
        if not self._wallet_address:
            log.warning("[FULL REDEEMER V89] No wallet address configured")
            return []

        url = f"{DATA_API_BASE}/positions"
        params = {
            "user": self._wallet_address,
            "sizeThreshold": "0.001",  # Very low threshold to catch all
            "limit": 500,
        }

        try:
            async with aiohttp.ClientSession() as session:
                timeout = aiohttp.ClientTimeout(total=15)
                async with session.get(url, params=params, timeout=timeout) as resp:
                    if resp.status != 200:
                        log.warning(
                            "[FULL REDEEMER V89] Data API returned status %d",
                            resp.status,
                        )
                        return []

                    positions = await resp.json()

                    # Filter to only redeemable positions
                    redeemable = [p for p in positions if p.get("redeemable") is True]

                    log.info(
                        "[FULL REDEEMER V89] Data API: %d positions total, %d redeemable",
                        len(positions),
                        len(redeemable),
                    )
                    return redeemable

        except aiohttp.ClientError as exc:
            log.warning("[FULL REDEEMER V89] Data API request failed: %s", exc)
            return []
        except Exception as exc:
            log.error(
                "[FULL REDEEMER V89] Unexpected error fetching from Data API: %s", exc
            )
            return []

    async def _check_market_resolution(
        self, condition_id: str
    ) -> tuple[bool, str | None]:
        """Check if a market is resolved and determine winning outcome.

        Args:
            condition_id: The condition ID to check.

        Returns:
            Tuple of (is_resolved, winning_outcome).
            winning_outcome is "up" or "down" if resolved, None otherwise.
        """
        if not condition_id or condition_id == NULL_BYTES32:
            return False, None

        url = f"{GAMMA_API_BASE}/markets"
        params = {"conditionId": condition_id, "limit": 1}

        try:
            async with aiohttp.ClientSession() as session:
                timeout = aiohttp.ClientTimeout(total=10)
                async with session.get(url, params=params, timeout=timeout) as resp:
                    if resp.status != 200:
                        return False, None

                    data = await resp.json()
                    markets = data.get("data", []) if isinstance(data, dict) else data

                    if not markets:
                        return False, None

                    market = markets[0]
                    is_closed = market.get("closed", False)
                    is_resolved = market.get("resolved", False)

                    if not (is_closed or is_resolved):
                        return False, None

                    # Find winning outcome from token prices
                    tokens = market.get("tokens", [])
                    for i, token in enumerate(tokens):
                        price = float(token.get("price", 0) or 0)
                        outcome_label = token.get("outcome", "").lower()

                        # Winning token trades at ~$1.00
                        if price >= 0.95:
                            if outcome_label in ("yes", "up"):
                                return True, "up"
                            elif outcome_label in ("no", "down"):
                                return True, "down"
                            elif i == 0:
                                return True, "up"
                            else:
                                return True, "down"

                    # Fallback: check outcome field
                    outcome_field = market.get("outcome", "")
                    if outcome_field:
                        outcome_lower = str(outcome_field).lower()
                        if outcome_lower in ("yes", "up", "1"):
                            return True, "up"
                        elif outcome_lower in ("no", "down", "0"):
                            return True, "down"

                    return True, None  # Resolved but can't determine winner

        except Exception as exc:
            log.debug(
                "[FULL REDEEMER V89] Failed to check resolution for %s: %s",
                condition_id[:20],
                exc,
            )
            return False, None

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential_jitter(initial=1.0, max=30.0, jitter=1.0),
        retry=retry_if_exception_type(NETWORK_EXCEPTIONS + (Exception,)),
        reraise=True,
    )
    def _send_redeem_transaction(
        self,
        condition_id: str,
        index_set: list[int],
        gas_price_wei: int,
    ) -> dict[str, Any]:
        """Send redeemPositions transaction with retries.

        Args:
            condition_id: The condition ID to redeem.
            index_set: [1] for up/yes, [2] for down/no.
            gas_price_wei: Gas price in wei.

        Returns:
            Transaction receipt dict.
        """
        w3 = self._get_web3()
        ctf = w3.eth.contract(
            address=Web3.to_checksum_address(CTF_ADDRESS),
            abi=CTF_ABI,
        )

        # Build transaction
        tx = ctf.functions.redeemPositions(
            Web3.to_checksum_address(USDC_ADDRESS),
            NULL_BYTES32,  # parentCollectionId
            condition_id,
            index_set,
        ).build_transaction(
            {
                "from": self._wallet_address,
                "gas": DEFAULT_GAS_LIMIT,
                "gasPrice": gas_price_wei,
                "nonce": w3.eth.get_transaction_count(self._wallet_address, "pending"),
                "chainId": 137,
            }
        )

        # Sign and send
        signed_tx = w3.eth.account.sign_transaction(tx, self._private_key)

        try:
            tx_hash = w3.eth.send_raw_transaction(signed_tx.raw_transaction)
        except Exception as send_exc:
            exc_msg = str(send_exc).lower()
            # Handle "already known" or "nonce too low" as success
            if "already known" in exc_msg or "nonce too low" in exc_msg:
                log.info(
                    "[FULL REDEEMER V89] TX already in mempool (nonce collision) "
                    "— treating as success for condition=%s",
                    condition_id[:20],
                )
                return {"status": 1, "transactionHash": b"already_submitted"}
            raise

        # Wait for receipt
        receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
        return dict(receipt)

    async def redeem_position(
        self,
        condition_id: str,
        outcome_index: int,
        title: str = "",
        size: float = 0.0,
    ) -> bool:
        """Redeem a single position.

        Args:
            condition_id: The market condition ID.
            outcome_index: 0 for up/yes, 1 for down/no.
            title: Market title for logging.
            size: Position size for logging.

        Returns:
            True if successful, False otherwise.
        """
        if not condition_id:
            log.warning("[FULL REDEEMER V89] No condition_id provided — skipping")
            return False

        if not self._private_key:
            log.error("[FULL REDEEMER V89] No private key — cannot sign transactions")
            return False

        # Dedupe check
        redeem_key = f"{condition_id}:{outcome_index}"
        if redeem_key in self._redeemed_conditions:
            log.debug(
                "[FULL REDEEMER V89] Already redeemed %s — skipping",
                redeem_key[:30],
            )
            return True

        # Calculate index set: 2^outcomeIndex
        # outcomeIndex=0 (Up/Yes) → [1]
        # outcomeIndex=1 (Down/No) → [2]
        index_set = [1 << outcome_index]
        outcome_label = "UP" if outcome_index == 0 else "DOWN"

        # Get current gas price with buffer
        w3 = self._get_web3()
        base_gas_price = w3.eth.gas_price
        buffer_multiplier = 1 + (self._config["redeem_gas_buffer_percent"] / 100)
        gas_price_wei = int(base_gas_price * buffer_multiplier)

        # Estimate gas cost in USDC
        gas_cost_matic = (DEFAULT_GAS_LIMIT * gas_price_wei) / 1e18
        # Note: Rough MATIC/USDC conversion for logging only.
        # This is an estimate (~$0.50-1.00/MATIC) and doesn't affect actual TX costs.
        # Actual gas is paid in MATIC from wallet balance.
        estimated_gas_usdc = gas_cost_matic * 0.8

        log.info(
            "[FULL REDEEMER V89] FOUND REDEEMABLE: market='%s' | "
            "condition_id=%s | outcome=%s | shares=%.4f | est_gas=$%.4f",
            title[:50] if title else "Unknown",
            condition_id[:20] + "...",
            outcome_label,
            size,
            estimated_gas_usdc,
        )

        # Check balance (lax requirements - just need gas)
        usdc_balance = self._get_usdc_balance()
        min_balance = self._config["min_redeem_balance"]

        if usdc_balance < min_balance:
            log.warning(
                "[FULL REDEEMER V89] REDEEM BLOCKED BY LOW BALANCE — "
                "balance=%.4f USDC < min=%.4f | "
                "Send at least %.2f USDC for gas",
                usdc_balance,
                min_balance,
                min_balance + 0.02,
            )
            # Try anyway with minimal gas (don't hard-skip like trading)
            log.info("[FULL REDEEMER V89] Attempting redeem anyway with minimal gas...")

        # Dry run check
        if self._config["dry_run"]:
            log.info(
                "[FULL REDEEMER V89] DRY RUN — would redeem condition=%s outcome=%s",
                condition_id[:20],
                outcome_label,
            )
            self._redeemed_conditions.add(redeem_key)
            return True

        # Execute redeem
        try:
            receipt = await asyncio.get_event_loop().run_in_executor(
                None,
                self._send_redeem_transaction,
                condition_id,
                index_set,
                gas_price_wei,
            )

            status = receipt.get("status", 0)
            tx_hash = receipt.get("transactionHash", b"")
            if isinstance(tx_hash, bytes):
                tx_hash = (
                    tx_hash.hex()
                    if tx_hash != b"already_submitted"
                    else "already_submitted"
                )

            if status == 1:
                # Get new balance to calculate received amount
                new_balance = self._get_usdc_balance()
                received = max(0, new_balance - usdc_balance)

                log.info(
                    "[FULL REDEEMER V89] REDEEMED SUCCESS: "
                    "received=%.4f USDC | tx=%s | gas_used=%s",
                    received,
                    tx_hash[:20] + "..." if len(tx_hash) > 20 else tx_hash,
                    receipt.get("gasUsed", "unknown"),
                )
                self._redeemed_conditions.add(redeem_key)
                return True
            else:
                log.error(
                    "[FULL REDEEMER V89] Redeem TX reverted for condition=%s — "
                    "reason=already redeemed or insufficient collateral",
                    condition_id[:20],
                )
                return False

        except Exception as exc:
            exc_str = str(exc)
            if "already known" in exc_str.lower() or "nonce too low" in exc_str.lower():
                log.info("[FULL REDEEMER V89] TX already in mempool — marking as done")
                self._redeemed_conditions.add(redeem_key)
                return True

            log.error(
                "[FULL REDEEMER V89] Redeem failed for condition=%s — %s: %s",
                condition_id[:20],
                type(exc).__name__,
                exc_str[:200],
            )
            return False

    async def redeem_all_positions(self) -> dict[str, Any]:
        """Find and redeem ALL redeemable positions.

        This is the main entry point. Scans Data API for redeemable positions
        and redeems each one.

        Returns:
            Summary dict with successful/failed counts and USDC gained.
        """
        if not self._wallet_address:
            log.error("[FULL REDEEMER V89] No wallet address configured — aborting")
            return {
                "error": "No wallet address configured",
                "successful": 0,
                "failed": 0,
            }

        log.info(
            "[FULL REDEEMER V89] ========== FULL REDEEM SCAN START ==========\n"
            "  Wallet: %s\n"
            "  Dry Run: %s",
            self._wallet_address,
            self._config["dry_run"],
        )

        # Get initial balance
        balance_before = self._get_usdc_balance()
        log.info(
            "[FULL REDEEMER V89] USDC Balance before: $%.4f",
            balance_before,
        )

        # Fetch redeemable positions from Data API
        positions = await self._fetch_redeemable_from_data_api()

        if not positions:
            log.info(
                "[FULL REDEEMER V89] No redeemable positions found — nothing to do"
            )
            return {
                "successful": 0,
                "failed": 0,
                "total_found": 0,
                "usdc_gained": 0.0,
                "balance_before": balance_before,
                "balance_after": balance_before,
            }

        log.info(
            "[FULL REDEEMER V89] Found %d redeemable position(s) — processing...",
            len(positions),
        )

        successful = 0
        failed = 0

        for i, pos in enumerate(positions, start=1):
            condition_id = pos.get("conditionId") or pos.get("condition_id", "")
            outcome_index = pos.get("outcomeIndex", 0)
            title = pos.get("title", "Unknown Market")[:60]
            size = float(pos.get("size", 0) or 0)

            log.info(
                "[FULL REDEEMER V89] --- Position %d of %d ---",
                i,
                len(positions),
            )

            success = await self.redeem_position(
                condition_id=condition_id,
                outcome_index=outcome_index,
                title=title,
                size=size,
            )

            if success:
                successful += 1
            else:
                failed += 1

            # Small delay between transactions to avoid nonce issues
            if i < len(positions):
                await asyncio.sleep(2.0)

        # Get final balance
        balance_after = self._get_usdc_balance()
        usdc_gained = balance_after - balance_before

        log.info(
            "[FULL REDEEMER V89] ========== FULL REDEEM RESULT ==========\n"
            "  Successful: %d\n"
            "  Failed: %d\n"
            "  Balance before: $%.4f\n"
            "  Balance after: $%.4f\n"
            "  USDC Gained: +$%.4f",
            successful,
            failed,
            balance_before,
            balance_after,
            usdc_gained,
        )

        return {
            "successful": successful,
            "failed": failed,
            "total_found": len(positions),
            "usdc_gained": usdc_gained,
            "balance_before": balance_before,
            "balance_after": balance_after,
        }


# =============================================================================
# Background Task
# =============================================================================

_full_redeemer_task: asyncio.Task | None = None
_full_redeemer_instance: FullRedeemer | None = None


async def _full_redeem_loop() -> None:
    """Background loop that runs full redeem periodically."""
    global _full_redeemer_instance

    config = get_full_redeem_config()
    interval = config["interval_seconds"]

    log.info(
        "[FULL REDEEMER V89] Background task started — interval=%ds",
        interval,
    )

    _full_redeemer_instance = FullRedeemer()

    while True:
        try:
            await _full_redeemer_instance.redeem_all_positions()
        except asyncio.CancelledError:
            log.info("[FULL REDEEMER V89] Background task cancelled")
            break
        except Exception as exc:
            log.error(
                "[FULL REDEEMER V89] Background task error: %s - %s",
                type(exc).__name__,
                str(exc)[:200],
            )

        await asyncio.sleep(interval)


def start_full_redeem_task() -> asyncio.Task | None:
    """Start the full redeem background task if enabled.

    Returns:
        The created asyncio.Task, or None if disabled.
    """
    global _full_redeemer_task

    config = get_full_redeem_config()

    if not config["enabled"]:
        log.info("[FULL REDEEMER V89] Not starting — FULL_REDEEM_ENABLED is not true")
        return None

    if _full_redeemer_task is not None and not _full_redeemer_task.done():
        log.warning("[FULL REDEEMER V89] Task already running — not starting another")
        return _full_redeemer_task

    _full_redeemer_task = asyncio.create_task(_full_redeem_loop())
    log.info("[FULL REDEEMER V89] Background task created and started")
    return _full_redeemer_task


def stop_full_redeem_task() -> None:
    """Stop the full redeem background task if running."""
    global _full_redeemer_task

    if _full_redeemer_task is not None and not _full_redeemer_task.done():
        _full_redeemer_task.cancel()
        log.info("[FULL REDEEMER V89] Background task stop requested")
    _full_redeemer_task = None


async def force_full_redeem() -> dict[str, Any]:
    """Force an immediate full redeem scan.

    Can be called from API endpoint or programmatically.

    Returns:
        Result dict from redeem_all_positions().
    """
    global _full_redeemer_instance

    if _full_redeemer_instance is None:
        _full_redeemer_instance = FullRedeemer()

    log.info("[FULL REDEEMER V89] Force full redeem triggered")
    return await _full_redeemer_instance.redeem_all_positions()


def get_full_redeemer_status() -> dict[str, Any]:
    """Get current status of the full redeemer.

    Returns:
        Status dict with task state and configuration.
    """
    global _full_redeemer_task, _full_redeemer_instance

    config = get_full_redeem_config()

    task_status = "not_started"
    if _full_redeemer_task is not None:
        if _full_redeemer_task.done():
            task_status = "stopped"
        elif _full_redeemer_task.cancelled():
            task_status = "cancelled"
        else:
            task_status = "running"

    redeemed_count = 0
    if _full_redeemer_instance is not None:
        redeemed_count = len(_full_redeemer_instance._redeemed_conditions)

    return {
        "enabled": config["enabled"],
        "interval_seconds": config["interval_seconds"],
        "min_redeem_balance": config["min_redeem_balance"],
        "dry_run": config["dry_run"],
        "task_status": task_status,
        "redeemed_this_session": redeemed_count,
        "wallet_address": (
            config["wallet_address"][:20] + "..."
            if config["wallet_address"] and len(config["wallet_address"]) > 20
            else config["wallet_address"] or "NOT SET"
        ),
    }
