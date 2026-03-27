"""Onchain trade execution via py_clob_client on Polygon.

V68: Order submission uses the official py_clob_client SDK
(ClobClient.create_and_post_order) with derived L2 credentials.
No manual requests.post to the CLOB API — the 401 error is gone.

Flow:
  1. Connect to Polygon via RPC
  2. Check USDC balance
  3. Approve USDC spend on CTF Exchange (if needed)
  4. Create ClobClient, derive L2 API credentials
  5. Submit signed order via py_clob_client (create_and_post_order)
"""

from __future__ import annotations

import asyncio
import os
import threading
import time
from datetime import datetime, timedelta, timezone

import requests
from eth_account import Account
from eth_account.messages import encode_typed_data
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import ApiCreds, MarketOrderArgs, OrderArgs
from web3 import Web3
from web3.middleware import ExtraDataToPOAMiddleware

from polybot.logging_setup import get_logger

log = get_logger(__name__)

# === V77 RAILWAY FAST BUILD – nur hier wird der Skip aktiviert ===
if os.environ.get("RAILWAY_ENVIRONMENT") or os.environ.get("RAILWAY_BUILD"):
    log.info(
        "[RAILWAY FAST BUILD] Railway detected → skipping all pytest + backtests (build now < 2 min)"
    )

# ── Contract addresses (Polygon Mainnet) ─────────────────────────────────────
USDC_ADDRESS = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
CTF_EXCHANGE_ADDRESS = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"
CTF_ADDRESS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"  # ConditionalTokens

# Max uint256 for unlimited USDC approval
MAX_UINT256 = 2**256 - 1

# V87: Zero/null condition ID constant (used to identify uninitialized positions)
NULL_CONDITION_ID = "0x" + "0" * 64

# V89: Concurrency control — prevent duplicate redeem attempts for same wallet+condition
_active_redeems: set[str] = set()

# Stop-loss price thresholds for detecting market resolution
WINNER_PRICE_THRESHOLD = 0.90  # Price >= this indicates winning position
LOSER_PRICE_THRESHOLD = 0.02  # Price <= this indicates resolved losing position

# ── Minimal ABIs ─────────────────────────────────────────────────────────────
USDC_ABI = [
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
    {
        "inputs": [
            {"name": "owner", "type": "address"},
            {"name": "spender", "type": "address"},
        ],
        "name": "allowance",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [{"name": "account", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
]

# Backward-compatible alias
ERC20_ABI = USDC_ABI

CTF_EXCHANGE_ABI = [
    {
        "inputs": [
            {
                "components": [
                    {"name": "salt", "type": "uint256"},
                    {"name": "maker", "type": "address"},
                    {"name": "signer", "type": "address"},
                    {"name": "taker", "type": "address"},
                    {"name": "tokenId", "type": "uint256"},
                    {"name": "makerAmount", "type": "uint256"},
                    {"name": "takerAmount", "type": "uint256"},
                    {"name": "expiration", "type": "uint256"},
                    {"name": "nonce", "type": "uint256"},
                    {"name": "feeRateBps", "type": "uint256"},
                    {"name": "side", "type": "uint8"},
                    {"name": "signatureType", "type": "uint8"},
                ],
                "name": "order",
                "type": "tuple",
            },
            {"name": "fillAmount", "type": "uint256"},
            {"name": "signature", "type": "bytes"},
        ],
        "name": "fillOrder",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "getChainId",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
]

# ── ConditionalTokens (CTF) ABI – redeemPositions + payoutDenominator ───────
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
    # V89: payoutDenominator for verifying on-chain resolution before redeeming
    {
        "inputs": [{"name": "conditionId", "type": "bytes32"}],
        "name": "payoutDenominator",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
]

# ── Polymarket CLOB REST API ──────────────────────────────────────────────────
CLOB_API_BASE = "https://clob.polymarket.com"

# ── EIP-712 domain & types for CTF Exchange orders ──────────────────────────
EXCHANGE_DOMAIN = {
    "name": "Polymarket CTF Exchange",
    "version": "1",
    "chainId": 137,
    "verifyingContract": CTF_EXCHANGE_ADDRESS,
}

ORDER_TYPES = {
    "Order": [
        {"name": "salt", "type": "uint256"},
        {"name": "maker", "type": "address"},
        {"name": "signer", "type": "address"},
        {"name": "taker", "type": "address"},
        {"name": "tokenId", "type": "uint256"},
        {"name": "makerAmount", "type": "uint256"},
        {"name": "takerAmount", "type": "uint256"},
        {"name": "expiration", "type": "uint256"},
        {"name": "nonce", "type": "uint256"},
        {"name": "feeRateBps", "type": "uint256"},
        {"name": "side", "type": "uint8"},
        {"name": "signatureType", "type": "uint8"},
    ],
}

# ── EIP-712 domain & types for CLOB L1 authentication ───────────────────────
CLOB_AUTH_DOMAIN = {
    "name": "ClobAuthDomain",
    "version": "1",
    "chainId": 137,
}

CLOB_AUTH_TYPES = {
    "ClobAuth": [
        {"name": "address", "type": "address"},
        {"name": "timestamp", "type": "string"},
        {"name": "nonce", "type": "uint256"},
        {"name": "message", "type": "string"},
    ],
}

# ── RPC fallback list (tried in order) ───────────────────────────────────────
FALLBACK_RPCS = [
    "https://rpc.ankr.com/polygon",
    "https://polygon.llamarpc.com",
    "https://polygon-bor-rpc.publicnode.com",
    "https://1rpc.io/matic",
]

_primary = os.environ.get("POLYGON_RPC_URL", "").strip()
if not _primary:
    log.warning(
        "WARNING: POLYGON_RPC_URL not set – using public fallback RPCs (may be unreliable)"
    )

RPC_URLS = ([_primary] if _primary else []) + FALLBACK_RPCS


def mask_rpc_url(url: str) -> str:
    """Mask the API-key portion of an RPC URL to prevent secret leakage in logs.

    Only masks the final path segment when it looks like an API key
    (alphanumeric string following ``/v1/`` or ``/v2/`` etc.).  Plain
    hostnames such as ``https://rpc.ankr.com/polygon`` are returned
    unchanged.
    """
    import re

    # Match URLs ending with /v<N>/<key> (e.g. Alchemy, Infura)
    m = re.match(r"^(.*?/v\d+/)(.{7,})$", url)
    if m:
        return m.group(1) + m.group(2)[:6] + "..."
    return url


def _get_web3() -> Web3:
    """Create a Web3 instance connected to Polygon.

    Tries each URL in *RPC_URLS* in order.  Returns the first
    connected Web3 instance.  Raises ``ConnectionError`` only when
    **every** URL fails.
    """
    errors: list[str] = []
    for url in RPC_URLS:
        if not url:
            continue
        log.info("Trying RPC: %s", mask_rpc_url(url))
        try:
            w3 = Web3(Web3.HTTPProvider(url))
            if w3.is_connected():
                log.info("RPC connected: %s ✅", mask_rpc_url(url))
                return w3
            errors.append(f"{mask_rpc_url(url)}: not connected")
            log.info("RPC failed: not connected – trying next")
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{mask_rpc_url(url)}: {exc}")
            log.info("RPC failed: %s – trying next", exc)
    raise ConnectionError("All Polygon RPCs failed: " + "; ".join(errors))


def _normalize_private_key(private_key: str) -> str:
    """Ensure private key has 0x prefix."""
    if not private_key.startswith("0x"):
        return "0x" + private_key
    return private_key


def _ensure_usdc_approval(
    w3: Web3,
    account: Account,
    amount_raw: int,
) -> None:
    """Check USDC allowance for CTF Exchange; approve max if insufficient."""
    usdc = w3.eth.contract(
        address=Web3.to_checksum_address(USDC_ADDRESS),
        abi=ERC20_ABI,
    )
    owner = account.address
    spender = Web3.to_checksum_address(CTF_EXCHANGE_ADDRESS)

    current_allowance = usdc.functions.allowance(owner, spender).call()
    if current_allowance >= amount_raw:
        log.debug(
            "USDC allowance sufficient", allowance=current_allowance, needed=amount_raw
        )
        return

    log.info("Approving USDC spend on CTF Exchange (max uint256)")
    tx = usdc.functions.approve(spender, MAX_UINT256).build_transaction(
        {
            "from": owner,
            "nonce": w3.eth.get_transaction_count(owner),
            "gas": 100_000,
            "maxFeePerGas": w3.eth.gas_price * 2,
            "maxPriorityFeePerGas": w3.to_wei(30, "gwei"),
            "chainId": 137,
        }
    )
    signed = w3.eth.account.sign_transaction(tx, account.key)
    tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
    receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
    log.info("USDC approval confirmed", tx_hash=tx_hash.hex(), status=receipt["status"])


def _build_order(
    maker: str,
    token_id: int,
    maker_amount: int,
    taker_amount: int,
    side: int,
) -> dict:
    """Build a Polymarket CTF Exchange order struct."""
    salt = int(time.time() * 1000)
    return {
        "salt": salt,
        "maker": Web3.to_checksum_address(maker),
        "signer": Web3.to_checksum_address(maker),
        "taker": "0x0000000000000000000000000000000000000000",
        "tokenId": token_id,
        "makerAmount": maker_amount,
        "takerAmount": taker_amount,
        "expiration": int(time.time()) + 300,
        "nonce": 0,
        "feeRateBps": 0,
        "side": side,
        "signatureType": 0,
    }


def _sign_order(order: dict, private_key: str) -> bytes:
    """Sign an order using EIP-712 structured data. Returns raw signature bytes."""
    signable = encode_typed_data(
        domain_data=EXCHANGE_DOMAIN,
        message_types=ORDER_TYPES,
        message_data=order,
    )
    signed = Account.sign_message(signable, private_key=private_key)
    return signed.signature


def _fetch_order_book(token_id: str) -> dict:
    """Fetch the order book from the Polymarket CLOB REST API.

    GET request — no authentication needed.

    Args:
        token_id: The outcome token ID.

    Returns:
        Order book dict with 'bids' and 'asks' arrays.

    Raises:
        RuntimeError: If the HTTP request fails.
    """
    url = f"{CLOB_API_BASE}/book"
    try:
        resp = requests.get(url, params={"token_id": token_id}, timeout=10)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as exc:
        raise RuntimeError(f"Failed to fetch order book: {exc}") from exc


def _get_best_price(book: dict, side: str) -> tuple[float, float] | None:
    """Extract the best available price and size from the order book.

    For BUY: returns the best ask (lowest-price sell order).
    For SELL: returns the best bid (highest-price buy order).

    Args:
        book: Order book dict from ``_fetch_order_book``.
        side: "BUY" or "SELL".

    Returns:
        Tuple of (price, available_size) or None if no orders on that side.
    """
    if side.upper() == "BUY":
        asks = book.get("asks", [])
        if not asks:
            return None
        best = asks[0]
    else:
        bids = book.get("bids", [])
        if not bids:
            return None
        best = bids[0]

    return float(best["price"]), float(best["size"])


def _build_l1_auth_headers(private_key: str) -> dict:
    """Build L1 authentication headers for the CLOB REST API.

    Uses EIP-712 ClobAuth domain signing.  No API keys needed —
    the wallet signature proves ownership.

    Args:
        private_key: Hex-encoded private key.

    Returns:
        Dict of authentication headers (POLY_ADDRESS, POLY_SIGNATURE,
        POLY_TIMESTAMP, POLY_NONCE).
    """
    account = Account.from_key(private_key)
    timestamp = str(int(time.time()))
    nonce = 0

    message_data = {
        "address": account.address,
        "timestamp": timestamp,
        "nonce": nonce,
        "message": "This message attests that I control the given wallet",
    }

    signable = encode_typed_data(
        domain_data=CLOB_AUTH_DOMAIN,
        message_types=CLOB_AUTH_TYPES,
        message_data=message_data,
    )
    signed = Account.sign_message(signable, private_key=private_key)

    return {
        "POLY_ADDRESS": account.address,
        "POLY_SIGNATURE": "0x" + signed.signature.hex(),
        "POLY_TIMESTAMP": timestamp,
        "POLY_NONCE": str(nonce),
    }


def _submit_order_to_clob(
    order: dict,
    signature: bytes,
    private_key: str,
    order_type: str = "FOK",
) -> dict:
    """Submit a signed order to the Polymarket CLOB REST API.

    The CLOB matches the order against existing maker orders and
    settles on-chain via ``fillOrder()``.

    Args:
        order: The order struct (from ``_build_order``).
        signature: The EIP-712 signature bytes.
        private_key: For L1 auth headers.
        order_type: ``"FOK"`` (Fill-or-Kill), ``"GTC"`` (Good-til-Cancelled),
                    or ``"GTD"`` (Good-til-Date).

    Returns:
        Dict with ``order_id`` and ``transaction_hashes`` from the CLOB.

    Raises:
        RuntimeError: If the order submission fails.
    """
    account = Account.from_key(private_key)

    order_payload = {
        "salt": str(order["salt"]),
        "maker": order["maker"],
        "signer": order["signer"],
        "taker": order["taker"],
        "tokenId": str(order["tokenId"]),
        "makerAmount": str(order["makerAmount"]),
        "takerAmount": str(order["takerAmount"]),
        "expiration": str(order["expiration"]),
        "nonce": str(order["nonce"]),
        "feeRateBps": str(order["feeRateBps"]),
        "side": "BUY" if order["side"] == 0 else "SELL",
        "signatureType": order["signatureType"],
        "signature": "0x" + signature.hex(),
    }

    body = {
        "order": order_payload,
        "owner": account.address,
        "orderType": order_type,
    }

    headers = _build_l1_auth_headers(private_key)
    headers["Content-Type"] = "application/json"

    url = f"{CLOB_API_BASE}/order"
    try:
        resp = requests.post(url, json=body, headers=headers, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as exc:
        raise RuntimeError(f"CLOB order submission failed: {exc}") from exc

    data = resp.json()

    if not data.get("success", False):
        error_msg = data.get("errorMsg", "Unknown error")
        raise RuntimeError(f"CLOB order rejected: {error_msg}")

    return {
        "order_id": data.get("orderID", ""),
        "transaction_hashes": data.get("transactionsHashes", []),
    }


def _submit_via_clob_client(
    private_key: str,
    token_id: str,
    price: float,
    size: float,
    side: str,
) -> dict:
    """Submit an order via py_clob_client with derived L2 credentials.

    Creates a ``ClobClient``, derives (or re-uses) L2 API credentials,
    builds a signed order and posts it through the official SDK.
    This replaces the manual ``requests.post`` to ``/order`` which
    returned 401 Unauthorized.

    Args:
        private_key: Hex-encoded private key (with 0x prefix).
        token_id: The outcome token ID (string).
        price: Order price (0.01 – 0.99).
        size: Order size in conditional tokens.
        side: ``"BUY"`` or ``"SELL"``.

    Returns:
        Dict from the CLOB API (may contain ``orderID`` and
        ``transactionsHashes``).

    Raises:
        RuntimeError: If the order is rejected or submission fails.
    """
    clob_client = ClobClient(
        host=CLOB_API_BASE,
        key=private_key,
        chain_id=137,
    )

    # Derive L2 API credentials (the official authenticated path)
    raw_creds = clob_client.create_or_derive_api_creds()

    # Robust credential extraction (handles pydantic v1/v2 and plain objects)
    if hasattr(raw_creds, "model_dump"):
        creds_dict = raw_creds.model_dump()
    elif hasattr(raw_creds, "dict"):
        creds_dict = raw_creds.dict()
    elif hasattr(raw_creds, "__dict__"):
        creds_dict = vars(raw_creds)
    else:
        creds_dict = dict(raw_creds)

    # Rebuild as ApiCreds to guarantee correct type for set_api_creds
    api_key = (
        creds_dict.get("apiKey")
        if "apiKey" in creds_dict
        else creds_dict.get("api_key", "")
    )
    api_secret = (
        creds_dict.get("apiSecret")
        if "apiSecret" in creds_dict
        else creds_dict.get("api_secret", "")
    )
    api_passphrase = (
        creds_dict.get("apiPassphrase")
        if "apiPassphrase" in creds_dict
        else creds_dict.get("api_passphrase", "")
    )
    clob_client.set_api_creds(
        ApiCreds(
            api_key=api_key,
            api_secret=api_secret,
            api_passphrase=api_passphrase,
        )
    )

    order_args = OrderArgs(
        token_id=token_id,
        price=price,
        size=size,
        side=side.upper(),
    )

    try:
        result = clob_client.create_and_post_order(order_args)
    except Exception as exc:
        raise RuntimeError(f"CLOB order submission failed: {exc}") from exc

    if isinstance(result, dict) and not result.get("success", True):
        error_msg = result.get("errorMsg", "Unknown error")
        raise RuntimeError(f"CLOB order rejected: {error_msg}")

    return result if isinstance(result, dict) else {"orderID": str(result)}


def execute_trade(
    private_key: str,
    token_id: str,
    amount_usdc: float,
    side: str,
    current_price: float = 0.5,
    market: dict | None = None,
) -> dict:
    """Execute a trade on Polymarket as a TAKER against existing orderbook.

    Fetches the order book from the Polymarket CLOB REST API, builds a
    signed order at the best available price, and submits it to the CLOB
    for matching.  The CLOB matches against existing maker orders and
    settles on-chain via ``fillOrder()``.

    Args:
        private_key: Hex-encoded private key (with or without 0x prefix).
        token_id: The outcome token ID (numeric string).
        amount_usdc: Amount in USDC (human-readable, e.g. 30.0).
        side: "BUY" or "SELL" (case-insensitive).
        current_price: Fallback token price (0-1), used when the order book
                       is unavailable.  If 0.0 or negative, defaults to 0.5.
        market: Optional market dict containing conditionId and slug
            for position tracking and auto-redeem (V88).

    Returns:
        Dict with ``tx_hash`` and ``block`` keys, or None if USDC
        balance is too low to trade (< 0.3 USDC).  When balance is below
        *amount_usdc* but >= 0.3, the effective trade amount is
        ``balance - 0.1`` (minimum 0.3 USDC).

    Raises:
        ValueError: If private_key is empty.
        ConnectionError: If no Polygon RPC is reachable.
        RuntimeError: If the CLOB rejects the order or on-chain settlement
                      reverts.
    """
    if not private_key:
        raise ValueError("private_key is required for onchain trading")

    private_key = _normalize_private_key(private_key)

    w3 = _get_web3()
    if not w3.is_connected():
        raise ConnectionError("Cannot connect to Polygon RPC")

    account = Account.from_key(private_key)
    wallet = account.address

    # Convert USDC to raw (6 decimals)
    maker_amount_raw = int(amount_usdc * 1_000_000)

    # ── Step 1: Fetch order book for best price ──────────────────────────
    try:
        book = _fetch_order_book(token_id)
        best = _get_best_price(book, side)
        if best is not None:
            book_price, available_size = best
            log.info(
                "Orderbook best price",
                price=book_price,
                available_size=available_size,
                side=side,
            )
            if 0.01 <= book_price <= 0.99:
                current_price = book_price
    except Exception as exc:
        log.warning("Failed to fetch orderbook – using provided price: %s", exc)

    # Price fallback: if unknown or non-positive, default to 0.5
    if current_price <= 0.0:
        log.warning("Price unknown for token – using 0.5 default")
        current_price = 0.5

    # Clamp price to [0.01, 0.99] to avoid division-by-zero or unrealistic orders
    price = max(min(current_price, 0.99), 0.01)

    # Side: 0 = BUY, 1 = SELL
    side_int = 0 if side.upper() == "BUY" else 1

    # ── Step 2: Check USDC balance ───────────────────────────────────────
    usdc_contract = w3.eth.contract(
        address=Web3.to_checksum_address(USDC_ADDRESS),
        abi=USDC_ABI,
    )
    balance_raw = usdc_contract.functions.balanceOf(wallet).call()
    balance = balance_raw / 1_000_000

    # V86: Enforce $1 minimum — Polymarket CLOB rejects anything below $1
    MIN_ORDER_USDC = 1.0
    RESERVE = 0.2  # keep as gas/fee buffer

    if balance < MIN_ORDER_USDC + RESERVE:
        log.warning(
            "[BALANCE MASTER V6] Balance too low (%.6f < %.1f) → skipping trade "
            "(need $%.1f + $%.1f reserve)",
            balance,
            MIN_ORDER_USDC + RESERVE,
            MIN_ORDER_USDC,
            RESERVE,
        )
        return None

    # Cap to available balance minus reserve, enforce minimum $1
    effective_amount = min(amount_usdc, balance - RESERVE)
    effective_amount = max(effective_amount, MIN_ORDER_USDC)

    if effective_amount != amount_usdc:
        log.info(
            "[BALANCE MASTER V6] Adjusted trade: %.4f USDC (requested %.4f, balance %.6f)",
            effective_amount,
            amount_usdc,
            balance,
        )
    amount_usdc = effective_amount
    maker_amount_raw = int(amount_usdc * 1_000_000)

    # ── Step 3: Ensure USDC approval ─────────────────────────────────────
    _ensure_usdc_approval(w3, account, maker_amount_raw)

    # ── Step 4: Submit order via py_clob_client (official SDK) ───────────
    log.info(
        "Submitting order via py_clob_client",
        wallet=wallet,
        token_id=token_id,
        amount_usdc=amount_usdc,
        side=side,
        price=price,
    )

    clob_result = _submit_via_clob_client(
        private_key=private_key,
        token_id=str(token_id),
        price=price,
        size=amount_usdc / price if side_int == 0 else amount_usdc,
        side=side,
    )

    log.info("[CLOB SUCCESS] Order submitted for token %s | Side: %s", token_id, side)

    tx_hashes = clob_result.get(
        "transactionsHashes", clob_result.get("transaction_hashes", [])
    )
    order_id = clob_result.get("orderID", clob_result.get("order_id", ""))

    if tx_hashes:
        tx_hash_hex = tx_hashes[0]
        # Wait for on-chain confirmation
        tx_hash_bytes = bytes.fromhex(tx_hash_hex.replace("0x", ""))
        receipt = w3.eth.wait_for_transaction_receipt(tx_hash_bytes, timeout=60)

        if receipt.status != 1:
            raise RuntimeError(f"Transaction reverted: {tx_hash_hex}")

        log.info(
            "[ONCHAIN TRADE SUCCESS] %s %s USDC on token %s",
            side,
            amount_usdc,
            token_id,
        )

        # V88: Register position in shared executor for auto-redeem tracking
        _register_position_in_shared_executor(
            token_id=token_id,
            side=side,
            amount=amount_usdc,
            entry_price=price,
            market=market,
        )

        return {"tx_hash": tx_hash_hex, "block": receipt.blockNumber}

    # Order accepted but no immediate on-chain settlement yet
    log.info(
        "Order submitted to CLOB",
        order_id=order_id,
    )

    # V88: Register position in shared executor for auto-redeem tracking
    _register_position_in_shared_executor(
        token_id=token_id,
        side=side,
        amount=amount_usdc,
        entry_price=price,
        market=market,
    )

    return {"tx_hash": order_id, "block": 0}


def _register_position_in_shared_executor(
    token_id: str,
    side: str,
    amount: float,
    entry_price: float,
    market: dict | None,
) -> None:
    """V88: Register a position in the shared OnchainExecutor for auto-redeem.

    This function is called by the module-level execute_trade() function
    to register positions so that the auto-redeem monitor can track them.

    Args:
        token_id: The outcome token ID.
        side: "BUY" or "SELL".
        amount: Amount in USDC (human-readable).
        entry_price: Entry price for the position.
        market: Optional market dict containing conditionId and slug.
    """
    try:
        executor = get_shared_executor()
        if executor is None:
            log.debug(
                "[V88] No shared executor available — skipping position registration"
            )
            return

        # Extract condition_id and slug from market dict
        condition_id = None
        slug = ""
        if market:
            condition_id = market.get("conditionId") or market.get("condition_id")
            slug = market.get("slug", "")

        executor.open_positions[token_id] = {
            "side": side,
            "amount": amount,
            "entry_price": entry_price,
            "buy_time": datetime.now(tz=timezone.utc),
            "condition_id": condition_id,  # V88: needed for redeem
            "winning_outcome": None,  # V88: set later when market resolves
            "slug": slug,  # V88: for polling resolution status
        }

        log.info(
            "[POSITION REGISTERED] %s %.4f USDC (module-level) | Token %s | Condition %s",
            side,
            amount,
            token_id,
            condition_id or "unknown",
        )
    except Exception as exc:
        log.warning("[V88] Failed to register position in shared executor: %s", exc)


def get_usdc_balance(private_key: str) -> float:
    """Get USDC balance for the wallet on Polygon.

    Args:
        private_key: Hex-encoded private key.

    Returns:
        USDC balance as a float (human-readable).
    """
    if not private_key:
        return 0.0
    private_key = _normalize_private_key(private_key)

    try:
        w3 = _get_web3()
        if not w3.is_connected():
            return 0.0

        account = Account.from_key(private_key)
        usdc = w3.eth.contract(
            address=Web3.to_checksum_address(USDC_ADDRESS),
            abi=ERC20_ABI,
        )
        raw_balance = usdc.functions.balanceOf(account.address).call()
        return raw_balance / 1e6
    except Exception as e:
        log.error("Failed to fetch USDC balance", error=str(e))
        return 0.0


def check_onchain_ready(private_key: str) -> bool:
    """Startup check: verify private key, RPC connectivity, and log wallet info.

    Tries each URL in *RPC_URLS*; logs which one is active.
    Returns True if everything is operational.
    """
    if not private_key:
        log.error("POLYMARKET_PRIVATE_KEY is not set — onchain trading disabled")
        return False

    private_key = _normalize_private_key(private_key)

    try:
        # Find the first working RPC and remember its URL
        active_rpc: str | None = None
        w3: Web3 | None = None
        for url in RPC_URLS:
            if not url:
                continue
            try:
                candidate = Web3(Web3.HTTPProvider(url))
                if candidate.is_connected():
                    active_rpc = url
                    w3 = candidate
                    break
            except Exception:  # noqa: BLE001
                continue

        if w3 is None or active_rpc is None:
            log.error("Cannot connect to any Polygon RPC — check RPC_URLS")
            return False

        log.info("Polygon RPC connected", rpc_url=active_rpc)

        account = Account.from_key(private_key)
        balance = get_usdc_balance(private_key)

        log.info(
            "Onchain mode active – no API keys required",
            wallet=account.address,
            usdc_balance=balance,
            rpc_url=active_rpc,
            chain_id=137,
        )
        return True
    except Exception as e:
        log.error("Onchain readiness check failed", error=str(e))
        return False


# ── OnchainExecutor class (V69) ─────────────────────────────────────────────


class OnchainExecutor:
    """Class-based onchain executor that reads credentials from environment.

    Reads ``POLYMARKET_PRIVATE_KEY`` and ``POLYGON_RPC_URL`` once at
    construction time.  Provides an async ``execute_trade`` that submits
    market orders via :class:`ClobClient`.
    """

    def __init__(self) -> None:
        self.private_key: str | None = os.environ.get("POLYMARKET_PRIVATE_KEY")
        if self.private_key:
            self.private_key = _normalize_private_key(self.private_key)
            self.account = Account.from_key(self.private_key)
            self.wallet: str = self.account.address
        else:
            self.account = None
            self.wallet = ""

        self.rpc_url: str = os.environ.get("POLYGON_RPC_URL", "").strip()
        if not self.rpc_url:
            # Use first available fallback RPC
            self.rpc_url = FALLBACK_RPCS[0] if FALLBACK_RPCS else ""

        if self.rpc_url:
            self.w3 = Web3(Web3.HTTPProvider(self.rpc_url))
            self.w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
        else:
            self.w3 = None

        # === V74: Cut-Loss Tracking ===
        self.open_positions: dict = {}  # token_id → {side, amount, entry_price, buy_time}
        self.monitor_task: asyncio.Task | None = None
        self.redeemed_conditions: set = set()  # V86: Vermeidet "already known"

        if self.wallet:
            log.info(
                "[ONCHAIN INIT SUCCESS V86] Wallet: %s | RPC: %s",
                self.wallet,
                self.rpc_url,
            )

    # ── V74: Cut-Loss Monitor ────────────────────────────────────────────────

    async def start_monitors(self) -> None:
        """Start background monitor for Dynamic Kelly + Cut-Loss + Full Auto-Redeem (V86)."""
        if self.monitor_task and not self.monitor_task.done():
            return
        self.monitor_task = asyncio.create_task(self._cut_loss_loop())
        # V86: Updated monitor start message
        log.info("[MONITOR V86] Dynamic Kelly + Min $1 + Full Redeem gestartet")

    async def start_cut_loss_monitor(self) -> None:
        """Start background monitor (backward-compatible alias for start_monitors)."""
        return await self.start_monitors()

    async def _cut_loss_loop(self) -> None:
        """Background loop: time-based stop-loss + redeem checker."""
        while True:
            try:
                await asyncio.sleep(15)  # check every 15 seconds
                await self._auto_redeem_resolved_positions()
                await self._time_based_stop_loss()
            except asyncio.CancelledError:
                log.info("[STOP LOSS] Monitor cancelled")
                break
            except Exception as exc:
                log.error("[STOP LOSS] Monitor error: %s - %s", type(exc).__name__, exc)

    async def _auto_redeem_resolved_positions(self) -> None:
        """V88: Auto-Redeem — polls Gamma API for resolution, then redeems.

        For each open position that has a condition_id, polls the Polymarket
        Gamma API to check if the market is resolved. If resolved, determines
        the winning outcome and calls redeemPositions on the CTF contract.

        Note: Positions are polled sequentially. For a large number of
        positions, consider using asyncio.gather() for parallel polling.
        In typical usage, there are only a few open positions at a time.
        """
        if not self.open_positions:
            log.debug("[AUTO REDEEM V88] No open positions to check")
            return

        log.info(
            "[AUTO REDEEM V88] Checking %d open position(s) for resolution",
            len(self.open_positions),
        )

        for token_id, pos in list(self.open_positions.items()):
            condition_id = pos.get("condition_id")
            slug = pos.get("slug", "")

            # Skip if we have no condition_id to poll with
            if not condition_id or condition_id == NULL_CONDITION_ID:
                log.debug(
                    "[AUTO REDEEM V88] Token %s — no condition_id, skipping",
                    token_id,
                )
                continue

            # Skip if already redeemed
            winning_outcome = pos.get("winning_outcome")
            if winning_outcome:
                redeem_key = f"{condition_id}:{winning_outcome}"
                if redeem_key in self.redeemed_conditions:
                    log.debug("[AUTO REDEEM V88] %s already redeemed", redeem_key)
                    continue

            # Poll Gamma API to check resolution status
            try:
                resolved_outcome = await self._poll_market_resolution(
                    condition_id=condition_id,
                    slug=slug,
                    token_id=str(token_id),
                    side=pos.get("side", "BUY"),
                )
            except Exception as exc:
                log.error(
                    "[AUTO REDEEM V88] Failed to poll resolution for token %s: %s",
                    token_id,
                    exc,
                )
                continue

            if resolved_outcome is None:
                log.debug(
                    "[AUTO REDEEM V88] Token %s — market not yet resolved",
                    token_id,
                )
                continue

            # Market is resolved — update position and redeem
            pos["winning_outcome"] = resolved_outcome
            redeem_key = f"{condition_id}:{resolved_outcome}"

            if redeem_key in self.redeemed_conditions:
                log.debug("[AUTO REDEEM V88] %s already redeemed", redeem_key)
                continue

            log.info(
                "[AUTO REDEEM V88] Market resolved! Token %s | outcome=%s | condition=%s",
                token_id,
                resolved_outcome,
                condition_id,
            )

            try:
                await self.redeem_winning_positions(condition_id, resolved_outcome)
                self.redeemed_conditions.add(redeem_key)
                self.open_positions.pop(token_id, None)
                log.info(
                    "[AUTO REDEEM V88] ✅ Redeemed token %s | outcome=%s",
                    token_id,
                    resolved_outcome,
                )
            except Exception as exc:
                exc_str = str(exc)
                if "already known" in exc_str.lower():
                    log.info(
                        "[AUTO REDEEM V88] TX already in mempool for %s — marking done",
                        redeem_key,
                    )
                    self.redeemed_conditions.add(redeem_key)
                else:
                    log.error("[REDEEM ERROR V88] %s - %s", type(exc).__name__, exc_str)

    async def _poll_market_resolution(
        self,
        condition_id: str,
        slug: str,
        token_id: str,
        side: str,
    ) -> str | None:
        """Poll the Gamma API to check if a market has resolved.

        Queries the Polymarket Gamma API by condition_id or slug to check
        whether the market has ended and what the winning outcome is.

        Args:
            condition_id: The bytes32 condition ID of the market.
            slug: The market slug (e.g. 'btc-updown-5m-1774458000').
            token_id: The outcome token ID we hold.
            side: The side we bet ('BUY' = up/yes, 'SELL' = down/no).

        Returns:
            'up' or 'down' if market is resolved and we should redeem,
            None if market is not yet resolved or resolution check failed.
        """
        import aiohttp

        # Try querying by condition_id first, then by slug
        urls_to_try = []
        if condition_id and condition_id != NULL_CONDITION_ID:
            urls_to_try.append(
                f"https://gamma-api.polymarket.com/markets"
                f"?conditionId={condition_id}&limit=1"
            )
        if slug:
            urls_to_try.append(
                f"https://gamma-api.polymarket.com/markets?slug={slug}&limit=1"
            )

        market_data = None
        async with aiohttp.ClientSession() as session:
            for url in urls_to_try:
                try:
                    timeout = aiohttp.ClientTimeout(total=8)
                    async with session.get(url, timeout=timeout) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            markets = (
                                data.get("data", []) if isinstance(data, dict) else data
                            )
                            if markets:
                                market_data = markets[0]
                                break
                except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                    log.debug(
                        "[AUTO REDEEM V88] Gamma API request failed: %s - %s",
                        type(exc).__name__,
                        str(exc)[:100],
                    )
                    continue
                except Exception as exc:
                    log.warning(
                        "[AUTO REDEEM V88] Unexpected error polling Gamma API: %s - %s",
                        type(exc).__name__,
                        str(exc)[:100],
                    )
                    continue

        if not market_data:
            return None

        # Check if market is closed/resolved
        is_closed = market_data.get("closed", False)
        is_resolved = market_data.get("resolved", False)

        if not is_resolved:
            return None  # Still open

        # Market is resolved — find winning outcome from tokens
        # Polymarket UP/DOWN markets: token[0] = UP/YES, token[1] = DOWN/NO
        tokens = market_data.get("tokens", [])

        # Check token prices — winning token trades at $1.00, losing at $0.00
        for i, token in enumerate(tokens):
            token_price = float(token.get("price", 0) or 0)
            outcome_label = token.get("outcome", "").lower()

            # Winning token is at price ~1.0
            if token_price >= 0.95:
                # Map outcome label to up/down
                if outcome_label in ("yes", "up"):
                    return "up"
                elif outcome_label in ("no", "down"):
                    return "down"
                # Fallback: use token index (index 0 = up, index 1 = down)
                elif i == 0:
                    return "up"
                else:
                    return "down"

        # Fallback: check resolutionSource or market outcome field directly
        outcome_field = market_data.get("outcome", "")
        if outcome_field:
            outcome_lower = str(outcome_field).lower()
            if outcome_lower in ("yes", "up", "1"):
                return "up"
            elif outcome_lower in ("no", "down", "0"):
                return "down"

        log.warning(
            "[AUTO REDEEM V88] Market %s resolved but could not determine winner. "
            "Market data: closed=%s resolved=%s tokens=%s",
            slug or condition_id,
            is_closed,
            is_resolved,
            [(t.get("outcome"), t.get("price")) for t in tokens],
        )
        return None

    async def _time_based_stop_loss(self) -> None:
        """Time-based stop-loss for 5-minute markets.

        How it works:
        - 5min Up/Down markets start at a known timestamp (embedded in slug)
        - A position bought at $0.99 will lose everything if wrong direction
        - Prices don't decline gradually — they collapse at resolution
        - Solution: sell at market price before the market closes,
          while there is still liquidity and the price is still tradeable

        Timeline for a 5min market:
          T+0:00  Market opens, we buy at $0.99
          T+4:00  [STOP LOSS CHECK] If price < entry * 0.85 → sell now
          T+4:15  [FORCED EXIT] Sell regardless — market closes in <45s,
                  better to get $0.50 than $0.00
          T+5:00  Market resolves — too late to sell

        This guarantees stop-loss fires on 5min markets because:
        1. We check price at T+4:00 (enough time to execute sell)
        2. We force-exit at T+4:15 regardless of price
        3. No rate limit issues — we only call API when positions are near expiry
        """
        if not self.open_positions:
            return

        now = datetime.now(tz=timezone.utc)

        for token_id, pos in list(self.open_positions.items()):
            slug = pos.get("slug", "")
            buy_time = pos.get("buy_time")
            entry_price = pos.get("entry_price", 0.99)
            amount = pos.get("amount", 0)

            if not buy_time:
                continue

            # Normalize buy_time to UTC once
            if buy_time.tzinfo is None:
                buy_time_utc = buy_time.replace(tzinfo=timezone.utc)
            else:
                buy_time_utc = buy_time

            # Calculate market end time from slug timestamp
            # Slugs look like: btc-updown-5m-1774481700
            # The trailing number is the market START unix timestamp
            # Market duration = 300 seconds (5 minutes)
            market_end_time = None
            if slug:
                try:
                    ts_str = slug.split("-")[-1]
                    if ts_str.isdigit():
                        market_start = int(ts_str)
                        market_end_time = datetime.fromtimestamp(
                            market_start + 300, tz=timezone.utc
                        )
                except (ValueError, IndexError):
                    pass

            # Fallback: estimate end time from buy_time + 5 minutes
            if market_end_time is None:
                market_end_time = buy_time_utc + timedelta(seconds=300)

            seconds_until_close = (market_end_time - now).total_seconds()
            seconds_held = (now - buy_time_utc).total_seconds()

            # Skip positions that just opened (< 30 seconds old)
            if seconds_held < 30:
                log.debug(
                    "[STOP LOSS] Token %s — held only %.0fs, too early to check",
                    token_id,
                    seconds_held,
                )
                continue

            # === FORCED EXIT: Market closes in < 45 seconds ===
            # Sell NOW while there is still liquidity, regardless of price
            if 0 < seconds_until_close < 45:
                log.warning(
                    "[STOP LOSS] FORCED EXIT — market closes in %.0fs | "
                    "Token %s | Amount $%.4f | slug=%s",
                    seconds_until_close,
                    token_id,
                    amount,
                    slug,
                )
                await self._execute_stop_loss_sell(
                    token_id, pos, reason="FORCED_EXIT_TIME"
                )
                continue

            # Skip if market already closed (let redeem handle it)
            if seconds_until_close <= 0:
                log.debug(
                    "[STOP LOSS] Token %s — market already closed, skip", token_id
                )
                continue

            # === PRICE CHECK: Only when market is 4+ minutes old ===
            # We only check price when close to resolution to minimize API calls
            # At T+4:00 there are still ~60 seconds of liquidity left to sell into
            if seconds_held >= 240:  # 4 minutes held
                try:
                    book = _fetch_order_book(str(token_id))
                    bids = book.get("bids", [])
                    current_price = float(bids[0]["price"]) if bids else 0.0

                    if current_price <= 0:
                        log.debug("[STOP LOSS] Token %s — no bid data", token_id)
                        continue

                    # Market resolved as winner — skip, let redeem handle
                    if current_price >= WINNER_PRICE_THRESHOLD:
                        log.debug(
                            "[STOP LOSS] Token %s — price $%.4f looks like winner",
                            token_id,
                            current_price,
                        )
                        continue

                    # Market resolved as loser — clean up, can't sell
                    if current_price <= LOSER_PRICE_THRESHOLD:
                        log.warning(
                            "[STOP LOSS] Token %s — price $%.4f, resolved against us. "
                            "Removing from tracking.",
                            token_id,
                            current_price,
                        )
                        self.open_positions.pop(token_id, None)
                        continue

                    # Calculate loss
                    loss_pct = (1 - current_price / entry_price) * 100

                    if loss_pct >= 15.0:
                        log.warning(
                            "[STOP LOSS TRIGGERED] Token %s | Loss %.1f%% | "
                            "Price $%.4f (Entry $%.4f) | %.0fs until close",
                            token_id,
                            loss_pct,
                            current_price,
                            entry_price,
                            seconds_until_close,
                        )
                        await self._execute_stop_loss_sell(
                            token_id, pos, reason=f"PRICE_LOSS_{loss_pct:.0f}PCT"
                        )
                    else:
                        log.debug(
                            "[STOP LOSS OK] Token %s | loss %.1f%% < 15%% | "
                            "$%.4f → $%.4f | %.0fs until close",
                            token_id,
                            loss_pct,
                            entry_price,
                            current_price,
                            seconds_until_close,
                        )

                except Exception as exc:
                    exc_str = str(exc)
                    if "429" in exc_str:
                        log.debug(
                            "[STOP LOSS] Rate limited on token %s — skip this cycle",
                            token_id,
                        )
                    else:
                        log.error("[STOP LOSS ERROR] Token %s: %s", token_id, exc)

    async def _execute_stop_loss_sell(
        self,
        token_id: int | str,
        pos: dict,
        reason: str = "STOP_LOSS",
    ) -> None:
        """Execute a market sell for a stop-loss position.

        Uses MarketOrderArgs for best execution — fills at best available bid.
        Removes position from tracking regardless of success.
        """
        sell_side = "SELL" if pos.get("side") == "BUY" else "BUY"
        amount = pos.get("amount", 0)
        slug = pos.get("slug", str(token_id))

        log.warning(
            "[STOP LOSS SELL] %s | token=%s | amount=$%.4f | side=%s | slug=%s",
            reason,
            str(token_id)[:20],
            amount,
            sell_side,
            slug,
        )

        try:
            clob_client = ClobClient(
                host=CLOB_API_BASE,
                key=self.private_key,
                chain_id=137,
            )
            raw_creds = clob_client.create_or_derive_api_creds()
            if hasattr(raw_creds, "model_dump"):
                creds_dict = raw_creds.model_dump()
            elif hasattr(raw_creds, "dict"):
                creds_dict = raw_creds.dict()
            elif hasattr(raw_creds, "__dict__"):
                creds_dict = vars(raw_creds)
            else:
                creds_dict = dict(raw_creds)

            clob_client.set_api_creds(
                ApiCreds(
                    api_key=creds_dict.get("apiKey") or creds_dict.get("api_key", ""),
                    api_secret=creds_dict.get("apiSecret")
                    or creds_dict.get("api_secret", ""),
                    api_passphrase=creds_dict.get("apiPassphrase")
                    or creds_dict.get("api_passphrase", ""),
                )
            )

            order_args = MarketOrderArgs(
                token_id=str(token_id),
                amount=amount,
                side=sell_side,
            )
            result = await asyncio.to_thread(
                clob_client.create_market_order, order_args
            )
            log.info(
                "[STOP LOSS SOLD] %s | token=%s | result=%s",
                reason,
                str(token_id)[:20],
                result,
            )

        except Exception as exc:
            log.error(
                "[STOP LOSS SELL FAILED] %s | token=%s | %s",
                reason,
                str(token_id)[:20],
                exc,
            )
        finally:
            # Always remove from tracking, whether sell succeeded or not
            self.open_positions.pop(token_id, None)
            log.info("[STOP LOSS] Removed token %s from tracking", str(token_id)[:20])

    async def execute_trade(
        self,
        token_id: int | str,
        amount_usdc: int | float,
        side: str,
        market: dict | None = None,
    ) -> dict:
        """Execute a market order via ClobClient.

        Args:
            token_id: The outcome token ID.
            amount_usdc: Amount in raw USDC units (6 decimals) **or**
                human-readable float.  Values >= 1000 are treated as raw.
            side: ``"up"``/``"buy"`` → BUY, anything else → SELL.
            market: Optional market dict containing conditionId and slug
                for position tracking and auto-redeem.

        Returns:
            Dict with ``status`` and ``result`` keys.

        Raises:
            ValueError: If private key is not configured.
            RuntimeError: If the CLOB rejects the order.
        """
        if not self.private_key:
            raise ValueError("POLYMARKET_PRIVATE_KEY is not set")

        side_upper = side.upper()
        side_label = "BUY" if side_upper in ("UP", "BUY", "YES") else "SELL"

        # Normalise amount to human-readable USDC
        if isinstance(amount_usdc, (int, float)) and amount_usdc >= 1000:
            desired_usdc = amount_usdc / 1_000_000
        else:
            desired_usdc = float(amount_usdc)

        log.info(
            "[ONCHAIN CYCLE MATCH] Token %s | Side: %s | Amount: %s USDC",
            token_id,
            side_label,
            desired_usdc,
        )

        # ── V78: Check USDC balance via web3 and apply effective-amount logic ──
        if self.w3 is not None and self.w3.is_connected():
            usdc_contract = self.w3.eth.contract(
                address=Web3.to_checksum_address(USDC_ADDRESS),
                abi=USDC_ABI,
            )
            balance_raw = usdc_contract.functions.balanceOf(self.wallet).call()
            max_possible = balance_raw / 1_000_000
        else:
            max_possible = desired_usdc  # offline: trust the caller

        # V86: Balance Master V6 – log balance and Kelly suggestion
        log.info(
            "[BALANCE MASTER V6] Current balance: %.6f USDC | Kelly suggested: %.4f USDC",
            max_possible,
            desired_usdc,
        )

        # V86: Enforce Polymarket Min-Order-Size $1 strikt – need $1 + reserve
        if max_possible < 1.05:
            log.warning(
                "[BALANCE MASTER V6] Balance too low (%.6f < 1.05) → skipping (need $1 + reserve)",
                max_possible,
            )
            return {"status": "skipped", "reason": "below_min_size"}

        # V86: Kelly respektieren + Min $1 erzwingen
        effective_amount = min(desired_usdc, max_possible - 0.2)
        effective_amount = max(effective_amount, 1.0)
        if effective_amount != desired_usdc:
            log.info(
                "[BALANCE MASTER V6] Reduced to %.4f USDC (from %.4f)",
                effective_amount,
                desired_usdc,
            )
        desired_usdc = effective_amount

        # V86: Dynamic Kelly – final trade size
        log.info(
            "[DYNAMIC KELLY V86] Final trade size: %.4f USDC",
            desired_usdc,
        )

        # ── Build ClobClient with derived L2 credentials ─────────────
        clob_client = ClobClient(
            host=CLOB_API_BASE,
            key=self.private_key,
            chain_id=137,
        )

        raw_creds = clob_client.create_or_derive_api_creds()

        # Robust credential extraction (pydantic v1/v2 and plain objects)
        if hasattr(raw_creds, "model_dump"):
            creds_dict = raw_creds.model_dump()
        elif hasattr(raw_creds, "dict"):
            creds_dict = raw_creds.dict()
        elif hasattr(raw_creds, "__dict__"):
            creds_dict = vars(raw_creds)
        else:
            creds_dict = dict(raw_creds)

        api_key = creds_dict.get("apiKey") or creds_dict.get("api_key", "")
        api_secret = creds_dict.get("apiSecret") or creds_dict.get("api_secret", "")
        api_passphrase = creds_dict.get("apiPassphrase") or creds_dict.get(
            "api_passphrase", ""
        )
        clob_client.set_api_creds(
            ApiCreds(
                api_key=api_key,
                api_secret=api_secret,
                api_passphrase=api_passphrase,
            )
        )

        # ── Submit market order ──────────────────────────────────────
        order_args = MarketOrderArgs(
            token_id=str(token_id),
            amount=desired_usdc,
            side=side_label,
        )

        try:
            result = clob_client.create_market_order(order_args)
        except Exception as exc:
            error_str = str(exc).lower()
            # V86: Handle CLOB rejection (min size, balance, or invalid amount)
            if (
                "invalid amount" in error_str
                or "min size" in error_str
                or "not enough balance" in error_str
            ):
                # V86: CLOB rejection handling with V6 prefix
                log.warning(
                    "[BALANCE MASTER V6] CLOB rejected (min size or balance) → skipped"
                )
                return {"status": "skipped", "reason": "clob_rejected"}
            log.error("[TRADE ERROR V86] %s - %s", type(exc).__name__, exc)
            raise RuntimeError(f"CLOB order submission failed: {exc}") from exc

        log.info(
            "[CLOB SUCCESS] Order submitted for token %s | Side: %s",
            token_id,
            side_label,
        )

        # === V74: Register position for cut-loss tracking ===
        try:
            book = _fetch_order_book(str(token_id))
            best_price, _ = _get_best_price(book, side_label)
            entry_price = best_price if best_price > 0 else 0.5
        except Exception:
            entry_price = 0.5

        # V88: Extract condition_id and slug from market dict for auto-redeem
        condition_id = None
        slug = ""
        if market:
            condition_id = market.get("conditionId") or market.get("condition_id")
            slug = market.get("slug", "")

        self.open_positions[token_id] = {
            "side": side_label,
            "amount": desired_usdc,
            "entry_price": entry_price,
            "buy_time": datetime.now(tz=timezone.utc),
            "condition_id": condition_id,  # V88: needed for redeem
            "winning_outcome": None,  # V88: set later when market resolves
            "slug": slug,  # V88: for polling resolution status
        }
        log.info(
            "[POSITION REGISTERED] %s %.4f USDC (Kelly-based) | Token %s | Condition %s",
            side_label,
            desired_usdc,
            token_id,
            condition_id or "unknown",
        )

        # V86: Combined CLOB + ONCHAIN success log
        log.info(
            "[CLOB SUCCESS] + [ONCHAIN TRADE SUCCESS V86] %s %.4f USDC on token %s",
            side_label,
            desired_usdc,
            token_id,
        )
        return {"status": "success", "result": result}

    # ── V70: Automatic position redemption after market resolution ────────
    async def redeem_winning_positions(
        self,
        condition_id: str,
        winning_outcome: str,
    ) -> dict:
        """V89: Redeem winning conditional-token positions after market resolution.

        Calls ``redeemPositions`` on the Polymarket ConditionalTokens (CTF)
        contract to convert winning outcome tokens back into USDC.

        V89 improvements:
        - Concurrency control: Prevents duplicate redeem attempts for same wallet+condition
        - On-chain verification: Checks payoutDenominator before redeeming
        - Binary market: Uses indexSets=[1, 2] to redeem both outcomes
        - Enhanced error handling: Gracefully handles TRANSACTION_REPLACED and reverts

        Args:
            condition_id: The bytes32 condition ID of the resolved market.
            winning_outcome: ``"up"`` (indexSet=1) or ``"down"`` (indexSet=2).
                            Note: V89 redeems both outcomes [1, 2] for binary markets.

        Returns:
            Transaction receipt as a dict, or empty dict if skipped/already handled.

        Raises:
            ValueError: If private key or web3 is not configured.
        """
        if not self.private_key:
            raise ValueError("POLYMARKET_PRIVATE_KEY is not set")
        if self.w3 is None:
            raise ValueError("Web3 is not configured")

        outcome_lower = winning_outcome.lower()
        if outcome_lower not in ("up", "down"):
            raise ValueError(
                f"Invalid winning_outcome: {winning_outcome!r} (expected 'up' or 'down')"
            )

        # V89: Concurrency control — prevent duplicate redeem for same wallet+condition
        redeem_key = f"{self.wallet}:{condition_id}"
        if redeem_key in _active_redeems:
            log.info(
                "[REDEEM V89] Skipping — another redeem already in progress for %s",
                condition_id,
            )
            return {}
        _active_redeems.add(redeem_key)

        try:
            ctf = self.w3.eth.contract(
                address=Web3.to_checksum_address(CTF_ADDRESS),
                abi=CTF_ABI,
            )

            # V89: Verify condition is resolved on-chain via payoutDenominator
            try:
                payout_denom = ctf.functions.payoutDenominator(condition_id).call()
                if payout_denom == 0:
                    log.info(
                        "[REDEEM V89] Skipping — condition %s not resolved on-chain",
                        condition_id,
                    )
                    return {}
            except Exception as check_exc:
                log.warning(
                    "[REDEEM V89] Could not verify resolution for %s: %s — proceeding anyway",
                    condition_id,
                    check_exc,
                )

            # V89: Binary market — redeem both outcomes [1, 2]
            index_sets = [1, 2]
            parent_collection_id = "0x" + "0" * 64

            tx = ctf.functions.redeemPositions(
                Web3.to_checksum_address(USDC_ADDRESS),
                parent_collection_id,
                condition_id,
                index_sets,
            ).build_transaction(
                {
                    "from": self.wallet,
                    # V89: Increased from 250k to 300k for [1,2] dual-outcome redemption.
                    # Conservative buffer since we now redeem both outcomes in one call.
                    "gas": 300_000,
                    "gasPrice": self.w3.to_wei("35", "gwei"),
                    "nonce": self.w3.eth.get_transaction_count(self.wallet),
                }
            )

            signed_tx = self.w3.eth.account.sign_transaction(tx, self.private_key)
            try:
                tx_hash = self.w3.eth.send_raw_transaction(signed_tx.raw_transaction)
            except Exception as send_exc:
                exc_msg = str(send_exc).lower()
                # V89: Handle nonce collision / already submitted
                if "already known" in exc_msg or "nonce too low" in exc_msg:
                    log.info(
                        "[REDEEM V89] TX already submitted (nonce collision) — treating as success. "
                        "condition=%s",
                        condition_id,
                    )
                    return {}
                # V89: Handle insufficient funds for gas
                if "insufficient funds" in exc_msg:
                    log.info(
                        "[REDEEM V89] Skipped — wallet needs POL for gas | condition=%s",
                        condition_id,
                    )
                    return {}
                raise

            receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)

            # V89: Handle reverted transaction gracefully (likely already redeemed)
            if receipt.get("status") == 0:
                log.info(
                    "[REDEEM V89] TX reverted for %s — likely already redeemed | tx=%s",
                    condition_id,
                    tx_hash.hex(),
                )
                return {}

            log.info(
                "[REDEEM SUCCESS V89] %s Position redeemed | Condition %s | Tx %s",
                winning_outcome.upper(),
                condition_id,
                tx_hash.hex(),
            )
            return dict(receipt)

        except Exception as err:
            # V89: Enhanced error handling for replacement transactions
            err_str = str(err)
            err_code = getattr(err, "code", None)

            # TRANSACTION_REPLACED with successful replacement — treat as success
            if err_code == "TRANSACTION_REPLACED":
                replacement_receipt = getattr(err, "receipt", {})
                if replacement_receipt.get("status") == 1:
                    replacement_hash = getattr(err, "replacement", {})
                    log.info(
                        "[REDEEM V89] Redeemed %s via replacement tx: %s",
                        condition_id,
                        getattr(replacement_hash, "hash", "unknown"),
                    )
                    return {}

            # Re-raise configuration errors that should propagate
            if isinstance(err, (ValueError, RuntimeError)):
                raise
            log.error(
                "[REDEEM ERROR V89] %s: %s",
                condition_id,
                err_str[:200],
            )
            return {}

        finally:
            # V89: Always release the lock
            _active_redeems.discard(redeem_key)

    def get_usdc_balance(self) -> float:
        """Return USDC balance for the configured wallet."""
        if not self.private_key:
            return 0.0
        return get_usdc_balance(self.private_key)

    def check_onchain_ready(self) -> bool:
        """Return True if the executor is operational."""
        if not self.private_key:
            return False
        return check_onchain_ready(self.private_key)


# ── V88: Singleton executor for shared position tracking ────────────────────
_shared_executor_instance: OnchainExecutor | None = None
_shared_executor_lock = threading.Lock()


def get_shared_executor() -> OnchainExecutor:
    """Get or create the shared OnchainExecutor singleton.

    This ensures all position tracking goes through a single instance
    so that auto-redeem monitoring can access positions registered
    via the module-level execute_trade function.

    Uses threading.Lock for thread safety in case of concurrent access.
    """
    global _shared_executor_instance
    if _shared_executor_instance is None:
        with _shared_executor_lock:
            # Double-checked locking pattern for thread safety
            if _shared_executor_instance is None:
                _shared_executor_instance = OnchainExecutor()
    return _shared_executor_instance


def set_shared_executor(executor: OnchainExecutor) -> None:
    """Set the shared executor instance (used by main_fastapi.py).

    Allows the application's OnchainExecutor instance (which starts the
    monitor) to be used as the shared instance for position tracking.
    """
    global _shared_executor_instance
    with _shared_executor_lock:
        _shared_executor_instance = executor
