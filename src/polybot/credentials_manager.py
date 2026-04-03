"""Auto-bootstrap Polymarket L2 API credentials at startup.

Derives L2 credentials from POLYMARKET_PRIVATE_KEY using py_clob_client.
Caches them to disk and exports to os.environ so all modules can use them.

Priority:
  1. Already set as env vars (POLY_API_KEY etc.) -> use them directly
  2. Cached in /tmp/polymarket_creds.json -> load and re-export to env
  3. Derive fresh from private key via py_clob_client -> cache + export to env
"""

from __future__ import annotations

import json
import logging
import os

logger = logging.getLogger(__name__)

CREDS_FILE = None  # V91: Credentials no longer cached to disk (security fix)
_REQUIRED_ENV_KEYS = ["POLY_API_KEY", "POLY_API_SECRET", "POLY_API_PASSPHRASE"]

# Module-level singleton so other modules (executor.py) can import cached creds
# without re-reading env vars or disk every time.
_L2_CREDS: dict | None = None


def get_or_create_l2_creds() -> dict:
    """Derive L2 API credentials from POLYMARKET_PRIVATE_KEY.

    Returns dict with keys: api_key, api_secret, api_passphrase
    """
    global _L2_CREDS

    # Priority 1: already in env
    if all(os.environ.get(k) for k in _REQUIRED_ENV_KEYS):
        logger.info("[CREDS] L2 credentials loaded from environment variables")
        creds = {
            "api_key": os.environ["POLY_API_KEY"],
            "api_secret": os.environ["POLY_API_SECRET"],
            "api_passphrase": os.environ["POLY_API_PASSPHRASE"],
        }
        _L2_CREDS = creds
        return creds

    # Priority 2: derive fresh from private key
    private_key = os.environ.get("POLYMARKET_PRIVATE_KEY")
    if not private_key:
        raise RuntimeError(
            "POLYMARKET_PRIVATE_KEY not set — cannot derive L2 credentials"
        )

    logger.info("[CREDS] Deriving L2 credentials from private key...")
    from py_clob_client.client import ClobClient

    client = ClobClient(
        "https://clob.polymarket.com",
        key=private_key,
        chain_id=137,
    )
    derived = client.create_or_derive_api_creds()
    creds = {
        "api_key": derived.api_key,
        "api_secret": derived.api_secret,
        "api_passphrase": derived.api_passphrase,
    }

    # Validate — all fields must be non-empty
    if not all(creds.values()):
        raise RuntimeError("[CREDS] Derived credentials are empty")

    _export_to_env(creds)
    _L2_CREDS = creds
    logger.info(
        f"[CREDS] L2 credentials derived successfully api_key={creds['api_key'][:8]}..."
    )
    return creds


def get_cached_creds() -> dict | None:
    """Return cached creds from singleton or env vars if available, else None."""
    global _L2_CREDS
    if _L2_CREDS:
        return _L2_CREDS
    # Also try env vars as fallback
    if all(os.environ.get(k) for k in _REQUIRED_ENV_KEYS):
        return {
            "api_key": os.environ["POLY_API_KEY"],
            "api_secret": os.environ["POLY_API_SECRET"],
            "api_passphrase": os.environ["POLY_API_PASSPHRASE"],
        }
    return None


def _export_to_env(creds: dict) -> None:
    """Write credentials into os.environ so all modules see them."""
    os.environ["POLY_API_KEY"] = creds["api_key"]
    os.environ["POLY_API_SECRET"] = creds["api_secret"]
    os.environ["POLY_API_PASSPHRASE"] = creds["api_passphrase"]


def validate_l2_creds(creds: dict) -> bool:
    """Test L2 credentials against the CLOB API. Returns True if valid."""
    try:
        from py_clob_client.client import ClobClient
        from py_clob_client.clob_types import ApiCreds

        client = ClobClient(
            "https://clob.polymarket.com",
            key=os.environ["POLYMARKET_PRIVATE_KEY"],
            chain_id=137,
            creds=ApiCreds(
                api_key=creds["api_key"],
                api_secret=creds["api_secret"],
                api_passphrase=creds["api_passphrase"],
            ),
        )
        result = client.get_api_keys()
        logger.info("[CREDS] L2 credentials validated OK")
        return True
    except Exception as e:
        logger.error(f"[CREDS] L2 credential validation FAILED: {e}")
        return False
