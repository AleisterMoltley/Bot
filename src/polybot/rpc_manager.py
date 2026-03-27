"""Polygon RPC Manager with async support and automatic failover.

Mimics ethers.js FallbackProvider functionality with:
- Priority-based provider selection
- Latency-based ranking
- Automatic failover on connection failures
- Stall timeout handling
- Exponential backoff on rate limits
- Alchemy auto-detection with fallback warning
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import os
import time
from dataclasses import dataclass
from typing import Any

from aiohttp import ClientResponseError
from web3 import AsyncWeb3
from web3.providers import AsyncHTTPProvider

logger = logging.getLogger(__name__)

# Delay between sequential RPC latency tests to respect rate limits of free RPCs
# 0.8s is conservative for most free tier APIs (typically allow 1-2 req/sec)
_RATE_LIMIT_DELAY_SECONDS = 0.8

# Exponential backoff settings
_INITIAL_BACKOFF_MS = 100
_MAX_BACKOFF_MS = 10000
_BACKOFF_MULTIPLIER = 2


@dataclass
class ProviderInfo:
    """Information about a single RPC provider."""

    w3: AsyncWeb3
    url: str
    priority: int = 1
    latency_ms: float = 9999.0
    last_error_time: float = 0.0
    error_count: int = 0
    # Backoff tracking for rate limits
    backoff_until: float = 0.0
    consecutive_errors: int = 0
    # Is this the Alchemy provider?
    is_alchemy: bool = False


@dataclass
class RpcStatusInfo:
    """Status information for a provider (returned via API)."""

    url: str
    priority: int
    latency_ms: float
    is_connected: bool = False
    error_count: int = 0
    is_alchemy: bool = False
    in_backoff: bool = False


@dataclass
class RpcHealthStats:
    """Aggregated health statistics for all providers."""

    total_providers: int = 0
    connected_providers: int = 0
    alchemy_available: bool = False
    alchemy_connected: bool = False
    avg_latency_ms: float = 0.0
    best_latency_ms: float = 0.0
    total_errors: int = 0
    providers_in_backoff: int = 0


class PolygonRpcManager:
    """Async RPC manager with fallback and latency ranking.

    Features:
    - Automatic failover when a provider is down
    - Latency-based provider ranking (fastest first)
    - Error tracking with automatic deprioritization
    - Thread-safe provider cycling

    Usage:
        rpc_manager = PolygonRpcManager()
        await rpc_manager.rank_by_latency()  # Call at startup
        w3 = await rpc_manager.get_best_provider()
        block = await w3.eth.block_number
    """

    # Default stable free Polygon RPC endpoints (2026 - verified working)
    # Removed: polygon-rpc.com (401), llamarpc.com (DNS fail), ankr (API key),
    #          meowrpc (404), 1rpc.io/matic (429 Too Many Requests)
    DEFAULT_RPCS = [
        "https://polygon.drpc.org",
        "https://polygon-rpc.com",  # Matches config.polygon_rpc_url_override default
        "https://polygon-bor-rpc.publicnode.com",
        "https://rpc-mainnet.polygon.technology",
        "https://polygon.api.onfinality.io/public",
    ]

    # Fast free fallback RPCs for when Alchemy is missing (FORCE_ALCHEMY mode)
    # These are prioritized by reliability and speed
    FAST_FALLBACK_RPCS = [
        "https://polygon.drpc.org",  # dRPC - very fast and reliable
        "https://polygon-rpc.com",  # Matches config.polygon_rpc_url_override default
        "https://polygon.blockpi.network/v1/rpc/public",  # BlockPi - fast
    ]

    # Guaranteed fallback RPC (uses settings.polygon_rpc_url default)
    # FORCED EXECUTION v7: Changed from publicnode to polygon-rpc.com
    FALLBACK_RPC = "https://polygon-rpc.com"

    # Alchemy configuration link for dashboard
    ALCHEMY_SIGNUP_URL = "https://dashboard.alchemy.com/signup?chain=polygon"

    def __init__(self, rpc_urls: list[str] | None = None, force_alchemy: bool = True):
        """Initialize the RPC manager.

        Args:
            rpc_urls: Optional list of RPC URLs. If not provided,
                      reads from POLYGON_RPC_URLS env var or uses defaults.
            force_alchemy: If True, warn when Alchemy is not configured
                          and prefer fast free fallbacks.
        """
        self.force_alchemy = force_alchemy
        self.alchemy_configured = False
        self.alchemy_connected = False
        self.alchemy_missing_error = False  # True when FORCE_ALCHEMY=true but no key
        self.alchemy_connection_error: str | None = (
            None  # Error message if connection test failed
        )
        self.connected_block_number: int | None = None
        self.urls = self._get_urls(rpc_urls)
        self.providers: list[ProviderInfo] = []
        self.current_index = 0
        self._lock = asyncio.Lock()
        self._init_providers()

    def _get_urls(self, rpc_urls: list[str] | None) -> list[str]:
        """Get RPC URLs from provided list, environment, or defaults.

        Priority order:
        1. Explicitly provided rpc_urls list (with Alchemy prepended if available)
        2. POLYGON_RPC_URLS environment variable (comma-separated)
        3. POLYGON_RPC_URL environment variable (single URL)
        4. Default free RPC endpoints (or fast fallbacks if force_alchemy=True)

        If ALCHEMY_API_KEY is set, it's always prepended as the first/primary RPC.
        If force_alchemy=True and no Alchemy key, logs warning and uses fast fallbacks.
        """
        urls: list[str] = []

        # Check for Alchemy API key first (highest priority when available)
        alchemy_key = os.getenv("ALCHEMY_API_KEY", "")
        if alchemy_key.strip():
            alchemy_url = (
                f"https://polygon-mainnet.g.alchemy.com/v2/{alchemy_key.strip()}"
            )
            urls.append(alchemy_url)
            self.alchemy_configured = True
            logger.info("🔑 Alchemy RPC configured (will be used as primary)")
        elif self.force_alchemy:
            # FORCE_ALCHEMY mode: set error state for dashboard red warning
            self.alchemy_missing_error = True
            logger.error(
                "❌ ALCHEMY_API_KEY in Railway setzen! "
                f"FORCE_ALCHEMY=true erfordert Alchemy API Key. "
                f"Hol dir einen kostenlosen API-Key: {self.ALCHEMY_SIGNUP_URL}"
            )
            logger.info(
                "🔄 Verwende schnelle kostenlose Fallback-RPCs statt Alchemy..."
            )

        # If custom URLs provided, use them (with Alchemy prepended if available)
        if rpc_urls:
            urls.extend([u.strip() for u in rpc_urls if u.strip()])
            return urls if urls else self.DEFAULT_RPCS.copy()

        # Check POLYGON_RPC_URLS environment variable
        env_urls = os.getenv("POLYGON_RPC_URLS", "")
        if env_urls:
            additional = [u.strip() for u in env_urls.split(",") if u.strip()]
            if additional:
                urls.extend(additional)
                return urls

        # Fallback to single URL from POLYGON_RPC_URL
        single_url = os.getenv("POLYGON_RPC_URL", "")
        if single_url.strip():
            urls.append(single_url.strip())
            return urls

        # If Alchemy is configured, add defaults as fallback
        if self.alchemy_configured:
            urls.extend(self.DEFAULT_RPCS)
            return urls

        # No Alchemy - use fast fallbacks if force_alchemy is True
        if self.force_alchemy:
            return self.FAST_FALLBACK_RPCS.copy()

        return self.DEFAULT_RPCS.copy()

    def _init_providers(self) -> None:
        """Initialize Web3 providers for all URLs."""
        for url in self.urls:
            try:
                w3 = AsyncWeb3(AsyncHTTPProvider(url))
                is_alchemy = url.startswith("https://polygon-mainnet.g.alchemy.com/")
                provider_info = ProviderInfo(w3=w3, url=url, is_alchemy=is_alchemy)
                self.providers.append(provider_info)
                if is_alchemy:
                    self.alchemy_configured = True
            except Exception as e:
                logger.warning(f"Failed to initialize provider {url}: {e}")

        # Log Alchemy status
        if self.alchemy_configured:
            logger.info("✅ Alchemy RPC konfiguriert als primärer Provider")
        elif self.force_alchemy:
            # Already warned in _get_urls, just log fallback info
            logger.info(
                f"ℹ️ Verwende {len(self.providers)} Fallback-RPCs (Alchemy empfohlen für Production)"
            )
        else:
            logger.warning(
                "⚠️ Alchemy nicht konfiguriert – public RPCs können instabil sein! "
                "Setze ALCHEMY_API_KEY für bessere Performance."
            )

        logger.info(
            f"✅ PolygonRpcManager initialized with {len(self.providers)} RPC endpoints"
        )

    async def get_best_provider(self, timeout: float = 5.0) -> AsyncWeb3:
        """Get the best available provider with automatic failover.

        Mimics ethers FallbackProvider behavior with async support.
        Includes exponential backoff for rate-limited providers.

        Args:
            timeout: Connection check timeout in seconds

        Returns:
            AsyncWeb3 instance for the best available provider

        Raises:
            Exception: If all providers are unavailable
        """
        async with self._lock:
            current_time = time.time()
            for _ in range(len(self.providers)):
                provider = self.providers[self.current_index]

                # Skip providers in backoff
                if provider.backoff_until > current_time:
                    logger.debug(
                        f"Skipping {provider.url}: in backoff until "
                        f"{provider.backoff_until - current_time:.1f}s"
                    )
                    self.current_index = (self.current_index + 1) % len(self.providers)
                    continue

                try:
                    # Check if provider is connected with timeout
                    # Cap at 4s max per test to avoid long waits on dead RPCs
                    async with asyncio.timeout(min(timeout, 4.0)):
                        is_connected = await provider.w3.is_connected()
                        if is_connected:
                            # Reset consecutive errors on success
                            provider.consecutive_errors = 0
                            return provider.w3
                except asyncio.TimeoutError:
                    logger.warning(f"RPC timeout: {provider.url}")
                    self._apply_backoff(provider)
                except ClientResponseError as e:
                    if e.status == 429:
                        # Rate limited - apply longer backoff
                        logger.warning(f"RPC rate limited (429): {provider.url}")
                        self._apply_backoff(provider, rate_limited=True)
                    else:
                        logger.warning(f"RPC HTTP error {e.status}: {provider.url}")
                        self._apply_backoff(provider)
                except Exception as e:
                    # Silent warning for dead RPCs (never ERROR)
                    logger.warning(f"RPC unavailable {provider.url}: {e}")
                    self._apply_backoff(provider)

                # Move to next provider
                self.current_index = (self.current_index + 1) % len(self.providers)

        raise Exception("All Polygon RPCs are unavailable")

    def _apply_backoff(
        self, provider: ProviderInfo, rate_limited: bool = False
    ) -> None:
        """Apply exponential backoff to a provider.

        Args:
            provider: The provider to apply backoff to
            rate_limited: If True, apply longer backoff for rate limits
        """
        provider.error_count += 1
        provider.consecutive_errors += 1
        provider.last_error_time = time.time()

        # Calculate backoff duration (exponential with cap)
        backoff_ms = _INITIAL_BACKOFF_MS * (
            _BACKOFF_MULTIPLIER**provider.consecutive_errors
        )
        if rate_limited:
            # Double the backoff for rate limits
            backoff_ms *= 2
        backoff_ms = min(backoff_ms, _MAX_BACKOFF_MS)

        provider.backoff_until = time.time() + (backoff_ms / 1000)

    async def rank_by_latency(self, timeout: float = 3.0) -> None:
        """Sort providers by actual latency (call at startup).

        Measures response time for each provider SEQUENTIALLY with sleep
        between tests to respect rate limits of free RPCs. Dead RPCs are
        automatically removed from the list. Guarantees at least 1 working RPC.

        Args:
            timeout: Maximum time to wait for each provider's response (default 3s)
        """
        working_providers: list[tuple[float, ProviderInfo]] = []
        dead_urls: list[str] = []

        for i, provider in enumerate(self.providers):
            # Add sleep between tests to respect rate limits (except for first)
            if i > 0:
                await asyncio.sleep(_RATE_LIMIT_DELAY_SECONDS)

            start = time.perf_counter()
            try:
                # Wrap each test with asyncio.timeout for safety
                async with asyncio.timeout(timeout):
                    await provider.w3.eth.block_number
                    latency_ms = (time.perf_counter() - start) * 1000
                    provider.latency_ms = latency_ms
                    working_providers.append((latency_ms, provider))
                    logger.debug(f"RPC {provider.url}: {latency_ms:.1f}ms")
            except asyncio.TimeoutError:
                # Silent warning (never ERROR) - timeout during latency test
                logger.warning(f"RPC timeout during latency test: {provider.url}")
                dead_urls.append(provider.url)
            except ClientResponseError as e:
                # Handle HTTP errors (429, 401, 404, etc.) with warning only (never ERROR)
                if e.status in (429, 401, 403, 404):
                    logger.warning(
                        f"RPC rate-limited or unavailable (HTTP {e.status}): {provider.url}"
                    )
                else:
                    logger.warning(
                        f"RPC HTTP error {e.status} during latency test: {provider.url}"
                    )
                dead_urls.append(provider.url)
            except (ConnectionError, OSError):
                # Handle connection errors with warning only (never ERROR)
                logger.warning(f"RPC connection error: {provider.url}")
                dead_urls.append(provider.url)
            except Exception as e:
                # Catch-all for other errors (never ERROR level)
                logger.warning(
                    f"RPC unavailable during latency test {provider.url}: {e}"
                )
                dead_urls.append(provider.url)

        # Sort by latency (fastest first) - only working RPCs
        self.providers = [p for _, p in sorted(working_providers, key=lambda x: x[0])]
        self.current_index = 0

        # Guarantee at least 1 working RPC (fallback to settings.polygon_rpc_url default)
        if not self.providers:
            logger.warning(f"All RPCs failed! Falling back to {self.FALLBACK_RPC}")
            try:
                w3 = AsyncWeb3(AsyncHTTPProvider(self.FALLBACK_RPC))
                fallback_provider = ProviderInfo(w3=w3, url=self.FALLBACK_RPC)
                self.providers.append(fallback_provider)
            except Exception as e:
                logger.warning(
                    f"Fallback RPC init failed: {e} - "
                    "No RPCs available, system may be non-functional"
                )

        # Log the final ranked list with clean output (required info log)
        stable_count = len(self.providers)
        ranked_urls = [p.url for p in self.providers]
        logger.info(
            f"✅ Using {stable_count} stable RPCs sorted by latency: {ranked_urls}"
        )

        if dead_urls:
            logger.debug(f"Removed {len(dead_urls)} dead RPCs: {dead_urls}")

    async def get_status(self) -> list[RpcStatusInfo]:
        """Get current status of all providers.

        Returns:
            List of RpcStatusInfo objects with connection status
        """
        status_list: list[RpcStatusInfo] = []
        current_time = time.time()

        for i, provider in enumerate(self.providers):
            try:
                is_connected = await asyncio.wait_for(
                    provider.w3.is_connected(), timeout=2.0
                )
            except Exception:
                is_connected = False

            status_list.append(
                RpcStatusInfo(
                    url=provider.url,
                    priority=i + 1,  # Position in sorted list = priority
                    latency_ms=provider.latency_ms,
                    is_connected=is_connected,
                    error_count=provider.error_count,
                    is_alchemy=provider.is_alchemy,
                    in_backoff=provider.backoff_until > current_time,
                )
            )

        return status_list

    async def get_health_stats(self) -> RpcHealthStats:
        """Get aggregated health statistics for all providers.

        Returns:
            RpcHealthStats with overall health information
        """
        status_list = await self.get_status()
        current_time = time.time()

        connected = [s for s in status_list if s.is_connected]
        alchemy_providers = [s for s in status_list if s.is_alchemy]
        in_backoff = [p for p in self.providers if p.backoff_until > current_time]

        latencies = [s.latency_ms for s in connected if s.latency_ms < 9999]

        return RpcHealthStats(
            total_providers=len(status_list),
            connected_providers=len(connected),
            alchemy_available=len(alchemy_providers) > 0,
            alchemy_connected=any(s.is_connected for s in alchemy_providers),
            avg_latency_ms=sum(latencies) / len(latencies) if latencies else 0,
            best_latency_ms=min(latencies) if latencies else 0,
            total_errors=sum(s.error_count for s in status_list),
            providers_in_backoff=len(in_backoff),
        )

    def is_alchemy_connected(self) -> bool:
        """Check if Alchemy RPC is the current active provider.

        Returns:
            True if Alchemy is connected and being used
        """
        if not self.providers:
            return False
        current = self.providers[self.current_index]
        return current.is_alchemy and self.alchemy_connected

    def get_alchemy_status(self) -> dict[str, Any]:
        """Get detailed Alchemy connection status for dashboard.

        Returns:
            Dict with Alchemy status, block number, and help link if not configured
        """
        # Check for missing key error state (FORCE_ALCHEMY=true but no key)
        if self.alchemy_missing_error:
            return {
                "configured": False,
                "connected": False,
                "missing_error": True,
                "connection_error": None,
                "block_number": None,
                "help_url": self.ALCHEMY_SIGNUP_URL,
                "message": "ALCHEMY_API_KEY in Railway setzen!",
            }

        if not self.alchemy_configured:
            return {
                "configured": False,
                "connected": False,
                "missing_error": False,
                "connection_error": None,
                "block_number": None,
                "help_url": self.ALCHEMY_SIGNUP_URL,
                "message": "Alchemy API Key nicht konfiguriert",
            }

        # Check for connection test failure
        if self.alchemy_connection_error:
            return {
                "configured": True,
                "connected": False,
                "missing_error": False,
                "connection_error": self.alchemy_connection_error,
                "block_number": None,
                "help_url": self.ALCHEMY_SIGNUP_URL,
                "message": self.alchemy_connection_error,
            }

        if not self.alchemy_connected:
            return {
                "configured": True,
                "connected": False,
                "missing_error": False,
                "connection_error": None,
                "block_number": None,
                "help_url": self.ALCHEMY_SIGNUP_URL,
                "message": "Alchemy konfiguriert aber nicht verbunden",
            }

        return {
            "configured": True,
            "connected": True,
            "missing_error": False,
            "connection_error": None,
            "block_number": self.connected_block_number,
            "help_url": None,
            "message": f"Alchemy verbunden (Block #{self.connected_block_number})"
            if self.connected_block_number
            else "Alchemy verbunden",
        }

    async def update_connection_status(self, block_number: int | None = None) -> None:
        """Update Alchemy connection status after successful connection.

        Args:
            block_number: Current block number from RPC
        """
        if self.providers and self.providers[self.current_index].is_alchemy:
            self.alchemy_connected = True
            self.connected_block_number = block_number
            if block_number:
                logger.info(f"✅ Alchemy connected at block #{block_number}")
        else:
            self.alchemy_connected = False
            # Only warn if Alchemy was configured but we're using a fallback
            # Don't warn if FORCE_ALCHEMY=true but no key was set (that's handled elsewhere)
            if self.alchemy_configured and not self.alchemy_missing_error:
                logger.warning("⚠️ Alchemy konfiguriert aber Fallback-RPC aktiv")

    async def test_alchemy_connection(
        self, timeout: float = 5.0
    ) -> tuple[bool, int | None, str | None]:
        """Test Alchemy RPC connection specifically.

        Performs a real connection test by calling eth.block_number on the Alchemy provider.

        Args:
            timeout: Connection test timeout in seconds

        Returns:
            Tuple of (success: bool, block_number: int | None, error_message: str | None)
        """
        # Find Alchemy provider
        alchemy_provider = None
        for provider in self.providers:
            if provider.is_alchemy:
                alchemy_provider = provider
                break

        if not alchemy_provider:
            if self.alchemy_configured:
                error_msg = "Alchemy konfiguriert aber Provider nicht gefunden"
                self.alchemy_connection_error = error_msg
                return False, None, error_msg
            return False, None, "Alchemy nicht konfiguriert"

        try:
            async with asyncio.timeout(timeout):
                block_number = await alchemy_provider.w3.eth.block_number
                self.alchemy_connected = True
                self.connected_block_number = block_number
                self.alchemy_connection_error = None  # Clear any previous error
                logger.info(
                    f"✅ Alchemy Test-Connect erfolgreich – Block #{block_number}"
                )
                return True, block_number, None
        except asyncio.TimeoutError:
            error_msg = "Alchemy Connection failed – Timeout. Key prüfen!"
            logger.error(f"❌ {error_msg}")
            self.alchemy_connected = False
            self.alchemy_connection_error = error_msg
            return False, None, error_msg
        except Exception as e:
            error_msg = f"Alchemy Connection failed – {e}. Key prüfen!"
            logger.error(f"❌ {error_msg}")
            self.alchemy_connected = False
            self.alchemy_connection_error = error_msg
            return False, None, error_msg

    async def execute_with_fallback(
        self,
        method: str,
        *args: Any,
        timeout: float = 10.0,
        **kwargs: Any,
    ) -> Any:
        """Execute a Web3 method with automatic fallback on failure.

        Args:
            method: Dot-separated method path (e.g., "eth.block_number",
                    "eth.get_balance"). For properties that return coroutines
                    (like block_number), omit the parentheses in the method name.
            *args: Positional arguments for method calls (ignored for properties)
            timeout: Timeout for the method call
            **kwargs: Keyword arguments for method calls (ignored for properties)

        Returns:
            Result of the Web3 method or property call

        Raises:
            Exception: If all providers fail

        Example:
            # For async properties:
            block = await rpc_manager.execute_with_fallback("eth.block_number")

            # For methods:
            balance = await rpc_manager.execute_with_fallback(
                "eth.get_balance", address
            )
        """
        last_error: Exception | None = None

        for _ in range(len(self.providers)):
            w3 = await self.get_best_provider()

            try:
                # Navigate to the method or property
                obj: Any = w3
                for part in method.split("."):
                    obj = getattr(obj, part)

                # Handle both callable methods and coroutine properties
                if inspect.iscoroutine(obj):
                    # Property that returns a coroutine (e.g., eth.block_number)
                    result = await asyncio.wait_for(obj, timeout=timeout)
                elif callable(obj):
                    # Method that needs to be called with args
                    result = await asyncio.wait_for(
                        obj(*args, **kwargs), timeout=timeout
                    )
                else:
                    # Synchronous property
                    result = obj

                return result

            except Exception as e:
                last_error = e
                logger.warning(f"Method {method} failed, trying next provider: {e}")
                # Increment error count for current provider
                if self.providers:
                    self.providers[self.current_index].error_count += 1
                continue

        raise last_error or Exception("All providers failed")


# Global instance (lazy initialization)
_rpc_manager: PolygonRpcManager | None = None


def get_rpc_manager() -> PolygonRpcManager:
    """Get or create the global RPC manager instance."""
    global _rpc_manager
    if _rpc_manager is None:
        _rpc_manager = PolygonRpcManager()
    return _rpc_manager
