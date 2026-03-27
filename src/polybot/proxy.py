"""Proxy management for geo-restricted API access.

Provides SOCKS5 proxy support with automatic rotation, residential proxy
fallback, and community mirror endpoints. Designed for reliable Polymarket
access from geo-restricted regions (USA, etc.) as of March 2026.

Uses tenacity for exponential backoff retries with structured logging.
"""

from __future__ import annotations

import asyncio
import random
import time
from dataclasses import dataclass

import aiohttp
import httpx
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from polybot.config import get_settings
from polybot.logging_setup import get_logger

log = get_logger(__name__)

# Community mirror endpoints (backup if main API is blocked)
GAMMA_MIRRORS = [
    "https://gamma-api.polymarket.com",
    # Add community mirrors here as they become available
]

CLOB_MIRRORS = [
    "https://clob.polymarket.com",
    # Add community mirrors here as they become available
]

DATA_MIRRORS = [
    "https://data-api.polymarket.com",
]


@dataclass
class ProxyHealth:
    """Track health status of a proxy."""

    url: str
    successes: int = 0
    failures: int = 0
    last_success: float = 0
    last_failure: float = 0
    latency_ms: float = 0
    is_blocked: bool = False
    consecutive_failures: int = 0

    @property
    def score(self) -> float:
        """Calculate health score (higher is better)."""
        if self.is_blocked or self.consecutive_failures >= 3:
            return -1
        total = self.successes + self.failures
        if total == 0:
            return 50  # Neutral score for untested
        success_rate = self.successes / total
        latency_penalty = min(self.latency_ms / 1000, 1.0)  # Max 1.0 penalty
        recency_bonus = 10 if time.time() - self.last_success < 300 else 0
        return (success_rate * 80) - (latency_penalty * 20) + recency_bonus


@dataclass
class ProxyConfig:
    """Configuration for a single proxy."""

    host: str
    port: int
    username: str = ""
    password: str = ""
    protocol: str = "socks5"

    @property
    def url(self) -> str:
        """Get full proxy URL."""
        if self.username and self.password:
            return f"{self.protocol}://{self.username}:{self.password}@{self.host}:{self.port}"
        return f"{self.protocol}://{self.host}:{self.port}"

    @property
    def url_masked(self) -> str:
        """Get proxy URL with password masked."""
        if self.username and self.password:
            return f"{self.protocol}://{self.username}:***@{self.host}:{self.port}"
        return f"{self.protocol}://{self.host}:{self.port}"


class ProxyManager:
    """Manages proxy rotation and health checking for API access.

    Features:
    - SOCKS5 proxy support
    - Automatic rotation on failure
    - Residential proxy fallback
    - Mirror endpoint failover
    - Health tracking with auto-blacklisting

    Usage:
        pm = ProxyManager()
        session = pm.get_requests_session()
        response = session.get(url)
    """

    def __init__(self) -> None:
        self._proxies: list[ProxyConfig] = []
        self._residential_proxies: list[ProxyConfig] = []
        self._health: dict[str, ProxyHealth] = {}
        self._current_proxy_idx: int = 0
        self._current_mirror_idx: dict[str, int] = {
            "gamma": 0,
            "clob": 0,
            "data": 0,
        }
        self._lock: asyncio.Lock | None = None  # Lazily initialized in async contexts
        self._last_rotation = time.time()
        self._rotation_interval = 300  # 5 minutes

        self._load_proxies()

    def _get_async_lock(self) -> asyncio.Lock:
        """Get or create async lock for thread-safe operations."""
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    def _load_proxies(self) -> None:
        """Load proxy configurations from settings."""
        settings = get_settings()

        # Primary SOCKS5 proxy from env
        if settings.socks5_proxy_host and settings.socks5_proxy_port:
            primary = ProxyConfig(
                host=settings.socks5_proxy_host,
                port=settings.socks5_proxy_port,
                username=settings.socks5_proxy_user,
                password=settings.socks5_proxy_pass.get_secret_value()
                if settings.socks5_proxy_pass
                else "",
                protocol="socks5",
            )
            self._proxies.append(primary)
            self._health[primary.url] = ProxyHealth(url=primary.url)
            log.info("Loaded primary SOCKS5 proxy", proxy=primary.url_masked)

        # Additional proxies from PROXY_POOL env
        proxy_pool = getattr(settings, "proxy_pool", "")
        if proxy_pool:
            for proxy_str in proxy_pool.split(","):
                proxy_str = proxy_str.strip()
                if not proxy_str:
                    continue
                try:
                    cfg = self._parse_proxy_string(proxy_str)
                    if cfg:
                        self._proxies.append(cfg)
                        self._health[cfg.url] = ProxyHealth(url=cfg.url)
                except Exception as e:
                    log.warning(
                        "Failed to parse proxy", proxy=proxy_str[:20], error=str(e)
                    )

        # Residential proxies (higher cost, use as fallback)
        residential_pool = getattr(settings, "residential_proxy_pool", "")
        if residential_pool:
            for proxy_str in residential_pool.split(","):
                proxy_str = proxy_str.strip()
                if not proxy_str:
                    continue
                try:
                    cfg = self._parse_proxy_string(proxy_str)
                    if cfg:
                        self._residential_proxies.append(cfg)
                        self._health[cfg.url] = ProxyHealth(url=cfg.url)
                except Exception as e:
                    log.warning(
                        "Failed to parse residential proxy",
                        proxy=proxy_str[:20],
                        error=str(e),
                    )

        log.info(
            "Proxy pool loaded",
            socks5_count=len(self._proxies),
            residential_count=len(self._residential_proxies),
        )

    def _parse_proxy_string(self, s: str) -> ProxyConfig | None:
        """Parse proxy string like socks5://user:pass@host:port."""  # pragma: allowlist secret
        if "://" not in s:
            s = f"socks5://{s}"

        protocol, rest = s.split("://", 1)

        if "@" in rest:
            creds, hostport = rest.rsplit("@", 1)
            if ":" in creds:
                username, password = creds.split(":", 1)
            else:
                username, password = creds, ""
        else:
            hostport = rest
            username = password = ""

        host, port_str = hostport.rsplit(":", 1)
        port = int(port_str)

        return ProxyConfig(
            host=host,
            port=port,
            username=username,
            password=password,
            protocol=protocol,
        )

    def get_best_proxy(self, use_residential: bool = False) -> ProxyConfig | None:
        """Get the best available proxy based on health scores."""
        pool = self._residential_proxies if use_residential else self._proxies

        if not pool:
            if not use_residential and self._residential_proxies:
                log.warning("No SOCKS5 proxies available, falling back to residential")
                return self.get_best_proxy(use_residential=True)
            return None

        # Sort by health score
        candidates = []
        for proxy in pool:
            health = self._health.get(proxy.url)
            if health and health.score >= 0:
                candidates.append((proxy, health.score))

        if not candidates:
            # All proxies unhealthy, reset and try first
            log.warning("All proxies unhealthy, resetting health stats")
            for proxy in pool:
                self._health[proxy.url] = ProxyHealth(url=proxy.url)
            return pool[0]

        candidates.sort(key=lambda x: x[1], reverse=True)
        return candidates[0][0]

    def record_success(self, proxy_url: str, latency_ms: float) -> None:
        """Record a successful request through a proxy."""
        if proxy_url in self._health:
            h = self._health[proxy_url]
            h.successes += 1
            h.last_success = time.time()
            h.latency_ms = (h.latency_ms * 0.8) + (
                latency_ms * 0.2
            )  # Exponential moving average
            h.consecutive_failures = 0
            h.is_blocked = False

    def record_failure(self, proxy_url: str, is_geo_blocked: bool = False) -> None:
        """Record a failed request through a proxy."""
        if proxy_url in self._health:
            h = self._health[proxy_url]
            h.failures += 1
            h.last_failure = time.time()
            h.consecutive_failures += 1
            if is_geo_blocked or h.consecutive_failures >= 3:
                h.is_blocked = True
                log.warning(
                    "Proxy marked as blocked",
                    proxy=proxy_url[:30],
                    geo_blocked=is_geo_blocked,
                )

    def get_mirror_endpoint(self, api_type: str) -> str:
        """Get the current mirror endpoint for an API type."""
        mirrors = {
            "gamma": GAMMA_MIRRORS,
            "clob": CLOB_MIRRORS,
            "data": DATA_MIRRORS,
        }.get(api_type, GAMMA_MIRRORS)

        idx = self._current_mirror_idx.get(api_type, 0)
        return mirrors[idx % len(mirrors)]

    def rotate_mirror(self, api_type: str) -> str:
        """Rotate to next mirror endpoint."""
        mirrors = {
            "gamma": GAMMA_MIRRORS,
            "clob": CLOB_MIRRORS,
            "data": DATA_MIRRORS,
        }.get(api_type, GAMMA_MIRRORS)

        current = self._current_mirror_idx.get(api_type, 0)
        self._current_mirror_idx[api_type] = (current + 1) % len(mirrors)
        new_mirror = mirrors[self._current_mirror_idx[api_type]]
        log.info("Rotated API mirror", api_type=api_type, new_mirror=new_mirror)
        return new_mirror

    def get_requests_session(self, retries: int = 3) -> requests.Session:
        """Get a requests.Session configured with proxy and retry logic."""
        session = requests.Session()

        proxy = self.get_best_proxy()
        if proxy:
            session.proxies = {
                "http": proxy.url,
                "https": proxy.url,
            }

        # Configure retries
        retry_strategy = Retry(
            total=retries,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS", "POST"],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        return session

    def get_httpx_client(
        self,
        timeout: float = 30,
        http2: bool = True,
    ) -> httpx.Client:
        """Get an httpx.Client configured with proxy."""
        proxy = self.get_best_proxy()
        proxy_url = proxy.url if proxy else None

        return httpx.Client(
            proxy=proxy_url,
            timeout=timeout,
            http2=http2,
            follow_redirects=True,
        )

    async def get_aiohttp_session(self) -> aiohttp.ClientSession:
        """Get an aiohttp.ClientSession configured with proxy."""
        proxy = self.get_best_proxy()
        proxy_url = proxy.url if proxy else None

        # aiohttp requires connector for SOCKS proxy
        connector = None
        if proxy_url and proxy_url.startswith("socks"):
            try:
                from aiohttp_socks import ProxyConnector

                connector = ProxyConnector.from_url(proxy_url)
            except ImportError:
                log.warning(
                    "aiohttp-socks not installed, proxy will not work with aiohttp"
                )
                proxy_url = None

        return aiohttp.ClientSession(connector=connector)


# Global singleton
_proxy_manager: ProxyManager | None = None


def get_proxy_manager() -> ProxyManager:
    """Get or create the global ProxyManager singleton."""
    global _proxy_manager
    if _proxy_manager is None:
        _proxy_manager = ProxyManager()
    return _proxy_manager


def _make_single_request(
    url: str,
    method: str,
    params: dict | None,
    json_data: dict | None,
    timeout: int,
    proxy_url: str | None,
) -> requests.Response:
    """Make a single HTTP request (internal helper for retry logic)."""
    session = requests.Session()
    if proxy_url:
        session.proxies = {"http": proxy_url, "https": proxy_url}

    if method.upper() == "GET":
        return session.get(url, params=params, timeout=timeout)
    elif method.upper() == "POST":
        return session.post(url, params=params, json=json_data, timeout=timeout)
    else:
        return session.request(
            method, url, params=params, json=json_data, timeout=timeout
        )


def make_proxied_request(
    url: str,
    method: str = "GET",
    params: dict | None = None,
    json_data: dict | None = None,
    timeout: int = 30,
    max_retries: int = 3,
) -> requests.Response:
    """Make an HTTP request through the proxy with automatic failover.

    This is the primary entry point for making proxied API requests.
    It handles:
    - Proxy rotation on failure
    - Mirror endpoint failover
    - Geo-block detection
    - Automatic retries with exponential backoff

    Args:
        url: Target URL
        method: HTTP method (GET, POST, etc.)
        params: Query parameters
        json_data: JSON body for POST requests
        timeout: Request timeout in seconds
        max_retries: Maximum retry attempts

    Returns:
        requests.Response object

    Raises:
        requests.RequestException on failure after all retries
    """
    pm = get_proxy_manager()

    # Detect API type from URL for mirror fallback
    api_type = None
    if "gamma-api" in url or "gamma" in url.lower():
        api_type = "gamma"
    elif "clob" in url.lower():
        api_type = "clob"
    elif "data-api" in url or "data" in url.lower():
        api_type = "data"

    last_error = None

    for attempt in range(max_retries):
        proxy = pm.get_best_proxy(use_residential=(attempt >= max_retries // 2))
        proxy_url = proxy.url if proxy else None

        try:
            start = time.time()

            resp = _make_single_request(
                url, method, params, json_data, timeout, proxy_url
            )

            latency = (time.time() - start) * 1000

            # Check for rate limiting
            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After", "")
                wait_time = (
                    float(retry_after) if retry_after.isdigit() else (2**attempt)
                )
                log.warning(
                    "Rate limited, backing off",
                    url=url[:50],
                    wait_secs=wait_time,
                    attempt=attempt + 1,
                )
                time.sleep(wait_time)
                continue

            # Check for geo-blocking
            if resp.status_code == 403:
                is_geo_block = _is_geo_blocked(resp)
                if proxy_url:
                    pm.record_failure(proxy_url, is_geo_blocked=is_geo_block)
                if is_geo_block and api_type:
                    # Try mirror
                    new_base = pm.rotate_mirror(api_type)
                    url = _replace_base_url(url, new_base, api_type)
                    log.info("Retrying with mirror", url=url[:50], attempt=attempt + 1)
                    continue
                raise requests.exceptions.HTTPError(
                    f"403 Forbidden - Geo-blocked: {is_geo_block}"
                )

            resp.raise_for_status()

            if proxy_url:
                pm.record_success(proxy_url, latency)

            # Log success after retries
            if attempt > 0:
                log.info(
                    "Request succeeded after retry",
                    url=url[:50],
                    attempt=attempt + 1,
                    latency_ms=f"{latency:.0f}",
                )

            return resp

        except (requests.exceptions.RequestException, OSError) as e:
            last_error = e
            if proxy_url:
                pm.record_failure(proxy_url, is_geo_blocked=False)

            # Exponential backoff with jitter
            base_delay = 0.5
            max_delay = 30.0
            delay = min(base_delay * (2**attempt) + random.uniform(0, 0.5), max_delay)

            log.warning(
                "Request failed, retrying with backoff",
                url=url[:50],
                error=str(e)[:100],
                attempt=attempt + 1,
                max_retries=max_retries,
                next_delay_secs=f"{delay:.2f}",
            )
            time.sleep(delay)

    raise last_error or requests.exceptions.RequestException(
        f"All {max_retries} retries failed"
    )


def _is_geo_blocked(response: requests.Response) -> bool:
    """Detect if response indicates geo-blocking."""
    if response.status_code != 403:
        return False

    text = response.text.lower()
    geo_indicators = [
        "geo",
        "region",
        "country",
        "location",
        "access denied",
        "not available in your",
        "restricted",
        "blocked",
        "vpn",
        "cloudflare",
    ]
    return any(ind in text for ind in geo_indicators)


def _replace_base_url(url: str, new_base: str, api_type: str) -> str:
    """Replace the base URL with a mirror."""
    old_bases = {
        "gamma": ["https://gamma-api.polymarket.com"],
        "clob": ["https://clob.polymarket.com"],
        "data": ["https://data-api.polymarket.com"],
    }.get(api_type, [])

    for old in old_bases:
        if url.startswith(old):
            return url.replace(old, new_base, 1)
    return url


# Async version for aiohttp users
async def make_proxied_request_async(
    url: str,
    method: str = "GET",
    params: dict | None = None,
    json_data: dict | None = None,
    timeout: int = 30,
    max_retries: int = 3,
) -> dict | list | str:
    """Async version of make_proxied_request using aiohttp.

    Uses exponential backoff with jitter for retries.
    Creates a single session for all retry attempts to avoid unclosed session warnings.
    """
    pm = get_proxy_manager()
    last_error = None

    # Create session once outside retry loop to avoid unclosed session warnings
    session = await pm.get_aiohttp_session()
    try:
        for attempt in range(max_retries):
            proxy = pm.get_best_proxy(use_residential=(attempt >= max_retries // 2))

            try:
                start = time.time()

                if method.upper() == "GET":
                    async with session.get(url, params=params, timeout=timeout) as resp:
                        latency = (time.time() - start) * 1000

                        # Handle rate limiting
                        if resp.status == 429:
                            retry_after = resp.headers.get("Retry-After", "")
                            wait_time = (
                                float(retry_after)
                                if retry_after.isdigit()
                                else (2**attempt)
                            )
                            log.warning(
                                "Rate limited (async), backing off",
                                url=url[:50],
                                wait_secs=wait_time,
                                attempt=attempt + 1,
                            )
                            await asyncio.sleep(wait_time)
                            continue

                        if resp.status == 403:
                            if proxy:
                                pm.record_failure(proxy.url, is_geo_blocked=True)
                            raise aiohttp.ClientResponseError(
                                resp.request_info, resp.history, status=403
                            )
                        resp.raise_for_status()
                        if proxy:
                            pm.record_success(proxy.url, latency)

                        # Log success after retries
                        if attempt > 0:
                            log.info(
                                "Async request succeeded after retry",
                                url=url[:50],
                                attempt=attempt + 1,
                                latency_ms=f"{latency:.0f}",
                            )

                        return await resp.json()
                else:
                    async with session.request(
                        method, url, params=params, json=json_data, timeout=timeout
                    ) as resp:
                        latency = (time.time() - start) * 1000

                        # Handle rate limiting
                        if resp.status == 429:
                            retry_after = resp.headers.get("Retry-After", "")
                            wait_time = (
                                float(retry_after)
                                if retry_after.isdigit()
                                else (2**attempt)
                            )
                            log.warning(
                                "Rate limited (async), backing off",
                                url=url[:50],
                                wait_secs=wait_time,
                                attempt=attempt + 1,
                            )
                            await asyncio.sleep(wait_time)
                            continue

                        resp.raise_for_status()
                        if proxy:
                            pm.record_success(proxy.url, latency)

                        # Log success after retries
                        if attempt > 0:
                            log.info(
                                "Async request succeeded after retry",
                                url=url[:50],
                                attempt=attempt + 1,
                                latency_ms=f"{latency:.0f}",
                            )

                        return await resp.json()

            except Exception as e:
                last_error = e
                if proxy:
                    pm.record_failure(proxy.url, is_geo_blocked=False)

                # Exponential backoff with jitter
                base_delay = 0.5
                max_delay = 30.0
                delay = min(
                    base_delay * (2**attempt) + random.uniform(0, 0.5), max_delay
                )

                log.warning(
                    "Async request failed, retrying with backoff",
                    url=url[:50],
                    error=str(e)[:100],
                    attempt=attempt + 1,
                    max_retries=max_retries,
                    next_delay_secs=f"{delay:.2f}",
                )
                await asyncio.sleep(delay)

        raise last_error or Exception(f"All {max_retries} async retries failed")
    finally:
        # Always close the session to avoid "Unclosed client session" warnings
        await session.close()


# Test function
def test_proxy_connection() -> dict:
    """Test proxy connectivity and geo-restriction status.

    Returns dict with test results for each API endpoint.
    """
    pm = get_proxy_manager()
    results = {}

    # Test IP detection
    try:
        resp = make_proxied_request("https://api.ipify.org?format=json", timeout=10)
        results["ip"] = resp.json().get("ip", "unknown")
    except Exception as e:
        results["ip"] = f"error: {e}"

    # Test Gamma API
    try:
        resp = make_proxied_request(
            f"{pm.get_mirror_endpoint('gamma')}/markets",
            params={"active": "true", "limit": "1"},
            timeout=15,
        )
        results["gamma_api"] = (
            "OK" if resp.status_code == 200 else f"HTTP {resp.status_code}"
        )
    except Exception as e:
        results["gamma_api"] = f"error: {e}"

    # Test CLOB API
    try:
        resp = make_proxied_request(
            f"{pm.get_mirror_endpoint('clob')}/markets",
            params={"limit": "1"},
            timeout=15,
        )
        results["clob_api"] = (
            "OK" if resp.status_code == 200 else f"HTTP {resp.status_code}"
        )
    except Exception as e:
        results["clob_api"] = f"error: {e}"

    return results
