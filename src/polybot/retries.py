"""Retry utilities with exponential backoff using tenacity.

Provides reusable decorators for API calls, blockchain operations, and
other network-dependent functions. Uses structured logging to track
retry attempts for observability.

Usage:
    from polybot.retries import retry_api_call, retry_blockchain_call

    @retry_api_call
    def fetch_markets():
        return requests.get("https://api.example.com/markets")

    @retry_blockchain_call
    async def submit_transaction():
        return await web3.eth.send_transaction(...)
"""

from __future__ import annotations

import asyncio
from typing import Any, Callable, TypeVar

from tenacity import (
    RetryCallState,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
    wait_exponential_jitter,
)

from polybot.logging_setup import get_logger

log = get_logger(__name__)

# Type variable for generic return type
T = TypeVar("T")


# ============================================================================
# Logging callbacks for tenacity
# ============================================================================


def log_retry_attempt(retry_state: RetryCallState) -> None:
    """Log each retry attempt with structured data."""
    exc = retry_state.outcome.exception() if retry_state.outcome else None
    exc_type = type(exc).__name__ if exc else "unknown"
    exc_msg = str(exc)[:100] if exc else ""

    log.warning(
        "Retry attempt",
        attempt=retry_state.attempt_number,
        fn=retry_state.fn.__name__ if retry_state.fn else "unknown",
        error_type=exc_type,
        error_msg=exc_msg,
        wait_secs=f"{retry_state.next_action.sleep if retry_state.next_action else 0:.2f}",
    )


def log_retry_exhausted(retry_state: RetryCallState) -> None:
    """Log when all retries are exhausted."""
    exc = retry_state.outcome.exception() if retry_state.outcome else None
    exc_type = type(exc).__name__ if exc else "unknown"

    log.error(
        "Retries exhausted",
        fn=retry_state.fn.__name__ if retry_state.fn else "unknown",
        total_attempts=retry_state.attempt_number,
        final_error=exc_type,
        final_msg=str(exc)[:200] if exc else "",
    )


def log_retry_success(retry_state: RetryCallState) -> None:
    """Log when retry succeeds after failures."""
    if retry_state.attempt_number > 1:
        log.info(
            "Retry succeeded",
            fn=retry_state.fn.__name__ if retry_state.fn else "unknown",
            attempt=retry_state.attempt_number,
            total_time=f"{retry_state.seconds_since_start:.2f}s",
        )


# ============================================================================
# Exception types for retry conditions
# ============================================================================

# Network errors that should trigger retry
NETWORK_EXCEPTIONS = (
    ConnectionError,
    TimeoutError,
    OSError,
)

# Import optional network exception types
try:
    import requests

    NETWORK_EXCEPTIONS = (*NETWORK_EXCEPTIONS, requests.exceptions.RequestException)
except ImportError:
    pass

try:
    import aiohttp

    NETWORK_EXCEPTIONS = (*NETWORK_EXCEPTIONS, aiohttp.ClientError)
except ImportError:
    pass

try:
    import httpx

    NETWORK_EXCEPTIONS = (*NETWORK_EXCEPTIONS, httpx.HTTPError)
except ImportError:
    pass


# ============================================================================
# Retry decorators
# ============================================================================


def retry_api_call(
    max_attempts: int = 3,
    min_wait: float = 0.5,
    max_wait: float = 30.0,
    jitter: bool = True,
):
    """Decorator for retrying API calls with exponential backoff.

    Uses tenacity with:
    - Exponential backoff (0.5s, 1s, 2s, 4s, ...) with optional jitter
    - Retries on network errors and rate limits (429)
    - Structured logging for observability

    Args:
        max_attempts: Maximum number of attempts (default 3)
        min_wait: Minimum wait time between retries in seconds (default 0.5)
        max_wait: Maximum wait time between retries in seconds (default 30)
        jitter: Whether to add random jitter to wait times (default True)

    Example:
        @retry_api_call(max_attempts=5)
        def fetch_data():
            return requests.get(url)
    """
    wait_strategy = (
        wait_exponential_jitter(initial=min_wait, max=max_wait, jitter=min_wait)
        if jitter
        else wait_exponential(multiplier=1, min=min_wait, max=max_wait)
    )

    return retry(
        stop=stop_after_attempt(max_attempts),
        wait=wait_strategy,
        retry=retry_if_exception_type(NETWORK_EXCEPTIONS),
        before_sleep=log_retry_attempt,
        after=log_retry_success,
        reraise=True,
    )


def retry_blockchain_call(
    max_attempts: int = 5,
    min_wait: float = 1.0,
    max_wait: float = 60.0,
):
    """Decorator for retrying blockchain/web3 calls with longer backoff.

    Blockchain operations often need longer wait times due to:
    - Network congestion
    - RPC node rate limits
    - Transaction confirmation times

    Args:
        max_attempts: Maximum number of attempts (default 5)
        min_wait: Minimum wait time between retries in seconds (default 1.0)
        max_wait: Maximum wait time between retries in seconds (default 60)
    """
    return retry(
        stop=stop_after_attempt(max_attempts),
        wait=wait_exponential_jitter(
            initial=min_wait, max=max_wait, jitter=min_wait / 2
        ),
        retry=retry_if_exception_type(
            NETWORK_EXCEPTIONS + (Exception,)
        ),  # Broader retry
        before_sleep=log_retry_attempt,
        after=log_retry_success,
        reraise=True,
    )


# ============================================================================
# Async retry decorators
# ============================================================================


def retry_api_call_async(
    max_attempts: int = 3,
    min_wait: float = 0.5,
    max_wait: float = 30.0,
    jitter: bool = True,
):
    """Async version of retry_api_call.

    Example:
        @retry_api_call_async(max_attempts=5)
        async def fetch_data():
            async with aiohttp.ClientSession() as session:
                return await session.get(url)
    """
    wait_strategy = (
        wait_exponential_jitter(initial=min_wait, max=max_wait, jitter=min_wait)
        if jitter
        else wait_exponential(multiplier=1, min=min_wait, max=max_wait)
    )

    return retry(
        stop=stop_after_attempt(max_attempts),
        wait=wait_strategy,
        retry=retry_if_exception_type(NETWORK_EXCEPTIONS),
        before_sleep=log_retry_attempt,
        after=log_retry_success,
        reraise=True,
    )


def retry_blockchain_call_async(
    max_attempts: int = 5,
    min_wait: float = 1.0,
    max_wait: float = 60.0,
):
    """Async version of retry_blockchain_call."""
    return retry(
        stop=stop_after_attempt(max_attempts),
        wait=wait_exponential_jitter(
            initial=min_wait, max=max_wait, jitter=min_wait / 2
        ),
        retry=retry_if_exception_type(NETWORK_EXCEPTIONS + (Exception,)),
        before_sleep=log_retry_attempt,
        after=log_retry_success,
        reraise=True,
    )


# ============================================================================
# Manual retry utilities
# ============================================================================


async def retry_async_with_backoff(
    fn: Callable[..., Any],
    *args: Any,
    max_attempts: int = 3,
    base_delay: float = 0.5,
    max_delay: float = 30.0,
    exceptions: tuple = NETWORK_EXCEPTIONS,
    **kwargs: Any,
) -> Any:
    """Manual async retry with exponential backoff.

    For cases where you can't use a decorator.

    Args:
        fn: Async function to call
        *args: Positional arguments for fn
        max_attempts: Maximum number of attempts
        base_delay: Initial delay in seconds
        max_delay: Maximum delay in seconds
        exceptions: Tuple of exception types to retry on
        **kwargs: Keyword arguments for fn

    Returns:
        Result of fn

    Raises:
        Last exception if all attempts fail
    """
    last_exception = None

    for attempt in range(1, max_attempts + 1):
        try:
            return await fn(*args, **kwargs)
        except exceptions as e:
            last_exception = e
            if attempt == max_attempts:
                log.error(
                    "Async retry exhausted",
                    fn=fn.__name__,
                    total_attempts=attempt,
                    final_error=type(e).__name__,
                )
                raise

            delay = min(base_delay * (2 ** (attempt - 1)), max_delay)
            log.warning(
                "Async retry attempt",
                fn=fn.__name__,
                attempt=attempt,
                error=str(e)[:100],
                next_delay=f"{delay:.2f}s",
            )
            await asyncio.sleep(delay)

    raise last_exception


def retry_sync_with_backoff(
    fn: Callable[..., T],
    *args: Any,
    max_attempts: int = 3,
    base_delay: float = 0.5,
    max_delay: float = 30.0,
    exceptions: tuple = NETWORK_EXCEPTIONS,
    **kwargs: Any,
) -> T:
    """Manual sync retry with exponential backoff.

    For cases where you can't use a decorator.

    Args:
        fn: Function to call
        *args: Positional arguments for fn
        max_attempts: Maximum number of attempts
        base_delay: Initial delay in seconds
        max_delay: Maximum delay in seconds
        exceptions: Tuple of exception types to retry on
        **kwargs: Keyword arguments for fn

    Returns:
        Result of fn

    Raises:
        Last exception if all attempts fail
    """
    import time

    last_exception = None

    for attempt in range(1, max_attempts + 1):
        try:
            return fn(*args, **kwargs)
        except exceptions as e:
            last_exception = e
            if attempt == max_attempts:
                log.error(
                    "Sync retry exhausted",
                    fn=fn.__name__,
                    total_attempts=attempt,
                    final_error=type(e).__name__,
                )
                raise

            delay = min(base_delay * (2 ** (attempt - 1)), max_delay)
            log.warning(
                "Sync retry attempt",
                fn=fn.__name__,
                attempt=attempt,
                error=str(e)[:100],
                next_delay=f"{delay:.2f}s",
            )
            time.sleep(delay)

    raise last_exception  # type: ignore


# ============================================================================
# Rate limit aware retry
# ============================================================================


class RateLimitError(Exception):
    """Raised when rate limited by an API."""

    def __init__(self, message: str, retry_after: float | None = None):
        super().__init__(message)
        self.retry_after = retry_after


def handle_rate_limit(response) -> None:
    """Check for rate limiting and raise RateLimitError if detected.

    Works with both requests.Response and aiohttp.ClientResponse.

    Args:
        response: HTTP response object

    Raises:
        RateLimitError: If response indicates rate limiting
    """
    status = getattr(response, "status", None) or getattr(response, "status_code", None)

    if status == 429:
        # Try to get Retry-After header
        retry_after = None
        headers = getattr(response, "headers", {})
        if headers:
            retry_str = headers.get("Retry-After") or headers.get("retry-after")
            if retry_str:
                try:
                    retry_after = float(retry_str)
                except (ValueError, TypeError):
                    pass

        raise RateLimitError(
            "Rate limited (HTTP 429)",
            retry_after=retry_after,
        )
