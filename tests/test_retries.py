"""Tests for polybot.retries module."""

from unittest.mock import Mock

import pytest

from polybot.retries import (
    retry_api_call,
    retry_api_call_async,
    retry_sync_with_backoff,
    retry_async_with_backoff,
    RateLimitError,
    handle_rate_limit,
    NETWORK_EXCEPTIONS,
)


class TestRetryDecorators:
    """Tests for retry decorator functions."""

    def test_retry_api_call_success_first_try(self):
        """Test that successful calls don't retry."""
        call_count = 0

        @retry_api_call(max_attempts=3)
        def successful_fn():
            nonlocal call_count
            call_count += 1
            return "success"

        result = successful_fn()
        assert result == "success"
        assert call_count == 1

    def test_retry_api_call_success_after_failures(self):
        """Test that function succeeds after retries."""
        call_count = 0

        @retry_api_call(max_attempts=3, min_wait=0.01, max_wait=0.1)
        def eventually_succeeds():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ConnectionError("Simulated failure")
            return "success"

        result = eventually_succeeds()
        assert result == "success"
        assert call_count == 3

    def test_retry_api_call_exhausted(self):
        """Test that function raises after all retries exhausted."""
        call_count = 0

        @retry_api_call(max_attempts=3, min_wait=0.01, max_wait=0.1)
        def always_fails():
            nonlocal call_count
            call_count += 1
            raise ConnectionError("Always fails")

        with pytest.raises(ConnectionError):
            always_fails()

        assert call_count == 3


class TestAsyncRetryDecorators:
    """Tests for async retry decorator functions."""

    @pytest.mark.asyncio
    async def test_retry_api_call_async_success(self):
        """Test async successful call."""
        call_count = 0

        @retry_api_call_async(max_attempts=3)
        async def async_success():
            nonlocal call_count
            call_count += 1
            return "async success"

        result = await async_success()
        assert result == "async success"
        assert call_count == 1

    @pytest.mark.asyncio
    async def test_retry_api_call_async_with_retries(self):
        """Test async function with retries."""
        call_count = 0

        @retry_api_call_async(max_attempts=3, min_wait=0.01, max_wait=0.1)
        async def async_eventually_succeeds():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise TimeoutError("Simulated timeout")
            return "async success after retry"

        result = await async_eventually_succeeds()
        assert result == "async success after retry"
        assert call_count == 2


class TestManualRetry:
    """Tests for manual retry utility functions."""

    def test_retry_sync_with_backoff_success(self):
        """Test manual sync retry succeeds."""

        def fn():
            return "result"

        result = retry_sync_with_backoff(fn, max_attempts=3, base_delay=0.01)
        assert result == "result"

    def test_retry_sync_with_backoff_exhausted(self):
        """Test manual sync retry exhausted."""

        def fn():
            raise OSError("Always fails")

        with pytest.raises(OSError):
            retry_sync_with_backoff(fn, max_attempts=2, base_delay=0.01)

    @pytest.mark.asyncio
    async def test_retry_async_with_backoff_success(self):
        """Test manual async retry succeeds."""

        async def fn():
            return "async result"

        result = await retry_async_with_backoff(fn, max_attempts=3, base_delay=0.01)
        assert result == "async result"


class TestRateLimitError:
    """Tests for RateLimitError exception."""

    def test_rate_limit_error_with_retry_after(self):
        """Test RateLimitError stores retry_after."""
        err = RateLimitError("Rate limited", retry_after=30.0)
        assert str(err) == "Rate limited"
        assert err.retry_after == 30.0

    def test_rate_limit_error_without_retry_after(self):
        """Test RateLimitError without retry_after."""
        err = RateLimitError("Rate limited")
        assert err.retry_after is None


class TestHandleRateLimit:
    """Tests for handle_rate_limit function."""

    def test_handle_rate_limit_429(self):
        """Test that 429 raises RateLimitError."""
        mock_response = Mock()
        mock_response.status_code = 429
        mock_response.status = None  # requests uses status_code
        mock_response.headers = {"Retry-After": "60"}

        with pytest.raises(RateLimitError) as exc_info:
            handle_rate_limit(mock_response)

        assert exc_info.value.retry_after == 60.0

    def test_handle_rate_limit_200(self):
        """Test that 200 doesn't raise."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.status = None
        mock_response.headers = {}

        # Should not raise
        handle_rate_limit(mock_response)

    def test_handle_rate_limit_aiohttp_style(self):
        """Test with aiohttp-style status attribute."""
        mock_response = Mock()
        mock_response.status = 429
        mock_response.status_code = None  # aiohttp uses .status
        mock_response.headers = {}

        with pytest.raises(RateLimitError):
            handle_rate_limit(mock_response)


class TestNetworkExceptions:
    """Tests for NETWORK_EXCEPTIONS tuple."""

    def test_includes_base_exceptions(self):
        """Test that base network exceptions are included."""
        assert ConnectionError in NETWORK_EXCEPTIONS
        assert TimeoutError in NETWORK_EXCEPTIONS
        assert OSError in NETWORK_EXCEPTIONS

    def test_includes_requests_exceptions(self):
        """Test that requests exceptions are included if available."""
        try:
            import requests

            assert requests.exceptions.RequestException in NETWORK_EXCEPTIONS
        except ImportError:
            pass  # Skip if requests not installed
