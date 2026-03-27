"""Tests for the PolygonRpcManager functionality."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
import pytest

from polybot.rpc_manager import (
    PolygonRpcManager,
    ProviderInfo,
    RpcStatusInfo,
    get_rpc_manager,
)


class TestPolygonRpcManager:
    """Test PolygonRpcManager class."""

    def test_manager_initialization_with_defaults(self):
        """Test manager initializes with default RPC URLs.

        When FORCE_ALCHEMY=True (default) and no ALCHEMY_API_KEY is set,
        the manager uses FAST_FALLBACK_RPCS (3 RPCs) instead of DEFAULT_RPCS (5).
        """
        with patch.dict("os.environ", {}, clear=True):
            manager = PolygonRpcManager()
            # With force_alchemy=True (default), uses FAST_FALLBACK_RPCS
            assert len(manager.providers) == len(PolygonRpcManager.FAST_FALLBACK_RPCS)
            assert manager.current_index == 0

    def test_manager_initialization_with_force_alchemy_false(self):
        """Test manager uses DEFAULT_RPCS when force_alchemy=False."""
        with patch.dict("os.environ", {}, clear=True):
            manager = PolygonRpcManager(force_alchemy=False)
            assert len(manager.providers) == len(PolygonRpcManager.DEFAULT_RPCS)
            assert manager.current_index == 0

    def test_manager_initialization_with_env_urls(self):
        """Test manager reads POLYGON_RPC_URLS from environment."""
        test_urls = "https://rpc1.test.com,https://rpc2.test.com"
        with patch.dict("os.environ", {"POLYGON_RPC_URLS": test_urls}, clear=True):
            manager = PolygonRpcManager()
            assert len(manager.providers) == 2
            assert manager.providers[0].url == "https://rpc1.test.com"
            assert manager.providers[1].url == "https://rpc2.test.com"

    def test_manager_initialization_with_single_url_fallback(self):
        """Test manager falls back to POLYGON_RPC_URL if POLYGON_RPC_URLS not set."""
        with patch.dict(
            "os.environ",
            {"POLYGON_RPC_URL": "https://single.rpc.com"},
            clear=True,
        ):
            manager = PolygonRpcManager()
            assert len(manager.providers) == 1
            assert manager.providers[0].url == "https://single.rpc.com"

    def test_manager_initialization_with_custom_urls(self):
        """Test manager accepts custom URLs directly.

        Note: When ALCHEMY_API_KEY is set, Alchemy URL is prepended to custom URLs.
        """
        custom_urls = ["https://custom1.com", "https://custom2.com"]
        manager = PolygonRpcManager(rpc_urls=custom_urls)
        # With ALCHEMY_API_KEY set (required in Railway 2026), expect 3 providers
        # (1 Alchemy + 2 custom) or just 2 if no Alchemy key
        import os

        has_alchemy = bool(os.getenv("ALCHEMY_API_KEY", "").strip())
        expected_count = 3 if has_alchemy else 2
        assert len(manager.providers) == expected_count
        # Last two should always be the custom URLs
        assert manager.providers[-2].url == "https://custom1.com"
        assert manager.providers[-1].url == "https://custom2.com"

    def test_provider_info_defaults(self):
        """Test ProviderInfo dataclass defaults."""
        mock_w3 = MagicMock()
        info = ProviderInfo(w3=mock_w3, url="https://test.com")
        assert info.priority == 1
        assert info.latency_ms == 9999.0
        assert info.last_error_time == 0.0
        assert info.error_count == 0

    def test_rpc_status_info_defaults(self):
        """Test RpcStatusInfo dataclass defaults."""
        info = RpcStatusInfo(url="https://test.com", priority=1, latency_ms=100.0)
        assert info.is_connected is False
        assert info.error_count == 0


class TestPolygonRpcManagerAsync:
    """Async tests for PolygonRpcManager."""

    @pytest.mark.asyncio
    async def test_get_best_provider_success(self):
        """Test get_best_provider returns connected provider."""
        manager = PolygonRpcManager(rpc_urls=["https://test1.com"])

        # Mock the provider's is_connected method
        manager.providers[0].w3.is_connected = AsyncMock(return_value=True)

        w3 = await manager.get_best_provider(timeout=1.0)
        assert w3 is manager.providers[0].w3

    @pytest.mark.asyncio
    async def test_get_best_provider_failover(self):
        """Test get_best_provider fails over to next provider on error."""
        manager = PolygonRpcManager(
            rpc_urls=["https://fail.com", "https://success.com"]
        )

        # First provider fails, second succeeds
        manager.providers[0].w3.is_connected = AsyncMock(side_effect=Exception("down"))
        manager.providers[1].w3.is_connected = AsyncMock(return_value=True)

        w3 = await manager.get_best_provider(timeout=1.0)
        assert w3 is manager.providers[1].w3
        assert manager.providers[0].error_count == 1

    @pytest.mark.asyncio
    async def test_get_best_provider_all_fail(self):
        """Test get_best_provider raises when all providers fail."""
        manager = PolygonRpcManager(rpc_urls=["https://fail1.com", "https://fail2.com"])

        manager.providers[0].w3.is_connected = AsyncMock(side_effect=Exception("down"))
        manager.providers[1].w3.is_connected = AsyncMock(side_effect=Exception("down"))

        with pytest.raises(Exception, match="All Polygon RPCs are unavailable"):
            await manager.get_best_provider(timeout=1.0)

    @pytest.mark.asyncio
    async def test_rank_by_latency(self):
        """Test rank_by_latency sorts providers by response time."""
        with patch.dict("os.environ", {"ALCHEMY_API_KEY": ""}, clear=False):
            manager = PolygonRpcManager(
                rpc_urls=["https://slow.com", "https://fast.com", "https://medium.com"]
            )

            # Create mock eth modules that return coroutines when block_number is accessed
            class MockSlowEth:
                @property
                def block_number(self):
                    async def slow():
                        await asyncio.sleep(0.1)
                        return 1000

                    return slow()

            class MockFastEth:
                @property
                def block_number(self):
                    async def fast():
                        return 1000

                    return fast()

            class MockMediumEth:
                @property
                def block_number(self):
                    async def medium():
                        await asyncio.sleep(0.05)
                        return 1000

                    return medium()

            # Replace eth modules
            manager.providers[0].w3.eth = MockSlowEth()
            manager.providers[1].w3.eth = MockFastEth()
            manager.providers[2].w3.eth = MockMediumEth()

            await manager.rank_by_latency(timeout=1.0)

            # Providers should be sorted: fast < medium < slow
            urls = [p.url for p in manager.providers]
            assert urls[0] == "https://fast.com"
            assert urls[1] == "https://medium.com"
            assert urls[2] == "https://slow.com"

    @pytest.mark.asyncio
    async def test_rank_by_latency_removes_dead_rpcs(self):
        """Test rank_by_latency removes dead RPCs from the list."""
        with patch.dict("os.environ", {"ALCHEMY_API_KEY": ""}, clear=False):
            manager = PolygonRpcManager(
                rpc_urls=["https://dead.com", "https://alive.com"]
            )

            # Mock: first is dead (raises exception), second is alive
            class MockDeadEth:
                @property
                def block_number(self):
                    async def fail():
                        raise Exception("Connection refused")

                    return fail()

            class MockAliveEth:
                @property
                def block_number(self):
                    async def success():
                        return 1000

                    return success()

            manager.providers[0].w3.eth = MockDeadEth()
            manager.providers[1].w3.eth = MockAliveEth()

            await manager.rank_by_latency(timeout=1.0)

            # Only the alive RPC should remain
            assert len(manager.providers) == 1
            assert manager.providers[0].url == "https://alive.com"

    @pytest.mark.asyncio
    async def test_rank_by_latency_fallback_when_all_fail(self):
        """Test rank_by_latency adds fallback RPC when all providers fail."""
        with patch.dict("os.environ", {"ALCHEMY_API_KEY": ""}, clear=False):
            manager = PolygonRpcManager(
                rpc_urls=["https://dead1.com", "https://dead2.com"]
            )

            # Mock: all RPCs fail
            class MockDeadEth:
                @property
                def block_number(self):
                    async def fail():
                        raise Exception("Connection refused")

                    return fail()

            manager.providers[0].w3.eth = MockDeadEth()
            manager.providers[1].w3.eth = MockDeadEth()

            await manager.rank_by_latency(timeout=1.0)

            # Should have fallback RPC
            assert len(manager.providers) == 1
            assert manager.providers[0].url == PolygonRpcManager.FALLBACK_RPC

    @pytest.mark.asyncio
    async def test_get_status(self):
        """Test get_status returns correct status info."""
        with patch.dict("os.environ", {"ALCHEMY_API_KEY": ""}, clear=False):
            manager = PolygonRpcManager(rpc_urls=["https://test.com"])
            manager.providers[0].latency_ms = 50.0
            manager.providers[0].error_count = 2
            manager.providers[0].w3.is_connected = AsyncMock(return_value=True)

            status_list = await manager.get_status()

            assert len(status_list) == 1
            assert status_list[0].url == "https://test.com"
            assert status_list[0].priority == 1
            assert status_list[0].latency_ms == 50.0
            assert status_list[0].is_connected is True
            assert status_list[0].error_count == 2


class TestGetRpcManager:
    """Test get_rpc_manager global instance function."""

    def test_get_rpc_manager_singleton(self):
        """Test get_rpc_manager returns same instance."""
        # Reset global instance
        import polybot.rpc_manager as rpc_module

        rpc_module._rpc_manager = None

        with patch.dict(
            "os.environ", {"POLYGON_RPC_URLS": "https://test.com"}, clear=True
        ):
            manager1 = get_rpc_manager()
            manager2 = get_rpc_manager()
            assert manager1 is manager2


class TestAlchemyConnection:
    """Tests for Alchemy-specific connection testing."""

    @pytest.mark.asyncio
    async def test_alchemy_connection_success(self):
        """Test successful Alchemy connection test."""
        with patch.dict(
            "os.environ",
            {"ALCHEMY_API_KEY": "test-key-12345"},
            clear=True,
        ):
            manager = PolygonRpcManager()

            # Find and mock Alchemy provider's eth.block_number
            for provider in manager.providers:
                if provider.is_alchemy:
                    # Create a mock eth property that returns block_number
                    class MockEth:
                        @property
                        def block_number(self):
                            async def get_block():
                                return 42000000

                            return get_block()

                    provider.w3.eth = MockEth()
                    break

            success, block_number, error = await manager.test_alchemy_connection(
                timeout=5.0
            )

            assert success is True
            assert block_number == 42000000
            assert error is None
            assert manager.alchemy_connected is True
            assert manager.connected_block_number == 42000000

    @pytest.mark.asyncio
    async def test_alchemy_connection_failure(self):
        """Test failed Alchemy connection test."""
        with patch.dict(
            "os.environ",
            {"ALCHEMY_API_KEY": "invalid-key"},
            clear=True,
        ):
            manager = PolygonRpcManager()

            # Find and mock Alchemy provider to fail
            for provider in manager.providers:
                if provider.is_alchemy:

                    class MockEth:
                        @property
                        def block_number(self):
                            async def fail():
                                raise Exception("Invalid API key")

                            return fail()

                    provider.w3.eth = MockEth()
                    break

            success, block_number, error = await manager.test_alchemy_connection(
                timeout=5.0
            )

            assert success is False
            assert block_number is None
            assert error is not None
            assert "Connection failed" in error
            assert manager.alchemy_connected is False
            assert manager.alchemy_connection_error is not None

    @pytest.mark.asyncio
    async def test_alchemy_connection_not_configured(self):
        """Test Alchemy connection test when not configured."""
        with patch.dict("os.environ", {}, clear=True):
            manager = PolygonRpcManager(force_alchemy=False)

            success, block_number, error = await manager.test_alchemy_connection()

            assert success is False
            assert block_number is None
            assert error == "Alchemy nicht konfiguriert"

    @pytest.mark.asyncio
    async def test_alchemy_configured_but_provider_missing(self):
        """Test when alchemy_configured is True but no provider exists.

        This edge case can occur if the Alchemy provider was removed
        during rank_by_latency due to connection issues.
        """
        with patch.dict(
            "os.environ",
            {"ALCHEMY_API_KEY": "test-key"},
            clear=True,
        ):
            manager = PolygonRpcManager()
            # Manually remove all Alchemy providers to simulate edge case
            manager.providers = [p for p in manager.providers if not p.is_alchemy]
            # Keep alchemy_configured True to trigger the edge case path
            manager.alchemy_configured = True

            success, block_number, error = await manager.test_alchemy_connection()

            assert success is False
            assert block_number is None
            assert error == "Alchemy konfiguriert aber Provider nicht gefunden"
            assert manager.alchemy_connection_error == error

    def test_get_alchemy_status_with_connection_error(self):
        """Test get_alchemy_status returns connection error message."""
        with patch.dict(
            "os.environ",
            {"ALCHEMY_API_KEY": "test-key"},
            clear=True,
        ):
            manager = PolygonRpcManager()
            manager.alchemy_connection_error = (
                "Alchemy Connection failed – Timeout. Key prüfen!"
            )

            status = manager.get_alchemy_status()

            assert status["configured"] is True
            assert status["connected"] is False
            assert (
                status["connection_error"]
                == "Alchemy Connection failed – Timeout. Key prüfen!"
            )
            assert "Key prüfen" in status["message"]
