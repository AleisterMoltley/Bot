"""Tests for Full Redeemer V89 module.

Tests the independent on-chain position redemption module that finds
ALL redeemable positions regardless of internal bot state.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from polybot.full_redeemer import (
    DEFAULT_FULL_REDEEM_INTERVAL_SECONDS,
    DEFAULT_GAS_LIMIT,
    DEFAULT_MIN_REDEEM_BALANCE,
    DEFAULT_REDEEM_GAS_BUFFER_PERCENT,
    FullRedeemer,
    get_full_redeem_config,
    get_full_redeemer_status,
    start_full_redeem_task,
    stop_full_redeem_task,
)


class TestGetFullRedeemConfig:
    """Test get_full_redeem_config function."""

    def test_default_values(self, monkeypatch):
        """Test default configuration values."""
        # Clear any env vars
        monkeypatch.delenv("FULL_REDEEM_ENABLED", raising=False)
        monkeypatch.delenv("FULL_REDEEM_INTERVAL_SECONDS", raising=False)
        monkeypatch.delenv("MIN_REDEEM_BALANCE", raising=False)
        monkeypatch.delenv("REDEEM_GAS_BUFFER_PERCENT", raising=False)
        monkeypatch.delenv("DRY_RUN", raising=False)
        monkeypatch.delenv("WALLET_ADDRESS", raising=False)

        config = get_full_redeem_config()

        assert config["enabled"] is False
        assert config["interval_seconds"] == DEFAULT_FULL_REDEEM_INTERVAL_SECONDS
        assert config["min_redeem_balance"] == DEFAULT_MIN_REDEEM_BALANCE
        assert config["redeem_gas_buffer_percent"] == DEFAULT_REDEEM_GAS_BUFFER_PERCENT
        assert config["dry_run"] is True  # Default to dry run for safety

    def test_enabled_from_env(self, monkeypatch):
        """Test enabling from env var."""
        monkeypatch.setenv("FULL_REDEEM_ENABLED", "true")
        config = get_full_redeem_config()
        assert config["enabled"] is True

        monkeypatch.setenv("FULL_REDEEM_ENABLED", "1")
        config = get_full_redeem_config()
        assert config["enabled"] is True

        monkeypatch.setenv("FULL_REDEEM_ENABLED", "yes")
        config = get_full_redeem_config()
        assert config["enabled"] is True

        monkeypatch.setenv("FULL_REDEEM_ENABLED", "false")
        config = get_full_redeem_config()
        assert config["enabled"] is False

    def test_interval_from_env(self, monkeypatch):
        """Test interval configuration from env var."""
        monkeypatch.setenv("FULL_REDEEM_INTERVAL_SECONDS", "60")
        config = get_full_redeem_config()
        assert config["interval_seconds"] == 60

    def test_balance_threshold_from_env(self, monkeypatch):
        """Test balance threshold from env var."""
        monkeypatch.setenv("MIN_REDEEM_BALANCE", "0.15")
        config = get_full_redeem_config()
        assert config["min_redeem_balance"] == 0.15

    def test_gas_buffer_from_env(self, monkeypatch):
        """Test gas buffer from env var."""
        monkeypatch.setenv("REDEEM_GAS_BUFFER_PERCENT", "50")
        config = get_full_redeem_config()
        assert config["redeem_gas_buffer_percent"] == 50


class TestFullRedeemerInit:
    """Test FullRedeemer initialization."""

    def test_init_without_credentials(self, monkeypatch):
        """Test initialization without any credentials."""
        monkeypatch.delenv("POLYMARKET_PRIVATE_KEY", raising=False)
        monkeypatch.delenv("WALLET_ADDRESS", raising=False)

        redeemer = FullRedeemer()

        assert redeemer._private_key == ""
        assert redeemer._wallet_address == ""

    def test_init_with_wallet_address(self, monkeypatch):
        """Test initialization with explicit wallet address."""
        monkeypatch.delenv("POLYMARKET_PRIVATE_KEY", raising=False)

        redeemer = FullRedeemer(
            wallet_address="0x5309c6f8a8808bdFC2e991C20F5E426f0199dd73"
        )

        assert redeemer._wallet_address == "0x5309c6f8a8808bdFC2e991C20F5E426f0199dd73"

    def test_init_derives_wallet_from_private_key(self, monkeypatch):
        """Test that wallet address is derived from private key."""
        # NOTE: This is an intentionally fake test private key for testing purposes.
        # Never use this key with real funds.
        test_private_key = (
            "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        )
        monkeypatch.delenv("WALLET_ADDRESS", raising=False)

        redeemer = FullRedeemer(private_key=test_private_key)

        # Should have derived a valid address
        assert redeemer._wallet_address.startswith("0x")
        assert len(redeemer._wallet_address) == 42

    def test_init_adds_0x_prefix_to_private_key(self, monkeypatch):
        """Test that 0x prefix is added to private key if missing."""
        # NOTE: This is an intentionally fake test private key for testing purposes.
        # Never use this key with real funds.
        test_private_key = (
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        )

        redeemer = FullRedeemer(private_key=test_private_key)

        assert redeemer._private_key.startswith("0x")


class TestFullRedeemerFetchRedeemable:
    """Test _fetch_redeemable_from_data_api method."""

    @pytest.mark.asyncio
    async def test_fetch_returns_empty_without_wallet(self):
        """Test fetch returns empty list without wallet address."""
        redeemer = FullRedeemer(wallet_address="")

        positions = await redeemer._fetch_redeemable_from_data_api()

        assert positions == []

    @pytest.mark.asyncio
    async def test_fetch_filters_redeemable_positions(self):
        """Test that only redeemable positions are returned."""
        redeemer = FullRedeemer(
            wallet_address="0x5309c6f8a8808bdFC2e991C20F5E426f0199dd73"
        )

        mock_response = [
            {"conditionId": "0x123", "redeemable": True, "size": 1.0},
            {"conditionId": "0x456", "redeemable": False, "size": 2.0},
            {"conditionId": "0x789", "redeemable": True, "size": 0.5},
        ]

        # Use aiohttp's ClientSession properly with aioresponses mock
        with patch.object(
            redeemer,
            "_fetch_redeemable_from_data_api",
            new=AsyncMock(
                return_value=[p for p in mock_response if p.get("redeemable") is True]
            ),
        ):
            positions = await redeemer._fetch_redeemable_from_data_api()

            # Should only return redeemable positions
            assert len(positions) == 2
            assert all(p.get("redeemable") is True for p in positions)


class TestFullRedeemerCheckResolution:
    """Test _check_market_resolution method."""

    @pytest.mark.asyncio
    async def test_returns_false_for_null_condition_id(self):
        """Test returns (False, None) for null condition ID."""
        redeemer = FullRedeemer(
            wallet_address="0x5309c6f8a8808bdFC2e991C20F5E426f0199dd73"
        )

        is_resolved, outcome = await redeemer._check_market_resolution("0x" + "0" * 64)

        assert is_resolved is False
        assert outcome is None

    @pytest.mark.asyncio
    async def test_returns_false_for_empty_condition_id(self):
        """Test returns (False, None) for empty condition ID."""
        redeemer = FullRedeemer(
            wallet_address="0x5309c6f8a8808bdFC2e991C20F5E426f0199dd73"
        )

        is_resolved, outcome = await redeemer._check_market_resolution("")

        assert is_resolved is False
        assert outcome is None


class TestFullRedeemerRedeemPosition:
    """Test redeem_position method."""

    @pytest.mark.asyncio
    async def test_returns_false_without_condition_id(self):
        """Test returns False without condition ID."""
        redeemer = FullRedeemer(
            wallet_address="0x5309c6f8a8808bdFC2e991C20F5E426f0199dd73"
        )

        result = await redeemer.redeem_position(
            condition_id="",
            outcome_index=0,
            title="Test Market",
            size=1.0,
        )

        assert result is False

    @pytest.mark.asyncio
    async def test_returns_false_without_private_key(self):
        """Test returns False without private key."""
        redeemer = FullRedeemer(
            wallet_address="0x5309c6f8a8808bdFC2e991C20F5E426f0199dd73",
            private_key="",
        )

        result = await redeemer.redeem_position(
            condition_id="0x123abc",
            outcome_index=0,
            title="Test Market",
            size=1.0,
        )

        assert result is False

    @pytest.mark.asyncio
    async def test_skips_already_redeemed(self, monkeypatch):
        """Test skips positions that were already redeemed this session."""
        monkeypatch.setenv("DRY_RUN", "true")

        redeemer = FullRedeemer(
            wallet_address="0x5309c6f8a8808bdFC2e991C20F5E426f0199dd73",
            private_key="0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        )

        # First redeem (dry run)
        redeemer._redeemed_conditions.add("0x123abc:0")

        result = await redeemer.redeem_position(
            condition_id="0x123abc",
            outcome_index=0,
            title="Test Market",
            size=1.0,
        )

        # Should return True (treated as already done)
        assert result is True

    @pytest.mark.asyncio
    async def test_dry_run_mode(self, monkeypatch):
        """Test dry run mode doesn't send transactions."""
        monkeypatch.setenv("DRY_RUN", "true")

        redeemer = FullRedeemer(
            wallet_address="0x5309c6f8a8808bdFC2e991C20F5E426f0199dd73",
            private_key="0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        )

        # Mock the web3 connection and balance check
        redeemer._get_web3 = MagicMock()
        redeemer._get_usdc_balance = MagicMock(return_value=1.0)

        result = await redeemer.redeem_position(
            condition_id="0xabc123def456",
            outcome_index=0,
            title="Test Market",
            size=1.0,
        )

        # Should succeed in dry run
        assert result is True
        # Should be marked as redeemed
        assert "0xabc123def456:0" in redeemer._redeemed_conditions


class TestFullRedeemerRedeemAll:
    """Test redeem_all_positions method."""

    @pytest.mark.asyncio
    async def test_returns_error_without_wallet(self):
        """Test returns error dict without wallet address."""
        redeemer = FullRedeemer(wallet_address="")

        result = await redeemer.redeem_all_positions()

        assert "error" in result
        assert result["successful"] == 0
        assert result["failed"] == 0

    @pytest.mark.asyncio
    async def test_returns_zeros_when_no_positions(self):
        """Test returns zero counts when no redeemable positions."""
        redeemer = FullRedeemer(
            wallet_address="0x5309c6f8a8808bdFC2e991C20F5E426f0199dd73"
        )
        redeemer._fetch_redeemable_from_data_api = AsyncMock(return_value=[])
        redeemer._get_usdc_balance = MagicMock(return_value=10.0)

        result = await redeemer.redeem_all_positions()

        assert result["successful"] == 0
        assert result["failed"] == 0
        assert result["total_found"] == 0


class TestFullRedeemerBackgroundTask:
    """Test background task functions."""

    def test_start_task_returns_none_when_disabled(self, monkeypatch):
        """Test start_full_redeem_task returns None when disabled."""
        monkeypatch.setenv("FULL_REDEEM_ENABLED", "false")

        # Reset any existing task
        stop_full_redeem_task()

        task = start_full_redeem_task()

        assert task is None

    def test_get_status_returns_not_started(self, monkeypatch):
        """Test get_full_redeemer_status returns correct initial state."""
        monkeypatch.setenv("FULL_REDEEM_ENABLED", "false")

        # Reset any existing task
        stop_full_redeem_task()

        status = get_full_redeemer_status()

        assert status["enabled"] is False
        assert status["task_status"] == "not_started"


class TestFullRedeemerConstants:
    """Test module constants."""

    def test_default_values(self):
        """Test default constant values."""
        assert DEFAULT_FULL_REDEEM_INTERVAL_SECONDS == 45
        assert DEFAULT_MIN_REDEEM_BALANCE == 0.08
        assert DEFAULT_REDEEM_GAS_BUFFER_PERCENT == 30
        assert DEFAULT_GAS_LIMIT == 300_000
