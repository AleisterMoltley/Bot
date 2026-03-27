#!/usr/bin/env python3
"""Tests for Solana bridge module — SOL→USDC swap and bridge to Polygon."""

import sys
import os

# Add src directory to path for imports when running directly
sys.path.insert(
    0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src")
)

import pytest
from decimal import Decimal
from unittest.mock import MagicMock, patch, AsyncMock


def test_solana_wallet_info_dataclass():
    """Test SolanaWalletInfo dataclass."""
    from polybot.solana_bridge import SolanaWalletInfo

    wallet = SolanaWalletInfo(
        address="So11111111111111111111111111111111111111112",
        sol_balance=Decimal("1.5"),
        usdc_balance=Decimal("100.50"),
    )

    assert wallet.address == "So11111111111111111111111111111111111111112"
    assert wallet.sol_balance == Decimal("1.5")
    assert wallet.usdc_balance == Decimal("100.50")
    assert wallet.last_updated > 0


def test_swap_quote_dataclass():
    """Test SwapQuote dataclass."""
    from polybot.solana_bridge import SwapQuote

    quote = SwapQuote(
        input_amount=Decimal("1.0"),
        output_amount=Decimal("150.0"),
        price_impact=Decimal("0.1"),
    )

    assert quote.input_amount == Decimal("1.0")
    assert quote.output_amount == Decimal("150.0")
    assert quote.price_impact == Decimal("0.1")


def test_bridge_result_dataclass():
    """Test BridgeResult dataclass."""
    from polybot.solana_bridge import BridgeResult

    result = BridgeResult(
        success=True,
        tx_signature="test_sig",
        amount_bridged=Decimal("100"),
        target_address="0x1234",
    )

    assert result.success is True
    assert result.tx_signature == "test_sig"
    assert result.amount_bridged == Decimal("100")


def test_constants():
    """Test that important constants are defined."""
    from polybot.solana_bridge import (
        SOLANA_USDC_MINT,
        WRAPPED_SOL_MINT,
        LAMPORTS_PER_SOL,
        USDC_DECIMALS,
        JUPITER_QUOTE_API,
        JUPITER_SWAP_API,
    )

    assert SOLANA_USDC_MINT == "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
    assert WRAPPED_SOL_MINT == "So11111111111111111111111111111111111111112"
    assert LAMPORTS_PER_SOL == 1_000_000_000
    assert USDC_DECIMALS == 6
    assert "jup.ag" in JUPITER_QUOTE_API
    assert "jup.ag" in JUPITER_SWAP_API


def test_get_solana_address_no_key():
    """Test get_solana_address returns None when no key configured."""
    with patch("polybot.solana_bridge.get_settings") as mock_settings:
        mock_settings.return_value = MagicMock(
            solana_private_key=MagicMock(get_secret_value=lambda: ""),
        )

        from polybot.solana_bridge import get_solana_address

        result = get_solana_address()
        assert result is None


def test_get_wallet_info_no_client():
    """Test get_wallet_info returns None when client unavailable."""
    with patch("polybot.solana_bridge.get_settings") as mock_settings:
        mock_settings.return_value = MagicMock(
            solana_private_key=MagicMock(get_secret_value=lambda: ""),
            solana_rpc_url="https://api.mainnet-beta.solana.com",
        )

        from polybot.solana_bridge import get_wallet_info

        result = get_wallet_info()
        assert result is None


def test_bridge_manager_should_check():
    """Test SolanaBridgeManager.should_check timing."""
    with patch("polybot.solana_bridge.get_settings") as mock_settings:
        mock_settings.return_value = MagicMock(
            solana_private_key=MagicMock(get_secret_value=lambda: ""),
        )

        from polybot.solana_bridge import SolanaBridgeManager

        manager = SolanaBridgeManager()

        # First check should be allowed
        assert manager.should_check() is True

        # After updating last_check, should_check returns False
        import time

        manager._last_check = time.time()
        assert manager.should_check() is False


def test_bridge_manager_singleton():
    """Test get_bridge_manager returns singleton."""
    with patch("polybot.solana_bridge.get_settings") as mock_settings:
        mock_settings.return_value = MagicMock(
            solana_private_key=MagicMock(get_secret_value=lambda: ""),
        )

        from polybot.solana_bridge import get_bridge_manager

        manager1 = get_bridge_manager()
        manager2 = get_bridge_manager()

        assert manager1 is manager2


def test_format_wallet_info_none():
    """Test format_wallet_info with None input."""
    from polybot.solana_bridge import format_wallet_info

    result = format_wallet_info(None)
    assert "not configured" in result.lower() or "unavailable" in result.lower()


def test_format_wallet_info_valid():
    """Test format_wallet_info with valid wallet."""
    from polybot.solana_bridge import format_wallet_info, SolanaWalletInfo

    wallet = SolanaWalletInfo(
        address="SoLaNaAddReSs1234567890AbCdEfGhIjKlMnOpQrSt",
        sol_balance=Decimal("1.5"),
        usdc_balance=Decimal("100.50"),
    )

    result = format_wallet_info(wallet)
    assert "Solana Wallet" in result
    assert "SoLaNaAd" in result  # First 8 chars
    assert "1.5" in result
    assert "100.50" in result


@pytest.mark.asyncio
async def test_get_swap_quote_mocked():
    """Test get_swap_quote with mocked HTTP response."""
    mock_response = {
        "outAmount": "150000000",  # 150 USDC in base units
        "priceImpactPct": "0.01",
        "routePlan": [{"name": "test_route"}],
    }

    import httpx

    with patch.object(httpx, "AsyncClient") as mock_client_class:
        mock_client_instance = AsyncMock()
        mock_client_class.return_value.__aenter__.return_value = mock_client_instance
        mock_response_obj = MagicMock()
        mock_response_obj.json.return_value = mock_response
        mock_response_obj.raise_for_status = MagicMock()
        mock_client_instance.get.return_value = mock_response_obj

        from polybot.solana_bridge import get_swap_quote

        quote = await get_swap_quote(Decimal("1.0"))

        assert quote is not None
        assert quote.input_amount == Decimal("1.0")
        assert quote.output_amount == Decimal("150")


@pytest.mark.asyncio
async def test_execute_swap_dry_run():
    """Test execute_swap in dry run mode."""
    from polybot.solana_bridge import execute_swap, SwapQuote

    quote = SwapQuote(
        input_amount=Decimal("1.0"),
        output_amount=Decimal("150.0"),
        price_impact=Decimal("0.1"),
        quote_response={"test": True},
    )

    # Mock the keypair to allow dry run
    with patch("polybot.solana_bridge._get_keypair") as mock_keypair:
        mock_kp = MagicMock()
        mock_kp.pubkey.return_value = "TestPubkey"
        mock_keypair.return_value = mock_kp

        result = await execute_swap(quote, dry_run=True)

        assert result is not None
        assert result["dry_run"] is True
        assert result["input_amount"] == 1.0
        assert result["output_amount"] == 150.0


@pytest.mark.asyncio
async def test_bridge_usdc_dry_run():
    """Test bridge_usdc_to_polygon in dry run mode."""
    from polybot.solana_bridge import bridge_usdc_to_polygon

    result = await bridge_usdc_to_polygon(
        Decimal("100"),
        "0x1234567890123456789012345678901234567890",
        dry_run=True,
    )

    assert result.success is True
    assert result.amount_bridged == Decimal("100")
    assert result.error == "dry_run"


@pytest.mark.asyncio
async def test_auto_swap_and_bridge_no_wallet():
    """Test auto_swap_and_bridge when wallet unavailable."""
    with patch("polybot.solana_bridge.get_settings") as mock_settings:
        mock_settings.return_value = MagicMock(
            solana_private_key=MagicMock(get_secret_value=lambda: ""),
            auto_swap_sol=True,
            sol_swap_threshold=0.1,
        )

        with patch("polybot.solana_bridge.get_wallet_info", return_value=None):
            from polybot.solana_bridge import auto_swap_and_bridge

            result = await auto_swap_and_bridge()

            assert result["success"] is False
            assert "wallet" in result.get("error", "").lower()


@pytest.mark.asyncio
async def test_auto_swap_and_bridge_below_threshold():
    """Test auto_swap_and_bridge when SOL below threshold."""
    from polybot.solana_bridge import SolanaWalletInfo

    mock_wallet = SolanaWalletInfo(
        address="TestAddress",
        sol_balance=Decimal("0.05"),  # Below default threshold of 0.1
        usdc_balance=Decimal("50"),
    )

    with patch("polybot.solana_bridge.get_settings") as mock_settings:
        mock_settings.return_value = MagicMock(
            solana_private_key=MagicMock(get_secret_value=lambda: "test"),
            auto_swap_sol=True,
            sol_swap_threshold=0.1,
            wallet_address="0x1234",
        )

        with patch("polybot.solana_bridge.get_wallet_info", return_value=mock_wallet):
            from polybot.solana_bridge import auto_swap_and_bridge

            result = await auto_swap_and_bridge()

            assert result.get("below_threshold") is True


def test_check_and_swap_wrapper():
    """Test check_and_swap synchronous wrapper."""
    with patch("polybot.solana_bridge.get_settings") as mock_settings:
        mock_settings.return_value = MagicMock(
            solana_private_key=MagicMock(get_secret_value=lambda: ""),
            auto_swap_sol=True,
            sol_swap_threshold=0.1,
        )

        with patch("polybot.solana_bridge.get_wallet_info", return_value=None):
            from polybot.solana_bridge import check_and_swap

            result = check_and_swap(dry_run=True)

            # Should return error dict since wallet is unavailable
            assert result is None or result.get("success") is False


if __name__ == "__main__":
    # Run tests when executed directly
    print("Running Solana bridge module tests...")

    test_solana_wallet_info_dataclass()
    print("✓ test_solana_wallet_info_dataclass passed")

    test_swap_quote_dataclass()
    print("✓ test_swap_quote_dataclass passed")

    test_bridge_result_dataclass()
    print("✓ test_bridge_result_dataclass passed")

    test_constants()
    print("✓ test_constants passed")

    test_get_solana_address_no_key()
    print("✓ test_get_solana_address_no_key passed")

    test_get_wallet_info_no_client()
    print("✓ test_get_wallet_info_no_client passed")

    test_bridge_manager_should_check()
    print("✓ test_bridge_manager_should_check passed")

    test_bridge_manager_singleton()
    print("✓ test_bridge_manager_singleton passed")

    test_format_wallet_info_none()
    print("✓ test_format_wallet_info_none passed")

    test_format_wallet_info_valid()
    print("✓ test_format_wallet_info_valid passed")

    # Async tests need to be run with asyncio
    import asyncio

    asyncio.run(test_get_swap_quote_mocked())
    print("✓ test_get_swap_quote_mocked passed")

    asyncio.run(test_execute_swap_dry_run())
    print("✓ test_execute_swap_dry_run passed")

    asyncio.run(test_bridge_usdc_dry_run())
    print("✓ test_bridge_usdc_dry_run passed")

    asyncio.run(test_auto_swap_and_bridge_no_wallet())
    print("✓ test_auto_swap_and_bridge_no_wallet passed")

    asyncio.run(test_auto_swap_and_bridge_below_threshold())
    print("✓ test_auto_swap_and_bridge_below_threshold passed")

    test_check_and_swap_wrapper()
    print("✓ test_check_and_swap_wrapper passed")

    print("\nAll Solana bridge tests passed! ✓")
