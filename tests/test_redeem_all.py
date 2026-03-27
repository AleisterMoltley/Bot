"""Tests for redeem_all.py – Railway cron job for automated position redemption."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from polybot.redeem_all import (
    CTF_ADDRESS,
    FALLBACK_RPCS,
    USDC_ADDRESS,
    fetch_redeemable_positions,
    get_web3,
    redeem_position,
)


class TestConstants:
    """Verify module constants."""

    def test_ctf_address(self):
        """CTF contract address should be correct."""
        assert CTF_ADDRESS == "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"

    def test_usdc_address(self):
        """USDC address should be correct."""
        assert USDC_ADDRESS == "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"

    def test_fallback_rpcs_exist(self):
        """Should have fallback RPC endpoints."""
        assert len(FALLBACK_RPCS) >= 2
        for rpc in FALLBACK_RPCS:
            assert rpc.startswith("https://")


class TestGetWeb3:
    """Tests for get_web3 function."""

    def test_uses_env_rpc_first(self):
        """get_web3 should try POLYGON_RPC_URL env var first."""
        with patch.dict(
            "os.environ", {"POLYGON_RPC_URL": "https://custom-rpc.example.com"}
        ):
            with patch("polybot.redeem_all.Web3") as mock_web3:
                mock_instance = MagicMock()
                mock_instance.is_connected.return_value = True
                mock_web3.return_value = mock_instance
                mock_web3.HTTPProvider = MagicMock()

                w3 = get_web3()

                assert w3 == mock_instance
                mock_web3.HTTPProvider.assert_called_with(
                    "https://custom-rpc.example.com"
                )

    def test_tries_fallback_rpcs(self):
        """get_web3 should try fallback RPCs if primary fails."""
        with patch.dict("os.environ", {"POLYGON_RPC_URL": ""}):
            with patch("polybot.redeem_all.Web3") as mock_web3:
                mock_instance = MagicMock()
                mock_instance.is_connected.side_effect = [False, True]
                mock_web3.return_value = mock_instance
                mock_web3.HTTPProvider = MagicMock()

                w3 = get_web3()

                assert w3 == mock_instance
                # Should have tried at least 2 RPCs
                assert mock_web3.call_count >= 2

    def test_raises_if_all_rpcs_fail(self):
        """get_web3 should raise ConnectionError if all RPCs fail."""
        with patch.dict("os.environ", {"POLYGON_RPC_URL": ""}):
            with patch("polybot.redeem_all.Web3") as mock_web3:
                mock_instance = MagicMock()
                mock_instance.is_connected.return_value = False
                mock_web3.return_value = mock_instance
                mock_web3.HTTPProvider = MagicMock()

                with pytest.raises(ConnectionError, match="All Polygon RPCs failed"):
                    get_web3()


class TestFetchRedeemablePositions:
    """Tests for fetch_redeemable_positions function."""

    def test_fetch_success_filters_redeemable(self):
        """fetch_redeemable_positions returns only redeemable positions."""
        mock_response = MagicMock()
        mock_response.json.return_value = [
            {"conditionId": "0x123", "redeemable": True, "title": "Redeemable"},
            {"conditionId": "0x456", "redeemable": False, "title": "Not redeemable"},
            {"conditionId": "0x789", "redeemable": True, "title": "Also redeemable"},
        ]
        mock_response.raise_for_status = MagicMock()

        with patch("polybot.redeem_all.requests.get") as mock_get:
            mock_get.return_value = mock_response
            positions = fetch_redeemable_positions("0xTestWallet")

            assert len(positions) == 2
            assert all(p["redeemable"] is True for p in positions)
            mock_get.assert_called_once()
            call_args = mock_get.call_args
            assert "https://data-api.polymarket.com/positions" in call_args[0]

    def test_fetch_empty_result(self):
        """fetch_redeemable_positions returns empty list when no positions."""
        mock_response = MagicMock()
        mock_response.json.return_value = []
        mock_response.raise_for_status = MagicMock()

        with patch("polybot.redeem_all.requests.get") as mock_get:
            mock_get.return_value = mock_response
            positions = fetch_redeemable_positions("0xTestWallet")
            assert positions == []

    def test_fetch_returns_empty_on_error(self):
        """fetch_redeemable_positions returns empty list on API error."""
        with patch("polybot.redeem_all.requests.get") as mock_get:
            mock_get.side_effect = Exception("API error")
            positions = fetch_redeemable_positions("0xTestWallet")
            assert positions == []


class TestRedeemPosition:
    """Tests for redeem_position function."""

    def test_redeem_skips_no_condition_id(self):
        """redeem_position returns False if no conditionId present."""
        mock_w3 = MagicMock()
        mock_account = MagicMock()
        mock_ctf = MagicMock()
        position = {"title": "No Condition", "outcome": "Up"}

        result = redeem_position(mock_w3, mock_account, mock_ctf, position)
        assert result is False

    def test_redeem_success(self):
        """redeem_position returns True on successful redemption."""
        mock_w3 = MagicMock()
        mock_w3.eth.get_transaction_count.return_value = 1
        mock_w3.eth.gas_price = 50_000_000_000
        mock_w3.eth.send_raw_transaction.return_value = b"\x12" * 32
        mock_w3.eth.wait_for_transaction_receipt.return_value = {
            "status": 1,
            "blockNumber": 12345,
        }
        mock_w3.eth.account.sign_transaction.return_value = MagicMock(
            raw_transaction=b"signed"
        )

        mock_account = MagicMock()
        mock_account.address = "0xTestAddress"
        mock_account.key = b"testkey"

        mock_ctf = MagicMock()
        mock_ctf.functions.redeemPositions.return_value.build_transaction.return_value = {}

        position = {
            "conditionId": "0x" + "a" * 64,
            "outcomeIndex": 0,
            "title": "Test Market",
            "outcome": "Up",
        }

        result = redeem_position(mock_w3, mock_account, mock_ctf, position)
        assert result is True

    def test_redeem_tx_reverted(self):
        """redeem_position returns False when TX reverts."""
        mock_w3 = MagicMock()
        mock_w3.eth.get_transaction_count.return_value = 1
        mock_w3.eth.gas_price = 50_000_000_000
        mock_w3.eth.send_raw_transaction.return_value = b"\x12" * 32
        mock_w3.eth.wait_for_transaction_receipt.return_value = {
            "status": 0,  # Reverted
            "blockNumber": 12345,
        }
        mock_w3.eth.account.sign_transaction.return_value = MagicMock(
            raw_transaction=b"signed"
        )

        mock_account = MagicMock()
        mock_account.address = "0xTestAddress"
        mock_account.key = b"testkey"

        mock_ctf = MagicMock()
        mock_ctf.functions.redeemPositions.return_value.build_transaction.return_value = {}

        position = {
            "conditionId": "0x" + "b" * 64,
            "outcomeIndex": 1,
            "title": "Test Market 2",
            "outcome": "Down",
        }

        result = redeem_position(mock_w3, mock_account, mock_ctf, position)
        assert result is False

    def test_redeem_handles_already_known(self):
        """redeem_position treats 'already known' error as success."""
        mock_w3 = MagicMock()
        mock_w3.eth.get_transaction_count.return_value = 1
        mock_w3.eth.gas_price = 50_000_000_000
        mock_w3.eth.send_raw_transaction.side_effect = Exception(
            "Transaction already known"
        )
        mock_w3.eth.account.sign_transaction.return_value = MagicMock(
            raw_transaction=b"signed"
        )

        mock_account = MagicMock()
        mock_account.address = "0xTestAddress"
        mock_account.key = b"testkey"

        mock_ctf = MagicMock()
        mock_ctf.functions.redeemPositions.return_value.build_transaction.return_value = {}

        position = {
            "conditionId": "0x" + "c" * 64,
            "outcomeIndex": 0,
            "title": "Test Market 3",
            "outcome": "Up",
        }

        result = redeem_position(mock_w3, mock_account, mock_ctf, position)
        assert result is True

    def test_redeem_handles_nonce_too_low(self):
        """redeem_position treats 'nonce too low' error as success."""
        mock_w3 = MagicMock()
        mock_w3.eth.get_transaction_count.return_value = 1
        mock_w3.eth.gas_price = 50_000_000_000
        mock_w3.eth.send_raw_transaction.side_effect = Exception("nonce too low")
        mock_w3.eth.account.sign_transaction.return_value = MagicMock(
            raw_transaction=b"signed"
        )

        mock_account = MagicMock()
        mock_account.address = "0xTestAddress"
        mock_account.key = b"testkey"

        mock_ctf = MagicMock()
        mock_ctf.functions.redeemPositions.return_value.build_transaction.return_value = {}

        position = {
            "conditionId": "0x" + "d" * 64,
            "outcomeIndex": 0,
            "title": "Test Market 4",
            "outcome": "Yes",
        }

        result = redeem_position(mock_w3, mock_account, mock_ctf, position)
        assert result is True

    def test_redeem_handles_exception(self):
        """redeem_position returns False on general exception."""
        mock_w3 = MagicMock()
        mock_w3.eth.get_transaction_count.side_effect = Exception("RPC error")

        mock_account = MagicMock()
        mock_account.address = "0xTestAddress"
        mock_account.key = b"testkey"

        mock_ctf = MagicMock()

        position = {
            "conditionId": "0x" + "e" * 64,
            "outcomeIndex": 0,
            "title": "Test Market 5",
            "outcome": "Up",
        }

        result = redeem_position(mock_w3, mock_account, mock_ctf, position)
        assert result is False

    def test_redeem_uses_condition_id_alternative_key(self):
        """redeem_position accepts condition_id (snake_case) as alternative."""
        mock_w3 = MagicMock()
        mock_w3.eth.get_transaction_count.return_value = 1
        mock_w3.eth.gas_price = 50_000_000_000
        mock_w3.eth.send_raw_transaction.return_value = b"\x12" * 32
        mock_w3.eth.wait_for_transaction_receipt.return_value = {
            "status": 1,
            "blockNumber": 12345,
        }
        mock_w3.eth.account.sign_transaction.return_value = MagicMock(
            raw_transaction=b"signed"
        )

        mock_account = MagicMock()
        mock_account.address = "0xTestAddress"
        mock_account.key = b"testkey"

        mock_ctf = MagicMock()
        mock_ctf.functions.redeemPositions.return_value.build_transaction.return_value = {}

        # Using snake_case condition_id
        position = {
            "condition_id": "0x" + "f" * 64,
            "outcomeIndex": 1,
            "title": "Test Market Snake Case",
            "outcome": "Down",
        }

        result = redeem_position(mock_w3, mock_account, mock_ctf, position)
        assert result is True
