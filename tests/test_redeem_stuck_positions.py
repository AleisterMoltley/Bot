"""Tests for redeem_stuck_positions.py – standalone position redemption script."""

from __future__ import annotations

from unittest.mock import MagicMock, patch


from polybot.redeem_stuck_positions import (
    DATA_API,
    fetch_redeemable_positions,
    redeem_position,
)


class TestConstants:
    """Verify module constants."""

    def test_data_api_url(self):
        """Data API URL should be correct."""
        assert DATA_API == "https://data-api.polymarket.com"


class TestFetchRedeemablePositions:
    """Tests for fetch_redeemable_positions function."""

    def test_fetch_success(self):
        """fetch_redeemable_positions returns positions on success."""
        mock_response = MagicMock()
        mock_response.json.return_value = [
            {"conditionId": "0x123", "redeemable": True, "title": "Test"},
        ]
        mock_response.raise_for_status = MagicMock()

        with patch("polybot.redeem_stuck_positions.requests.get") as mock_get:
            mock_get.return_value = mock_response
            positions = fetch_redeemable_positions("0xTestWallet")

            assert len(positions) == 1
            assert positions[0]["conditionId"] == "0x123"
            mock_get.assert_called_once()
            call_kwargs = mock_get.call_args
            assert call_kwargs[1]["params"]["redeemable"] == "true"

    def test_fetch_empty_result(self):
        """fetch_redeemable_positions returns empty list when no positions."""
        mock_response = MagicMock()
        mock_response.json.return_value = []
        mock_response.raise_for_status = MagicMock()

        with patch("polybot.redeem_stuck_positions.requests.get") as mock_get:
            mock_get.return_value = mock_response
            positions = fetch_redeemable_positions("0xTestWallet")
            assert positions == []

    def test_fetch_fallback_on_error(self):
        """fetch_redeemable_positions uses fallback when redeemable param fails."""
        # First call fails
        mock_error_response = MagicMock()
        mock_error_response.raise_for_status.side_effect = Exception("API error")

        # Second call succeeds
        mock_success_response = MagicMock()
        mock_success_response.json.return_value = [
            {"conditionId": "0x111", "redeemable": True},
            {"conditionId": "0x222", "redeemable": False},
        ]
        mock_success_response.raise_for_status = MagicMock()

        with patch("polybot.redeem_stuck_positions.requests.get") as mock_get:
            mock_get.side_effect = [mock_error_response, mock_success_response]
            positions = fetch_redeemable_positions("0xTestWallet")

            # Should only return the redeemable position
            assert len(positions) == 1
            assert positions[0]["conditionId"] == "0x111"
            assert mock_get.call_count == 2

    def test_fetch_all_calls_fail(self):
        """fetch_redeemable_positions returns empty list when all calls fail."""
        mock_error_response = MagicMock()
        mock_error_response.raise_for_status.side_effect = Exception("API error")

        with patch("polybot.redeem_stuck_positions.requests.get") as mock_get:
            mock_get.return_value = mock_error_response
            positions = fetch_redeemable_positions("0xTestWallet")
            assert positions == []

    def test_fetch_warns_on_limit_reached(self):
        """fetch_redeemable_positions warns when limit is reached."""
        # Create exactly 10 positions (limit=10)
        mock_response = MagicMock()
        mock_response.json.return_value = [
            {"conditionId": f"0x{i:064x}", "redeemable": True} for i in range(10)
        ]
        mock_response.raise_for_status = MagicMock()

        with patch("polybot.redeem_stuck_positions.requests.get") as mock_get:
            with patch("polybot.redeem_stuck_positions.log") as mock_log:
                mock_get.return_value = mock_response
                positions = fetch_redeemable_positions("0xTestWallet", limit=10)

                assert len(positions) == 10
                # Should have called log.warning about limit
                mock_log.warning.assert_called()


class TestRedeemPosition:
    """Tests for redeem_position function."""

    def test_redeem_skips_no_condition_id(self):
        """redeem_position returns False if no conditionId present."""
        mock_w3 = MagicMock()
        mock_account = MagicMock()
        mock_ctf = MagicMock()
        position = {"title": "No Condition", "size": 1.0}

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
            "size": 1.0,
            "curPrice": 0.95,
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
            "size": 2.0,
            "curPrice": 0.05,
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
        }

        with patch("polybot.redeem_stuck_positions.time.sleep"):
            result = redeem_position(mock_w3, mock_account, mock_ctf, position)
            assert result is True

    def test_redeem_handles_nonce_too_low(self):
        """redeem_position returns False on 'nonce too low' error."""
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
            "outcomeIndex": 1,
            "title": "Test Market 4",
        }

        result = redeem_position(mock_w3, mock_account, mock_ctf, position)
        assert result is False

    def test_redeem_handles_generic_error(self):
        """redeem_position returns False on unexpected errors."""
        mock_w3 = MagicMock()
        mock_w3.eth.get_transaction_count.return_value = 1
        mock_w3.eth.gas_price = 50_000_000_000
        mock_w3.eth.send_raw_transaction.side_effect = Exception(
            "Unexpected blockchain error"
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
            "conditionId": "0x" + "e" * 64,
            "outcomeIndex": 0,
            "title": "Test Market 5",
        }

        result = redeem_position(mock_w3, mock_account, mock_ctf, position)
        assert result is False

    def test_redeem_uses_condition_id_alternate_key(self):
        """redeem_position supports both conditionId and condition_id keys."""
        mock_w3 = MagicMock()
        mock_w3.eth.get_transaction_count.return_value = 1
        mock_w3.eth.gas_price = 50_000_000_000
        mock_w3.eth.send_raw_transaction.return_value = b"\x12" * 32
        mock_w3.eth.wait_for_transaction_receipt.return_value = {
            "status": 1,
            "blockNumber": 99999,
        }
        mock_w3.eth.account.sign_transaction.return_value = MagicMock(
            raw_transaction=b"signed"
        )

        mock_account = MagicMock()
        mock_account.address = "0xTestAddress"
        mock_account.key = b"testkey"

        mock_ctf = MagicMock()
        mock_ctf.functions.redeemPositions.return_value.build_transaction.return_value = {}

        # Use condition_id (underscore) instead of conditionId
        position = {
            "condition_id": "0x" + "f" * 64,
            "outcomeIndex": 0,
            "title": "Underscore Test",
        }

        result = redeem_position(mock_w3, mock_account, mock_ctf, position)
        assert result is True

    def test_redeem_outcome_index_creates_correct_index_set(self):
        """redeem_position creates correct indexSet based on outcomeIndex."""
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

        # Test outcomeIndex=0 → indexSet=[1]
        position = {
            "conditionId": "0x" + "1" * 64,
            "outcomeIndex": 0,
        }
        redeem_position(mock_w3, mock_account, mock_ctf, position)
        call_args = mock_ctf.functions.redeemPositions.call_args
        assert call_args[0][3] == [1]  # indexSet for outcomeIndex 0

        # Test outcomeIndex=1 → indexSet=[2]
        position["outcomeIndex"] = 1
        redeem_position(mock_w3, mock_account, mock_ctf, position)
        call_args = mock_ctf.functions.redeemPositions.call_args
        assert call_args[0][3] == [2]  # indexSet for outcomeIndex 1
