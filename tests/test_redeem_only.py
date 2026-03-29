"""Tests for REDEEM_ONLY wind-down mode.

When REDEEM_ONLY=true, all new trades must be blocked while redemptions
continue to work normally.
"""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock, patch

from polybot.config import Settings


class TestRedeemOnlyConfig:
    """Verify the REDEEM_ONLY setting is properly defined."""

    def test_default_is_false(self):
        """REDEEM_ONLY defaults to false – trading is allowed by default."""
        s = Settings(
            polygon_private_key="0x" + "a" * 64,
            wallet_address="0x" + "b" * 40,
        )
        assert s.redeem_only is False

    def test_env_override_true(self, monkeypatch):
        """REDEEM_ONLY=true from env stops trading."""
        monkeypatch.setenv("REDEEM_ONLY", "true")
        s = Settings(
            polygon_private_key="0x" + "a" * 64,
            wallet_address="0x" + "b" * 40,
        )
        assert s.redeem_only is True

    def test_env_override_false(self, monkeypatch):
        """REDEEM_ONLY=false keeps trading enabled."""
        monkeypatch.setenv("REDEEM_ONLY", "false")
        s = Settings(
            polygon_private_key="0x" + "a" * 64,
            wallet_address="0x" + "b" * 40,
        )
        assert s.redeem_only is False


class TestRedeemOnlyPlaceTradeAsync:
    """place_trade_async must return skipped when REDEEM_ONLY=true."""

    def test_trade_blocked_when_redeem_only(self):
        mock_settings = MagicMock()
        mock_settings.redeem_only = True
        mock_settings.dry_run = False

        with patch("polybot.executor.get_settings", return_value=mock_settings):
            from polybot.executor import place_trade_async

            result = asyncio.new_event_loop().run_until_complete(
                place_trade_async(
                    market={"question": "test", "tokens": []},
                    outcome="yes",
                    amount=10.0,
                )
            )
        assert result == {"status": "skipped", "reason": "redeem_only"}

    def test_trade_allowed_when_redeem_only_false(self):
        """When REDEEM_ONLY=false and dry_run=true, trade should proceed as dry run."""
        mock_settings = MagicMock()
        mock_settings.redeem_only = False
        mock_settings.dry_run = True

        with patch("polybot.executor.get_settings", return_value=mock_settings):
            with patch(
                "polybot.executor._prepare_trade_params",
                return_value=("token123", 0.5),
            ):
                from polybot.executor import place_trade_async

                result = asyncio.new_event_loop().run_until_complete(
                    place_trade_async(
                        market={"question": "test", "tokens": []},
                        outcome="yes",
                        amount=10.0,
                    )
                )
        assert result["status"] == "dry_run"


class TestRedeemOnlyStandaloneExecuteTrade:
    """Module-level execute_trade must return skipped when REDEEM_ONLY=true."""

    FAKE_PK = "0x" + "a" * 64

    def test_trade_blocked_when_redeem_only(self):
        mock_settings = MagicMock()
        mock_settings.redeem_only = True

        with patch("polybot.config.get_settings", return_value=mock_settings):
            from polybot.onchain_executor import execute_trade

            result = execute_trade(
                private_key=self.FAKE_PK,
                token_id="12345",
                amount_usdc=30.0,
                side="BUY",
            )
        assert result == {"status": "skipped", "reason": "redeem_only"}


class TestRedeemOnlyClassExecuteTrade:
    """OnchainExecutor.execute_trade must return skipped when REDEEM_ONLY=true."""

    def test_trade_blocked_when_redeem_only(self):
        mock_settings = MagicMock()
        mock_settings.redeem_only = True

        with patch("polybot.config.get_settings", return_value=mock_settings):
            from polybot.onchain_executor import OnchainExecutor

            executor = OnchainExecutor.__new__(OnchainExecutor)
            executor.private_key = "0x" + "a" * 64

            result = asyncio.new_event_loop().run_until_complete(
                executor.execute_trade(
                    token_id="12345",
                    amount_usdc=30.0,
                    side="BUY",
                )
            )
        assert result == {"status": "skipped", "reason": "redeem_only"}
