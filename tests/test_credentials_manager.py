"""Tests for credentials_manager.py – L2 credential auto-bootstrap."""

from __future__ import annotations

import json
import os
from unittest.mock import MagicMock, patch

import pytest

from polybot.credentials_manager import (
    _export_to_env,
    get_cached_creds,
    get_or_create_l2_creds,
    validate_l2_creds,
)


class TestExportToEnv:
    """Test _export_to_env writes credentials to os.environ."""

    def test_exports_all_three_keys(self):
        creds = {
            "api_key": "test_key",
            "api_secret": "test_secret",
            "api_passphrase": "test_pass",
        }
        with patch.dict(os.environ, {}, clear=False):
            _export_to_env(creds)
            assert os.environ["POLY_API_KEY"] == "test_key"
            assert os.environ["POLY_API_SECRET"] == "test_secret"
            assert os.environ["POLY_API_PASSPHRASE"] == "test_pass"


class TestGetOrCreateL2CredsFromEnv:
    """Test priority 1: load from environment variables."""

    def test_returns_env_creds_when_all_set(self):
        env = {
            "POLY_API_KEY": "env_key",
            "POLY_API_SECRET": "env_secret",
            "POLY_API_PASSPHRASE": "env_pass",
        }
        with patch.dict(os.environ, env, clear=False):
            creds = get_or_create_l2_creds()
            assert creds["api_key"] == "env_key"
            assert creds["api_secret"] == "env_secret"
            assert creds["api_passphrase"] == "env_pass"

    def test_skips_env_when_partial(self):
        """If only some env vars are set, should not use priority 1."""
        env = {
            "POLY_API_KEY": "env_key",
            "POLY_API_SECRET": "",
            "POLY_API_PASSPHRASE": "",
        }
        with patch.dict(os.environ, env, clear=False):
            # Should fall through to priority 2/3
            with patch(
                "polybot.credentials_manager.os.path.exists", return_value=False
            ):
                with pytest.raises(
                    RuntimeError, match="POLYMARKET_PRIVATE_KEY not set"
                ):
                    # No private key → RuntimeError
                    env_no_pk = dict(env)
                    env_no_pk.pop("POLYMARKET_PRIVATE_KEY", None)
                    with patch.dict(os.environ, env_no_pk, clear=False):
                        os.environ.pop("POLYMARKET_PRIVATE_KEY", None)
                        get_or_create_l2_creds()


class TestGetOrCreateL2CredsFromCache:
    """Test priority 2: load from cached file."""

    def test_loads_from_cache_file(self, tmp_path):
        cache_file = tmp_path / "creds.json"
        cache_data = {
            "api_key": "cached_key",
            "api_secret": "cached_secret",
            "api_passphrase": "cached_pass",
        }
        cache_file.write_text(json.dumps(cache_data))

        # Clear env vars so priority 1 is skipped
        env = {"POLY_API_KEY": "", "POLY_API_SECRET": "", "POLY_API_PASSPHRASE": ""}
        with patch.dict(os.environ, env, clear=False):
            with patch("polybot.credentials_manager.CREDS_FILE", str(cache_file)):
                creds = get_or_create_l2_creds()
                assert creds["api_key"] == "cached_key"
                assert creds["api_secret"] == "cached_secret"
                assert creds["api_passphrase"] == "cached_pass"
                # Should have exported to env
                assert os.environ["POLY_API_KEY"] == "cached_key"

    def test_skips_invalid_cache(self, tmp_path):
        cache_file = tmp_path / "creds.json"
        cache_file.write_text("not valid json {{{")

        env = {"POLY_API_KEY": "", "POLY_API_SECRET": "", "POLY_API_PASSPHRASE": ""}
        with patch.dict(os.environ, env, clear=False):
            with patch("polybot.credentials_manager.CREDS_FILE", str(cache_file)):
                os.environ.pop("POLYMARKET_PRIVATE_KEY", None)
                with pytest.raises(
                    RuntimeError, match="POLYMARKET_PRIVATE_KEY not set"
                ):
                    get_or_create_l2_creds()


class TestGetOrCreateL2CredsDeriveFresh:
    """Test priority 3: derive from private key."""

    def test_derives_and_caches(self, tmp_path):
        cache_file = tmp_path / "creds.json"

        mock_derived = MagicMock()
        mock_derived.api_key = "derived_key"
        mock_derived.api_secret = "derived_secret"
        mock_derived.api_passphrase = "derived_pass"

        mock_client = MagicMock()
        mock_client.create_or_derive_api_creds.return_value = mock_derived

        env = {
            "POLY_API_KEY": "",
            "POLY_API_SECRET": "",
            "POLY_API_PASSPHRASE": "",
            "POLYMARKET_PRIVATE_KEY": "0x" + "a" * 64,
        }
        with patch.dict(os.environ, env, clear=False):
            with patch("polybot.credentials_manager.CREDS_FILE", str(cache_file)):
                with patch(
                    "py_clob_client.client.ClobClient", return_value=mock_client
                ):
                    creds = get_or_create_l2_creds()

        assert creds["api_key"] == "derived_key"
        assert creds["api_secret"] == "derived_secret"
        assert creds["api_passphrase"] == "derived_pass"
        # Should have been cached
        assert cache_file.exists()
        cached = json.loads(cache_file.read_text())
        assert cached["api_key"] == "derived_key"

    def test_raises_on_empty_derived_creds(self):
        mock_derived = MagicMock()
        mock_derived.api_key = ""
        mock_derived.api_secret = ""
        mock_derived.api_passphrase = ""

        mock_client = MagicMock()
        mock_client.create_or_derive_api_creds.return_value = mock_derived

        env = {
            "POLY_API_KEY": "",
            "POLY_API_SECRET": "",
            "POLY_API_PASSPHRASE": "",
            "POLYMARKET_PRIVATE_KEY": "0x" + "a" * 64,
        }
        with patch.dict(os.environ, env, clear=False):
            with patch(
                "polybot.credentials_manager.os.path.exists", return_value=False
            ):
                with patch(
                    "py_clob_client.client.ClobClient", return_value=mock_client
                ):
                    with pytest.raises(
                        RuntimeError, match="Derived credentials are empty"
                    ):
                        get_or_create_l2_creds()

    def test_raises_when_no_private_key(self):
        env = {"POLY_API_KEY": "", "POLY_API_SECRET": "", "POLY_API_PASSPHRASE": ""}
        with patch.dict(os.environ, env, clear=False):
            os.environ.pop("POLYMARKET_PRIVATE_KEY", None)
            with patch(
                "polybot.credentials_manager.os.path.exists", return_value=False
            ):
                with pytest.raises(
                    RuntimeError, match="POLYMARKET_PRIVATE_KEY not set"
                ):
                    get_or_create_l2_creds()


class TestValidateL2Creds:
    """Test validate_l2_creds."""

    def test_returns_true_on_success(self):
        mock_client = MagicMock()
        mock_client.get_api_keys.return_value = [{"api_key": "test"}]

        env = {"POLYMARKET_PRIVATE_KEY": "0x" + "a" * 64}
        with patch.dict(os.environ, env, clear=False):
            with patch("py_clob_client.client.ClobClient", return_value=mock_client):
                result = validate_l2_creds(
                    {
                        "api_key": "k",
                        "api_secret": "s",
                        "api_passphrase": "p",
                    }
                )
                assert result is True

    def test_returns_false_on_exception(self):
        env = {"POLYMARKET_PRIVATE_KEY": "0x" + "a" * 64}
        with patch.dict(os.environ, env, clear=False):
            with patch(
                "py_clob_client.client.ClobClient",
                side_effect=Exception("connection failed"),
            ):
                result = validate_l2_creds(
                    {
                        "api_key": "k",
                        "api_secret": "s",
                        "api_passphrase": "p",
                    }
                )
                assert result is False


class TestGetCachedCreds:
    """Test get_cached_creds returns cached creds or None."""

    def test_returns_none_when_no_creds(self):
        """When singleton is None and env vars are empty, return None."""
        import polybot.credentials_manager as cm

        original = cm._L2_CREDS
        try:
            cm._L2_CREDS = None
            env = {"POLY_API_KEY": "", "POLY_API_SECRET": "", "POLY_API_PASSPHRASE": ""}
            with patch.dict(os.environ, env, clear=False):
                result = get_cached_creds()
                assert result is None
        finally:
            cm._L2_CREDS = original

    def test_returns_singleton_when_set(self):
        """When singleton is populated, return it directly."""
        import polybot.credentials_manager as cm

        original = cm._L2_CREDS
        try:
            cm._L2_CREDS = {
                "api_key": "cached_k",
                "api_secret": "cached_s",
                "api_passphrase": "cached_p",
            }
            result = get_cached_creds()
            assert result is not None
            assert result["api_key"] == "cached_k"
            assert result["api_secret"] == "cached_s"
            assert result["api_passphrase"] == "cached_p"
        finally:
            cm._L2_CREDS = original

    def test_falls_back_to_env_vars(self):
        """When singleton is None but env vars are set, return from env."""
        import polybot.credentials_manager as cm

        original = cm._L2_CREDS
        try:
            cm._L2_CREDS = None
            env = {
                "POLY_API_KEY": "env_key",
                "POLY_API_SECRET": "env_secret",
                "POLY_API_PASSPHRASE": "env_pass",
            }
            with patch.dict(os.environ, env, clear=False):
                result = get_cached_creds()
                assert result is not None
                assert result["api_key"] == "env_key"
        finally:
            cm._L2_CREDS = original


class TestSingletonPopulated:
    """Test that get_or_create_l2_creds populates the _L2_CREDS singleton."""

    def test_singleton_set_from_env(self):
        import polybot.credentials_manager as cm

        original = cm._L2_CREDS
        try:
            cm._L2_CREDS = None
            env = {
                "POLY_API_KEY": "env_key",
                "POLY_API_SECRET": "env_secret",
                "POLY_API_PASSPHRASE": "env_pass",
            }
            with patch.dict(os.environ, env, clear=False):
                get_or_create_l2_creds()
                assert cm._L2_CREDS is not None
                assert cm._L2_CREDS["api_key"] == "env_key"
        finally:
            cm._L2_CREDS = original

    def test_singleton_set_from_cache(self, tmp_path):
        import polybot.credentials_manager as cm

        original = cm._L2_CREDS
        try:
            cm._L2_CREDS = None
            cache_file = tmp_path / "creds.json"
            cache_data = {
                "api_key": "cached_key",
                "api_secret": "cached_secret",
                "api_passphrase": "cached_pass",
            }
            cache_file.write_text(json.dumps(cache_data))

            env = {"POLY_API_KEY": "", "POLY_API_SECRET": "", "POLY_API_PASSPHRASE": ""}
            with patch.dict(os.environ, env, clear=False):
                with patch("polybot.credentials_manager.CREDS_FILE", str(cache_file)):
                    get_or_create_l2_creds()
                    assert cm._L2_CREDS is not None
                    assert cm._L2_CREDS["api_key"] == "cached_key"
        finally:
            cm._L2_CREDS = original
