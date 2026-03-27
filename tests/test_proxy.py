#!/usr/bin/env python3
"""Tests for proxy module — geo-bypass and rotation logic."""

import sys
import os

# Add src directory to path for imports when running directly
sys.path.insert(
    0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src")
)


# Mock the get_settings before importing proxy module
from unittest.mock import MagicMock, patch


def test_proxy_config_url():
    """Test ProxyConfig URL generation."""
    from polybot.proxy import ProxyConfig

    # Basic proxy without auth
    cfg = ProxyConfig(host="example.com", port=1080)
    assert cfg.url == "socks5://example.com:1080"
    assert cfg.url_masked == "socks5://example.com:1080"

    # Proxy with auth
    cfg_auth = ProxyConfig(
        host="example.com",
        port=1080,
        username="user",
        password="secret123",
    )
    assert cfg_auth.url == "socks5://user:secret123@example.com:1080"
    assert cfg_auth.url_masked == "socks5://user:***@example.com:1080"

    # HTTP proxy
    cfg_http = ProxyConfig(
        host="example.com",
        port=8080,
        protocol="http",
    )
    assert cfg_http.url == "http://example.com:8080"


def test_proxy_health_score():
    """Test ProxyHealth scoring logic."""
    from polybot.proxy import ProxyHealth
    import time

    # New proxy should have neutral score
    health = ProxyHealth(url="test://proxy")
    assert health.score == 50  # Neutral for untested

    # Successful proxy should have high score
    health.successes = 10
    health.last_success = time.time()
    health.latency_ms = 100
    assert health.score > 70

    # Failed proxy should have low score
    health2 = ProxyHealth(url="test://proxy2")
    health2.failures = 10
    health2.successes = 0
    health2.latency_ms = 500
    assert health2.score < 20

    # Blocked proxy should have negative score
    health3 = ProxyHealth(url="test://proxy3")
    health3.is_blocked = True
    assert health3.score == -1


def test_proxy_manager_parse_string():
    """Test proxy string parsing."""
    with patch("polybot.proxy.get_settings") as mock_settings:
        mock_settings.return_value = MagicMock(
            socks5_proxy_host="",
            socks5_proxy_port=0,
            proxy_pool="",
            residential_proxy_pool="",
            use_api_mirrors=True,
        )

        from polybot.proxy import ProxyManager

        pm = ProxyManager()

        # Test parsing various formats
        cfg = pm._parse_proxy_string("socks5://user:pass@host.com:1080")
        assert cfg.host == "host.com"
        assert cfg.port == 1080
        assert cfg.username == "user"
        assert cfg.password == "pass"
        assert cfg.protocol == "socks5"

        # Without protocol
        cfg2 = pm._parse_proxy_string("user:pass@host.com:1080")
        assert cfg2.host == "host.com"
        assert cfg2.protocol == "socks5"  # Default

        # HTTP proxy
        cfg3 = pm._parse_proxy_string("http://host.com:8080")
        assert cfg3.protocol == "http"
        assert cfg3.username == ""


def test_is_geo_blocked():
    """Test geo-block detection logic."""
    from polybot.proxy import _is_geo_blocked

    # Mock a 403 response with geo-block indicators
    class MockResponse:
        def __init__(self, status_code, text):
            self.status_code = status_code
            self.text = text

    # Should detect geo-block
    resp_geo = MockResponse(403, "Access denied: not available in your region")
    assert _is_geo_blocked(resp_geo) is True

    resp_geo2 = MockResponse(403, "This content is blocked in your country")
    assert _is_geo_blocked(resp_geo2) is True

    resp_geo3 = MockResponse(403, "Cloudflare blocking VPN traffic")
    assert _is_geo_blocked(resp_geo3) is True

    # Should not detect geo-block for regular 403
    resp_auth = MockResponse(403, "Invalid API key")
    assert _is_geo_blocked(resp_auth) is False

    # Non-403 should return False
    resp_ok = MockResponse(200, "OK")
    assert _is_geo_blocked(resp_ok) is False


def test_replace_base_url():
    """Test URL base replacement for mirrors."""
    from polybot.proxy import _replace_base_url

    # Gamma API mirror
    url = "https://gamma-api.polymarket.com/markets?active=true"
    new_url = _replace_base_url(url, "https://gamma-mirror.example.com", "gamma")
    assert new_url == "https://gamma-mirror.example.com/markets?active=true"

    # CLOB API mirror
    url2 = "https://clob.polymarket.com/orders"
    new_url2 = _replace_base_url(url2, "https://clob-mirror.example.com", "clob")
    assert new_url2 == "https://clob-mirror.example.com/orders"

    # Unknown base should return unchanged
    url3 = "https://other-api.example.com/endpoint"
    new_url3 = _replace_base_url(url3, "https://mirror.example.com", "gamma")
    assert new_url3 == url3


def test_mirror_rotation():
    """Test mirror endpoint rotation."""
    with patch("polybot.proxy.get_settings") as mock_settings:
        mock_settings.return_value = MagicMock(
            socks5_proxy_host="",
            socks5_proxy_port=0,
            proxy_pool="",
            residential_proxy_pool="",
            use_api_mirrors=True,
        )

        from polybot.proxy import ProxyManager, GAMMA_MIRRORS

        pm = ProxyManager()

        # Get initial endpoint
        initial = pm.get_mirror_endpoint("gamma")
        assert initial == GAMMA_MIRRORS[0]

        # Rotate and verify it changed (if multiple mirrors exist)
        if len(GAMMA_MIRRORS) > 1:
            rotated = pm.rotate_mirror("gamma")
            assert rotated == GAMMA_MIRRORS[1]


if __name__ == "__main__":
    # Run tests when executed directly
    print("Running proxy module tests...")

    test_proxy_config_url()
    print("✓ test_proxy_config_url passed")

    test_proxy_health_score()
    print("✓ test_proxy_health_score passed")

    test_proxy_manager_parse_string()
    print("✓ test_proxy_manager_parse_string passed")

    test_is_geo_blocked()
    print("✓ test_is_geo_blocked passed")

    test_replace_base_url()
    print("✓ test_replace_base_url passed")

    test_mirror_rotation()
    print("✓ test_mirror_rotation passed")

    print("\nAll proxy tests passed! ✓")
