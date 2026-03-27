"""Pytest configuration for PolyBot tests."""

import sys
import os

# Add src directory to path for imports
sys.path.insert(
    0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src")
)

import pytest


@pytest.fixture(autouse=True)
def _clear_traded_slugs_cache():
    """Reset module-level _traded_slugs cache before each test."""
    from polybot import scanner

    scanner._traded_slugs.clear()
    yield
    scanner._traded_slugs.clear()
