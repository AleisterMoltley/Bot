"""
PolyBot main entry point.

This module allows running PolyBot with: python -m polybot
It's designed for Railway deployment as a pure API backend.

Usage:
    python -m polybot           # Runs the FastAPI server (default)
    python -m polybot api       # Runs the FastAPI server
    python -m polybot run       # Runs the main trading loop
    python -m polybot dashboard # Runs the web dashboard
"""

from __future__ import annotations

import sys
import os


def main():
    """
    Main entry point for PolyBot.

    When run without arguments or with 'api', starts the FastAPI server.
    Other commands are passed to the CLI.
    """
    # Force unbuffered output for Railway logging
    os.environ.setdefault("PYTHONUNBUFFERED", "1")

    print("=" * 60, flush=True)
    print("PolyBot Starting via python -m polybot", flush=True)
    print("=" * 60, flush=True)
    print(f"Python: {sys.executable}", flush=True)
    print(f"Version: {sys.version}", flush=True)
    print(f"Arguments: {sys.argv}", flush=True)
    print(f"Working directory: {os.getcwd()}", flush=True)
    print("=" * 60, flush=True)

    # Auto-bootstrap L2 credentials on every startup
    import logging

    _logger = logging.getLogger(__name__)
    try:
        from polybot.credentials_manager import (
            get_or_create_l2_creds,
            validate_l2_creds,
        )

        _logger.info("[STARTUP] Bootstrapping Polymarket L2 credentials...")
        print("[STARTUP] Bootstrapping Polymarket L2 credentials...", flush=True)
        l2_creds = get_or_create_l2_creds()
        if not validate_l2_creds(l2_creds):
            _logger.warning("[STARTUP] Credential validation failed — re-deriving...")
            if os.path.exists("/tmp/polymarket_creds.json"):
                os.remove("/tmp/polymarket_creds.json")
            l2_creds = get_or_create_l2_creds()
        _logger.info("[STARTUP] L2 credentials ready ✅")
        print("[STARTUP] L2 credentials ready ✅", flush=True)
    except Exception as e:
        _logger.warning(f"[STARTUP] L2 credential bootstrap skipped: {e}")
        print(f"[STARTUP] L2 credential bootstrap skipped: {e}", flush=True)

    # Determine command to run
    args = sys.argv[
        1:
    ]  # Skip module path from sys.argv (sys.argv[0] is __main__.py path)

    if not args or args[0] == "api":
        # Default: run FastAPI server
        print("Starting FastAPI server...", flush=True)
        from polybot.main_fastapi import run_api_server

        run_api_server()
    else:
        # For other commands, use the CLI
        from polybot.cli import cli

        cli()


if __name__ == "__main__":
    main()
