#!/usr/bin/env python3
"""Test Polymarket API geo-restrictions from current location.

This is a standalone test that doesn't require the polybot package.
Run directly: python tests/test_geo.py
"""

import asyncio

import aiohttp

GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_API = "https://clob.polymarket.com"


async def _check_endpoint(session, name, url, params=None):
    """Check if an endpoint is accessible (internal function, not a pytest test)."""
    try:
        async with session.get(url, params=params, timeout=10) as resp:
            if resp.status == 200:
                return {"name": name, "status": "✅ OK", "code": resp.status}
            elif resp.status == 403:
                return {"name": name, "status": "❌ BLOCKED", "code": resp.status}
            return {"name": name, "status": f"⚠️ {resp.status}", "code": resp.status}
    except Exception as e:
        return {"name": name, "status": f"⚠️ Error: {e}", "code": 0}


async def main():
    print("=" * 50)
    print("POLYMARKET GEO-RESTRICTION TEST")
    print("=" * 50)

    async with aiohttp.ClientSession() as session:
        # Get IP
        try:
            async with session.get("https://api.ipify.org?format=json", timeout=5) as r:
                ip = (await r.json()).get("ip", "unknown")
        except Exception:
            ip = "unknown"
        print(f"IP: {ip}\n")

        results = await asyncio.gather(
            _check_endpoint(
                session,
                "Gamma Markets",
                f"{GAMMA_API}/markets",
                {"active": "true", "limit": "3"},
            ),
            _check_endpoint(
                session, "CLOB Markets", f"{CLOB_API}/markets", {"limit": "3"}
            ),
            _check_endpoint(
                session,
                "CLOB Price",
                f"{CLOB_API}/price",
                {"token_id": "0", "side": "BUY"},
            ),
        )

        for r in results:
            print(f"  {r['name']:20s} {r['status']}")

    print()


if __name__ == "__main__":
    asyncio.run(main())
