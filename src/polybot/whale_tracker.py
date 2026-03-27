"""Whale copy tracker for Up/Down crypto markets.

Monitors top trader wallets and copies their high-conviction bets
on BTC/XRP/SOL/ETH Up/Down markets only.

Features:
- Gamma Top Wallets integration
- Filter for Up/Down crypto markets only
- Minimum bet threshold ($5k+)
- Tiered copying based on whale conviction
"""

from __future__ import annotations

from typing import Any

from polybot.config import get_settings
from polybot.copy_trading import (
    fetch_trader_activity,
    calculate_copy_size,
    get_trader_balance,
)
from polybot.proxy import make_proxied_request
from polybot.logging_setup import get_logger

log = get_logger(__name__)

# Gamma API endpoint
GAMMA_API = "https://gamma-api.polymarket.com"

# Whale bet minimum threshold
WHALE_MIN_BET_USD = 5000  # $5k minimum

# Known top whale addresses (example - should be updated with real addresses)
DEFAULT_WHALE_ADDRESSES = [
    "0x5c76...example1",  # Placeholder - add real whale addresses
    "0x8a2f...example2",
]


class WhaleCopyTracker:
    """Tracks and copies whale bets on Up/Down crypto markets.

    Filters for:
    - High-value bets (>$5k)
    - Up/Down crypto markets (BTC/XRP/SOL/ETH)
    - Follows top performers from Gamma
    """

    def __init__(
        self,
        min_bet_usd: float = WHALE_MIN_BET_USD,
        whale_addresses: list[str] | None = None,
    ):
        """Initialize the WhaleCopyTracker.

        Args:
            min_bet_usd: Minimum bet size to track ($5k default)
            whale_addresses: List of whale wallet addresses to track
        """
        self.min_bet_usd = min_bet_usd
        self.whale_addresses = whale_addresses or []
        self.enabled = False
        self.tracked_bets: list[dict] = []
        self.copied_trades: list[dict] = []

    def enable(self, addresses: list[str] | None = None) -> None:
        """Enable whale tracking.

        Args:
            addresses: Optional new list of addresses to track
        """
        if addresses:
            self.whale_addresses = addresses
        self.enabled = True
        log.info(
            "Whale tracking ENABLED",
            min_bet=f"${self.min_bet_usd}",
            tracking_count=len(self.whale_addresses),
        )

    def disable(self) -> None:
        """Disable whale tracking."""
        self.enabled = False
        log.info("Whale tracking DISABLED")

    def is_up_down_crypto_market(self, market_question: str) -> bool:
        """Check if market is an Up/Down crypto market.

        Args:
            market_question: The market question text

        Returns:
            True if market qualifies
        """
        q = market_question.lower()

        # Check for crypto keywords
        crypto_keywords = ["bitcoin", "btc", "xrp", "solana", "sol", "ethereum", "eth"]
        has_crypto = any(kw in q for kw in crypto_keywords)

        # Check for up/down patterns
        up_down_patterns = ["up or down", "will hit", "above or below"]
        has_pattern = any(pattern in q for pattern in up_down_patterns)

        return has_crypto and has_pattern

    def fetch_top_whales(self, limit: int = 20) -> list[dict]:
        """Fetch top whale wallets from Gamma API.

        Args:
            limit: Number of top wallets to fetch

        Returns:
            List of top whale wallet info
        """
        try:
            resp = make_proxied_request(
                f"{GAMMA_API}/users/leaderboard",
                params={"limit": limit, "period": "7d"},
                timeout=30,
                max_retries=3,
            )
            data = resp.json()
            return data if isinstance(data, list) else []
        except Exception as e:
            log.error("Failed to fetch whale leaderboard", error=str(e))
            return []

    def get_whale_up_down_bets(self, address: str) -> list[dict]:
        """Get whale bets filtered for Up/Down crypto markets.

        Args:
            address: Whale wallet address

        Returns:
            List of qualifying bets
        """
        activity = fetch_trader_activity(address, limit=50)
        qualifying_bets = []

        for trade in activity:
            # Check bet size
            bet_size = float(trade.get("usdcSize", 0) or trade.get("size", 0))
            if bet_size < self.min_bet_usd:
                continue

            # Check if Up/Down crypto market
            market_title = trade.get("title", "") or trade.get("market_title", "")
            if not self.is_up_down_crypto_market(market_title):
                continue

            # Format address as 0x1234...abcd for display (first 6 + last 4 chars)
            addr_display = (
                f"{address[:6]}...{address[-4:]}" if len(address) >= 10 else address
            )

            qualifying_bets.append(
                {
                    "whale_address": addr_display,
                    "whale_address_full": address,  # Keep full address for operations
                    "market": market_title,
                    "side": trade.get("side", "").upper(),
                    "bet_size": bet_size,
                    "price": float(trade.get("price", 0)),
                    "token_id": trade.get("asset", "") or trade.get("token_id", ""),
                    "timestamp": trade.get("timestamp", ""),
                    "raw_trade": trade,
                }
            )

        return qualifying_bets

    def get_top_whales_up_down(self, limit: int = 10) -> list[dict]:
        """Get top whale bets on Up/Down crypto markets.

        Combines tracking of configured addresses with Gamma top wallets.

        Args:
            limit: Maximum results to return

        Returns:
            List of top whale bets on Up/Down markets
        """
        all_bets = []

        # Track configured addresses
        for address in self.whale_addresses:
            bets = self.get_whale_up_down_bets(address)
            all_bets.extend(bets)

        # Also check Gamma leaderboard
        top_whales = self.fetch_top_whales(20)
        for whale in top_whales:
            address = whale.get("address") or whale.get("user", "")
            if address and address not in self.whale_addresses:
                bets = self.get_whale_up_down_bets(address)
                all_bets.extend(bets)

        # Sort by bet size (highest first)
        all_bets.sort(key=lambda x: -x.get("bet_size", 0))

        # Store and return top N
        self.tracked_bets = all_bets[:limit]
        return self.tracked_bets

    def format_whale_bets(self) -> str:
        """Format tracked whale bets for display.

        Returns:
            Formatted string for CLI/Dashboard display
        """
        if not self.tracked_bets:
            return "🐋 No whale bets on Up/Down crypto markets found."

        lines = ["🐋 **TOP WHALE BETS - Up/Down Crypto**\n"]

        for i, bet in enumerate(self.tracked_bets[:10], 1):
            market = bet.get("market", "Unknown")[:50]
            side = bet.get("side", "?")
            size = bet.get("bet_size", 0)
            price = bet.get("price", 0)
            whale = bet.get("whale_address", "?")

            side_emoji = "🟢" if side == "BUY" else "🔴"
            lines.append(f"**{i}. {market}...**")
            lines.append(f"   {side_emoji} {side} ${size:,.0f} @ {price:.2f}")
            lines.append(f"   🐋 Whale: `{whale}`")
            lines.append("")

        lines.append("_Use /copy_whale <index> to copy a bet_")
        return "\n".join(lines)

    def get_copy_recommendation(self, bet_index: int) -> dict[str, Any]:
        """Get copy recommendation for a specific whale bet.

        Args:
            bet_index: Index of the bet to copy (1-based)

        Returns:
            Dict with copy recommendation details
        """
        if not self.tracked_bets or bet_index < 1 or bet_index > len(self.tracked_bets):
            return {"error": "Invalid bet index"}

        bet = self.tracked_bets[bet_index - 1]

        # Get whale balance for proportional sizing
        whale_address = bet.get("raw_trade", {}).get("user", "")
        whale_balance = get_trader_balance(whale_address) if whale_address else 10000

        # Calculate copy size
        from polybot.executor import get_polygon_balance

        my_balance = get_polygon_balance()

        calc = calculate_copy_size(
            trader_order_usd=bet.get("bet_size", 0),
            trader_balance=whale_balance,
            my_balance=my_balance,
        )

        return {
            "market": bet.get("market"),
            "side": bet.get("side"),
            "whale_size": bet.get("bet_size"),
            "recommended_size": calc.get("size", 0),
            "multiplier": calc.get("multiplier", 1.0),
            "token_id": bet.get("token_id"),
            "price": bet.get("price"),
            "skip_reason": calc.get("reason") if calc.get("skipped") else None,
        }

    def get_status(self) -> dict:
        """Get whale tracker status for display.

        Returns:
            Dict with tracker statistics
        """
        return {
            "enabled": self.enabled,
            "min_bet_usd": self.min_bet_usd,
            "tracking_addresses": len(self.whale_addresses),
            "tracked_bets": len(self.tracked_bets),
            "copied_trades": len(self.copied_trades),
        }


# Global whale tracker instance
_whale_tracker: WhaleCopyTracker | None = None


def get_whale_tracker() -> WhaleCopyTracker:
    """Get or create the global whale tracker instance."""
    global _whale_tracker
    if _whale_tracker is None:
        settings = get_settings()
        _whale_tracker = WhaleCopyTracker(
            whale_addresses=settings.copy_trader_addresses,
        )
    return _whale_tracker


def enable_whale_tracking(addresses: list[str] | None = None) -> WhaleCopyTracker:
    """Enable whale tracking globally."""
    tracker = get_whale_tracker()
    tracker.enable(addresses)
    return tracker


def disable_whale_tracking() -> WhaleCopyTracker:
    """Disable whale tracking globally."""
    tracker = get_whale_tracker()
    tracker.disable()
    return tracker
