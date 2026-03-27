"""Execution Logger: Trade execution tracking with slippage analysis.

Logs all trade executions with detailed metrics including:
- Entry/exit prices and times
- Expected vs actual execution prices (slippage)
- Execution latency
- Fee tracking

Provides data for performance analysis and execution quality monitoring.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
import json

from polybot.logging_setup import get_logger

log = get_logger(__name__)


@dataclass
class ExecutionRecord:
    """Record of a single trade execution."""

    trade_id: str
    market_id: str
    market_name: str
    side: str  # 'yes' or 'no'
    direction: str  # 'buy' or 'sell'
    size_usd: float
    expected_price: float
    actual_price: float
    slippage_pct: float
    execution_time_ms: float
    timestamp: str
    status: str = "filled"  # 'filled', 'partial', 'failed'
    fees_usd: float = 0.0
    shares: float = 0.0
    notes: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "trade_id": self.trade_id,
            "market_id": self.market_id,
            "market_name": self.market_name,
            "side": self.side,
            "direction": self.direction,
            "size_usd": round(self.size_usd, 2),
            "expected_price": round(self.expected_price, 4),
            "actual_price": round(self.actual_price, 4),
            "slippage_pct": round(self.slippage_pct, 3),
            "execution_time_ms": round(self.execution_time_ms, 1),
            "timestamp": self.timestamp,
            "status": self.status,
            "fees_usd": round(self.fees_usd, 4),
            "shares": round(self.shares, 4),
            "notes": self.notes,
        }


@dataclass
class ExecutionStats:
    """Aggregated execution statistics."""

    total_trades: int = 0
    successful_trades: int = 0
    failed_trades: int = 0
    total_volume_usd: float = 0.0
    total_fees_usd: float = 0.0
    avg_slippage_pct: float = 0.0
    max_slippage_pct: float = 0.0
    avg_execution_time_ms: float = 0.0
    total_slippage_cost_usd: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "total_trades": self.total_trades,
            "successful_trades": self.successful_trades,
            "failed_trades": self.failed_trades,
            "success_rate": round(
                self.successful_trades / self.total_trades * 100
                if self.total_trades > 0
                else 0,
                1,
            ),
            "total_volume_usd": round(self.total_volume_usd, 2),
            "total_fees_usd": round(self.total_fees_usd, 2),
            "avg_slippage_pct": round(self.avg_slippage_pct, 3),
            "max_slippage_pct": round(self.max_slippage_pct, 3),
            "avg_execution_time_ms": round(self.avg_execution_time_ms, 1),
            "total_slippage_cost_usd": round(self.total_slippage_cost_usd, 2),
        }


class ExecutionLogger:
    """Logs and tracks trade execution quality.

    Provides:
    - Slippage tracking and analysis
    - Execution time monitoring
    - Fee aggregation
    - Historical execution data for analysis
    """

    # Maximum records to keep in memory
    MAX_RECORDS = 1000

    def __init__(self):
        """Initialize the execution logger."""
        self._records: list[ExecutionRecord] = []
        self._trade_counter = 0

    def _generate_trade_id(self) -> str:
        """Generate a unique trade ID."""
        self._trade_counter += 1
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        return f"EXEC-{timestamp}-{self._trade_counter:04d}"

    def log_execution(
        self,
        market_id: str,
        market_name: str,
        side: str,
        direction: str,
        size_usd: float,
        expected_price: float,
        actual_price: float,
        execution_time_ms: float,
        status: str = "filled",
        fees_usd: float = 0.0,
        notes: str = "",
    ) -> ExecutionRecord:
        """Log a trade execution.

        Args:
            market_id: Market identifier
            market_name: Market question/name
            side: 'yes' or 'no'
            direction: 'buy' or 'sell'
            size_usd: Trade size in USD
            expected_price: Expected execution price
            actual_price: Actual execution price
            execution_time_ms: Execution latency in milliseconds
            status: 'filled', 'partial', or 'failed'
            fees_usd: Trading fees paid
            notes: Optional notes about the execution

        Returns:
            ExecutionRecord for the logged trade
        """
        # Calculate slippage
        if expected_price > 0:
            slippage_pct = ((actual_price - expected_price) / expected_price) * 100
        else:
            slippage_pct = 0.0

        # For buys, positive slippage is bad (paid more)
        # For sells, negative slippage is bad (received less)
        if direction == "sell":
            slippage_pct = -slippage_pct  # Normalize so positive = bad

        # Calculate shares
        shares = size_usd / actual_price if actual_price > 0 else 0

        record = ExecutionRecord(
            trade_id=self._generate_trade_id(),
            market_id=market_id,
            market_name=market_name[:100],
            side=side,
            direction=direction,
            size_usd=size_usd,
            expected_price=expected_price,
            actual_price=actual_price,
            slippage_pct=slippage_pct,
            execution_time_ms=execution_time_ms,
            timestamp=datetime.now(timezone.utc).isoformat(),
            status=status,
            fees_usd=fees_usd,
            shares=shares,
            notes=notes,
        )

        self._records.append(record)

        # Trim to max size
        if len(self._records) > self.MAX_RECORDS:
            self._records = self._records[-self.MAX_RECORDS :]

        # Log the execution
        log_func = log.info if status == "filled" else log.warning
        log_func(
            "Trade executed",
            trade_id=record.trade_id,
            market=market_name[:50],
            side=side,
            direction=direction,
            size=f"${size_usd:.2f}",
            slippage=f"{slippage_pct:.2f}%",
            latency=f"{execution_time_ms:.0f}ms",
            status=status,
        )

        return record

    def get_stats(self, last_n: int | None = None) -> ExecutionStats:
        """Get aggregated execution statistics.

        Args:
            last_n: Optional limit to last N trades

        Returns:
            ExecutionStats with aggregated metrics
        """
        records = self._records[-last_n:] if last_n else self._records

        if not records:
            return ExecutionStats()

        stats = ExecutionStats(
            total_trades=len(records),
            successful_trades=sum(1 for r in records if r.status == "filled"),
            failed_trades=sum(1 for r in records if r.status == "failed"),
            total_volume_usd=sum(r.size_usd for r in records),
            total_fees_usd=sum(r.fees_usd for r in records),
            avg_slippage_pct=sum(r.slippage_pct for r in records) / len(records),
            max_slippage_pct=max(abs(r.slippage_pct) for r in records),
            avg_execution_time_ms=sum(r.execution_time_ms for r in records)
            / len(records),
        )

        # Calculate total slippage cost
        for r in records:
            if r.slippage_pct > 0:  # Bad slippage
                slippage_cost = r.size_usd * (r.slippage_pct / 100)
                stats.total_slippage_cost_usd += slippage_cost

        return stats

    def get_recent_executions(self, limit: int = 50) -> list[dict]:
        """Get recent execution records.

        Args:
            limit: Maximum number of records to return

        Returns:
            List of execution record dicts
        """
        records = self._records[-limit:]
        return [r.to_dict() for r in reversed(records)]

    def get_slippage_analysis(self) -> dict[str, Any]:
        """Analyze slippage patterns.

        Returns:
            Dictionary with slippage analysis data
        """
        if not self._records:
            return {"message": "No execution data available"}

        buy_slippages = [r.slippage_pct for r in self._records if r.direction == "buy"]
        sell_slippages = [
            r.slippage_pct for r in self._records if r.direction == "sell"
        ]

        analysis = {
            "total_executions": len(self._records),
            "buy_trades": len(buy_slippages),
            "sell_trades": len(sell_slippages),
            "avg_buy_slippage": round(sum(buy_slippages) / len(buy_slippages), 3)
            if buy_slippages
            else 0,
            "avg_sell_slippage": round(sum(sell_slippages) / len(sell_slippages), 3)
            if sell_slippages
            else 0,
            "slippage_by_hour": self._get_slippage_by_hour(),
        }

        return analysis

    def _get_slippage_by_hour(self) -> dict[int, float]:
        """Get average slippage by hour of day.

        Returns:
            Dict mapping hour (0-23) to average slippage
        """
        hourly: dict[int, list[float]] = {h: [] for h in range(24)}

        for r in self._records:
            try:
                dt = datetime.fromisoformat(r.timestamp.replace("Z", "+00:00"))
                hour = dt.hour
                hourly[hour].append(r.slippage_pct)
            except (ValueError, AttributeError):
                continue

        return {
            hour: round(sum(slippages) / len(slippages), 3) if slippages else 0.0
            for hour, slippages in hourly.items()
        }

    def export_to_json(self) -> str:
        """Export all execution records to JSON.

        Returns:
            JSON string of all records
        """
        return json.dumps([r.to_dict() for r in self._records], indent=2)

    def clear_records(self) -> int:
        """Clear all execution records.

        Returns:
            Number of records cleared
        """
        count = len(self._records)
        self._records = []
        log.info("Execution records cleared", count=count)
        return count


# Singleton instance
_execution_logger: ExecutionLogger | None = None


def get_execution_logger() -> ExecutionLogger:
    """Get or create the global execution logger.

    Returns:
        ExecutionLogger instance
    """
    global _execution_logger
    if _execution_logger is None:
        _execution_logger = ExecutionLogger()
    return _execution_logger


def log_trade_execution(
    market_id: str,
    market_name: str,
    side: str,
    direction: str,
    size_usd: float,
    expected_price: float,
    actual_price: float,
    execution_time_ms: float,
    **kwargs: Any,
) -> ExecutionRecord:
    """Convenience function to log a trade execution.

    See ExecutionLogger.log_execution for parameter details.
    """
    logger = get_execution_logger()
    return logger.log_execution(
        market_id=market_id,
        market_name=market_name,
        side=side,
        direction=direction,
        size_usd=size_usd,
        expected_price=expected_price,
        actual_price=actual_price,
        execution_time_ms=execution_time_ms,
        **kwargs,
    )
