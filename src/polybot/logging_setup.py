"""Structured logging setup for PolyBot.

Provides dual-mode logging:
- Console mode: Colorful, human-readable output for development
- JSON mode: Structured JSON logs for production/log aggregation

The mode is automatically selected based on environment:
- JSON mode when LOG_FORMAT=json or running in CI/production
- Console mode otherwise (local development)
"""

from __future__ import annotations

import logging
import os
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Literal

import structlog
from structlog.typing import Processor


# Default log directory - can be overridden by LOG_DIR env variable
DEFAULT_LOG_DIR = Path.home() / "polybot_logs"
LOG_FILE_NAME = "polybot.log"
SCAN_LOG_FILE_NAME = "polybot_scan.log"

# Log format mode - set via LOG_FORMAT env var
LogFormat = Literal["console", "json"]


def get_log_dir() -> Path:
    """Get the log directory path from environment or use default."""
    log_dir = os.environ.get("POLYBOT_LOG_DIR")
    if log_dir:
        return Path(log_dir)
    return DEFAULT_LOG_DIR


def get_log_file_path() -> Path:
    """Get the full path to the main log file."""
    return get_log_dir() / LOG_FILE_NAME


def get_scan_log_file_path() -> Path:
    """Get the full path to the scan-specific log file."""
    return get_log_dir() / SCAN_LOG_FILE_NAME


def _get_log_format() -> LogFormat:
    """Determine log format based on environment."""
    env_format = os.environ.get("LOG_FORMAT", "").lower()
    if env_format == "json":
        return "json"
    # Auto-detect production environments
    if os.environ.get("RAILWAY_ENVIRONMENT") or os.environ.get("CI"):
        return "json"
    return "console"


def _get_structlog_processors(log_format: LogFormat) -> list[Processor]:
    """Build the processor chain based on log format."""
    # Shared processors for all modes
    shared_processors: list[Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(
            fmt="iso" if log_format == "json" else "%H:%M:%S"
        ),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.UnicodeDecoder(),
    ]

    if log_format == "json":
        # JSON mode: structured output for log aggregation
        shared_processors.extend(
            [
                structlog.processors.format_exc_info,
                structlog.processors.JSONRenderer(),
            ]
        )
    else:
        # Console mode: colorful, human-readable output
        shared_processors.append(
            structlog.dev.ConsoleRenderer(
                colors=True, exception_formatter=structlog.dev.plain_traceback
            )
        )

    return shared_processors


def setup_logging(
    level: str = "INFO",
    enable_file_logging: bool = True,
    log_format: LogFormat | None = None,
) -> None:
    """Configure structlog + stdlib logging.

    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR)
        enable_file_logging: If True, also log to file for terminal logger
        log_format: Force log format ('console' or 'json'). Auto-detected if None.
    """
    log_level = getattr(logging, level.upper(), logging.INFO)
    actual_format = log_format or _get_log_format()

    # Configure handlers
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]

    if enable_file_logging:
        log_dir = get_log_dir()
        log_dir.mkdir(parents=True, exist_ok=True)

        # Main log file - rotating, 10MB max, keep 5 backups
        main_log_file = log_dir / LOG_FILE_NAME
        main_file_handler = RotatingFileHandler(
            main_log_file,
            maxBytes=10 * 1024 * 1024,  # 10MB
            backupCount=5,
            encoding="utf-8",
        )
        main_file_handler.setLevel(log_level)
        main_file_handler.setFormatter(
            logging.Formatter(
                "%(asctime)s %(levelname)-8s [%(name)s] %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )
        handlers.append(main_file_handler)

        # Scan-specific log file for scan_logger.py
        scan_log_file = log_dir / SCAN_LOG_FILE_NAME
        scan_file_handler = RotatingFileHandler(
            scan_log_file,
            maxBytes=5 * 1024 * 1024,  # 5MB
            backupCount=3,
            encoding="utf-8",
        )
        scan_file_handler.setLevel(log_level)
        scan_file_handler.setFormatter(
            logging.Formatter(
                "%(asctime)s %(levelname)-8s [%(name)s] %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )
        # Filter to only include scan/market/API related logs
        scan_file_handler.addFilter(ScanLogFilter())
        handlers.append(scan_file_handler)

    # Configure basic logging with all handlers
    # force=True removes any existing handlers and reconfigures the root logger
    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%H:%M:%S",
        level=log_level,
        handlers=handlers,
        force=True,
    )

    # Build structlog processors
    processors = _get_structlog_processors(actual_format)

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


class ScanLogFilter(logging.Filter):
    """Filter that only passes scan/market/API related log records."""

    SCAN_KEYWORDS = frozenset(
        [
            "scan",
            "market",
            "polymarket",
            "gamma",
            "api",
            "clob",
            "price",
            "odds",
            "mispriced",
            "fetch",
            "query",
            "arb",
            "arbitrage",
            "deviation",
            "spread",
            "volume",
        ]
    )

    def filter(self, record: logging.LogRecord) -> bool:
        """Return True if this log record is scan-related."""
        message = record.getMessage().lower()
        logger_name = record.name.lower()

        # Check if any scan keyword appears in message or logger name
        for keyword in self.SCAN_KEYWORDS:
            if keyword in message or keyword in logger_name:
                return True

        # Also include scanner module logs
        if "scanner" in logger_name:
            return True

        return False


def get_logger(name: str) -> structlog.BoundLogger:
    """Get a structlog logger with the given name.

    Args:
        name: Logger name (typically __name__)

    Returns:
        A bound structlog logger
    """
    return structlog.get_logger(name)


def get_scan_logger(name: str) -> structlog.BoundLogger:
    """Get a logger specifically for scan/market operations.

    Logs from this logger will be tagged for easy filtering.

    Args:
        name: Logger name (typically __name__)

    Returns:
        A bound structlog logger with scan context
    """
    return structlog.get_logger(name).bind(component="scan")
