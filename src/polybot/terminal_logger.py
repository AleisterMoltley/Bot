"""
Terminal Logger for PolyBot

A separate terminal-based log viewer that displays ALL print() outputs,
logging messages, and scan/market queries in real-time. Features:

- Runs completely separate from the main bot
- Tails the log file with file watching
- Colorful output using Rich library
- Categorized log entries with prefixes: [SCAN] [TRADE] [API] [ERROR] etc.
- Emoji support for visual distinction

Usage:
    polybot logs                    # Start live log viewer
    polybot logs --log-file /path   # Use custom log file
    polybot logs --filter scan      # Filter for specific keywords
    polybot logs --no-colors        # Disable colored output

See also:
    polybot scan-log                # Scan-only logger (market/API focused)
"""

from __future__ import annotations

import signal
import sys
import time
from pathlib import Path
from typing import Iterator

# Rich is already a dependency of PolyBot
from rich.console import Console
from rich.text import Text

# Import log path helper from logging_setup
from polybot.logging_setup import get_log_file_path

# Poll interval for following log files (seconds)
FOLLOW_POLL_INTERVAL = 0.1


class LogCategory:
    """Log entry categories with styling."""

    SCAN = ("🔍 [SCAN]", "cyan bold")
    TRADE = ("💰 [TRADE]", "yellow bold")
    API = ("🌐 [API]", "blue")
    ERROR = ("❌ [ERROR]", "red bold")
    WARNING = ("⚠️  [WARN]", "orange1")
    SIGNAL = ("📊 [SIGNAL]", "magenta")
    COPY = ("📋 [COPY]", "green")
    ARB = ("⚡ [ARB]", "purple")
    RISK = ("🛡️  [RISK]", "dark_orange")
    WALLET = ("💳 [WALLET]", "gold1")
    DEBUG = ("🐛 [DEBUG]", "dim")
    INFO = ("ℹ️  [INFO]", "white")
    LOG = ("📝 [LOG]", "grey70")


# Keywords to category mapping (lowercase for matching)
KEYWORD_CATEGORIES = {
    # Scan/Query related
    (
        "scan",
        "query",
        "fetch",
        "market",
        "polymarket",
        "price",
        "odds",
        "mispriced",
    ): LogCategory.SCAN,
    # Trade related
    ("trade", "order", "buy", "sell", "position", "execute", "fill"): LogCategory.TRADE,
    # API related
    ("api", "request", "response", "http", "endpoint", "clob"): LogCategory.API,
    # Error related
    ("error", "exception", "failed", "traceback", "crash"): LogCategory.ERROR,
    # Warning related
    ("warning", "warn", "caution"): LogCategory.WARNING,
    # Signal related
    (
        "signal",
        "rsi",
        "macd",
        "momentum",
        "indicator",
        "confidence",
    ): LogCategory.SIGNAL,
    # Copy trading related
    ("copy", "follow", "mirror", "whale"): LogCategory.COPY,
    # Arbitrage related
    ("arb", "arbitrage", "spread", "opportunity"): LogCategory.ARB,
    # Risk management related
    ("risk", "circuit", "breaker", "kelly", "loss", "limit"): LogCategory.RISK,
    # Wallet/Balance related
    ("wallet", "balance", "usdc", "matic", "polygon", "solana"): LogCategory.WALLET,
    # Debug related
    ("debug",): LogCategory.DEBUG,
    # Info related (lower priority)
    ("info",): LogCategory.INFO,
}


def categorize_log_line(line: str) -> tuple[str, str]:
    """Categorize a log line and return (prefix, style) tuple.

    Args:
        line: The log line to categorize

    Returns:
        Tuple of (prefix_with_emoji, rich_style)
    """
    lower_line = line.lower()

    # Check keywords in priority order
    for keywords, category in KEYWORD_CATEGORIES.items():
        if any(kw in lower_line for kw in keywords):
            return category

    return LogCategory.LOG


def tail_file(file_path: Path, follow: bool = True) -> Iterator[str]:
    """Generator that yields lines from a file, optionally following new lines.

    Uses tailer library if available, falls back to manual implementation.

    Args:
        file_path: Path to the file to tail
        follow: If True, keep following the file for new content

    Yields:
        Lines from the file
    """
    with file_path.open("r", encoding="utf-8", errors="replace") as f:
        if not follow:
            # Read all existing content
            for line in f:
                yield line.rstrip("\n")
            return
        # Follow mode: seek to end and watch for new lines
        f.seek(0, 2)
        while True:
            line = f.readline()
            if line:
                yield line.rstrip("\n")
            else:
                time.sleep(FOLLOW_POLL_INTERVAL)


def run_terminal_logger(
    log_file: Path | None = None,
    filter_keywords: list[str] | None = None,
    use_colors: bool = True,
    show_all_lines: bool = False,
) -> None:
    """Run the terminal logger to display PolyBot logs in real-time.

    Args:
        log_file: Path to log file. Uses default if None.
        filter_keywords: Only show lines containing these keywords.
        use_colors: Enable colorful output using Rich.
        show_all_lines: Show all lines, even without keywords.
    """
    console = Console(color_system="auto" if use_colors else None)

    # Determine log file path
    if log_file is None:
        log_file = get_log_file_path()

    # Wait for log file to exist
    console.print(
        "[bold green]🚀 PolyBot Terminal Logger[/bold green]",
        highlight=False,
    )
    console.print(f"[dim]Log file: {log_file}[/dim]")
    console.print(
        "[dim]Press Ctrl+C to stop[/dim]\n",
        highlight=False,
    )

    if not log_file.exists():
        console.print(
            "[yellow]⏳ Waiting for log file to be created...[/yellow]",
            highlight=False,
        )
        while not log_file.exists():
            time.sleep(1)
        console.print("[green]✓ Log file found![/green]\n")

    # Set up graceful shutdown
    running = True

    def signal_handler(sig, frame):
        nonlocal running
        running = False
        console.print("\n[bold red]🛑 Logger stopped by user[/bold red]")
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    console.print(
        "[bold cyan]📡 Live log stream started — waiting for entries...[/bold cyan]\n"
    )

    try:
        for line in tail_file(log_file, follow=True):
            if not running:
                break

            line = line.strip()
            if not line:
                continue

            # Apply keyword filter if specified
            if filter_keywords:
                lower_line = line.lower()
                if not any(kw.lower() in lower_line for kw in filter_keywords):
                    continue

            # Categorize and format the line
            prefix, style = categorize_log_line(line)

            if use_colors:
                # Create styled text with prefix
                text = Text()
                text.append(f"{prefix} ", style=style)
                text.append(line)
                console.print(text)
            else:
                console.print(f"{prefix} {line}")

            # Small delay for readability
            time.sleep(0.02)

    except KeyboardInterrupt:
        console.print("\n[bold red]🛑 Logger stopped by user[/bold red]")
    except FileNotFoundError:
        console.print(f"[red]Error: Log file not found: {log_file}[/red]")
        sys.exit(1)
    except PermissionError:
        console.print(f"[red]Error: Permission denied reading: {log_file}[/red]")
        sys.exit(1)


def launch_in_new_terminal(log_file: Path | None = None) -> None:
    """Launch the terminal logger in a new terminal window (macOS/Linux).

    This opens a new Terminal.app window on macOS or xterm on Linux
    running the polybot logs command.

    Args:
        log_file: Optional custom log file path.
    """
    import subprocess
    import shutil

    cmd_args = ["polybot", "logs"]
    if log_file:
        cmd_args.extend(["--log-file", str(log_file)])

    cmd_str = " ".join(cmd_args)

    if sys.platform == "darwin":
        # macOS - use Terminal.app
        apple_script = f"""
        tell application "Terminal"
            activate
            do script "{cmd_str}"
        end tell
        """
        subprocess.Popen(["osascript", "-e", apple_script])
        print(f"✓ Opened new Terminal window running: {cmd_str}")

    elif sys.platform.startswith("linux"):
        # Linux - try common terminal emulators
        terminals = [
            (
                "gnome-terminal",
                ["gnome-terminal", "--", "bash", "-c", f"{cmd_str}; exec bash"],
            ),
            ("konsole", ["konsole", "-e", "bash", "-c", f"{cmd_str}; exec bash"]),
            (
                "xfce4-terminal",
                ["xfce4-terminal", "-e", f"bash -c '{cmd_str}; exec bash'"],
            ),
            ("xterm", ["xterm", "-e", f"bash -c '{cmd_str}; exec bash'"]),
        ]

        for name, term_cmd in terminals:
            if shutil.which(name):
                subprocess.Popen(term_cmd)
                print(f"✓ Opened {name} running: {cmd_str}")
                return

        print("Error: No supported terminal emulator found.")
        print("Please run 'polybot logs' manually in a new terminal.")
        sys.exit(1)

    else:
        print(f"Unsupported platform: {sys.platform}")
        print(f"Please run '{cmd_str}' manually in a new terminal.")
        sys.exit(1)


if __name__ == "__main__":
    # Allow running directly for testing
    run_terminal_logger()
