"""
Scan Logger for PolyBot

A specialized terminal-based log viewer that displays ONLY scan/market/API
related logs in real-time. Designed to run in a separate terminal window
to provide focused market monitoring without bot noise.

Features:
- Filters for scan/market/API/arbitrage related entries ONLY
- Real-time colorful output with Rich
- Opens in a new macOS Terminal window via osascript
- No interference with the main bot process
- Categorized log entries with emoji prefixes

Usage:
    polybot scan-log                    # Start in current terminal
    polybot scan-log --new-terminal     # Open in new macOS Terminal window
    polybot scan-log --filter arb       # Additional keyword filter
"""

from __future__ import annotations

import signal
import subprocess
import shutil
import sys
import time
from pathlib import Path
from typing import Iterator

from rich.console import Console
from rich.panel import Panel
from rich.text import Text

from polybot.logging_setup import get_scan_log_file_path, get_log_file_path


class ScanCategory:
    """Scan log entry categories with styling."""

    MARKET = ("🏪 [MARKET]", "cyan bold")
    SCAN = ("🔍 [SCAN]", "green bold")
    API = ("🌐 [API]", "blue")
    ARB = ("⚡ [ARB]", "yellow bold")
    PRICE = ("💵 [PRICE]", "magenta")
    VOLUME = ("📊 [VOLUME]", "purple")
    DEVIATION = ("📈 [DEVIATION]", "orange1")
    ERROR = ("❌ [ERROR]", "red bold")
    WARNING = ("⚠️  [WARN]", "dark_orange")
    INFO = ("ℹ️  [INFO]", "white")


# Keywords to category mapping (lowercase for matching)
# Order matters - first match wins
SCAN_KEYWORD_CATEGORIES = {
    # Error/Warning first (highest priority)
    ("error", "exception", "failed", "traceback"): ScanCategory.ERROR,
    ("warning", "warn", "caution"): ScanCategory.WARNING,
    # Specific scan categories
    ("arbitrage", "arb ", "arb_", "spread"): ScanCategory.ARB,
    ("deviation", "mispriced", "underpriced", "overpriced"): ScanCategory.DEVIATION,
    ("volume", "volume24hr", "volumenum"): ScanCategory.VOLUME,
    ("price", "odds", "yes_price", "no_price"): ScanCategory.PRICE,
    ("market", "polymarket", "gamma", "question"): ScanCategory.MARKET,
    ("scan", "scanner", "fetch", "query"): ScanCategory.SCAN,
    ("api", "clob", "endpoint", "request", "response"): ScanCategory.API,
}

# Keywords that indicate this is a scan-related log line
SCAN_FILTER_KEYWORDS = frozenset(
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
        "scanner",
        "yes_price",
        "no_price",
        "category",
        "offset",
    ]
)


def is_scan_related(line: str) -> bool:
    """Check if a log line is scan/market related."""
    lower_line = line.lower()
    return any(kw in lower_line for kw in SCAN_FILTER_KEYWORDS)


def categorize_scan_line(line: str) -> tuple[str, str]:
    """Categorize a scan log line and return (prefix, style) tuple.

    Args:
        line: The log line to categorize

    Returns:
        Tuple of (prefix_with_emoji, rich_style)
    """
    lower_line = line.lower()

    # Check keywords in priority order
    for keywords, category in SCAN_KEYWORD_CATEGORIES.items():
        if any(kw in lower_line for kw in keywords):
            return category

    return ScanCategory.INFO


def tail_file(file_path: Path, follow: bool = True) -> Iterator[str]:
    """Generator that yields lines from a file, optionally following new lines.

    Args:
        file_path: Path to the file to tail
        follow: If True, keep following the file for new content

    Yields:
        Lines from the file
    """
    try:
        import tailer

        with open(file_path, "r", encoding="utf-8", errors="replace") as f:
            if follow:
                yield from tailer.follow(f)
            else:
                for line in f:
                    yield line.rstrip("\n\r")
    except ImportError:
        # Fallback implementation
        with open(file_path, "r", encoding="utf-8", errors="replace") as f:
            f.seek(0, 2)  # Start at end
            while True:
                line = f.readline()
                if line:
                    yield line.rstrip("\n\r")
                elif follow:
                    time.sleep(0.1)
                else:
                    break


def run_scan_logger(
    log_file: Path | None = None,
    filter_keywords: list[str] | None = None,
    use_colors: bool = True,
    use_scan_log: bool = True,
) -> None:
    """Run the scan logger to display market/API logs in real-time.

    Args:
        log_file: Path to log file. Uses scan log if None.
        filter_keywords: Additional keywords to filter for.
        use_colors: Enable colorful output using Rich.
        use_scan_log: If True, use dedicated scan log file (pre-filtered).
    """
    console = Console(color_system="auto" if use_colors else None)

    # Determine log file path
    if log_file is None:
        if use_scan_log:
            log_file = get_scan_log_file_path()
        else:
            log_file = get_log_file_path()

    # Print header
    header = Panel(
        "[bold green]🔍 PolyBot Scan Logger[/bold green]\n"
        "[dim]Real-time market & API monitoring[/dim]",
        border_style="green",
    )
    console.print(header)
    console.print(f"[dim]Log file: {log_file}[/dim]")
    console.print("[dim]Filter: Scan/Market/API/Arbitrage logs only[/dim]")
    if filter_keywords:
        console.print(f"[dim]Additional filters: {', '.join(filter_keywords)}[/dim]")
    console.print("[dim]Press Ctrl+C to stop[/dim]\n")

    # Wait for log file to exist
    if not log_file.exists():
        console.print(
            "[yellow]⏳ Waiting for log file to be created...[/yellow]",
            highlight=False,
        )
        console.print(
            "[dim]Tip: Start the bot in another terminal with 'polybot api' or 'polybot run'[/dim]"
        )
        while not log_file.exists():
            time.sleep(1)
        console.print("[green]✓ Log file found![/green]\n")

    # Set up graceful shutdown
    running = True

    def signal_handler(sig, frame):
        nonlocal running
        running = False
        console.print("\n[bold red]🛑 Scan logger stopped[/bold red]")
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    console.print(
        "[bold cyan]📡 Live scan stream started — monitoring market activity...[/bold cyan]\n"
    )

    line_count = 0
    try:
        for line in tail_file(log_file, follow=True):
            if not running:
                break

            line = line.strip()
            if not line:
                continue

            # If using main log file (not scan-specific), filter here
            if not use_scan_log and not is_scan_related(line):
                continue

            # Apply additional keyword filter if specified
            if filter_keywords:
                lower_line = line.lower()
                if not any(kw.lower() in lower_line for kw in filter_keywords):
                    continue

            # Categorize and format the line
            prefix, style = categorize_scan_line(line)

            if use_colors:
                text = Text()
                text.append(f"{prefix} ", style=style)
                text.append(line)
                console.print(text)
            else:
                console.print(f"{prefix} {line}")

            line_count += 1

            # Small delay for readability
            time.sleep(0.02)

    except KeyboardInterrupt:
        console.print(
            f"\n[bold red]🛑 Scan logger stopped ({line_count} entries shown)[/bold red]"
        )
    except FileNotFoundError:
        console.print(f"[red]Error: Log file not found: {log_file}[/red]")
        sys.exit(1)
    except PermissionError:
        console.print(f"[red]Error: Permission denied reading: {log_file}[/red]")
        sys.exit(1)


def launch_scan_logger_in_new_terminal(
    log_file: Path | None = None,
    filter_keywords: list[str] | None = None,
) -> bool:
    """Launch the scan logger in a new terminal window (macOS/Linux).

    On macOS, opens a new Terminal.app window via osascript.
    On Linux, tries common terminal emulators.

    Args:
        log_file: Optional custom log file path.
        filter_keywords: Optional additional filter keywords.

    Returns:
        True if successfully launched, False otherwise.
    """
    cmd_args = ["polybot", "scan-log"]
    if log_file:
        cmd_args.extend(["--log-file", str(log_file)])
    if filter_keywords:
        for kw in filter_keywords:
            cmd_args.extend(["--filter", kw])

    cmd_str = " ".join(cmd_args)

    if sys.platform == "darwin":
        # macOS - use Terminal.app via osascript
        apple_script = f"""
        tell application "Terminal"
            activate
            set newTab to do script "{cmd_str}"
            set custom title of front window to "PolyBot Scan Logger 🔍"
        end tell
        """
        try:
            subprocess.Popen(
                ["osascript", "-e", apple_script],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            print(f"✓ Opened new Terminal window: {cmd_str}")
            print("  The scan logger will monitor market/API activity in real-time.")
            return True
        except Exception as e:
            print(f"Error launching Terminal: {e}")
            return False

    elif sys.platform.startswith("linux"):
        # Linux - try common terminal emulators
        terminals = [
            (
                "gnome-terminal",
                [
                    "gnome-terminal",
                    "--title=PolyBot Scan Logger",
                    "--",
                    "bash",
                    "-c",
                    f"{cmd_str}; exec bash",
                ],
            ),
            (
                "konsole",
                [
                    "konsole",
                    "--title",
                    "PolyBot Scan Logger",
                    "-e",
                    "bash",
                    "-c",
                    f"{cmd_str}; exec bash",
                ],
            ),
            (
                "xfce4-terminal",
                [
                    "xfce4-terminal",
                    "--title=PolyBot Scan Logger",
                    "-e",
                    f"bash -c '{cmd_str}; exec bash'",
                ],
            ),
            (
                "xterm",
                [
                    "xterm",
                    "-T",
                    "PolyBot Scan Logger",
                    "-e",
                    f"bash -c '{cmd_str}; exec bash'",
                ],
            ),
        ]

        for name, term_cmd in terminals:
            if shutil.which(name):
                try:
                    subprocess.Popen(
                        term_cmd,
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                    )
                    print(f"✓ Opened {name}: {cmd_str}")
                    return True
                except Exception as e:
                    print(f"Error launching {name}: {e}")
                    continue

        print("Error: No supported terminal emulator found.")
        print("Please run 'polybot scan-log' manually in a new terminal.")
        return False

    else:
        print(f"Unsupported platform: {sys.platform}")
        print(f"Please run '{cmd_str}' manually in a new terminal.")
        return False


if __name__ == "__main__":
    # Allow running directly for testing
    run_scan_logger()
