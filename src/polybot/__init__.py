from .config import get_settings, reload_settings
from .indicators import linear_regression_slope, std_dev

# Re-export commonly used submodules for tests and users
from . import proxy
from . import scanner
from . import risk
from . import database
from . import logging_setup
from . import retries
from . import terminal_logger

__version__ = "2.0.0"

__all__ = [
    "get_settings",
    "reload_settings",
    "proxy",
    "scanner",
    "risk",
    "database",
    "logging_setup",
    "retries",
    "terminal_logger",
    "linear_regression_slope",
    "std_dev",
]
