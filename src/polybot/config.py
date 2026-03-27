"""Unified configuration for PolyBot.

Loads from environment variables and .env file. All settings are validated
with pydantic-settings v2 and have sensible defaults for dry-run mode.

Configuration Philosophy (March 2026):
- HARDCODED: Contract addresses, chain ID, Polymarket host, signal engine defaults,
  fee configuration - these never change and don't need env vars.
- RAILWAY REQUIRED: Private keys, wallet address, Alchemy API key, DRY_RUN, MODE
- FINE-TUNING: Scan intervals, position sizes, risk limits - for advanced users

All sensitive values (API keys, tokens, passwords, private keys) use SecretStr
for secure handling and to prevent accidental logging of credentials.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Literal, Type, Tuple

from pydantic import Field, SecretStr, computed_field, field_validator
from pydantic_settings import (
    BaseSettings,
    SettingsConfigDict,
    PydanticBaseSettingsSource,
)
from pydantic_settings.sources import EnvSettingsSource, DotEnvSettingsSource

logger = logging.getLogger(__name__)


def _parse_comma_separated_list(value: Any) -> list[str]:
    """Parse comma-separated string or list into uppercase list.

    Used by both custom env sources and field validator to ensure consistent parsing.
    """
    if value is None or value == "":
        return []
    if isinstance(value, list):
        return [s.upper() if isinstance(s, str) else str(s).upper() for s in value if s]
    if isinstance(value, str):
        value = value.strip()
        if value.startswith("["):
            # JSON array
            return [s.upper() for s in json.loads(value) if s]
        # Comma-separated string
        return [s.strip().upper() for s in value.split(",") if s.strip()]
    return []


# Fields that should be parsed as comma-separated lists
_COMMA_SEPARATED_FIELDS = {"target_symbols", "copy_trader_addresses"}


class CustomEnvSettingsSource(EnvSettingsSource):
    """Custom env source that handles comma-separated lists properly."""

    def prepare_field_value(
        self, field_name: str, field: Any, value: Any, value_is_complex: bool
    ) -> Any:
        """Parse comma-separated list fields before JSON parsing fails."""
        if field_name in _COMMA_SEPARATED_FIELDS and isinstance(value, str):
            return _parse_comma_separated_list(value)
        return super().prepare_field_value(field_name, field, value, value_is_complex)


class CustomDotEnvSettingsSource(DotEnvSettingsSource):
    """Custom dotenv source that handles comma-separated lists properly."""

    def prepare_field_value(
        self, field_name: str, field: Any, value: Any, value_is_complex: bool
    ) -> Any:
        """Parse comma-separated list fields before JSON parsing fails."""
        if field_name in _COMMA_SEPARATED_FIELDS and isinstance(value, str):
            return _parse_comma_separated_list(value)
        return super().prepare_field_value(field_name, field, value, value_is_complex)


# ═══════════════════════════════════════════════════════════════════════════════
#                     HARDCODED CONSTANTS (NEVER CHANGE)
# ═══════════════════════════════════════════════════════════════════════════════

# Polygon Mainnet Contract Addresses (permanent)
USDC_ADDRESS_HARDCODED = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
USDC_NATIVE_HARDCODED = "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359"
CTF_EXCHANGE_HARDCODED = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"
NEG_RISK_EXCHANGE_HARDCODED = "0xC5d563A36AE78145C45a50134d48A1215220f80a"
NEG_RISK_ADAPTER_HARDCODED = "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296"
CONDITIONAL_TOKENS_HARDCODED = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"

# Network Constants (Polygon Mainnet)
CHAIN_ID_HARDCODED = 137
POLYMARKET_HOST_HARDCODED = "https://clob.polymarket.com"

# Default RPC URLs (used as fallback)
DEFAULT_RPC_URL = "https://polygon-rpc.com"
SOLANA_RPC_DEFAULT = "https://api.mainnet-beta.solana.com"
JUPITER_API_DEFAULT = "https://quote-api.jup.ag/v6"

# Signal Engine Defaults (technical indicator settings)
SIGNAL_DEFAULTS = {
    "crypto_id": "bitcoin",
    "short_window": 5,
    "long_window": 20,
    "rsi_period": 14,
    "rsi_overbought": 70,
    "rsi_oversold": 30,
    "macd_fast": 12,
    "macd_slow": 26,
    "macd_signal": 9,
    "signal_weights": {
        "ma_crossover": 27,
        "rsi": 27,
        "macd": 23,
        "momentum": 13,
        "volume_momentum": 10,
    },
}

# Scanner Defaults
SCANNER_DEFAULTS = {
    "min_volume": 10_000,
    "min_deviation_pct": 12.0,
    "top_count": 8,
    "prioritize_politics": True,
    "target_symbols": ["BTC", "ETH", "SOL"],
}

# Fee Configuration (Polymarket fees - rarely change)
FEE_DEFAULTS = {
    "maker_fee_bps": 0,
    "taker_fee_bps": 10,
    "negrisk_funding_daily_bps": 2,
}

# Kelly Criterion Defaults (position sizing math)
# EXECUTION FIX v3: min_trade_usd lowered to 1.0
KELLY_DEFAULTS = {
    "avg_win_pct": 0.07,
    "avg_loss_pct": 0.04,
    "max_fraction": 0.25,
    "sizing_multiplier": 0.5,  # Half-Kelly
    "min_trade_usd": 1.0,  # EXECUTION FIX v3: Lowered from 3.0
}

# Solana Bridge Defaults
SOLANA_DEFAULTS = {
    "min_balance_usdc": 20.0,
    "bridge_amount": 50.0,
    "swap_threshold": 0.1,
    "slippage_bps": 50,
}

# Arbitrage Defaults
ARB_DEFAULTS = {
    "min_profit": 0.005,
    "max_position": 100.0,
    "min_liquidity": 10_000,
    "max_days": 7,
    "ws_connections": 6,
}

# ═══════════════════════════════════════════════════════════════════════════════
#                     RISK MANAGEMENT THRESHOLDS (Legacy Constants)
# ═══════════════════════════════════════════════════════════════════════════════

# NOTE: These module-level constants are deprecated.
# Use config.min_balance_usd, config.min_trade_size_usd, etc. instead.
# Kept for backwards compatibility with existing imports.
MIN_BALANCE_USD = 0.3  # V78: Lowered from 0.5 for final-balance trading
MIN_TRADE_SIZE_USD = 1.0  # EXECUTION FIX v3: Lowered from 5.0


class Settings(BaseSettings):
    """All bot configuration in one place.

    Sensitive fields use SecretStr to prevent accidental exposure in logs.
    Access secret values with .get_secret_value() method.

    Configuration is split into:
    1. RAILWAY REQUIRED: Must be set in Railway environment
    2. FINE-TUNING: Optional adjustments for power users
    3. HARDCODED: Fixed values accessed via properties/constants
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        # Validate default values
        validate_default=True,
    )

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: Type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> Tuple[PydanticBaseSettingsSource, ...]:
        """Use custom env/dotenv sources that handle comma-separated lists."""
        return (
            init_settings,
            CustomEnvSettingsSource(settings_cls),
            CustomDotEnvSettingsSource(settings_cls, env_file=".env"),
            file_secret_settings,
        )

    # ═══════════════════════════════════════════════════════════════════════════
    #  🚨 RAILWAY REQUIRED - Must be set in Railway environment
    # ═══════════════════════════════════════════════════════════════════════════

    # ── PRODUCTION DEFAULTS – Bot tradet jetzt wirklich ────────────
    # PATCH 2026: Changed defaults for production trading
    dry_run: bool = Field(
        default=False,  # PATCH 2026: Bot now really trades
        description="Live trading mode (set DRY_RUN=true to simulate)",
        alias="DRY_RUN",
    )
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = Field(
        default="DEBUG",  # PATCH 2026: Detailed logging for production
        description="Logging level",
        alias="LOG_LEVEL",
    )

    # ── Kelly & Trade-Execution (Auto-Trade v2) ────────────────────
    kelly_multiplier: float = Field(
        default=0.5,
        ge=0.1,
        le=1.0,
        description="Kelly fraction multiplier: 0.5=Half-Kelly (safer), 1.0=Full-Kelly (aggressive)",
        alias="KELLY_MULTIPLIER",
    )
    max_position_usd: float = Field(
        default=50.0,
        description="Hard cap per trade in USD",
        ge=1.0,
        alias="MAX_POSITION_USD",
    )
    min_trade_usd: float = Field(
        default=5.0,
        description="Minimum trade size in USD",
        ge=1.0,
        alias="MIN_TRADE_USD",
    )
    auto_execute: bool = Field(
        default=True,  # PATCH 2026: Bot now auto-executes trades
        description="Enable auto-trade execution (requires DRY_RUN=false for real trades)",
        alias="AUTO_EXECUTE",
    )
    execution_slippage_bps: int = Field(
        default=30,
        description="Slippage tolerance in basis points (100 bps = 1%)",
        ge=0,
        le=500,
        alias="EXECUTION_SLIPPAGE_BPS",
    )

    mode: Literal[
        "signal", "copy", "arbitrage", "all", "updown", "backtest", "hyperopt"
    ] = Field(
        default="updown",  # PATCH 2026: Default to 5min Up/Down crypto trading
        description="Operating mode: signal | copy | arbitrage | all | updown | backtest | hyperopt",
        alias="MODE",
    )

    # ── PRODUCTION DEFAULTS – 5min Up/Down Filter ──────────────────
    # PATCH 2026: These fields enable strong pre-filtering for production
    up_down_only: bool = Field(
        default=True,  # PATCH 2026: Only trade 5min Up/Down markets
        description="Filter for 5-minute Up/Down crypto markets only",
        alias="UP_DOWN_ONLY",
    )
    # TEST PATCH V9.1 – nur zum Testen (kann später wieder auf 0.010 gesetzt werden)
    min_ev: float = Field(
        default=0.005,  # ← TEST: Temporär auf 0.5% gesenkt, um zu sehen, ob Trades kommen
        description="TEST PATCH: Minimum Expected Value (0.005 = 0.5%) – nur für Testzwecke",
        ge=0.0,
        le=1.0,
        alias="MIN_EV",
    )
    max_daily_risk_usd: float = Field(
        default=100.0,
        description="Maximales Risiko pro Tag (in USD) – verhindert Over-Trading",
        alias="MAX_DAILY_RISK_USD",
    )
    aggressive_mode: bool = Field(
        default=True,
        description="V6: Aktiviert extra-aggressive Logik",
        alias="AGGRESSIVE_MODE",
    )

    # ── V7 BONUS: Auto-Position-Scaling + Daily Reset ──────────────────
    position_scaling_factor: float = Field(
        default=1.0,
        description="Auto-Scaling Faktor (1.0 = normal, 1.5 = aggressiver)",
        ge=0.1,
        le=5.0,
        alias="POSITION_SCALING_FACTOR",
    )
    daily_risk_reset_hour: int = Field(
        default=0,
        description="UTC Stunde für täglichen Risk-Reset (0 = Mitternacht)",
        ge=0,
        le=23,
        alias="DAILY_RISK_RESET_HOUR",
    )

    # ── V8 BONUS: 100% INTERNAL FEATURES ───────────────────────────
    trade_journal_path: str = Field(
        default="/app/data/trade_journal.json",
        description="Interner Trade-Journal (JSON) – 100% lokal",
        alias="TRADE_JOURNAL_PATH",
    )

    # ── V9 FINAL: Pure internal extras ─────────────────────────────
    daily_summary_txt: str = Field(
        default="/app/data/daily_summary.txt",
        description="Täglicher Text-Summary – 100% lokal",
        alias="DAILY_SUMMARY_TXT",
    )
    adaptive_scaling: bool = Field(
        default=True,
        description="V9: Position automatisch an vergangene Performance anpassen",
        alias="ADAPTIVE_SCALING",
    )

    min_volume_usd: float = Field(
        default=100.0,  # PATCH 2026: $100 min volume for production
        description="Minimum 24h volume in USD for market to be tradeable",
        ge=0.0,
        alias="MIN_VOLUME_USD",
    )

    # ── Backtesting for Edge-Strategien ────────────────────────────
    backtest_days: int = Field(
        default=30,
        description="Tage zurück für Backtest (closed markets)",
        ge=1,
        le=365,
        alias="BACKTEST_DAYS",
    )
    backtest_mode: Literal["edge", "full", "walkforward"] = Field(
        default="edge",
        description="edge = nur EdgeEngine, full = mit Execution, walkforward = rolling",
        alias="BACKTEST_MODE",
    )
    backtest_min_liquidity: float = Field(
        default=200.0,
        description="Minimum Liquidität für Backtest-Märkte",
        ge=0.0,
        alias="BACKTEST_MIN_LIQUIDITY",
    )
    backtest_commission_bps: float = Field(
        default=20.0,
        description="Polymarket Gebühren in Basispunkten",
        ge=0.0,
        alias="BACKTEST_COMMISSION_BPS",
    )
    backtest_output_csv: str = Field(
        default="backtest_results_edge.csv",
        description="CSV-Datei für Backtest-Ergebnisse",
        alias="BACKTEST_OUTPUT_CSV",
    )

    # ── Hyperparameter-Optimierung (Walk-Forward) ──────────────────
    hyperopt_enabled: bool = Field(
        default=False,
        description="True = Grid-Search + Walk-Forward Optimization aktivieren",
        alias="HYPEROPT_ENABLED",
    )
    hyperopt_params: dict = Field(
        default={
            "MIN_EV": [0.010, 0.015, 0.020, 0.025],
            "KELLY_MULTIPLIER": [0.3, 0.5, 0.7],
            "MAX_POSITION_USD": [30, 50, 75],
            "MIN_TRADE_USD": [5, 10],
            "backtest_min_liquidity": [150, 200, 300],
        },
        description="Parameter-Grid für Hyperopt (Schlüssel = Setting, Werte = zu testende Werte)",
        alias="HYPEROPT_PARAMS",
    )
    walkforward_windows: int = Field(
        default=5,
        description="Anzahl rollender Walk-Forward Windows für Hyperopt",
        ge=1,
        le=20,
        alias="WALKFORWARD_WINDOWS",
    )

    # ── Optuna Sampler Configuration ───────────────────────────────
    optuna_sampler: Literal["tpe", "cmaes", "random", "grid"] = Field(
        default="tpe",
        description="Optuna sampler: tpe (Bayesian), cmaes (evolutionary), random, grid",
        alias="OPTUNA_SAMPLER",
    )
    optuna_n_trials: int = Field(
        default=50,
        description="Number of Optuna trials per walk-forward window",
        ge=1,
        le=1000,
        alias="OPTUNA_N_TRIALS",
    )
    optuna_direction: Literal["maximize", "minimize"] = Field(
        default="maximize",
        description="Optimization direction: maximize or minimize the objective",
        alias="OPTUNA_DIRECTION",
    )

    # ── Optuna Visualisierungen + Auto-Apply ───────────────────────
    optuna_viz_enabled: bool = Field(
        default=True,
        description="True = erstellt HTML/PNG Plots nach Optuna Optimierung",
        alias="OPTUNA_VIZ_ENABLED",
    )
    optuna_viz_formats: list[str] = Field(
        default=["html", "png"],
        description="Output-Formate für Plots: html (interaktiv) + png (statisch)",
        alias="OPTUNA_VIZ_FORMATS",
    )
    auto_apply_best: bool = Field(
        default=True,
        description="True = erzeugt best_params.env + best_config.json nach Optimierung",
        alias="AUTO_APPLY_BEST",
    )
    viz_dir: str = Field(
        default="/app/static/optuna_viz",
        description="Persistent directory for Optuna visualizations (Railway Static Assets)",
        alias="VIZ_DIR",
    )

    # ── Credentials (MUST SET ON RAILWAY) ──────────────────────────
    polygon_private_key: SecretStr = Field(
        default=SecretStr(""),
        description="Polygon wallet private key (hex with or without 0x prefix)",
    )
    wallet_address: str = Field(default="", description="Polygon wallet address")
    alchemy_api_key: SecretStr = Field(
        default=SecretStr(""),
        description="Alchemy API key for Polygon RPC (get free at https://alchemy.com/)",
    )
    # Optional: Solana for auto-funding
    solana_private_key: SecretStr = Field(
        default=SecretStr(""),
        description="Solana wallet private key (base58 encoded, optional)",
    )

    # ── RPC & Approval Settings ────────────────────────────────────
    polygon_rpc_url_override: str = Field(
        default="https://polygon-rpc.com",
        description="Primary Polygon RPC URL for fallback (overridden by Alchemy if configured)",
        alias="POLYGON_RPC_URL",
    )
    force_alchemy: bool = Field(
        default=True,
        description="Require Alchemy RPC (true = warn + use fast fallback if missing)",
    )
    auto_approve_enabled: bool = Field(
        default=True,
        description="Auto-approve USDC allowance at startup if below threshold",
    )
    min_allowance_usdc: float = Field(
        default=10000.0,
        description="Minimum USDC allowance threshold for auto-approve (default 10,000)",
        ge=0,
    )

    # ═══════════════════════════════════════════════════════════════════════════
    #  ⚙️ FINE-TUNING - Adjustable parameters for power users
    # ═══════════════════════════════════════════════════════════════════════════

    # ── Timing & Scan Intervals ────────────────────────────────────
    scan_interval_seconds: int = Field(
        default=12,
        alias="SCAN_INTERVAL_SECONDS",
        description="Scan-Intervall in Sekunden (5–300s). Vorsicht: <10s erhöht RPC-Kosten & Rate-Limit-Risiko!",
        ge=5,
        le=300,
    )
    min_confidence: int = Field(
        default=68,
        description="Minimum signal confidence to trade (0-100)",
        ge=0,
        le=100,
    )

    # ── Position Sizing & Limits ───────────────────────────────────
    trade_amount: float = Field(
        default=5.0,
        description="Base trade amount in USD",
        ge=0,
    )
    max_order_size_usd: float = Field(
        default=100.0, description="Maximum order size in USD", ge=0
    )
    min_order_size_usd: float = Field(
        default=1.0, description="Minimum order size in USD", ge=0
    )
    max_daily_trades: int = Field(
        default=30,
        description="Maximum number of trades per day",
        ge=1,
    )
    min_liquidity_usd: float = Field(
        default=200.0,  # PATCH 2026: $200 min liquidity for production
        description="Minimum market liquidity in USD",
        ge=0,
        alias="MIN_LIQUIDITY_USD",
    )

    # ── Risk Management ────────────────────────────────────────────
    max_daily_loss: float = Field(
        default=999.0,  # FORCED EXECUTION v5: Increased from 25
        description="Maximum daily loss limit in USD (FORCED EXECUTION v5: high limit)",
        ge=0,
    )
    max_position_size_pct: float = Field(
        default=30.0,
        description="Maximum position size as % of balance",
        ge=0,
        le=100,
    )
    max_drawdown_pct: float = Field(
        default=25.0,
        description="Maximum drawdown percentage to trigger circuit breaker",
        ge=0,
        le=100,
    )
    max_concurrent_positions: int = Field(
        default=3,
        description="Maximale gleichzeitige offene Positionen (Drawdown-Schutz)",
        ge=1,
        le=10,
        alias="MAX_CONCURRENT_POSITIONS",
    )
    circuit_breaker_consecutive_losses: int = Field(
        default=20,  # FORCED EXECUTION v5: Increased from 3
        description="Consecutive losses to trigger circuit breaker (FORCED EXECUTION v5: high limit)",
        ge=1,
    )
    max_category_exposure: float = Field(
        default=50.0,
        description="Maximum category exposure as %",
        ge=0,
        le=100,
    )

    # ── Trading Filters (Railway Environment Variables) ────────────
    min_balance_usd_env: float = Field(
        default=0.3,  # V78: Lowered from 0.5 for final-balance trading
        description="Minimum wallet balance in USD",
        ge=0,
        alias="MIN_BALANCE_USD",
    )
    min_trade_size_usd_env: float = Field(
        default=1.0,  # EXECUTION FIX v3: Lowered from 5.0
        description="Minimum trade size in USD (EXECUTION FIX v3: lowered threshold)",
        ge=0,
        alias="MIN_TRADE_SIZE_USD",
    )
    min_edge_percent: float = Field(
        default=0.85,
        description="Minimum edge percentage for trades",
        ge=0,
        le=100,
        alias="MIN_EDGE_PERCENT",
    )
    max_risk_per_trade: float = Field(
        default=2.0,
        description="Maximum risk per trade as percentage of balance (0-100)",
        ge=0,
        le=100,
        alias="MAX_RISK_PER_TRADE",
    )
    min_confidence_filter: float = Field(
        default=0.65,
        description="Minimum confidence threshold for trades (0.0-1.0)",
        ge=0.0,
        le=1.0,
        alias="MIN_CONFIDENCE_FILTER",
    )
    usdc_address_env: str = Field(
        default="0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",
        description="USDC contract address on Polygon (can override for testing)",
        alias="USDC_ADDRESS",
    )

    # ── Target Markets ─────────────────────────────────────────────
    target_symbols: list[str] = Field(
        default=["BTC", "ETH", "SOL"],
        description="Only scan these symbols for 5min Up/Down markets",
    )

    # ── Copy Trading (optional) ────────────────────────────────────
    copy_trader_addresses: list[str] = Field(
        default_factory=list,
        description="Trader addresses to copy (comma-separated)",
    )
    copy_size: float = Field(default=10.0, description="Copy trade size in USD", ge=0)
    trade_multiplier: float = Field(
        default=1.0, description="Trade size multiplier", ge=0
    )
    tiered_multipliers: str = Field(
        default="",
        description="Tiered multipliers (e.g., '1-100:2.0,100-1000:0.5,1000+:0.1')",
    )

    # ── Proxy (optional for geo-bypass) ────────────────────────────
    socks5_proxy_host: str = Field(default="", description="SOCKS5 proxy hostname")
    socks5_proxy_port: int = Field(
        default=0,
        description="SOCKS5 proxy port",
        ge=0,
        le=65535,
    )
    socks5_proxy_user: str = Field(default="", description="SOCKS5 proxy username")
    socks5_proxy_pass: SecretStr = Field(
        default=SecretStr(""),
        description="SOCKS5 proxy password",
    )
    proxy_pool: str = Field(
        default="",
        description="Comma-separated proxy URLs for rotation",
    )

    # ── Dashboard Auth (optional) ──────────────────────────────────
    dashboard_password: SecretStr = Field(
        default=SecretStr(""),
        description="Dashboard login password (leave empty to disable auth)",
    )

    # ── Full Redeemer V89 Settings ─────────────────────────────────
    # Independent on-chain redemption module that finds ALL redeemable positions
    # regardless of internal bot state (survives restarts, state loss)
    full_redeem_enabled: bool = Field(
        default=False,
        description="Enable independent full redeem scanner (scans on-chain + Data API)",
        alias="FULL_REDEEM_ENABLED",
    )
    full_redeem_interval_seconds: int = Field(
        default=45,
        description="Full redeem scan interval in seconds",
        ge=10,
        le=600,
        alias="FULL_REDEEM_INTERVAL_SECONDS",
    )
    min_redeem_balance: float = Field(
        default=0.08,
        description="Minimum USDC balance for redeem (only need gas, much lower than trading)",
        ge=0.0,
        alias="MIN_REDEEM_BALANCE",
    )
    redeem_gas_buffer_percent: int = Field(
        default=30,
        description="Gas buffer percentage for redeem transactions",
        ge=0,
        le=100,
        alias="REDEEM_GAS_BUFFER_PERCENT",
    )

    # ═══════════════════════════════════════════════════════════════════════════
    #  📌 HARDCODED PROPERTIES - Fixed values, no env vars needed
    # ═══════════════════════════════════════════════════════════════════════════

    # Contract addresses - read-only properties returning constants
    @property
    def usdc_address(self) -> str:
        """USDC.e contract address on Polygon (configurable via USDC_ADDRESS env)."""
        return self.usdc_address_env

    @property
    def usdc_native(self) -> str:
        """Native USDC contract address on Polygon (hardcoded)."""
        return USDC_NATIVE_HARDCODED

    @property
    def ctf_exchange(self) -> str:
        """CTF Exchange contract address (hardcoded)."""
        return CTF_EXCHANGE_HARDCODED

    @property
    def neg_risk_exchange(self) -> str:
        """NegRisk Exchange contract address (hardcoded)."""
        return NEG_RISK_EXCHANGE_HARDCODED

    @property
    def neg_risk_adapter(self) -> str:
        """NegRisk Adapter contract address (hardcoded)."""
        return NEG_RISK_ADAPTER_HARDCODED

    @property
    def conditional_tokens(self) -> str:
        """Conditional Tokens contract address (hardcoded)."""
        return CONDITIONAL_TOKENS_HARDCODED

    @property
    def chain_id(self) -> int:
        """Polygon mainnet chain ID (hardcoded)."""
        return CHAIN_ID_HARDCODED

    @property
    def polymarket_host(self) -> str:
        """Polymarket CLOB API host (hardcoded)."""
        return POLYMARKET_HOST_HARDCODED

    @property
    def solana_rpc_url(self) -> str:
        """Solana RPC URL (hardcoded)."""
        return SOLANA_RPC_DEFAULT

    @property
    def jupiter_api_url(self) -> str:
        """Jupiter DEX API URL (hardcoded)."""
        return JUPITER_API_DEFAULT

    @property
    def db_path(self) -> str:
        """SQLite database path (hardcoded)."""
        return "polybot.db"

    @property
    def dashboard_port(self) -> int:
        """Dashboard HTTP port (hardcoded)."""
        return 8080

    @property
    def dashboard_username(self) -> str:
        """Dashboard login username (hardcoded)."""
        return "admin"

    # Signal Engine defaults (hardcoded)
    @property
    def crypto_id(self) -> str:
        return SIGNAL_DEFAULTS["crypto_id"]

    @property
    def short_window(self) -> int:
        return SIGNAL_DEFAULTS["short_window"]

    @property
    def long_window(self) -> int:
        return SIGNAL_DEFAULTS["long_window"]

    @property
    def rsi_period(self) -> int:
        return SIGNAL_DEFAULTS["rsi_period"]

    @property
    def rsi_overbought(self) -> int:
        return SIGNAL_DEFAULTS["rsi_overbought"]

    @property
    def rsi_oversold(self) -> int:
        return SIGNAL_DEFAULTS["rsi_oversold"]

    @property
    def macd_fast(self) -> int:
        return SIGNAL_DEFAULTS["macd_fast"]

    @property
    def macd_slow(self) -> int:
        return SIGNAL_DEFAULTS["macd_slow"]

    @property
    def macd_signal(self) -> int:
        return SIGNAL_DEFAULTS["macd_signal"]

    @property
    def signal_weights(self) -> dict[str, int]:
        return SIGNAL_DEFAULTS["signal_weights"].copy()

    # Scanner defaults (hardcoded)
    @property
    def scanner_min_volume(self) -> float:
        return SCANNER_DEFAULTS["min_volume"]

    @property
    def scanner_min_deviation_pct(self) -> float:
        return SCANNER_DEFAULTS["min_deviation_pct"]

    @property
    def scanner_top_count(self) -> int:
        return SCANNER_DEFAULTS["top_count"]

    @property
    def scanner_prioritize_politics(self) -> bool:
        return SCANNER_DEFAULTS["prioritize_politics"]

    # Fee defaults (hardcoded)
    @property
    def maker_fee_bps(self) -> int:
        return FEE_DEFAULTS["maker_fee_bps"]

    @property
    def taker_fee_bps(self) -> int:
        return FEE_DEFAULTS["taker_fee_bps"]

    @property
    def negrisk_funding_daily_bps(self) -> int:
        return FEE_DEFAULTS["negrisk_funding_daily_bps"]

    @property
    def pnl_tracking_enabled(self) -> bool:
        return True

    # Kelly defaults (hardcoded)
    @property
    def kelly_avg_win_pct(self) -> float:
        return KELLY_DEFAULTS["avg_win_pct"]

    @property
    def kelly_avg_loss_pct(self) -> float:
        return KELLY_DEFAULTS["avg_loss_pct"]

    @property
    def kelly_max_fraction(self) -> float:
        return KELLY_DEFAULTS["max_fraction"]

    @property
    def kelly_sizing_multiplier(self) -> float:
        return KELLY_DEFAULTS["sizing_multiplier"]

    @property
    def kelly_min_trade_usd(self) -> float:
        return KELLY_DEFAULTS["min_trade_usd"]

    # Solana bridge defaults (hardcoded)
    @property
    def min_poly_balance_usdc(self) -> float:
        return SOLANA_DEFAULTS["min_balance_usdc"]

    @property
    def bridge_fund_amount(self) -> float:
        return SOLANA_DEFAULTS["bridge_amount"]

    @property
    def auto_swap_sol(self) -> bool:
        return True

    @property
    def sol_swap_threshold(self) -> float:
        return SOLANA_DEFAULTS["swap_threshold"]

    @property
    def sol_swap_slippage_bps(self) -> int:
        return SOLANA_DEFAULTS["slippage_bps"]

    # Arbitrage defaults (hardcoded)
    @property
    def arb_min_profit_threshold(self) -> float:
        return ARB_DEFAULTS["min_profit"]

    @property
    def arb_max_position_size(self) -> float:
        return ARB_DEFAULTS["max_position"]

    @property
    def arb_min_liquidity_usd(self) -> float:
        return ARB_DEFAULTS["min_liquidity"]

    @property
    def arb_max_days_resolution(self) -> int:
        return ARB_DEFAULTS["max_days"]

    @property
    def arb_num_ws_connections(self) -> int:
        return ARB_DEFAULTS["ws_connections"]

    # Risk management thresholds (now configurable via Railway ENV)
    @property
    def min_balance_usd(self) -> float:
        """Minimum wallet balance threshold in USD (warns if below)."""
        return self.min_balance_usd_env

    @property
    def min_trade_size_usd(self) -> float:
        """Minimum trade size threshold in USD (warns if below)."""
        return self.min_trade_size_usd_env

    # Other hardcoded defaults
    @property
    def onchain_mode(self) -> bool:
        return True  # Always use onchain mode (auto-derive L2 creds)

    @property
    def copy_strategy(self) -> str:
        return "PERCENTAGE"

    @property
    def smart_scan_enabled(self) -> bool:
        return True

    @property
    def high_frequency_interval(self) -> int:
        return 15

    @property
    def normal_interval(self) -> int:
        return 45

    @property
    def auto_approve(self) -> bool:
        """Whether to auto-approve USDC allowance at startup."""
        return self.auto_approve_enabled

    @property
    def residential_proxy_pool(self) -> str:
        return ""

    @property
    def proxy_rotation_enabled(self) -> bool:
        return True

    @property
    def use_api_mirrors(self) -> bool:
        return True

    # L2 API credentials (auto-derived, not needed as env vars)
    @property
    def poly_api_key(self) -> SecretStr:
        return SecretStr("")

    @property
    def poly_api_secret(self) -> SecretStr:
        return SecretStr("")

    @property
    def poly_api_passphrase(self) -> SecretStr:
        return SecretStr("")

    # ═══════════════════════════════════════════════════════════════════════════
    #  🔧 COMPUTED FIELDS - Built from other settings
    # ═══════════════════════════════════════════════════════════════════════════

    @computed_field
    @property
    def polygon_rpc_url(self) -> str:
        """Auto-build Alchemy RPC URL from ALCHEMY_API_KEY, or use configured fallback."""
        key = self.alchemy_api_key.get_secret_value()
        if key:
            logger.info(
                "✅ Alchemy Key erkannt – using https://polygon-mainnet.g.alchemy.com/v2/..."
            )
            return f"https://polygon-mainnet.g.alchemy.com/v2/{key}"
        logger.debug("ℹ️ ALCHEMY_API_KEY not set – using POLYGON_RPC_URL fallback")
        return self.polygon_rpc_url_override

    @computed_field
    @property
    def effective_log_level(self) -> int:
        """Convert log_level string to logging module constant.

        Returns numeric log level for comparison in DEBUG checks:
        - logging.DEBUG = 10
        - logging.INFO = 20
        - logging.WARNING = 30
        - logging.ERROR = 40

        Usage: if settings.effective_log_level <= logging.DEBUG: logger.debug(...)
        """
        level_map = {
            "DEBUG": logging.DEBUG,
            "INFO": logging.INFO,
            "WARNING": logging.WARNING,
            "ERROR": logging.ERROR,
        }
        return level_map.get(self.log_level, logging.INFO)

    # ═══════════════════════════════════════════════════════════════════════════
    #  🔧 VALIDATORS & METHODS
    # ═══════════════════════════════════════════════════════════════════════════

    @field_validator("target_symbols", "copy_trader_addresses", mode="before")
    @classmethod
    def validate_comma_separated_list(cls, v: Any) -> list[str]:
        """Parse comma-separated string or list into uppercase list."""
        return _parse_comma_separated_list(v)

    @property
    def private_key_hex(self) -> str:
        pk = self.polygon_private_key.get_secret_value()
        if not pk:
            return ""
        return pk if pk.startswith("0x") else f"0x{pk}"

    @property
    def poll_interval_seconds(self) -> int:
        """Alias for scan_interval_seconds used by config handler."""
        return self.scan_interval_seconds

    @property
    def cycle_interval_seconds(self) -> int:
        """Backwards compatibility alias for scan_interval_seconds."""
        return self.scan_interval_seconds

    @property
    def alchemy_rpc_url(self) -> str | None:
        """Construct Alchemy RPC URL from API key if available."""
        key = self.alchemy_api_key.get_secret_value()
        if not key:
            return None
        return f"https://polygon-mainnet.g.alchemy.com/v2/{key}"

    @property
    def proxy_url(self) -> str | None:
        if not self.socks5_proxy_host or not self.socks5_proxy_port:
            return None
        if self.socks5_proxy_user:
            return (
                f"socks5://{self.socks5_proxy_user}:"
                f"{self.socks5_proxy_pass.get_secret_value()}"
                f"@{self.socks5_proxy_host}:{self.socks5_proxy_port}"
            )
        return f"socks5://{self.socks5_proxy_host}:{self.socks5_proxy_port}"

    def parse_tiered_multipliers(self) -> list[dict]:
        """Parse tiered multiplier string into structured list."""
        if not self.tiered_multipliers:
            return []
        tiers = []
        for part in self.tiered_multipliers.split(","):
            part = part.strip()
            range_str, mult_str = part.split(":")
            multiplier = float(mult_str)
            if range_str.endswith("+"):
                low = float(range_str[:-1])
                tiers.append(
                    {"min": low, "max": float("inf"), "multiplier": multiplier}
                )
            else:
                low, high = range_str.split("-")
                tiers.append(
                    {"min": float(low), "max": float(high), "multiplier": multiplier}
                )
        return tiers


_settings: Settings | None = None


def get_settings() -> Settings:
    """Get or create the global settings singleton."""
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings


def reload_settings() -> Settings:
    """Force reload settings from environment."""
    global _settings
    _settings = Settings()
    return _settings
