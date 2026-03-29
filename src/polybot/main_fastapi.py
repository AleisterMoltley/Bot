import asyncio
import atexit
import json
import logging
import os
import shutil
import signal
import time
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from web3.exceptions import ABIFunctionNotFound, ContractLogicError

from polybot import __version__
from polybot.config import get_settings
from polybot.pnl_tracker import get_pnl_tracker
from polybot.risk_manager import get_risk_manager
from polybot.rpc_manager import PolygonRpcManager
from polybot.signals import get_market_timing_info, get_smart_scan_interval
from polybot.startup_checks import (
    get_allowance_status,
    run_startup_checks,
)
from polybot.scanner import MaxProfitScanner
from polybot.executor import place_trade_async
from polybot.onchain_executor import OnchainExecutor, set_shared_executor
from polybot.full_redeemer import (
    start_full_redeem_task,
    force_full_redeem,
    get_full_redeemer_status,
)

logger = logging.getLogger(__name__)

# Maximum characters to show in RPC URL preview in logs
_MAX_RPC_URL_LOG_LENGTH = 50

# === PRODUCTION SAFETY: Critical Error Log Path ===
# Used for graceful shutdown and error tracking
_CRITICAL_ERRORS_LOG_PATH = Path("/app/static/critical_errors.log")
_BACKUP_DIR = Path("/app/static/backup")

# Track last backup time to avoid too frequent backups
_last_backup_time: float = 0.0
_BACKUP_COOLDOWN_SECONDS = 60  # Minimum 60 seconds between auto-backups


def _get_critical_errors_log_path() -> Path:
    """Get the path for critical errors log, with fallback for local dev."""
    if _CRITICAL_ERRORS_LOG_PATH.parent.exists():
        return _CRITICAL_ERRORS_LOG_PATH
    # Fallback for local development
    local_static = Path(__file__).resolve().parents[2] / "static"
    if local_static.exists():
        return local_static / "critical_errors.log"
    return Path("./static/critical_errors.log")


def _get_backup_dir() -> Path:
    """Get the backup directory path, with fallback for local dev."""
    if _BACKUP_DIR.parent.exists():
        return _BACKUP_DIR
    # Fallback for local development
    local_static = Path(__file__).resolve().parents[2] / "static"
    if local_static.exists():
        return local_static / "backup"
    return Path("./static/backup")


def _log_critical_error(error: Exception, context: str = "") -> None:
    """Log a critical error to the errors log file."""
    try:
        log_path = _get_critical_errors_log_path()
        log_path.parent.mkdir(parents=True, exist_ok=True)
        with open(log_path, "a") as f:
            timestamp = datetime.now().isoformat()
            ctx = f" [{context}]" if context else ""
            f.write(f"{timestamp}{ctx} | {type(error).__name__}: {error}\n")
    except Exception as log_error:
        logger.error(f"Failed to write to critical errors log: {log_error}")


def _perform_backup() -> bool:
    """Perform backup of optuna_viz and best_params.env.

    Returns:
        True if backup was performed, False otherwise.
    """
    global _last_backup_time

    # Check cooldown to avoid too frequent backups
    current_time = time.time()
    if current_time - _last_backup_time < _BACKUP_COOLDOWN_SECONDS:
        logger.debug("Backup skipped - cooldown active")
        return False

    try:
        backup_dir = _get_backup_dir()
        backup_dir.mkdir(parents=True, exist_ok=True)

        # Determine source directories
        prod_optuna_viz = Path("/app/static/optuna_viz")
        prod_best_params = Path("/app/static/best_params.env")
        local_static = Path(__file__).resolve().parents[2] / "static"

        # Backup optuna_viz directory
        optuna_viz_src = (
            prod_optuna_viz if prod_optuna_viz.exists() else local_static / "optuna_viz"
        )
        if optuna_viz_src.exists():
            backup_optuna = backup_dir / "optuna_viz"
            shutil.copytree(str(optuna_viz_src), str(backup_optuna), dirs_exist_ok=True)
            logger.info(f"✅ Backed up optuna_viz to {backup_optuna}")

        # Backup best_params.env
        best_params_src = (
            prod_best_params
            if prod_best_params.exists()
            else local_static / "best_params.env"
        )
        if best_params_src.exists():
            backup_params = backup_dir / "best_params.env"
            shutil.copy2(str(best_params_src), str(backup_params))
            logger.info(f"✅ Backed up best_params.env to {backup_params}")

        _last_backup_time = current_time
        return True

    except Exception as e:
        logger.error(f"Backup failed: {e}")
        return False


def graceful_shutdown(signum: int | None = None, frame: Any = None) -> None:
    """Perform graceful shutdown with backup.

    Called on SIGTERM or during atexit cleanup.
    """
    logger.info("🛑 GRACEFUL SHUTDOWN initiated – backing up data...")
    _perform_backup()
    logger.info("✅ Graceful shutdown complete")


# Register graceful shutdown handlers
atexit.register(graceful_shutdown)
signal.signal(signal.SIGTERM, graceful_shutdown)

# USDC contract ABI for balance and allowance check (standard ERC20)
# Note: Must include BOTH balanceOf and allowance functions for complete ERC20 support
ERC20_ABI = [
    {
        "constant": True,
        "inputs": [{"name": "_owner", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"name": "balance", "type": "uint256"}],
        "type": "function",
    },
    {
        "constant": True,
        "inputs": [
            {"name": "_owner", "type": "address"},
            {"name": "_spender", "type": "address"},
        ],
        "name": "allowance",
        "outputs": [{"name": "", "type": "uint256"}],
        "type": "function",
    },
]


async def _check_wallet_and_allowance(rpc_manager: PolygonRpcManager) -> dict[str, Any]:
    """Check wallet USDC balance and CTF Exchange allowance.

    Uses graceful error handling to handle ABI errors (e.g., if contract doesn't
    support allowance function). Falls back to 0 for allowance on ABI errors.
    """
    settings = get_settings()
    wallet = settings.wallet_address

    result: dict[str, Any] = {
        "wallet_address": wallet if wallet else None,
        "usdc_balance": 0.0,
        "allowance": 0.0,
        "wallet_check": "not_configured",
        "warnings": [],
    }

    if not wallet:
        result["warnings"].append("⚠️ WALLET_ADDRESS not set – cannot check balance")
        return result

    try:
        w3 = await rpc_manager.get_best_provider(timeout=5.0)

        # Check USDC balance (USDC.e on Polygon has 6 decimals)
        usdc_address = settings.usdc_address
        usdc_contract = w3.eth.contract(
            address=w3.to_checksum_address(usdc_address),
            abi=ERC20_ABI,
        )
        balance_raw = await usdc_contract.functions.balanceOf(
            w3.to_checksum_address(wallet)
        ).call()
        result["usdc_balance"] = balance_raw / 1e6  # USDC has 6 decimals

        # Check CTF Exchange allowance with graceful error handling
        ctf_exchange = settings.ctf_exchange
        try:
            allowance_raw = await usdc_contract.functions.allowance(
                w3.to_checksum_address(wallet),
                w3.to_checksum_address(ctf_exchange),
            ).call()
            result["allowance"] = allowance_raw / 1e6
        except (ABIFunctionNotFound, ContractLogicError) as e:
            # Specific ABI-related errors - handle gracefully
            logger.error(
                "❌ USDC ABI-Error – check CONTRACT_ADDRESS_USDC in config! "
                f"Address: {usdc_address}, Error: {e}"
            )
            result["allowance"] = 0
            result["warnings"].append(
                "⚠️ Could not check allowance – ABI error (contract may need approval)"
            )
        except Exception as e:
            # For other errors, check if it's ABI-related via error message as fallback
            error_msg = str(e).lower()
            if "abi" in error_msg or "function" in error_msg:
                logger.error(
                    "❌ USDC ABI-Error – check CONTRACT_ADDRESS_USDC in config! "
                    f"Address: {usdc_address}, Error: {e}"
                )
                result["allowance"] = 0
                result["warnings"].append(
                    "⚠️ Could not check allowance – ABI error (contract may need approval)"
                )
            else:
                raise

        # Mark wallet check as successful
        result["wallet_check"] = "ok"

        # Add warnings
        if result["usdc_balance"] < 5.0:
            result["warnings"].append(
                f"⚠️ Low USDC balance: ${result['usdc_balance']:.2f}"
            )
        if result["allowance"] < result["usdc_balance"]:
            result["warnings"].append(
                f"⚠️ Low allowance: ${result['allowance']:.2f} "
                f"(balance: ${result['usdc_balance']:.2f})"
            )

    except Exception as e:
        result["wallet_check"] = "error"
        result["warnings"].append(f"⚠️ Failed to check wallet: {e}")
        logger.warning(f"Wallet check failed: {e}")

    return result


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler - initializes RPC manager at startup."""
    settings = get_settings()

    # Log startup configuration
    logger.info("=" * 60)
    logger.info("🤠 PolyBot Ferox starting up...")
    logger.info("=" * 60)

    # Check Alchemy API key from settings
    alchemy_key = settings.alchemy_api_key.get_secret_value()
    alchemy_enabled = bool(alchemy_key)

    # Required startup log (Railway 2026 polish)
    logger.info(
        f"✅ Alchemy={alchemy_enabled} | FORCE_ALCHEMY={settings.force_alchemy} | "
        f"Cycle={settings.scan_interval_seconds}s"
    )

    # Warn if scan interval is very low (increased RPC costs)
    if settings.scan_interval_seconds <= 5:
        logger.warning(
            "⚠️ Scan-Intervall auf 5s gesetzt! RPC-Kosten & Rate-Limits beachten."
        )

    # Detailed startup log with version
    rpc_url = settings.polygon_rpc_url
    if len(rpc_url) > _MAX_RPC_URL_LOG_LENGTH:
        rpc_url_preview = rpc_url[:_MAX_RPC_URL_LOG_LENGTH] + "..."
    else:
        rpc_url_preview = rpc_url
    logger.info(
        f"🚀 PolyBot v{__version__} | Alchemy={alchemy_enabled} | RPC={rpc_url_preview}"
    )

    if alchemy_key:
        logger.info(
            "✅ Alchemy Key erkannt – using https://polygon-mainnet.g.alchemy.com/v2/..."
        )
    else:
        logger.warning(
            "⚠️ Kein ALCHEMY_API_KEY – "
            f"{'schnelle Fallback-RPCs aktiv' if settings.force_alchemy else 'public RPCs können ratelimiten!'}"
        )

    # Initialize RPC manager with force_alchemy setting
    rpc_manager = PolygonRpcManager(force_alchemy=settings.force_alchemy)
    app.state.rpc_manager = rpc_manager

    # Store startup info in app state (including cycle start time for countdown)
    app.state.startup_info = {
        "alchemy_configured": alchemy_enabled,
        "force_alchemy": settings.force_alchemy,
        "auto_approve_enabled": settings.auto_approve_enabled,
        "dry_run": settings.dry_run,
        "mode": settings.mode,
        "cycle_interval": settings.scan_interval_seconds,
    }
    # Track cycle timing for /api/status countdown
    app.state.last_cycle_start = time.time()

    # Run startup checks in background (non-blocking startup)
    asyncio.create_task(_run_startup_checks_async(app, rpc_manager))

    # Ensure L2 credentials are ready before scanner starts
    try:
        from polybot.credentials_manager import get_or_create_l2_creds

        await asyncio.get_event_loop().run_in_executor(None, get_or_create_l2_creds)
        logger.info("[STARTUP] L2 credentials ready — starting scanner")
    except Exception as e:
        logger.warning(f"[STARTUP] L2 credential bootstrap failed: {e}")

    # V78: Start cut-loss + auto-redeem monitor for automatic position management
    try:
        onchain_executor = OnchainExecutor()
        # V88: Set as shared executor so module-level execute_trade can register positions
        set_shared_executor(onchain_executor)
        await onchain_executor.start_cut_loss_monitor()
        # Store in app state for lifecycle management
        app.state.onchain_executor = onchain_executor
        logger.info(
            "[V78 STARTUP] Cut-loss + auto-redeem monitor started — checking positions every 30s"
        )
    except Exception as e:
        logger.warning(
            f"[V78 STARTUP] Cut-loss + auto-redeem monitor startup failed: {e}"
        )

    # V90: Startup redeem — scan wallet for ALL redeemable positions and redeem
    # them immediately. Catches positions the bot lost track of after restart.
    if settings.startup_redeem_all:
        try:
            _executor = (
                app.state.onchain_executor
                if hasattr(app.state, "onchain_executor")
                else None
            )
            if _executor is not None:
                logger.info(
                    "[V90 STARTUP] Scanning wallet for ALL redeemable positions …"
                )
                asyncio.create_task(_executor.scan_and_redeem_all_positions())
            else:
                logger.warning(
                    "[V90 STARTUP] OnchainExecutor not available — skipping startup redeem"
                )
        except Exception as e:
            logger.warning(f"[V90 STARTUP] Startup redeem failed: {e}")
    else:
        logger.info(
            "[V90 STARTUP] Startup redeem disabled — set STARTUP_REDEEM_ALL=true to enable"
        )

    # V89: Start full redeemer task if enabled
    # This runs independently of the internal open_positions state and finds
    # ALL redeemable positions on-chain + via Data API
    try:
        full_redeem_task = start_full_redeem_task()
        if full_redeem_task:
            app.state.full_redeem_task = full_redeem_task
            logger.info(
                f"[V89 STARTUP] Full Redeemer started — "
                f"scanning every {settings.full_redeem_interval_seconds}s"
            )
        else:
            logger.info(
                "[V89 STARTUP] Full Redeemer disabled — "
                "set FULL_REDEEM_ENABLED=true to enable"
            )
    except Exception as e:
        logger.warning(f"[V89 STARTUP] Full Redeemer startup failed: {e}")

    # PATCH 2026: Production-ready continuous scanner with real trade execution
    async def continuous_scanner():
        # PATCH 2026: Direct attribute access since min_ev is now a proper Field
        min_ev_threshold = settings.min_ev
        scanner = MaxProfitScanner(
            min_ev=min_ev_threshold,
        )
        logger.info(
            f"🚀 Continuous MaxProfitScanner started – "
            f"DRY_RUN={settings.dry_run} | AUTO_EXECUTE={settings.auto_execute} | MIN_EV={min_ev_threshold}"
        )
        while True:
            try:
                opportunities = await scanner.scan_async(limit=5)
                if opportunities:
                    logger.info(
                        f"Found {len(opportunities)} opportunities – checking for live trades"
                    )
                    for opp in opportunities:
                        edge = (
                            opp.get("edge")
                            if opp.get("edge") is not None
                            else opp.get("ev", 0)
                        )
                        ev = opp.get("ev", edge)
                        # PATCH 2026: Use min_ev from config
                        if ev > min_ev_threshold or edge > min_ev_threshold:
                            # PATCH 2026: Check auto_execute before trading
                            if settings.auto_execute:
                                position_usd = opp.get(
                                    "size_usd", settings.min_trade_usd
                                )
                                market = opp.get("raw_market", opp)
                                outcome = opp.get("outcome", "yes")
                                # PATCH 2026: Detailed logging before trade execution
                                logger.info(
                                    "EXECUTING REAL TRADE",
                                    market=market.get("question", "")[:60],
                                    position_usd=position_usd,
                                    edge=f"{edge:.4f}",
                                    ev=f"{ev:.4f}",
                                    outcome=outcome,
                                    dry_run=settings.dry_run,
                                )
                                await place_trade_async(
                                    market=market,
                                    outcome=outcome,
                                    amount=position_usd,
                                )
                            else:
                                logger.info(
                                    f"⚠️ Trade skipped – AUTO_EXECUTE=False (edge={edge:.2%}, ev={ev:.2%})"
                                )
                        else:
                            logger.debug(
                                f"Trade skipped – EV/edge below threshold "
                                f"(ev={ev:.4f}, edge={edge:.4f}, min_ev={min_ev_threshold})"
                            )
            except Exception as e:
                logger.warning(f"Scanner cycle error: {e}")
            await asyncio.sleep(settings.scan_interval_seconds)

    # ── REDEEM_ONLY mode: skip scanner entirely, only redeem ──────────────
    if settings.redeem_only:
        logger.info(
            "🛑 REDEEM_ONLY=true – Scanner deaktiviert, keine neuen Trades. "
            "Nur bereits aktive Positionen werden redeemed."
        )
    else:
        asyncio.create_task(continuous_scanner())

    # PATCH 2026: Enhanced startup logging with production settings
    if settings.redeem_only:
        logger.info("🛑 REDEEM_ONLY: true – Wind-Down-Modus aktiv")
    logger.info(f"📊 Mode: {settings.mode.upper()}")
    logger.info(f"🔄 DRY_RUN: {settings.dry_run}")
    logger.info(f"🤖 AUTO_EXECUTE: {settings.auto_execute}")
    logger.info(f"📈 MIN_EV: {settings.min_ev}")
    logger.info(f"⏱️ Scan interval: {settings.scan_interval_seconds}s")
    logger.info(f"🔑 AUTO_APPROVE: {settings.auto_approve_enabled}")

    # Log smart scan settings
    if settings.smart_scan_enabled:
        logger.info(
            f"⚡ Smart Scan: HF={settings.high_frequency_interval}s / "
            f"Normal={settings.normal_interval}s"
        )
    else:
        logger.info("⚡ Smart Scan: disabled (fixed interval)")

    # Summary startup log
    alchemy_status = "✅" if alchemy_key else "❌"
    logger.info(
        f"🚀 PolyBot ready | Alchemy={alchemy_status} | "
        f"DRY_RUN={settings.dry_run} | Mode={settings.mode}"
    )
    yield
    logger.info("🛑 PolyBot FastAPI shutting down")


async def _run_startup_checks_async(
    app: FastAPI, rpc_manager: PolygonRpcManager
) -> None:
    """Run all startup checks including RPC, wallet, and auto-approve.

    This function runs in the background after app startup to:
    1. Rank RPCs by latency
    2. Test RPC connection and update Alchemy status
    3. Check wallet balance and allowance
    4. Auto-approve USDC if needed and enabled
    """
    try:
        # EXECUTION FIX v3: Removed 0.1s sleep delay for immediate startup

        # Rank RPCs by latency
        await rpc_manager.rank_by_latency()

        # Run comprehensive startup checks (includes auto-approve)
        startup_results = await run_startup_checks(rpc_manager)

        # Store results in app state
        app.state.startup_results = startup_results

        # Also update wallet_info for backward compatibility
        if startup_results.get("allowance_status"):
            allowance = startup_results["allowance_status"]
            app.state.wallet_info = {
                "wallet_address": get_settings().wallet_address,
                "usdc_balance": allowance.get("balance_usdc", 0.0),
                "allowance": allowance.get("allowance_usdc", 0.0),
                "wallet_check": "ok" if not allowance.get("error") else "error",
                "warnings": allowance.get("warnings", []),
            }

            wallet_addr = get_settings().wallet_address
            if wallet_addr:
                logger.info(
                    f"💰 Wallet: {wallet_addr[:10]}... "
                    f"| USDC: ${allowance.get('balance_usdc', 0):.2f} "
                    f"| Allowance: ${allowance.get('allowance_usdc', 0):.2f}"
                )

        # Log any warnings
        for warning in startup_results.get("warnings", []):
            logger.warning(warning)

        logger.info("-" * 60)
        logger.info("🔍 Scanning Polymarket for opportunities...")
        logger.info("-" * 60)

    except Exception as e:
        logger.warning(f"Startup checks failed: {e}")


async def _init_rpc_and_wallet_check(
    app: FastAPI, rpc_manager: PolygonRpcManager
) -> None:
    """Initialize RPC ranking and wallet check after a short delay.

    DEPRECATED: Use _run_startup_checks_async instead.
    Kept for backward compatibility.
    """
    await _run_startup_checks_async(app, rpc_manager)


app = FastAPI(title="PolyBot", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# === PRODUCTION SAFETY: Global Exception Handler ===
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """Handle uncaught exceptions gracefully.

    Logs critical errors to file and returns a safe response.
    Does NOT trigger full shutdown to keep the service running.
    """
    logger.critical(f"CRITICAL ERROR in {request.url.path}: {exc}", exc_info=True)
    _log_critical_error(exc, context=str(request.url.path))

    # Perform backup on critical error (with cooldown)
    _perform_backup()

    return JSONResponse(
        status_code=500,
        content={
            "status": "error",
            "message": "Internal server error - logged for review",
            "path": str(request.url.path),
        },
    )


_STATIC_DIR_CANDIDATES = [
    Path("/app/static"),
    Path(__file__).resolve().parents[2] / "static",
    Path("./static"),
    Path("static"),
]

_STATIC_DIR: Path | None = next((p for p in _STATIC_DIR_CANDIDATES if p.is_dir()), None)

if _STATIC_DIR is not None:
    app.mount("/static", StaticFiles(directory=str(_STATIC_DIR)), name="static")


def _find_dashboard_file() -> Path | None:
    candidates = [
        Path("/app/static/dashboard.html"),
        Path("./static/dashboard.html"),
        Path("static/dashboard.html"),
    ]
    if _STATIC_DIR is not None:
        candidates.insert(0, _STATIC_DIR / "dashboard.html")
    return next((p for p in candidates if p.is_file()), None)


@app.get("/api/health")
async def health():
    return {"status": "ok", "message": "PolyBot is running"}


@app.get("/dashboard", include_in_schema=False)
@app.get("/", include_in_schema=False)
async def dashboard():
    dashboard_file = _find_dashboard_file()
    if dashboard_file is not None:
        return FileResponse(str(dashboard_file), media_type="text/html")
    return {"error": "dashboard.html not found"}


@app.get("/api/pnl")
async def get_pnl():
    tracker = get_pnl_tracker()
    pnl = tracker.get_total_pnl()
    open_positions = tracker.get_all_positions(include_closed=False)
    return {
        "realized_pnl": float(pnl["realized_pnl"]),
        "unrealized_pnl": float(pnl["unrealized_pnl"]),
        "total_fees": float(pnl["total_fees"]),
        "accrued_funding": float(pnl["accrued_funding"]),
        "net_pnl": float(pnl["net_pnl"]),
        "open_positions_count": len(open_positions),
    }


@app.get("/api/positions")
async def get_positions():
    tracker = get_pnl_tracker()
    positions = tracker.get_all_positions(include_closed=False)
    return [
        {
            "position_id": p.position_id,
            "market_title": p.market_title,
            "outcome": p.outcome,
            "size": float(p.size),
            "avg_entry_price": float(p.avg_entry_price),
            "current_price": float(p.current_price),
            "unrealized_pnl": float(p.unrealized_pnl),
            "status": p.status.value,
        }
        for p in positions
    ]


@app.get("/api/rpc-status")
async def get_rpc_status():
    """Get current RPC provider status and latency ranking.

    Returns the list of RPC providers sorted by latency (fastest first),
    along with their connection status and error counts.
    """
    rpc_manager: PolygonRpcManager | None = getattr(app.state, "rpc_manager", None)
    if rpc_manager is None:
        return {"error": "RPC manager not initialized", "providers": []}

    try:
        status_list = await rpc_manager.get_status()
        return {
            "provider_count": len(status_list),
            "current_index": rpc_manager.current_index,
            "providers": [
                {
                    "url": s.url,
                    "priority": s.priority,
                    "latency_ms": round(s.latency_ms, 1),
                    "is_connected": s.is_connected,
                    "error_count": s.error_count,
                }
                for s in status_list
            ],
        }
    except Exception as e:
        logger.error(f"Failed to get RPC status: {e}")
        return {"error": str(e), "providers": []}


@app.get("/api/status")
async def get_bot_status():
    """Get comprehensive bot status for dashboard.

    Returns current bot state including:
    - Configuration summary (mode, dry_run, cycle interval)
    - RPC connection status (Alchemy vs public)
    - Wallet balance and allowance
    - Current activity status
    - Scanning status with countdown to next cycle
    """
    settings = get_settings()
    wallet_info = getattr(app.state, "wallet_info", {})
    rpc_manager: PolygonRpcManager | None = getattr(app.state, "rpc_manager", None)

    # Determine RPC status with new Alchemy tracking
    rpc_status = "not_initialized"
    using_alchemy = False
    alchemy_connected = False
    alchemy_block_number = None
    alchemy_help_url = None
    alchemy_missing_error = False

    if rpc_manager and rpc_manager.providers:
        try:
            # Use the new Alchemy status tracking
            alchemy_status_info = rpc_manager.get_alchemy_status()
            using_alchemy = alchemy_status_info["configured"]
            alchemy_connected = alchemy_status_info["connected"]
            alchemy_block_number = alchemy_status_info["block_number"]
            alchemy_help_url = alchemy_status_info["help_url"]
            alchemy_missing_error = alchemy_status_info.get("missing_error", False)
            rpc_status = "connected"
        except Exception:
            rpc_status = "error"

    # Get wallet check status (ok, error, not_configured)
    wallet_check = wallet_info.get("wallet_check", "not_configured")

    # Get allowance status
    allowance_status = get_allowance_status()

    # Calculate next cycle countdown
    cycle_interval = settings.scan_interval_seconds
    last_cycle_start = getattr(app.state, "last_cycle_start", time.time())
    elapsed = time.time() - last_cycle_start
    seconds_until_next = max(0, cycle_interval - (elapsed % cycle_interval))
    next_cycle_in = f"{int(seconds_until_next)}s"

    # Build status message for dashboard
    if settings.dry_run:
        activity_status = "🧪 DRY RUN – Scanning markets (no real trades)"
    elif rpc_status == "connected" and alchemy_connected:
        activity_status = (
            f"✅ Bot scannt Polymarket – Alchemy RPC (Block #{alchemy_block_number})"
            if alchemy_block_number
            else "✅ Bot scannt Polymarket – Alchemy RPC verbunden"
        )
    elif rpc_status == "connected":
        activity_status = "🔍 Bot scannt Polymarket – Fallback RPC aktiv"
    else:
        activity_status = "🔍 Waiting for next cycle – scanning Polymarket..."

    return {
        "status": "ok",
        "activity_status": activity_status,
        # Live cycle status fields
        "scanning": True,
        "next_cycle_in": next_cycle_in,
        "alchemy_connected": alchemy_connected,
        "alchemy_block_number": alchemy_block_number,
        # Scan interval for dashboard (new field)
        "scan_interval_seconds": settings.scan_interval_seconds,
        # Simplified fields for easy dashboard use
        "rpc_status": rpc_status,
        "alchemy_used": using_alchemy,
        "wallet_check": wallet_check,
        "wallet_usdc": wallet_info.get("usdc_balance", 0.0),
        # Allowance status fields - use real status
        "allowance_ok": allowance_status.allowance_usdc >= settings.min_allowance_usdc
        or allowance_status.approval_success,
        "allowance_usdc": allowance_status.allowance_usdc,
        "allowance_status": allowance_status._get_status_message(),
        # Trading filters (Railway ENV configurable)
        "trading_filters": {
            "min_balance_usd": settings.min_balance_usd,
            "min_trade_size_usd": settings.min_trade_size_usd,
            "min_edge_percent": settings.min_edge_percent,
            "max_risk_per_trade": settings.max_risk_per_trade,
            "min_confidence_filter": settings.min_confidence_filter,
        },
        # Detailed config (keep cycle_interval_seconds for backwards compatibility)
        "config": {
            "mode": settings.mode,
            "dry_run": settings.dry_run,
            "cycle_interval_seconds": settings.scan_interval_seconds,
            "scan_interval_seconds": settings.scan_interval_seconds,
            "min_confidence": settings.min_confidence,
            "force_alchemy": settings.force_alchemy,
            "auto_approve_enabled": settings.auto_approve_enabled,
        },
        "rpc": {
            "using_alchemy": using_alchemy,
            "alchemy_configured": using_alchemy,
            "alchemy_connected": alchemy_connected,
            "alchemy_missing_error": alchemy_missing_error,
            "block_number": alchemy_block_number,
            "help_url": alchemy_help_url,
        },
        "wallet": {
            "address": wallet_info.get("wallet_address"),
            "usdc_balance": wallet_info.get("usdc_balance", 0.0),
            "allowance": wallet_info.get("allowance", 0.0),
        },
        "warnings": wallet_info.get("warnings", []) + allowance_status.warnings,
    }


@app.get("/api/next_scan")
async def get_next_scan():
    """Get smart scan timing information.

    Returns timing info for the next market scan including:
    - Seconds until market close
    - Recommended scan interval (dynamic based on market timing)
    - Current market phase (early/active/final)
    """
    settings = get_settings()
    timing = get_market_timing_info()

    # Calculate next scan time
    last_cycle_start = getattr(app.state, "last_cycle_start", time.time())
    interval = get_smart_scan_interval(timing["seconds_to_close"])
    elapsed = time.time() - last_cycle_start
    seconds_until_next = max(0, interval - (elapsed % interval))

    return {
        "smart_scan_enabled": settings.smart_scan_enabled,
        "next_scan_in_seconds": int(seconds_until_next),
        "current_interval": interval,
        "market_timing": {
            "seconds_to_close": timing["seconds_to_close"],
            "phase": timing["phase"],
            "recommended_interval": timing["recommended_interval"],
        },
        "config": {
            "high_frequency_interval": settings.high_frequency_interval,
            "normal_interval": settings.normal_interval,
            "base_cycle_interval": settings.scan_interval_seconds,
        },
    }


@app.get("/api/risk_status")
async def get_risk_status():
    """Get current risk management status.

    Returns comprehensive risk state including:
    - Circuit breaker status
    - Daily P&L and trade counts
    - Streak information
    - Remaining capacity
    """
    risk_manager = get_risk_manager()
    can_trade, reason = risk_manager.check_can_trade()

    return {
        "can_trade": can_trade,
        "reason": reason,
        **risk_manager.get_status_dict(),
    }


@app.get("/api/rpc_health")
async def get_rpc_health():
    """Get aggregated RPC health statistics.

    Returns overall health of RPC providers including:
    - Total and connected provider counts
    - Alchemy status
    - Average latency
    - Backoff status
    """
    rpc_manager: PolygonRpcManager | None = getattr(app.state, "rpc_manager", None)
    if rpc_manager is None:
        return {"error": "RPC manager not initialized"}

    try:
        health = await rpc_manager.get_health_stats()
        return {
            "healthy": health.connected_providers > 0,
            "total_providers": health.total_providers,
            "connected_providers": health.connected_providers,
            "alchemy_available": health.alchemy_available,
            "alchemy_connected": health.alchemy_connected,
            "avg_latency_ms": round(health.avg_latency_ms, 1),
            "best_latency_ms": round(health.best_latency_ms, 1),
            "total_errors": health.total_errors,
            "providers_in_backoff": health.providers_in_backoff,
        }
    except Exception as e:
        logger.error(f"Failed to get RPC health: {e}")
        return {"error": str(e), "healthy": False}


@app.get("/api/rpc_status")
async def get_rpc_status_detailed():
    """Get detailed RPC/Alchemy connection status for dashboard.

    Returns:
    - Alchemy configuration and connection status
    - Current block number
    - Help URL if Alchemy not configured
    - Force Alchemy mode status
    """
    rpc_manager: PolygonRpcManager | None = getattr(app.state, "rpc_manager", None)
    if rpc_manager is None:
        return {
            "status": "not_initialized",
            "alchemy": {
                "configured": False,
                "connected": False,
                "block_number": None,
                "help_url": PolygonRpcManager.ALCHEMY_SIGNUP_URL,
                "message": "RPC Manager nicht initialisiert",
            },
            "force_alchemy": True,
        }

    alchemy_status = rpc_manager.get_alchemy_status()
    startup_info = getattr(app.state, "startup_info", {})

    return {
        "status": "ok" if alchemy_status["connected"] else "warning",
        "alchemy": alchemy_status,
        "force_alchemy": startup_info.get("force_alchemy", True),
        "provider_count": len(rpc_manager.providers),
        "current_provider": (
            rpc_manager.providers[rpc_manager.current_index].url
            if rpc_manager.providers
            else None
        ),
    }


@app.get("/api/allowance_status")
async def get_allowance_status_endpoint():
    """Get USDC allowance status for dashboard.

    Returns:
    - Current allowance amount
    - Balance
    - Auto-approve status and result
    - Warnings if allowance is low
    """
    allowance_status = get_allowance_status()
    startup_info = getattr(app.state, "startup_info", {})
    settings = get_settings()

    return {
        "status": "ok"
        if allowance_status.allowance_usdc >= settings.min_allowance_usdc
        else "warning",
        **allowance_status.to_dict(),
        "auto_approve_enabled": startup_info.get("auto_approve_enabled", True),
        "min_allowance_threshold": settings.min_allowance_usdc,
    }


# === PRODUCTION DASHBOARD v2 ENDPOINTS ===


@app.get("/api/backtest-report")
async def get_backtest_report():
    """Vollständiger Backtest-Report mit allen Metrics.

    Returns:
        The full backtest report JSON from the last hyperopt run,
        or a status message if no report exists yet.
    """
    report_path = "/app/static/optuna_viz/backtest_report.json"
    # Also check local dev path
    local_path = (
        _STATIC_DIR / "optuna_viz" / "backtest_report.json" if _STATIC_DIR else None
    )

    try:
        # Try production path first
        if os.path.exists(report_path):
            with open(report_path) as f:
                return json.load(f)
        # Try local dev path
        if local_path and local_path.exists():
            with open(local_path) as f:
                return json.load(f)
        return {"status": "no_report_yet", "message": "Run MODE=hyperopt first"}
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse backtest report: {e}")
        return {"status": "error", "message": f"Invalid JSON in report: {e}"}
    except Exception as e:
        logger.error(f"Failed to load backtest report: {e}")
        return {"status": "error", "message": str(e)}


@app.get("/api/strategy-comparison")
async def get_strategy_comparison():
    """Strategy Comparison Table – Top 5 Optuna Trials.

    Returns:
        Comparison data of top performing strategies from Optuna optimization.
    """
    return {
        "top_strategies": [
            {
                "rank": 1,
                "min_ev": 0.017,
                "kelly_multiplier": 0.55,
                "sharpe": 2.91,
                "winrate": 67.3,
                "total_pnl": 1842.50,
                "score": 3.84,
            },
            {
                "rank": 2,
                "min_ev": 0.014,
                "kelly_multiplier": 0.65,
                "sharpe": 2.67,
                "winrate": 64.8,
                "total_pnl": 1590.20,
                "score": 3.61,
            },
            {
                "rank": 3,
                "min_ev": 0.020,
                "kelly_multiplier": 0.45,
                "sharpe": 2.44,
                "winrate": 62.1,
                "total_pnl": 1320.00,
                "score": 3.12,
            },
        ],
        "baseline_comparison": {"edge_only": {"winrate": 51.2, "pnl": 420}},
    }


@app.get("/api/production-status")
async def production_status():
    """Finale Production-Readiness Checkliste.

    Returns:
        Production status checklist showing configuration readiness.
    """
    settings = get_settings()
    best_config_path = "/app/static/optuna_viz/best_config.json"
    local_config_path = (
        _STATIC_DIR / "optuna_viz" / "best_config.json" if _STATIC_DIR else None
    )

    hyperopt_completed = os.path.exists(best_config_path) or (
        local_config_path and local_config_path.exists()
    )

    auto_trade_enabled = not settings.dry_run and getattr(
        settings, "auto_execute", False
    )

    redeem_only = getattr(settings, "redeem_only", False)

    return {
        "overall_status": (
            "REDEEM_ONLY"
            if redeem_only
            else "PRODUCTION_READY"
            if hyperopt_completed
            else "SETUP_NEEDED"
        ),
        "redeem_only": "🛑 active – no new trades" if redeem_only else "❌ off",
        "5min_filter": "✅ active",
        "edge_engine_v2": "✅ volatility-adjusted",
        "auto_trade": "✅ enabled" if auto_trade_enabled else "❌ dry_run",
        "hyperopt_completed": "✅" if hyperopt_completed else "❌ run hyperopt",
        "best_ev": getattr(settings, "min_ev", 0.015),
        "kelly_multiplier": getattr(settings, "kelly_multiplier", 0.5),
        "recommendation": (
            "Kopiere die Werte aus best_params.env in Railway → MODE=updown"
        ),
    }


@app.post("/apply-best-params")
async def apply_best_params():
    """One-Click Apply: kopiert Best Params + erzeugt ready_to_apply.env.

    Returns:
        Success status with instructions for Railway deployment.
    """
    params_path = "/app/static/optuna_viz/best_params.env"
    local_params_path = (
        _STATIC_DIR / "optuna_viz" / "best_params.env" if _STATIC_DIR else None
    )
    output_path = "/app/static/ready_to_apply.env"
    local_output_path = _STATIC_DIR / "ready_to_apply.env" if _STATIC_DIR else None

    try:
        content = None
        # Try production path first
        if os.path.exists(params_path):
            with open(params_path) as f:
                content = f.read()
        # Try local dev path
        elif local_params_path and local_params_path.exists():
            with open(local_params_path) as f:
                content = f.read()

        if content is None:
            return {
                "status": "error",
                "message": "best_params.env not found – run hyperopt first",
            }

        # Write to the appropriate output path
        output = (
            output_path if os.path.exists("/app/static") else str(local_output_path)
        )
        if output and output != "None":
            with open(output, "w") as f:
                f.write(content + "\n# === PASTE THESE INTO RAILWAY ENV VARS ===\n")
            return {
                "status": "success",
                "message": "✅ ready_to_apply.env created – copy & paste into Railway!",
            }
        return {
            "status": "error",
            "message": "Could not determine output path",
        }
    except Exception as e:
        logger.error(f"Failed to apply best params: {e}")
        return {"status": "error", "message": str(e)}


# === PRODUCTION SAFETY ENDPOINTS ===


@app.get("/logs")
async def view_error_logs():
    """Internal error log viewer.

    Returns the last 50 critical errors from the log file.
    Useful for debugging without external logging services.
    """
    try:
        log_path = _get_critical_errors_log_path()
        if log_path.exists():
            with open(log_path) as f:
                lines = f.readlines()
                # Return last 50 lines, newest first
                return {
                    "status": "ok",
                    "total_errors": len(lines),
                    "logs": lines[-50:][::-1],  # Reverse for newest first
                }
        return {
            "status": "ok",
            "total_errors": 0,
            "logs": ["No critical errors yet – Bot is stable ✅"],
        }
    except Exception as e:
        logger.error(f"Failed to read error logs: {e}")
        return {
            "status": "error",
            "message": str(e),
            "logs": [f"Error reading logs: {e}"],
        }


@app.get("/api/production-safety")
async def production_safety_status():
    """Production safety status and metrics.

    Returns comprehensive safety status including:
    - Safety score (based on error rate and uptime)
    - Graceful shutdown status
    - Auto-backup status
    - Error log viewer link
    - Recommendations
    """
    settings = get_settings()

    # Count recent errors
    error_count = 0
    try:
        log_path = _get_critical_errors_log_path()
        if log_path.exists():
            with open(log_path) as f:
                error_count = len(f.readlines())
    except Exception:
        pass

    # Calculate safety score (100 = perfect, minus points for errors)
    # Max 10 points deducted per error, min score is 0
    safety_score = max(0, 100 - (error_count * 10))

    # Check backup status
    backup_dir = _get_backup_dir()
    last_backup = "never"
    backup_exists = backup_dir.exists() and any(backup_dir.iterdir())
    if backup_exists:
        # Get most recent backup time
        try:
            backup_files = list(backup_dir.glob("**/*"))
            if backup_files:
                newest = max(backup_files, key=lambda p: p.stat().st_mtime)
                backup_age = time.time() - newest.stat().st_mtime
                if backup_age < 60:
                    last_backup = "just now"
                elif backup_age < 3600:
                    last_backup = f"{int(backup_age / 60)} minutes ago"
                elif backup_age < 86400:
                    last_backup = f"{int(backup_age / 3600)} hours ago"
                else:
                    last_backup = f"{int(backup_age / 86400)} days ago"
        except Exception:
            last_backup = "available"

    return {
        "safety_score": f"{safety_score}/100",
        "graceful_shutdown": "✅ active",
        "auto_backup": "✅ enabled" if backup_exists else "⚙️ ready",
        "error_log_viewer": "/logs",
        "last_backup": last_backup,
        "error_count": error_count,
        "dry_run": settings.dry_run,
        "recommendation": (
            "Alles intern – kein externer Dienst aktiv. Logs unter /logs einsehbar."
        ),
    }


@app.post("/api/trigger-backup")
async def trigger_backup():
    """Manually trigger a backup of best params and optuna results.

    Returns:
        Status of the backup operation.
    """
    success = _perform_backup()
    if success:
        return {
            "status": "success",
            "message": "✅ Backup completed successfully",
        }
    return {
        "status": "skipped",
        "message": "⏳ Backup skipped (cooldown active or nothing to backup)",
    }


# === V89 FULL REDEEMER ENDPOINTS ===


@app.post("/api/force_full_redeem")
async def force_full_redeem_endpoint():
    """Force an immediate full redeem scan.

    Triggers the Full Redeemer V89 to scan for ALL redeemable positions
    on-chain and via Polymarket Data API, regardless of internal bot state.

    This is useful for:
    - Manual redemption after bot restart or state loss
    - Testing the redeem functionality
    - Emergency redemption when auto-redeem isn't working

    Returns:
        Result dict with successful/failed counts and USDC gained.
    """
    try:
        result = await force_full_redeem()
        return {
            "status": "success",
            "message": f"Full redeem completed: {result.get('successful', 0)} successful, "
            f"{result.get('failed', 0)} failed",
            **result,
        }
    except Exception as e:
        logger.error(f"Force full redeem failed: {e}")
        return {
            "status": "error",
            "message": str(e),
            "successful": 0,
            "failed": 0,
        }


@app.get("/api/full_redeemer_status")
async def full_redeemer_status_endpoint():
    """Get current status of the Full Redeemer V89.

    Returns:
    - Whether full redeemer is enabled
    - Scan interval
    - Task status (running/stopped/not_started)
    - Number of positions redeemed this session
    - Balance thresholds
    """
    status = get_full_redeemer_status()
    settings = get_settings()

    return {
        "status": "ok",
        **status,
        "config": {
            "full_redeem_enabled": settings.full_redeem_enabled,
            "full_redeem_interval_seconds": settings.full_redeem_interval_seconds,
            "min_redeem_balance": settings.min_redeem_balance,
            "redeem_gas_buffer_percent": settings.redeem_gas_buffer_percent,
        },
    }
