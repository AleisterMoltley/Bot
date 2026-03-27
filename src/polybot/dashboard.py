"""FastAPI web dashboard for monitoring PolyBot.

Provides:
- HTML pages with Jinja2 templates (Overview, Trades, Positions, P&L)
- JSON API endpoints for programmatic access
- Real-time trade and P&L monitoring
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from polybot.config import get_settings
from polybot.database import init_db, get_db


def _get_risk_state() -> dict[str, Any]:
    """Get current risk management state."""
    with get_db() as conn:
        row = conn.execute("SELECT * FROM risk_state WHERE id = 1").fetchone()
        if row:
            return dict(row)
    return {"daily_loss": 0, "consecutive_losses": 0, "is_paused": False}


def _get_today_pnl() -> dict[str, Any]:
    """Get today's P&L summary."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    with get_db() as conn:
        row = conn.execute(
            "SELECT * FROM daily_pnl WHERE date = ?", (today,)
        ).fetchone()
        if row:
            data = dict(row)
            total = data.get("total_trades", 0)
            winning = data.get("winning_trades", 0)
            data["winrate"] = (winning / total * 100) if total > 0 else 0
            return data
    return {
        "realized": 0,
        "unrealized": 0,
        "total_trades": 0,
        "winning_trades": 0,
        "losing_trades": 0,
        "winrate": 0,
    }


def _get_total_pnl() -> dict[str, Any]:
    """Get total P&L across all time."""
    with get_db() as conn:
        # Sum all daily P&L
        row = conn.execute(
            """SELECT
                COALESCE(SUM(realized), 0) as total_realized,
                COALESCE(SUM(unrealized), 0) as total_unrealized,
                COALESCE(SUM(total_trades), 0) as total_trades,
                COALESCE(SUM(winning_trades), 0) as winning_trades,
                COALESCE(SUM(losing_trades), 0) as losing_trades
            FROM daily_pnl"""
        ).fetchone()
        if row:
            data = dict(row)
            total = data.get("total_trades", 0)
            winning = data.get("winning_trades", 0)
            data["overall_winrate"] = (winning / total * 100) if total > 0 else 0
            return data
    return {
        "total_realized": 0,
        "total_unrealized": 0,
        "total_trades": 0,
        "winning_trades": 0,
        "losing_trades": 0,
        "overall_winrate": 0,
    }


def run_dashboard():
    """Start the FastAPI dashboard with Jinja2 templates."""
    import uvicorn
    from fastapi import FastAPI, Request
    from fastapi.responses import HTMLResponse
    from fastapi.templating import Jinja2Templates
    from starlette.middleware.cors import CORSMiddleware

    settings = get_settings()
    init_db()

    app = FastAPI(
        title="PolyBot Dashboard",
        description="Web dashboard for monitoring PolyBot trading activity",
        version="1.0.0",
    )

    # Enable CORS for API access
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Setup Jinja2 templates
    templates_dir = Path(__file__).parent / "templates"
    templates = Jinja2Templates(directory=str(templates_dir))

    # =========================================================================
    # HTML Pages
    # =========================================================================

    @app.get("/", response_class=HTMLResponse)
    async def index(request: Request):
        """Dashboard overview page."""
        with get_db() as conn:
            trades = conn.execute(
                "SELECT * FROM trades ORDER BY id DESC LIMIT 20"
            ).fetchall()
            positions = conn.execute(
                "SELECT * FROM positions WHERE status = 'OPEN' ORDER BY id DESC LIMIT 10"
            ).fetchall()
            arb_executions = conn.execute(
                "SELECT * FROM arb_executions ORDER BY id DESC LIMIT 10"
            ).fetchall()

        return templates.TemplateResponse(
            "index.html",
            {
                "request": request,
                "active_page": "overview",
                "risk": _get_risk_state(),
                "pnl": _get_today_pnl(),
                "trades": [dict(t) for t in trades],
                "positions": [dict(p) for p in positions],
                "arb_executions": [dict(a) for a in arb_executions],
            },
        )

    @app.get("/trades", response_class=HTMLResponse)
    async def trades_page(request: Request):
        """Trades history page."""
        with get_db() as conn:
            trades = conn.execute(
                "SELECT * FROM trades ORDER BY id DESC LIMIT 100"
            ).fetchall()

        return templates.TemplateResponse(
            "trades.html",
            {
                "request": request,
                "active_page": "trades",
                "trades": [dict(t) for t in trades],
            },
        )

    @app.get("/positions", response_class=HTMLResponse)
    async def positions_page(request: Request):
        """Positions page."""
        with get_db() as conn:
            open_positions = conn.execute(
                "SELECT * FROM positions WHERE status = 'OPEN' ORDER BY id DESC"
            ).fetchall()
            closed_positions = conn.execute(
                "SELECT * FROM positions WHERE status != 'OPEN' ORDER BY id DESC LIMIT 50"
            ).fetchall()

        return templates.TemplateResponse(
            "positions.html",
            {
                "request": request,
                "active_page": "positions",
                "open_positions": [dict(p) for p in open_positions],
                "closed_positions": [dict(p) for p in closed_positions],
            },
        )

    @app.get("/pnl", response_class=HTMLResponse)
    async def pnl_page(request: Request):
        """P&L history page."""
        with get_db() as conn:
            daily_pnl = conn.execute(
                "SELECT * FROM daily_pnl ORDER BY date DESC LIMIT 30"
            ).fetchall()

        # Add winrate to each day
        daily_data = []
        for day in daily_pnl:
            d = dict(day)
            total = d.get("total_trades", 0)
            winning = d.get("winning_trades", 0)
            d["winrate"] = (winning / total * 100) if total > 0 else 0
            daily_data.append(d)

        return templates.TemplateResponse(
            "pnl.html",
            {
                "request": request,
                "active_page": "pnl",
                "total_pnl": _get_total_pnl(),
                "today_pnl": _get_today_pnl(),
                "daily_pnl": daily_data,
            },
        )

    # =========================================================================
    # JSON API Endpoints
    # =========================================================================

    @app.get("/api/trades")
    async def api_trades(limit: int = 100):
        """Get recent trades as JSON."""
        with get_db() as conn:
            trades = conn.execute(
                "SELECT * FROM trades ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
        return [dict(t) for t in trades]

    @app.get("/api/positions")
    async def api_positions(status: str = "OPEN"):
        """Get positions as JSON."""
        with get_db() as conn:
            if status.upper() == "ALL":
                positions = conn.execute(
                    "SELECT * FROM positions ORDER BY id DESC"
                ).fetchall()
            else:
                positions = conn.execute(
                    "SELECT * FROM positions WHERE status = ? ORDER BY id DESC",
                    (status.upper(),),
                ).fetchall()
        return [dict(p) for p in positions]

    @app.get("/api/risk")
    async def api_risk():
        """Get current risk state as JSON."""
        return _get_risk_state()

    @app.get("/api/pnl")
    async def api_pnl():
        """Get P&L summary as JSON."""
        return {
            "today": _get_today_pnl(),
            "total": _get_total_pnl(),
        }

    @app.get("/api/pnl/daily")
    async def api_daily_pnl(days: int = 30):
        """Get daily P&L history as JSON."""
        with get_db() as conn:
            daily_pnl = conn.execute(
                "SELECT * FROM daily_pnl ORDER BY date DESC LIMIT ?", (days,)
            ).fetchall()

        result = []
        for day in daily_pnl:
            d = dict(day)
            total = d.get("total_trades", 0)
            winning = d.get("winning_trades", 0)
            d["winrate"] = (winning / total * 100) if total > 0 else 0
            result.append(d)
        return result

    @app.get("/api/arb")
    async def api_arb(limit: int = 50):
        """Get arbitrage executions as JSON."""
        with get_db() as conn:
            arbs = conn.execute(
                "SELECT * FROM arb_executions ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
        return [dict(a) for a in arbs]

    @app.get("/api/scanner")
    async def api_scanner(limit: int = 5):
        """Run MaxProfit scanner and return high-EV opportunities.

        This endpoint triggers a live scan of Polymarket markets and returns
        the top high-EV opportunities prioritized by:
        1. Arbitrage opportunities (risk-free)
        2. CEX-Edge opportunities (Binance price vs market implied)
        3. High liquidity + volume markets
        """
        from polybot.scanner import MaxProfitScanner

        scanner = MaxProfitScanner()
        results = await scanner.scan_async(limit=limit)

        return {
            "status": f"Scanning {scanner.markets_scanned} markets... {len(results)} high-EV found!",
            "markets_scanned": scanner.markets_scanned,
            "high_ev_count": len(results),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "high_ev": results,
        }

    @app.get("/api/scanner/5min")
    async def api_scanner_5min():
        """Get 5-minute EXCLUSIVE scanner status and active markets.

        Returns the current scanner configuration and any active
        5-minute crypto markets (BTC, ETH, SOL, XRP only).
        """
        try:
            from polybot.scanner_updown import get_updown_scanner

            scanner = get_updown_scanner()
            markets = scanner.scan()
            status = scanner.get_status()

            return {
                "mode": "5-MINUTE EXCLUSIVE",
                "mode_active": True,
                "description": "Only 5-minute crypto events (max 300 seconds)",
                "scanner_status": status,
                "active_5min_markets": len(markets),
                "markets": markets,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
        except ImportError:
            return {"error": "5-minute scanner not available"}

    @app.get("/api/backtest")
    async def api_backtest(days: int = 180):
        """Run backtest and return results.

        Args:
            days: Number of days to backtest (default: 180)

        Returns:
            Backtest results with equity curve and metrics
        """
        from polybot.backtester import Backtester

        bt = Backtester()
        result = bt.run_backtest(days=days)
        return result.to_dict()

    @app.get("/api/portfolio")
    async def api_portfolio():
        """Get current portfolio state and exposure metrics."""
        try:
            from polybot.portfolio_manager import get_portfolio_manager

            pm = get_portfolio_manager()
            state = pm.get_state()
            return {
                **state.to_dict(),
                "warnings": pm.get_correlation_warnings(),
                "heatmap_data": pm.get_heatmap_data(),
            }
        except ImportError:
            return {"error": "Portfolio manager not available"}

    @app.get("/api/volatility")
    async def api_volatility():
        """Get current volatility regime and Kelly multiplier."""
        try:
            from polybot.volatility_regime import get_volatility_detector

            detector = get_volatility_detector()
            state = detector.get_state()
            return state.to_dict()
        except ImportError:
            return {"error": "Volatility detector not available"}

    @app.get("/api/hourly_risk")
    async def api_hourly_risk():
        """Get hourly risk regime state and 24-hour heatmap data.

        Returns the current Berlin hour, risk multiplier, and a
        complete 24-hour heatmap for dashboard visualization.

        Colors:
        - Grey (#6B7280): inactive (multiplier = 0.0)
        - Red (#EF4444): conservative (multiplier = 0.3)
        - Green (#22C55E): aggressive (multiplier = 1.6-1.8)
        """
        try:
            from polybot.hourly_risk_regime import get_hourly_risk_regime

            regime = get_hourly_risk_regime()
            state = regime.get_state()
            desc = (
                "Hourly Risk Regime (Berlin-Zeit): "
                "Grau = inaktiv | Rot = konservativ | Grün = aggressiv"
            )
            return {
                "current": state.to_dict(),
                "heatmap": regime.get_heatmap_data(),
                "description": desc,
            }
        except ImportError:
            return {"error": "Hourly risk regime not available"}

    @app.get("/api/executions")
    async def api_executions(limit: int = 50):
        """Get recent trade executions with slippage data."""
        try:
            from polybot.execution_logger import get_execution_logger

            logger = get_execution_logger()
            return {
                "executions": logger.get_recent_executions(limit=limit),
                "stats": logger.get_stats().to_dict(),
                "slippage_analysis": logger.get_slippage_analysis(),
            }
        except ImportError:
            return {"error": "Execution logger not available"}

    @app.get("/api/analytics")
    async def api_analytics():
        """Get combined analytics data for dashboard."""
        analytics = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        # Get backtest data
        try:
            from polybot.backtester import Backtester

            bt = Backtester()
            result = bt.run_backtest(days=30)
            analytics["backtest"] = result.to_dict()
        except Exception as e:
            analytics["backtest"] = {"error": str(e)}

        # Get portfolio data
        try:
            from polybot.portfolio_manager import get_portfolio_manager

            pm = get_portfolio_manager()
            state = pm.get_state()
            analytics["portfolio"] = {
                **state.to_dict(),
                "heatmap_data": pm.get_heatmap_data(),
            }
        except Exception as e:
            analytics["portfolio"] = {"error": str(e)}

        # Get volatility data
        try:
            from polybot.volatility_regime import get_volatility_detector

            detector = get_volatility_detector()
            analytics["volatility"] = detector.get_state().to_dict()
        except Exception as e:
            analytics["volatility"] = {"error": str(e)}

        # Get hourly risk regime data
        try:
            from polybot.hourly_risk_regime import get_hourly_risk_regime

            regime = get_hourly_risk_regime()
            analytics["hourly_risk"] = {
                "current": regime.get_state().to_dict(),
                "heatmap": regime.get_heatmap_data(),
            }
        except Exception as e:
            analytics["hourly_risk"] = {"error": str(e)}

        # Get execution stats
        try:
            from polybot.execution_logger import get_execution_logger

            logger = get_execution_logger()
            analytics["executions"] = {
                "stats": logger.get_stats().to_dict(),
                "recent": logger.get_recent_executions(limit=10),
            }
        except Exception as e:
            analytics["executions"] = {"error": str(e)}

        return analytics

    @app.get("/api/health")
    async def health():
        """Health check endpoint."""
        return {"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat()}

    print(f"🌐 Dashboard starting on http://0.0.0.0:{settings.dashboard_port}")
    uvicorn.run(app, host="0.0.0.0", port=settings.dashboard_port)
