"""
Dashboard API — REST endpoints consumed by the Trident dashboard
(Next.js at trident-dashboard-phi.vercel.app).

All endpoints:
  - Return JSON with CORS headers (Access-Control-Allow-Origin: *)
  - Accept query parameters where documented
  - Degrade gracefully when dependent services are not running

Register with: register_dashboard_routes(app)
"""

import logging
import time
from datetime import datetime, timezone

from aiohttp import web

from bot.config import PACIFICA_REST_URL

logger = logging.getLogger(__name__)

ADMIN_USER_ID = 6994676998

CORS_HEADERS = {"Access-Control-Allow-Origin": "*"}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _json(data: dict | list, status: int = 200) -> web.Response:
    """Return a JSON response with CORS headers."""
    return web.json_response(data, status=status, headers=CORS_HEADERS)


def _user_id(request: web.Request) -> int:
    """Extract user_id from query params, defaulting to admin."""
    try:
        return int(request.rel_url.query.get("user_id", ADMIN_USER_ID))
    except (ValueError, TypeError):
        return ADMIN_USER_ID


async def _make_client_for_user(user: dict):
    """Create a PacificaClient from a user DB row. Returns None if missing keys."""
    if not user or not user.get("pacifica_account") or not user.get("agent_wallet_encrypted"):
        return None
    try:
        from bot.services.wallet_manager import decrypt_private_key
        from bot.services.pacifica_client import PacificaClient
        kp = decrypt_private_key(user["agent_wallet_encrypted"])
        return PacificaClient(account=user["pacifica_account"], keypair=kp)
    except Exception as exc:
        logger.debug("Failed to create PacificaClient for user %s: %s", user.get("telegram_id"), exc)
        return None


# ---------------------------------------------------------------------------
# 1. GET /api/overview
# ---------------------------------------------------------------------------


async def api_overview(request: web.Request) -> web.Response:
    """Complete bot overview for the dashboard home page."""
    try:
        # Regime
        regime_data = {"state": "CALM", "multiplier": 1.0, "sigma": 0.0}
        try:
            from bot.services.regime_classifier import get_regime
            r = get_regime()
            regime_data = {
                "state": r.get("regime", "CALM"),
                "multiplier": r.get("multiplier", 1.0),
                "sigma": r.get("sigma", 0.0),
            }
        except Exception:
            pass

        # Risk gate
        risk_data = {"state": "OPEN", "reason": "N/A", "daily_pnl": 0.0, "daily_limit": 500.0}
        try:
            from bot.services.risk_guardian import get_gate_state
            g = get_gate_state()
            risk_data = {
                "state": g.get("state", "OPEN"),
                "reason": g.get("reason", "N/A"),
                "daily_pnl": g.get("daily_pnl", 0.0),
                "daily_limit": g.get("daily_limit", 500.0),
            }
        except Exception:
            pass

        # Active strategy counts from DB
        active_strategies = {
            "grid": 0, "dca": 0, "twap": 0, "trail": 0,
            "arb": 0, "copy": 0, "mean_reversion": 0, "brackets": 0,
        }
        positions_count = 0
        orders_count = 0
        balance = 0.0
        equity = 0.0

        try:
            from database.db import get_db
            db = await get_db()

            async with db.execute("SELECT COUNT(*) FROM grid_configs WHERE active = 1") as cur:
                row = await cur.fetchone()
                active_strategies["grid"] = row[0] if row else 0

            async with db.execute("SELECT COUNT(*) FROM dca_configs WHERE active = 1") as cur:
                row = await cur.fetchone()
                active_strategies["dca"] = row[0] if row else 0

            async with db.execute("SELECT COUNT(*) FROM twap_orders WHERE active = 1") as cur:
                row = await cur.fetchone()
                active_strategies["twap"] = row[0] if row else 0

            async with db.execute("SELECT COUNT(*) FROM trailing_stops WHERE active = 1") as cur:
                row = await cur.fetchone()
                active_strategies["trail"] = row[0] if row else 0

            async with db.execute("SELECT COUNT(*) FROM copy_configs WHERE active = 1") as cur:
                row = await cur.fetchone()
                active_strategies["copy"] = row[0] if row else 0

            # arb_positions table may not exist
            try:
                async with db.execute("SELECT COUNT(*) FROM arb_positions WHERE active = 1") as cur:
                    row = await cur.fetchone()
                    active_strategies["arb"] = row[0] if row else 0
            except Exception:
                pass
        except Exception as exc:
            logger.debug("DB query failed in overview: %s", exc)

        # Account balance/equity from admin user
        try:
            from database.db import get_user
            user = await get_user(ADMIN_USER_ID)
            client = await _make_client_for_user(user) if user else None
            if client:
                try:
                    acct = await client.get_account_info()
                    balance = float(acct.get("balance", 0) or 0)
                    equity = float(acct.get("equity", 0) or 0)
                    positions = await client.get_positions() or []
                    positions_count = len(positions)
                    orders = await client.get_open_orders() or []
                    orders_count = len(orders)
                finally:
                    await client.close()
        except Exception as exc:
            logger.debug("Account fetch failed in overview: %s", exc)

        return _json({
            "balance": balance,
            "equity": equity,
            "positions_count": positions_count,
            "orders_count": orders_count,
            "active_strategies": active_strategies,
            "regime": regime_data,
            "risk_gate": risk_data,
            "uptime": time.time(),
            "last_updated": datetime.now(timezone.utc).isoformat(),
        })

    except Exception as exc:
        logger.error("api_overview error: %s", exc)
        return _json({"error": str(exc)}, status=500)


# ---------------------------------------------------------------------------
# 2. GET /api/positions
# ---------------------------------------------------------------------------


async def api_positions(request: web.Request) -> web.Response:
    """All open positions across all users."""
    try:
        from database.db import get_db, get_user
        db = await get_db()

        async with db.execute(
            "SELECT telegram_id, pacifica_account, agent_wallet_encrypted "
            "FROM users WHERE pacifica_account IS NOT NULL AND agent_wallet_encrypted IS NOT NULL"
        ) as cursor:
            users = [dict(r) for r in await cursor.fetchall()]

        all_positions = []
        for u in users:
            client = await _make_client_for_user(u)
            if not client:
                continue
            try:
                positions = await client.get_positions() or []
                for p in positions:
                    mark = float(p.get("mark_price", p.get("mark", 0)) or 0)
                    entry = float(p.get("entry_price", 0) or 0)
                    amount = float(p.get("amount", 0) or 0)
                    side = p.get("side", "")
                    pnl = float(p.get("unrealized_pnl", p.get("pnl", 0)) or 0)
                    pnl_pct = 0.0
                    if entry > 0 and amount != 0:
                        notional = abs(amount) * entry
                        pnl_pct = (pnl / notional * 100) if notional else 0.0
                    all_positions.append({
                        "symbol": p.get("symbol", ""),
                        "side": side,
                        "amount": amount,
                        "entry_price": entry,
                        "mark_price": mark,
                        "pnl": round(pnl, 4),
                        "pnl_pct": round(pnl_pct, 4),
                        "user_id": u["telegram_id"],
                    })
            except Exception as exc:
                logger.debug("Positions fetch failed for user %s: %s", u["telegram_id"], exc)
            finally:
                await client.close()

        return _json(all_positions)

    except Exception as exc:
        logger.error("api_positions error: %s", exc)
        return _json({"error": str(exc)}, status=500)


# ---------------------------------------------------------------------------
# 3. GET /api/regime
# ---------------------------------------------------------------------------


async def api_regime(request: web.Request) -> web.Response:
    """Regime classifier current state + 60-point history."""
    try:
        current = {
            "regime": "CALM", "multiplier": 1.0, "sigma": 0.0,
            "drawdown_amp": 1.0, "timestamp": time.time(),
        }
        history = []
        try:
            from bot.services.regime_classifier import get_regime, get_regime_history
            current = get_regime()
            history = get_regime_history()
        except Exception:
            pass

        return _json({"current": current, "history": history})

    except Exception as exc:
        logger.error("api_regime error: %s", exc)
        return _json({"error": str(exc)}, status=500)


# ---------------------------------------------------------------------------
# 4. GET /api/risk
# ---------------------------------------------------------------------------


async def api_risk(request: web.Request) -> web.Response:
    """Risk guardian state + transition history."""
    try:
        state_data = {
            "state": "OPEN",
            "reason": "N/A",
            "consecutive_losses": 0,
            "daily_pnl": 0.0,
            "daily_limit": 500.0,
            "cooldown_expires": None,
            "history": [],
        }
        try:
            from bot.services.risk_guardian import get_gate_state, get_guardian_history
            g = get_gate_state()
            h = get_guardian_history()
            state_data = {
                "state": g.get("state", "OPEN"),
                "reason": g.get("reason", "N/A"),
                "consecutive_losses": g.get("consecutive_losses", 0),
                "daily_pnl": g.get("daily_pnl", 0.0),
                "daily_limit": g.get("daily_limit", 500.0),
                "cooldown_expires": g.get("cooldown_expires"),
                "history": h,
            }
        except Exception:
            pass

        return _json(state_data)

    except Exception as exc:
        logger.error("api_risk error: %s", exc)
        return _json({"error": str(exc)}, status=500)


# ---------------------------------------------------------------------------
# 5. GET /api/portfolio
# ---------------------------------------------------------------------------


async def api_portfolio(request: web.Request) -> web.Response:
    """Portfolio exposure breakdown by group."""
    LARGE_CAP = {"BTC", "ETH", "SOL", "BNB", "AVAX"}
    MID_CAP = {"LINK", "UNI", "AAVE", "SNX", "CRV", "APT", "SUI"}
    MEME = {"DOGE", "SHIB", "PEPE", "WIF", "BONK", "FLOKI"}

    try:
        from database.db import get_db

        db = await get_db()
        async with db.execute(
            "SELECT telegram_id, pacifica_account, agent_wallet_encrypted "
            "FROM users WHERE pacifica_account IS NOT NULL AND agent_wallet_encrypted IS NOT NULL"
        ) as cursor:
            users = [dict(r) for r in await cursor.fetchall()]

        groups: dict[str, dict] = {
            "large_cap": {"positions": [], "count": 0},
            "mid_cap": {"positions": [], "count": 0},
            "meme": {"positions": [], "count": 0},
            "other": {"positions": [], "count": 0},
        }
        long_count = 0
        short_count = 0
        total_margin = 0.0
        total_equity = 0.0

        for u in users:
            client = await _make_client_for_user(u)
            if not client:
                continue
            try:
                positions = await client.get_positions() or []
                acct = await client.get_account_info() or {}
                eq = float(acct.get("equity", 0) or 0)
                total_equity += eq

                for p in positions:
                    symbol_raw = p.get("symbol", "")
                    base = symbol_raw.replace("-PERP", "").replace("-perp", "").upper()
                    side = (p.get("side") or "").lower()
                    margin = float(p.get("margin", p.get("initial_margin", 0)) or 0)
                    total_margin += margin

                    entry = {
                        "symbol": symbol_raw,
                        "side": side,
                        "amount": float(p.get("amount", 0) or 0),
                        "entry_price": float(p.get("entry_price", 0) or 0),
                        "pnl": float(p.get("unrealized_pnl", p.get("pnl", 0)) or 0),
                        "user_id": u["telegram_id"],
                    }

                    if base in LARGE_CAP:
                        groups["large_cap"]["positions"].append(entry)
                    elif base in MID_CAP:
                        groups["mid_cap"]["positions"].append(entry)
                    elif base in MEME:
                        groups["meme"]["positions"].append(entry)
                    else:
                        groups["other"]["positions"].append(entry)

                    if side in ("bid", "long", "buy"):
                        long_count += 1
                    elif side in ("ask", "short", "sell"):
                        short_count += 1
            except Exception as exc:
                logger.debug("Portfolio fetch failed for user %s: %s", u["telegram_id"], exc)
            finally:
                await client.close()

        for g in groups.values():
            g["count"] = len(g["positions"])

        margin_util = (total_margin / total_equity * 100) if total_equity > 0 else 0.0

        return _json({
            "groups": groups,
            "direction": {"long": long_count, "short": short_count},
            "margin_util": round(margin_util, 2),
        })

    except Exception as exc:
        logger.error("api_portfolio error: %s", exc)
        return _json({"error": str(exc)}, status=500)


# ---------------------------------------------------------------------------
# 6. GET /api/signals
# ---------------------------------------------------------------------------


async def api_signals(request: web.Request) -> web.Response:
    """Active pulse + radar signals."""
    try:
        pulse = []
        radar = []

        try:
            from bot.services.pulse_detector import get_active_signals
            raw = get_active_signals()
            pulse = [
                {
                    "tier": s.get("tier"),
                    "symbol": s.get("asset"),
                    "sector": s.get("sector"),
                    "confidence": s.get("confidence"),
                    "direction": s.get("direction"),
                    "timestamp": s.get("timestamp"),
                }
                for s in raw
            ]
        except Exception:
            pass

        try:
            from bot.services.radar_scanner import get_latest_scan
            raw = get_latest_scan()
            radar = [
                {
                    "symbol": s.get("symbol"),
                    "score": s.get("score"),
                    "direction": s.get("direction"),
                    "breakdown": s.get("breakdown", {}),
                    "rsi": s.get("rsi"),
                    "funding_rate": s.get("funding_rate"),
                    "change_1h_pct": s.get("change_1h_pct"),
                }
                for s in raw
            ]
        except Exception:
            pass

        return _json({"pulse": pulse, "radar": radar})

    except Exception as exc:
        logger.error("api_signals error: %s", exc)
        return _json({"error": str(exc)}, status=500)


# ---------------------------------------------------------------------------
# 7. GET /api/funding
# ---------------------------------------------------------------------------


async def api_funding(request: web.Request) -> web.Response:
    """Funding rates + arb opportunities."""
    try:
        rates = []
        arb_positions = []

        try:
            from bot.services.funding_monitor import get_all_funding_rates
            from bot.services.funding_arb import scan_funding_spreads

            pac_rates, spreads = await _gather_safely(
                get_all_funding_rates(), scan_funding_spreads()
            )

            # Build rates list from spreads (cross-exchange)
            for s in (spreads or []):
                rates.append({
                    "symbol": s.get("symbol"),
                    "pac_rate": s.get("pacifica_rate"),
                    "hl_rate": s.get("hl_rate"),
                    "spread": s.get("spread"),
                    "annualized": s.get("annualized_pct"),
                })

            # If no spreads, fall back to Pacifica-only rates
            if not rates and pac_rates:
                for r in pac_rates:
                    rates.append({
                        "symbol": r.get("symbol"),
                        "pac_rate": r.get("funding_rate"),
                        "hl_rate": None,
                        "spread": None,
                        "annualized": None,
                    })
        except Exception as exc:
            logger.debug("Funding rates fetch failed: %s", exc)

        try:
            from database.db import get_db
            db = await get_db()
            async with db.execute(
                "SELECT * FROM arb_positions WHERE active = 1"
            ) as cursor:
                rows = await cursor.fetchall()
                arb_positions = [dict(r) for r in rows]
        except Exception:
            pass

        return _json({"rates": rates, "arb_positions": arb_positions})

    except Exception as exc:
        logger.error("api_funding error: %s", exc)
        return _json({"error": str(exc)}, status=500)


# ---------------------------------------------------------------------------
# 8. GET /api/gaps
# ---------------------------------------------------------------------------


async def api_gaps(request: web.Request) -> web.Response:
    """Cross-exchange price gaps."""
    try:
        gaps = []
        stats = {}

        try:
            from bot.services.gap_monitor import get_gap_stats
            raw_stats = get_gap_stats()
            stats = {
                "total_symbols": len(raw_stats),
                "top_avg_gap": raw_stats[0]["avg_gap"] if raw_stats else 0,
            }
            gaps = [
                {
                    "symbol": s["symbol"],
                    "gap_pct": s["current_gap"],
                    "avg_gap": s["avg_gap"],
                    "max_gap": s["max_gap"],
                    "direction": s["direction"],
                    "samples": s["samples"],
                }
                for s in raw_stats
            ]
        except Exception:
            pass

        return _json({"gaps": gaps, "stats": stats})

    except Exception as exc:
        logger.error("api_gaps error: %s", exc)
        return _json({"error": str(exc)}, status=500)


# ---------------------------------------------------------------------------
# 9. GET /api/reflect?period=24h&user_id=...
# ---------------------------------------------------------------------------


async def api_reflect(request: web.Request) -> web.Response:
    """REFLECT-style report built from trade_log."""
    user_id = _user_id(request)
    period_str = request.rel_url.query.get("period", "24h")

    # Parse period into hours
    period_hours = 24
    try:
        if period_str.endswith("h"):
            period_hours = int(period_str[:-1])
        elif period_str.endswith("d"):
            period_hours = int(period_str[:-1]) * 24
        elif period_str.endswith("w"):
            period_hours = int(period_str[:-1]) * 168
    except ValueError:
        pass

    try:
        from database.db import get_db
        db = await get_db()

        cutoff = datetime.utcnow().timestamp() - period_hours * 3600
        cutoff_dt = datetime.utcfromtimestamp(cutoff).strftime("%Y-%m-%d %H:%M:%S")

        async with db.execute(
            """SELECT tl.*, tl.side, tl.symbol, tl.amount, tl.price, tl.created_at
               FROM trade_log tl
               WHERE tl.telegram_id = ? AND tl.created_at >= ?
               ORDER BY tl.created_at DESC""",
            (user_id, cutoff_dt),
        ) as cursor:
            trades = [dict(r) for r in await cursor.fetchall()]

        total_trades = len(trades)
        wins = 0
        net_pnl = 0.0
        buy_count = 0
        sell_count = 0

        for t in trades:
            side = (t.get("side") or "").lower()
            if side in ("bid", "long", "buy"):
                buy_count += 1
            else:
                sell_count += 1

        return _json({
            "total_trades": total_trades,
            "win_rate": round(wins / total_trades * 100, 1) if total_trades else 0,
            "net_pnl": round(net_pnl, 2),
            "fee_drag": 0.0,
            "monster_dep": 0,
            "direction_stats": {"long": buy_count, "short": sell_count},
            "hold_buckets": {},
            "recommendations": [],
            "period": period_str,
            "user_id": user_id,
        })

    except Exception as exc:
        logger.error("api_reflect error: %s", exc)
        return _json({"error": str(exc)}, status=500)


# ---------------------------------------------------------------------------
# 10. GET /api/journal?user_id=...&limit=20
# ---------------------------------------------------------------------------


async def api_journal(request: web.Request) -> web.Response:
    """Trade journal entries from trade_log."""
    user_id = _user_id(request)
    try:
        limit = int(request.rel_url.query.get("limit", 20))
    except ValueError:
        limit = 20
    limit = min(limit, 200)

    try:
        from database.db import get_db
        db = await get_db()

        async with db.execute(
            """SELECT symbol, side, amount, price, order_type, created_at
               FROM trade_log
               WHERE telegram_id = ?
               ORDER BY created_at DESC
               LIMIT ?""",
            (user_id, limit),
        ) as cursor:
            rows = [dict(r) for r in await cursor.fetchall()]

        entries = []
        for r in rows:
            entries.append({
                "symbol": r.get("symbol"),
                "side": r.get("side"),
                "pnl": None,
                "entry_reason": r.get("order_type"),
                "exit_reason": None,
                "quality": None,
                "hold_time": None,
                "timestamp": r.get("created_at"),
                "amount": r.get("amount"),
                "price": r.get("price"),
            })

        return _json({"entries": entries, "user_id": user_id})

    except Exception as exc:
        logger.error("api_journal error: %s", exc)
        return _json({"error": str(exc)}, status=500)


# ---------------------------------------------------------------------------
# 11. GET /api/journal/review?user_id=...
# ---------------------------------------------------------------------------


async def api_journal_review(request: web.Request) -> web.Response:
    """Daily review comparing today vs 7-day average."""
    user_id = _user_id(request)

    try:
        from database.db import get_db
        db = await get_db()

        today_start = datetime.utcnow().strftime("%Y-%m-%d") + " 00:00:00"

        async with db.execute(
            "SELECT COUNT(*) FROM trade_log WHERE telegram_id = ? AND created_at >= ?",
            (user_id, today_start),
        ) as cursor:
            row = await cursor.fetchone()
            today_trades = row[0] if row else 0

        week_start = datetime.utcfromtimestamp(time.time() - 7 * 86400).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        async with db.execute(
            "SELECT COUNT(*) FROM trade_log WHERE telegram_id = ? AND created_at >= ?",
            (user_id, week_start),
        ) as cursor:
            row = await cursor.fetchone()
            week_trades = row[0] if row else 0

        avg_7d_trades = round(week_trades / 7, 1) if week_trades else 0

        return _json({
            "today": {"trades": today_trades, "pnl": 0.0, "win_rate": 0.0},
            "avg_7d": {"trades": avg_7d_trades, "pnl": 0.0, "win_rate": 0.0},
            "trend": {
                "trades": "up" if today_trades >= avg_7d_trades else "down",
                "pnl": "neutral",
            },
            "user_id": user_id,
        })

    except Exception as exc:
        logger.error("api_journal_review error: %s", exc)
        return _json({"error": str(exc)}, status=500)


# ---------------------------------------------------------------------------
# 12. GET /api/strategies
# ---------------------------------------------------------------------------


async def api_strategies(request: web.Request) -> web.Response:
    """All active strategies with their states."""
    try:
        from database.db import get_db
        db = await get_db()

        # Grids
        grids = []
        try:
            async with db.execute(
                "SELECT telegram_id, symbol, price_low, price_high, num_grids, "
                "       grids_filled, realized_pnl, active "
                "FROM grid_configs WHERE active = 1"
            ) as cursor:
                for row in await cursor.fetchall():
                    r = dict(row)
                    # grids_filled is a JSON dict like {"1": order_id, ...}
                    import json as _json_mod
                    filled_raw = r.get("grids_filled", "{}")
                    try:
                        fills_count = len(_json_mod.loads(filled_raw or "{}"))
                    except Exception:
                        fills_count = 0
                    grids.append({
                        "user": r.get("telegram_id"),
                        "symbol": r.get("symbol"),
                        "levels": r.get("num_grids"),
                        "fills": fills_count,
                        "pnl": r.get("realized_pnl", 0.0),
                        "active": bool(r.get("active")),
                        "price_low": r.get("price_low"),
                        "price_high": r.get("price_high"),
                    })
        except Exception as exc:
            logger.debug("Grid query failed: %s", exc)

        # DCA
        dca = []
        try:
            async with db.execute(
                "SELECT telegram_id, symbol, side, orders_total, orders_executed, "
                "       next_execution "
                "FROM dca_configs WHERE active = 1"
            ) as cursor:
                for row in await cursor.fetchall():
                    r = dict(row)
                    dca.append({
                        "user": r.get("telegram_id"),
                        "symbol": r.get("symbol"),
                        "side": r.get("side"),
                        "remaining": (r.get("orders_total", 0) or 0) - (r.get("orders_executed", 0) or 0),
                        "next_at": r.get("next_execution"),
                    })
        except Exception as exc:
            logger.debug("DCA query failed: %s", exc)

        # TWAP
        twap = []
        try:
            async with db.execute(
                "SELECT telegram_id, symbol, side, num_slices, slices_executed "
                "FROM twap_orders WHERE active = 1"
            ) as cursor:
                for row in await cursor.fetchall():
                    r = dict(row)
                    twap.append({
                        "user": r.get("telegram_id"),
                        "symbol": r.get("symbol"),
                        "side": r.get("side"),
                        "slices_done": r.get("slices_executed", 0),
                        "slices_total": r.get("num_slices", 0),
                    })
        except Exception as exc:
            logger.debug("TWAP query failed: %s", exc)

        # Trailing stops
        trails = []
        try:
            async with db.execute(
                "SELECT telegram_id, symbol, side, trail_percent, peak_price, "
                "       callback_price "
                "FROM trailing_stops WHERE active = 1"
            ) as cursor:
                for row in await cursor.fetchall():
                    r = dict(row)
                    trails.append({
                        "user": r.get("telegram_id"),
                        "symbol": r.get("symbol"),
                        "phase": "1",
                        "tier": None,
                        "peak_roe": None,
                        "trail_pct": r.get("trail_percent"),
                    })
        except Exception as exc:
            logger.debug("Trailing stops query failed: %s", exc)

        # Copy trading
        copy = []
        try:
            async with db.execute(
                "SELECT telegram_id, master_wallet, sizing_mode "
                "FROM copy_configs WHERE active = 1"
            ) as cursor:
                for row in await cursor.fetchall():
                    r = dict(row)
                    copy.append({
                        "user": r.get("telegram_id"),
                        "master": r.get("master_wallet"),
                        "mode": r.get("sizing_mode"),
                        "total_copied": 0,
                    })
        except Exception as exc:
            logger.debug("Copy config query failed: %s", exc)

        return _json({
            "grids": grids,
            "dca": dca,
            "twap": twap,
            "trails": trails,
            "brackets": [],
            "mean_reversion": [],
            "copy": copy,
        })

    except Exception as exc:
        logger.error("api_strategies error: %s", exc)
        return _json({"error": str(exc)}, status=500)


# ---------------------------------------------------------------------------
# 13. GET /api/reconciliation?user_id=...
# ---------------------------------------------------------------------------


async def api_reconciliation(request: web.Request) -> web.Response:
    """Last reconciliation result for a user."""
    user_id = _user_id(request)

    try:
        from database.db import get_db, get_user
        user = await get_user(user_id)
        if not user or not user.get("pacifica_account"):
            return _json({
                "status": "no_wallet",
                "message": "No wallet linked for this user.",
                "user_id": user_id,
            })

        client = await _make_client_for_user(user)
        if not client:
            return _json({
                "status": "error",
                "message": "Failed to create client.",
                "user_id": user_id,
            })

        try:
            account = await client.get_account_info()
            positions = await client.get_positions() or []
            orders = await client.get_open_orders() or []
        finally:
            await client.close()

        return _json({
            "status": "ok",
            "account": account,
            "positions_count": len(positions),
            "open_orders_count": len(orders),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "user_id": user_id,
        })

    except Exception as exc:
        logger.error("api_reconciliation error: %s", exc)
        return _json({"error": str(exc)}, status=500)


# ---------------------------------------------------------------------------
# 14. GET /api/markets
# ---------------------------------------------------------------------------


async def api_markets(request: web.Request) -> web.Response:
    """All available markets with prices from Pacifica."""
    try:
        from bot.services.funding_monitor import get_all_funding_rates
        rates = await get_all_funding_rates()

        markets = []
        for r in rates:
            markets.append({
                "symbol": r.get("symbol"),
                "price": r.get("mark_price"),
                "funding_rate": r.get("funding_rate"),
                "volume_24h": None,
                "oi": r.get("open_interest"),
                "max_leverage": None,
            })

        return _json(markets)

    except Exception as exc:
        logger.error("api_markets error: %s", exc)
        return _json({"error": str(exc)}, status=500)


# ---------------------------------------------------------------------------
# CORS pre-flight handler
# ---------------------------------------------------------------------------


async def cors_preflight(request: web.Request) -> web.Response:
    """Handle CORS pre-flight OPTIONS requests."""
    return web.Response(
        status=204,
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type, Authorization",
        },
    )


# ---------------------------------------------------------------------------
# Helpers (async gather with graceful degradation)
# ---------------------------------------------------------------------------


async def _gather_safely(*coros):
    """Run coroutines concurrently; return None for any that raise."""
    import asyncio
    results = []
    for coro in coros:
        try:
            results.append(await coro)
        except Exception:
            results.append(None)
    return results


# ---------------------------------------------------------------------------
# Route registration
# ---------------------------------------------------------------------------


def register_dashboard_routes(app: web.Application):
    """Register all dashboard API routes on the aiohttp application."""
    app.router.add_get("/api/overview", api_overview)
    app.router.add_get("/api/positions", api_positions)
    app.router.add_get("/api/regime", api_regime)
    app.router.add_get("/api/risk", api_risk)
    app.router.add_get("/api/portfolio", api_portfolio)
    app.router.add_get("/api/signals", api_signals)
    app.router.add_get("/api/funding", api_funding)
    app.router.add_get("/api/gaps", api_gaps)
    app.router.add_get("/api/reflect", api_reflect)
    app.router.add_get("/api/journal", api_journal)
    app.router.add_get("/api/journal/review", api_journal_review)
    app.router.add_get("/api/strategies", api_strategies)
    app.router.add_get("/api/reconciliation", api_reconciliation)
    app.router.add_get("/api/markets", api_markets)

    # OPTIONS pre-flight for all /api/* paths
    app.router.add_route("OPTIONS", "/api/{path_info:.*}", cors_preflight)

    logger.info("Dashboard API routes registered (/api/*)")
