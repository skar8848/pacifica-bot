"""
Trade Journal — Auto-generates journal entries for each closed position.

Each entry includes:
- Entry reasoning (which signal triggered it, market context)
- Exit reasoning (what caused the exit)
- Signal quality assessment (good / fair / poor)
- Retrospective suggestion
- P&L summary

DB table created on first use:
  journal_entries:
    id INTEGER PRIMARY KEY AUTOINCREMENT
    telegram_id INTEGER
    symbol TEXT
    side TEXT              -- 'long' or 'short'
    entry_price REAL
    exit_price REAL
    size REAL              -- notional USD
    pnl REAL
    pnl_pct REAL
    entry_reason TEXT
    exit_reason TEXT
    signal_quality TEXT    -- 'good', 'fair', 'poor'
    retrospective TEXT
    hold_duration_min REAL
    entry_time TIMESTAMP
    exit_time TIMESTAMP
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
"""

import logging
import json
from datetime import datetime, timedelta
from typing import Optional

from database.db import get_db

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Table initialisation
# ---------------------------------------------------------------------------

_table_ready = False


async def _init_table() -> None:
    """Create journal_entries table if it does not exist yet."""
    global _table_ready
    if _table_ready:
        return
    try:
        db = await get_db()
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS journal_entries (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                telegram_id      INTEGER NOT NULL,
                symbol           TEXT NOT NULL,
                side             TEXT NOT NULL,
                entry_price      REAL NOT NULL,
                exit_price       REAL NOT NULL,
                size             REAL DEFAULT 0,
                pnl              REAL DEFAULT 0,
                pnl_pct          REAL DEFAULT 0,
                entry_reason     TEXT DEFAULT '',
                exit_reason      TEXT DEFAULT '',
                signal_quality   TEXT DEFAULT 'fair',
                retrospective    TEXT DEFAULT '',
                hold_duration_min REAL DEFAULT 0,
                entry_time       TIMESTAMP,
                exit_time        TIMESTAMP,
                created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        await db.execute(
            "CREATE INDEX IF NOT EXISTS idx_journal_user "
            "ON journal_entries(telegram_id, created_at DESC)"
        )
        await db.commit()
        _table_ready = True
    except Exception as exc:
        logger.error("trade_journal: failed to init table — %s", exc)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _classify_side(side: str) -> str:
    s = (side or "").lower().strip()
    if s in ("bid", "buy", "long"):
        return "long"
    return "short"


def _assess_signal_quality(pnl: float, pnl_pct: float, hold_minutes: float) -> str:
    """
    GOOD : PnL > 0 and hold > 5 min
    FAIR : PnL > 0 but hold < 5 min (scalp), OR PnL < 0 but small loss (< 1%)
    POOR : PnL < -2% OR hold < 1 min (likely bad entry)
    """
    if hold_minutes < 1:
        return "poor"
    if pnl_pct < -2.0:
        return "poor"
    if pnl > 0 and hold_minutes >= 5:
        return "good"
    if pnl > 0 and hold_minutes < 5:
        return "fair"   # profitable scalp
    if pnl < 0 and abs(pnl_pct) < 1.0:
        return "fair"   # disciplined small loss
    return "poor"


def _auto_entry_reason(side: str, symbol: str, entry_price: float, reason: str) -> str:
    if reason:
        return f"Opened {side} {symbol} — {reason} at ${entry_price:.4f}"
    return f"Opened {side} {symbol} — manual entry at ${entry_price:.4f}"


def _auto_exit_reason(exit_price: float, pnl_pct: float, hold_minutes: float, reason: str) -> str:
    hold_str = _format_duration(hold_minutes)
    roi_str = f"{'+' if pnl_pct >= 0 else ''}{pnl_pct:.2f}% ROE"
    base = f"Closed at ${exit_price:.4f} — {reason or 'manual close'} | {roi_str} after {hold_str}"
    return base


def _auto_retrospective(pnl: float, pnl_pct: float, hold_minutes: float, signal_quality: str) -> str:
    """Generate a one-line lesson based on the trade outcome."""
    if signal_quality == "good":
        if pnl_pct > 5:
            return "Excellent execution — patience paid off. Replicate this setup."
        return "Solid trade. Entry and exit timing were disciplined."
    if signal_quality == "fair":
        if pnl > 0 and hold_minutes < 5:
            return "Profitable but held less than 5 min — consider letting winners run further."
        if pnl < 0 and abs(pnl_pct) < 1.0:
            return "Small loss, stop respected — good risk discipline. Review entry trigger."
        return "Marginal outcome. Look for higher-conviction setups before entry."
    # poor
    if hold_minutes < 1:
        return "Position held under 1 min — likely a bad entry. Wait for confirmation before entering."
    if pnl_pct < -2.0:
        return f"Loss of {pnl_pct:.1f}% — stop-loss may need to be tighter, or entry criteria stricter."
    return "Review entry conditions; avoid low-quality signals."


def _format_duration(minutes: float) -> str:
    if minutes < 1:
        return f"{int(minutes * 60)}s"
    if minutes < 60:
        return f"{int(minutes)}m"
    hrs = int(minutes // 60)
    mins = int(minutes % 60)
    return f"{hrs}h {mins}m" if mins else f"{hrs}h"


def _row_to_dict(row) -> dict:
    """Convert a sqlite3.Row or tuple+cols to dict."""
    if hasattr(row, "keys"):
        return dict(row)
    return dict(row)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def record_journal_entry(
    telegram_id: int,
    symbol: str,
    side: str,
    entry_price: float,
    exit_price: float,
    size: float,
    pnl: float,
    entry_reason: str = "",
    exit_reason: str = "",
    entry_time: Optional[datetime] = None,
    exit_time: Optional[datetime] = None,
) -> int:
    """
    Record a closed-trade journal entry.  All text fields are auto-generated
    when not provided.

    Returns the new row id.
    """
    await _init_table()

    side_norm = _classify_side(side)
    now = datetime.utcnow()
    entry_time = entry_time or now
    exit_time = exit_time or now

    hold_minutes = max((exit_time - entry_time).total_seconds() / 60, 0.0)
    pnl_pct = (pnl / (size + 1e-12)) * 100 if size else 0.0

    signal_quality = _assess_signal_quality(pnl, pnl_pct, hold_minutes)

    full_entry_reason = _auto_entry_reason(side_norm, symbol, entry_price, entry_reason)
    full_exit_reason = _auto_exit_reason(exit_price, pnl_pct, hold_minutes, exit_reason)
    retrospective = _auto_retrospective(pnl, pnl_pct, hold_minutes, signal_quality)

    try:
        db = await get_db()
        cursor = await db.execute(
            """
            INSERT INTO journal_entries
              (telegram_id, symbol, side, entry_price, exit_price, size,
               pnl, pnl_pct, entry_reason, exit_reason, signal_quality,
               retrospective, hold_duration_min, entry_time, exit_time)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                telegram_id, symbol, side_norm, entry_price, exit_price, size,
                pnl, pnl_pct, full_entry_reason, full_exit_reason, signal_quality,
                retrospective, hold_minutes,
                entry_time.isoformat(), exit_time.isoformat(),
            ),
        )
        await db.commit()
        return cursor.lastrowid  # type: ignore
    except Exception as exc:
        logger.error("trade_journal: record_journal_entry failed — %s", exc)
        return -1


async def get_journal(telegram_id: int, limit: int = 20) -> list[dict]:
    """
    Fetch the most recent journal entries for the user.

    Returns a list of dicts, newest first.
    """
    await _init_table()
    try:
        db = await get_db()
        async with db.execute(
            """SELECT * FROM journal_entries
               WHERE telegram_id = ?
               ORDER BY created_at DESC
               LIMIT ?""",
            (telegram_id, limit),
        ) as cursor:
            cols = [d[0] for d in cursor.description]
            rows = await cursor.fetchall()
            return [dict(zip(cols, row)) for row in rows]
    except Exception as exc:
        logger.error("trade_journal: get_journal failed — %s", exc)
        return []


async def get_journal_stats(telegram_id: int, days: int = 7) -> dict:
    """
    Aggregate stats over the past `days` days.

    Returns:
        total_entries, avg_pnl, best_pnl, worst_pnl, win_rate,
        most_traded_symbol, avg_hold_time_min,
        signal_quality_breakdown: {good: N, fair: N, poor: N}
    """
    await _init_table()

    since = datetime.utcnow() - timedelta(days=days)
    since_str = since.strftime("%Y-%m-%d %H:%M:%S")

    empty = {
        "total_entries": 0,
        "avg_pnl": 0.0,
        "best_pnl": 0.0,
        "worst_pnl": 0.0,
        "win_rate": 0.0,
        "most_traded_symbol": None,
        "avg_hold_time_min": 0.0,
        "signal_quality_breakdown": {"good": 0, "fair": 0, "poor": 0},
    }

    try:
        db = await get_db()

        async with db.execute(
            """SELECT pnl, hold_duration_min, signal_quality, symbol
               FROM journal_entries
               WHERE telegram_id = ? AND created_at >= ?""",
            (telegram_id, since_str),
        ) as cursor:
            cols = [d[0] for d in cursor.description]
            rows = [dict(zip(cols, r)) for r in await cursor.fetchall()]

        if not rows:
            return empty

        total = len(rows)
        pnls = [r["pnl"] for r in rows]
        wins = sum(1 for p in pnls if p > 0)
        avg_pnl = sum(pnls) / total
        best_pnl = max(pnls)
        worst_pnl = min(pnls)
        avg_hold = sum(r["hold_duration_min"] for r in rows) / total

        # Most traded symbol
        sym_counts: dict[str, int] = {}
        for r in rows:
            sym_counts[r["symbol"]] = sym_counts.get(r["symbol"], 0) + 1
        most_traded = max(sym_counts, key=sym_counts.get) if sym_counts else None  # type: ignore

        # Signal quality breakdown
        sq_breakdown: dict[str, int] = {"good": 0, "fair": 0, "poor": 0}
        for r in rows:
            sq = r.get("signal_quality", "fair")
            sq_breakdown[sq] = sq_breakdown.get(sq, 0) + 1

        return {
            "total_entries": total,
            "avg_pnl": round(avg_pnl, 4),
            "best_pnl": round(best_pnl, 4),
            "worst_pnl": round(worst_pnl, 4),
            "win_rate": round(wins / total * 100, 1),
            "most_traded_symbol": most_traded,
            "avg_hold_time_min": round(avg_hold, 1),
            "signal_quality_breakdown": sq_breakdown,
        }

    except Exception as exc:
        logger.error("trade_journal: get_journal_stats failed — %s", exc)
        return empty


async def get_daily_review(telegram_id: int) -> dict:
    """
    Compare today's trading vs the prior 7-day rolling average.

    Returns:
        today: {trades, pnl, win_rate}
        avg_7d: {trades, pnl, win_rate}
        trends: {trades: '↑'/'↓'/'→', pnl: '↑'/'↓'/'→', win_rate: '↑'/'↓'/'→'}
    """
    await _init_table()

    now = datetime.utcnow()
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    week_start = today_start - timedelta(days=7)

    empty_day = {"trades": 0, "pnl": 0.0, "win_rate": 0.0}

    def _trend(today_val: float, avg_val: float, threshold: float = 0.01) -> str:
        if today_val > avg_val + threshold:
            return "↑"
        if today_val < avg_val - threshold:
            return "↓"
        return "→"

    try:
        db = await get_db()

        # Today's entries
        async with db.execute(
            """SELECT pnl FROM journal_entries
               WHERE telegram_id = ? AND created_at >= ?""",
            (telegram_id, today_start.isoformat()),
        ) as cursor:
            today_rows = [r[0] for r in await cursor.fetchall()]

        # Last 7 days' entries (excluding today)
        async with db.execute(
            """SELECT created_at, pnl FROM journal_entries
               WHERE telegram_id = ? AND created_at >= ? AND created_at < ?""",
            (telegram_id, week_start.isoformat(), today_start.isoformat()),
        ) as cursor:
            cols = [d[0] for d in cursor.description]
            week_rows = [dict(zip(cols, r)) for r in await cursor.fetchall()]

    except Exception as exc:
        logger.error("trade_journal: get_daily_review failed — %s", exc)
        return {
            "today": empty_day,
            "avg_7d": empty_day,
            "trends": {"trades": "→", "pnl": "→", "win_rate": "→"},
        }

    # Today stats
    if today_rows:
        today_stats = {
            "trades": len(today_rows),
            "pnl": round(sum(today_rows), 4),
            "win_rate": round(sum(1 for p in today_rows if p > 0) / len(today_rows) * 100, 1),
        }
    else:
        today_stats = empty_day.copy()

    # 7-day rolling average (per day)
    if week_rows:
        days_with_data: dict[str, list[float]] = {}
        for r in week_rows:
            day_key = str(r["created_at"])[:10]
            days_with_data.setdefault(day_key, []).append(r["pnl"])

        num_days = max(len(days_with_data), 1)
        all_pnls = [r["pnl"] for r in week_rows]
        all_wins = sum(1 for p in all_pnls if p > 0)
        avg_7d_stats = {
            "trades": round(len(week_rows) / num_days, 1),
            "pnl": round(sum(all_pnls) / num_days, 4),
            "win_rate": round(all_wins / len(week_rows) * 100, 1) if week_rows else 0.0,
        }
    else:
        avg_7d_stats = empty_day.copy()

    trends = {
        "trades": _trend(float(today_stats["trades"]), float(avg_7d_stats["trades"]), 0.5),
        "pnl": _trend(float(today_stats["pnl"]), float(avg_7d_stats["pnl"]), 0.01),
        "win_rate": _trend(float(today_stats["win_rate"]), float(avg_7d_stats["win_rate"]), 1.0),
    }

    return {
        "today": today_stats,
        "avg_7d": avg_7d_stats,
        "trends": trends,
    }


def format_journal_entry(entry: dict) -> str:
    """Format a single journal entry dict for Telegram display."""
    side = entry.get("side", "?").upper()
    symbol = entry.get("symbol", "?")
    pnl = float(entry.get("pnl", 0))
    pnl_pct = float(entry.get("pnl_pct", 0))
    hold = float(entry.get("hold_duration_min", 0))
    quality = (entry.get("signal_quality") or "fair").upper()
    entry_reason = entry.get("entry_reason", "")
    exit_reason = entry.get("exit_reason", "")
    retrospective = entry.get("retrospective", "")
    created = str(entry.get("created_at", ""))[:16].replace("T", " ")

    pnl_sign = "+" if pnl >= 0 else ""
    pct_sign = "+" if pnl_pct >= 0 else ""

    lines = [
        f"Trade Journal — {side} {symbol}",
        f"Date: {created} UTC",
        f"Quality: {quality}",
        "",
        f"Entry:  {entry_reason}",
        f"Exit:   {exit_reason}",
        "",
        f"PnL:    {pnl_sign}${pnl:.2f}  ({pct_sign}{pnl_pct:.2f}%)",
        f"Hold:   {_format_duration(hold)}",
        "",
        f"Lesson: {retrospective}",
    ]
    return "\n".join(lines)


def format_daily_review(review: dict) -> str:
    """Format a daily review dict for Telegram display."""
    today = review.get("today", {})
    avg = review.get("avg_7d", {})
    trends = review.get("trends", {})

    t_arrow = trends.get("trades", "→")
    p_arrow = trends.get("pnl", "→")
    w_arrow = trends.get("win_rate", "→")

    today_pnl = float(today.get("pnl", 0))
    avg_pnl = float(avg.get("pnl", 0))

    lines = [
        "Daily Review",
        "",
        f"{'Metric':<14} {'Today':>10}   {'7d Avg':>10}   Trend",
        f"{'Trades':<14} {today.get('trades', 0):>10}   {avg.get('trades', 0):>10.1f}   {t_arrow}",
        f"{'PnL ($)':<14} {'+' if today_pnl >= 0 else ''}{today_pnl:>9.2f}   "
        f"{'+' if avg_pnl >= 0 else ''}{avg_pnl:>9.2f}   {p_arrow}",
        f"{'Win Rate':<14} {today.get('win_rate', 0):>9.1f}%   "
        f"{avg.get('win_rate', 0):>9.1f}%   {w_arrow}",
    ]
    return "\n".join(lines)
