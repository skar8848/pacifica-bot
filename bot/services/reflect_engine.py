"""
REFLECT — Self-improvement engine.
Analyzes closed trades to provide actionable insights.

- FIFO round-trip pairing (matches buys/sells into complete trades)
- Win rate, gross/net PnL, profit factor
- Fee Drag Ratio: total_fees / gross_wins * 100
- Monster Trade Dependency: best_trade_pnl / net_pnl * 100
- Direction analysis (longs vs shorts performance)
- Holding period buckets (<5m, 5-15m, 15-60m, 1-4h, 4h+)
- Consecutive win/loss streaks
- Per-symbol breakdown
- Auto-generated text recommendations

NOTE: trade_log schema (as of current DB):
  id, telegram_id, symbol, side, amount (TEXT), price (TEXT),
  order_type, is_copy_trade, master_wallet, client_order_id, created_at

  'side' values observed: 'bid' (long/buy) and 'ask' (short/sell).
  amount and price are stored as TEXT — we cast to float.
  There is no pnl column; PnL is computed from paired round-trips.
"""

import logging
import asyncio
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from typing import Optional

from database.db import get_db

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_FEE_RATE = 0.0004  # taker fee 0.04%

PERIOD_HOURS = {
    "24h": 24,
    "7d": 168,
    "30d": 720,
}

HOLD_BUCKETS = ["<5m", "5-15m", "15-60m", "1-4h", "4h+"]

# Simple in-process cache: {telegram_id: (generated_at_ts, ReflectReport)}
_cache: dict[int, tuple[float, "ReflectReport"]] = {}
CACHE_TTL_SECONDS = 300  # 5 minutes


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class RoundTrip:
    symbol: str
    side: str           # "long" or "short"
    entry_price: float
    exit_price: float
    size: float         # notional USD size
    entry_time: datetime
    exit_time: datetime
    pnl: float          # USD
    fees: float         # USD
    hold_minutes: float
    source: str = ""    # 'copy' or '' for manual


@dataclass
class ReflectReport:
    period: str                  # "24h", "7d", "30d"
    total_trades: int
    win_rate: float              # 0-100
    gross_pnl: float
    net_pnl: float
    total_fees: float
    fee_drag_ratio: float        # fees / gross_wins * 100
    profit_factor: float         # gross_wins / |gross_losses|
    monster_dependency: float    # best_trade / net_pnl * 100
    best_trade: float
    worst_trade: float
    avg_win: float
    avg_loss: float
    max_streak_wins: int
    max_streak_losses: int
    direction_stats: dict        # {long: {count, pnl, win_rate}, short: {...}}
    hold_buckets: dict           # {"<5m": {count, pnl}, ...}
    symbol_stats: dict           # {BTC: {count, pnl, win_rate}, ...}
    recommendations: list
    generated_at: str


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _parse_float(val) -> float:
    """Cast a value to float safely; returns 0.0 on failure."""
    try:
        return float(val) if val not in (None, "", "None") else 0.0
    except (ValueError, TypeError):
        return 0.0


def _parse_dt(val) -> Optional[datetime]:
    """Parse an ISO datetime string or datetime object."""
    if val is None:
        return None
    if isinstance(val, datetime):
        return val
    try:
        # SQLite returns strings like "2024-01-15 12:34:56"
        return datetime.fromisoformat(str(val).replace(" ", "T"))
    except (ValueError, AttributeError):
        return None


def _classify_side(side: str) -> str:
    """Normalise 'bid'/'ask'/'buy'/'sell'/'long'/'short' to 'long'/'short'."""
    s = (side or "").lower().strip()
    if s in ("bid", "buy", "long"):
        return "long"
    return "short"


def _hold_bucket(minutes: float) -> str:
    if minutes < 5:
        return "<5m"
    if minutes < 15:
        return "5-15m"
    if minutes < 60:
        return "15-60m"
    if minutes < 240:
        return "1-4h"
    return "4h+"


def _pair_fifo(rows: list[dict]) -> list[RoundTrip]:
    """
    FIFO pairing of buy/sell legs into RoundTrip objects.

    A 'bid' (long entry) is matched with the next 'ask' (long exit) for the
    same symbol, and vice-versa for shorts.

    Because trade_log records individual orders (not fills), we treat each
    row as one complete leg.  Unmatched legs (open positions at period end)
    are discarded.
    """
    trips: list[RoundTrip] = []

    # Group by symbol
    by_symbol: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        by_symbol[row["symbol"]].append(row)

    for symbol, legs in by_symbol.items():
        # Sort chronologically
        legs_sorted = sorted(legs, key=lambda r: r.get("created_at") or "")

        # Separate queues: entries waiting for exits
        long_queue: list[dict] = []   # bid orders waiting for ask close
        short_queue: list[dict] = []  # ask orders waiting for bid close

        for leg in legs_sorted:
            raw_side = _classify_side(leg.get("side", ""))
            price = _parse_float(leg.get("price"))
            size = _parse_float(leg.get("amount"))
            ts = _parse_dt(leg.get("created_at"))
            is_copy = bool(leg.get("is_copy_trade", 0))

            if price <= 0 or size <= 0 or ts is None:
                continue

            # Determine if this is an opening or closing leg.
            # Heuristic: a 'bid' either opens a long OR closes a short.
            # We close an existing opposite position first (FIFO).
            if raw_side == "long":
                if short_queue:
                    # Close the oldest short
                    entry_leg = short_queue.pop(0)
                    e_price = _parse_float(entry_leg.get("price"))
                    e_size = _parse_float(entry_leg.get("amount"))
                    e_ts = _parse_dt(entry_leg.get("created_at"))
                    notional = e_size * e_price
                    pnl = (e_price - price) * e_size
                    fees = (notional + size * price) * DEFAULT_FEE_RATE
                    hold = (ts - e_ts).total_seconds() / 60 if e_ts else 0
                    trips.append(RoundTrip(
                        symbol=symbol, side="short",
                        entry_price=e_price, exit_price=price,
                        size=notional, entry_time=e_ts, exit_time=ts,
                        pnl=pnl, fees=fees,
                        hold_minutes=max(hold, 0),
                        source="copy" if is_copy else "",
                    ))
                else:
                    long_queue.append(leg)
            else:  # short / ask
                if long_queue:
                    # Close the oldest long
                    entry_leg = long_queue.pop(0)
                    e_price = _parse_float(entry_leg.get("price"))
                    e_size = _parse_float(entry_leg.get("amount"))
                    e_ts = _parse_dt(entry_leg.get("created_at"))
                    notional = e_size * e_price
                    pnl = (price - e_price) * e_size
                    fees = (notional + size * price) * DEFAULT_FEE_RATE
                    hold = (ts - e_ts).total_seconds() / 60 if e_ts else 0
                    trips.append(RoundTrip(
                        symbol=symbol, side="long",
                        entry_price=e_price, exit_price=price,
                        size=notional, entry_time=e_ts, exit_time=ts,
                        pnl=pnl, fees=fees,
                        hold_minutes=max(hold, 0),
                        source="copy" if is_copy else "",
                    ))
                else:
                    short_queue.append(leg)

    return trips


def _build_recommendations(
    win_rate: float,
    fdr: float,
    monster: float,
    direction_stats: dict,
    hold_buckets: dict,
    profit_factor: float,
    total_trades: int,
) -> list[str]:
    recs = []

    if total_trades == 0:
        return ["No closed trades in this period — nothing to analyse yet."]

    if fdr > 30:
        recs.append(
            f"Fee Drag Ratio is {fdr:.1f}% — fees are eating your edge. "
            "Reduce trade frequency or target higher-reward setups."
        )

    if monster > 50:
        recs.append(
            f"Monster Trade Dependency is {monster:.1f}% — profitability hinges on one outlier trade. "
            "Focus on consistent setups, not home runs."
        )

    if win_rate < 40:
        recs.append(
            f"Win rate is only {win_rate:.1f}%. "
            "Consider tighter entry criteria or waiting for stronger confluence."
        )

    short_stats = direction_stats.get("short", {})
    if short_stats.get("count", 0) >= 3 and short_stats.get("pnl", 0) < 0:
        recs.append(
            "Short positions are net negative this period. "
            "Consider avoiding shorts in the current market regime."
        )

    sub5m = hold_buckets.get("<5m", {}).get("count", 0)
    if total_trades > 0 and sub5m / total_trades > 0.5:
        recs.append(
            f"{sub5m}/{total_trades} trades closed in under 5 minutes. "
            "You may be overtrading — let positions breathe and give setups room to develop."
        )

    if 1.0 < profit_factor < 1.5:
        recs.append(
            f"Profit factor is {profit_factor:.2f} — your edge is thin. "
            "Tighten stops or improve entries to push PF above 1.5."
        )

    if profit_factor <= 1.0 and total_trades >= 3:
        recs.append(
            "Profit factor is below 1.0 — you're losing money net of wins. "
            "Review your risk/reward ratio and stop-loss discipline."
        )

    if not recs:
        recs.append("Performance looks solid. Keep executing your plan consistently.")

    return recs


def _compute_report(
    telegram_id: int,
    period: str,
    trips: list[RoundTrip],
) -> ReflectReport:
    if not trips:
        return ReflectReport(
            period=period, total_trades=0, win_rate=0.0,
            gross_pnl=0.0, net_pnl=0.0, total_fees=0.0,
            fee_drag_ratio=0.0, profit_factor=0.0,
            monster_dependency=0.0, best_trade=0.0, worst_trade=0.0,
            avg_win=0.0, avg_loss=0.0,
            max_streak_wins=0, max_streak_losses=0,
            direction_stats={"long": {"count": 0, "pnl": 0.0, "win_rate": 0.0},
                             "short": {"count": 0, "pnl": 0.0, "win_rate": 0.0}},
            hold_buckets={b: {"count": 0, "pnl": 0.0} for b in HOLD_BUCKETS},
            symbol_stats={},
            recommendations=["No closed trades in this period — nothing to analyse yet."],
            generated_at=datetime.utcnow().isoformat(),
        )

    wins = [t for t in trips if t.pnl > 0]
    losses = [t for t in trips if t.pnl <= 0]

    gross_wins = sum(t.pnl for t in wins)
    gross_losses = sum(t.pnl for t in losses)  # negative number
    total_fees = sum(t.fees for t in trips)
    gross_pnl = gross_wins + gross_losses
    net_pnl = gross_pnl - total_fees

    win_rate = len(wins) / len(trips) * 100
    fdr = (total_fees / gross_wins * 100) if gross_wins > 0 else 0.0
    gross_loss_abs = abs(gross_losses) if gross_losses else 0.0
    profit_factor = (gross_wins / gross_loss_abs) if gross_loss_abs > 0 else float("inf")

    best_trade = max((t.pnl for t in trips), default=0.0)
    worst_trade = min((t.pnl for t in trips), default=0.0)
    avg_win = (gross_wins / len(wins)) if wins else 0.0
    avg_loss = (gross_losses / len(losses)) if losses else 0.0

    monster_dependency = (best_trade / net_pnl * 100) if net_pnl > 0 else 0.0

    # Streaks
    max_win_streak = max_loss_streak = 0
    cur_win = cur_loss = 0
    for t in sorted(trips, key=lambda x: x.entry_time):
        if t.pnl > 0:
            cur_win += 1
            cur_loss = 0
        else:
            cur_loss += 1
            cur_win = 0
        max_win_streak = max(max_win_streak, cur_win)
        max_loss_streak = max(max_loss_streak, cur_loss)

    # Direction stats
    dir_data: dict[str, dict] = {
        "long": {"count": 0, "pnl": 0.0, "wins": 0},
        "short": {"count": 0, "pnl": 0.0, "wins": 0},
    }
    for t in trips:
        d = dir_data.get(t.side, dir_data["long"])
        d["count"] += 1
        d["pnl"] += t.pnl
        if t.pnl > 0:
            d["wins"] += 1

    direction_stats = {}
    for side, d in dir_data.items():
        direction_stats[side] = {
            "count": d["count"],
            "pnl": round(d["pnl"], 4),
            "win_rate": round(d["wins"] / d["count"] * 100, 1) if d["count"] else 0.0,
        }

    # Hold buckets
    bucket_data: dict[str, dict] = {b: {"count": 0, "pnl": 0.0} for b in HOLD_BUCKETS}
    for t in trips:
        b = _hold_bucket(t.hold_minutes)
        bucket_data[b]["count"] += 1
        bucket_data[b]["pnl"] += t.pnl

    hold_buckets = {b: {"count": v["count"], "pnl": round(v["pnl"], 4)}
                    for b, v in bucket_data.items()}

    # Symbol stats
    sym_data: dict[str, dict] = defaultdict(lambda: {"count": 0, "pnl": 0.0, "wins": 0})
    for t in trips:
        sym_data[t.symbol]["count"] += 1
        sym_data[t.symbol]["pnl"] += t.pnl
        if t.pnl > 0:
            sym_data[t.symbol]["wins"] += 1

    symbol_stats = {
        sym: {
            "count": v["count"],
            "pnl": round(v["pnl"], 4),
            "win_rate": round(v["wins"] / v["count"] * 100, 1),
        }
        for sym, v in sorted(sym_data.items(), key=lambda x: -x[1]["count"])
    }

    recs = _build_recommendations(
        win_rate=win_rate,
        fdr=fdr,
        monster=monster_dependency,
        direction_stats=direction_stats,
        hold_buckets=hold_buckets,
        profit_factor=profit_factor,
        total_trades=len(trips),
    )

    return ReflectReport(
        period=period,
        total_trades=len(trips),
        win_rate=round(win_rate, 1),
        gross_pnl=round(gross_pnl, 4),
        net_pnl=round(net_pnl, 4),
        total_fees=round(total_fees, 4),
        fee_drag_ratio=round(fdr, 1),
        profit_factor=round(profit_factor, 2) if profit_factor != float("inf") else 999.0,
        monster_dependency=round(monster_dependency, 1),
        best_trade=round(best_trade, 4),
        worst_trade=round(worst_trade, 4),
        avg_win=round(avg_win, 4),
        avg_loss=round(avg_loss, 4),
        max_streak_wins=max_win_streak,
        max_streak_losses=max_loss_streak,
        direction_stats=direction_stats,
        hold_buckets=hold_buckets,
        symbol_stats=symbol_stats,
        recommendations=recs,
        generated_at=datetime.utcnow().isoformat(),
    )


async def _fetch_trades(telegram_id: int, since: datetime) -> list[dict]:
    """Fetch trade_log rows for this user since the given datetime."""
    try:
        db = await get_db()
        since_str = since.strftime("%Y-%m-%d %H:%M:%S")
        async with db.execute(
            """SELECT symbol, side, amount, price, order_type, is_copy_trade,
                      client_order_id, created_at
               FROM trade_log
               WHERE telegram_id = ? AND created_at >= ?
               ORDER BY created_at ASC""",
            (telegram_id, since_str),
        ) as cursor:
            cols = [desc[0] for desc in cursor.description]
            rows = await cursor.fetchall()
            return [dict(zip(cols, row)) for row in rows]
    except Exception as exc:
        logger.warning("reflect_engine: could not fetch trades — %s", exc)
        return []


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def generate_report(telegram_id: int, period: str = "24h") -> ReflectReport:
    """
    Generate a REFLECT performance report for the given user and time period.

    Parameters
    ----------
    telegram_id : int
    period : str
        One of "24h", "7d", "30d".  Defaults to "24h".

    Returns
    -------
    ReflectReport dataclass.
    """
    hours = PERIOD_HOURS.get(period, 24)
    since = datetime.utcnow() - timedelta(hours=hours)

    rows = await _fetch_trades(telegram_id, since)
    trips = _pair_fifo(rows)
    report = _compute_report(telegram_id, period, trips)

    import time
    _cache[telegram_id] = (time.monotonic(), report)
    return report


async def get_cached_report(telegram_id: int) -> Optional[ReflectReport]:
    """
    Return the cached report if it was generated within the last 5 minutes,
    otherwise return None.
    """
    import time
    entry = _cache.get(telegram_id)
    if entry is None:
        return None
    ts, report = entry
    if time.monotonic() - ts > CACHE_TTL_SECONDS:
        return None
    return report


def format_report_text(report: ReflectReport) -> str:
    """Format a ReflectReport for Telegram display (MarkdownV2-safe plain text)."""
    if report.total_trades == 0:
        return (
            f"REFLECT — {report.period} Performance\n\n"
            "No closed round-trips found in this period.\n"
            "Pair a BUY and a matching SELL to generate analytics."
        )

    pnl_sign = "+" if report.net_pnl >= 0 else ""
    pf_str = (
        f"{report.profit_factor:.2f}"
        if report.profit_factor < 900
        else "inf"
    )

    lines = [
        f"REFLECT — {report.period} Performance",
        f"Generated: {report.generated_at[:16].replace('T', ' ')} UTC",
        "",
        f"Trades:        {report.total_trades}",
        f"Win Rate:      {report.win_rate:.1f}%",
        f"Net PnL:       {pnl_sign}${report.net_pnl:.2f}",
        f"Gross PnL:     {'+' if report.gross_pnl >= 0 else ''}${report.gross_pnl:.2f}",
        f"Total Fees:    -${report.total_fees:.2f}",
        f"Profit Factor: {pf_str}",
        "",
        f"Best Trade:    +${report.best_trade:.2f}",
        f"Worst Trade:   -${abs(report.worst_trade):.2f}",
        f"Avg Win:       +${report.avg_win:.2f}",
        f"Avg Loss:      -${abs(report.avg_loss):.2f}",
        "",
        f"Fee Drag Ratio:      {report.fee_drag_ratio:.1f}%",
        f"Monster Dependency:  {report.monster_dependency:.1f}%",
        f"Streak W/L:          {report.max_streak_wins}/{report.max_streak_losses}",
        "",
        "Direction",
    ]

    for side in ("long", "short"):
        s = report.direction_stats.get(side, {})
        cnt = s.get("count", 0)
        pnl = s.get("pnl", 0.0)
        wr = s.get("win_rate", 0.0)
        if cnt > 0:
            sign = "+" if pnl >= 0 else ""
            lines.append(f"  {side.capitalize():5}  {cnt} trades  {sign}${pnl:.2f}  WR {wr:.0f}%")

    lines.append("")
    lines.append("Hold Time Distribution")
    for bucket in HOLD_BUCKETS:
        bd = report.hold_buckets.get(bucket, {})
        cnt = bd.get("count", 0)
        if cnt > 0:
            pnl = bd.get("pnl", 0.0)
            sign = "+" if pnl >= 0 else ""
            lines.append(f"  {bucket:8}  {cnt} trades  {sign}${pnl:.2f}")

    if report.symbol_stats:
        lines.append("")
        lines.append("Per-Symbol")
        for sym, stats in list(report.symbol_stats.items())[:8]:
            pnl = stats["pnl"]
            sign = "+" if pnl >= 0 else ""
            lines.append(
                f"  {sym:10}  {stats['count']} trades  "
                f"{sign}${pnl:.2f}  WR {stats['win_rate']:.0f}%"
            )

    lines.append("")
    lines.append("Recommendations")
    for i, rec in enumerate(report.recommendations, 1):
        lines.append(f"  {i}. {rec}")

    return "\n".join(lines)


def format_report_dict(report: ReflectReport) -> dict:
    """Serialise a ReflectReport to a plain dict for the dashboard API."""
    return {
        "period": report.period,
        "total_trades": report.total_trades,
        "win_rate": report.win_rate,
        "gross_pnl": report.gross_pnl,
        "net_pnl": report.net_pnl,
        "total_fees": report.total_fees,
        "fee_drag_ratio": report.fee_drag_ratio,
        "profit_factor": report.profit_factor,
        "monster_dependency": report.monster_dependency,
        "best_trade": report.best_trade,
        "worst_trade": report.worst_trade,
        "avg_win": report.avg_win,
        "avg_loss": report.avg_loss,
        "max_streak_wins": report.max_streak_wins,
        "max_streak_losses": report.max_streak_losses,
        "direction_stats": report.direction_stats,
        "hold_buckets": report.hold_buckets,
        "symbol_stats": report.symbol_stats,
        "recommendations": report.recommendations,
        "generated_at": report.generated_at,
    }
