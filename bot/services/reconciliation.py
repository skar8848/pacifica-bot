"""
Reconciliation — sync internal state with exchange positions.
Runs periodically to detect:
- Orphan internal positions (we think we have a position but exchange doesn't)
- Orphan exchange positions (exchange has position but we don't track it)
- Size mismatches (>1% difference)
"""

import asyncio
import json
import logging
import time
from datetime import datetime

from aiogram import Bot

from bot.services.pacifica_client import PacificaClient
from bot.services.wallet_manager import decrypt_private_key
from database.db import get_db

logger = logging.getLogger(__name__)

_running = False
CHECK_INTERVAL = 300  # 5 minutes

# Size mismatch threshold (1% = 0.01)
MISMATCH_THRESHOLD = 0.01

# In-memory cache of last reconciliation results per user
_last_results: dict[int, dict] = {}


# ---------------------------------------------------------------------------
# DB setup
# ---------------------------------------------------------------------------

async def _init_reconciliation_tables():
    """Create reconciliation result tables if they don't exist."""
    db = await get_db()
    await db.executescript("""
        CREATE TABLE IF NOT EXISTS reconciliation_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            telegram_id INTEGER,
            matched_count INTEGER DEFAULT 0,
            orphan_internal_count INTEGER DEFAULT 0,
            orphan_exchange_count INTEGER DEFAULT 0,
            mismatch_count INTEGER DEFAULT 0,
            details TEXT DEFAULT '{}',
            run_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_recon_telegram
            ON reconciliation_results(telegram_id, run_at);
    """)
    await db.commit()
    logger.info("Reconciliation tables ready")


# ---------------------------------------------------------------------------
# Internal position aggregation
# ---------------------------------------------------------------------------

async def _get_internal_positions(telegram_id: int) -> dict[str, dict]:
    """
    Collect all internally tracked positions for a user.

    Returns: {"{symbol}:{side}": {"symbol": str, "side": str, "size": float, "source": str}}

    Sources: trailing_stops, grid_configs, dca_configs, arb_positions, mean_reversion_positions
    """
    db = await get_db()
    positions: dict[str, dict] = {}

    # --- Trailing stops ---
    async with db.execute(
        "SELECT symbol, side, 1.0 as size FROM trailing_stops WHERE telegram_id = ? AND active = 1",
        (telegram_id,),
    ) as cursor:
        for row in await cursor.fetchall():
            key = f"{row[0]}:{_normalize_side(row[1])}"
            positions[key] = {
                "symbol": row[0],
                "side": _normalize_side(row[1]),
                "size": float(row[2]),
                "source": "trailing_stop",
            }

    # --- Grid configs (active grids imply open positions on both sides) ---
    async with db.execute(
        "SELECT symbol, 0.0 as size FROM grid_configs WHERE telegram_id = ? AND active = 1",
        (telegram_id,),
    ) as cursor:
        for row in await cursor.fetchall():
            # Grids hold positions in both directions; flag symbol as tracked
            for side in ("long", "short"):
                key = f"{row[0]}:{side}"
                if key not in positions:
                    positions[key] = {
                        "symbol": row[0],
                        "side": side,
                        "size": 0.0,  # grid size varies, can't easily sum
                        "source": "grid",
                    }

    # --- DCA configs (active = position being built) ---
    async with db.execute(
        """SELECT symbol, side, avg_entry, orders_executed
           FROM dca_configs WHERE telegram_id = ? AND active = 1""",
        (telegram_id,),
    ) as cursor:
        for row in await cursor.fetchall():
            norm_side = _normalize_side(row[1])
            key = f"{row[0]}:{norm_side}"
            if key not in positions:
                positions[key] = {
                    "symbol": row[0],
                    "side": norm_side,
                    "size": 0.0,  # DCA accumulates; approximate
                    "source": "dca",
                }

    # --- Arb positions ---
    try:
        async with db.execute(
            "SELECT symbol, long_exchange, short_exchange, size_usd FROM arb_positions WHERE telegram_id = ? AND active = 1",
            (telegram_id,),
        ) as cursor:
            for row in await cursor.fetchall():
                symbol = row[0]
                pac_side = "long" if row[1] == "pacifica" else "short"
                key = f"{symbol}:{pac_side}"
                if key not in positions:
                    positions[key] = {
                        "symbol": symbol,
                        "side": pac_side,
                        "size": 0.0,
                        "source": "arb",
                    }
    except Exception:
        pass  # Table may not exist

    # --- Mean reversion positions ---
    try:
        async with db.execute(
            "SELECT symbol, side, size FROM mean_reversion_positions WHERE telegram_id = ? AND status = 'open'",
            (telegram_id,),
        ) as cursor:
            for row in await cursor.fetchall():
                norm_side = _normalize_side(row[1])
                key = f"{row[0]}:{norm_side}"
                positions[key] = {
                    "symbol": row[0],
                    "side": norm_side,
                    "size": float(row[2]),
                    "source": "mean_reversion",
                }
    except Exception:
        pass  # Table may not exist

    return positions


def _normalize_side(side: str) -> str:
    """Normalize side to 'long' or 'short'."""
    s = side.lower()
    if s in ("buy", "bid", "long"):
        return "long"
    if s in ("sell", "ask", "short"):
        return "short"
    return s


def _parse_exchange_positions(raw_positions: list) -> dict[str, dict]:
    """
    Convert exchange position list to internal format.

    Returns: {"{symbol}:{side}": {"symbol": str, "side": str, "size": float}}
    """
    result: dict[str, dict] = {}
    if not raw_positions:
        return result

    for pos in raw_positions:
        if not isinstance(pos, dict):
            continue

        symbol = pos.get("symbol", "")
        if not symbol:
            continue

        # Determine side from position fields
        raw_side = pos.get("side", "")
        if not raw_side:
            # Some APIs use positive/negative size to indicate direction
            size = float(pos.get("size", pos.get("amount", 0)) or 0)
            raw_side = "long" if size >= 0 else "short"

        side = _normalize_side(raw_side)
        size = abs(float(pos.get("size", pos.get("amount", 0)) or 0))

        if size == 0:
            continue  # skip zero-size positions

        key = f"{symbol}:{side}"
        result[key] = {
            "symbol": symbol,
            "side": side,
            "size": size,
            "raw": pos,
        }

    return result


# ---------------------------------------------------------------------------
# Core reconciliation logic
# ---------------------------------------------------------------------------

async def run_reconciliation(telegram_id: int) -> dict:
    """
    Reconcile internal vs exchange positions for a single user.

    Returns dict with matched, orphan_internal, orphan_exchange, mismatches.
    """
    await _init_reconciliation_tables()
    result = {
        "matched": [],
        "orphan_internal": [],
        "orphan_exchange": [],
        "mismatches": [],
        "telegram_id": telegram_id,
        "run_at": time.time(),
    }

    # Get user wallet
    db = await get_db()
    async with db.execute(
        "SELECT pacifica_account, agent_wallet_encrypted FROM users WHERE telegram_id = ?",
        (telegram_id,),
    ) as cursor:
        user = await cursor.fetchone()

    if not user or not user[0] or not user[1]:
        logger.debug("Reconciliation: user %s has no wallet, skipping", telegram_id)
        return result

    # Fetch exchange positions
    exchange_positions: dict[str, dict] = {}
    client = None
    try:
        kp = decrypt_private_key(user[1])
        client = PacificaClient(account=user[0], keypair=kp)
        raw_positions = await client.get_positions()
        exchange_positions = _parse_exchange_positions(raw_positions or [])
        logger.debug(
            "Reconciliation user %s: %d exchange positions", telegram_id, len(exchange_positions)
        )
    except Exception as e:
        logger.error("Reconciliation: failed to fetch positions for user %s: %s", telegram_id, e)
        return result
    finally:
        if client:
            await client.close()

    # Get internal positions
    try:
        internal_positions = await _get_internal_positions(telegram_id)
    except Exception as e:
        logger.error("Reconciliation: failed to load internal positions for user %s: %s", telegram_id, e)
        return result

    # Compare
    all_keys = set(internal_positions.keys()) | set(exchange_positions.keys())

    for key in all_keys:
        internal = internal_positions.get(key)
        exchange = exchange_positions.get(key)

        if internal and exchange:
            # Both sides know about this position — check size
            internal_size = internal.get("size", 0)
            exchange_size = exchange.get("size", 0)

            if internal_size > 0 and exchange_size > 0:
                diff_pct = abs(internal_size - exchange_size) / exchange_size
                if diff_pct > MISMATCH_THRESHOLD:
                    entry = {
                        "symbol": internal["symbol"],
                        "side": internal["side"],
                        "internal_size": internal_size,
                        "exchange_size": exchange_size,
                        "diff_pct": round(diff_pct * 100, 2),
                        "source": internal.get("source", "unknown"),
                    }
                    result["mismatches"].append(entry)
                    logger.warning(
                        "Reconciliation MISMATCH %s %s: internal=%.4f exchange=%.4f (%.1f%%), user %s",
                        internal["symbol"], internal["side"],
                        internal_size, exchange_size, diff_pct * 100, telegram_id,
                    )
                    continue

            result["matched"].append({
                "symbol": internal["symbol"],
                "side": internal["side"],
                "size": exchange_size,
                "source": internal.get("source", "unknown"),
            })

        elif internal and not exchange:
            # We track it internally but exchange doesn't have it
            entry = {
                "symbol": internal["symbol"],
                "side": internal["side"],
                "internal_size": internal.get("size", 0),
                "source": internal.get("source", "unknown"),
            }
            result["orphan_internal"].append(entry)
            logger.warning(
                "Reconciliation ORPHAN_INTERNAL: %s %s tracked internally (source=%s) "
                "but NOT on exchange — potential untracked exposure! User %s",
                internal["symbol"], internal["side"], internal.get("source"), telegram_id,
            )

        else:  # exchange and not internal
            # Exchange has it, we don't track it — likely a manual trade
            entry = {
                "symbol": exchange["symbol"],
                "side": exchange["side"],
                "exchange_size": exchange.get("size", 0),
            }
            result["orphan_exchange"].append(entry)
            logger.info(
                "Reconciliation ORPHAN_EXCHANGE: %s %s on exchange (size=%.4f) "
                "not tracked internally (manual trade?) for user %s",
                exchange["symbol"], exchange["side"], exchange.get("size", 0), telegram_id,
            )

    # Persist result to DB
    try:
        await db.execute(
            """INSERT INTO reconciliation_results
               (telegram_id, matched_count, orphan_internal_count,
                orphan_exchange_count, mismatch_count, details)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (
                telegram_id,
                len(result["matched"]),
                len(result["orphan_internal"]),
                len(result["orphan_exchange"]),
                len(result["mismatches"]),
                json.dumps({
                    "matched": result["matched"],
                    "orphan_internal": result["orphan_internal"],
                    "orphan_exchange": result["orphan_exchange"],
                    "mismatches": result["mismatches"],
                }),
            ),
        )
        await db.commit()
    except Exception as e:
        logger.error("Reconciliation: failed to persist result for user %s: %s", telegram_id, e)

    # Cache in memory for dashboard
    _last_results[telegram_id] = result

    return result


# ---------------------------------------------------------------------------
# Telegram alerts for critical issues
# ---------------------------------------------------------------------------

async def _send_reconciliation_alerts(bot: Bot, telegram_id: int, result: dict):
    """Send Telegram alerts for critical reconciliation findings."""
    orphan_internal = result.get("orphan_internal", [])
    mismatches = result.get("mismatches", [])

    if not orphan_internal and not mismatches:
        return

    lines = ["<b>Reconciliation Alert</b>\n"]

    if orphan_internal:
        lines.append(f"<b>Untracked Exposure ({len(orphan_internal)})</b>")
        lines.append("Bot tracks these but exchange has no position:")
        for item in orphan_internal:
            lines.append(
                f"  - <b>{item['symbol']}</b> {item['side'].upper()} "
                f"[{item.get('source', '?')}]"
            )
        lines.append("")

    if mismatches:
        lines.append(f"<b>Size Mismatches ({len(mismatches)})</b>")
        for item in mismatches:
            lines.append(
                f"  - <b>{item['symbol']}</b> {item['side'].upper()}: "
                f"internal={item['internal_size']:.4f} "
                f"vs exchange={item['exchange_size']:.4f} "
                f"({item['diff_pct']:.1f}% diff)"
            )

    try:
        await bot.send_message(telegram_id, "\n".join(lines))
    except Exception as e:
        logger.debug("Reconciliation: failed to send alert to %s: %s", telegram_id, e)


# ---------------------------------------------------------------------------
# Service loop
# ---------------------------------------------------------------------------

async def _run_all_users(bot: Bot):
    """Run reconciliation for all users who have a configured wallet."""
    db = await get_db()
    async with db.execute(
        "SELECT telegram_id FROM users WHERE pacifica_account IS NOT NULL AND agent_wallet_encrypted IS NOT NULL"
    ) as cursor:
        users = [row[0] for row in await cursor.fetchall()]

    if not users:
        return

    logger.debug("Reconciliation: checking %d users", len(users))

    for tg_id in users:
        try:
            result = await run_reconciliation(tg_id)

            # Alert on critical issues
            has_issues = (
                len(result.get("orphan_internal", [])) > 0 or
                len(result.get("mismatches", [])) > 0
            )
            if has_issues:
                await _send_reconciliation_alerts(bot, tg_id, result)

            if result.get("orphan_internal") or result.get("mismatches"):
                logger.warning(
                    "Reconciliation for user %s: %d matched, %d orphan_internal, "
                    "%d orphan_exchange, %d mismatches",
                    tg_id,
                    len(result.get("matched", [])),
                    len(result.get("orphan_internal", [])),
                    len(result.get("orphan_exchange", [])),
                    len(result.get("mismatches", [])),
                )

        except Exception as e:
            logger.error("Reconciliation failed for user %s: %s", tg_id, e)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def get_last_reconciliation(telegram_id: int | None = None) -> dict:
    """
    Return the last reconciliation result(s) for dashboard consumption.

    If telegram_id is None, returns results for all users (from DB).
    If telegram_id is provided, returns that user's last result.
    """
    if telegram_id is not None:
        # Try in-memory cache first
        if telegram_id in _last_results:
            return _last_results[telegram_id]

        # Fall back to DB
        db = await get_db()
        async with db.execute(
            """SELECT * FROM reconciliation_results
               WHERE telegram_id = ?
               ORDER BY run_at DESC LIMIT 1""",
            (telegram_id,),
        ) as cursor:
            row = await cursor.fetchone()
        if not row:
            return {}
        row = dict(row)
        try:
            row["details"] = json.loads(row["details"])
        except Exception:
            pass
        return row

    # All users: return latest result per user from DB
    db = await get_db()
    async with db.execute(
        """SELECT r.*
           FROM reconciliation_results r
           INNER JOIN (
               SELECT telegram_id, MAX(run_at) as max_run
               FROM reconciliation_results
               GROUP BY telegram_id
           ) latest ON r.telegram_id = latest.telegram_id AND r.run_at = latest.max_run
           ORDER BY r.run_at DESC"""
    ) as cursor:
        rows = [dict(r) for r in await cursor.fetchall()]

    for row in rows:
        try:
            row["details"] = json.loads(row["details"])
        except Exception:
            pass
    return {"results": rows}


# ---------------------------------------------------------------------------
# Service lifecycle
# ---------------------------------------------------------------------------

async def start_reconciliation_service(bot: Bot):
    """Start the reconciliation background loop."""
    global _running
    _running = True

    await _init_reconciliation_tables()

    logger.info("Reconciliation service started (check every %ds)", CHECK_INTERVAL)

    while _running:
        try:
            await _run_all_users(bot)
        except Exception as e:
            logger.error("Reconciliation service error: %s", e)
        await asyncio.sleep(CHECK_INTERVAL)


async def stop_reconciliation_service():
    global _running
    _running = False
    logger.info("Reconciliation service stopped")
