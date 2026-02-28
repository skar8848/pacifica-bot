"""
Whale monitoring service — snapshots leaderboard, detects whale activity,
sends alerts to users who track wallets.
"""

import asyncio
import logging
import time

from aiogram import Bot

from database.db import get_db

logger = logging.getLogger(__name__)

_running = False
_task: asyncio.Task | None = None

SNAPSHOT_INTERVAL = 300  # 5 minutes
MIN_OI_CHANGE_USD = 50_000  # alert if OI changes by $50K+
MIN_EQUITY_FOR_WHALE = 100_000  # $100K+ equity = whale


# ------------------------------------------------------------------
# DB helpers
# ------------------------------------------------------------------

async def _init_whale_tables():
    """Create tables for snapshots and wallet tracking."""
    db = await get_db()
    await db.executescript("""
        CREATE TABLE IF NOT EXISTS leaderboard_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            address TEXT NOT NULL,
            pnl_all_time REAL,
            pnl_30d REAL,
            pnl_7d REAL,
            pnl_1d REAL,
            equity REAL,
            oi REAL,
            volume_all_time REAL,
            volume_30d REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_snapshots_address
            ON leaderboard_snapshots(address);
        CREATE INDEX IF NOT EXISTS idx_snapshots_created
            ON leaderboard_snapshots(created_at);

        CREATE TABLE IF NOT EXISTS tracked_wallets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            telegram_id INTEGER NOT NULL,
            wallet_address TEXT NOT NULL,
            label TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(telegram_id, wallet_address)
        );

        CREATE TABLE IF NOT EXISTS whale_alert_settings (
            telegram_id INTEGER PRIMARY KEY,
            enabled INTEGER DEFAULT 1,
            min_oi_change REAL DEFAULT 50000,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    await db.commit()


async def add_tracked_wallet(telegram_id: int, wallet: str, label: str | None = None) -> bool:
    """Track a wallet. Returns True if added, False if already tracked."""
    db = await get_db()
    try:
        await db.execute(
            "INSERT INTO tracked_wallets (telegram_id, wallet_address, label) VALUES (?, ?, ?)",
            (telegram_id, wallet, label),
        )
        await db.commit()
        return True
    except Exception:
        return False


async def remove_tracked_wallet(telegram_id: int, wallet: str) -> bool:
    db = await get_db()
    cursor = await db.execute(
        "DELETE FROM tracked_wallets WHERE telegram_id = ? AND wallet_address = ?",
        (telegram_id, wallet),
    )
    await db.commit()
    return cursor.rowcount > 0


async def get_tracked_wallets(telegram_id: int) -> list[dict]:
    db = await get_db()
    async with db.execute(
        "SELECT * FROM tracked_wallets WHERE telegram_id = ? ORDER BY created_at DESC",
        (telegram_id,),
    ) as cursor:
        return [dict(r) for r in await cursor.fetchall()]


async def get_all_tracked_addresses() -> dict[str, list[int]]:
    """Returns {wallet_address: [telegram_id1, telegram_id2, ...]}."""
    db = await get_db()
    result: dict[str, list[int]] = {}
    async with db.execute("SELECT wallet_address, telegram_id FROM tracked_wallets") as cursor:
        async for row in cursor:
            addr = row[0]
            tg_id = row[1]
            result.setdefault(addr, []).append(tg_id)
    return result


async def get_whale_alert_subscribers() -> list[int]:
    """Get all users who enabled whale alerts."""
    db = await get_db()
    async with db.execute(
        "SELECT telegram_id FROM whale_alert_settings WHERE enabled = 1"
    ) as cursor:
        return [row[0] async for row in cursor]


async def set_whale_alerts(telegram_id: int, enabled: bool):
    db = await get_db()
    await db.execute(
        """INSERT INTO whale_alert_settings (telegram_id, enabled) VALUES (?, ?)
           ON CONFLICT(telegram_id) DO UPDATE SET enabled = ?""",
        (telegram_id, int(enabled), int(enabled)),
    )
    await db.commit()


async def get_last_snapshot(address: str) -> dict | None:
    db = await get_db()
    async with db.execute(
        "SELECT * FROM leaderboard_snapshots WHERE address = ? ORDER BY created_at DESC LIMIT 1",
        (address,),
    ) as cursor:
        row = await cursor.fetchone()
        return dict(row) if row else None


async def get_wallet_history(address: str, limit: int = 100) -> list[dict]:
    """Get snapshot history for a wallet (for charts)."""
    db = await get_db()
    async with db.execute(
        "SELECT * FROM leaderboard_snapshots WHERE address = ? ORDER BY created_at DESC LIMIT ?",
        (address, limit),
    ) as cursor:
        return [dict(r) for r in await cursor.fetchall()]


# ------------------------------------------------------------------
# Core monitoring loop
# ------------------------------------------------------------------

async def _snapshot_and_alert(bot: Bot):
    """Take a leaderboard snapshot and detect whale movements."""
    from bot.services.market_data import _get_client

    try:
        client = await _get_client()
        entries = await client.get_leaderboard(limit=100)
    except Exception as e:
        logger.warning("Leaderboard fetch failed: %s", e)
        return

    if not entries:
        return

    db = await get_db()
    tracked = await get_all_tracked_addresses()
    whale_subs = await get_whale_alert_subscribers()

    for entry in entries:
        addr = entry.get("address", "")
        if not addr:
            continue

        equity = float(entry.get("equity_current", 0))
        oi = float(entry.get("oi_current", 0))
        pnl_all = float(entry.get("pnl_all_time", 0))
        pnl_30d = float(entry.get("pnl_30d", 0))
        pnl_7d = float(entry.get("pnl_7d", 0))
        pnl_1d = float(entry.get("pnl_1d", 0))
        vol_all = float(entry.get("volume_all_time", 0))
        vol_30d = float(entry.get("volume_30d", 0))

        # Get previous snapshot
        prev = await get_last_snapshot(addr)

        # Save new snapshot
        await db.execute(
            """INSERT INTO leaderboard_snapshots
               (address, pnl_all_time, pnl_30d, pnl_7d, pnl_1d, equity, oi,
                volume_all_time, volume_30d)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (addr, pnl_all, pnl_30d, pnl_7d, pnl_1d, equity, oi, vol_all, vol_30d),
        )

        if not prev:
            continue

        # Detect significant OI change
        prev_oi = prev.get("oi", 0) or 0
        oi_change = oi - prev_oi

        if abs(oi_change) < MIN_OI_CHANGE_USD:
            continue

        # Build alert message
        direction = "opened" if oi_change > 0 else "closed"
        username = entry.get("username") or f"{addr[:6]}...{addr[-4:]}"
        tier = _pnl_tier(pnl_all)

        alert_text = (
            f"🐋 <b>Whale Alert</b>\n\n"
            f"<b>{username}</b> ({tier})\n"
            f"All-time PnL: <b>{_fmt_usd(pnl_all)}</b>\n"
            f"Equity: <b>{_fmt_usd(equity)}</b>\n\n"
            f"{'📈' if oi_change > 0 else '📉'} {direction} <b>{_fmt_usd(abs(oi_change))}</b> in positions\n"
            f"Current OI: {_fmt_usd(oi)}\n\n"
            f"<i>Track this wallet with /track {addr[:8]}...</i>"
        )

        # Send to whale alert subscribers
        if equity >= MIN_EQUITY_FOR_WHALE and whale_subs:
            for tg_id in whale_subs:
                try:
                    await bot.send_message(tg_id, alert_text)
                except Exception as e:
                    logger.debug("Failed to send whale alert to %s: %s", tg_id, e)

        # Send to users tracking this specific wallet
        if addr in tracked:
            tracked_alert = (
                f"🔔 <b>Tracked Wallet Activity</b>\n\n"
                f"<b>{username}</b>\n"
                f"All-time PnL: <b>{_fmt_usd(pnl_all)}</b>\n\n"
                f"{'📈' if oi_change > 0 else '📉'} {direction} <b>{_fmt_usd(abs(oi_change))}</b> in positions\n"
                f"Current OI: {_fmt_usd(oi)} | Equity: {_fmt_usd(equity)}\n"
            )
            for tg_id in tracked[addr]:
                try:
                    await bot.send_message(tg_id, tracked_alert)
                except Exception as e:
                    logger.debug("Failed to send tracked alert to %s: %s", tg_id, e)

    await db.commit()


def _pnl_tier(pnl: float) -> str:
    if pnl >= 100_000:
        return "🏆 Top Trader"
    if pnl >= 10_000:
        return "💰 Profitable"
    if pnl >= 0:
        return "📊 Active"
    return "📉 Underwater"


def _fmt_usd(val: float) -> str:
    abs_val = abs(val)
    if abs_val >= 1_000_000:
        return f"{'−' if val < 0 else ''}${abs_val / 1_000_000:.2f}M"
    if abs_val >= 1_000:
        return f"{'−' if val < 0 else ''}${abs_val / 1_000:.1f}K"
    return f"{'−' if val < 0 else ''}${abs_val:.0f}"


# ------------------------------------------------------------------
# Start / Stop
# ------------------------------------------------------------------

async def start_whale_monitor(bot: Bot):
    global _running, _task
    await _init_whale_tables()
    _running = True
    logger.info("Whale monitor started (interval=%ds)", SNAPSHOT_INTERVAL)

    while _running:
        try:
            await _snapshot_and_alert(bot)
        except Exception as e:
            logger.error("Whale monitor error: %s", e, exc_info=True)

        await asyncio.sleep(SNAPSHOT_INTERVAL)


def stop_whale_monitor():
    global _running
    _running = False
    logger.info("Whale monitor stopped.")
