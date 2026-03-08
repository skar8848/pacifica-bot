"""
Async SQLite database layer.
"""

import os
import secrets
import string
import aiosqlite
from bot.config import DATABASE_PATH

_db: aiosqlite.Connection | None = None


async def get_db() -> aiosqlite.Connection:
    global _db
    if _db is None:
        os.makedirs(os.path.dirname(DATABASE_PATH) or ".", exist_ok=True)
        _db = await aiosqlite.connect(DATABASE_PATH)
        _db.row_factory = aiosqlite.Row
        await _db.execute("PRAGMA journal_mode=WAL")
        await _init_tables(_db)
    return _db


async def close_db():
    global _db
    if _db:
        await _db.close()
        _db = None


async def _init_tables(db: aiosqlite.Connection):
    await db.executescript(
        """
        CREATE TABLE IF NOT EXISTS users (
            telegram_id INTEGER PRIMARY KEY,
            pacifica_account TEXT,
            agent_wallet_public TEXT,
            agent_wallet_encrypted TEXT,
            builder_approved INTEGER DEFAULT 0,
            ref_code TEXT UNIQUE,
            referred_by INTEGER,
            username TEXT,
            settings TEXT DEFAULT '{}',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS copy_configs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            telegram_id INTEGER REFERENCES users(telegram_id),
            master_wallet TEXT NOT NULL,
            active INTEGER DEFAULT 1,
            sizing_mode TEXT DEFAULT 'fixed_usd',
            size_multiplier REAL DEFAULT 1.0,
            fixed_amount_usd REAL DEFAULT 10.0,
            pct_equity REAL DEFAULT 5.0,
            min_trade_usd REAL DEFAULT 0,
            max_position_usd REAL DEFAULT 1000,
            max_total_usd REAL DEFAULT 5000,
            symbols TEXT DEFAULT '*',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS trade_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            telegram_id INTEGER REFERENCES users(telegram_id),
            symbol TEXT,
            side TEXT,
            amount TEXT,
            price TEXT,
            order_type TEXT,
            is_copy_trade INTEGER DEFAULT 0,
            master_wallet TEXT,
            client_order_id TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS price_alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            telegram_id INTEGER REFERENCES users(telegram_id),
            symbol TEXT NOT NULL,
            direction TEXT NOT NULL,  -- 'above' or 'below'
            target_price REAL NOT NULL,
            active INTEGER DEFAULT 1,
            triggered INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS referral_fees (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            referrer_id INTEGER NOT NULL,
            referee_id INTEGER NOT NULL,
            symbol TEXT,
            trade_volume REAL DEFAULT 0,
            fee_earned REAL DEFAULT 0,
            claimed INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS beta_codes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT NOT NULL UNIQUE,
            added_by INTEGER,
            active INTEGER DEFAULT 1,
            uses INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS trailing_stops (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            telegram_id INTEGER REFERENCES users(telegram_id),
            symbol TEXT NOT NULL,
            side TEXT NOT NULL,
            trail_percent REAL NOT NULL,
            entry_price REAL NOT NULL,
            peak_price REAL NOT NULL,
            callback_price REAL NOT NULL,
            active INTEGER DEFAULT 1,
            triggered_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS dca_configs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            telegram_id INTEGER REFERENCES users(telegram_id),
            symbol TEXT NOT NULL,
            side TEXT NOT NULL,
            mode TEXT NOT NULL,                   -- 'time' or 'price'
            total_amount_usd REAL NOT NULL,
            amount_per_order REAL NOT NULL,
            leverage INTEGER DEFAULT 1,
            interval_seconds INTEGER,             -- for time-based
            price_levels TEXT,                    -- JSON array for price-based
            orders_executed INTEGER DEFAULT 0,
            orders_total INTEGER NOT NULL,
            avg_entry REAL DEFAULT 0,
            active INTEGER DEFAULT 1,
            next_execution TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS scaled_orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            telegram_id INTEGER REFERENCES users(telegram_id),
            symbol TEXT NOT NULL,
            side TEXT NOT NULL,
            total_amount_usd REAL NOT NULL,
            price_low REAL NOT NULL,
            price_high REAL NOT NULL,
            num_levels INTEGER NOT NULL,
            distribution TEXT DEFAULT 'even',
            leverage INTEGER DEFAULT 1,
            orders_placed INTEGER DEFAULT 0,
            active INTEGER DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS leader_profiles (
            telegram_id INTEGER PRIMARY KEY REFERENCES users(telegram_id),
            display_name TEXT NOT NULL,
            bio TEXT DEFAULT '',
            profit_share_pct REAL DEFAULT 10.0,
            is_public INTEGER DEFAULT 1,
            total_followers INTEGER DEFAULT 0,
            total_pnl_shared REAL DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS follower_pnl (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            follower_id INTEGER NOT NULL REFERENCES users(telegram_id),
            leader_id INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            side TEXT NOT NULL,
            entry_price REAL NOT NULL,
            exit_price REAL,
            amount REAL NOT NULL,
            realized_pnl REAL DEFAULT 0,
            profit_shared REAL DEFAULT 0,
            status TEXT DEFAULT 'open',
            opened_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            closed_at TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_follower_pnl_leader
            ON follower_pnl(leader_id);
        CREATE INDEX IF NOT EXISTS idx_follower_pnl_follower
            ON follower_pnl(follower_id, status);

        CREATE TABLE IF NOT EXISTS twap_orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            telegram_id INTEGER REFERENCES users(telegram_id),
            symbol TEXT NOT NULL,
            side TEXT NOT NULL,
            total_amount_usd REAL NOT NULL,
            num_slices INTEGER NOT NULL,
            interval_seconds INTEGER NOT NULL,
            leverage INTEGER DEFAULT 1,
            slices_executed INTEGER DEFAULT 0,
            amount_per_slice REAL NOT NULL,
            avg_price REAL DEFAULT 0,
            randomize INTEGER DEFAULT 0,
            active INTEGER DEFAULT 1,
            next_execution TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS onchain_watches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            telegram_id INTEGER NOT NULL,
            wallet_address TEXT NOT NULL,
            label TEXT,
            min_tx_usd REAL DEFAULT 10000,
            active INTEGER DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(telegram_id, wallet_address)
        );

        CREATE TABLE IF NOT EXISTS bot_settings (
            key TEXT PRIMARY KEY,
            value TEXT
        );
        """
    )
    # Migrations for existing DBs
    migrations = [
        "ALTER TABLE users ADD COLUMN username TEXT",
        "ALTER TABLE copy_configs ADD COLUMN sizing_mode TEXT DEFAULT 'fixed_usd'",
        "ALTER TABLE copy_configs ADD COLUMN fixed_amount_usd REAL DEFAULT 10.0",
        "ALTER TABLE copy_configs ADD COLUMN pct_equity REAL DEFAULT 5.0",
        "ALTER TABLE copy_configs ADD COLUMN min_trade_usd REAL DEFAULT 0",
        "ALTER TABLE copy_configs ADD COLUMN max_total_usd REAL DEFAULT 5000",
        "ALTER TABLE copy_configs ADD COLUMN source TEXT DEFAULT 'pacifica'",
    ]
    for sql in migrations:
        try:
            await db.execute(sql)
            await db.commit()
        except Exception:
            pass  # column already exists

    await db.commit()


# ------------------------------------------------------------------
# User CRUD
# ------------------------------------------------------------------

async def get_user(telegram_id: int) -> dict | None:
    db = await get_db()
    async with db.execute(
        "SELECT * FROM users WHERE telegram_id = ?", (telegram_id,)
    ) as cursor:
        row = await cursor.fetchone()
        return dict(row) if row else None


async def create_user(
    telegram_id: int,
    agent_wallet_public: str | None,
    agent_wallet_encrypted: str,
) -> dict:
    db = await get_db()
    await db.execute(
        """INSERT INTO users (telegram_id, agent_wallet_public, agent_wallet_encrypted)
           VALUES (?, ?, ?)""",
        (telegram_id, agent_wallet_public, agent_wallet_encrypted),
    )
    await db.commit()
    return (await get_user(telegram_id))  # type: ignore


async def delete_user(telegram_id: int):
    db = await get_db()
    await db.execute("DELETE FROM price_alerts WHERE telegram_id = ?", (telegram_id,))
    await db.execute("DELETE FROM trade_log WHERE telegram_id = ?", (telegram_id,))
    await db.execute("DELETE FROM copy_configs WHERE telegram_id = ?", (telegram_id,))
    await db.execute("DELETE FROM users WHERE telegram_id = ?", (telegram_id,))
    await db.commit()


async def get_user_by_wallet(wallet: str, exclude_tg_id: int | None = None) -> dict | None:
    """Check if a wallet is already registered to another user."""
    db = await get_db()
    if exclude_tg_id:
        async with db.execute(
            "SELECT * FROM users WHERE pacifica_account = ? AND telegram_id != ?",
            (wallet, exclude_tg_id),
        ) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None
    async with db.execute(
        "SELECT * FROM users WHERE pacifica_account = ?", (wallet,)
    ) as cursor:
        row = await cursor.fetchone()
        return dict(row) if row else None


async def update_user(telegram_id: int, **fields):
    db = await get_db()
    sets = ", ".join(f"{k} = ?" for k in fields)
    vals = list(fields.values()) + [telegram_id]
    await db.execute(f"UPDATE users SET {sets} WHERE telegram_id = ?", vals)
    await db.commit()


# ------------------------------------------------------------------
# Copy configs
# ------------------------------------------------------------------

async def add_copy_config(
    telegram_id: int,
    master_wallet: str,
    sizing_mode: str = "fixed_usd",
    size_multiplier: float = 1.0,
    fixed_amount_usd: float = 10.0,
    pct_equity: float = 5.0,
    min_trade_usd: float = 0,
    max_position_usd: float = 1000,
    max_total_usd: float = 5000,
    symbols: str = "*",
) -> int:
    db = await get_db()
    cursor = await db.execute(
        """INSERT INTO copy_configs
           (telegram_id, master_wallet, sizing_mode, size_multiplier,
            fixed_amount_usd, pct_equity, min_trade_usd, max_position_usd,
            max_total_usd, symbols)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (telegram_id, master_wallet, sizing_mode, size_multiplier,
         fixed_amount_usd, pct_equity, min_trade_usd, max_position_usd,
         max_total_usd, symbols),
    )
    await db.commit()
    return cursor.lastrowid  # type: ignore


async def get_active_copy_configs(telegram_id: int | None = None) -> list[dict]:
    db = await get_db()
    if telegram_id:
        q = "SELECT * FROM copy_configs WHERE telegram_id = ? AND active = 1 AND COALESCE(source, 'pacifica') = 'pacifica'"
        params = (telegram_id,)
    else:
        q = "SELECT * FROM copy_configs WHERE active = 1 AND COALESCE(source, 'pacifica') = 'pacifica'"
        params = ()
    async with db.execute(q, params) as cursor:
        return [dict(r) for r in await cursor.fetchall()]


async def deactivate_copy_config(telegram_id: int, master_wallet: str):
    db = await get_db()
    await db.execute(
        "UPDATE copy_configs SET active = 0 WHERE telegram_id = ? AND master_wallet = ?",
        (telegram_id, master_wallet),
    )
    await db.commit()


# ------------------------------------------------------------------
# Trade log
# ------------------------------------------------------------------

async def log_trade(
    telegram_id: int,
    symbol: str,
    side: str,
    amount: str,
    price: str = "",
    order_type: str = "market",
    is_copy_trade: bool = False,
    master_wallet: str = "",
    client_order_id: str = "",
):
    db = await get_db()
    await db.execute(
        """INSERT INTO trade_log
           (telegram_id, symbol, side, amount, price, order_type,
            is_copy_trade, master_wallet, client_order_id)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            telegram_id, symbol, side, amount, price, order_type,
            int(is_copy_trade), master_wallet, client_order_id,
        ),
    )
    await db.commit()


async def get_trade_history(telegram_id: int, limit: int = 20) -> list[dict]:
    db = await get_db()
    async with db.execute(
        "SELECT * FROM trade_log WHERE telegram_id = ? ORDER BY created_at DESC LIMIT ?",
        (telegram_id, limit),
    ) as cursor:
        return [dict(r) for r in await cursor.fetchall()]


# ------------------------------------------------------------------
# User settings helpers
# ------------------------------------------------------------------

import json as _json


async def get_user_settings(telegram_id: int) -> dict:
    """Get parsed user settings dict."""
    user = await get_user(telegram_id)
    if not user:
        return {}
    try:
        return _json.loads(user.get("settings") or "{}")
    except Exception:
        return {}


async def set_user_setting(telegram_id: int, key: str, value):
    """Update a single setting key."""
    settings = await get_user_settings(telegram_id)
    settings[key] = value
    await update_user(telegram_id, settings=_json.dumps(settings))


# ------------------------------------------------------------------
# Referrals
# ------------------------------------------------------------------

def _generate_ref_code() -> str:
    chars = string.ascii_uppercase + string.digits
    return "".join(secrets.choice(chars) for _ in range(6))


async def get_or_create_ref_code(telegram_id: int) -> str:
    user = await get_user(telegram_id)
    if user and user.get("ref_code"):
        return user["ref_code"]
    code = _generate_ref_code()
    await update_user(telegram_id, ref_code=code)
    return code


async def get_user_by_ref_code(code: str) -> dict | None:
    """Find user by ref_code OR username (both work as referral identifiers)."""
    db = await get_db()
    async with db.execute(
        "SELECT * FROM users WHERE ref_code = ? OR username = ?", (code, code)
    ) as cursor:
        row = await cursor.fetchone()
        return dict(row) if row else None


async def is_username_taken(username: str, exclude_tg_id: int | None = None) -> bool:
    """Check if a username is already taken by another user (case-insensitive)."""
    db = await get_db()
    lower = username.lower()
    if exclude_tg_id:
        async with db.execute(
            "SELECT 1 FROM users WHERE LOWER(username) = ? AND telegram_id != ?",
            (lower, exclude_tg_id),
        ) as cursor:
            return (await cursor.fetchone()) is not None
    async with db.execute(
        "SELECT 1 FROM users WHERE LOWER(username) = ?", (lower,)
    ) as cursor:
        return (await cursor.fetchone()) is not None


# ------------------------------------------------------------------
# Price alerts
# ------------------------------------------------------------------

async def add_price_alert(
    telegram_id: int, symbol: str, direction: str, target_price: float,
) -> int:
    db = await get_db()
    cursor = await db.execute(
        """INSERT INTO price_alerts (telegram_id, symbol, direction, target_price)
           VALUES (?, ?, ?, ?)""",
        (telegram_id, symbol, direction, target_price),
    )
    await db.commit()
    return cursor.lastrowid  # type: ignore


async def get_active_alerts(telegram_id: int | None = None) -> list[dict]:
    db = await get_db()
    if telegram_id:
        q = "SELECT * FROM price_alerts WHERE telegram_id = ? AND active = 1 AND triggered = 0"
        params = (telegram_id,)
    else:
        q = "SELECT * FROM price_alerts WHERE active = 1 AND triggered = 0"
        params = ()
    async with db.execute(q, params) as cursor:
        return [dict(r) for r in await cursor.fetchall()]


async def trigger_alert(alert_id: int):
    db = await get_db()
    await db.execute(
        "UPDATE price_alerts SET triggered = 1, active = 0 WHERE id = ?", (alert_id,)
    )
    await db.commit()


async def delete_alert(alert_id: int, telegram_id: int):
    db = await get_db()
    await db.execute(
        "DELETE FROM price_alerts WHERE id = ? AND telegram_id = ?",
        (alert_id, telegram_id),
    )
    await db.commit()


async def count_referrals(telegram_id: int) -> int:
    db = await get_db()
    async with db.execute(
        "SELECT COUNT(*) FROM users WHERE referred_by = ?", (telegram_id,)
    ) as cursor:
        row = await cursor.fetchone()
        return row[0] if row else 0


async def get_referrals(telegram_id: int) -> list[dict]:
    """Get all users referred by this user."""
    db = await get_db()
    async with db.execute(
        "SELECT telegram_id, pacifica_account, created_at FROM users WHERE referred_by = ?",
        (telegram_id,),
    ) as cursor:
        return [dict(r) for r in await cursor.fetchall()]


# ------------------------------------------------------------------
# Referral fee tracking
# ------------------------------------------------------------------

REFERRAL_FEE_SHARE = 0.10  # Referrer gets 10% of referee's trading fees
REFEREE_FEE_REBATE = 0.05  # Referee gets 5% fee rebate


async def log_referral_fee(
    referrer_id: int, referee_id: int, symbol: str,
    trade_volume: float, fee_earned: float,
):
    """Log a referral fee earned from a referee's trade."""
    db = await get_db()
    await db.execute(
        """INSERT INTO referral_fees (referrer_id, referee_id, symbol, trade_volume, fee_earned)
           VALUES (?, ?, ?, ?, ?)""",
        (referrer_id, referee_id, symbol, trade_volume, fee_earned),
    )
    await db.commit()


async def get_unclaimed_fees(telegram_id: int) -> float:
    """Get total unclaimed referral fees for a user."""
    db = await get_db()
    async with db.execute(
        "SELECT COALESCE(SUM(fee_earned), 0) FROM referral_fees WHERE referrer_id = ? AND claimed = 0",
        (telegram_id,),
    ) as cursor:
        row = await cursor.fetchone()
        return float(row[0]) if row else 0.0


async def get_total_fees_earned(telegram_id: int) -> float:
    """Get total referral fees ever earned."""
    db = await get_db()
    async with db.execute(
        "SELECT COALESCE(SUM(fee_earned), 0) FROM referral_fees WHERE referrer_id = ?",
        (telegram_id,),
    ) as cursor:
        row = await cursor.fetchone()
        return float(row[0]) if row else 0.0


async def claim_referral_fees(telegram_id: int) -> float:
    """Mark all unclaimed fees as claimed. Returns the amount claimed."""
    unclaimed = await get_unclaimed_fees(telegram_id)
    if unclaimed > 0:
        db = await get_db()
        await db.execute(
            "UPDATE referral_fees SET claimed = 1 WHERE referrer_id = ? AND claimed = 0",
            (telegram_id,),
        )
        await db.commit()
    return unclaimed


async def get_referral_stats(telegram_id: int) -> dict:
    """Get full referral stats for a user."""
    ref_count = await count_referrals(telegram_id)
    total_earned = await get_total_fees_earned(telegram_id)
    unclaimed = await get_unclaimed_fees(telegram_id)

    # Total volume generated by referrals
    db = await get_db()
    async with db.execute(
        "SELECT COALESCE(SUM(trade_volume), 0) FROM referral_fees WHERE referrer_id = ?",
        (telegram_id,),
    ) as cursor:
        row = await cursor.fetchone()
        total_volume = float(row[0]) if row else 0.0

    return {
        "referral_count": ref_count,
        "total_earned": total_earned,
        "unclaimed": unclaimed,
        "total_volume": total_volume,
    }


# ------------------------------------------------------------------
# Beta codes (runtime-managed)
# ------------------------------------------------------------------

async def add_beta_code(code: str, added_by: int | None = None) -> bool:
    """Add a beta code. Returns True if added, False if duplicate."""
    db = await get_db()
    try:
        await db.execute(
            "INSERT INTO beta_codes (code, added_by) VALUES (?, ?)",
            (code.strip(), added_by),
        )
        await db.commit()
        return True
    except Exception:
        return False


async def get_active_beta_codes() -> list[str]:
    """Get all active beta codes, newest first."""
    db = await get_db()
    async with db.execute(
        "SELECT code FROM beta_codes WHERE active = 1 ORDER BY created_at DESC"
    ) as cursor:
        return [row[0] async for row in cursor]


async def deactivate_beta_code(code: str):
    db = await get_db()
    await db.execute(
        "UPDATE beta_codes SET active = 0 WHERE code = ?", (code,)
    )
    await db.commit()


async def increment_beta_code_uses(code: str):
    db = await get_db()
    await db.execute(
        "UPDATE beta_codes SET uses = uses + 1 WHERE code = ?", (code,)
    )
    await db.commit()


async def get_all_beta_codes() -> list[dict]:
    """Get all beta codes with stats."""
    db = await get_db()
    async with db.execute(
        "SELECT * FROM beta_codes ORDER BY created_at DESC"
    ) as cursor:
        return [dict(r) for r in await cursor.fetchall()]


# ------------------------------------------------------------------
# Trailing stops
# ------------------------------------------------------------------

async def add_trailing_stop(
    telegram_id: int, symbol: str, side: str, trail_percent: float,
    entry_price: float, peak_price: float, callback_price: float,
) -> int:
    db = await get_db()
    cursor = await db.execute(
        """INSERT INTO trailing_stops
           (telegram_id, symbol, side, trail_percent, entry_price, peak_price, callback_price)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (telegram_id, symbol, side, trail_percent, entry_price, peak_price, callback_price),
    )
    await db.commit()
    return cursor.lastrowid  # type: ignore


async def get_active_trailing_stops(telegram_id: int | None = None) -> list[dict]:
    db = await get_db()
    if telegram_id:
        q = "SELECT * FROM trailing_stops WHERE telegram_id = ? AND active = 1"
        params = (telegram_id,)
    else:
        q = "SELECT * FROM trailing_stops WHERE active = 1"
        params = ()
    async with db.execute(q, params) as cursor:
        return [dict(r) for r in await cursor.fetchall()]


async def cancel_trailing_stop(stop_id: int, telegram_id: int):
    db = await get_db()
    await db.execute(
        "UPDATE trailing_stops SET active = 0 WHERE id = ? AND telegram_id = ?",
        (stop_id, telegram_id),
    )
    await db.commit()


# ------------------------------------------------------------------
# DCA configs
# ------------------------------------------------------------------

async def add_dca_config(
    telegram_id: int, symbol: str, side: str, mode: str,
    total_amount_usd: float, amount_per_order: float, orders_total: int,
    leverage: int = 1, interval_seconds: int | None = None,
    price_levels: str | None = None,
) -> int:
    db = await get_db()
    cursor = await db.execute(
        """INSERT INTO dca_configs
           (telegram_id, symbol, side, mode, total_amount_usd, amount_per_order,
            orders_total, leverage, interval_seconds, price_levels, next_execution)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))""",
        (telegram_id, symbol, side, mode, total_amount_usd, amount_per_order,
         orders_total, leverage, interval_seconds, price_levels),
    )
    await db.commit()
    return cursor.lastrowid  # type: ignore


async def get_active_dca_configs(telegram_id: int | None = None) -> list[dict]:
    db = await get_db()
    if telegram_id:
        q = "SELECT * FROM dca_configs WHERE telegram_id = ? AND active = 1"
        params = (telegram_id,)
    else:
        q = "SELECT * FROM dca_configs WHERE active = 1"
        params = ()
    async with db.execute(q, params) as cursor:
        return [dict(r) for r in await cursor.fetchall()]


async def update_dca_progress(dca_id: int, orders_executed: int, avg_entry: float, next_execution: str | None = None):
    db = await get_db()
    if next_execution:
        await db.execute(
            "UPDATE dca_configs SET orders_executed = ?, avg_entry = ?, next_execution = ? WHERE id = ?",
            (orders_executed, avg_entry, next_execution, dca_id),
        )
    else:
        await db.execute(
            "UPDATE dca_configs SET orders_executed = ?, avg_entry = ?, active = 0 WHERE id = ?",
            (orders_executed, avg_entry, dca_id),
        )
    await db.commit()


async def cancel_dca(dca_id: int, telegram_id: int):
    db = await get_db()
    await db.execute(
        "UPDATE dca_configs SET active = 0 WHERE id = ? AND telegram_id = ?",
        (dca_id, telegram_id),
    )
    await db.commit()


# ------------------------------------------------------------------
# Scaled orders
# ------------------------------------------------------------------

async def add_scaled_order(
    telegram_id: int, symbol: str, side: str, total_amount_usd: float,
    price_low: float, price_high: float, num_levels: int,
    distribution: str = "even", leverage: int = 1,
) -> int:
    db = await get_db()
    cursor = await db.execute(
        """INSERT INTO scaled_orders
           (telegram_id, symbol, side, total_amount_usd, price_low, price_high,
            num_levels, distribution, leverage)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (telegram_id, symbol, side, total_amount_usd, price_low, price_high,
         num_levels, distribution, leverage),
    )
    await db.commit()
    return cursor.lastrowid  # type: ignore


# ------------------------------------------------------------------
# Leader profiles (Copy Trading v2)
# ------------------------------------------------------------------

async def create_leader_profile(
    telegram_id: int, display_name: str, bio: str = "",
    profit_share_pct: float = 10.0,
) -> dict:
    db = await get_db()
    await db.execute(
        """INSERT INTO leader_profiles (telegram_id, display_name, bio, profit_share_pct)
           VALUES (?, ?, ?, ?)
           ON CONFLICT(telegram_id) DO UPDATE SET
             display_name = ?, bio = ?, profit_share_pct = ?, is_public = 1""",
        (telegram_id, display_name, bio, profit_share_pct,
         display_name, bio, profit_share_pct),
    )
    await db.commit()
    return (await get_leader_profile(telegram_id))  # type: ignore


async def get_leader_profile(telegram_id: int) -> dict | None:
    db = await get_db()
    async with db.execute(
        "SELECT * FROM leader_profiles WHERE telegram_id = ?", (telegram_id,)
    ) as cursor:
        row = await cursor.fetchone()
        return dict(row) if row else None


async def get_public_leaders() -> list[dict]:
    db = await get_db()
    async with db.execute(
        """SELECT lp.*, u.pacifica_account, u.username
           FROM leader_profiles lp
           JOIN users u ON lp.telegram_id = u.telegram_id
           WHERE lp.is_public = 1
           ORDER BY lp.total_followers DESC"""
    ) as cursor:
        return [dict(r) for r in await cursor.fetchall()]


async def update_leader_followers(leader_id: int, delta: int):
    db = await get_db()
    await db.execute(
        "UPDATE leader_profiles SET total_followers = MAX(0, total_followers + ?) WHERE telegram_id = ?",
        (delta, leader_id),
    )
    await db.commit()


async def deactivate_leader(telegram_id: int):
    db = await get_db()
    await db.execute(
        "UPDATE leader_profiles SET is_public = 0 WHERE telegram_id = ?",
        (telegram_id,),
    )
    await db.commit()


# ------------------------------------------------------------------
# Follower PnL tracking
# ------------------------------------------------------------------

async def open_follower_position(
    follower_id: int, leader_id: int, symbol: str, side: str,
    entry_price: float, amount: float,
) -> int:
    db = await get_db()
    cursor = await db.execute(
        """INSERT INTO follower_pnl
           (follower_id, leader_id, symbol, side, entry_price, amount)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (follower_id, leader_id, symbol, side, entry_price, amount),
    )
    await db.commit()
    return cursor.lastrowid  # type: ignore


async def close_follower_position(
    follower_id: int, leader_id: int, symbol: str, exit_price: float,
) -> dict | None:
    """Close a follower's tracked position and calculate PnL + profit share."""
    db = await get_db()
    async with db.execute(
        """SELECT * FROM follower_pnl
           WHERE follower_id = ? AND leader_id = ? AND symbol = ? AND status = 'open'
           ORDER BY opened_at DESC LIMIT 1""",
        (follower_id, leader_id, symbol),
    ) as cursor:
        row = await cursor.fetchone()
        if not row:
            return None
        pos = dict(row)

    amount = pos["amount"]
    entry = pos["entry_price"]
    if pos["side"].lower() in ("long", "bid", "buy"):
        pnl = (exit_price - entry) * amount
    else:
        pnl = (entry - exit_price) * amount

    # Calculate profit share (only on profits)
    profit_share = 0.0
    if pnl > 0:
        leader = await get_leader_profile(leader_id)
        share_pct = leader.get("profit_share_pct", 10.0) if leader else 10.0
        profit_share = pnl * (share_pct / 100.0)

    await db.execute(
        """UPDATE follower_pnl
           SET exit_price = ?, realized_pnl = ?, profit_shared = ?,
               status = 'closed', closed_at = CURRENT_TIMESTAMP
           WHERE id = ?""",
        (exit_price, pnl, profit_share, pos["id"]),
    )

    # Update leader's total shared
    if profit_share > 0:
        await db.execute(
            "UPDATE leader_profiles SET total_pnl_shared = total_pnl_shared + ? WHERE telegram_id = ?",
            (profit_share, leader_id),
        )

    await db.commit()
    return {"pnl": pnl, "profit_share": profit_share, "entry": entry, "exit": exit_price, "amount": amount}


async def get_leader_performance(leader_id: int) -> dict:
    """Get aggregated performance stats for a leader."""
    db = await get_db()
    async with db.execute(
        """SELECT
             COUNT(*) as total_trades,
             SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END) as wins,
             SUM(realized_pnl) as total_pnl,
             SUM(profit_shared) as total_shared
           FROM follower_pnl WHERE leader_id = ? AND status = 'closed'""",
        (leader_id,),
    ) as cursor:
        row = await cursor.fetchone()
        if not row or not row[0]:
            return {"total_trades": 0, "wins": 0, "total_pnl": 0, "total_shared": 0, "win_rate": 0}
        total = row[0]
        wins = row[1] or 0
        return {
            "total_trades": total,
            "wins": wins,
            "total_pnl": row[2] or 0,
            "total_shared": row[3] or 0,
            "win_rate": (wins / total * 100) if total > 0 else 0,
        }


# ------------------------------------------------------------------
# TWAP orders
# ------------------------------------------------------------------

async def add_twap_order(
    telegram_id: int, symbol: str, side: str, total_amount_usd: float,
    num_slices: int, interval_seconds: int, amount_per_slice: float,
    leverage: int = 1, randomize: bool = False,
) -> int:
    db = await get_db()
    cursor = await db.execute(
        """INSERT INTO twap_orders
           (telegram_id, symbol, side, total_amount_usd, num_slices,
            interval_seconds, amount_per_slice, leverage, randomize,
            next_execution)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))""",
        (telegram_id, symbol, side, total_amount_usd, num_slices,
         interval_seconds, amount_per_slice, leverage, int(randomize)),
    )
    await db.commit()
    return cursor.lastrowid  # type: ignore


async def get_active_twap_orders(telegram_id: int | None = None) -> list[dict]:
    db = await get_db()
    if telegram_id:
        q = "SELECT * FROM twap_orders WHERE telegram_id = ? AND active = 1"
        params = (telegram_id,)
    else:
        q = "SELECT * FROM twap_orders WHERE active = 1"
        params = ()
    async with db.execute(q, params) as cursor:
        return [dict(r) for r in await cursor.fetchall()]


async def update_twap_progress(
    twap_id: int, slices_executed: int, avg_price: float,
    next_execution: str | None = None,
):
    db = await get_db()
    if next_execution:
        await db.execute(
            """UPDATE twap_orders
               SET slices_executed = ?, avg_price = ?, next_execution = ?
               WHERE id = ?""",
            (slices_executed, avg_price, next_execution, twap_id),
        )
    else:
        await db.execute(
            """UPDATE twap_orders
               SET slices_executed = ?, avg_price = ?, active = 0
               WHERE id = ?""",
            (slices_executed, avg_price, twap_id),
        )
    await db.commit()


async def cancel_twap(twap_id: int, telegram_id: int):
    db = await get_db()
    await db.execute(
        "UPDATE twap_orders SET active = 0 WHERE id = ? AND telegram_id = ?",
        (twap_id, telegram_id),
    )
    await db.commit()


# ------------------------------------------------------------------
# On-chain watches
# ------------------------------------------------------------------

async def add_onchain_watch(
    telegram_id: int, wallet_address: str, label: str | None = None,
    min_tx_usd: float = 10000,
) -> bool:
    db = await get_db()
    try:
        await db.execute(
            """INSERT INTO onchain_watches (telegram_id, wallet_address, label, min_tx_usd)
               VALUES (?, ?, ?, ?)""",
            (telegram_id, wallet_address, label, min_tx_usd),
        )
        await db.commit()
        return True
    except Exception:
        return False


async def remove_onchain_watch(telegram_id: int, wallet_address: str) -> bool:
    db = await get_db()
    cursor = await db.execute(
        "DELETE FROM onchain_watches WHERE telegram_id = ? AND wallet_address = ?",
        (telegram_id, wallet_address),
    )
    await db.commit()
    return cursor.rowcount > 0


async def get_onchain_watches(telegram_id: int | None = None) -> list[dict]:
    db = await get_db()
    if telegram_id:
        q = "SELECT * FROM onchain_watches WHERE telegram_id = ? AND active = 1"
        params = (telegram_id,)
    else:
        q = "SELECT * FROM onchain_watches WHERE active = 1"
        params = ()
    async with db.execute(q, params) as cursor:
        return [dict(r) for r in await cursor.fetchall()]


async def get_all_onchain_addresses() -> dict[str, list[tuple[int, float]]]:
    """Returns {wallet_address: [(telegram_id, min_tx_usd), ...]}."""
    db = await get_db()
    result: dict[str, list[tuple[int, float]]] = {}
    async with db.execute(
        "SELECT wallet_address, telegram_id, min_tx_usd FROM onchain_watches WHERE active = 1"
    ) as cursor:
        async for row in cursor:
            result.setdefault(row[0], []).append((row[1], row[2]))
    return result
