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
            settings TEXT DEFAULT '{}',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS copy_configs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            telegram_id INTEGER REFERENCES users(telegram_id),
            master_wallet TEXT NOT NULL,
            active INTEGER DEFAULT 1,
            size_multiplier REAL DEFAULT 1.0,
            max_position_usd REAL DEFAULT 1000,
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
        """
    )
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
    await db.execute("DELETE FROM trade_log WHERE telegram_id = ?", (telegram_id,))
    await db.execute("DELETE FROM copy_configs WHERE telegram_id = ?", (telegram_id,))
    await db.execute("DELETE FROM users WHERE telegram_id = ?", (telegram_id,))
    await db.commit()


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
    size_multiplier: float = 1.0,
    max_position_usd: float = 1000,
    symbols: str = "*",
) -> int:
    db = await get_db()
    cursor = await db.execute(
        """INSERT INTO copy_configs
           (telegram_id, master_wallet, size_multiplier, max_position_usd, symbols)
           VALUES (?, ?, ?, ?, ?)""",
        (telegram_id, master_wallet, size_multiplier, max_position_usd, symbols),
    )
    await db.commit()
    return cursor.lastrowid  # type: ignore


async def get_active_copy_configs(telegram_id: int | None = None) -> list[dict]:
    db = await get_db()
    if telegram_id:
        q = "SELECT * FROM copy_configs WHERE telegram_id = ? AND active = 1"
        params = (telegram_id,)
    else:
        q = "SELECT * FROM copy_configs WHERE active = 1"
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
    db = await get_db()
    async with db.execute(
        "SELECT * FROM users WHERE ref_code = ?", (code,)
    ) as cursor:
        row = await cursor.fetchone()
        return dict(row) if row else None


async def count_referrals(telegram_id: int) -> int:
    db = await get_db()
    async with db.execute(
        "SELECT COUNT(*) FROM users WHERE referred_by = ?", (telegram_id,)
    ) as cursor:
        row = await cursor.fetchone()
        return row[0] if row else 0
