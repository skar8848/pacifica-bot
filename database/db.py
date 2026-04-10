"""
Async PostgreSQL database layer (asyncpg) with SQLite-compat wrapper.

The wrapper lets existing service code call db.execute("... ? ...", (val,))
and db.commit() without changes — it converts ? → $N automatically and
wraps results in a cursor-like object supporting:
    async with db.execute(...) as cursor:
        rows = await cursor.fetchall()
        row  = await cursor.fetchone()
"""

import re
import secrets
import string
import json as _json
import logging

import asyncpg

from bot.config import DATABASE_URL

logger = logging.getLogger(__name__)

_pool: asyncpg.Pool | None = None


# ======================================================================
# SQLite → PostgreSQL compatibility wrapper
# ======================================================================

def _sqlite_to_pg(sql: str) -> str:
    """Convert a SQLite-style query to PostgreSQL syntax."""
    # Replace ? placeholders with $1, $2, ...
    counter = 0
    def _repl(m):
        nonlocal counter
        counter += 1
        return f"${counter}"
    sql = re.sub(r"\?", _repl, sql)

    # INTEGER PRIMARY KEY AUTOINCREMENT → SERIAL PRIMARY KEY
    sql = re.sub(
        r"INTEGER\s+PRIMARY\s+KEY\s+AUTOINCREMENT",
        "SERIAL PRIMARY KEY",
        sql,
        flags=re.IGNORECASE,
    )

    # REAL → DOUBLE PRECISION (optional, REAL works in PG too)
    # Keep REAL — it's valid in PostgreSQL.

    # datetime('now', '-N days') → NOW() - INTERVAL 'N days'
    sql = re.sub(
        r"datetime\(\s*'now'\s*,\s*'(-?\d+)\s+days?'\s*\)",
        lambda m: f"NOW() - INTERVAL '{abs(int(m.group(1)))} days'",
        sql,
        flags=re.IGNORECASE,
    )

    return sql


class _CursorLike:
    """Wraps asyncpg results to look like an aiosqlite cursor."""

    def __init__(self, records: list[asyncpg.Record], status: str = ""):
        self._records = records
        self._status = status
        self._index = 0

    # For `await cursor.fetchall()`
    async def fetchall(self) -> list[asyncpg.Record]:
        return self._records

    # For `await cursor.fetchone()`
    async def fetchone(self) -> asyncpg.Record | None:
        return self._records[0] if self._records else None

    # For `async for row in cursor:`
    def __aiter__(self):
        self._index = 0
        return self

    async def __anext__(self):
        if self._index >= len(self._records):
            raise StopAsyncIteration
        row = self._records[self._index]
        self._index += 1
        return row

    # For `cursor.lastrowid` (requires RETURNING id in the query)
    @property
    def lastrowid(self) -> int | None:
        if self._records:
            first = self._records[0]
            if "id" in first.keys():
                return first["id"]
        return None

    # For `cursor.rowcount`
    @property
    def rowcount(self) -> int:
        # asyncpg execute() returns "DELETE N" / "UPDATE N" etc.
        try:
            return int(self._status.split()[-1])
        except (ValueError, IndexError):
            return len(self._records)

    # For `cursor.description`  (column names)
    @property
    def description(self) -> list[tuple] | None:
        if self._records:
            return [(k, None, None, None, None, None, None) for k in self._records[0].keys()]
        return None


class _PoolWrapper:
    """Wraps asyncpg.Pool with an aiosqlite-like interface.

    Supports:
        db.execute(sql_with_?, tuple_of_params) → _CursorLike
        db.executescript(multi_statement_sql) → runs each statement
        db.commit() → no-op
        async with db.execute(...) as cursor: → context-manager cursor
        db.row_factory  → ignored attribute (kept for compatibility)
    """

    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool
        self.row_factory = None  # compat stub

    # --- execute() ---

    class _ExecContext:
        """Async context manager wrapping a coroutine that returns _CursorLike."""
        def __init__(self, coro):
            self._coro = coro
            self._cursor: _CursorLike | None = None
        async def __aenter__(self) -> _CursorLike:
            self._cursor = await self._coro
            return self._cursor
        async def __aexit__(self, *exc):
            pass
        # Also support plain `await db.execute(...)`
        def __await__(self):
            return self._coro.__await__()

    def execute(self, sql: str, params: tuple = ()) -> "_PoolWrapper._ExecContext":
        return self._ExecContext(self._execute(sql, params))

    async def _execute(self, sql: str, params: tuple = ()) -> _CursorLike:
        sql = _sqlite_to_pg(sql)
        sql_upper = sql.strip().upper()

        # Detect RETURNING / SELECT to decide fetch vs execute
        is_select = sql_upper.startswith("SELECT") or sql_upper.startswith("WITH")
        has_returning = "RETURNING" in sql_upper

        # Add RETURNING id for INSERT if not already present, for lastrowid compat
        is_insert = sql_upper.startswith("INSERT")
        if is_insert and not has_returning:
            sql = sql.rstrip().rstrip(";") + " RETURNING id"
            has_returning = True

        try:
            if is_select or has_returning:
                records = await self._pool.fetch(sql, *params)
                return _CursorLike(records)
            else:
                status = await self._pool.execute(sql, *params)
                return _CursorLike([], status=status)
        except asyncpg.exceptions.UndefinedColumnError:
            # RETURNING id on a table without 'id' column — retry without
            if has_returning and is_insert:
                sql_no_ret = re.sub(r"\s+RETURNING\s+id\s*$", "", sql, flags=re.IGNORECASE)
                status = await self._pool.execute(sql_no_ret, *params)
                return _CursorLike([], status=status)
            raise

    # --- executescript() ---

    async def executescript(self, script: str):
        """Execute multiple SQL statements separated by semicolons."""
        stmts = [s.strip() for s in script.split(";") if s.strip()]
        async with self._pool.acquire() as conn:
            for stmt in stmts:
                try:
                    await conn.execute(_sqlite_to_pg(stmt))
                except asyncpg.exceptions.DuplicateColumnError:
                    pass  # ALTER TABLE ADD COLUMN that already exists
                except asyncpg.exceptions.DuplicateTableError:
                    pass  # CREATE TABLE that already exists
                except asyncpg.exceptions.DuplicateObjectError:
                    pass  # CREATE INDEX that already exists

    # --- commit() — no-op in PostgreSQL (auto-commit) ---

    async def commit(self):
        pass

    # --- Close ---

    async def close(self):
        await self._pool.close()


# ======================================================================
# Pool + wrapper management
# ======================================================================

_wrapper: _PoolWrapper | None = None


async def get_db() -> _PoolWrapper:
    """Return the shared PoolWrapper (creates pool + tables on first call)."""
    global _pool, _wrapper
    if _wrapper is None:
        if not DATABASE_URL:
            raise RuntimeError(
                "DATABASE_URL is not set. "
                "Provide a PostgreSQL connection string (e.g. from Neon, Supabase, or Render)."
            )
        _pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
        _wrapper = _PoolWrapper(_pool)
        await _init_tables()
        logger.info("PostgreSQL pool created.")
    return _wrapper


async def get_raw_pool() -> asyncpg.Pool:
    """Return the raw asyncpg pool (for code that already uses native asyncpg)."""
    await get_db()  # ensure initialized
    assert _pool is not None
    return _pool


async def close_db():
    global _pool, _wrapper
    if _wrapper:
        await _wrapper.close()
        _wrapper = None
        _pool = None


# ======================================================================
# Schema init
# ======================================================================


async def _init_tables():
    assert _wrapper is not None
    await _wrapper.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            telegram_id BIGINT PRIMARY KEY,
            pacifica_account TEXT,
            agent_wallet_public TEXT,
            agent_wallet_encrypted TEXT,
            builder_approved INTEGER DEFAULT 0,
            ref_code TEXT UNIQUE,
            referred_by BIGINT,
            username TEXT,
            settings TEXT DEFAULT '{}',
            created_at TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS copy_configs (
            id SERIAL PRIMARY KEY,
            telegram_id BIGINT REFERENCES users(telegram_id),
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
            source TEXT DEFAULT 'pacifica',
            created_at TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS trade_log (
            id SERIAL PRIMARY KEY,
            telegram_id BIGINT REFERENCES users(telegram_id),
            symbol TEXT,
            side TEXT,
            amount TEXT,
            price TEXT,
            order_type TEXT,
            is_copy_trade INTEGER DEFAULT 0,
            master_wallet TEXT,
            client_order_id TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS price_alerts (
            id SERIAL PRIMARY KEY,
            telegram_id BIGINT REFERENCES users(telegram_id),
            symbol TEXT NOT NULL,
            direction TEXT NOT NULL,
            target_price REAL NOT NULL,
            active INTEGER DEFAULT 1,
            triggered INTEGER DEFAULT 0,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS referral_fees (
            id SERIAL PRIMARY KEY,
            referrer_id BIGINT NOT NULL,
            referee_id BIGINT NOT NULL,
            symbol TEXT,
            trade_volume REAL DEFAULT 0,
            fee_earned REAL DEFAULT 0,
            claimed INTEGER DEFAULT 0,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS beta_codes (
            id SERIAL PRIMARY KEY,
            code TEXT NOT NULL UNIQUE,
            added_by BIGINT,
            active INTEGER DEFAULT 1,
            uses INTEGER DEFAULT 0,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS trailing_stops (
            id SERIAL PRIMARY KEY,
            telegram_id BIGINT REFERENCES users(telegram_id),
            symbol TEXT NOT NULL,
            side TEXT NOT NULL,
            trail_percent REAL NOT NULL,
            entry_price REAL NOT NULL,
            peak_price REAL NOT NULL,
            callback_price REAL NOT NULL,
            active INTEGER DEFAULT 1,
            triggered_at TIMESTAMPTZ,
            phase INTEGER DEFAULT 1,
            current_tier INTEGER DEFAULT 0,
            peak_roe REAL DEFAULT 0,
            phase_start_time REAL,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS dca_configs (
            id SERIAL PRIMARY KEY,
            telegram_id BIGINT REFERENCES users(telegram_id),
            symbol TEXT NOT NULL,
            side TEXT NOT NULL,
            mode TEXT NOT NULL,
            total_amount_usd REAL NOT NULL,
            amount_per_order REAL NOT NULL,
            leverage INTEGER DEFAULT 1,
            interval_seconds INTEGER,
            price_levels TEXT,
            orders_executed INTEGER DEFAULT 0,
            orders_total INTEGER NOT NULL,
            avg_entry REAL DEFAULT 0,
            active INTEGER DEFAULT 1,
            next_execution TIMESTAMPTZ,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS scaled_orders (
            id SERIAL PRIMARY KEY,
            telegram_id BIGINT REFERENCES users(telegram_id),
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
            created_at TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS leader_profiles (
            telegram_id BIGINT PRIMARY KEY REFERENCES users(telegram_id),
            display_name TEXT NOT NULL,
            bio TEXT DEFAULT '',
            profit_share_pct REAL DEFAULT 10.0,
            is_public INTEGER DEFAULT 1,
            total_followers INTEGER DEFAULT 0,
            total_pnl_shared REAL DEFAULT 0,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS follower_pnl (
            id SERIAL PRIMARY KEY,
            follower_id BIGINT NOT NULL REFERENCES users(telegram_id),
            leader_id BIGINT NOT NULL,
            symbol TEXT NOT NULL,
            side TEXT NOT NULL,
            entry_price REAL NOT NULL,
            exit_price REAL,
            amount REAL NOT NULL,
            realized_pnl REAL DEFAULT 0,
            profit_shared REAL DEFAULT 0,
            status TEXT DEFAULT 'open',
            opened_at TIMESTAMPTZ DEFAULT NOW(),
            closed_at TIMESTAMPTZ
        );

        CREATE INDEX IF NOT EXISTS idx_follower_pnl_leader
            ON follower_pnl(leader_id);
        CREATE INDEX IF NOT EXISTS idx_follower_pnl_follower
            ON follower_pnl(follower_id, status);

        CREATE TABLE IF NOT EXISTS twap_orders (
            id SERIAL PRIMARY KEY,
            telegram_id BIGINT REFERENCES users(telegram_id),
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
            next_execution TIMESTAMPTZ,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS onchain_watches (
            id SERIAL PRIMARY KEY,
            telegram_id BIGINT NOT NULL,
            wallet_address TEXT NOT NULL,
            label TEXT,
            min_tx_usd REAL DEFAULT 10000,
            active INTEGER DEFAULT 1,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(telegram_id, wallet_address)
        );

        CREATE TABLE IF NOT EXISTS bot_settings (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)
    logger.info("Database tables initialised.")


# ------------------------------------------------------------------
# Helper: convert asyncpg.Record to dict
# ------------------------------------------------------------------

def _row(record) -> dict | None:
    if record is None:
        return None
    return dict(record)


def _rows(records: list) -> list[dict]:
    return [dict(r) for r in records]


# ------------------------------------------------------------------
# User CRUD
# ------------------------------------------------------------------

async def get_user(telegram_id: int) -> dict | None:
    pool = await get_raw_pool()
    row = await pool.fetchrow(
        "SELECT * FROM users WHERE telegram_id = $1", telegram_id
    )
    return _row(row)


async def create_user(
    telegram_id: int,
    agent_wallet_public: str | None,
    agent_wallet_encrypted: str,
) -> dict:
    pool = await get_raw_pool()
    await pool.execute(
        """INSERT INTO users (telegram_id, agent_wallet_public, agent_wallet_encrypted)
           VALUES ($1, $2, $3)""",
        telegram_id, agent_wallet_public, agent_wallet_encrypted,
    )
    return (await get_user(telegram_id))  # type: ignore


async def delete_user(telegram_id: int):
    pool = await get_raw_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("DELETE FROM price_alerts WHERE telegram_id = $1", telegram_id)
            await conn.execute("DELETE FROM trade_log WHERE telegram_id = $1", telegram_id)
            await conn.execute("DELETE FROM copy_configs WHERE telegram_id = $1", telegram_id)
            await conn.execute("DELETE FROM trailing_stops WHERE telegram_id = $1", telegram_id)
            await conn.execute("DELETE FROM dca_configs WHERE telegram_id = $1", telegram_id)
            await conn.execute("DELETE FROM scaled_orders WHERE telegram_id = $1", telegram_id)
            await conn.execute("DELETE FROM twap_orders WHERE telegram_id = $1", telegram_id)
            await conn.execute("DELETE FROM onchain_watches WHERE telegram_id = $1", telegram_id)
            await conn.execute("DELETE FROM follower_pnl WHERE follower_id = $1", telegram_id)
            await conn.execute("DELETE FROM leader_profiles WHERE telegram_id = $1", telegram_id)
            await conn.execute("DELETE FROM users WHERE telegram_id = $1", telegram_id)


async def get_user_by_wallet(wallet: str, exclude_tg_id: int | None = None) -> dict | None:
    pool = await get_raw_pool()
    if exclude_tg_id:
        row = await pool.fetchrow(
            "SELECT * FROM users WHERE pacifica_account = $1 AND telegram_id != $2",
            wallet, exclude_tg_id,
        )
    else:
        row = await pool.fetchrow(
            "SELECT * FROM users WHERE pacifica_account = $1", wallet
        )
    return _row(row)


async def update_user(telegram_id: int, **fields):
    if not fields:
        return
    pool = await get_raw_pool()
    sets = ", ".join(f"{k} = ${i+1}" for i, k in enumerate(fields))
    vals = list(fields.values()) + [telegram_id]
    idx = len(fields) + 1
    await pool.execute(
        f"UPDATE users SET {sets} WHERE telegram_id = ${idx}", *vals
    )


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
    pool = await get_raw_pool()
    row = await pool.fetchrow(
        """INSERT INTO copy_configs
           (telegram_id, master_wallet, sizing_mode, size_multiplier,
            fixed_amount_usd, pct_equity, min_trade_usd, max_position_usd,
            max_total_usd, symbols)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
           RETURNING id""",
        telegram_id, master_wallet, sizing_mode, size_multiplier,
        fixed_amount_usd, pct_equity, min_trade_usd, max_position_usd,
        max_total_usd, symbols,
    )
    return row["id"]  # type: ignore


async def get_active_copy_configs(telegram_id: int | None = None) -> list[dict]:
    pool = await get_raw_pool()
    if telegram_id:
        rows = await pool.fetch(
            "SELECT * FROM copy_configs WHERE telegram_id = $1 AND active = 1 AND COALESCE(source, 'pacifica') = 'pacifica'",
            telegram_id,
        )
    else:
        rows = await pool.fetch(
            "SELECT * FROM copy_configs WHERE active = 1 AND COALESCE(source, 'pacifica') = 'pacifica'"
        )
    return _rows(rows)


async def deactivate_copy_config(telegram_id: int, master_wallet: str):
    pool = await get_raw_pool()
    await pool.execute(
        "UPDATE copy_configs SET active = 0 WHERE telegram_id = $1 AND master_wallet = $2",
        telegram_id, master_wallet,
    )


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
    pool = await get_raw_pool()
    await pool.execute(
        """INSERT INTO trade_log
           (telegram_id, symbol, side, amount, price, order_type,
            is_copy_trade, master_wallet, client_order_id)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)""",
        telegram_id, symbol, side, amount, price, order_type,
        int(is_copy_trade), master_wallet, client_order_id,
    )


async def get_trade_history(telegram_id: int, limit: int = 20) -> list[dict]:
    pool = await get_raw_pool()
    rows = await pool.fetch(
        "SELECT * FROM trade_log WHERE telegram_id = $1 ORDER BY created_at DESC LIMIT $2",
        telegram_id, limit,
    )
    return _rows(rows)


# ------------------------------------------------------------------
# User settings helpers
# ------------------------------------------------------------------

async def get_user_settings(telegram_id: int) -> dict:
    user = await get_user(telegram_id)
    if not user:
        return {}
    try:
        return _json.loads(user.get("settings") or "{}")
    except Exception:
        return {}


async def set_user_setting(telegram_id: int, key: str, value):
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
    pool = await get_raw_pool()
    row = await pool.fetchrow(
        "SELECT * FROM users WHERE ref_code = $1 OR username = $1", code
    )
    return _row(row)


async def is_username_taken(username: str, exclude_tg_id: int | None = None) -> bool:
    pool = await get_raw_pool()
    lower = username.lower()
    if exclude_tg_id:
        row = await pool.fetchrow(
            "SELECT 1 FROM users WHERE LOWER(username) = $1 AND telegram_id != $2",
            lower, exclude_tg_id,
        )
    else:
        row = await pool.fetchrow(
            "SELECT 1 FROM users WHERE LOWER(username) = $1", lower
        )
    return row is not None


# ------------------------------------------------------------------
# Price alerts
# ------------------------------------------------------------------

async def add_price_alert(
    telegram_id: int, symbol: str, direction: str, target_price: float,
) -> int:
    pool = await get_raw_pool()
    row = await pool.fetchrow(
        """INSERT INTO price_alerts (telegram_id, symbol, direction, target_price)
           VALUES ($1, $2, $3, $4) RETURNING id""",
        telegram_id, symbol, direction, target_price,
    )
    return row["id"]  # type: ignore


async def get_active_alerts(telegram_id: int | None = None) -> list[dict]:
    pool = await get_raw_pool()
    if telegram_id:
        rows = await pool.fetch(
            "SELECT * FROM price_alerts WHERE telegram_id = $1 AND active = 1 AND triggered = 0",
            telegram_id,
        )
    else:
        rows = await pool.fetch(
            "SELECT * FROM price_alerts WHERE active = 1 AND triggered = 0"
        )
    return _rows(rows)


async def trigger_alert(alert_id: int):
    pool = await get_raw_pool()
    await pool.execute(
        "UPDATE price_alerts SET triggered = 1, active = 0 WHERE id = $1", alert_id
    )


async def delete_alert(alert_id: int, telegram_id: int):
    pool = await get_raw_pool()
    await pool.execute(
        "DELETE FROM price_alerts WHERE id = $1 AND telegram_id = $2",
        alert_id, telegram_id,
    )


async def count_referrals(telegram_id: int) -> int:
    pool = await get_raw_pool()
    row = await pool.fetchrow(
        "SELECT COUNT(*) AS cnt FROM users WHERE referred_by = $1", telegram_id
    )
    return row["cnt"] if row else 0


async def get_referrals(telegram_id: int) -> list[dict]:
    pool = await get_raw_pool()
    rows = await pool.fetch(
        "SELECT telegram_id, pacifica_account, created_at FROM users WHERE referred_by = $1",
        telegram_id,
    )
    return _rows(rows)


# ------------------------------------------------------------------
# Referral fee tracking
# ------------------------------------------------------------------

REFERRAL_FEE_SHARE = 0.10
REFEREE_FEE_REBATE = 0.05


async def log_referral_fee(
    referrer_id: int, referee_id: int, symbol: str,
    trade_volume: float, fee_earned: float,
):
    pool = await get_raw_pool()
    await pool.execute(
        """INSERT INTO referral_fees (referrer_id, referee_id, symbol, trade_volume, fee_earned)
           VALUES ($1, $2, $3, $4, $5)""",
        referrer_id, referee_id, symbol, trade_volume, fee_earned,
    )


async def get_unclaimed_fees(telegram_id: int) -> float:
    pool = await get_raw_pool()
    row = await pool.fetchrow(
        "SELECT COALESCE(SUM(fee_earned), 0) AS total FROM referral_fees WHERE referrer_id = $1 AND claimed = 0",
        telegram_id,
    )
    return float(row["total"]) if row else 0.0


async def get_total_fees_earned(telegram_id: int) -> float:
    pool = await get_raw_pool()
    row = await pool.fetchrow(
        "SELECT COALESCE(SUM(fee_earned), 0) AS total FROM referral_fees WHERE referrer_id = $1",
        telegram_id,
    )
    return float(row["total"]) if row else 0.0


async def claim_referral_fees(telegram_id: int) -> float:
    unclaimed = await get_unclaimed_fees(telegram_id)
    if unclaimed > 0:
        pool = await get_raw_pool()
        await pool.execute(
            "UPDATE referral_fees SET claimed = 1 WHERE referrer_id = $1 AND claimed = 0",
            telegram_id,
        )
    return unclaimed


async def get_referral_stats(telegram_id: int) -> dict:
    ref_count = await count_referrals(telegram_id)
    total_earned = await get_total_fees_earned(telegram_id)
    unclaimed = await get_unclaimed_fees(telegram_id)

    pool = await get_raw_pool()
    row = await pool.fetchrow(
        "SELECT COALESCE(SUM(trade_volume), 0) AS total FROM referral_fees WHERE referrer_id = $1",
        telegram_id,
    )
    total_volume = float(row["total"]) if row else 0.0

    return {
        "referral_count": ref_count,
        "total_earned": total_earned,
        "unclaimed": unclaimed,
        "total_volume": total_volume,
    }


# ------------------------------------------------------------------
# Beta codes
# ------------------------------------------------------------------

async def add_beta_code(code: str, added_by: int | None = None) -> bool:
    pool = await get_raw_pool()
    try:
        await pool.execute(
            "INSERT INTO beta_codes (code, added_by) VALUES ($1, $2)",
            code.strip(), added_by,
        )
        return True
    except Exception:
        return False


async def get_active_beta_codes() -> list[str]:
    pool = await get_raw_pool()
    rows = await pool.fetch(
        "SELECT code FROM beta_codes WHERE active = 1 ORDER BY created_at DESC"
    )
    return [r["code"] for r in rows]


async def deactivate_beta_code(code: str):
    pool = await get_raw_pool()
    await pool.execute(
        "UPDATE beta_codes SET active = 0 WHERE code = $1", code
    )


async def increment_beta_code_uses(code: str):
    pool = await get_raw_pool()
    await pool.execute(
        "UPDATE beta_codes SET uses = uses + 1 WHERE code = $1", code
    )


async def get_all_beta_codes() -> list[dict]:
    pool = await get_raw_pool()
    rows = await pool.fetch(
        "SELECT * FROM beta_codes ORDER BY created_at DESC"
    )
    return _rows(rows)


# ------------------------------------------------------------------
# Trailing stops
# ------------------------------------------------------------------

async def add_trailing_stop(
    telegram_id: int, symbol: str, side: str, trail_percent: float,
    entry_price: float, peak_price: float, callback_price: float,
) -> int:
    pool = await get_raw_pool()
    row = await pool.fetchrow(
        """INSERT INTO trailing_stops
           (telegram_id, symbol, side, trail_percent, entry_price, peak_price, callback_price)
           VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id""",
        telegram_id, symbol, side, trail_percent, entry_price, peak_price, callback_price,
    )
    return row["id"]  # type: ignore


async def get_active_trailing_stops(telegram_id: int | None = None) -> list[dict]:
    pool = await get_raw_pool()
    if telegram_id:
        rows = await pool.fetch(
            "SELECT * FROM trailing_stops WHERE telegram_id = $1 AND active = 1",
            telegram_id,
        )
    else:
        rows = await pool.fetch(
            "SELECT * FROM trailing_stops WHERE active = 1"
        )
    return _rows(rows)


async def cancel_trailing_stop(stop_id: int, telegram_id: int):
    pool = await get_raw_pool()
    await pool.execute(
        "UPDATE trailing_stops SET active = 0 WHERE id = $1 AND telegram_id = $2",
        stop_id, telegram_id,
    )


# ------------------------------------------------------------------
# DCA configs
# ------------------------------------------------------------------

async def add_dca_config(
    telegram_id: int, symbol: str, side: str, mode: str,
    total_amount_usd: float, amount_per_order: float, orders_total: int,
    leverage: int = 1, interval_seconds: int | None = None,
    price_levels: str | None = None,
) -> int:
    pool = await get_raw_pool()
    row = await pool.fetchrow(
        """INSERT INTO dca_configs
           (telegram_id, symbol, side, mode, total_amount_usd, amount_per_order,
            orders_total, leverage, interval_seconds, price_levels, next_execution)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW()) RETURNING id""",
        telegram_id, symbol, side, mode, total_amount_usd, amount_per_order,
        orders_total, leverage, interval_seconds, price_levels,
    )
    return row["id"]  # type: ignore


async def get_active_dca_configs(telegram_id: int | None = None) -> list[dict]:
    pool = await get_raw_pool()
    if telegram_id:
        rows = await pool.fetch(
            "SELECT * FROM dca_configs WHERE telegram_id = $1 AND active = 1",
            telegram_id,
        )
    else:
        rows = await pool.fetch(
            "SELECT * FROM dca_configs WHERE active = 1"
        )
    return _rows(rows)


async def update_dca_progress(dca_id: int, orders_executed: int, avg_entry: float, next_execution: str | None = None):
    pool = await get_raw_pool()
    if next_execution:
        await pool.execute(
            "UPDATE dca_configs SET orders_executed = $1, avg_entry = $2, next_execution = $3 WHERE id = $4",
            orders_executed, avg_entry, next_execution, dca_id,
        )
    else:
        await pool.execute(
            "UPDATE dca_configs SET orders_executed = $1, avg_entry = $2, active = 0 WHERE id = $3",
            orders_executed, avg_entry, dca_id,
        )


async def cancel_dca(dca_id: int, telegram_id: int):
    pool = await get_raw_pool()
    await pool.execute(
        "UPDATE dca_configs SET active = 0 WHERE id = $1 AND telegram_id = $2",
        dca_id, telegram_id,
    )


# ------------------------------------------------------------------
# Scaled orders
# ------------------------------------------------------------------

async def add_scaled_order(
    telegram_id: int, symbol: str, side: str, total_amount_usd: float,
    price_low: float, price_high: float, num_levels: int,
    distribution: str = "even", leverage: int = 1,
) -> int:
    pool = await get_raw_pool()
    row = await pool.fetchrow(
        """INSERT INTO scaled_orders
           (telegram_id, symbol, side, total_amount_usd, price_low, price_high,
            num_levels, distribution, leverage)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) RETURNING id""",
        telegram_id, symbol, side, total_amount_usd, price_low, price_high,
        num_levels, distribution, leverage,
    )
    return row["id"]  # type: ignore


# ------------------------------------------------------------------
# Leader profiles
# ------------------------------------------------------------------

async def create_leader_profile(
    telegram_id: int, display_name: str, bio: str = "",
    profit_share_pct: float = 10.0,
) -> dict:
    pool = await get_raw_pool()
    await pool.execute(
        """INSERT INTO leader_profiles (telegram_id, display_name, bio, profit_share_pct)
           VALUES ($1, $2, $3, $4)
           ON CONFLICT(telegram_id) DO UPDATE SET
             display_name = $2, bio = $3, profit_share_pct = $4, is_public = 1""",
        telegram_id, display_name, bio, profit_share_pct,
    )
    return (await get_leader_profile(telegram_id))  # type: ignore


async def get_leader_profile(telegram_id: int) -> dict | None:
    pool = await get_raw_pool()
    row = await pool.fetchrow(
        "SELECT * FROM leader_profiles WHERE telegram_id = $1", telegram_id
    )
    return _row(row)


async def get_public_leaders() -> list[dict]:
    pool = await get_raw_pool()
    rows = await pool.fetch(
        """SELECT lp.*, u.pacifica_account, u.username
           FROM leader_profiles lp
           JOIN users u ON lp.telegram_id = u.telegram_id
           WHERE lp.is_public = 1
           ORDER BY lp.total_followers DESC"""
    )
    return _rows(rows)


async def update_leader_followers(leader_id: int, delta: int):
    pool = await get_raw_pool()
    await pool.execute(
        "UPDATE leader_profiles SET total_followers = GREATEST(0, total_followers + $1) WHERE telegram_id = $2",
        delta, leader_id,
    )


async def deactivate_leader(telegram_id: int):
    pool = await get_raw_pool()
    await pool.execute(
        "UPDATE leader_profiles SET is_public = 0 WHERE telegram_id = $1",
        telegram_id,
    )


# ------------------------------------------------------------------
# Follower PnL tracking
# ------------------------------------------------------------------

async def open_follower_position(
    follower_id: int, leader_id: int, symbol: str, side: str,
    entry_price: float, amount: float,
) -> int:
    pool = await get_raw_pool()
    row = await pool.fetchrow(
        """INSERT INTO follower_pnl
           (follower_id, leader_id, symbol, side, entry_price, amount)
           VALUES ($1, $2, $3, $4, $5, $6) RETURNING id""",
        follower_id, leader_id, symbol, side, entry_price, amount,
    )
    return row["id"]  # type: ignore


async def close_follower_position(
    follower_id: int, leader_id: int, symbol: str, exit_price: float,
) -> dict | None:
    pool = await get_raw_pool()
    row = await pool.fetchrow(
        """SELECT * FROM follower_pnl
           WHERE follower_id = $1 AND leader_id = $2 AND symbol = $3 AND status = 'open'
           ORDER BY opened_at DESC LIMIT 1""",
        follower_id, leader_id, symbol,
    )
    if not row:
        return None
    pos = dict(row)

    amount = pos["amount"]
    entry = pos["entry_price"]
    if pos["side"].lower() in ("long", "bid", "buy"):
        pnl = (exit_price - entry) * amount
    else:
        pnl = (entry - exit_price) * amount

    profit_share = 0.0
    if pnl > 0:
        leader = await get_leader_profile(leader_id)
        share_pct = leader.get("profit_share_pct", 10.0) if leader else 10.0
        profit_share = pnl * (share_pct / 100.0)

    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                """UPDATE follower_pnl
                   SET exit_price = $1, realized_pnl = $2, profit_shared = $3,
                       status = 'closed', closed_at = NOW()
                   WHERE id = $4""",
                exit_price, pnl, profit_share, pos["id"],
            )
            if profit_share > 0:
                await conn.execute(
                    "UPDATE leader_profiles SET total_pnl_shared = total_pnl_shared + $1 WHERE telegram_id = $2",
                    profit_share, leader_id,
                )

    return {"pnl": pnl, "profit_share": profit_share, "entry": entry, "exit": exit_price, "amount": amount}


async def get_leader_performance(leader_id: int) -> dict:
    pool = await get_raw_pool()
    row = await pool.fetchrow(
        """SELECT
             COUNT(*) as total_trades,
             SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END) as wins,
             SUM(realized_pnl) as total_pnl,
             SUM(profit_shared) as total_shared
           FROM follower_pnl WHERE leader_id = $1 AND status = 'closed'""",
        leader_id,
    )
    if not row or not row["total_trades"]:
        return {"total_trades": 0, "wins": 0, "total_pnl": 0, "total_shared": 0, "win_rate": 0}
    total = row["total_trades"]
    wins = row["wins"] or 0
    return {
        "total_trades": total,
        "wins": wins,
        "total_pnl": row["total_pnl"] or 0,
        "total_shared": row["total_shared"] or 0,
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
    pool = await get_raw_pool()
    row = await pool.fetchrow(
        """INSERT INTO twap_orders
           (telegram_id, symbol, side, total_amount_usd, num_slices,
            interval_seconds, amount_per_slice, leverage, randomize,
            next_execution)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW()) RETURNING id""",
        telegram_id, symbol, side, total_amount_usd, num_slices,
        interval_seconds, amount_per_slice, leverage, int(randomize),
    )
    return row["id"]  # type: ignore


async def get_active_twap_orders(telegram_id: int | None = None) -> list[dict]:
    pool = await get_raw_pool()
    if telegram_id:
        rows = await pool.fetch(
            "SELECT * FROM twap_orders WHERE telegram_id = $1 AND active = 1",
            telegram_id,
        )
    else:
        rows = await pool.fetch(
            "SELECT * FROM twap_orders WHERE active = 1"
        )
    return _rows(rows)


async def update_twap_progress(
    twap_id: int, slices_executed: int, avg_price: float,
    next_execution: str | None = None,
):
    pool = await get_raw_pool()
    if next_execution:
        await pool.execute(
            """UPDATE twap_orders
               SET slices_executed = $1, avg_price = $2, next_execution = $3
               WHERE id = $4""",
            slices_executed, avg_price, next_execution, twap_id,
        )
    else:
        await pool.execute(
            """UPDATE twap_orders
               SET slices_executed = $1, avg_price = $2, active = 0
               WHERE id = $3""",
            slices_executed, avg_price, twap_id,
        )


async def cancel_twap(twap_id: int, telegram_id: int):
    pool = await get_raw_pool()
    await pool.execute(
        "UPDATE twap_orders SET active = 0 WHERE id = $1 AND telegram_id = $2",
        twap_id, telegram_id,
    )


# ------------------------------------------------------------------
# On-chain watches
# ------------------------------------------------------------------

async def add_onchain_watch(
    telegram_id: int, wallet_address: str, label: str | None = None,
    min_tx_usd: float = 10000,
) -> bool:
    pool = await get_raw_pool()
    try:
        await pool.execute(
            """INSERT INTO onchain_watches (telegram_id, wallet_address, label, min_tx_usd)
               VALUES ($1, $2, $3, $4)""",
            telegram_id, wallet_address, label, min_tx_usd,
        )
        return True
    except Exception:
        return False


async def remove_onchain_watch(telegram_id: int, wallet_address: str) -> bool:
    pool = await get_raw_pool()
    result = await pool.execute(
        "DELETE FROM onchain_watches WHERE telegram_id = $1 AND wallet_address = $2",
        telegram_id, wallet_address,
    )
    return result.split()[-1] != "0"


async def get_onchain_watches(telegram_id: int | None = None) -> list[dict]:
    pool = await get_raw_pool()
    if telegram_id:
        rows = await pool.fetch(
            "SELECT * FROM onchain_watches WHERE telegram_id = $1 AND active = 1",
            telegram_id,
        )
    else:
        rows = await pool.fetch(
            "SELECT * FROM onchain_watches WHERE active = 1"
        )
    return _rows(rows)


async def get_all_onchain_addresses() -> dict[str, list[tuple[int, float]]]:
    pool = await get_raw_pool()
    rows = await pool.fetch(
        "SELECT wallet_address, telegram_id, min_tx_usd FROM onchain_watches WHERE active = 1"
    )
    result: dict[str, list[tuple[int, float]]] = {}
    for row in rows:
        result.setdefault(row["wallet_address"], []).append((row["telegram_id"], row["min_tx_usd"]))
    return result
