"""
Mean Reversion — buys below SMA, sells above SMA.
- Tracks 20-period SMA from 1-minute price snapshots
- Enters when price deviates > ENTRY_DEVIATION bps from SMA
- Exits when price returns to SMA (or via stop loss)
- Works best in CALM/NORMAL regime (check regime_classifier)
"""

import asyncio
import logging
import time
from collections import defaultdict, deque

from aiogram import Bot

from bot.services.pacifica_client import PacificaClient
from bot.services.market_data import get_price, usd_to_token, get_lot_size
from bot.services.wallet_manager import decrypt_private_key
from database.db import get_db

logger = logging.getLogger(__name__)

_running = False

# Config
SMA_PERIOD = 20          # periods for SMA
ENTRY_DEVIATION = 30     # bps deviation to trigger entry
EXIT_DEVIATION = 5       # bps from SMA to exit (near mean)
STOP_LOSS_BPS = 80       # stop loss in bps
CHECK_INTERVAL = 60      # 1 min
MAX_POSITIONS = 2        # max simultaneous mean reversion positions

# In-memory price history per symbol: {symbol: deque([price, ...])}
_price_history: dict[str, deque] = defaultdict(lambda: deque(maxlen=SMA_PERIOD))

# Performance tracking (in-memory, reset on restart)
_stats: dict = {
    "total_trades": 0,
    "winning_trades": 0,
    "total_pnl": 0.0,
    "last_updated": 0.0,
}


# ---------------------------------------------------------------------------
# DB setup
# ---------------------------------------------------------------------------

async def _init_mr_tables():
    """Create mean reversion tables if they don't exist."""
    db = await get_db()
    await db.executescript("""
        CREATE TABLE IF NOT EXISTS mean_reversion_configs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            telegram_id INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            active INTEGER DEFAULT 1,
            size_usd REAL NOT NULL,
            entry_deviation INTEGER DEFAULT 30,
            stop_loss_bps INTEGER DEFAULT 80,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS mean_reversion_positions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            telegram_id INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            side TEXT NOT NULL,
            entry_price REAL NOT NULL,
            size REAL NOT NULL,
            sma_at_entry REAL NOT NULL,
            exit_price REAL,
            pnl REAL,
            status TEXT DEFAULT 'open',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            closed_at TIMESTAMP
        );
    """)
    await db.commit()
    logger.info("Mean reversion tables ready")


# ---------------------------------------------------------------------------
# Regime check (graceful fallback if classifier not running)
# ---------------------------------------------------------------------------

def _is_tradeable_regime() -> bool:
    """Return True if current regime allows mean reversion (CALM or NORMAL)."""
    try:
        from bot.services.regime_classifier import get_regime
        regime = get_regime()
        return regime.get("regime", "NORMAL") in ("CALM", "NORMAL")
    except Exception:
        return True  # fallback: always tradeable


# ---------------------------------------------------------------------------
# SMA calculation
# ---------------------------------------------------------------------------

def _compute_sma(prices: deque) -> float | None:
    """Compute SMA from the price deque. Returns None if not enough data."""
    if len(prices) < SMA_PERIOD:
        return None
    return sum(prices) / len(prices)


def _deviation_bps(price: float, sma: float) -> float:
    """Return signed deviation of price from SMA in basis points."""
    return (price - sma) / sma * 10_000


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

async def _get_active_configs() -> list[dict]:
    db = await get_db()
    async with db.execute(
        "SELECT * FROM mean_reversion_configs WHERE active = 1"
    ) as cursor:
        return [dict(r) for r in await cursor.fetchall()]


async def _get_open_positions(telegram_id: int | None = None) -> list[dict]:
    await _init_mr_tables()
    db = await get_db()
    if telegram_id is not None:
        q = "SELECT * FROM mean_reversion_positions WHERE status = 'open' AND telegram_id = ?"
        params = (telegram_id,)
    else:
        q = "SELECT * FROM mean_reversion_positions WHERE status = 'open'"
        params = ()
    async with db.execute(q, params) as cursor:
        return [dict(r) for r in await cursor.fetchall()]


async def _open_position(
    telegram_id: int, symbol: str, side: str,
    entry_price: float, size: float, sma_at_entry: float,
) -> int:
    db = await get_db()
    cursor = await db.execute(
        """INSERT INTO mean_reversion_positions
           (telegram_id, symbol, side, entry_price, size, sma_at_entry)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (telegram_id, symbol, side, entry_price, size, sma_at_entry),
    )
    await db.commit()
    return cursor.lastrowid  # type: ignore


async def _close_position(pos_id: int, exit_price: float, pnl: float):
    db = await get_db()
    await db.execute(
        """UPDATE mean_reversion_positions
           SET exit_price = ?, pnl = ?, status = 'closed',
               closed_at = CURRENT_TIMESTAMP
           WHERE id = ?""",
        (exit_price, pnl, pos_id),
    )
    await db.commit()


# ---------------------------------------------------------------------------
# User client helper
# ---------------------------------------------------------------------------

async def _get_user_client(telegram_id: int) -> PacificaClient | None:
    db = await get_db()
    async with db.execute(
        "SELECT pacifica_account, agent_wallet_encrypted FROM users WHERE telegram_id = ?",
        (telegram_id,),
    ) as cursor:
        user = await cursor.fetchone()
    if not user or not user[0] or not user[1]:
        return None
    kp = decrypt_private_key(user[1])
    return PacificaClient(account=user[0], keypair=kp)


# ---------------------------------------------------------------------------
# Entry logic
# ---------------------------------------------------------------------------

async def _try_enter(bot: Bot, config: dict, price: float, sma: float):
    """Attempt to open a mean reversion position if conditions are met."""
    tg_id = config["telegram_id"]
    symbol = config["symbol"]
    size_usd = config["size_usd"]
    entry_deviation = config.get("entry_deviation", ENTRY_DEVIATION)

    # Check regime
    if not _is_tradeable_regime():
        logger.debug("MR %s: regime not tradeable, skipping entry", symbol)
        return

    # Check max open positions for this user
    open_pos = await _get_open_positions(tg_id)
    user_symbol_pos = [p for p in open_pos if p["symbol"] == symbol]
    if len(open_pos) >= MAX_POSITIONS:
        logger.debug("MR %s: max positions (%d) reached for user %s", symbol, MAX_POSITIONS, tg_id)
        return

    # Determine signal direction
    dev_bps = _deviation_bps(price, sma)

    if dev_bps <= -entry_deviation:
        side = "buy"   # price below SMA — buy the dip
    elif dev_bps >= entry_deviation:
        side = "sell"  # price above SMA — sell the rip
    else:
        return  # no signal

    # Already have a position in this direction for this symbol?
    already = any(
        p["symbol"] == symbol and p["side"] == side
        for p in user_symbol_pos
    )
    if already:
        return

    client = await _get_user_client(tg_id)
    if not client:
        logger.warning("MR: no wallet for user %s", tg_id)
        return

    try:
        lot_size = await get_lot_size(symbol)
        token_amount = usd_to_token(size_usd, price, lot_size)
        if float(token_amount) <= 0:
            return

        result = await client.create_market_order(
            symbol=symbol,
            side=side,
            amount=token_amount,
        )
        logger.info(
            "MR entry: %s %s %s at $%.4f (SMA $%.4f, dev %.1fbps), user %s",
            side.upper(), token_amount, symbol, price, sma, dev_bps, tg_id,
        )

        pos_id = await _open_position(
            telegram_id=tg_id,
            symbol=symbol,
            side=side,
            entry_price=price,
            size=float(token_amount),
            sma_at_entry=sma,
        )

        _stats["total_trades"] += 1
        _stats["last_updated"] = time.time()

        direction_label = "below" if side == "buy" else "above"
        try:
            await bot.send_message(
                tg_id,
                f"<b>Mean Reversion Entry</b>\n\n"
                f"<b>{symbol}</b> {side.upper()} opened\n"
                f"Price: <code>${price:,.4f}</code> ({direction_label} SMA)\n"
                f"SMA: <code>${sma:,.4f}</code>\n"
                f"Deviation: <code>{dev_bps:.1f}bps</code>\n"
                f"Size: <code>{token_amount}</code> (${size_usd:,.2f})\n"
                f"Stop loss: <code>{config.get('stop_loss_bps', STOP_LOSS_BPS)}bps</code>",
            )
        except Exception:
            pass

    except Exception as e:
        logger.error("MR entry failed for %s %s: %s", tg_id, symbol, e)
    finally:
        await client.close()


# ---------------------------------------------------------------------------
# Exit logic
# ---------------------------------------------------------------------------

async def _monitor_positions(bot: Bot):
    """Check open MR positions for exit conditions (mean revert or stop loss)."""
    open_positions = await _get_open_positions()
    if not open_positions:
        return

    for pos in open_positions:
        tg_id = pos["telegram_id"]
        symbol = pos["symbol"]
        side = pos["side"]
        entry_price = pos["entry_price"]
        sma_at_entry = pos["sma_at_entry"]
        size = pos["size"]

        current_price = await get_price(symbol)
        if not current_price:
            continue

        # Compute current SMA (may have updated since entry)
        history = _price_history[symbol]
        current_sma = _compute_sma(history) or sma_at_entry

        dev_bps = _deviation_bps(current_price, current_sma)
        stop_loss_bps = STOP_LOSS_BPS  # could be per-config

        # Determine exit conditions
        exit_reason = None
        if side == "buy":
            # Exit when price returns near SMA
            if dev_bps >= -EXIT_DEVIATION:
                exit_reason = "mean_revert"
            # Stop loss: price fell further below entry
            elif _deviation_bps(current_price, entry_price) <= -stop_loss_bps:
                exit_reason = "stop_loss"
        else:  # sell
            # Exit when price returns near SMA
            if dev_bps <= EXIT_DEVIATION:
                exit_reason = "mean_revert"
            # Stop loss: price rose further above entry
            elif _deviation_bps(current_price, entry_price) >= stop_loss_bps:
                exit_reason = "stop_loss"

        if exit_reason is None:
            continue

        # Execute close
        client = await _get_user_client(tg_id)
        if not client:
            continue

        try:
            close_side = "sell" if side == "buy" else "buy"
            lot_size = await get_lot_size(symbol)
            token_amount = usd_to_token(size * current_price, current_price, lot_size)
            if float(token_amount) <= 0:
                token_amount = str(size)

            await client.create_market_order(
                symbol=symbol,
                side=close_side,
                amount=token_amount,
                reduce_only=True,
            )

            # Calculate PnL
            if side == "buy":
                pnl = (current_price - entry_price) * size
            else:
                pnl = (entry_price - current_price) * size

            await _close_position(pos["id"], current_price, pnl)

            # Update stats
            if pnl > 0:
                _stats["winning_trades"] += 1
            _stats["total_pnl"] += pnl
            _stats["last_updated"] = time.time()

            logger.info(
                "MR exit (%s): %s %s at $%.4f, PnL $%.2f, user %s",
                exit_reason, symbol, side.upper(), current_price, pnl, tg_id,
            )

            emoji = "green_circle" if pnl >= 0 else "red_circle"
            pnl_emoji = "\U0001f7e2" if pnl >= 0 else "\U0001f534"
            reason_label = "Mean Reverted" if exit_reason == "mean_revert" else "Stop Loss Hit"
            try:
                await bot.send_message(
                    tg_id,
                    f"<b>Mean Reversion Exit — {reason_label}</b>\n\n"
                    f"{pnl_emoji} <b>{symbol}</b> {side.upper()} closed\n"
                    f"Entry: <code>${entry_price:,.4f}</code>\n"
                    f"Exit: <code>${current_price:,.4f}</code>\n"
                    f"PnL: <code>${pnl:+,.2f}</code>\n"
                    f"SMA: <code>${current_sma:,.4f}</code>",
                )
            except Exception:
                pass

        except Exception as e:
            logger.error("MR exit failed for %s %s: %s", tg_id, symbol, e)
        finally:
            await client.close()


# ---------------------------------------------------------------------------
# Main loop tick
# ---------------------------------------------------------------------------

async def _tick(bot: Bot):
    """One check cycle: collect prices, compute SMA, evaluate entries and exits."""
    # 1. Monitor open positions first (exit logic)
    await _monitor_positions(bot)

    # 2. Fetch active configs and collect prices for all tracked symbols
    configs = await _get_active_configs()
    if not configs:
        return

    # Collect unique symbols
    symbols = list({c["symbol"] for c in configs})

    for symbol in symbols:
        price = await get_price(symbol)
        if not price:
            logger.debug("MR: no price for %s", symbol)
            continue
        _price_history[symbol].append(price)

    # 3. Evaluate entry signals per config
    for config in configs:
        symbol = config["symbol"]
        price = await get_price(symbol)
        if not price:
            continue

        sma = _compute_sma(_price_history[symbol])
        if sma is None:
            logger.debug(
                "MR %s: not enough price history (%d/%d periods)",
                symbol, len(_price_history[symbol]), SMA_PERIOD,
            )
            continue

        await _try_enter(bot, config, price, sma)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def get_active_mr_positions() -> list:
    """Return all currently open mean reversion positions (across all users)."""
    return await _get_open_positions()


async def get_mr_stats() -> dict:
    """Return performance stats for dashboard consumption."""
    db = await get_db()
    async with db.execute(
        """SELECT
               COUNT(*) as total,
               SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as wins,
               COALESCE(SUM(pnl), 0) as total_pnl,
               COALESCE(SUM(CASE WHEN status = 'open' THEN 1 ELSE 0 END), 0) as open_count
           FROM mean_reversion_positions"""
    ) as cursor:
        row = await cursor.fetchone()

    if row:
        total = row[0] or 0
        wins = row[1] or 0
        total_pnl = float(row[2] or 0)
        open_count = int(row[3] or 0)
    else:
        total = wins = open_count = 0
        total_pnl = 0.0

    return {
        "total_trades": total,
        "winning_trades": wins,
        "win_rate": (wins / total * 100) if total > 0 else 0.0,
        "total_pnl": total_pnl,
        "open_positions": open_count,
        "sma_period": SMA_PERIOD,
        "entry_deviation_bps": ENTRY_DEVIATION,
        "stop_loss_bps": STOP_LOSS_BPS,
        "running": _running,
        "last_updated": _stats["last_updated"],
    }


# ---------------------------------------------------------------------------
# Service lifecycle
# ---------------------------------------------------------------------------

async def start_mean_reversion(bot: Bot):
    """Start the mean reversion strategy background loop."""
    global _running
    _running = True

    await _init_mr_tables()

    logger.info(
        "Mean reversion service started (SMA=%d, entry=%dbps, stop=%dbps, check=%ds)",
        SMA_PERIOD, ENTRY_DEVIATION, STOP_LOSS_BPS, CHECK_INTERVAL,
    )

    while _running:
        try:
            await _tick(bot)
        except Exception as e:
            logger.error("Mean reversion error: %s", e)
        await asyncio.sleep(CHECK_INTERVAL)


async def stop_mean_reversion():
    global _running
    _running = False
    logger.info("Mean reversion service stopped")
