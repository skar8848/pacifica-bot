"""
Grid trading engine — places buy/sell orders at evenly spaced price levels
within a user-defined range, capturing profit from each completed grid cycle.
"""

import asyncio
import json
import logging

from aiogram import Bot

from bot.services.pacifica_client import PacificaClient
from bot.services.market_data import get_price, usd_to_token, get_lot_size
from bot.services.wallet_manager import decrypt_private_key
from database.db import get_db

logger = logging.getLogger(__name__)

_running = False
CHECK_INTERVAL = 10  # seconds


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

async def _init_grid_tables():
    """Create the grid_configs table if it doesn't exist."""
    db = await get_db()
    await db.execute(
        """
        CREATE TABLE IF NOT EXISTS grid_configs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            telegram_id INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            price_low REAL NOT NULL,
            price_high REAL NOT NULL,
            num_grids INTEGER NOT NULL,
            total_amount_usd REAL NOT NULL,
            amount_per_grid REAL NOT NULL,
            grids_filled TEXT DEFAULT '{}',
            realized_pnl REAL DEFAULT 0,
            active INTEGER DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    await db.commit()
    logger.info("grid_configs table ready")


async def add_grid_config(
    telegram_id: int,
    symbol: str,
    price_low: float,
    price_high: float,
    num_grids: int,
    total_amount_usd: float,
) -> int:
    """Insert a new grid configuration and return its ID."""
    amount_per_grid = total_amount_usd / num_grids
    db = await get_db()
    cursor = await db.execute(
        """
        INSERT INTO grid_configs
            (telegram_id, symbol, price_low, price_high, num_grids,
             total_amount_usd, amount_per_grid)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (telegram_id, symbol, price_low, price_high, num_grids,
         total_amount_usd, amount_per_grid),
    )
    await db.commit()
    grid_id = cursor.lastrowid
    logger.info(
        "Grid #%s created for user %s: %s [%.2f–%.2f] x%d grids, $%.2f each",
        grid_id, telegram_id, symbol, price_low, price_high,
        num_grids, amount_per_grid,
    )
    return grid_id


async def get_active_grids(telegram_id: int | None = None) -> list[dict]:
    """Return all active grid configs, optionally filtered by user."""
    db = await get_db()
    if telegram_id is not None:
        async with db.execute(
            "SELECT * FROM grid_configs WHERE active = 1 AND telegram_id = ?",
            (telegram_id,),
        ) as cursor:
            rows = await cursor.fetchall()
    else:
        async with db.execute(
            "SELECT * FROM grid_configs WHERE active = 1"
        ) as cursor:
            rows = await cursor.fetchall()

    columns = [d[0] for d in cursor.description] if cursor.description else []
    return [dict(zip(columns, row)) for row in rows]


async def cancel_grid(grid_id: int, telegram_id: int):
    """Deactivate a grid config owned by the given user."""
    db = await get_db()
    await db.execute(
        "UPDATE grid_configs SET active = 0 WHERE id = ? AND telegram_id = ?",
        (grid_id, telegram_id),
    )
    await db.commit()
    logger.info("Grid #%s cancelled by user %s", grid_id, telegram_id)


# ---------------------------------------------------------------------------
# Grid level helpers
# ---------------------------------------------------------------------------

def _compute_levels(price_low: float, price_high: float, num_grids: int) -> list[float]:
    """Return evenly spaced grid levels from price_low to price_high."""
    step = (price_high - price_low) / num_grids
    return [price_low + i * step for i in range(num_grids + 1)]


def _parse_filled(raw: str | None) -> dict:
    """Deserialize grids_filled JSON. Keys are stringified grid indices."""
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return {}


async def _save_filled(grid_id: int, filled: dict, realized_pnl: float):
    """Persist updated grids_filled and realized_pnl."""
    db = await get_db()
    await db.execute(
        "UPDATE grid_configs SET grids_filled = ?, realized_pnl = ? WHERE id = ?",
        (json.dumps(filled), realized_pnl, grid_id),
    )
    await db.commit()


# ---------------------------------------------------------------------------
# Order execution
# ---------------------------------------------------------------------------

async def _execute_grid_order(
    bot: Bot,
    grid: dict,
    grid_level: float,
    side: str,
    price: float,
):
    """Execute a single grid buy or sell order and notify the user."""
    tg_id = grid["telegram_id"]
    symbol = grid["symbol"]
    amount_usd = grid["amount_per_grid"]

    db = await get_db()
    async with db.execute(
        "SELECT pacifica_account, agent_wallet_encrypted FROM users WHERE telegram_id = ?",
        (tg_id,),
    ) as cursor:
        user = await cursor.fetchone()

    if not user or not user[0] or not user[1]:
        logger.warning("Grid #%s: user %s has no wallet configured", grid["id"], tg_id)
        return False

    account = user[0]
    encrypted_key = user[1]

    try:
        kp = decrypt_private_key(encrypted_key)
        client = PacificaClient(account=account, keypair=kp)

        try:
            lot_size = await get_lot_size(symbol)
            token_amount = usd_to_token(amount_usd, price, lot_size)

            if float(token_amount) <= 0:
                logger.warning("Grid #%s: token amount is 0 at $%.2f", grid["id"], price)
                return False

            await client.create_market_order(
                symbol=symbol,
                side=side,
                amount=token_amount,
                slippage="1",
            )

            logger.info(
                "Grid #%s: %s %s $%.2f at $%.4f (level $%.4f)",
                grid["id"], side.upper(), symbol, amount_usd, price, grid_level,
            )

            # Notify user
            emoji = "BUY" if side == "buy" else "SELL"
            text = (
                f"<b>Grid {emoji} Filled</b>\n\n"
                f"Grid #{grid['id']} — <b>{symbol}</b>\n"
                f"Level: <code>${grid_level:,.4f}</code>\n"
                f"Price: <code>${price:,.4f}</code>\n"
                f"Size: <code>${amount_usd:,.2f}</code>\n"
                f"Range: <code>${grid['price_low']:,.2f}</code> – "
                f"<code>${grid['price_high']:,.2f}</code>"
            )
            try:
                await bot.send_message(tg_id, text)
            except Exception:
                pass

            return True

        finally:
            await client.close()

    except Exception as e:
        logger.error("Grid #%s: %s order failed: %s", grid["id"], side, e)
        return False


# ---------------------------------------------------------------------------
# Main check loop
# ---------------------------------------------------------------------------

async def _check_grids(bot: Bot):
    """Iterate over all active grid configs and execute orders as needed."""
    grids = await get_active_grids()
    if not grids:
        return

    for grid in grids:
        try:
            symbol = grid["symbol"]
            price = await get_price(symbol)
            if not price:
                continue

            levels = _compute_levels(
                grid["price_low"], grid["price_high"], grid["num_grids"]
            )
            filled = _parse_filled(grid.get("grids_filled"))
            realized_pnl = grid.get("realized_pnl", 0) or 0
            changed = False

            for i, level in enumerate(levels):
                idx = str(i)

                if price <= level and idx not in filled:
                    # Price dropped to or below this level — BUY
                    success = await _execute_grid_order(
                        bot, grid, level, "buy", price,
                    )
                    if success:
                        filled[idx] = {
                            "side": "buy",
                            "price": price,
                            "filled": True,
                        }
                        changed = True

                elif price >= level and idx in filled and filled[idx]["side"] == "buy":
                    # Price rose to or above a level we bought — SELL
                    buy_price = filled[idx]["price"]
                    success = await _execute_grid_order(
                        bot, grid, level, "sell", price,
                    )
                    if success:
                        pnl = grid["amount_per_grid"] * (price - buy_price) / buy_price
                        realized_pnl += pnl
                        del filled[idx]
                        changed = True

                        # Notify PnL
                        try:
                            await bot.send_message(
                                grid["telegram_id"],
                                f"<b>Grid #{grid['id']} PnL</b>\n"
                                f"Cycle profit: <code>${pnl:+,.4f}</code>\n"
                                f"Total realized: <code>${realized_pnl:+,.4f}</code>",
                            )
                        except Exception:
                            pass

            if changed:
                await _save_filled(grid["id"], filled, realized_pnl)

        except Exception as e:
            logger.error("Grid #%s check failed: %s", grid.get("id"), e)


# ---------------------------------------------------------------------------
# Engine lifecycle
# ---------------------------------------------------------------------------

async def start_grid_engine(bot: Bot):
    """Start the grid trading engine background loop."""
    global _running
    _running = True

    await _init_grid_tables()

    logger.info("Grid engine started (check every %ss)", CHECK_INTERVAL)

    while _running:
        try:
            await _check_grids(bot)
        except Exception as e:
            logger.error("Grid engine error: %s", e)
        await asyncio.sleep(CHECK_INTERVAL)


def stop_grid_engine():
    """Signal the grid engine to stop."""
    global _running
    _running = False
    logger.info("Grid engine stopped")
