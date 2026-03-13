"""
Grid trading engine v2 — places **limit orders** at evenly spaced price levels
within a user-defined range, then monitors fills and replaces them with the
opposite side to capture profit from each completed grid cycle.
"""

import asyncio
import json
import logging

from aiogram import Bot

from bot.services.pacifica_client import PacificaClient
from bot.services.market_data import get_price, usd_to_token, get_lot_size, get_market_info
from bot.services.wallet_manager import decrypt_private_key
from database.db import get_db

logger = logging.getLogger(__name__)

_running = False
CHECK_INTERVAL = 10  # seconds
ORDER_PLACE_DELAY = 0.2  # seconds between placing orders (rate-limit guard)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _price_to_tick_level(price: float, tick_size: str) -> int:
    """Convert a price to the integer tick_level expected by the API."""
    ts = float(tick_size)
    if ts <= 0:
        ts = 1
    return int(round(price / ts))


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
    """Cancel all tracked limit orders for a grid, then deactivate it."""
    db = await get_db()

    # Fetch grid details so we can cancel its open orders
    async with db.execute(
        "SELECT * FROM grid_configs WHERE id = ? AND telegram_id = ?",
        (grid_id, telegram_id),
    ) as cursor:
        row = await cursor.fetchone()

    if not row:
        logger.warning("cancel_grid: grid #%s not found for user %s", grid_id, telegram_id)
        return

    columns = [d[0] for d in cursor.description] if cursor.description else []
    grid = dict(zip(columns, row))
    filled = _parse_filled(grid.get("grids_filled"))
    symbol = grid["symbol"]

    # Cancel every tracked order that is still on the book
    if filled:
        client = await _get_user_client(telegram_id)
        if client:
            try:
                for _idx, info in filled.items():
                    order_id = info.get("order_id")
                    if order_id:
                        try:
                            await client.cancel_order(order_id, symbol)
                            logger.info("Grid #%s: cancelled order %s", grid_id, order_id)
                        except Exception as e:
                            logger.debug("Grid #%s: cancel order %s failed (may already be filled): %s",
                                         grid_id, order_id, e)
                        await asyncio.sleep(ORDER_PLACE_DELAY)
            finally:
                await client.close()

    # Deactivate
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
    """Deserialize grids_filled JSON.

    Returns {str(level_index): {"order_id": ..., "side": ..., "price": ...}}
    If the stored data uses the old v1 format (no "order_id" key), treat it as
    empty so that initial limit orders are placed on the next check.
    """
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return {}

    if not isinstance(data, dict):
        return {}

    # Detect old format: any entry without an "order_id" key means v1 data
    for _key, val in data.items():
        if not isinstance(val, dict) or "order_id" not in val:
            logger.info("Detected old grid format — resetting to empty for limit-order placement")
            return {}

    return data


async def _save_filled(grid_id: int, filled: dict, realized_pnl: float):
    """Persist updated grids_filled and realized_pnl."""
    db = await get_db()
    await db.execute(
        "UPDATE grid_configs SET grids_filled = ?, realized_pnl = ? WHERE id = ?",
        (json.dumps(filled), realized_pnl, grid_id),
    )
    await db.commit()


# ---------------------------------------------------------------------------
# User client helper
# ---------------------------------------------------------------------------

async def _get_user_client(telegram_id: int) -> PacificaClient | None:
    """Build a PacificaClient for the given Telegram user, or None."""
    db = await get_db()
    async with db.execute(
        "SELECT pacifica_account, agent_wallet_encrypted FROM users WHERE telegram_id = ?",
        (telegram_id,),
    ) as cursor:
        user = await cursor.fetchone()

    if not user or not user[0] or not user[1]:
        return None

    account = user[0]
    encrypted_key = user[1]
    kp = decrypt_private_key(encrypted_key)
    return PacificaClient(account=account, keypair=kp)


async def _get_user_account(telegram_id: int) -> str | None:
    """Return the Pacifica account address for a user, or None."""
    db = await get_db()
    async with db.execute(
        "SELECT pacifica_account FROM users WHERE telegram_id = ?",
        (telegram_id,),
    ) as cursor:
        row = await cursor.fetchone()
    return row[0] if row else None


# ---------------------------------------------------------------------------
# Initial order placement (new grid or migrated from v1)
# ---------------------------------------------------------------------------

async def _place_initial_orders(bot: Bot, grid: dict, client: PacificaClient):
    """Place buy limits below current price and sell limits above for a grid."""
    grid_id = grid["id"]
    symbol = grid["symbol"]
    amount_usd = grid["amount_per_grid"]

    price = await get_price(symbol)
    if not price:
        logger.warning("Grid #%s: cannot get price for %s — skipping initial placement", grid_id, symbol)
        return

    _, tick_size, lot_size = await get_market_info(symbol)
    token_amount = usd_to_token(amount_usd, price, lot_size)
    if float(token_amount) <= 0:
        logger.warning("Grid #%s: token amount is 0 at $%.2f", grid_id, price)
        return

    levels = _compute_levels(grid["price_low"], grid["price_high"], grid["num_grids"])
    filled: dict[str, dict] = {}

    for i, level in enumerate(levels):
        idx = str(i)

        if level < price:
            side = "buy"
        elif level > price:
            side = "sell"
        else:
            # Level is at current price — skip (ambiguous)
            continue

        try:
            # Recalculate token amount for this specific level price
            level_token_amount = usd_to_token(amount_usd, level, lot_size)
            if float(level_token_amount) <= 0:
                continue

            result = await client.create_limit_order(
                symbol=symbol,
                side=side,
                amount=level_token_amount,
                price=str(level),
            )

            order_id = result.get("order_id") if isinstance(result, dict) else None
            if not order_id:
                logger.warning("Grid #%s: limit order at level %d returned no order_id", grid_id, i)
                continue

            filled[idx] = {
                "order_id": order_id,
                "side": side,
                "price": level,
            }
            logger.info(
                "Grid #%s: placed %s limit at $%.4f (tick %d), order %s",
                grid_id, side.upper(), level, tick_level, order_id,
            )
        except Exception as e:
            logger.error("Grid #%s: failed to place %s limit at $%.4f: %s", grid_id, side, level, e)

        await asyncio.sleep(ORDER_PLACE_DELAY)

    # Persist tracking data
    realized_pnl = grid.get("realized_pnl", 0) or 0
    await _save_filled(grid_id, filled, realized_pnl)

    # Notify user
    buy_count = sum(1 for v in filled.values() if v["side"] == "buy")
    sell_count = sum(1 for v in filled.values() if v["side"] == "sell")
    try:
        await bot.send_message(
            grid["telegram_id"],
            f"<b>Grid #{grid_id} Initialized</b>\n\n"
            f"Symbol: <b>{symbol}</b>\n"
            f"Current price: <code>${price:,.4f}</code>\n"
            f"Buy orders: <code>{buy_count}</code>\n"
            f"Sell orders: <code>{sell_count}</code>\n"
            f"Range: <code>${grid['price_low']:,.2f}</code> – "
            f"<code>${grid['price_high']:,.2f}</code>",
        )
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Fill detection & replacement
# ---------------------------------------------------------------------------

async def _handle_fills(
    bot: Bot,
    grid: dict,
    filled: dict,
    open_order_ids: set[str],
    client: PacificaClient,
) -> tuple[dict, float, bool]:
    """Check which tracked orders have been filled and place replacement orders.

    Returns (updated_filled, realized_pnl_delta, changed).
    """
    grid_id = grid["id"]
    symbol = grid["symbol"]
    amount_usd = grid["amount_per_grid"]
    realized_pnl = grid.get("realized_pnl", 0) or 0
    changed = False

    levels = _compute_levels(grid["price_low"], grid["price_high"], grid["num_grids"])
    step = (grid["price_high"] - grid["price_low"]) / grid["num_grids"]

    _, tick_size, lot_size = await get_market_info(symbol)

    # Collect indices whose orders have been filled (no longer in open orders)
    filled_indices = []
    for idx, info in list(filled.items()):
        order_id = info.get("order_id")
        if order_id and order_id not in open_order_ids:
            filled_indices.append(idx)

    for idx in filled_indices:
        info = filled[idx]
        side = info["side"]
        fill_price = info["price"]
        level_i = int(idx)

        # Determine replacement order
        if side == "buy":
            # Buy was filled -> place sell one step above
            new_level_i = level_i + 1
            new_side = "sell"
        else:
            # Sell was filled -> place buy one step below
            new_level_i = level_i - 1
            new_side = "buy"

            # PnL: sell filled means we completed a buy-sell cycle
            pnl = amount_usd * step / fill_price
            realized_pnl += pnl

            try:
                await bot.send_message(
                    grid["telegram_id"],
                    f"<b>Grid #{grid_id} PnL</b>\n"
                    f"Cycle profit: <code>${pnl:+,.4f}</code>\n"
                    f"Total realized: <code>${realized_pnl:+,.4f}</code>",
                )
            except Exception:
                pass

        # Notify user of the fill
        emoji = "BUY" if side == "buy" else "SELL"
        try:
            await bot.send_message(
                grid["telegram_id"],
                f"<b>Grid {emoji} Filled</b>\n\n"
                f"Grid #{grid_id} — <b>{symbol}</b>\n"
                f"Level: <code>${fill_price:,.4f}</code>\n"
                f"Size: <code>${amount_usd:,.2f}</code>\n"
                f"Range: <code>${grid['price_low']:,.2f}</code> – "
                f"<code>${grid['price_high']:,.2f}</code>",
            )
        except Exception:
            pass

        # Remove the filled entry
        del filled[idx]
        changed = True

        # Place replacement if within range
        if 0 <= new_level_i <= grid["num_grids"]:
            new_idx = str(new_level_i)
            # Only place if that level isn't already tracked
            if new_idx not in filled:
                new_price = levels[new_level_i]

                try:
                    new_token_amount = usd_to_token(amount_usd, new_price, lot_size)
                    if float(new_token_amount) <= 0:
                        continue

                    result = await client.create_limit_order(
                        symbol=symbol,
                        side=new_side,
                        amount=new_token_amount,
                        price=str(new_price),
                    )

                    order_id = result.get("order_id") if isinstance(result, dict) else None
                    if order_id:
                        filled[new_idx] = {
                            "order_id": order_id,
                            "side": new_side,
                            "price": new_price,
                        }
                        logger.info(
                            "Grid #%s: replacement %s limit at $%.4f (tick %d), order %s",
                            grid_id, new_side.upper(), new_price, tick_level, order_id,
                        )
                except Exception as e:
                    logger.error("Grid #%s: failed to place replacement %s at $%.4f: %s",
                                 grid_id, new_side, new_price, e)

                await asyncio.sleep(ORDER_PLACE_DELAY)

    return filled, realized_pnl, changed


# ---------------------------------------------------------------------------
# Main check loop
# ---------------------------------------------------------------------------

async def _check_grids(bot: Bot):
    """Iterate over all active grid configs, detect fills, place replacements."""
    grids = await get_active_grids()
    if not grids:
        return

    # Group grids by telegram_id so we fetch open orders once per user
    user_grids: dict[int, list[dict]] = {}
    for grid in grids:
        tg_id = grid["telegram_id"]
        user_grids.setdefault(tg_id, []).append(grid)

    for tg_id, grids_for_user in user_grids.items():
        client = None
        try:
            client = await _get_user_client(tg_id)
            if not client:
                logger.warning("User %s has no wallet configured — skipping %d grids",
                               tg_id, len(grids_for_user))
                continue

            # --- Handle grids that need initial order placement first ---
            needs_initial = [g for g in grids_for_user if not _parse_filled(g.get("grids_filled"))]
            for grid in needs_initial:
                try:
                    await _place_initial_orders(bot, grid, client)
                except Exception as e:
                    logger.error("Grid #%s: initial placement failed: %s", grid["id"], e)

            # Refresh grids after initial placement (to get updated grids_filled)
            grids_for_user = [g for g in await get_active_grids(tg_id)]
            already_initialized = [g for g in grids_for_user if _parse_filled(g.get("grids_filled"))]

            if not already_initialized:
                continue

            # Fetch open orders once for this user
            account = await _get_user_account(tg_id)
            if not account:
                continue

            try:
                open_orders = await client.get_open_orders(account)
            except Exception as e:
                logger.error("User %s: failed to fetch open orders: %s", tg_id, e)
                continue

            open_order_ids = set()
            if isinstance(open_orders, list):
                for o in open_orders:
                    oid = o.get("order_id")
                    if oid:
                        open_order_ids.add(oid)

            # --- Check each grid for fills ---
            for grid in already_initialized:
                try:
                    filled = _parse_filled(grid.get("grids_filled"))
                    if not filled:
                        continue

                    filled, realized_pnl, changed = await _handle_fills(
                        bot, grid, filled, open_order_ids, client,
                    )

                    if changed:
                        await _save_filled(grid["id"], filled, realized_pnl)

                except Exception as e:
                    logger.error("Grid #%s check failed: %s", grid.get("id"), e)

        except Exception as e:
            logger.error("Error processing grids for user %s: %s", tg_id, e)

        finally:
            if client:
                await client.close()


# ---------------------------------------------------------------------------
# Engine lifecycle
# ---------------------------------------------------------------------------

async def start_grid_engine(bot: Bot):
    """Start the grid trading engine background loop."""
    global _running
    _running = True

    await _init_grid_tables()

    logger.info("Grid engine v2 started (limit orders, check every %ss)", CHECK_INTERVAL)

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
