"""
Managed Order Types — Bracket, Conditional, Pegged.
These are virtual orders managed by the bot (not native exchange orders).
Evaluated every CHECK_INTERVAL seconds.

BracketOrder: entry + take_profit + stop_loss as state machine
ConditionalOrder: triggers child order when price crosses threshold
PeggedOrder: tracks mid price with offset, re-prices each cycle
"""

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field, asdict

from aiogram import Bot

from bot.services.pacifica_client import PacificaClient
from bot.services.market_data import get_price, usd_to_token, get_lot_size
from bot.services.wallet_manager import decrypt_private_key
from database.db import get_db

logger = logging.getLogger(__name__)

_running = False
CHECK_INTERVAL = 10  # seconds

# How many bps of price movement trigger a pegged order reprice
PEGGED_REPRICE_THRESHOLD_BPS = 5


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------

@dataclass
class BracketOrder:
    telegram_id: int
    symbol: str
    side: str             # long/short
    size_usd: float
    entry_price: float | None  # None = market entry immediately
    take_profit: float
    stop_loss: float
    state: str = "pending_entry"  # pending_entry | active | done
    entry_order_id: str = ""
    db_id: int = 0


@dataclass
class ConditionalOrder:
    telegram_id: int
    symbol: str
    condition: str        # "above" or "below"
    trigger_price: float
    action_side: str
    action_size_usd: float
    action_type: str      # "market" or "limit"
    action_price: float | None = None  # for limit
    expiry_minutes: int = 0            # 0 = no expiry
    state: str = "waiting"  # waiting | triggered | expired | cancelled
    created_at: float = field(default_factory=time.time)
    db_id: int = 0


@dataclass
class PeggedOrder:
    telegram_id: int
    symbol: str
    side: str
    size_usd: float
    offset_bps: float           # offset from mid in basis points
    current_order_id: str = ""
    last_order_price: float = 0.0
    state: str = "active"       # active | done | cancelled
    db_id: int = 0


# ---------------------------------------------------------------------------
# DB setup
# ---------------------------------------------------------------------------

async def _init_managed_order_tables():
    """Create the managed_orders table if it doesn't exist."""
    db = await get_db()
    await db.executescript("""
        CREATE TABLE IF NOT EXISTS managed_orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            telegram_id INTEGER NOT NULL,
            order_type TEXT NOT NULL,
            symbol TEXT NOT NULL,
            config TEXT NOT NULL,
            state TEXT NOT NULL DEFAULT 'pending',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_managed_orders_state
            ON managed_orders(state);
        CREATE INDEX IF NOT EXISTS idx_managed_orders_tg
            ON managed_orders(telegram_id);
    """)
    await db.commit()
    logger.info("managed_orders table ready")


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

async def _insert_managed_order(
    telegram_id: int, order_type: str, symbol: str, config: dict, state: str,
) -> int:
    db = await get_db()
    cursor = await db.execute(
        """INSERT INTO managed_orders (telegram_id, order_type, symbol, config, state)
           VALUES (?, ?, ?, ?, ?)""",
        (telegram_id, order_type, symbol, json.dumps(config), state),
    )
    await db.commit()
    return cursor.lastrowid  # type: ignore


async def _update_managed_order_state(order_id: int, state: str, config: dict | None = None):
    db = await get_db()
    if config is not None:
        await db.execute(
            """UPDATE managed_orders
               SET state = ?, config = ?, updated_at = CURRENT_TIMESTAMP
               WHERE id = ?""",
            (state, json.dumps(config), order_id),
        )
    else:
        await db.execute(
            """UPDATE managed_orders
               SET state = ?, updated_at = CURRENT_TIMESTAMP
               WHERE id = ?""",
            (state, order_id),
        )
    await db.commit()


async def _load_active_managed_orders() -> list[dict]:
    db = await get_db()
    async with db.execute(
        "SELECT * FROM managed_orders WHERE state NOT IN ('done', 'cancelled', 'expired', 'triggered')"
    ) as cursor:
        return [dict(r) for r in await cursor.fetchall()]


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
# Bracket order processing
# ---------------------------------------------------------------------------

async def _process_bracket(bot: Bot, row: dict):
    """Evaluate one bracket order row from DB."""
    cfg = json.loads(row["config"])
    order_id = row["id"]
    tg_id = row["telegram_id"]
    symbol = row["symbol"]
    state = row["state"]

    current_price = await get_price(symbol)
    if not current_price:
        return

    client = await _get_user_client(tg_id)
    if not client:
        logger.warning("Bracket #%d: no wallet for user %s", order_id, tg_id)
        return

    try:
        if state == "pending_entry":
            entry_price = cfg.get("entry_price")

            # Market entry (no entry_price) — execute immediately
            if entry_price is None:
                await _bracket_place_entry(bot, client, order_id, tg_id, symbol, cfg, current_price, "market")
                return

            # Limit entry: check if price reached the entry
            side = cfg["side"]
            if side in ("long", "buy"):
                triggered = current_price <= entry_price
            else:
                triggered = current_price >= entry_price

            if triggered:
                await _bracket_place_entry(bot, client, order_id, tg_id, symbol, cfg, current_price, "limit_triggered")

        elif state == "active":
            side = cfg["side"]
            tp = cfg["take_profit"]
            sl = cfg["stop_loss"]

            # Check TP
            if side in ("long", "buy"):
                tp_hit = current_price >= tp
                sl_hit = current_price <= sl
            else:
                tp_hit = current_price <= tp
                sl_hit = current_price >= sl

            if tp_hit:
                await _bracket_close(bot, client, order_id, tg_id, symbol, cfg, current_price, "take_profit")
            elif sl_hit:
                await _bracket_close(bot, client, order_id, tg_id, symbol, cfg, current_price, "stop_loss")

    except Exception as e:
        logger.error("Bracket #%d processing error: %s", order_id, e)
    finally:
        await client.close()


async def _bracket_place_entry(
    bot: Bot, client: PacificaClient, order_id: int, tg_id: int,
    symbol: str, cfg: dict, current_price: float, entry_type: str,
):
    """Place the entry market order and move bracket to 'active'."""
    side = cfg["side"]
    size_usd = cfg["size_usd"]
    lot_size = await get_lot_size(symbol)
    token_amount = usd_to_token(size_usd, current_price, lot_size)

    if float(token_amount) <= 0:
        logger.warning("Bracket #%d: token amount is 0, skipping entry", order_id)
        return

    order_side = "buy" if side in ("long", "buy") else "sell"
    result = await client.create_market_order(
        symbol=symbol,
        side=order_side,
        amount=token_amount,
    )
    entry_order_id = result.get("order_id", "") if isinstance(result, dict) else ""

    cfg["entry_order_id"] = entry_order_id
    cfg["actual_entry_price"] = current_price
    await _update_managed_order_state(order_id, "active", cfg)

    logger.info(
        "Bracket #%d entry placed: %s %s %s at $%.4f (type=%s)",
        order_id, order_side.upper(), token_amount, symbol, current_price, entry_type,
    )
    try:
        await bot.send_message(
            tg_id,
            f"<b>Bracket #{order_id} Entered</b>\n\n"
            f"<b>{symbol}</b> {side.upper()} position opened\n"
            f"Entry: <code>${current_price:,.4f}</code>\n"
            f"Take Profit: <code>${cfg['take_profit']:,.4f}</code>\n"
            f"Stop Loss: <code>${cfg['stop_loss']:,.4f}</code>\n"
            f"Size: <code>${size_usd:,.2f}</code>",
        )
    except Exception:
        pass


async def _bracket_close(
    bot: Bot, client: PacificaClient, order_id: int, tg_id: int,
    symbol: str, cfg: dict, current_price: float, reason: str,
):
    """Close a bracket position (TP or SL hit)."""
    side = cfg["side"]
    size_usd = cfg["size_usd"]
    lot_size = await get_lot_size(symbol)
    token_amount = usd_to_token(size_usd, current_price, lot_size)

    if float(token_amount) <= 0:
        token_amount = "0"

    close_side = "sell" if side in ("long", "buy") else "buy"

    try:
        await client.create_market_order(
            symbol=symbol,
            side=close_side,
            amount=token_amount,
            reduce_only=True,
        )
    except Exception as e:
        logger.error("Bracket #%d close order failed: %s", order_id, e)

    await _update_managed_order_state(order_id, "done", cfg)

    entry = cfg.get("actual_entry_price") or cfg.get("entry_price") or current_price
    if side in ("long", "buy"):
        pnl = (current_price - entry) * float(token_amount)
    else:
        pnl = (entry - current_price) * float(token_amount)

    logger.info(
        "Bracket #%d closed (%s): %s at $%.4f, PnL ~$%.2f, user %s",
        order_id, reason, symbol, current_price, pnl, tg_id,
    )

    reason_label = "Take Profit" if reason == "take_profit" else "Stop Loss"
    pnl_emoji = "\U0001f7e2" if pnl >= 0 else "\U0001f534"
    try:
        await bot.send_message(
            tg_id,
            f"<b>Bracket #{order_id} — {reason_label} Hit</b>\n\n"
            f"{pnl_emoji} <b>{symbol}</b> {side.upper()} closed\n"
            f"Entry: <code>${entry:,.4f}</code>\n"
            f"Exit: <code>${current_price:,.4f}</code>\n"
            f"Est. PnL: <code>${pnl:+,.2f}</code>",
        )
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Conditional order processing
# ---------------------------------------------------------------------------

async def _process_conditional(bot: Bot, row: dict):
    """Evaluate one conditional order row from DB."""
    cfg = json.loads(row["config"])
    order_id = row["id"]
    tg_id = row["telegram_id"]
    symbol = row["symbol"]

    # Check expiry
    expiry_minutes = cfg.get("expiry_minutes", 0)
    if expiry_minutes > 0:
        created_at = cfg.get("created_at", time.time())
        elapsed_min = (time.time() - created_at) / 60
        if elapsed_min >= expiry_minutes:
            await _update_managed_order_state(order_id, "expired")
            logger.info("Conditional #%d expired after %.0f min", order_id, elapsed_min)
            try:
                await bot.send_message(
                    tg_id,
                    f"<b>Conditional #{order_id} Expired</b>\n\n"
                    f"<b>{symbol}</b> — trigger price <code>${cfg['trigger_price']:,.4f}</code>\n"
                    f"Never triggered within {expiry_minutes}min window.",
                )
            except Exception:
                pass
            return

    current_price = await get_price(symbol)
    if not current_price:
        return

    condition = cfg["condition"]
    trigger_price = cfg["trigger_price"]

    triggered = (
        (condition == "above" and current_price >= trigger_price) or
        (condition == "below" and current_price <= trigger_price)
    )

    if not triggered:
        return

    # Execute the action
    client = await _get_user_client(tg_id)
    if not client:
        logger.warning("Conditional #%d: no wallet for user %s", order_id, tg_id)
        return

    try:
        action_side = cfg["action_side"]
        action_size_usd = cfg["action_size_usd"]
        action_type = cfg.get("action_type", "market")
        action_price = cfg.get("action_price")

        lot_size = await get_lot_size(symbol)
        token_amount = usd_to_token(action_size_usd, current_price, lot_size)

        if float(token_amount) <= 0:
            logger.warning("Conditional #%d: token amount is 0", order_id)
            return

        if action_type == "limit" and action_price:
            result = await client.create_limit_order(
                symbol=symbol,
                side=action_side,
                amount=token_amount,
                price=str(action_price),
            )
        else:
            result = await client.create_market_order(
                symbol=symbol,
                side=action_side,
                amount=token_amount,
            )

        await _update_managed_order_state(order_id, "triggered", cfg)

        logger.info(
            "Conditional #%d triggered: %s %s %s %s at $%.4f, user %s",
            order_id, action_type.upper(), action_side.upper(), token_amount,
            symbol, current_price, tg_id,
        )

        condition_label = f">= ${trigger_price:,.4f}" if condition == "above" else f"<= ${trigger_price:,.4f}"
        try:
            await bot.send_message(
                tg_id,
                f"<b>Conditional #{order_id} Triggered</b>\n\n"
                f"<b>{symbol}</b> price {condition_label}\n"
                f"Action: {action_type.upper()} {action_side.upper()}\n"
                f"Size: <code>{token_amount}</code> (${action_size_usd:,.2f})\n"
                f"Executed at: <code>${current_price:,.4f}</code>",
            )
        except Exception:
            pass

    except Exception as e:
        logger.error("Conditional #%d execution failed: %s", order_id, e)
    finally:
        await client.close()


# ---------------------------------------------------------------------------
# Pegged order processing
# ---------------------------------------------------------------------------

async def _process_pegged(bot: Bot, row: dict):
    """Evaluate one pegged order — reprice if mid moved more than threshold."""
    cfg = json.loads(row["config"])
    order_id = row["id"]
    tg_id = row["telegram_id"]
    symbol = row["symbol"]

    current_price = await get_price(symbol)
    if not current_price:
        return

    side = cfg["side"]
    offset_bps = cfg["offset_bps"]
    size_usd = cfg["size_usd"]
    current_order_id = cfg.get("current_order_id", "")
    last_order_price = cfg.get("last_order_price", 0.0)

    # Compute the target pegged price: mid ± offset
    if side in ("buy", "long"):
        target_price = current_price * (1 - offset_bps / 10_000)
    else:
        target_price = current_price * (1 + offset_bps / 10_000)

    # Check if price moved enough to warrant repricing
    if last_order_price > 0:
        drift_bps = abs(target_price - last_order_price) / last_order_price * 10_000
        if drift_bps < PEGGED_REPRICE_THRESHOLD_BPS:
            return  # no reprice needed

    client = await _get_user_client(tg_id)
    if not client:
        logger.warning("Pegged #%d: no wallet for user %s", order_id, tg_id)
        return

    try:
        lot_size = await get_lot_size(symbol)
        token_amount = usd_to_token(size_usd, target_price, lot_size)

        if float(token_amount) <= 0:
            logger.warning("Pegged #%d: token amount is 0 at $%.4f", order_id, target_price)
            return

        # Cancel existing order if present
        if current_order_id:
            try:
                await client.cancel_order(current_order_id, symbol)
                logger.debug("Pegged #%d: cancelled old order %s", order_id, current_order_id)
            except Exception as e:
                logger.debug("Pegged #%d: cancel old order failed (may be filled): %s", order_id, e)

        # Place new limit order at pegged price
        order_side = "buy" if side in ("buy", "long") else "sell"
        result = await client.create_limit_order(
            symbol=symbol,
            side=order_side,
            amount=token_amount,
            price=str(round(target_price, 4)),
        )
        new_order_id = result.get("order_id", "") if isinstance(result, dict) else ""

        cfg["current_order_id"] = new_order_id
        cfg["last_order_price"] = target_price
        await _update_managed_order_state(order_id, "active", cfg)

        logger.info(
            "Pegged #%d repriced: %s %s %s at $%.4f (mid=$%.4f, offset=%.1fbps), user %s",
            order_id, order_side.upper(), token_amount, symbol,
            target_price, current_price, offset_bps, tg_id,
        )

    except Exception as e:
        logger.error("Pegged #%d reprice failed: %s", order_id, e)
    finally:
        await client.close()


# ---------------------------------------------------------------------------
# Main evaluation loop
# ---------------------------------------------------------------------------

async def _evaluate_all_managed_orders(bot: Bot):
    """Load and dispatch all active managed orders."""
    rows = await _load_active_managed_orders()
    if not rows:
        return

    for row in rows:
        try:
            order_type = row["order_type"]
            if order_type == "bracket":
                await _process_bracket(bot, row)
            elif order_type == "conditional":
                await _process_conditional(bot, row)
            elif order_type == "pegged":
                await _process_pegged(bot, row)
            else:
                logger.warning("Unknown managed order type: %s (id=%d)", order_type, row["id"])
        except Exception as e:
            logger.error("Managed order #%d eval error: %s", row.get("id", "?"), e)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def create_bracket(
    telegram_id: int,
    symbol: str,
    side: str,
    size_usd: float,
    entry_price: float | None,
    tp: float,
    sl: float,
) -> int:
    """Create a bracket order. Returns the DB id."""
    await _init_managed_order_tables()
    cfg = {
        "side": side,
        "size_usd": size_usd,
        "entry_price": entry_price,
        "take_profit": tp,
        "stop_loss": sl,
    }
    initial_state = "pending_entry"
    order_id = await _insert_managed_order(telegram_id, "bracket", symbol, cfg, initial_state)
    logger.info(
        "Bracket #%d created: %s %s %s, entry=%s, TP=$%.4f, SL=$%.4f",
        order_id, side.upper(), size_usd, symbol,
        f"${entry_price:,.4f}" if entry_price else "market", tp, sl,
    )
    return order_id


async def create_conditional(
    telegram_id: int,
    symbol: str,
    condition: str,
    trigger: float,
    action_side: str,
    size_usd: float,
    type: str,
    price: float | None,
    expiry: int,
) -> int:
    """Create a conditional order. Returns the DB id."""
    cfg = {
        "condition": condition,
        "trigger_price": trigger,
        "action_side": action_side,
        "action_size_usd": size_usd,
        "action_type": type,
        "action_price": price,
        "expiry_minutes": expiry,
        "created_at": time.time(),
    }
    order_id = await _insert_managed_order(telegram_id, "conditional", symbol, cfg, "waiting")
    logger.info(
        "Conditional #%d created: %s %s trigger=$%.4f, action=%s %s",
        order_id, symbol, condition.upper(), trigger, type.upper(), action_side.upper(),
    )
    return order_id


async def create_pegged(
    telegram_id: int,
    symbol: str,
    side: str,
    size_usd: float,
    offset_bps: float,
) -> int:
    """Create a pegged order. Returns the DB id."""
    cfg = {
        "side": side,
        "size_usd": size_usd,
        "offset_bps": offset_bps,
        "current_order_id": "",
        "last_order_price": 0.0,
    }
    order_id = await _insert_managed_order(telegram_id, "pegged", symbol, cfg, "active")
    logger.info(
        "Pegged #%d created: %s %s %s $%.2f, offset=%.1fbps",
        order_id, side.upper(), symbol, size_usd, offset_bps,
    )
    return order_id


async def cancel_managed_order(order_id: int) -> bool:
    """Cancel a managed order by id. Returns True if found and cancelled."""
    db = await get_db()
    async with db.execute(
        "SELECT * FROM managed_orders WHERE id = ?", (order_id,)
    ) as cursor:
        row = await cursor.fetchone()

    if not row:
        return False

    row = dict(row)
    if row["state"] in ("done", "cancelled", "expired", "triggered"):
        return False

    # For pegged orders, cancel the live exchange order
    if row["order_type"] == "pegged":
        cfg = json.loads(row["config"])
        current_order_id = cfg.get("current_order_id", "")
        if current_order_id:
            client = await _get_user_client(row["telegram_id"])
            if client:
                try:
                    await client.cancel_order(current_order_id, row["symbol"])
                except Exception as e:
                    logger.debug("Pegged #%d: cancel exchange order on cancel: %s", order_id, e)
                finally:
                    await client.close()

    await _update_managed_order_state(order_id, "cancelled")
    logger.info("Managed order #%d cancelled", order_id)
    return True


async def get_managed_orders(telegram_id: int) -> list:
    """Return all managed orders for a user (active and recent history)."""
    await _init_managed_order_tables()
    db = await get_db()
    async with db.execute(
        """SELECT * FROM managed_orders
           WHERE telegram_id = ?
           ORDER BY created_at DESC
           LIMIT 100""",
        (telegram_id,),
    ) as cursor:
        rows = [dict(r) for r in await cursor.fetchall()]

    # Parse config JSON for convenience
    for row in rows:
        try:
            row["config"] = json.loads(row["config"])
        except Exception:
            pass
    return rows


# ---------------------------------------------------------------------------
# Service lifecycle
# ---------------------------------------------------------------------------

async def start_bracket_engine(bot: Bot):
    """Start the managed order evaluation engine."""
    global _running
    _running = True

    await _init_managed_order_tables()

    logger.info("Bracket engine started (check every %ds)", CHECK_INTERVAL)

    while _running:
        try:
            await _evaluate_all_managed_orders(bot)
        except Exception as e:
            logger.error("Bracket engine error: %s", e)
        await asyncio.sleep(CHECK_INTERVAL)


async def stop_bracket_engine():
    global _running
    _running = False
    logger.info("Bracket engine stopped")
