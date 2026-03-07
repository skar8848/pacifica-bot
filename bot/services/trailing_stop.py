"""
Trailing stop loss service — dynamically follows price and closes when reversed.
"""

import asyncio
import logging

from aiogram import Bot
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from bot.services.pacifica_client import PacificaClient
from bot.services.market_data import get_price, usd_to_token, get_lot_size
from bot.services.wallet_manager import decrypt_private_key
from database.db import get_db

logger = logging.getLogger(__name__)

_running = False
CHECK_INTERVAL = 5  # seconds — needs to be fast for trailing stops


async def start_trailing_stop_service(bot: Bot):
    global _running
    _running = True
    logger.info("Trailing stop service started (check every %ss)", CHECK_INTERVAL)

    while _running:
        try:
            await _check_trailing_stops(bot)
        except Exception as e:
            logger.error("Trailing stop error: %s", e)
        await asyncio.sleep(CHECK_INTERVAL)


def stop_trailing_stop_service():
    global _running
    _running = False


async def _check_trailing_stops(bot: Bot):
    db = await get_db()
    async with db.execute(
        "SELECT * FROM trailing_stops WHERE active = 1"
    ) as cursor:
        stops = [dict(r) for r in await cursor.fetchall()]

    if not stops:
        return

    for stop in stops:
        try:
            await _process_trailing_stop(bot, stop)
        except Exception as e:
            logger.debug("Trailing stop check failed for %s: %s", stop.get("symbol"), e)


async def _process_trailing_stop(bot: Bot, stop: dict):
    symbol = stop["symbol"]
    tg_id = stop["telegram_id"]
    side = stop["side"]
    trail_pct = stop["trail_percent"]
    peak_price = stop["peak_price"]
    callback_price = stop["callback_price"]

    current_price = await get_price(symbol)
    if not current_price:
        return

    db = await get_db()
    triggered = False

    if side.lower() in ("long", "buy"):
        # Long: track highest price, trigger when drops by trail_pct
        if current_price > peak_price:
            new_peak = current_price
            new_callback = current_price * (1 - trail_pct / 100)
            await db.execute(
                "UPDATE trailing_stops SET peak_price = ?, callback_price = ? WHERE id = ?",
                (new_peak, new_callback, stop["id"]),
            )
            await db.commit()
        elif current_price <= callback_price:
            triggered = True
    else:
        # Short: track lowest price, trigger when rises by trail_pct
        if current_price < peak_price:
            new_peak = current_price
            new_callback = current_price * (1 + trail_pct / 100)
            await db.execute(
                "UPDATE trailing_stops SET peak_price = ?, callback_price = ? WHERE id = ?",
                (new_peak, new_callback, stop["id"]),
            )
            await db.commit()
        elif current_price >= callback_price:
            triggered = True

    if triggered:
        await _execute_trailing_close(bot, stop, current_price)


async def _execute_trailing_close(bot: Bot, stop: dict, trigger_price: float):
    tg_id = stop["telegram_id"]
    symbol = stop["symbol"]
    db = await get_db()

    # Mark as triggered
    await db.execute(
        "UPDATE trailing_stops SET active = 0, triggered_at = CURRENT_TIMESTAMP WHERE id = ?",
        (stop["id"],),
    )
    await db.commit()

    # Get user credentials to close position
    async with db.execute(
        "SELECT pacifica_account, agent_wallet_encrypted FROM users WHERE telegram_id = ?",
        (tg_id,),
    ) as cursor:
        user = await cursor.fetchone()

    if not user or not user[0] or not user[1]:
        return

    account = user[0]
    encrypted_key = user[1]

    try:
        kp = decrypt_private_key(encrypted_key)
        client = PacificaClient(account=account, keypair=kp)

        try:
            # Get current position to determine close side & size
            positions = await client.get_positions()
            pos = next((p for p in (positions or []) if p.get("symbol") == symbol), None)
            if not pos:
                return

            close_side = "sell" if stop["side"].lower() in ("long", "buy") else "buy"
            amount = str(pos.get("amount", "0"))

            from bot.config import BUILDER_CODE, BUILDER_FEE_RATE
            result = await client.create_market_order(
                symbol=symbol,
                side=close_side,
                amount=amount,
                reduce_only=True,
                slippage="1",
                builder_code=BUILDER_CODE,
                builder_fee_rate=BUILDER_FEE_RATE,
            )

            entry = float(stop.get("entry_price", 0))
            pnl = (trigger_price - entry) * float(amount) if stop["side"].lower() in ("long", "buy") \
                else (entry - trigger_price) * float(amount)
            pnl_pct = ((trigger_price - entry) / entry * 100) if stop["side"].lower() in ("long", "buy") \
                else ((entry - trigger_price) / entry * 100)

            emoji = "\U0001f7e2" if pnl >= 0 else "\U0001f534"
            text = (
                f"<b>Trailing Stop Triggered!</b>\n\n"
                f"{emoji} <b>{symbol}</b> {stop['side'].upper()} closed\n"
                f"Entry: <code>${entry:,.2f}</code>\n"
                f"Exit: <code>${trigger_price:,.2f}</code>\n"
                f"Trail: <code>{stop['trail_percent']}%</code>\n"
                f"PnL: <code>${pnl:,.2f}</code> ({pnl_pct:+.2f}%)\n"
            )

            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="View Positions", callback_data="nav:positions")],
            ])

            await bot.send_message(tg_id, text, reply_markup=kb)
        finally:
            await client.close()
    except Exception as e:
        logger.error("Trailing stop close failed for %s %s: %s", tg_id, symbol, e)
