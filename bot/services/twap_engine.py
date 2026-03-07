"""
TWAP engine — executes time-weighted average price orders by splitting
large orders into equal slices over time.
"""

import asyncio
import logging
import random
from datetime import datetime, timedelta

from aiogram import Bot

from bot.services.pacifica_client import PacificaClient
from bot.services.market_data import get_price, usd_to_token, get_lot_size
from bot.services.wallet_manager import decrypt_private_key
from bot.config import BUILDER_CODE
from database.db import get_db, get_active_twap_orders, update_twap_progress

logger = logging.getLogger(__name__)

_running = False
CHECK_INTERVAL = 10  # seconds


async def start_twap_engine(bot: Bot):
    global _running
    _running = True
    logger.info("TWAP engine started (check every %ss)", CHECK_INTERVAL)

    while _running:
        try:
            await _check_twap_orders(bot)
        except Exception as e:
            logger.error("TWAP engine error: %s", e)
        await asyncio.sleep(CHECK_INTERVAL)


def stop_twap_engine():
    global _running
    _running = False


async def _check_twap_orders(bot: Bot):
    orders = await get_active_twap_orders()
    if not orders:
        return

    now = datetime.utcnow()

    for order in orders:
        next_exec = order.get("next_execution")
        if next_exec:
            try:
                next_dt = datetime.fromisoformat(next_exec)
                if next_dt > now:
                    continue
            except (ValueError, TypeError):
                pass

        if order["slices_executed"] >= order["num_slices"]:
            db = await get_db()
            await db.execute(
                "UPDATE twap_orders SET active = 0 WHERE id = ?", (order["id"],)
            )
            await db.commit()
            continue

        await _execute_twap_slice(bot, order)


async def _execute_twap_slice(bot: Bot, order: dict):
    tg_id = order["telegram_id"]
    symbol = order["symbol"]
    side = order["side"]
    amount_usd = order["amount_per_slice"]
    leverage = order.get("leverage", 1)

    db = await get_db()
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
            price = await get_price(symbol)
            if not price:
                return

            lot_size = await get_lot_size(symbol)
            token_amount = usd_to_token(amount_usd, price, lot_size)

            if float(token_amount) <= 0:
                return

            order_side = "buy" if side == "long" else "sell"

            await client.create_market_order(
                symbol=symbol,
                side=order_side,
                amount=token_amount,
                slippage="1",
            )

            # Update progress
            executed = order["slices_executed"] + 1
            old_avg = order.get("avg_price", 0) or 0
            new_avg = ((old_avg * (executed - 1)) + price) / executed if executed > 0 else price

            if executed >= order["num_slices"]:
                await update_twap_progress(order["id"], executed, new_avg, None)
                text = (
                    f"<b>TWAP Complete!</b>\n\n"
                    f"<b>{symbol}</b> {side.upper()} {leverage}x\n"
                    f"Slices: <code>{executed}/{order['num_slices']}</code>\n"
                    f"Total: <code>${order['total_amount_usd']:,.2f}</code>\n"
                    f"VWAP: <code>${new_avg:,.2f}</code>"
                )
            else:
                interval = order["interval_seconds"]
                # Add randomization if enabled (+-20%)
                if order.get("randomize"):
                    jitter = interval * 0.2
                    interval = int(interval + random.uniform(-jitter, jitter))

                next_exec = (datetime.utcnow() + timedelta(seconds=interval)).isoformat()
                await update_twap_progress(order["id"], executed, new_avg, next_exec)
                remaining = order["num_slices"] - executed
                est_time = remaining * order["interval_seconds"]
                text = (
                    f"<b>TWAP Slice #{executed}</b>\n\n"
                    f"<b>{symbol}</b> {side.upper()} {leverage}x\n"
                    f"Filled: <code>${amount_usd:,.2f}</code> at <code>${price:,.2f}</code>\n"
                    f"Progress: <code>{executed}/{order['num_slices']}</code>\n"
                    f"VWAP: <code>${new_avg:,.2f}</code>\n"
                    f"Remaining: ~{_fmt_duration(est_time)}"
                )

            try:
                await bot.send_message(tg_id, text)
            except Exception:
                pass

        finally:
            await client.close()
    except Exception as e:
        logger.error("TWAP slice failed for %s %s: %s", tg_id, symbol, e)


def _fmt_duration(seconds: int) -> str:
    if seconds < 60:
        return f"{seconds}s"
    if seconds < 3600:
        return f"{seconds // 60}m"
    hours = seconds // 3600
    mins = (seconds % 3600) // 60
    return f"{hours}h{mins}m" if mins else f"{hours}h"
