"""
DCA engine — executes scheduled DCA orders.
"""

import asyncio
import logging
from datetime import datetime, timedelta

from aiogram import Bot

from bot.services.pacifica_client import PacificaClient
from bot.services.market_data import get_price, usd_to_token, get_lot_size
from bot.services.wallet_manager import decrypt_private_key
from bot.config import BUILDER_CODE
from database.db import get_db, get_active_dca_configs, update_dca_progress

logger = logging.getLogger(__name__)

_running = False
CHECK_INTERVAL = 15  # seconds


async def start_dca_engine(bot: Bot):
    global _running
    _running = True
    logger.info("DCA engine started (check every %ss)", CHECK_INTERVAL)

    while _running:
        try:
            await _check_dca_orders(bot)
        except Exception as e:
            logger.error("DCA engine error: %s", e)
        await asyncio.sleep(CHECK_INTERVAL)


def stop_dca_engine():
    global _running
    _running = False


async def _check_dca_orders(bot: Bot):
    configs = await get_active_dca_configs()
    if not configs:
        return

    now = datetime.utcnow()

    for config in configs:
        next_exec = config.get("next_execution")
        if next_exec:
            try:
                next_dt = datetime.fromisoformat(next_exec)
                if next_dt > now:
                    continue
            except (ValueError, TypeError):
                pass

        if config["orders_executed"] >= config["orders_total"]:
            db = await get_db()
            await db.execute(
                "UPDATE dca_configs SET active = 0 WHERE id = ?", (config["id"],)
            )
            await db.commit()
            continue

        await _execute_dca_order(bot, config)


async def _execute_dca_order(bot: Bot, config: dict):
    tg_id = config["telegram_id"]
    symbol = config["symbol"]
    side = config["side"]
    amount_usd = config["amount_per_order"]
    leverage = config.get("leverage", 1)

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
            executed = config["orders_executed"] + 1
            old_avg = config.get("avg_entry", 0)
            new_avg = ((old_avg * (executed - 1)) + price) / executed if executed > 0 else price

            if executed >= config["orders_total"]:
                await update_dca_progress(config["id"], executed, new_avg, None)
                text = (
                    f"<b>DCA Complete!</b>\n\n"
                    f"<b>{symbol}</b> {side.upper()} {leverage}x\n"
                    f"Orders: <code>{executed}/{config['orders_total']}</code>\n"
                    f"Total: <code>${config['total_amount_usd']:,.2f}</code>\n"
                    f"Avg Entry: <code>${new_avg:,.2f}</code>"
                )
            else:
                interval = config.get("interval_seconds", 3600)
                next_exec = (datetime.utcnow() + timedelta(seconds=interval)).isoformat()
                await update_dca_progress(config["id"], executed, new_avg, next_exec)
                text = (
                    f"<b>DCA Order #{executed}</b>\n\n"
                    f"<b>{symbol}</b> {side.upper()} {leverage}x\n"
                    f"Bought: <code>${amount_usd:,.2f}</code> at <code>${price:,.2f}</code>\n"
                    f"Progress: <code>{executed}/{config['orders_total']}</code>\n"
                    f"Avg Entry: <code>${new_avg:,.2f}</code>"
                )

            try:
                await bot.send_message(tg_id, text)
            except Exception:
                pass

        finally:
            await client.close()
    except Exception as e:
        logger.error("DCA order failed for %s %s: %s", tg_id, symbol, e)
