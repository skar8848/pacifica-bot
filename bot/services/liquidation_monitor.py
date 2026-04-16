"""
Liquidation proximity monitor — alerts users when positions approach liquidation.
"""

import asyncio
import logging

from aiogram import Bot
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from bot.services.pacifica_client import PacificaClient
from bot.services.market_data import get_price
from database.db import get_db

logger = logging.getLogger(__name__)

_running = False
CHECK_INTERVAL = 30  # seconds

# Alert thresholds (% distance from liquidation)
THRESHOLDS = [10, 5, 3]


async def start_liquidation_monitor(bot: Bot):
    global _running
    _running = True
    logger.info("Liquidation monitor started (check every %ss)", CHECK_INTERVAL)

    while _running:
        try:
            await _check_all_positions(bot)
        except Exception as e:
            logger.error("Liquidation monitor error: %s", e)
        await asyncio.sleep(CHECK_INTERVAL)


def stop_liquidation_monitor():
    global _running
    _running = False


# Track which alerts have been sent: {(telegram_id, symbol): last_threshold_sent}
_sent_alerts: dict[tuple[int, str], int] = {}


async def _check_all_positions(bot: Bot):
    db = await get_db()
    async with db.execute(
        "SELECT telegram_id, pacifica_account, agent_wallet_encrypted, username FROM users "
        "WHERE pacifica_account IS NOT NULL"
    ) as cursor:
        users = await cursor.fetchall()

    for row in users:
        tg_id = row[0]
        account = row[1]
        encrypted_key = row[2]
        username = row[3]
        if not account or not encrypted_key:
            continue

        try:
            from bot.services.wallet_manager import decrypt_private_key
            kp = decrypt_private_key(encrypted_key)
            client = PacificaClient(account=account, keypair=kp)

            try:
                positions = await client.get_positions()
                if not positions:
                    continue

                for pos in positions:
                    await _check_position(bot, tg_id, pos, username)
            finally:
                await client.close()
        except Exception as e:
            logger.debug("Liq check failed for %s: %s", account, e)


async def _check_position(bot: Bot, tg_id: int, pos: dict, username: str | None = None):
    symbol = pos.get("symbol", "")
    liq_price = float(pos.get("liquidation_price", 0) or 0)
    entry_price = float(pos.get("entry_price", 0) or 0)
    side = pos.get("side", "")

    if not entry_price or not symbol or not side:
        return

    # Skip absurd liquidation prices (negative, zero, or very far from entry)
    if liq_price <= 0:
        return

    current_price = await get_price(symbol)
    if not current_price:
        return

    is_long = side.lower() in ("long", "buy", "bid")

    # Calculate distance to liquidation as percentage
    if is_long:
        if current_price <= liq_price:
            distance_pct = 0
        else:
            distance_pct = ((current_price - liq_price) / current_price) * 100
    else:
        if current_price >= liq_price:
            distance_pct = 0
        else:
            distance_pct = ((liq_price - current_price) / current_price) * 100

    # Check thresholds (highest first)
    key = (tg_id, symbol)
    last_sent = _sent_alerts.get(key, 100)  # default: no alert sent

    for threshold in THRESHOLDS:
        if distance_pct <= threshold and last_sent > threshold:
            _sent_alerts[key] = threshold
            await _send_liq_alert(bot, tg_id, symbol, side, current_price, liq_price, distance_pct, username)
            break

    # Reset if price moved away from liquidation
    if distance_pct > THRESHOLDS[0] + 5:
        _sent_alerts.pop(key, None)


async def _send_liq_alert(
    bot: Bot, tg_id: int, symbol: str, side: str,
    current_price: float, liq_price: float, distance_pct: float,
    username: str | None = None,
):
    severity = "CRITICAL" if distance_pct <= 3 else "WARNING"
    emoji = "\U0001f6a8" if distance_pct <= 3 else "\u26a0\ufe0f"

    text = (
        f"<b>{emoji} Liquidation {severity}</b>\n\n"
        f"<b>{symbol}</b> {side.upper()}\n"
        f"Current: <code>${current_price:,.2f}</code>\n"
        f"Liq Price: <code>${liq_price:,.2f}</code>\n"
        f"Distance: <b>{distance_pct:.1f}%</b>\n\n"
    )

    if distance_pct <= 3:
        text += "Your position is extremely close to liquidation!"
    elif distance_pct <= 5:
        text += "Consider adding margin or reducing position size."
    else:
        text += "Monitor your position closely."

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="Close Position", callback_data=f"close_pos:{symbol}"),
            InlineKeyboardButton(text="View Positions", callback_data="nav:positions"),
        ],
    ])

    try:
        await bot.send_message(tg_id, text, reply_markup=kb)
    except Exception as e:
        logger.debug("Failed to send liq alert to %s: %s", tg_id, e)

    # Post to group feed
    if username and distance_pct <= 5:
        from bot.services.group_feed import post_liquidation_alert
        await post_liquidation_alert(
            bot, username, symbol, side, distance_pct, liq_price, current_price,
        )
