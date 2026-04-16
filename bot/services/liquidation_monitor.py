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

# Alert thresholds (% of margin lost)
THRESHOLDS = [75, 85, 90]


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
    entry_price = float(pos.get("entry_price", 0) or 0)
    side = pos.get("side", "")
    amount = abs(float(pos.get("amount", pos.get("size", 0)) or 0))
    margin = float(pos.get("margin", pos.get("initial_margin", 0)) or 0)

    if not entry_price or not symbol or not side or not amount or not margin:
        return

    current_price = await get_price(symbol)
    if not current_price:
        return

    is_long = side.lower() in ("long", "buy", "bid")

    # Calculate unrealized PnL
    if is_long:
        pnl = (current_price - entry_price) * amount
    else:
        pnl = (entry_price - current_price) * amount

    # % of margin lost (0% = breakeven, 100% = total loss)
    loss_pct = (-pnl / margin * 100) if margin > 0 else 0

    if loss_pct < 0:
        loss_pct = 0  # position is profitable, no alert needed

    # Check thresholds (lowest first → alert escalation)
    key = (tg_id, symbol)
    last_sent = _sent_alerts.get(key, 0)

    for threshold in THRESHOLDS:
        if loss_pct >= threshold and last_sent < threshold:
            _sent_alerts[key] = threshold
            await _send_liq_alert(bot, tg_id, symbol, side, current_price, entry_price, loss_pct, margin, pnl, username)
            break

    # Reset if position recovered below first threshold
    if loss_pct < THRESHOLDS[0] - 10:
        _sent_alerts.pop(key, None)


async def _send_liq_alert(
    bot: Bot, tg_id: int, symbol: str, side: str,
    current_price: float, entry_price: float, loss_pct: float,
    margin: float, pnl: float,
    username: str | None = None,
):
    severity = "CRITICAL" if loss_pct >= 90 else "WARNING"
    emoji = "\U0001f6a8" if loss_pct >= 90 else "\u26a0\ufe0f"
    direction = "LONG" if side.lower() in ("long", "buy", "bid") else "SHORT"

    text = (
        f"<b>{emoji} {severity} — {loss_pct:.0f}% margin lost</b>\n\n"
        f"<b>{symbol}</b> {direction}\n"
        f"Entry: <code>${entry_price:,.2f}</code>\n"
        f"Current: <code>${current_price:,.2f}</code>\n"
        f"PnL: <code>${pnl:,.2f}</code>\n"
        f"Margin: <code>${margin:,.2f}</code>\n\n"
    )

    if loss_pct >= 90:
        text += "You are about to lose your entire margin. Close now!"
    elif loss_pct >= 85:
        text += "Your position is in critical danger. Consider closing."
    else:
        text += "Your position is losing significant margin. Monitor closely."

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
    if username and loss_pct >= 85:
        from bot.services.group_feed import post_liquidation_alert
        await post_liquidation_alert(
            bot, username, symbol, side, loss_pct, entry_price, current_price,
        )
