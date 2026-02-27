"""
Price alert monitor — checks active alerts against current prices.
"""

import asyncio
import logging

from aiogram import Bot
from solders.keypair import Keypair

from bot.services.pacifica_client import PacificaClient
from database.db import get_active_alerts, trigger_alert

logger = logging.getLogger(__name__)

_running = False
ALERT_CHECK_INTERVAL = 15  # seconds


async def start_alert_monitor(bot: Bot):
    global _running
    _running = True
    logger.info("Alert monitor started (check every %ss)", ALERT_CHECK_INTERVAL)

    while _running:
        try:
            await _check_alerts(bot)
        except Exception as e:
            logger.error("Alert monitor error: %s", e)
        await asyncio.sleep(ALERT_CHECK_INTERVAL)


def stop_alert_monitor():
    global _running
    _running = False


async def _check_alerts(bot: Bot):
    alerts = await get_active_alerts()
    if not alerts:
        return

    # Group alerts by symbol for efficient price fetching
    symbols: dict[str, list[dict]] = {}
    for a in alerts:
        sym = a["symbol"]
        symbols.setdefault(sym, []).append(a)

    client = PacificaClient(account="public", keypair=Keypair())
    try:
        for sym, sym_alerts in symbols.items():
            try:
                trades = await client.get_trades(sym, limit=1)
                if not trades:
                    continue
                current_price = float(trades[0]["price"])
            except Exception:
                continue

            for alert in sym_alerts:
                target = alert["target_price"]
                direction = alert["direction"]
                triggered = False

                if direction == "above" and current_price >= target:
                    triggered = True
                elif direction == "below" and current_price <= target:
                    triggered = True

                if triggered:
                    await trigger_alert(alert["id"])
                    emoji = "📈" if direction == "above" else "📉"
                    try:
                        await bot.send_message(
                            alert["telegram_id"],
                            f"<b>🔔 Price Alert Triggered!</b>\n\n"
                            f"{emoji} <b>{sym}</b> is now ${current_price:,.2f}\n"
                            f"Target: {direction} ${target:,.2f}\n\n"
                            f"Tap below to trade:",
                            reply_markup=_trade_kb(sym),
                        )
                    except Exception as e:
                        logger.debug("Failed to send alert to %s: %s", alert["telegram_id"], e)
    finally:
        await client.close()


def _trade_kb(symbol: str):
    from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🟢 Long", callback_data=f"trade:long:{symbol}"),
            InlineKeyboardButton(text="🔴 Short", callback_data=f"trade:short:{symbol}"),
        ],
        [InlineKeyboardButton(text="◀️ Menu", callback_data="nav:menu")],
    ])
