"""
Funding rate monitor — tracks funding rates and alerts users.
"""

import asyncio
import logging

from aiogram import Bot
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from bot.services.pacifica_client import PacificaClient
from database.db import get_db

logger = logging.getLogger(__name__)

_running = False
CHECK_INTERVAL = 60  # seconds

# Alert threshold: absolute funding rate above this triggers alert
FUNDING_ALERT_THRESHOLD = 0.01  # 1% per interval


async def start_funding_monitor(bot: Bot):
    global _running
    _running = True
    logger.info("Funding monitor started (check every %ss)", CHECK_INTERVAL)

    while _running:
        try:
            await _check_funding_alerts(bot)
        except Exception as e:
            logger.error("Funding monitor error: %s", e)
        await asyncio.sleep(CHECK_INTERVAL)


def stop_funding_monitor():
    global _running
    _running = False


async def get_all_funding_rates() -> list[dict]:
    """Fetch current funding rates for all markets."""
    from solders.keypair import Keypair as _Kp
    client = PacificaClient(account="public", keypair=_Kp())
    try:
        prices = await client.get_prices()
        if not isinstance(prices, list):
            return []

        rates = []
        for p in prices:
            symbol = p.get("symbol", "")
            funding = p.get("funding", "0")
            next_funding = p.get("next_funding", "0")
            if symbol:
                rates.append({
                    "symbol": symbol,
                    "funding_rate": float(funding),
                    "next_funding_rate": float(next_funding),
                    "mark_price": float(p.get("mark", p.get("mid", 0))),
                    "open_interest": float(p.get("open_interest", 0)),
                })
        return sorted(rates, key=lambda x: abs(x["funding_rate"]), reverse=True)
    finally:
        await client.close()


async def _check_funding_alerts(bot: Bot):
    """Check if any user's open position has extreme funding."""
    db = await get_db()

    # Get funding alert subscribers
    async with db.execute(
        "SELECT telegram_id, pacifica_account, agent_wallet_encrypted, settings "
        "FROM users WHERE pacifica_account IS NOT NULL"
    ) as cursor:
        users = await cursor.fetchall()

    if not users:
        return

    # Fetch funding rates once
    rates = await get_all_funding_rates()
    rate_map = {r["symbol"]: r for r in rates}

    # Post extreme spikes to group feed
    for r in rates:
        if abs(r["funding_rate"]) >= FUNDING_ALERT_THRESHOLD * 2:
            from bot.services.group_feed import post_funding_spike
            await post_funding_spike(bot, r["symbol"], r["funding_rate"])

    for row in users:
        tg_id = row[0]
        account = row[1]
        encrypted_key = row[2]
        settings_raw = row[3] or "{}"

        import json
        settings = json.loads(settings_raw)
        if not settings.get("funding_alerts", True):
            continue

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
                    symbol = pos.get("symbol", "")
                    rate_info = rate_map.get(symbol)
                    if not rate_info:
                        continue

                    funding = rate_info["funding_rate"]
                    if abs(funding) < FUNDING_ALERT_THRESHOLD:
                        continue

                    side = pos.get("side", "").lower()
                    size = float(pos.get("amount", 0))
                    price = rate_info["mark_price"]

                    # Funding cost: positive rate = longs pay shorts
                    if side in ("long", "buy"):
                        cost_per_interval = size * price * funding
                    else:
                        cost_per_interval = -size * price * funding

                    # Only alert if user is paying (negative cost = receiving)
                    if cost_per_interval <= 0:
                        continue

                    daily_cost = cost_per_interval * 24  # approximate

                    text = (
                        f"<b>\U0001f4b8 High Funding Rate Alert</b>\n\n"
                        f"<b>{symbol}</b> {side.upper()}\n"
                        f"Funding Rate: <code>{funding*100:.4f}%</code>/hr\n"
                        f"Est. Hourly Cost: <code>${abs(cost_per_interval):,.2f}</code>\n"
                        f"Est. Daily Cost: <code>${abs(daily_cost):,.2f}</code>\n\n"
                        f"{'Longs paying shorts' if funding > 0 else 'Shorts paying longs'}"
                    )

                    kb = InlineKeyboardMarkup(inline_keyboard=[
                        [
                            InlineKeyboardButton(
                                text="Close Position",
                                callback_data=f"close_pos:{symbol}",
                            ),
                            InlineKeyboardButton(
                                text="View Funding",
                                callback_data="cmd:funding",
                            ),
                        ],
                    ])

                    try:
                        await bot.send_message(tg_id, text, reply_markup=kb)
                    except Exception:
                        pass
            finally:
                await client.close()
        except Exception as e:
            logger.debug("Funding check failed for %s: %s", tg_id, e)
