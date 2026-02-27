"""
SOL balance monitoring — alerts users when their balance is low.
"""

import asyncio
import logging

import aiohttp
from aiogram import Bot

from bot.config import (
    SOLANA_RPC_URL,
    GAS_CHECK_INTERVAL,
    LOW_SOL_THRESHOLD,
)
from database.db import get_db

logger = logging.getLogger(__name__)

_running = False


async def start_gas_monitor(bot: Bot):
    global _running
    _running = True
    logger.info(
        "Gas monitor started (check every %ss, threshold %.3f SOL)",
        GAS_CHECK_INTERVAL,
        LOW_SOL_THRESHOLD,
    )

    while _running:
        try:
            await _check_all_users(bot)
        except Exception as e:
            logger.error("Gas monitor error: %s", e)
        await asyncio.sleep(GAS_CHECK_INTERVAL)


def stop_gas_monitor():
    global _running
    _running = False


async def _check_all_users(bot: Bot):
    db = await get_db()
    async with db.execute(
        "SELECT telegram_id, pacifica_account FROM users WHERE pacifica_account IS NOT NULL"
    ) as cursor:
        users = await cursor.fetchall()

    for row in users:
        tg_id = row[0]
        account = row[1]
        try:
            balance = await _get_sol_balance(account)
            if balance is not None and balance < LOW_SOL_THRESHOLD:
                await bot.send_message(
                    tg_id,
                    f"<b>Low SOL Balance Warning</b>\n\n"
                    f"Your SOL balance is low ({balance:.4f} SOL).\n"
                    f"Transactions may fail without enough SOL for gas.\n\n"
                    f"Send SOL to your wallet:\n"
                    f"<code>{account}</code>\n\n"
                    f"Gasless mode coming soon!",
                )
        except Exception as e:
            logger.debug("Gas check failed for %s: %s", account, e)


async def _get_sol_balance(pubkey: str) -> float | None:
    """Get SOL balance via Solana JSON-RPC."""
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getBalance",
        "params": [pubkey],
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(
            SOLANA_RPC_URL,
            json=payload,
            headers={"Content-Type": "application/json"},
        ) as resp:
            data = await resp.json()

    result = data.get("result")
    if result and "value" in result:
        lamports = result["value"]
        return lamports / 1_000_000_000  # Convert lamports to SOL

    return None
