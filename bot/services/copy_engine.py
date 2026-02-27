"""
Copy trading engine — polls master wallets and replicates positions.
"""

import asyncio
import logging
from typing import Any

from aiogram import Bot
from solders.keypair import Keypair

from bot.config import COPY_POLL_INTERVAL
from bot.services.pacifica_client import PacificaClient, PacificaAPIError
from bot.models.user import build_client_from_user
from database.db import (
    get_active_copy_configs,
    get_user,
    log_trade,
)

logger = logging.getLogger(__name__)

_running = False
# Track last known positions per master: {master_wallet: {symbol: position_dict}}
_master_snapshots: dict[str, dict[str, dict]] = {}


async def start_copy_engine(bot: Bot):
    global _running
    _running = True
    logger.info("Copy engine started (poll every %ss)", COPY_POLL_INTERVAL)

    while _running:
        try:
            await _poll_cycle(bot)
        except Exception as e:
            logger.error("Copy engine error: %s", e)
        await asyncio.sleep(COPY_POLL_INTERVAL)


def stop_copy_engine():
    global _running
    _running = False


async def _poll_cycle(bot: Bot):
    configs = await get_active_copy_configs()
    if not configs:
        return

    # Group configs by master wallet
    masters: dict[str, list[dict]] = {}
    for cfg in configs:
        masters.setdefault(cfg["master_wallet"], []).append(cfg)

    for master_wallet, followers in masters.items():
        try:
            await _check_master(bot, master_wallet, followers)
        except Exception as e:
            logger.error("Error checking master %s: %s", master_wallet, e)


async def _check_master(bot: Bot, master_wallet: str, followers: list[dict]):
    # Fetch master's current positions (read-only, no signing needed)
    temp_kp = Keypair()  # throwaway for GET
    client = PacificaClient(account=master_wallet, keypair=temp_kp)

    try:
        positions = await client.get_positions(master_wallet)
    except PacificaAPIError:
        logger.warning("Could not fetch positions for master %s", master_wallet)
        return
    finally:
        await client.close()

    # Build current snapshot: {symbol: position}
    current: dict[str, dict] = {}
    for pos in positions:
        symbol = pos.get("symbol", "")
        if symbol:
            current[symbol] = pos

    prev = _master_snapshots.get(master_wallet, {})
    _master_snapshots[master_wallet] = current

    # Detect changes
    # New positions (opened)
    for symbol, pos in current.items():
        if symbol not in prev:
            await _replicate_open(bot, master_wallet, pos, followers)
        else:
            # Check if side changed (flipped position)
            if pos.get("side") != prev[symbol].get("side"):
                await _replicate_close(bot, master_wallet, prev[symbol], followers)
                await _replicate_open(bot, master_wallet, pos, followers)

    # Closed positions
    for symbol, pos in prev.items():
        if symbol not in current:
            await _replicate_close(bot, master_wallet, pos, followers)


async def _replicate_open(
    bot: Bot, master_wallet: str, master_pos: dict, followers: list[dict]
):
    symbol = master_pos.get("symbol", "?")
    side = master_pos.get("side", "bid")
    master_size = abs(float(master_pos.get("amount", master_pos.get("size", 0))))

    for cfg in followers:
        tg_id = cfg["telegram_id"]
        multiplier = cfg["size_multiplier"]
        max_usd = cfg["max_position_usd"]

        # Calculate copy size
        copy_size = master_size * multiplier

        # TODO: convert to USD and cap at max_position_usd
        # For now, just apply the multiplier

        try:
            user = await get_user(tg_id)
            if not user or not user.get("pacifica_account"):
                continue

            client = build_client_from_user(user)
            resp = await client.create_market_order(
                symbol=symbol,
                side=side,
                amount=str(copy_size),
            )
            await client.close()

            await log_trade(
                tg_id, symbol, side, str(copy_size),
                order_type="copy_open",
                is_copy_trade=True,
                master_wallet=master_wallet,
            )

            direction = "LONG" if side == "bid" else "SHORT"
            await bot.send_message(
                tg_id,
                f"<b>Copy Trade Executed</b>\n"
                f"Master: <code>{master_wallet[:8]}...</code>\n"
                f"{symbol} {direction} {copy_size}\n",
            )
        except Exception as e:
            logger.error("Copy open failed for user %s: %s", tg_id, e)
            try:
                await bot.send_message(
                    tg_id,
                    f"Copy trade failed for {symbol}: {e}",
                )
            except Exception:
                pass


async def _replicate_close(
    bot: Bot, master_wallet: str, master_pos: dict, followers: list[dict]
):
    symbol = master_pos.get("symbol", "?")
    close_side = "ask" if master_pos.get("side") == "bid" else "bid"

    for cfg in followers:
        tg_id = cfg["telegram_id"]

        try:
            user = await get_user(tg_id)
            if not user or not user.get("pacifica_account"):
                continue

            client = build_client_from_user(user)

            # Get follower's actual position for this symbol
            positions = await client.get_positions()
            pos = next(
                (p for p in positions if p.get("symbol", "").upper() == symbol.upper()),
                None,
            )
            if not pos:
                await client.close()
                continue

            amount = str(abs(float(pos.get("amount", pos.get("size", 0)))))
            actual_close_side = "ask" if pos.get("side") == "bid" else "bid"

            resp = await client.create_market_order(
                symbol=symbol,
                side=actual_close_side,
                amount=amount,
                reduce_only=True,
            )
            await client.close()

            await log_trade(
                tg_id, symbol, actual_close_side, amount,
                order_type="copy_close",
                is_copy_trade=True,
                master_wallet=master_wallet,
            )

            await bot.send_message(
                tg_id,
                f"<b>Copy Trade — Position Closed</b>\n"
                f"Master: <code>{master_wallet[:8]}...</code>\n"
                f"{symbol} closed (size: {amount})\n",
            )
        except Exception as e:
            logger.error("Copy close failed for user %s: %s", tg_id, e)
