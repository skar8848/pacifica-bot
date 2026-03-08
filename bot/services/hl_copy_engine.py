"""
HL → Pacifica Mirror Copy Engine

Polls Hyperliquid positions for master wallets and replicates
trades on Pacifica. Uses the same copy_configs table with
source='hyperliquid'.
"""

import asyncio
import logging

from aiogram import Bot

from bot.config import COPY_POLL_INTERVAL
from bot.services.hl_whale_tracker import hl_get_positions, _fmt_usd
from bot.services.market_data import get_price, get_lot_size, usd_to_token, round_to_lot, _get_client
from bot.models.user import build_client_from_user
from database.db import get_user, get_db, log_trade

logger = logging.getLogger(__name__)

_running = False
_hl_snapshots: dict[str, dict[str, dict]] = {}

HL_POLL_INTERVAL = 15  # seconds


async def _get_hl_copy_configs() -> list[dict]:
    """Get all active copy configs with source='hyperliquid'."""
    db = await get_db()
    async with db.execute(
        "SELECT * FROM copy_configs WHERE active = 1 AND source = 'hyperliquid'"
    ) as cursor:
        return [dict(r) for r in await cursor.fetchall()]


def _parse_hl_positions(state: dict) -> dict[str, dict]:
    """Parse HL clearinghouse state into {coin: {side, size_usd, size_token}}."""
    result = {}
    for p in state.get("assetPositions", []):
        pos = p.get("position", p)
        coin = pos.get("coin", "")
        if not coin:
            continue
        szi = float(pos.get("szi", "0"))
        ntl = abs(float(pos.get("positionValue", "0")))
        if ntl < 10:  # ignore dust
            continue
        result[coin] = {
            "side": "bid" if szi > 0 else "ask",
            "side_label": "LONG" if szi > 0 else "SHORT",
            "size_usd": ntl,
            "size_token": abs(szi),
        }
    return result


async def _replicate_hl_open(
    bot: Bot, master_wallet: str, coin: str, pos: dict, followers: list[dict]
):
    """Open a position on Pacifica mirroring an HL position."""
    symbol = coin  # Pacifica uses same symbol names
    side = pos["side"]
    master_usd = pos["size_usd"]

    price = await get_price(symbol)
    if not price or price <= 0:
        logger.debug("No Pacifica price for %s, skipping HL mirror", symbol)
        return

    lot_size = await get_lot_size(symbol)

    for cfg in followers:
        tg_id = cfg["telegram_id"]
        client = None
        try:
            user = await get_user(tg_id)
            if not user or not user.get("pacifica_account"):
                continue

            # Calculate copy amount
            sizing_mode = cfg.get("sizing_mode", "fixed_usd")
            max_usd = cfg.get("max_position_usd", 1000)

            if sizing_mode == "fixed_usd":
                copy_usd = cfg.get("fixed_amount_usd", 10.0)
            elif sizing_mode == "pct_equity":
                pct = cfg.get("pct_equity", 5.0)
                client = build_client_from_user(user)
                try:
                    info = await client.get_account_info()
                    equity = float(info.get("equity", 0))
                    copy_usd = equity * (pct / 100.0)
                except Exception:
                    copy_usd = 10.0
            else:  # proportional
                multiplier = cfg.get("size_multiplier", 1.0)
                copy_usd = master_usd * multiplier

            copy_usd = min(copy_usd, max_usd)
            if copy_usd < 1:
                continue

            amount = usd_to_token(copy_usd, price, lot_size)
            if float(amount) <= 0:
                continue

            if not client:
                client = build_client_from_user(user)

            await client.create_market_order(symbol=symbol, side=side, amount=amount)

            await log_trade(
                tg_id, symbol, side, amount,
                order_type="hl_mirror_open", is_copy_trade=True,
                master_wallet=f"hl:{master_wallet[:10]}",
            )

            direction = pos["side_label"]
            short_addr = f"{master_wallet[:6]}...{master_wallet[-4:]}"
            await bot.send_message(
                tg_id,
                f"<b>HL Mirror Trade</b>\n\n"
                f"Whale <code>{short_addr}</code> opened {coin} {direction} "
                f"({_fmt_usd(master_usd)}) on Hyperliquid\n\n"
                f"Mirrored: {symbol} {direction} {amount} (~${copy_usd:,.0f}) on Pacifica",
            )

        except Exception as e:
            logger.error("HL mirror open failed for user %s: %s", tg_id, e)
            try:
                await bot.send_message(tg_id, f"HL mirror failed for {coin}: {e}")
            except Exception:
                pass
        finally:
            if client:
                await client.close()


async def _replicate_hl_close(
    bot: Bot, master_wallet: str, coin: str, prev_pos: dict, followers: list[dict]
):
    """Close a Pacifica position when HL whale closes."""
    symbol = coin

    for cfg in followers:
        tg_id = cfg["telegram_id"]
        client = None
        try:
            user = await get_user(tg_id)
            if not user or not user.get("pacifica_account"):
                continue

            client = build_client_from_user(user)
            positions = await client.get_positions()
            pos = next(
                (p for p in positions if p.get("symbol", "").upper() == symbol.upper()),
                None,
            )
            if not pos:
                continue

            pos_size = abs(float(pos.get("amount", pos.get("size", 0))))
            lot = await get_lot_size(symbol)
            amount = round_to_lot(pos_size, lot)
            close_side = "ask" if pos.get("side") == "bid" else "bid"

            if float(amount) <= 0:
                continue

            await client.create_market_order(
                symbol=symbol, side=close_side, amount=amount, reduce_only=True,
            )

            await log_trade(
                tg_id, symbol, close_side, amount,
                order_type="hl_mirror_close", is_copy_trade=True,
                master_wallet=f"hl:{master_wallet[:10]}",
            )

            short_addr = f"{master_wallet[:6]}...{master_wallet[-4:]}"
            await bot.send_message(
                tg_id,
                f"<b>HL Mirror — Position Closed</b>\n\n"
                f"Whale <code>{short_addr}</code> closed {coin} on Hyperliquid\n"
                f"Mirrored: {symbol} closed (size: {amount}) on Pacifica",
            )

        except Exception as e:
            logger.error("HL mirror close failed for user %s: %s", tg_id, e)
        finally:
            if client:
                await client.close()


async def _poll_hl_cycle(bot: Bot):
    """One poll cycle: check all HL master wallets for changes."""
    configs = await _get_hl_copy_configs()
    if not configs:
        return

    # Group by master wallet
    masters: dict[str, list[dict]] = {}
    for cfg in configs:
        masters.setdefault(cfg["master_wallet"], []).append(cfg)

    for master_wallet, followers in masters.items():
        try:
            state = await hl_get_positions(master_wallet)
            current = _parse_hl_positions(state)
            prev = _hl_snapshots.get(master_wallet, {})
            _hl_snapshots[master_wallet] = current

            # Skip first snapshot
            if not prev:
                continue

            # Detect new positions
            for coin, pos in current.items():
                if coin not in prev:
                    await _replicate_hl_open(bot, master_wallet, coin, pos, followers)
                elif prev[coin]["side"] != pos["side"]:
                    # Side flipped — close then open
                    await _replicate_hl_close(bot, master_wallet, coin, prev[coin], followers)
                    await _replicate_hl_open(bot, master_wallet, coin, pos, followers)

            # Detect closed positions
            for coin, pos in prev.items():
                if coin not in current:
                    await _replicate_hl_close(bot, master_wallet, coin, pos, followers)

        except Exception as e:
            logger.error("HL mirror check failed for %s: %s", master_wallet, e)


async def start_hl_copy_engine(bot: Bot):
    global _running
    _running = True
    logger.info("HL copy engine started (poll every %ss)", HL_POLL_INTERVAL)

    while _running:
        try:
            await _poll_hl_cycle(bot)
        except Exception as e:
            logger.error("HL copy engine error: %s", e)
        await asyncio.sleep(HL_POLL_INTERVAL)


def stop_hl_copy_engine():
    global _running
    _running = False
