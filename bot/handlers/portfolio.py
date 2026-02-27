"""
Portfolio command shortcuts — /positions, /orders, /pnl, /balance, /history
These are quick text-command alternatives to the button navigation.
"""

import logging

from aiogram import Router
from aiogram.filters import Command
from aiogram.types import Message

from database.db import get_user, get_trade_history
from bot.models.user import build_client_from_user
from bot.utils.formatters import fmt_position, fmt_order, fmt_balance, fmt_pnl
from bot.utils.keyboards import main_menu_kb, back_to_menu_kb, positions_kb

logger = logging.getLogger(__name__)
router = Router()


async def _require_linked_user(message: Message) -> dict | None:
    user = await get_user(message.from_user.id)  # type: ignore
    if not user or not user.get("pacifica_account"):
        await message.answer(
            "Link your account first!\nUse /start then /link <wallet>",
            reply_markup=main_menu_kb(),
        )
        return None
    return user


@router.message(Command("positions"))
async def cmd_positions(message: Message):
    user = await _require_linked_user(message)
    if not user:
        return

    try:
        client = build_client_from_user(user)
        positions = await client.get_positions()
        await client.close()
    except Exception as e:
        await message.answer(f"Error: {e}", reply_markup=back_to_menu_kb())
        return

    if not positions:
        text = "<b>📈 Positions</b>\n\nNo open positions."
    else:
        text = "<b>📈 Positions</b>\n\n"
        for pos in positions:
            text += fmt_position(pos) + "\n"

    await message.answer(text, reply_markup=positions_kb(positions))


@router.message(Command("orders"))
async def cmd_orders(message: Message):
    user = await _require_linked_user(message)
    if not user:
        return

    try:
        client = build_client_from_user(user)
        orders = await client.get_open_orders()
        await client.close()
    except Exception as e:
        await message.answer(f"Error: {e}", reply_markup=back_to_menu_kb())
        return

    if not orders:
        text = "<b>📋 Orders</b>\n\nNo open orders."
    else:
        text = "<b>📋 Orders</b>\n\n"
        for o in orders:
            text += fmt_order(o) + "\n"

    await message.answer(text, reply_markup=back_to_menu_kb())


@router.message(Command("balance"))
async def cmd_balance(message: Message):
    user = await _require_linked_user(message)
    if not user:
        return

    try:
        client = build_client_from_user(user)
        info = await client.get_account_info()
        await client.close()
    except Exception as e:
        await message.answer(f"Error: {e}", reply_markup=back_to_menu_kb())
        return

    await message.answer(fmt_balance(info), reply_markup=main_menu_kb())


@router.message(Command("pnl"))
async def cmd_pnl(message: Message):
    user = await _require_linked_user(message)
    if not user:
        return

    try:
        client = build_client_from_user(user)
        trades = await client.get_trades_history()
        await client.close()
    except Exception as e:
        await message.answer(f"Error: {e}", reply_markup=back_to_menu_kb())
        return

    await message.answer(fmt_pnl(trades), reply_markup=main_menu_kb())


@router.message(Command("history"))
async def cmd_history(message: Message):
    tg_id = message.from_user.id  # type: ignore
    trades = await get_trade_history(tg_id, limit=20)

    if not trades:
        await message.answer("No trade history yet.", reply_markup=main_menu_kb())
        return

    text = "<b>📜 Trade History</b>\n\n"
    for t in trades:
        side_label = "BUY" if t["side"] == "bid" else "SELL"
        copy_tag = " [COPY]" if t["is_copy_trade"] else ""
        text += f"{t['symbol']} {side_label} {t['amount']} ({t['order_type']}){copy_tag}\n"

    await message.answer(text, reply_markup=main_menu_kb())
