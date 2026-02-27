"""
Copy trading handlers: /copy, /unfollow, /masters, /copylog
With FSM for interactive wallet paste.
"""

import logging
import re

from aiogram import Router, F
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

from database.db import (
    get_user,
    add_copy_config,
    get_active_copy_configs,
    deactivate_copy_config,
    get_trade_history,
)
from bot.utils.keyboards import copy_settings_kb, copy_menu_kb, main_menu_kb

logger = logging.getLogger(__name__)
router = Router()

# Temp storage for copy setup in progress: {tg_id: {wallet, multiplier, max_usd}}
_copy_setup: dict[int, dict] = {}


class CopyStates(StatesGroup):
    waiting_wallet = State()


# ------------------------------------------------------------------
# FSM: user pastes wallet from "Copy a Wallet" button
# ------------------------------------------------------------------

@router.message(CopyStates.waiting_wallet)
async def msg_copy_wallet(message: Message, state: FSMContext):
    wallet = (message.text or "").strip()
    await state.clear()

    if len(wallet) < 20 or " " in wallet:
        await message.answer(
            "That doesn't look like a valid wallet. Try again or /menu.",
            reply_markup=copy_menu_kb(),
        )
        return

    tg_id = message.from_user.id  # type: ignore
    _copy_setup[tg_id] = {
        "wallet": wallet,
        "multiplier": 1.0,
        "max_usd": 1000.0,
    }

    await message.answer(
        f"<b>Copy Trading Setup</b>\n\n"
        f"Master: <code>{wallet}</code>\n"
        f"Size multiplier: 1.0x\n"
        f"Max position: $1000\n\n"
        f"Adjust settings or start:",
        reply_markup=copy_settings_kb(wallet),
    )


# ------------------------------------------------------------------
# /copy command (still works as shortcut)
# ------------------------------------------------------------------

@router.message(Command("copy"))
async def cmd_copy(message: Message):
    user = await get_user(message.from_user.id)  # type: ignore
    if not user or not user.get("pacifica_account"):
        await message.answer("Please /start and link your account first.", reply_markup=main_menu_kb())
        return

    args = (message.text or "").split()
    if len(args) < 2:
        await message.answer(
            "Usage: /copy <wallet> [multiplier] [max=amount]\n\n"
            "Or tap 👥 Copy Trading in the menu.",
            reply_markup=copy_menu_kb(),
        )
        return

    wallet = args[1]
    multiplier = 1.0
    max_usd = 1000.0

    if len(args) > 2:
        mult_raw = args[2].lower().rstrip("x")
        try:
            multiplier = float(mult_raw)
        except ValueError:
            pass

    for arg in args[2:]:
        match = re.match(r"max=(\d+\.?\d*)", arg, re.IGNORECASE)
        if match:
            max_usd = float(match.group(1))

    tg_id = message.from_user.id  # type: ignore
    _copy_setup[tg_id] = {
        "wallet": wallet,
        "multiplier": multiplier,
        "max_usd": max_usd,
    }

    await message.answer(
        f"<b>Copy Trading Setup</b>\n\n"
        f"Master: <code>{wallet}</code>\n"
        f"Size multiplier: {multiplier}x\n"
        f"Max position: ${max_usd}\n\n"
        f"Adjust or start:",
        reply_markup=copy_settings_kb(wallet),
    )


# ------------------------------------------------------------------
# Copy settings callbacks (multiplier / max / start)
# ------------------------------------------------------------------

@router.callback_query(F.data.startswith("cm:"))
async def copy_mult_callback(callback: CallbackQuery):
    parts = callback.data.split(":")  # type: ignore
    wallet_prefix = parts[1]
    mult = float(parts[2])
    tg_id = callback.from_user.id

    if tg_id in _copy_setup:
        _copy_setup[tg_id]["multiplier"] = mult
        wallet = _copy_setup[tg_id]["wallet"]
    else:
        wallet = wallet_prefix

    await callback.answer(f"Multiplier: {mult}x")
    await callback.message.edit_text(  # type: ignore
        f"<b>Copy Trading Setup</b>\n\n"
        f"Master: <code>{wallet}</code>\n"
        f"Size multiplier: {mult}x\n"
        f"Max position: ${_copy_setup.get(tg_id, {}).get('max_usd', 1000)}\n\n"
        f"Adjust or start:",
        reply_markup=copy_settings_kb(wallet),
    )


@router.callback_query(F.data.startswith("cx:"))
async def copy_max_callback(callback: CallbackQuery):
    parts = callback.data.split(":")  # type: ignore
    wallet_prefix = parts[1]
    max_usd = float(parts[2])
    tg_id = callback.from_user.id

    if tg_id in _copy_setup:
        _copy_setup[tg_id]["max_usd"] = max_usd
        wallet = _copy_setup[tg_id]["wallet"]
    else:
        wallet = wallet_prefix

    await callback.answer(f"Max: ${max_usd}")
    await callback.message.edit_text(  # type: ignore
        f"<b>Copy Trading Setup</b>\n\n"
        f"Master: <code>{wallet}</code>\n"
        f"Size multiplier: {_copy_setup.get(tg_id, {}).get('multiplier', 1.0)}x\n"
        f"Max position: ${max_usd}\n\n"
        f"Adjust or start:",
        reply_markup=copy_settings_kb(wallet),
    )


@router.callback_query(F.data.startswith("copy_go:"))
async def copy_start_callback(callback: CallbackQuery):
    tg_id = callback.from_user.id
    wallet = callback.data.split(":", 1)[1]  # type: ignore
    setup = _copy_setup.pop(tg_id, None)

    if not setup:
        setup = {"wallet": wallet, "multiplier": 1.0, "max_usd": 1000.0}

    await callback.answer("Starting copy...")

    await add_copy_config(
        telegram_id=tg_id,
        master_wallet=setup["wallet"],
        size_multiplier=setup["multiplier"],
        max_position_usd=setup["max_usd"],
    )

    await callback.message.edit_text(  # type: ignore
        f"<b>✅ Copy Active!</b>\n\n"
        f"Master: <code>{setup['wallet']}</code>\n"
        f"Multiplier: {setup['multiplier']}x | Max: ${setup['max_usd']}\n\n"
        f"The bot will automatically mirror their trades.",
        reply_markup=copy_menu_kb(),
    )


# ------------------------------------------------------------------
# /unfollow, /masters, /copylog commands
# ------------------------------------------------------------------

@router.message(Command("unfollow"))
async def cmd_unfollow(message: Message):
    args = (message.text or "").split()
    if len(args) < 2:
        await message.answer("Usage: /unfollow <wallet_address>", reply_markup=copy_menu_kb())
        return

    wallet = args[1]
    tg_id = message.from_user.id  # type: ignore
    await deactivate_copy_config(tg_id, wallet)
    await message.answer(f"Stopped copying <code>{wallet}</code>", reply_markup=copy_menu_kb())


@router.message(Command("masters"))
async def cmd_masters(message: Message):
    tg_id = message.from_user.id  # type: ignore
    configs = await get_active_copy_configs(tg_id)

    if not configs:
        await message.answer("You're not copying anyone.", reply_markup=copy_menu_kb())
        return

    text = "<b>👥 My Masters</b>\n\n"
    for cfg in configs:
        w = cfg["master_wallet"]
        text += (
            f"<code>{w[:8]}...{w[-4:]}</code>\n"
            f"  {cfg['size_multiplier']}x | Max ${cfg['max_position_usd']}\n\n"
        )
    await message.answer(text, reply_markup=copy_menu_kb())


@router.message(Command("copylog"))
async def cmd_copylog(message: Message):
    tg_id = message.from_user.id  # type: ignore
    trades = await get_trade_history(tg_id, limit=20)
    copy_trades = [t for t in trades if t["is_copy_trade"]]

    if not copy_trades:
        await message.answer("No copy trades yet.", reply_markup=copy_menu_kb())
        return

    text = "<b>📜 Copy Log</b>\n\n"
    for t in copy_trades:
        side_label = "BUY" if t["side"] == "bid" else "SELL"
        text += (
            f"{t['symbol']} {side_label} {t['amount']} ({t['order_type']})\n"
            f"  Master: <code>{t['master_wallet'][:8]}...</code>\n"
        )
    await message.answer(text, reply_markup=copy_menu_kb())
