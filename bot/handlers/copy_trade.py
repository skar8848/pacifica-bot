"""
Copy trading handlers: /copy, /unfollow, /masters, /copylog
With FSM for interactive wallet paste.

Sizing modes:
  - fixed_usd: fixed dollar amount per trade (e.g. $10)
  - pct_equity: percentage of user's equity per trade (e.g. 5%)
  - proportional: multiply master's position size (e.g. 0.5x)
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

# Temp storage for copy setup in progress: {tg_id: {wallet, sizing_mode, ...}}
_copy_setup: dict[int, dict] = {}

_DEFAULT_SETUP = {
    "wallet": "",
    "sizing_mode": "fixed_usd",
    "size_multiplier": 1.0,
    "fixed_amount_usd": 10.0,
    "pct_equity": 5.0,
    "min_trade_usd": 0,
    "max_position_usd": 1000.0,
    "max_total_usd": 5000.0,
}


class CopyStates(StatesGroup):
    waiting_wallet = State()


def _get_setup(tg_id: int, wallet: str = "") -> dict:
    """Get or create setup for a user."""
    if tg_id not in _copy_setup:
        _copy_setup[tg_id] = {**_DEFAULT_SETUP, "wallet": wallet}
    return _copy_setup[tg_id]


def _fmt_setup(setup: dict) -> str:
    """Format setup summary for display."""
    wallet = setup["wallet"]
    mode = setup["sizing_mode"]

    if mode == "fixed_usd":
        size_line = f"Size: <b>${setup['fixed_amount_usd']:.0f}</b> per trade"
    elif mode == "pct_equity":
        size_line = f"Size: <b>{setup['pct_equity']:.0f}%</b> of equity per trade"
    else:
        size_line = f"Size: <b>{setup['size_multiplier']}x</b> of master's size"

    min_line = ""
    if setup.get("min_trade_usd", 0) > 0:
        min_line = f"\nMin trigger: ${setup['min_trade_usd']:.0f}"

    return (
        f"<b>Copy Trading Setup</b>\n\n"
        f"Master: <code>{wallet}</code>\n"
        f"Mode: {mode.replace('_', ' ').title()}\n"
        f"{size_line}\n"
        f"Max per position: ${setup['max_position_usd']:,.0f}\n"
        f"Max total exposure: ${setup.get('max_total_usd', 5000):,.0f}"
        f"{min_line}\n\n"
        f"Adjust settings or start:"
    )


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
    setup = _get_setup(tg_id, wallet)
    setup["wallet"] = wallet

    await message.answer(
        _fmt_setup(setup),
        reply_markup=copy_settings_kb(wallet, setup),
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
            "<b>Usage:</b>\n"
            "<code>/copy &lt;wallet&gt;</code> — start setup\n"
            "<code>/copy &lt;wallet&gt; $10</code> — fixed $10/trade\n"
            "<code>/copy &lt;wallet&gt; 5%</code> — 5% of equity\n"
            "<code>/copy &lt;wallet&gt; 0.5x</code> — half master's size\n\n"
            "Or tap 👥 Copy Trading in the menu.",
            reply_markup=copy_menu_kb(),
        )
        return

    wallet = args[1]
    tg_id = message.from_user.id  # type: ignore
    setup = _get_setup(tg_id, wallet)
    setup["wallet"] = wallet

    # Parse optional sizing arg
    if len(args) > 2:
        raw = args[2]
        if raw.startswith("$"):
            try:
                setup["sizing_mode"] = "fixed_usd"
                setup["fixed_amount_usd"] = float(raw.lstrip("$"))
            except ValueError:
                pass
        elif raw.endswith("%"):
            try:
                setup["sizing_mode"] = "pct_equity"
                setup["pct_equity"] = float(raw.rstrip("%"))
            except ValueError:
                pass
        elif raw.lower().endswith("x"):
            try:
                setup["sizing_mode"] = "proportional"
                setup["size_multiplier"] = float(raw.lower().rstrip("x"))
            except ValueError:
                pass

    # Parse max= and min= args
    for arg in args[2:]:
        match_max = re.match(r"max=(\d+\.?\d*)", arg, re.IGNORECASE)
        match_min = re.match(r"min=(\d+\.?\d*)", arg, re.IGNORECASE)
        if match_max:
            setup["max_position_usd"] = float(match_max.group(1))
        if match_min:
            setup["min_trade_usd"] = float(match_min.group(1))

    await message.answer(
        _fmt_setup(setup),
        reply_markup=copy_settings_kb(wallet, setup),
    )


# ------------------------------------------------------------------
# Settings callbacks — mode, amounts, filters
# ------------------------------------------------------------------

@router.callback_query(F.data.startswith("cmode:"))
async def copy_mode_callback(callback: CallbackQuery):
    """Switch sizing mode."""
    parts = callback.data.split(":")  # type: ignore
    wallet = parts[1]
    mode = parts[2]
    tg_id = callback.from_user.id

    setup = _get_setup(tg_id, wallet)
    setup["sizing_mode"] = mode
    await callback.answer(f"Mode: {mode.replace('_', ' ').title()}")

    await callback.message.edit_text(  # type: ignore
        _fmt_setup(setup),
        reply_markup=copy_settings_kb(wallet, setup),
    )


@router.callback_query(F.data.startswith("camt:"))
async def copy_fixed_amount(callback: CallbackQuery):
    """Set fixed USD amount."""
    parts = callback.data.split(":")  # type: ignore
    wallet = parts[1]
    amount = float(parts[2])
    tg_id = callback.from_user.id

    setup = _get_setup(tg_id, wallet)
    setup["sizing_mode"] = "fixed_usd"
    setup["fixed_amount_usd"] = amount
    await callback.answer(f"${amount}/trade")

    await callback.message.edit_text(  # type: ignore
        _fmt_setup(setup),
        reply_markup=copy_settings_kb(wallet, setup),
    )


@router.callback_query(F.data.startswith("cpct:"))
async def copy_pct_equity(callback: CallbackQuery):
    """Set % equity per trade."""
    parts = callback.data.split(":")  # type: ignore
    wallet = parts[1]
    pct = float(parts[2])
    tg_id = callback.from_user.id

    setup = _get_setup(tg_id, wallet)
    setup["sizing_mode"] = "pct_equity"
    setup["pct_equity"] = pct
    await callback.answer(f"{pct}% of equity")

    await callback.message.edit_text(  # type: ignore
        _fmt_setup(setup),
        reply_markup=copy_settings_kb(wallet, setup),
    )


@router.callback_query(F.data.startswith("cm:"))
async def copy_mult_callback(callback: CallbackQuery):
    """Set proportional multiplier."""
    parts = callback.data.split(":")  # type: ignore
    wallet = parts[1]
    mult = float(parts[2])
    tg_id = callback.from_user.id

    setup = _get_setup(tg_id, wallet)
    setup["sizing_mode"] = "proportional"
    setup["size_multiplier"] = mult
    await callback.answer(f"Multiplier: {mult}x")

    await callback.message.edit_text(  # type: ignore
        _fmt_setup(setup),
        reply_markup=copy_settings_kb(wallet, setup),
    )


@router.callback_query(F.data.startswith("cmin:"))
async def copy_min_trade(callback: CallbackQuery):
    """Set minimum master trade size to trigger copy."""
    parts = callback.data.split(":")  # type: ignore
    wallet = parts[1]
    min_usd = float(parts[2])
    tg_id = callback.from_user.id

    setup = _get_setup(tg_id, wallet)
    setup["min_trade_usd"] = min_usd
    label = "Off" if min_usd == 0 else f"${min_usd:.0f}"
    await callback.answer(f"Min trigger: {label}")

    await callback.message.edit_text(  # type: ignore
        _fmt_setup(setup),
        reply_markup=copy_settings_kb(wallet, setup),
    )


@router.callback_query(F.data.startswith("cx:"))
async def copy_max_callback(callback: CallbackQuery):
    """Set max position cap."""
    parts = callback.data.split(":")  # type: ignore
    wallet = parts[1]
    max_usd = float(parts[2])
    tg_id = callback.from_user.id

    setup = _get_setup(tg_id, wallet)
    setup["max_position_usd"] = max_usd
    await callback.answer(f"Cap: ${max_usd:,.0f}")

    await callback.message.edit_text(  # type: ignore
        _fmt_setup(setup),
        reply_markup=copy_settings_kb(wallet, setup),
    )


@router.callback_query(F.data.startswith("ctotal:"))
async def copy_total_cap(callback: CallbackQuery):
    """Set max total exposure cap across all copy positions."""
    parts = callback.data.split(":")  # type: ignore
    wallet = parts[1]
    total_usd = float(parts[2])
    tg_id = callback.from_user.id

    setup = _get_setup(tg_id, wallet)
    setup["max_total_usd"] = total_usd
    await callback.answer(f"Total cap: ${total_usd:,.0f}")

    await callback.message.edit_text(  # type: ignore
        _fmt_setup(setup),
        reply_markup=copy_settings_kb(wallet, setup),
    )


@router.callback_query(F.data.startswith("copy_go:"))
async def copy_start_callback(callback: CallbackQuery):
    tg_id = callback.from_user.id
    wallet = callback.data.split(":", 1)[1]  # type: ignore
    setup = _copy_setup.pop(tg_id, None)

    if not setup:
        setup = {**_DEFAULT_SETUP, "wallet": wallet}

    await callback.answer("Starting copy...")

    await add_copy_config(
        telegram_id=tg_id,
        master_wallet=setup["wallet"],
        sizing_mode=setup["sizing_mode"],
        size_multiplier=setup["size_multiplier"],
        fixed_amount_usd=setup["fixed_amount_usd"],
        pct_equity=setup["pct_equity"],
        min_trade_usd=setup.get("min_trade_usd", 0),
        max_position_usd=setup["max_position_usd"],
        max_total_usd=setup.get("max_total_usd", 5000),
    )

    mode = setup["sizing_mode"]
    if mode == "fixed_usd":
        size_str = f"${setup['fixed_amount_usd']:.0f}/trade"
    elif mode == "pct_equity":
        size_str = f"{setup['pct_equity']:.0f}% equity/trade"
    else:
        size_str = f"{setup['size_multiplier']}x"

    min_str = ""
    if setup.get("min_trade_usd", 0) > 0:
        min_str = f" | Min trigger: ${setup['min_trade_usd']:.0f}"

    await callback.message.edit_text(  # type: ignore
        f"<b>✅ Copy Active!</b>\n\n"
        f"Master: <code>{setup['wallet']}</code>\n"
        f"Size: {size_str} | Cap: ${setup['max_position_usd']:,.0f}{min_str}\n"
        f"Total exposure limit: ${setup.get('max_total_usd', 5000):,.0f}\n\n"
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
        mode = cfg.get("sizing_mode", "proportional")
        if mode == "fixed_usd":
            size = f"${cfg.get('fixed_amount_usd', 10):.0f}/trade"
        elif mode == "pct_equity":
            size = f"{cfg.get('pct_equity', 5):.0f}% equity"
        else:
            size = f"{cfg['size_multiplier']}x"

        min_str = ""
        if cfg.get("min_trade_usd", 0) > 0:
            min_str = f" | Min: ${cfg['min_trade_usd']:.0f}"

        text += (
            f"<code>{w[:8]}...{w[-4:]}</code>\n"
            f"  {size} | Cap ${cfg['max_position_usd']:,.0f}{min_str}\n\n"
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
