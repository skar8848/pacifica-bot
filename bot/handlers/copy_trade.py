"""
Copy trading — /copy, /unfollow, /masters, /copylog
Leaderboard — /top, /inspect, nav:leaderboard

Sizing modes:
  - fixed_usd: fixed dollar amount per trade (e.g. $10)
  - pct_equity: percentage of user's equity per trade (e.g. 5%)
  - proportional: multiply master's position size (e.g. 0.5x)
"""

import asyncio
import logging
import re

from aiogram import Router, F
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

from database.db import (
    get_user,
    add_copy_config,
    get_active_copy_configs,
    deactivate_copy_config,
    get_trade_history,
)
from bot.services.market_data import _get_client
from bot.utils.keyboards import copy_settings_kb, copy_menu_kb, main_menu_kb, back_to_menu_kb
from bot.utils.formatters import fmt_leaderboard

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

_SORT_MAP = {
    "pnl": ("pnl_all_time", "All-Time PnL"),
    "pnl7d": ("pnl_7d", "7-Day PnL"),
    "pnl1d": ("pnl_1d", "1-Day PnL"),
    "volume": ("volume_all_time", "All-Time Volume"),
    "equity": ("equity_current", "Equity"),
}


class CopyStates(StatesGroup):
    waiting_wallet = State()


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _get_setup(tg_id: int, wallet: str = "") -> dict:
    if tg_id not in _copy_setup:
        _copy_setup[tg_id] = {**_DEFAULT_SETUP, "wallet": wallet}
    return _copy_setup[tg_id]


def _fmt_setup(setup: dict) -> str:
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


def _fmt_top_traders(traders: list, sort_by: str = "pnl") -> str:
    """Format top traders list (shared between /top and refresh callback)."""
    sort_key, sort_label = _SORT_MAP.get(sort_by, _SORT_MAP["pnl"])
    traders.sort(key=lambda t: float(t.get(sort_key, 0)), reverse=True)

    text = f"<b>🏆 Top Traders — {sort_label}</b>\n\n"
    for i, t in enumerate(traders[:10], 1):
        addr = t.get("address", "?")
        val = float(t.get(sort_key, 0))
        equity = float(t.get("equity_current", 0))
        sign = "+" if val >= 0 else ""
        emoji = "🟢" if val >= 0 else "🔴"
        val_str = f"{sign}${val:,.0f}" if sort_key.startswith("pnl") else f"${val:,.0f}"
        short = f"{addr[:6]}...{addr[-4:]}"
        text += (
            f"<b>{i}.</b> {emoji} {short}\n"
            f"   {sort_label}: {val_str} | Equity: ${equity:,.0f}\n"
            f"   /copy <code>{addr}</code>\n"
            f"   /inspect <code>{addr}</code>\n\n"
        )

    text += f"<i>Sort: /top pnl | /top pnl7d | /top volume | /top equity</i>"
    return text


def _top_kb(sort_by: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🔄 Refresh", callback_data=f"top:{sort_by}"),
            InlineKeyboardButton(text="◀️ Menu", callback_data="nav:menu"),
        ],
    ])


def _inspect_kb(wallet: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="👥 Copy This Trader", callback_data=f"copy:start:{wallet}"),
            InlineKeyboardButton(text="🔄 Refresh", callback_data=f"inspect:{wallet}"),
        ],
        [InlineKeyboardButton(text="◀️ Menu", callback_data="nav:menu")],
    ])


# ------------------------------------------------------------------
# Leaderboard (nav:leaderboard button)
# ------------------------------------------------------------------

@router.callback_query(F.data == "nav:leaderboard")
async def nav_leaderboard(callback: CallbackQuery):
    await callback.answer("Loading top traders...")

    traders = []
    try:
        client = await _get_client()
        traders = await client.get_leaderboard()
    except Exception:
        pass

    if not traders:
        from database.db import get_db
        db = await get_db()
        async with db.execute(
            """SELECT u.username, u.pacifica_account, COUNT(t.id) as trades
               FROM users u LEFT JOIN trade_log t ON u.telegram_id = t.telegram_id
               WHERE u.pacifica_account IS NOT NULL
               GROUP BY u.telegram_id ORDER BY trades DESC LIMIT 10"""
        ) as cursor:
            rows = await cursor.fetchall()

        if rows:
            text = "<b>🏆 Top Traders</b>\n\n"
            for i, row in enumerate(rows, 1):
                name = row[0] or f"{row[1][:6]}...{row[1][-4:]}"
                text += f"<b>{i}.</b> @{name} — {row[2]} trades\n"
        else:
            text = "<b>🏆 Top Traders</b>\n\nNo traders yet. Be the first!"

        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Copy Trading", callback_data="nav:copy"),
             InlineKeyboardButton(text="◀️ Menu", callback_data="nav:menu")],
        ])
        await callback.message.edit_text(text, reply_markup=kb)  # type: ignore
        return

    text = fmt_leaderboard(traders)
    if len(text) > 4000:
        text = text[:4000] + "\n..."

    # Build per-trader copy buttons (top 5)
    sorted_traders = sorted(traders, key=lambda t: float(t.get("pnl_all_time", 0)), reverse=True)
    rows = []
    for t in sorted_traders[:5]:
        addr = t.get("address", "")
        if not addr:
            continue
        short = f"{addr[:6]}...{addr[-4:]}"
        rows.append([
            InlineKeyboardButton(text=f"📋 Copy {short}", callback_data=f"copy:start:{addr}"),
            InlineKeyboardButton(text=f"🔍 Inspect", callback_data=f"inspect:{addr}"),
        ])
    rows.append([
        InlineKeyboardButton(text="◀️ Copy Trading", callback_data="nav:copy"),
        InlineKeyboardButton(text="◀️ Menu", callback_data="nav:menu"),
    ])

    kb = InlineKeyboardMarkup(inline_keyboard=rows)
    await callback.message.edit_text(text, reply_markup=kb)  # type: ignore


# ------------------------------------------------------------------
# /top — top traders with sorting
# ------------------------------------------------------------------

@router.message(Command("top"))
async def cmd_top(message: Message):
    args = (message.text or "").split()
    sort_by = args[1].lower() if len(args) > 1 else "pnl"

    await message.answer("Loading top traders...")

    try:
        client = await _get_client()
        traders = await client.get_leaderboard(limit=100)
    except Exception as e:
        await message.answer(f"Error: {e}", reply_markup=back_to_menu_kb())
        return

    if not traders:
        await message.answer("No top traders data available.", reply_markup=main_menu_kb())
        return

    await message.answer(_fmt_top_traders(traders, sort_by), reply_markup=_top_kb(sort_by))


@router.callback_query(F.data.startswith("top:"))
async def cb_top_refresh(callback: CallbackQuery):
    sort_by = callback.data.split(":")[1]  # type: ignore
    await callback.answer("Refreshing...")

    try:
        client = await _get_client()
        traders = await client.get_leaderboard(limit=100)
    except Exception as e:
        await callback.message.edit_text(f"Error: {e}", reply_markup=back_to_menu_kb())  # type: ignore
        return

    await callback.message.edit_text(  # type: ignore
        _fmt_top_traders(traders, sort_by), reply_markup=_top_kb(sort_by),
    )


# ------------------------------------------------------------------
# /inspect <wallet> — inspect a trader's positions & stats
# ------------------------------------------------------------------

async def _build_inspect_text(wallet: str) -> str:
    """Build the inspect text for a wallet address."""
    client = await _get_client()

    acc_task = asyncio.create_task(client.get_account_info(account=wallet))
    pos_task = asyncio.create_task(client.get_positions(account=wallet))
    trades_task = asyncio.create_task(client.get_trades_history(account=wallet, limit=10))

    account = await acc_task
    positions = await pos_task
    trades = await trades_task

    equity = float(account.get("equity", 0))
    balance = float(account.get("balance", 0))
    margin_used = float(account.get("margin_used", 0))
    unrealized_pnl = float(account.get("unrealized_pnl", 0))
    upnl_sign = "+" if unrealized_pnl >= 0 else ""

    text = (
        f"<b>🔍 Trader: <code>{wallet[:8]}...{wallet[-4:]}</code></b>\n\n"
        f"💰 Equity: <b>${equity:,.2f}</b>\n"
        f"💵 Balance: ${balance:,.2f}\n"
        f"📊 Margin used: ${margin_used:,.2f}\n"
        f"📈 Unrealized PnL: {upnl_sign}${unrealized_pnl:,.2f}\n\n"
    )

    if positions:
        text += f"<b>📋 Open Positions ({len(positions)})</b>\n"
        for p in positions[:8]:
            sym = p.get("symbol", "?")
            side = "LONG" if p.get("side") == "bid" else "SHORT"
            side_emoji = "🟢" if p.get("side") == "bid" else "🔴"
            amt = abs(float(p.get("amount", 0)))
            entry = float(p.get("entry_price", 0))
            lev = p.get("leverage", "?")
            notional = amt * entry
            text += (
                f"  {side_emoji} {sym} {side} {lev}x\n"
                f"    Size: {amt} (${notional:,.0f}) | Entry: ${entry:,.2f}\n"
            )
        if len(positions) > 8:
            text += f"  <i>... +{len(positions) - 8} more</i>\n"
    else:
        text += "<i>No open positions</i>\n"

    if trades:
        text += f"\n<b>📜 Recent Trades</b>\n"
        for t in trades[:5]:
            sym = t.get("symbol", "?")
            side = t.get("side", "?")
            price = float(t.get("price", 0))
            pnl = float(t.get("pnl", 0))
            pnl_str = f" | PnL: {'+'if pnl >= 0 else ''}{pnl:,.2f}" if pnl else ""
            ts = t.get("timestamp", t.get("created_at", t.get("time", "")))
            date_str = ""
            if ts:
                try:
                    from datetime import datetime
                    if isinstance(ts, (int, float)):
                        dt = datetime.utcfromtimestamp(ts / 1000 if ts > 1e12 else ts)
                    else:
                        dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
                    date_str = f" | {dt.strftime('%d/%m %H:%M')}"
                except Exception:
                    pass
            text += f"  {sym} {side} @${price:,.2f}{pnl_str}{date_str}\n"

    if len(text) > 4000:
        text = text[:4000] + "\n..."

    return text


@router.message(Command("inspect"))
async def cmd_inspect(message: Message):
    args = (message.text or "").split()
    if len(args) < 2:
        await message.answer(
            "<b>Usage:</b> <code>/inspect &lt;wallet_address&gt;</code>\n\n"
            "Find wallets with /top or the leaderboard.",
            reply_markup=main_menu_kb(),
        )
        return

    wallet = args[1].strip()
    await message.answer(f"Inspecting <code>{wallet[:8]}...</code>...")

    try:
        text = await _build_inspect_text(wallet)
    except Exception as e:
        await message.answer(f"Error: {e}", reply_markup=back_to_menu_kb())
        return

    await message.answer(text, reply_markup=_inspect_kb(wallet))


@router.callback_query(F.data.startswith("inspect:"))
async def cb_inspect(callback: CallbackQuery):
    wallet = callback.data.split(":", 1)[1]  # type: ignore
    await callback.answer("Refreshing...")

    try:
        text = await _build_inspect_text(wallet)
    except Exception as e:
        await callback.message.edit_text(f"Error: {e}", reply_markup=back_to_menu_kb())  # type: ignore
        return

    await callback.message.edit_text(text, reply_markup=_inspect_kb(wallet))  # type: ignore


# ------------------------------------------------------------------
# copy:start — direct copy from inspect view
# ------------------------------------------------------------------

@router.callback_query(F.data.startswith("copy:start:"))
async def cb_copy_start_direct(callback: CallbackQuery):
    wallet = callback.data.split(":", 2)[2]  # type: ignore
    tg_id = callback.from_user.id
    await callback.answer()

    setup = _get_setup(tg_id, wallet)
    setup["wallet"] = wallet

    await callback.message.edit_text(  # type: ignore
        _fmt_setup(setup),
        reply_markup=copy_settings_kb(wallet, setup),
    )


# ------------------------------------------------------------------
# copy:add / copy:masters / copy:log — button callbacks
# ------------------------------------------------------------------

@router.callback_query(F.data == "copy:add")
async def copy_add(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.set_state(CopyStates.waiting_wallet)
    await callback.message.answer(  # type: ignore
        "<b>👥 Copy a Wallet</b>\n\n"
        "Paste the wallet address of the trader you want to copy.\n"
        "You can find top traders in 🏆 Top Traders."
    )


@router.callback_query(F.data == "copy:masters")
async def copy_masters(callback: CallbackQuery):
    await callback.answer()
    configs = await get_active_copy_configs(callback.from_user.id)

    if not configs:
        await callback.message.edit_text(  # type: ignore
            "<b>👥 My Masters</b>\n\n"
            "You're not copying anyone yet.\n"
            "Use ➕ Copy a Wallet to start.",
            reply_markup=copy_menu_kb(),
        )
        return

    text = "<b>👥 My Masters</b>\n\n"
    rows = []
    for cfg in configs:
        w = cfg["master_wallet"]
        short = f"{w[:6]}...{w[-4:]}"
        mode = cfg.get("sizing_mode", "proportional")
        if mode == "fixed_usd":
            size = f"${cfg.get('fixed_amount_usd', 10):.0f}/trade"
        elif mode == "pct_equity":
            size = f"{cfg.get('pct_equity', 5):.0f}% equity"
        else:
            size = f"{cfg['size_multiplier']}x"
        text += (
            f"{short} — {size} | Cap ${cfg['max_position_usd']:,.0f}\n\n"
        )
        rows.append([
            InlineKeyboardButton(text=f"🔍 {short}", callback_data=f"inspect:{w}"),
            InlineKeyboardButton(text=f"❌ Unfollow", callback_data=f"unfollow_ask:{w}"),
        ])

    rows.append([
        InlineKeyboardButton(text="➕ Copy a Wallet", callback_data="copy:add"),
        InlineKeyboardButton(text="◀️ Menu", callback_data="nav:menu"),
    ])
    kb = InlineKeyboardMarkup(inline_keyboard=rows)
    await callback.message.edit_text(text, reply_markup=kb)  # type: ignore


@router.callback_query(F.data == "copy:log")
async def copy_log_cb(callback: CallbackQuery):
    await callback.answer()
    trades = await get_trade_history(callback.from_user.id, limit=20)
    copy_trades = [t for t in trades if t["is_copy_trade"]]

    if not copy_trades:
        text = "<b>📜 Copy Log</b>\n\nNo copy trades yet."
    else:
        text = "<b>📜 Copy Log</b>\n\n"
        for t in copy_trades:
            side_label = "BUY" if t["side"] == "bid" else "SELL"
            text += (
                f"{t['symbol']} {side_label} {t['amount']} ({t['order_type']})\n"
                f"  Master: <code>{t['master_wallet'][:8]}...</code>\n"
            )

    await callback.message.edit_text(text, reply_markup=copy_menu_kb())  # type: ignore


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
# /copy command
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
# Settings callbacks
# ------------------------------------------------------------------

@router.callback_query(F.data.startswith("cmode:"))
async def copy_mode_callback(callback: CallbackQuery):
    parts = callback.data.split(":")  # type: ignore
    wallet, mode = parts[1], parts[2]
    setup = _get_setup(callback.from_user.id, wallet)
    setup["sizing_mode"] = mode
    await callback.answer(f"Mode: {mode.replace('_', ' ').title()}")
    await callback.message.edit_text(_fmt_setup(setup), reply_markup=copy_settings_kb(wallet, setup))  # type: ignore


@router.callback_query(F.data.startswith("camt:"))
async def copy_fixed_amount(callback: CallbackQuery):
    parts = callback.data.split(":")  # type: ignore
    wallet, amount = parts[1], float(parts[2])
    setup = _get_setup(callback.from_user.id, wallet)
    setup["sizing_mode"] = "fixed_usd"
    setup["fixed_amount_usd"] = amount
    await callback.answer(f"${amount}/trade")
    await callback.message.edit_text(_fmt_setup(setup), reply_markup=copy_settings_kb(wallet, setup))  # type: ignore


@router.callback_query(F.data.startswith("cpct:"))
async def copy_pct_equity(callback: CallbackQuery):
    parts = callback.data.split(":")  # type: ignore
    wallet, pct = parts[1], float(parts[2])
    setup = _get_setup(callback.from_user.id, wallet)
    setup["sizing_mode"] = "pct_equity"
    setup["pct_equity"] = pct
    await callback.answer(f"{pct}% of equity")
    await callback.message.edit_text(_fmt_setup(setup), reply_markup=copy_settings_kb(wallet, setup))  # type: ignore


@router.callback_query(F.data.startswith("cm:"))
async def copy_mult_callback(callback: CallbackQuery):
    parts = callback.data.split(":")  # type: ignore
    wallet, mult = parts[1], float(parts[2])
    setup = _get_setup(callback.from_user.id, wallet)
    setup["sizing_mode"] = "proportional"
    setup["size_multiplier"] = mult
    await callback.answer(f"Multiplier: {mult}x")
    await callback.message.edit_text(_fmt_setup(setup), reply_markup=copy_settings_kb(wallet, setup))  # type: ignore


@router.callback_query(F.data.startswith("cmin:"))
async def copy_min_trade(callback: CallbackQuery):
    parts = callback.data.split(":")  # type: ignore
    wallet, min_usd = parts[1], float(parts[2])
    setup = _get_setup(callback.from_user.id, wallet)
    setup["min_trade_usd"] = min_usd
    label = "Off" if min_usd == 0 else f"${min_usd:.0f}"
    await callback.answer(f"Min trigger: {label}")
    await callback.message.edit_text(_fmt_setup(setup), reply_markup=copy_settings_kb(wallet, setup))  # type: ignore


@router.callback_query(F.data.startswith("cx:"))
async def copy_max_callback(callback: CallbackQuery):
    parts = callback.data.split(":")  # type: ignore
    wallet, max_usd = parts[1], float(parts[2])
    setup = _get_setup(callback.from_user.id, wallet)
    setup["max_position_usd"] = max_usd
    await callback.answer(f"Cap: ${max_usd:,.0f}")
    await callback.message.edit_text(_fmt_setup(setup), reply_markup=copy_settings_kb(wallet, setup))  # type: ignore


@router.callback_query(F.data.startswith("ctotal:"))
async def copy_total_cap(callback: CallbackQuery):
    parts = callback.data.split(":")  # type: ignore
    wallet, total_usd = parts[1], float(parts[2])
    setup = _get_setup(callback.from_user.id, wallet)
    setup["max_total_usd"] = total_usd
    await callback.answer(f"Total cap: ${total_usd:,.0f}")
    await callback.message.edit_text(_fmt_setup(setup), reply_markup=copy_settings_kb(wallet, setup))  # type: ignore


@router.callback_query(F.data.startswith("copy_go:"))
async def copy_start_callback(callback: CallbackQuery):
    tg_id = callback.from_user.id
    wallet = callback.data.split(":", 1)[1]  # type: ignore
    setup = _copy_setup.pop(tg_id, None) or {**_DEFAULT_SETUP, "wallet": wallet}

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

    # Check if master has open positions to offer mirroring
    mirror_row = []
    try:
        client = await _get_client()
        positions = await client.get_positions(setup["wallet"])
        if positions:
            n = len(positions)
            mirror_row = [InlineKeyboardButton(
                text=f"📋 Mirror {n} current position{'s' if n > 1 else ''}",
                callback_data=f"copy_mirror:{setup['wallet']}",
            )]
    except Exception:
        pass

    rows = []
    if mirror_row:
        rows.append(mirror_row)
    rows.append([
        InlineKeyboardButton(text="◀️ Copy Trading", callback_data="nav:copy"),
        InlineKeyboardButton(text="◀️ Menu", callback_data="nav:menu"),
    ])

    await callback.message.edit_text(  # type: ignore
        f"<b>✅ Copy Active!</b>\n\n"
        f"Master: <code>{setup['wallet']}</code>\n"
        f"Size: {size_str} | Cap: ${setup['max_position_usd']:,.0f}{min_str}\n"
        f"Total exposure limit: ${setup.get('max_total_usd', 5000):,.0f}\n\n"
        f"The bot will mirror their <b>new trades</b> from now on."
        f"{f' The master has {len(positions)} open position(s) you can mirror.' if mirror_row else ''}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows),
    )


@router.callback_query(F.data.startswith("copy_mirror:"))
async def cb_copy_mirror(callback: CallbackQuery):
    """Mirror the master's current open positions."""
    master_wallet = callback.data.split(":", 1)[1]  # type: ignore
    tg_id = callback.from_user.id
    await callback.answer("Mirroring positions...")

    user = await get_user(tg_id)
    if not user or not user.get("pacifica_account"):
        return

    configs = await get_active_copy_configs(tg_id)
    cfg = next((c for c in configs if c["master_wallet"] == master_wallet), None)
    if not cfg:
        await callback.message.edit_text(  # type: ignore
            "Copy config not found. Start copying first.",
            reply_markup=copy_menu_kb(),
        )
        return

    try:
        client = await _get_client()
        positions = await client.get_positions(master_wallet)
    except Exception as e:
        await callback.message.edit_text(  # type: ignore
            f"Could not fetch master positions: {e}",
            reply_markup=copy_menu_kb(),
        )
        return

    if not positions:
        await callback.message.edit_text(  # type: ignore
            "Master has no open positions to mirror.",
            reply_markup=copy_menu_kb(),
        )
        return

    from bot.services.copy_engine import _replicate_open
    opened = 0
    errors = 0
    for pos in positions:
        try:
            await _replicate_open(callback.bot, master_wallet, pos, [cfg])
            opened += 1
        except Exception as e:
            logger.debug("Mirror position failed: %s", e)
            errors += 1

    error_line = f"\n{errors} failed" if errors else ""
    await callback.message.edit_text(  # type: ignore
        f"<b>✅ Mirrored {opened} position{'s' if opened != 1 else ''}</b>{error_line}\n\n"
        f"⚠️ Entry prices may differ from the master's original entries.",
        reply_markup=copy_menu_kb(),
    )


# ------------------------------------------------------------------
# /unfollow, /masters, /copylog
# ------------------------------------------------------------------

@router.callback_query(F.data.startswith("unfollow_ask:"))
async def cb_unfollow_ask(callback: CallbackQuery):
    wallet = callback.data.split(":", 1)[1]  # type: ignore
    short = f"{wallet[:6]}...{wallet[-4:]}"
    await callback.answer()
    await callback.message.edit_text(  # type: ignore
        f"<b>Stop copying {short}?</b>\n\n"
        f"This will stop replicating new trades from this master.\n"
        f"Your existing copied positions will stay open.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Yes, unfollow", callback_data=f"unfollow_yes:{wallet}"),
                InlineKeyboardButton(text="◀️ Cancel", callback_data="copy:masters"),
            ],
        ]),
    )


@router.callback_query(F.data.startswith("unfollow_yes:"))
async def cb_unfollow_yes(callback: CallbackQuery):
    wallet = callback.data.split(":", 1)[1]  # type: ignore
    tg_id = callback.from_user.id
    await callback.answer()
    await deactivate_copy_config(tg_id, wallet)
    # Clear the snapshot so re-following won't replay old positions
    from bot.services.copy_engine import _master_snapshots
    _master_snapshots.pop(wallet, None)
    short = f"{wallet[:6]}...{wallet[-4:]}"
    await callback.message.edit_text(  # type: ignore
        f"✅ Stopped copying <b>{short}</b>",
        reply_markup=copy_menu_kb(),
    )


@router.message(Command("unfollow"))
async def cmd_unfollow(message: Message):
    args = (message.text or "").split()
    if len(args) < 2:
        await message.answer("Usage: /unfollow <wallet_address>", reply_markup=copy_menu_kb())
        return
    wallet = args[1]
    tg_id = message.from_user.id  # type: ignore
    await deactivate_copy_config(tg_id, wallet)
    from bot.services.copy_engine import _master_snapshots
    _master_snapshots.pop(wallet, None)
    await message.answer(f"✅ Stopped copying <code>{wallet}</code>", reply_markup=copy_menu_kb())


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
