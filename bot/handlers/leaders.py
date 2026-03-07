"""
Copy Trading v2 — Public leader profiles, follower discovery, profit sharing.

Commands:
  /leader <name> [share%]  — Register as public leader (default 10% profit share)
  /leaders                 — Browse public leaders
  /follow <leader>         — Follow a leader (uses existing copy engine)
  /mystats                 — View your leader stats
  /stopleading             — Deactivate your leader profile
"""

import logging

from aiogram import Router, F
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton

from database.db import (
    get_user,
    create_leader_profile,
    get_leader_profile,
    get_public_leaders,
    get_leader_performance,
    deactivate_leader,
    update_leader_followers,
    add_copy_config,
)
from bot.utils.keyboards import main_menu_kb, back_to_menu_kb

logger = logging.getLogger(__name__)
router = Router()


# ------------------------------------------------------------------
# /leader — register as public leader
# ------------------------------------------------------------------

@router.message(Command("leader"))
async def cmd_leader(message: Message):
    tg_id = message.from_user.id  # type: ignore
    user = await get_user(tg_id)
    if not user or not user.get("pacifica_account"):
        await message.answer("Please /start and link your account first.", reply_markup=main_menu_kb())
        return

    username = user.get("username")
    if not username:
        await message.answer("Set your username first with /username", reply_markup=main_menu_kb())
        return

    args = (message.text or "").split()
    share_pct = 10.0
    if len(args) > 1:
        try:
            share_pct = float(args[1])
            share_pct = max(0, min(50, share_pct))  # cap at 50%
        except ValueError:
            pass

    # Use the user's username as display name
    display_name = username
    profile = await create_leader_profile(tg_id, display_name, profit_share_pct=share_pct)

    await message.answer(
        f"<b>Leader Profile Active!</b>\n\n"
        f"Name: <b>{profile['display_name']}</b>\n"
        f"Profit Share: <b>{profile['profit_share_pct']}%</b>\n"
        f"Wallet: <code>{user['pacifica_account'][:8]}...{user['pacifica_account'][-4:]}</code>\n\n"
        f"Other traders can now find and follow you via /leaders.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="View Leaders", callback_data="nav:leaders")],
            [InlineKeyboardButton(text="My Stats", callback_data="leader:mystats")],
            [InlineKeyboardButton(text="Menu", callback_data="nav:menu")],
        ]),
    )


# ------------------------------------------------------------------
# /leaders — browse public leaders
# ------------------------------------------------------------------

@router.message(Command("leaders"))
async def cmd_leaders(message: Message):
    await _show_leaders(message)


@router.callback_query(F.data == "nav:leaders")
async def nav_leaders(callback: CallbackQuery):
    await callback.answer()
    leaders = await get_public_leaders()

    if not leaders:
        text = (
            "<b>Public Leaders</b>\n\n"
            "No public leaders yet.\n"
            "Be the first: <code>/leader YourName</code>"
        )
        await callback.message.edit_text(text, reply_markup=back_to_menu_kb())  # type: ignore
        return

    text = await _build_leaders_text(leaders)
    await callback.message.edit_text(text, reply_markup=_leaders_kb())  # type: ignore


async def _show_leaders(message: Message):
    leaders = await get_public_leaders()

    if not leaders:
        text = (
            "<b>Public Leaders</b>\n\n"
            "No public leaders yet.\n"
            "Be the first: <code>/leader YourName</code>"
        )
        await message.answer(text, reply_markup=back_to_menu_kb())
        return

    text = await _build_leaders_text(leaders)
    await message.answer(text, reply_markup=_leaders_kb())


async def _build_leaders_text(leaders: list[dict]) -> str:
    text = "<b>Public Leaders</b>\n\n"

    for i, ldr in enumerate(leaders[:15], 1):
        name = ldr["display_name"]
        username = ldr.get("username") or name
        wallet = ldr.get("pacifica_account", "?")
        short_wallet = f"{wallet[:6]}...{wallet[-4:]}" if wallet and len(wallet) > 10 else wallet
        followers = ldr.get("total_followers", 0)
        share = ldr.get("profit_share_pct", 10)

        perf = await get_leader_performance(ldr["telegram_id"])
        win_rate = perf.get("win_rate", 0)
        total_pnl = perf.get("total_pnl", 0)
        total_trades = perf.get("total_trades", 0)

        pnl_str = f"+${total_pnl:,.0f}" if total_pnl >= 0 else f"-${abs(total_pnl):,.0f}"

        text += (
            f"<b>{i}. {name}</b> (@{username})\n"
            f"   <code>{short_wallet}</code>\n"
            f"   PnL: <b>{pnl_str}</b> | WR: {win_rate:.0f}% | Trades: {total_trades}\n"
            f"   Followers: {followers} | Share: {share}%\n"
            f"   <code>/follow {username}</code>\n\n"
        )

    return text


def _leaders_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="Refresh", callback_data="nav:leaders"),
            InlineKeyboardButton(text="Become Leader", callback_data="leader:register"),
        ],
        [InlineKeyboardButton(text="Menu", callback_data="nav:menu")],
    ])


# ------------------------------------------------------------------
# /follow <username_or_wallet> — follow a leader by username or wallet
# ------------------------------------------------------------------

@router.message(Command("follow"))
async def cmd_follow(message: Message):
    tg_id = message.from_user.id  # type: ignore
    args = (message.text or "").split()
    if len(args) < 2:
        await message.answer(
            "<b>Usage:</b>\n"
            "<code>/follow &lt;username&gt;</code>\n"
            "<code>/follow &lt;wallet_address&gt;</code>\n\n"
            "Browse leaders with /leaders",
            reply_markup=back_to_menu_kb(),
        )
        return

    query = args[1].strip().lstrip("@")
    user = await get_user(tg_id)
    if not user or not user.get("pacifica_account"):
        await message.answer("Please /start first.", reply_markup=main_menu_kb())
        return

    # Search by username or display name
    leaders = await get_public_leaders()
    leader = next(
        (l for l in leaders
         if (l.get("username") or "").lower() == query.lower()
         or l["display_name"].lower() == query.lower()),
        None,
    )

    if leader:
        leader_id = leader["telegram_id"]
        if leader_id == tg_id:
            await message.answer("You can't follow yourself!", reply_markup=back_to_menu_kb())
            return

        wallet = leader.get("pacifica_account", "")
        await add_copy_config(
            telegram_id=tg_id,
            master_wallet=wallet,
            sizing_mode="fixed_usd",
            fixed_amount_usd=10.0,
            max_position_usd=1000,
            max_total_usd=5000,
        )
        await update_leader_followers(leader_id, 1)

        leader_name = leader.get("username") or leader["display_name"]
        await message.answer(
            f"<b>Now Following: @{leader_name}</b>\n\n"
            f"Wallet: <code>{wallet[:8]}...{wallet[-4:]}</code>\n"
            f"Profit Share: {leader.get('profit_share_pct', 10)}%\n"
            f"Size: $10/trade (default)\n\n"
            f"Customize: <code>/copy {wallet}</code>\n"
            f"Stop: <code>/unfollow {wallet}</code>",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="View Leaders", callback_data="nav:leaders")],
                [InlineKeyboardButton(text="Menu", callback_data="nav:menu")],
            ]),
        )
        return

    # If not a leader name, treat as wallet address
    if len(query) > 20:
        await add_copy_config(
            telegram_id=tg_id,
            master_wallet=query,
            sizing_mode="fixed_usd",
            fixed_amount_usd=10.0,
            max_position_usd=1000,
            max_total_usd=5000,
        )
        await message.answer(
            f"<b>Now Copying Wallet</b>\n\n"
            f"<code>{query[:8]}...{query[-4:]}</code>\n"
            f"Size: $10/trade | Cap: $1,000\n\n"
            f"Customize: <code>/copy {query}</code>",
            reply_markup=back_to_menu_kb(),
        )
    else:
        await message.answer(
            f"Leader '@{query}' not found. Browse with /leaders",
            reply_markup=back_to_menu_kb(),
        )


# ------------------------------------------------------------------
# /mystats — view leader stats
# ------------------------------------------------------------------

@router.message(Command("mystats"))
async def cmd_mystats(message: Message):
    tg_id = message.from_user.id  # type: ignore
    await _show_stats(message, tg_id)


@router.callback_query(F.data == "leader:mystats")
async def cb_mystats(callback: CallbackQuery):
    await callback.answer()
    tg_id = callback.from_user.id
    profile = await get_leader_profile(tg_id)

    if not profile:
        await callback.message.edit_text(  # type: ignore
            "You're not a registered leader.\n"
            "Register with <code>/leader YourName</code>",
            reply_markup=back_to_menu_kb(),
        )
        return

    perf = await get_leader_performance(tg_id)
    text = _build_stats_text(profile, perf)
    await callback.message.edit_text(text, reply_markup=_stats_kb())  # type: ignore


async def _show_stats(message: Message, tg_id: int):
    profile = await get_leader_profile(tg_id)

    if not profile:
        await message.answer(
            "You're not a registered leader.\n"
            "Register with <code>/leader YourName</code>",
            reply_markup=back_to_menu_kb(),
        )
        return

    perf = await get_leader_performance(tg_id)
    text = _build_stats_text(profile, perf)
    await message.answer(text, reply_markup=_stats_kb())


def _build_stats_text(profile: dict, perf: dict) -> str:
    pnl = perf.get("total_pnl", 0)
    pnl_str = f"+${pnl:,.2f}" if pnl >= 0 else f"-${abs(pnl):,.2f}"
    shared = perf.get("total_shared", 0)

    return (
        f"<b>Leader Stats: {profile['display_name']}</b>\n\n"
        f"Status: {'Public' if profile.get('is_public') else 'Private'}\n"
        f"Followers: <b>{profile.get('total_followers', 0)}</b>\n"
        f"Profit Share: <b>{profile.get('profit_share_pct', 10)}%</b>\n\n"
        f"<b>Performance</b>\n"
        f"Total Trades: {perf.get('total_trades', 0)}\n"
        f"Win Rate: {perf.get('win_rate', 0):.1f}%\n"
        f"Follower PnL: {pnl_str}\n"
        f"Profit Earned: ${shared:,.2f}\n"
    )


def _stats_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="Refresh", callback_data="leader:mystats"),
            InlineKeyboardButton(text="View Leaders", callback_data="nav:leaders"),
        ],
        [InlineKeyboardButton(text="Menu", callback_data="nav:menu")],
    ])


# ------------------------------------------------------------------
# /stopleading — deactivate leader profile
# ------------------------------------------------------------------

@router.message(Command("stopleading"))
async def cmd_stop_leading(message: Message):
    tg_id = message.from_user.id  # type: ignore
    profile = await get_leader_profile(tg_id)
    if not profile:
        await message.answer("You're not a registered leader.", reply_markup=back_to_menu_kb())
        return

    await deactivate_leader(tg_id)
    await message.answer(
        f"<b>Leader profile deactivated.</b>\n\n"
        f"You won't appear in /leaders anymore.\n"
        f"Existing followers still copy until they /unfollow.\n"
        f"Re-register anytime with /leader",
        reply_markup=back_to_menu_kb(),
    )


# ------------------------------------------------------------------
# leader:register callback (from leaders list)
# ------------------------------------------------------------------

@router.callback_query(F.data == "leader:register")
async def cb_register_leader(callback: CallbackQuery):
    await callback.answer()
    await callback.message.edit_text(  # type: ignore
        "<b>Become a Public Leader</b>\n\n"
        "Register with:\n"
        "<code>/leader</code> — 10% profit share (default)\n"
        "<code>/leader 15</code> — 15% profit share\n\n"
        "Your username will be your leader name.\n"
        "Followers copy your trades, you earn a % of their profits.",
        reply_markup=back_to_menu_kb(),
    )
