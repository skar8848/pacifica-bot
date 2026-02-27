"""
/start, onboarding, navigation hub, and settings.
All nav: callbacks route through here.
"""

import logging

from aiogram import Router, F
from aiogram.filters import CommandStart, Command
from aiogram.types import Message, CallbackQuery
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

from database.db import get_user, create_user, update_user
from bot.services.wallet_manager import generate_agent_wallet
from bot.services.pacifica_client import PacificaClient, PacificaAPIError
from bot.config import BUILDER_CODE, BUILDER_FEE_RATE, PACIFICA_NETWORK
from bot.utils.keyboards import (
    main_menu_kb,
    back_to_menu_kb,
    markets_kb,
    markets_all_kb,
    copy_menu_kb,
    settings_kb,
    positions_kb,
)
from bot.utils.formatters import (
    fmt_position,
    fmt_order,
    fmt_balance,
    fmt_pnl,
    fmt_leaderboard,
)

logger = logging.getLogger(__name__)
router = Router()

# Shared client for public (unauthenticated) API calls
_pub_client: PacificaClient | None = None


class LinkStates(StatesGroup):
    waiting_wallet = State()


class ClaimStates(StatesGroup):
    waiting_code = State()


async def _pub() -> PacificaClient:
    """Get a shared client for read-only public endpoints."""
    global _pub_client
    if _pub_client is None or (_pub_client._session and _pub_client._session.closed):
        from solders.keypair import Keypair
        _pub_client = PacificaClient(account="public", keypair=Keypair())
    return _pub_client


# ------------------------------------------------------------------
# /start
# ------------------------------------------------------------------

@router.message(CommandStart())
async def cmd_start(message: Message):
    tg_id = message.from_user.id  # type: ignore
    user = await get_user(tg_id)

    if user and user.get("pacifica_account"):
        await message.answer(
            f"<b>Pacifica Trading Bot</b>\n\n"
            f"Network: <code>{PACIFICA_NETWORK}</code>\n"
            f"Account: <code>{user['pacifica_account'][:12]}...</code>\n\n"
            f"What do you want to do?",
            reply_markup=main_menu_kb(),
        )
        return

    # New user or not linked yet
    if not user:
        pub, enc = generate_agent_wallet()
        user = await create_user(tg_id, pub, enc)
        agent_pub = pub
    else:
        agent_pub = user["agent_wallet_public"]

    app_url = "https://test-app.pacifica.fi" if PACIFICA_NETWORK == "testnet" else "https://app.pacifica.fi"
    await message.answer(
        f"<b>Welcome to Pacifica Trading Bot!</b>\n\n"
        f"Trade perpetual futures on Solana — right from Telegram.\n\n"
        f"Your agent wallet:\n<code>{agent_pub}</code>\n\n"
        f"<b>Quick setup (4 steps):</b>\n\n"
        f"1️⃣ Claim a referral code (🎟️ Claim Code below)\n"
        f"2️⃣ Deposit on <a href='{app_url}'>Pacifica</a>\n"
        f"3️⃣ Register the agent wallet above in your Pacifica settings\n"
        f"4️⃣ Approve builder code <b>{BUILDER_CODE}</b>\n\n"
        f"Then tap 🔗 Link Wallet to connect:",
        reply_markup=settings_kb(),
        disable_web_page_preview=True,
    )


# ------------------------------------------------------------------
# /menu
# ------------------------------------------------------------------

@router.message(Command("menu"))
async def cmd_menu(message: Message):
    await message.answer(
        "<b>Pacifica Trading Bot</b>\nWhat do you want to do?",
        reply_markup=main_menu_kb(),
    )


# ------------------------------------------------------------------
# Navigation callbacks
# ------------------------------------------------------------------

@router.callback_query(F.data == "nav:menu")
async def nav_menu(callback: CallbackQuery):
    await callback.answer()
    await callback.message.edit_text(  # type: ignore
        "<b>Pacifica Trading Bot</b>\nWhat do you want to do?",
        reply_markup=main_menu_kb(),
    )


@router.callback_query(F.data == "nav:markets")
async def nav_markets(callback: CallbackQuery):
    await callback.answer("Loading markets...")

    try:
        client = await _pub()
        markets = await client.get_markets_info()
    except Exception as e:
        logger.error("Failed to fetch markets: %s", e)
        await callback.message.edit_text(  # type: ignore
            f"<b>📊 Markets</b>\n\nCould not load markets: {e}",
            reply_markup=back_to_menu_kb(),
        )
        return

    # Fetch latest trade prices for top symbols
    prices: dict[str, str] = {}
    top = ["BTC", "ETH", "SOL", "TRUMP", "HYPE", "DOGE", "XRP", "SUI", "LINK", "AVAX"]
    for sym in top:
        try:
            trades = await client.get_trades(sym, limit=1)
            if trades:
                prices[sym] = trades[0].get("price", "?")
        except Exception:
            pass

    await callback.message.edit_text(  # type: ignore
        f"<b>📊 Markets</b> ({len(markets)} pairs)\n\nTap to trade:",
        reply_markup=markets_kb(markets, prices),
    )


@router.callback_query(F.data == "nav:trade")
async def nav_trade(callback: CallbackQuery):
    await nav_markets(callback)


@router.callback_query(F.data == "nav:markets_all")
async def nav_markets_all(callback: CallbackQuery):
    await callback.answer("Loading all markets...")
    try:
        client = await _pub()
        markets = await client.get_markets_info()
    except Exception as e:
        await callback.message.edit_text(  # type: ignore
            f"Error: {e}", reply_markup=back_to_menu_kb(),
        )
        return

    await callback.message.edit_text(  # type: ignore
        f"<b>📊 All Markets</b> ({len(markets)} pairs)\n\nTap to trade:",
        reply_markup=markets_all_kb(markets),
    )


@router.callback_query(F.data == "nav:leaderboard")
async def nav_leaderboard(callback: CallbackQuery):
    await callback.answer("Loading leaderboard...")
    try:
        client = await _pub()
        traders = await client.get_leaderboard()
    except Exception as e:
        await callback.message.edit_text(  # type: ignore
            f"Error: {e}", reply_markup=back_to_menu_kb(),
        )
        return

    text = fmt_leaderboard(traders)
    # Truncate if too long for Telegram (4096 chars)
    if len(text) > 4000:
        text = text[:4000] + "\n..."

    from bot.utils.keyboards import InlineKeyboardMarkup, InlineKeyboardButton
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ Markets", callback_data="nav:markets"),
         InlineKeyboardButton(text="◀️ Menu", callback_data="nav:menu")],
    ])
    await callback.message.edit_text(text, reply_markup=kb)  # type: ignore


@router.callback_query(F.data == "nav:positions")
async def nav_positions(callback: CallbackQuery):
    await callback.answer("Loading positions...")
    user = await get_user(callback.from_user.id)

    if not user or not user.get("pacifica_account"):
        await callback.message.edit_text(  # type: ignore
            "Link your account first in ⚙️ Settings.",
            reply_markup=settings_kb(),
        )
        return

    try:
        from bot.models.user import build_client_from_user
        client = build_client_from_user(user)
        positions = await client.get_positions()
        await client.close()
    except PacificaAPIError as e:
        # Account not found on Pacifica — hasn't deposited yet
        if "not found" in str(e).lower():
            await callback.message.edit_text(  # type: ignore
                "<b>📈 Positions</b>\n\n"
                "Account not found on Pacifica.\n"
                "Make sure you've deposited on app.pacifica.fi first!",
                reply_markup=back_to_menu_kb(),
            )
        else:
            await callback.message.edit_text(  # type: ignore
                f"Error: {e}", reply_markup=back_to_menu_kb(),
            )
        return
    except Exception as e:
        await callback.message.edit_text(  # type: ignore
            f"Error: {e}", reply_markup=back_to_menu_kb(),
        )
        return

    if not positions:
        text = "<b>📈 Positions</b>\n\nNo open positions."
    else:
        text = "<b>📈 Positions</b>\n\n"
        for pos in positions:
            text += fmt_position(pos) + "\n"

    await callback.message.edit_text(text, reply_markup=positions_kb(positions))  # type: ignore


@router.callback_query(F.data == "nav:orders")
async def nav_orders(callback: CallbackQuery):
    await callback.answer("Loading orders...")
    user = await get_user(callback.from_user.id)

    if not user or not user.get("pacifica_account"):
        await callback.message.edit_text(  # type: ignore
            "Link your account first.", reply_markup=settings_kb(),
        )
        return

    try:
        from bot.models.user import build_client_from_user
        client = build_client_from_user(user)
        orders = await client.get_open_orders()
        await client.close()
    except PacificaAPIError as e:
        if "not found" in str(e).lower():
            text = "<b>📋 Orders</b>\n\nNo orders (account not active on Pacifica yet)."
        else:
            text = f"Error: {e}"
        await callback.message.edit_text(text, reply_markup=back_to_menu_kb())  # type: ignore
        return
    except Exception as e:
        await callback.message.edit_text(f"Error: {e}", reply_markup=back_to_menu_kb())  # type: ignore
        return

    if not orders:
        text = "<b>📋 Orders</b>\n\nNo open orders."
    else:
        text = "<b>📋 Orders</b>\n\n"
        for o in orders:
            text += fmt_order(o) + "\n"

    await callback.message.edit_text(text, reply_markup=back_to_menu_kb())  # type: ignore


@router.callback_query(F.data == "nav:balance")
async def nav_balance(callback: CallbackQuery):
    await callback.answer("Loading balance...")
    user = await get_user(callback.from_user.id)

    if not user or not user.get("pacifica_account"):
        await callback.message.edit_text(  # type: ignore
            "Link your account first.", reply_markup=settings_kb(),
        )
        return

    try:
        from bot.models.user import build_client_from_user
        client = build_client_from_user(user)
        info = await client.get_account_info()
        await client.close()
    except PacificaAPIError as e:
        if "not found" in str(e).lower():
            text = (
                "<b>💰 Balance</b>\n\n"
                "Account not found on Pacifica.\n"
                "Deposit first at app.pacifica.fi"
            )
        else:
            text = f"Error: {e}"
        await callback.message.edit_text(text, reply_markup=back_to_menu_kb())  # type: ignore
        return
    except Exception as e:
        await callback.message.edit_text(f"Error: {e}", reply_markup=back_to_menu_kb())  # type: ignore
        return

    await callback.message.edit_text(fmt_balance(info), reply_markup=back_to_menu_kb())  # type: ignore


@router.callback_query(F.data == "nav:pnl")
async def nav_pnl(callback: CallbackQuery):
    await callback.answer("Loading PnL...")
    user = await get_user(callback.from_user.id)

    if not user or not user.get("pacifica_account"):
        await callback.message.edit_text(  # type: ignore
            "Link your account first.", reply_markup=settings_kb(),
        )
        return

    try:
        from bot.models.user import build_client_from_user
        client = build_client_from_user(user)
        trades = await client.get_trades_history()
        await client.close()
    except PacificaAPIError as e:
        if "not found" in str(e).lower():
            text = "<b>📉 PnL</b>\n\nNo trade history yet."
        else:
            text = f"Error: {e}"
        await callback.message.edit_text(text, reply_markup=back_to_menu_kb())  # type: ignore
        return
    except Exception as e:
        await callback.message.edit_text(f"Error: {e}", reply_markup=back_to_menu_kb())  # type: ignore
        return

    await callback.message.edit_text(fmt_pnl(trades), reply_markup=back_to_menu_kb())  # type: ignore


@router.callback_query(F.data == "nav:copy")
async def nav_copy(callback: CallbackQuery):
    await callback.answer()
    await callback.message.edit_text(  # type: ignore
        "<b>👥 Copy Trading</b>\n\n"
        "Mirror trades from top wallets automatically.\n"
        "Every copied trade includes your builder code for fee generation.",
        reply_markup=copy_menu_kb(),
    )


@router.callback_query(F.data == "nav:settings")
async def nav_settings(callback: CallbackQuery):
    await callback.answer()
    user = await get_user(callback.from_user.id)

    if user and user.get("pacifica_account"):
        text = (
            f"<b>⚙️ Settings</b>\n\n"
            f"Network: <code>{PACIFICA_NETWORK}</code>\n"
            f"Account: <code>{user['pacifica_account']}</code>\n"
            f"Agent: <code>{user['agent_wallet_public']}</code>\n"
            f"Builder: {BUILDER_CODE} ({'✅' if user.get('builder_approved') else '⏳ pending'})"
        )
    else:
        agent = user["agent_wallet_public"] if user else "Run /start first"
        text = (
            f"<b>⚙️ Settings</b>\n\n"
            f"Network: <code>{PACIFICA_NETWORK}</code>\n"
            f"Agent wallet: <code>{agent}</code>\n\n"
            f"Tap 🔗 Link Wallet to connect your Pacifica account."
        )

    await callback.message.edit_text(text, reply_markup=settings_kb())  # type: ignore


# ------------------------------------------------------------------
# Settings callbacks
# ------------------------------------------------------------------

@router.callback_query(F.data == "set:link")
async def set_link(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.set_state(LinkStates.waiting_wallet)
    await callback.message.answer(  # type: ignore
        "<b>🔗 Link Wallet</b>\n\n"
        "Send me your Pacifica wallet address.\n"
        "Just paste it here:"
    )


@router.message(LinkStates.waiting_wallet)
async def msg_link_wallet(message: Message, state: FSMContext):
    """User pasted their wallet address — no /link prefix needed."""
    wallet = (message.text or "").strip()
    await state.clear()

    if len(wallet) < 20 or " " in wallet:
        await message.answer(
            "That doesn't look like a valid wallet address. Try again:",
            reply_markup=settings_kb(),
        )
        return

    tg_id = message.from_user.id  # type: ignore
    user = await get_user(tg_id)
    if not user:
        await message.answer("Run /start first.")
        return

    await update_user(tg_id, pacifica_account=wallet)

    # Check builder approval
    approved = False
    try:
        client = await _pub()
        approvals = await client.get_builder_codes_approvals(wallet)
        approved = any(a.get("builder_code") == BUILDER_CODE for a in approvals)
        if approved:
            await update_user(tg_id, builder_approved=1)
    except Exception:
        pass

    status = "✅ approved" if approved else "⏳ not yet — approve on Pacifica"

    await message.answer(
        f"<b>Account Linked!</b>\n\n"
        f"Account: <code>{wallet}</code>\n"
        f"Agent: <code>{user['agent_wallet_public']}</code>\n"
        f"Builder ({BUILDER_CODE}): {status}\n\n"
        f"You're ready to trade!",
        reply_markup=main_menu_kb(),
    )


@router.callback_query(F.data == "set:agent")
async def set_agent(callback: CallbackQuery):
    await callback.answer()
    user = await get_user(callback.from_user.id)
    if user:
        await callback.message.answer(  # type: ignore
            f"<b>🔑 Your Agent Wallet</b>\n\n"
            f"<code>{user['agent_wallet_public']}</code>\n\n"
            f"Register this wallet on Pacifica:\n"
            f"Settings → Agent Wallets → Add"
        )
    else:
        await callback.message.answer("Run /start first.")  # type: ignore


@router.callback_query(F.data == "set:network")
async def set_network(callback: CallbackQuery):
    await callback.answer()
    from bot.config import PACIFICA_REST_URL, PACIFICA_WS_URL
    await callback.message.answer(  # type: ignore
        f"<b>📊 Network Info</b>\n\n"
        f"Network: <code>{PACIFICA_NETWORK}</code>\n"
        f"REST: <code>{PACIFICA_REST_URL}</code>\n"
        f"WS: <code>{PACIFICA_WS_URL}</code>\n"
        f"Builder: <code>{BUILDER_CODE}</code>\n"
        f"Fee rate: {BUILDER_FEE_RATE}"
    )


@router.callback_query(F.data == "set:claim")
async def set_claim(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    user = await get_user(callback.from_user.id)
    if not user or not user.get("pacifica_account"):
        await callback.message.answer(  # type: ignore
            "Link your wallet first (🔗 Link Wallet), then claim a code.",
            reply_markup=settings_kb(),
        )
        return
    await state.set_state(ClaimStates.waiting_code)
    await callback.message.answer(  # type: ignore
        "<b>🎟️ Claim Referral Code</b>\n\n"
        "Paste your Pacifica referral/beta code below.\n"
        "Get one from the Pacifica team:\n"
        "• Discord: discord.gg/pacifica\n"
        "• Telegram: @PacificaTGPortalBot\n"
        "• Email: ops@pacifica.fi"
    )


@router.message(ClaimStates.waiting_code)
async def msg_claim_code(message: Message, state: FSMContext):
    code = (message.text or "").strip()
    await state.clear()

    if not code or len(code) > 16 or " " in code:
        await message.answer(
            "Invalid code format (alphanumeric, max 16 chars). Try again:",
            reply_markup=settings_kb(),
        )
        return

    tg_id = message.from_user.id  # type: ignore
    user = await get_user(tg_id)
    if not user or not user.get("pacifica_account"):
        await message.answer("Link your wallet first.", reply_markup=settings_kb())
        return

    try:
        from bot.models.user import build_client_from_user
        client = build_client_from_user(user)
        result = await client.claim_referral_code(code)
        await client.close()

        await message.answer(
            f"<b>✅ Code Claimed!</b>\n\n"
            f"Code: <code>{code}</code>\n"
            f"You should now have beta/whitelist access.\n\n"
            f"Try trading now!",
            reply_markup=main_menu_kb(),
        )
    except PacificaAPIError as e:
        await message.answer(
            f"<b>❌ Claim Failed</b>\n\n{e}\n\n"
            f"Make sure the code is valid and hasn't been used.",
            reply_markup=settings_kb(),
        )
    except Exception as e:
        await message.answer(f"Error: {e}", reply_markup=settings_kb())


# ------------------------------------------------------------------
# Copy trading callbacks (handled here since copy:add/masters/log are nav-like)
# ------------------------------------------------------------------

@router.callback_query(F.data == "copy:add")
async def copy_add(callback: CallbackQuery, state: FSMContext):
    """Prompt user to paste a wallet to copy."""
    from bot.handlers.copy_trade import CopyStates
    await callback.answer()
    await state.set_state(CopyStates.waiting_wallet)
    await callback.message.answer(  # type: ignore
        "<b>👥 Copy a Wallet</b>\n\n"
        "Paste the wallet address of the trader you want to copy.\n"
        "You can find top traders in the 🏆 Leaderboard."
    )


@router.callback_query(F.data == "copy:masters")
async def copy_masters(callback: CallbackQuery):
    await callback.answer()
    from database.db import get_active_copy_configs
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
    for cfg in configs:
        w = cfg["master_wallet"]
        text += (
            f"<code>{w[:8]}...{w[-4:]}</code>\n"
            f"  Multiplier: {cfg['size_multiplier']}x | Max: ${cfg['max_position_usd']}\n"
            f"  /unfollow <code>{w}</code>\n\n"
        )
    await callback.message.edit_text(text, reply_markup=copy_menu_kb())  # type: ignore


@router.callback_query(F.data == "copy:log")
async def copy_log(callback: CallbackQuery):
    await callback.answer()
    from database.db import get_trade_history
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
# /link command (still works as shortcut)
# ------------------------------------------------------------------

@router.message(Command("link"))
async def cmd_link(message: Message):
    args = message.text.split() if message.text else []  # type: ignore
    if len(args) < 2:
        await message.answer(
            "Usage: /link <code>&lt;wallet_address&gt;</code>\n\n"
            "Or just tap 🔗 Link Wallet in Settings and paste your address."
        )
        return

    wallet = args[1].strip()
    tg_id = message.from_user.id  # type: ignore
    user = await get_user(tg_id)
    if not user:
        await message.answer("Run /start first.")
        return

    await update_user(tg_id, pacifica_account=wallet)

    approved = False
    try:
        client = await _pub()
        approvals = await client.get_builder_codes_approvals(wallet)
        approved = any(a.get("builder_code") == BUILDER_CODE for a in approvals)
        if approved:
            await update_user(tg_id, builder_approved=1)
    except Exception:
        pass

    status = "✅ approved" if approved else "⏳ not yet — approve on Pacifica"
    await message.answer(
        f"<b>Account Linked!</b>\n\n"
        f"Account: <code>{wallet}</code>\n"
        f"Builder ({BUILDER_CODE}): {status}",
        reply_markup=main_menu_kb(),
    )


# ------------------------------------------------------------------
# /help
# ------------------------------------------------------------------

@router.message(Command("help"))
async def cmd_help(message: Message):
    await message.answer(
        "<b>Pacifica Trading Bot</b>\n\n"
        "Use the buttons to navigate, or type commands:\n\n"
        "<b>Navigation:</b>\n"
        "/menu — Main menu\n"
        "/markets — Live prices\n\n"
        "<b>Quick trading:</b>\n"
        "/long BTC 0.1 10x\n"
        "/short ETH 1 20x\n"
        "/close BTC\n\n"
        "<b>Copy trading:</b>\n"
        "/copy &lt;wallet&gt; [0.5x] [max=500]\n"
        "/unfollow &lt;wallet&gt;\n\n"
        "<b>Account:</b>\n"
        "/link &lt;wallet&gt; — Link Pacifica\n"
        "/balance — Check balance\n"
        "/positions — Open positions\n",
        reply_markup=main_menu_kb(),
    )
