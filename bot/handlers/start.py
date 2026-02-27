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

from database.db import get_user, create_user, update_user, get_or_create_ref_code, get_user_by_ref_code, count_referrals
from bot.services.wallet_manager import generate_wallet, import_wallet
from bot.services.pacifica_client import PacificaClient, PacificaAPIError
from bot.config import BUILDER_CODE, BUILDER_FEE_RATE, PACIFICA_NETWORK, PACIFICA_REFERRAL_CODE, BOT_USERNAME
from bot.utils.keyboards import (
    main_menu_kb,
    back_to_menu_kb,
    markets_kb,
    markets_all_kb,
    copy_menu_kb,
    settings_kb,
    positions_kb,
    onboarding_kb,
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

BOT_NAME = "Trident"
APP_URL = "https://test-app.pacifica.fi" if PACIFICA_NETWORK == "testnet" else "https://app.pacifica.fi"


class ImportStates(StatesGroup):
    waiting_private_key = State()


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
async def cmd_start(message: Message, state: FSMContext):
    tg_id = message.from_user.id  # type: ignore
    user = await get_user(tg_id)

    # Parse deeplink: /start ref_XXXXXX
    args = (message.text or "").split()
    ref_code = None
    if len(args) > 1 and args[1].startswith("ref_"):
        ref_code = args[1][4:]

    # Store referral code in FSM so we can use it after wallet setup
    if ref_code:
        await state.update_data(ref_code=ref_code)

    if user and user.get("pacifica_account"):
        wallet = user["pacifica_account"]
        await message.answer(
            f"<b>{BOT_NAME}</b> — Pacifica Perps\n\n"
            f"Wallet: <code>{wallet[:8]}...{wallet[-4:]}</code>",
            reply_markup=main_menu_kb(),
        )
        return

    # New or incomplete user — show wallet setup
    await message.answer(
        f"<b>{BOT_NAME}</b> — Trade perps on Pacifica from Telegram.\n\n"
        f"Import your Solana wallet or generate a new one:",
        reply_markup=onboarding_kb(),
    )


# ------------------------------------------------------------------
# Onboarding: Import or Generate wallet
# ------------------------------------------------------------------

@router.callback_query(F.data == "onboard:import")
async def onboard_import(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.set_state(ImportStates.waiting_private_key)
    await callback.message.edit_text(  # type: ignore
        "<b>Import Wallet</b>\n\n"
        "Paste your Solana wallet <b>private key</b> (base58).\n\n"
        "This is the same format used by Phantom, Solflare, etc.\n"
        "Your key is encrypted and stored locally.",
    )


async def _finish_wallet_setup(tg_id: int, pub: str, enc: str, state: FSMContext):
    """Common logic after wallet import/generate: save, track referral, auto-claim Pacifica code."""
    user = await get_user(tg_id)
    if user:
        await update_user(tg_id, pacifica_account=pub, agent_wallet_public=None, agent_wallet_encrypted=enc)
    else:
        user = await create_user(tg_id, None, enc)
        await update_user(tg_id, pacifica_account=pub)

    # Track referral if came via deeplink
    fsm_data = await state.get_data()
    ref_code = fsm_data.get("ref_code")
    if ref_code:
        referrer = await get_user_by_ref_code(ref_code)
        if referrer and referrer["telegram_id"] != tg_id:
            await update_user(tg_id, referred_by=referrer["telegram_id"])

    # Auto-claim Pacifica beta code in background
    if PACIFICA_REFERRAL_CODE:
        try:
            from bot.models.user import build_client_from_user
            u = await get_user(tg_id)
            client = build_client_from_user(u)
            await client.claim_referral_code(PACIFICA_REFERRAL_CODE)
            await client.close()
            logger.info("Auto-claimed Pacifica code for user %s", tg_id)
        except Exception as e:
            logger.debug("Auto-claim failed (may already be claimed): %s", e)

    await state.clear()


@router.message(ImportStates.waiting_private_key)
async def msg_import_key(message: Message, state: FSMContext):
    raw = (message.text or "").strip()

    # Delete the message containing the private key for safety
    try:
        await message.delete()
    except Exception:
        pass

    if len(raw) < 40 or " " in raw:
        await message.answer(
            "That doesn't look like a valid private key.\n"
            "It should be a base58 string (64-88 chars).\n\n"
            "Try again or generate a new wallet:",
            reply_markup=onboarding_kb(),
        )
        return

    try:
        pub, enc = import_wallet(raw)
    except Exception as e:
        await message.answer(
            f"Invalid private key: {e}\n\nTry again:",
            reply_markup=onboarding_kb(),
        )
        return

    tg_id = message.from_user.id  # type: ignore
    await _finish_wallet_setup(tg_id, pub, enc, state)

    await message.answer(
        f"<b>Wallet Imported!</b>\n\n"
        f"Address: <code>{pub}</code>\n\n"
        f"<b>Next steps:</b>\n"
        f"1. Deposit USDC on <a href='{APP_URL}'>Pacifica</a>\n"
        f"2. Start trading!\n\n"
        f"Your private key has been deleted from chat.",
        reply_markup=main_menu_kb(),
        disable_web_page_preview=True,
    )


@router.callback_query(F.data == "onboard:generate")
async def onboard_generate(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    tg_id = callback.from_user.id

    user = await get_user(tg_id)
    if user and user.get("pacifica_account"):
        await callback.message.edit_text(  # type: ignore
            "You already have a wallet set up!",
            reply_markup=main_menu_kb(),
        )
        return

    pub, enc = generate_wallet()
    await _finish_wallet_setup(tg_id, pub, enc, state)

    await callback.message.edit_text(  # type: ignore
        f"<b>Wallet Generated!</b>\n\n"
        f"Address:\n<code>{pub}</code>\n\n"
        f"<b>Next steps:</b>\n"
        f"1. Send SOL to this address (for tx fees)\n"
        f"2. Deposit USDC on <a href='{APP_URL}'>Pacifica</a>\n"
        f"3. Start trading!\n\n"
        f"Your private key is stored encrypted.",
        reply_markup=main_menu_kb(),
        disable_web_page_preview=True,
    )


# ------------------------------------------------------------------
# /menu
# ------------------------------------------------------------------

@router.message(Command("menu"))
async def cmd_menu(message: Message):
    await message.answer(
        f"<b>{BOT_NAME}</b>\nWhat do you want to do?",
        reply_markup=main_menu_kb(),
    )


# ------------------------------------------------------------------
# Navigation callbacks
# ------------------------------------------------------------------

@router.callback_query(F.data == "nav:menu")
async def nav_menu(callback: CallbackQuery):
    await callback.answer()
    await callback.message.edit_text(  # type: ignore
        f"<b>{BOT_NAME}</b>\nWhat do you want to do?",
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
            "Set up your wallet first — /start",
            reply_markup=back_to_menu_kb(),
        )
        return

    try:
        from bot.models.user import build_client_from_user
        client = build_client_from_user(user)
        positions = await client.get_positions()
        await client.close()
    except PacificaAPIError as e:
        if "not found" in str(e).lower():
            await callback.message.edit_text(  # type: ignore
                "<b>📈 Positions</b>\n\n"
                f"Account not found on Pacifica.\n"
                f"Deposit first at <a href='{APP_URL}'>Pacifica</a>",
                reply_markup=back_to_menu_kb(),
                disable_web_page_preview=True,
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
            "Set up your wallet first — /start", reply_markup=back_to_menu_kb(),
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
            "Set up your wallet first — /start", reply_markup=back_to_menu_kb(),
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
                f"Account not found on Pacifica.\n"
                f"Deposit first at <a href='{APP_URL}'>Pacifica</a>"
            )
        else:
            text = f"Error: {e}"
        await callback.message.edit_text(text, reply_markup=back_to_menu_kb(), disable_web_page_preview=True)  # type: ignore
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
            "Set up your wallet first — /start", reply_markup=back_to_menu_kb(),
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
        "Mirror trades from top wallets automatically.",
        reply_markup=copy_menu_kb(),
    )


@router.callback_query(F.data == "nav:settings")
async def nav_settings(callback: CallbackQuery):
    await callback.answer()
    user = await get_user(callback.from_user.id)

    if user and user.get("pacifica_account"):
        wallet = user["pacifica_account"]
        text = (
            f"<b>⚙️ Settings</b>\n\n"
            f"Network: <code>{PACIFICA_NETWORK}</code>\n"
            f"Wallet: <code>{wallet}</code>\n"
            f"Builder: {BUILDER_CODE} ({'✅' if user.get('builder_approved') else '⏳ pending'})\n\n"
            f"To deposit, send USDC to your wallet on <a href='{APP_URL}'>Pacifica</a>"
        )
    else:
        text = (
            f"<b>⚙️ Settings</b>\n\n"
            f"Network: <code>{PACIFICA_NETWORK}</code>\n\n"
            f"No wallet set up yet. Use /start to get started."
        )

    await callback.message.edit_text(text, reply_markup=settings_kb(), disable_web_page_preview=True)  # type: ignore


# ------------------------------------------------------------------
# Settings callbacks
# ------------------------------------------------------------------

@router.callback_query(F.data == "set:wallet")
async def set_wallet(callback: CallbackQuery):
    """Show wallet address for deposits."""
    await callback.answer()
    user = await get_user(callback.from_user.id)

    if not user or not user.get("pacifica_account"):
        await callback.message.answer("Set up your wallet first — /start")  # type: ignore
        return

    wallet = user["pacifica_account"]
    await callback.message.answer(  # type: ignore
        f"<b>💳 Your Wallet</b>\n\n"
        f"<code>{wallet}</code>\n\n"
        f"Send SOL (for tx fees) and USDC to this address.\n"
        f"Then deposit USDC into Pacifica at:\n"
        f"<a href='{APP_URL}'>{APP_URL}</a>",
        disable_web_page_preview=True,
    )


@router.callback_query(F.data == "set:import")
async def set_import(callback: CallbackQuery, state: FSMContext):
    """Switch wallet — import a different one."""
    await callback.answer()
    await state.set_state(ImportStates.waiting_private_key)
    await callback.message.answer(  # type: ignore
        "<b>Import Wallet</b>\n\n"
        "Paste your Solana wallet <b>private key</b> (base58).\n"
        "This will replace your current wallet.\n\n"
        "Your key is encrypted and stored locally.",
    )


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


@router.callback_query(F.data == "set:referral")
async def set_referral(callback: CallbackQuery):
    await callback.answer()
    tg_id = callback.from_user.id
    user = await get_user(tg_id)

    if not user or not user.get("pacifica_account"):
        await callback.message.answer(  # type: ignore
            "Set up your wallet first — /start",
        )
        return

    ref_code = await get_or_create_ref_code(tg_id)
    ref_link = f"https://t.me/{BOT_USERNAME}?start=ref_{ref_code}"
    ref_count = await count_referrals(tg_id)

    await callback.message.answer(  # type: ignore
        f"<b>🔗 Your Referral</b>\n\n"
        f"Share this link to earn from your friends' trades:\n\n"
        f"<code>{ref_link}</code>\n\n"
        f"Friends who join get reduced fees.\n"
        f"You earn a share of their trading fees.\n\n"
        f"Referrals: <b>{ref_count}</b>",
    )


# ------------------------------------------------------------------
# Copy trading callbacks
# ------------------------------------------------------------------

@router.callback_query(F.data == "copy:add")
async def copy_add(callback: CallbackQuery, state: FSMContext):
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
# /clear — reset user data (dev/testing)
# ------------------------------------------------------------------

@router.message(Command("clear"))
async def cmd_clear(message: Message, state: FSMContext):
    tg_id = message.from_user.id  # type: ignore
    await state.clear()

    from database.db import delete_user
    await delete_user(tg_id)

    await message.answer(
        f"<b>Data cleared.</b>\n\nTap /start to set up again.",
    )


# ------------------------------------------------------------------
# /help
# ------------------------------------------------------------------

@router.message(Command("help"))
async def cmd_help(message: Message):
    await message.answer(
        f"<b>{BOT_NAME}</b> — Pacifica Perps on Telegram\n\n"
        "Use the buttons to navigate, or type commands:\n\n"
        "<b>Navigation:</b>\n"
        "/menu — Main menu\n"
        "/markets — Live prices\n\n"
        "<b>Quick trading:</b>\n"
        "/long BTC 100 10x\n"
        "/short ETH 200 20x\n"
        "/close BTC\n\n"
        "<b>Copy trading:</b>\n"
        "/copy &lt;wallet&gt; [0.5x] [max=500]\n"
        "/unfollow &lt;wallet&gt;\n\n"
        "<b>Account:</b>\n"
        "/balance — Check balance\n"
        "/positions — Open positions\n",
        reply_markup=main_menu_kb(),
    )
