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

from database.db import (
    get_user, create_user, update_user, get_user_by_wallet,
    get_or_create_ref_code, get_user_by_ref_code, count_referrals,
    get_user_settings, set_user_setting,
    get_active_alerts, add_price_alert, delete_alert,
    get_referral_stats, claim_referral_fees, is_username_taken,
    REFERRAL_FEE_SHARE, REFEREE_FEE_REBATE,
)
from bot.services.wallet_manager import generate_wallet, import_wallet
from bot.services.pacifica_client import PacificaClient, PacificaAPIError
from bot.config import BUILDER_CODE, BUILDER_FEE_RATE, PACIFICA_NETWORK, PACIFICA_REFERRAL_CODE, BOT_USERNAME, DISPENSER_PRIVATE_KEY, DISPENSER_SOL_AMOUNT, DISPENSER_USDC_AMOUNT
from bot.utils.keyboards import (
    main_menu_kb,
    back_to_menu_kb,
    markets_kb,
    markets_all_kb,
    copy_menu_kb,
    settings_kb,
    slippage_menu_kb,
    leverage_menu_kb,
    positions_kb,
    onboarding_kb,
    alerts_kb,
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
APP_URL = "https://app.pacifica.fi" if PACIFICA_NETWORK == "mainnet" else "https://test-app.pacifica.fi"


class ImportStates(StatesGroup):
    waiting_private_key = State()


class AlertStates(StatesGroup):
    waiting_symbol = State()
    waiting_price = State()


class ReferralStates(StatesGroup):
    waiting_username = State()


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
        display_name = user.get("username") or message.from_user.first_name or "trader"
        # Quick account summary
        summary = ""
        try:
            from bot.models.user import build_client_from_user
            client = build_client_from_user(user)
            try:
                info = await client.get_account_info()
                bal = info.get("balance", "0")
                equity = info.get("account_equity", "0")
                positions = await client.get_positions()
                pos_count = len(positions)
                summary = (
                    f"\nBalance: <b>${bal}</b>\n"
                    f"Equity: ${equity}\n"
                    f"Open positions: {pos_count}\n"
                )
            except Exception:
                pass
            finally:
                await client.close()
        except Exception:
            pass

        # Show referrer if applicable
        referred_line = ""
        if user.get("referred_by"):
            referrer = await get_user(user["referred_by"])
            if referrer and referrer.get("username"):
                referred_line = f"\nReferred by: <b>{referrer['username']}</b>\n"

        await message.answer(
            f"Hey <b>{display_name}</b>, what do you want to trade?\n\n"
            f"Wallet: <code>{wallet[:8]}...{wallet[-4:]}</code>"
            f"{summary}{referred_line}",
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


@router.message(Command("import"))
async def cmd_import(message: Message, state: FSMContext):
    """Import an existing wallet via /import command."""
    await state.set_state(ImportStates.waiting_private_key)
    await message.answer(
        "<b>Import Wallet</b>\n\n"
        "Paste your Solana wallet <b>private key</b> (base58).\n\n"
        "This is the same format used by Phantom, Solflare, etc.\n"
        "Your key is encrypted and stored locally.",
    )


async def _finish_wallet_setup(tg_id: int, pub: str, enc: str, state: FSMContext):
    """Common logic after wallet import/generate: save, track referral, dispense SOL, auto-claim."""
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

    # Auto-dispense SOL on devnet so user can transact immediately
    import asyncio
    asyncio.create_task(_auto_dispense_sol(pub))

    # Auto-setup: claim beta code + approve builder code (background)
    asyncio.create_task(_auto_claim_setup(tg_id))

    await state.clear()


async def _auto_dispense_sol(wallet_pubkey: str):
    """Send SOL + USDC from dispenser wallet to new user (devnet only)."""
    try:
        from bot.services.solana_client import is_devnet, send_sol, send_usdc, get_sol_balance, get_usdc_balance
        if not is_devnet() or not DISPENSER_PRIVATE_KEY:
            return

        from solders.keypair import Keypair
        import base58
        dispenser_kp = Keypair.from_bytes(base58.b58decode(DISPENSER_PRIVATE_KEY))

        # Check dispenser SOL balance
        disp_sol = await get_sol_balance(str(dispenser_kp.pubkey()))
        if disp_sol < DISPENSER_SOL_AMOUNT + 0.02:
            logger.warning("Dispenser low on SOL: %.4f", disp_sol)
            return

        # Send SOL first (needed for USDC tx fees)
        user_sol = await get_sol_balance(wallet_pubkey)
        if user_sol < 0.05:
            sig = await send_sol(dispenser_kp, wallet_pubkey, DISPENSER_SOL_AMOUNT)
            logger.info("Dispensed %.2f SOL to %s: %s", DISPENSER_SOL_AMOUNT, wallet_pubkey, sig)
            import asyncio
            await asyncio.sleep(2)  # Wait for SOL to land

        # Send USDC if dispenser has enough
        if DISPENSER_USDC_AMOUNT > 0:
            disp_usdc = await get_usdc_balance(str(dispenser_kp.pubkey()))
            if disp_usdc >= DISPENSER_USDC_AMOUNT:
                sig = await send_usdc(dispenser_kp, wallet_pubkey, DISPENSER_USDC_AMOUNT)
                logger.info("Dispensed %d USDC to %s: %s", DISPENSER_USDC_AMOUNT, wallet_pubkey, sig)
            else:
                logger.warning("Dispenser low on USDC: %.0f (need %d)", disp_usdc, DISPENSER_USDC_AMOUNT)
    except Exception as e:
        logger.error("Dispense failed for %s: %s", wallet_pubkey, e)


async def _auto_claim_setup(tg_id: int):
    """Background: claim beta code + approve builder code."""
    import asyncio
    await asyncio.sleep(3)  # Wait for SOL to arrive
    try:
        from bot.models.user import build_client_from_user
        u = await get_user(tg_id)
        if not u:
            return
        client = build_client_from_user(u)
        try:
            if PACIFICA_REFERRAL_CODE:
                try:
                    await client.claim_referral_code(PACIFICA_REFERRAL_CODE)
                    logger.info("Auto-claimed beta code for user %s", tg_id)
                except Exception as e:
                    if "already" not in str(e).lower():
                        logger.debug("Beta claim failed: %s", e)

            try:
                await client.approve_builder_code(BUILDER_CODE, BUILDER_FEE_RATE)
                await update_user(tg_id, builder_approved=1)
                logger.info("Approved builder code for user %s", tg_id)
            except Exception as e:
                if "not found" not in str(e).lower():
                    logger.debug("Builder code approval failed: %s", e)
        finally:
            await client.close()
    except Exception as e:
        logger.debug("Auto-setup failed for %s: %s", tg_id, e)


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

    # Check if wallet is already used by another user
    existing = await get_user_by_wallet(pub, exclude_tg_id=tg_id)
    if existing:
        other_name = existing.get("username") or f"user #{existing['telegram_id']}"
        await message.answer(
            f"This wallet is already linked to <b>{other_name}</b>.\n\n"
            f"Each wallet can only be used by one account.\n"
            f"Generate a new wallet or use a different key.",
            reply_markup=onboarding_kb(),
        )
        return

    await _finish_wallet_setup(tg_id, pub, enc, state)

    from bot.utils.keyboards import wallet_kb
    await message.answer(
        f"<b>Wallet Imported!</b>\n\n"
        f"Address: <code>{pub}</code>\n\n"
        f"Your private key has been deleted from chat.\n\n"
        f"<b>Next:</b> Open your Wallet to get USDC and deposit.",
        reply_markup=wallet_kb(0, 0),
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

    from bot.utils.keyboards import wallet_kb
    await callback.message.edit_text(  # type: ignore
        f"<b>Wallet Generated!</b>\n\n"
        f"Address:\n<code>{pub}</code>\n\n"
        f"Your private key is stored encrypted.\n\n"
        f"<b>Next:</b> Get SOL + USDC from the wallet page below.",
        reply_markup=wallet_kb(0, 0),
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
        try:
            positions = await client.get_positions()
        finally:
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
        from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📊 Trade", callback_data="nav:markets")],
            [InlineKeyboardButton(text="◀️ Menu", callback_data="nav:menu")],
        ])
        await callback.message.edit_text(  # type: ignore
            "<b>📈 Positions</b>\n\n"
            "No open positions.\n\n"
            "Open a trade to get started!",
            reply_markup=kb,
        )
        return
    else:
        # Fetch mark prices and funding rates for all position symbols (parallel)
        import asyncio
        mark_prices: dict[str, float] = {}
        funding_rates: dict[str, float] = {}
        try:
            pub = await _pub()
            markets = await pub.get_markets_info()
            funding_map = {m.get("symbol"): float(m.get("funding_rate", 0) or 0) for m in markets}
            for pos in positions:
                sym = pos.get("symbol", "")
                if sym in funding_map:
                    funding_rates[sym] = funding_map[sym]

            # Fetch all prices in parallel
            async def _fetch_price(sym: str):
                try:
                    trades = await pub.get_trades(sym, limit=1)
                    if trades:
                        return sym, float(trades[0]["price"])
                except Exception:
                    pass
                return sym, None

            symbols = [pos.get("symbol", "") for pos in positions]
            results = await asyncio.gather(*[_fetch_price(s) for s in symbols])
            for sym, price in results:
                if price is not None:
                    mark_prices[sym] = price
        except Exception as e:
            logger.debug("Failed to fetch mark prices: %s", e)

        text = "<b>📈 Positions</b>\n\n"
        for pos in positions:
            sym = pos.get("symbol", "")
            text += fmt_position(
                pos,
                mark_price=mark_prices.get(sym),
                funding_rate=funding_rates.get(sym),
            ) + "\n"

    await callback.message.edit_text(text, reply_markup=positions_kb(positions, mark_prices))  # type: ignore


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
        try:
            orders = await client.get_open_orders()
        finally:
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

    # Build keyboard with cancel buttons for each order
    from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
    rows = []
    for o in (orders or []):
        oid = o.get("order_id", o.get("id", ""))
        sym = o.get("symbol", "?")
        if oid:
            rows.append([
                InlineKeyboardButton(
                    text=f"❌ Cancel {sym} #{str(oid)[:8]}",
                    callback_data=f"cancel_ord:{oid}:{sym}",
                )
            ])
    if orders:
        rows.append([
            InlineKeyboardButton(text="❌ Cancel All", callback_data="cancel_all_orders"),
        ])
    rows.append([
        InlineKeyboardButton(text="🔄 Refresh", callback_data="nav:orders"),
        InlineKeyboardButton(text="◀️ Menu", callback_data="nav:menu"),
    ])
    kb = InlineKeyboardMarkup(inline_keyboard=rows)
    await callback.message.edit_text(text, reply_markup=kb)  # type: ignore


@router.callback_query(F.data.startswith("cancel_ord:"))
async def cb_cancel_order(callback: CallbackQuery):
    """Cancel a specific order."""
    parts = callback.data.split(":")  # type: ignore
    order_id, symbol = parts[1], parts[2]
    user = await get_user(callback.from_user.id)
    if not user or not user.get("pacifica_account"):
        await callback.answer("Not linked")
        return

    await callback.answer(f"Cancelling order...")
    try:
        from bot.models.user import build_client_from_user
        client = build_client_from_user(user)
        try:
            await client.cancel_order(order_id, symbol)
        finally:
            await client.close()
        await callback.answer("Order cancelled!", show_alert=True)
        # Refresh orders
        await nav_orders(callback)
    except Exception as e:
        await callback.answer(f"Failed: {e}", show_alert=True)


@router.callback_query(F.data == "cancel_all_orders")
async def cb_cancel_all_orders(callback: CallbackQuery):
    """Cancel all open orders."""
    user = await get_user(callback.from_user.id)
    if not user or not user.get("pacifica_account"):
        await callback.answer("Not linked")
        return

    await callback.answer("Cancelling all orders...")
    try:
        from bot.models.user import build_client_from_user
        client = build_client_from_user(user)
        try:
            await client.cancel_all_orders()
        finally:
            await client.close()
        await callback.answer("All orders cancelled!", show_alert=True)
        await nav_orders(callback)
    except Exception as e:
        await callback.answer(f"Failed: {e}", show_alert=True)


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
        try:
            info = await client.get_account_info()
        finally:
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
        try:
            trades = await client.get_trades_history()
        finally:
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


@router.callback_query(F.data == "nav:history")
async def nav_history(callback: CallbackQuery):
    """Show trade history from Pacifica API."""
    await callback.answer("Loading history...")
    user = await get_user(callback.from_user.id)

    if not user or not user.get("pacifica_account"):
        await callback.message.edit_text(  # type: ignore
            "Set up your wallet first — /start", reply_markup=back_to_menu_kb(),
        )
        return

    try:
        from bot.models.user import build_client_from_user
        client = build_client_from_user(user)
        try:
            orders = await client.get_orders_history(limit=20)
        finally:
            await client.close()
    except PacificaAPIError as e:
        if "not found" in str(e).lower():
            text = "<b>📜 History</b>\n\nNo history yet."
        else:
            text = f"Error: {e}"
        await callback.message.edit_text(text, reply_markup=back_to_menu_kb())  # type: ignore
        return
    except Exception as e:
        await callback.message.edit_text(f"Error: {e}", reply_markup=back_to_menu_kb())  # type: ignore
        return

    if not orders:
        text = "<b>📜 History</b>\n\nNo orders yet. Start trading!"
    else:
        if orders:
            logger.info("Order history sample: %s", orders[0])
        text = "<b>📜 Order History</b>\n\n"
        for o in orders[:15]:
            symbol = o.get("symbol", "?")
            side = o.get("side", "?")
            amount = o.get("amount", "?")
            price = o.get("price") or o.get("fill_price") or o.get("avg_fill_price") or o.get("average_fill_price") or ""
            otype = o.get("order_type", o.get("type", "?"))
            status = o.get("status", "")
            direction = "BUY" if side == "bid" else "SELL"
            emoji = "🟢" if side == "bid" else "🔴"
            price_str = f" @ ${price}" if price else ""
            text += f"{emoji} {symbol} {direction} {amount}{price_str} ({otype}) {status}\n"

    if len(text) > 4000:
        text = text[:4000] + "\n..."

    from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Refresh", callback_data="nav:history"),
         InlineKeyboardButton(text="◀️ Menu", callback_data="nav:menu")],
    ])
    await callback.message.edit_text(text, reply_markup=kb)  # type: ignore


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

    user_settings = await get_user_settings(callback.from_user.id)
    await callback.message.edit_text(text, reply_markup=settings_kb(user_settings), disable_web_page_preview=True)  # type: ignore


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


@router.callback_query(F.data == "set:slippage_menu")
async def set_slippage_menu(callback: CallbackQuery):
    """Show slippage selection menu."""
    await callback.answer()
    user_settings = await get_user_settings(callback.from_user.id)
    current = user_settings.get("slippage", "0.5")
    await callback.message.edit_text(  # type: ignore
        f"<b>Slippage Tolerance</b>\n\n"
        f"Current: <b>{current}%</b>\n\n"
        f"Higher slippage = faster fills but worse price.\n"
        f"Lower slippage = better price but may fail.",
        reply_markup=slippage_menu_kb(),
    )


@router.callback_query(F.data == "set:leverage_menu")
async def set_leverage_menu(callback: CallbackQuery):
    """Show default leverage selection menu."""
    await callback.answer()
    user_settings = await get_user_settings(callback.from_user.id)
    current = user_settings.get("default_leverage", "10")
    await callback.message.edit_text(  # type: ignore
        f"<b>Default Leverage</b>\n\n"
        f"Current: <b>{current}x</b>\n\n"
        f"This is pre-selected when you open a new trade.\n"
        f"You can always change it per-trade.",
        reply_markup=leverage_menu_kb(),
    )


@router.callback_query(F.data.startswith("set:slippage:"))
async def set_slippage(callback: CallbackQuery):
    """Set slippage preference."""
    val = callback.data.split(":")[2]  # type: ignore
    await callback.answer(f"Slippage set to {val}%")
    await set_user_setting(callback.from_user.id, "slippage", val)
    # Go back to settings
    await nav_settings(callback)


@router.callback_query(F.data.startswith("set:deflev:"))
async def set_default_leverage(callback: CallbackQuery):
    """Set default leverage preference."""
    val = callback.data.split(":")[2]  # type: ignore
    await callback.answer(f"Default leverage set to {val}x")
    await set_user_setting(callback.from_user.id, "default_leverage", val)
    await nav_settings(callback)


@router.callback_query(F.data == "set:referral")
async def set_referral(callback: CallbackQuery):
    await callback.answer()
    tg_id = callback.from_user.id
    user = await get_user(tg_id)

    if not user or not user.get("pacifica_account"):
        await callback.message.edit_text(  # type: ignore
            "Set up your wallet first — /start",
            reply_markup=back_to_menu_kb(),
        )
        return

    ref_code = await get_or_create_ref_code(tg_id)
    current_name = user.get("username") or ""
    # Use username as ref link if set, fallback to random code
    ref_id = current_name if current_name else ref_code
    ref_link = f"https://t.me/{BOT_USERNAME}?start=ref_{ref_id}"
    stats = await get_referral_stats(tg_id)

    ref_pct = int(REFERRAL_FEE_SHARE * 100)
    rebate_pct = int(REFEREE_FEE_REBATE * 100)

    from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
    rows = []

    # Username button
    name_label = f"✏️ @{current_name}" if current_name else "✏️ Set Username"
    rows.append([InlineKeyboardButton(text=name_label, callback_data="ref:set_name")])

    if stats["unclaimed"] > 0.001:
        rows.append([InlineKeyboardButton(
            text=f"💰 Claim ${stats['unclaimed']:,.2f}",
            callback_data="ref:claim",
        )])
    rows.append([
        InlineKeyboardButton(text="🔄 Refresh", callback_data="set:referral"),
        InlineKeyboardButton(text="◀️ Menu", callback_data="nav:menu"),
    ])
    kb = InlineKeyboardMarkup(inline_keyboard=rows)

    name_line = f"Username: <b>@{current_name}</b>\n" if current_name else ""

    await callback.message.edit_text(  # type: ignore
        f"<b>🎟️ Referral Program</b>\n\n"
        f"{name_line}"
        f"Share your link to earn <b>{ref_pct}%</b> of your friends' trading fees.\n"
        f"They get a <b>{rebate_pct}%</b> fee rebate.\n\n"
        f"<b>Your link:</b>\n<code>{ref_link}</code>\n\n"
        f"<b>Stats:</b>\n"
        f"  Referrals: <b>{stats['referral_count']}</b>\n"
        f"  Volume generated: <b>${stats['total_volume']:,.0f}</b>\n"
        f"  Total earned: <b>${stats['total_earned']:,.2f}</b>\n"
        f"  Unclaimed: <b>${stats['unclaimed']:,.2f}</b>",
        reply_markup=kb,
    )


@router.callback_query(F.data == "ref:set_name")
async def cb_set_username(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.set_state(ReferralStates.waiting_username)
    user = await get_user(callback.from_user.id)
    current = user.get("username", "") if user else ""
    hint = f"\n\nCurrent: <b>@{current}</b>" if current else ""
    await callback.message.edit_text(  # type: ignore
        f"<b>Choose your username</b>\n\n"
        f"Pick a unique name (3-15 chars, letters/numbers/underscore only).\n"
        f"This is shown to your referrals and on your profile.{hint}",
    )


@router.message(ReferralStates.waiting_username)
async def msg_set_username(message: Message, state: FSMContext):
    import re
    raw = (message.text or "").strip().lstrip("@")
    tg_id = message.from_user.id  # type: ignore

    if not re.match(r'^[a-zA-Z0-9_]{3,15}$', raw):
        await message.answer(
            "Invalid username. Use 3-15 characters: letters, numbers, underscore.\n"
            "Try again:"
        )
        return

    if await is_username_taken(raw, exclude_tg_id=tg_id):
        await message.answer(
            f"<b>@{raw}</b> is already taken. Try another one:"
        )
        return

    await update_user(tg_id, username=raw)
    await state.clear()
    await message.answer(
        f"<b>✅ Username set!</b>\n\n"
        f"You are now <b>@{raw}</b>",
        reply_markup=main_menu_kb(),
    )


@router.callback_query(F.data == "ref:claim")
async def cb_claim_referral(callback: CallbackQuery):
    tg_id = callback.from_user.id
    claimed = await claim_referral_fees(tg_id)

    if claimed < 0.001:
        await callback.answer("Nothing to claim!", show_alert=True)
        return

    await callback.answer(f"Claimed ${claimed:,.2f}!", show_alert=True)
    # Refresh referral page
    await set_referral(callback)


# ------------------------------------------------------------------
# Price alerts
# ------------------------------------------------------------------

@router.callback_query(F.data == "nav:alerts")
async def nav_alerts(callback: CallbackQuery):
    await callback.answer()
    alerts = await get_active_alerts(callback.from_user.id)
    count = len(alerts)
    await callback.message.edit_text(  # type: ignore
        f"<b>🔔 Price Alerts</b> ({count} active)\n\n"
        f"Get notified when a price crosses your target.\n"
        f"Tap an alert to delete it.",
        reply_markup=alerts_kb(alerts),
    )


@router.callback_query(F.data == "alert:new")
async def alert_new(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.set_state(AlertStates.waiting_symbol)
    await callback.message.edit_text(  # type: ignore
        "<b>New Price Alert</b>\n\n"
        "Type the symbol (e.g. BTC, ETH, SOL):",
    )


@router.message(AlertStates.waiting_symbol)
async def msg_alert_symbol(message: Message, state: FSMContext):
    symbol = (message.text or "").strip().upper()
    if not symbol or len(symbol) > 10:
        await message.answer("Invalid symbol. Try again (e.g. BTC):")
        return
    await state.update_data(alert_symbol=symbol)
    await state.set_state(AlertStates.waiting_price)
    await message.answer(
        f"<b>Alert for {symbol}</b>\n\n"
        f"Type the target price.\n"
        f"Use <code>&gt;95000</code> for above or <code>&lt;90000</code> for below.\n"
        f"If no prefix, defaults to 'above' if higher than current price.",
    )


@router.message(AlertStates.waiting_price)
async def msg_alert_price(message: Message, state: FSMContext):
    raw = (message.text or "").strip()
    data = await state.get_data()
    symbol = data["alert_symbol"]

    # Parse direction
    direction = None
    if raw.startswith(">"):
        direction = "above"
        raw = raw[1:].strip()
    elif raw.startswith("<"):
        direction = "below"
        raw = raw[1:].strip()

    raw = raw.lstrip("$").replace(",", "")
    try:
        target = float(raw)
        if target <= 0:
            raise ValueError
    except (ValueError, TypeError):
        await message.answer("Invalid price. Try again (e.g. >95000 or <90000):")
        return

    # Auto-detect direction from current price if not specified
    if not direction:
        try:
            from solders.keypair import Keypair
            from bot.services.pacifica_client import PacificaClient
            client = PacificaClient(account="public", keypair=Keypair())
            trades = await client.get_trades(symbol, limit=1)
            await client.close()
            if trades:
                current = float(trades[0]["price"])
                direction = "above" if target > current else "below"
            else:
                direction = "above"
        except Exception:
            direction = "above"

    await state.clear()
    tg_id = message.from_user.id  # type: ignore
    alert_id = await add_price_alert(tg_id, symbol, direction, target)

    emoji = "📈" if direction == "above" else "📉"
    await message.answer(
        f"<b>✅ Alert Set!</b>\n\n"
        f"{emoji} {symbol} {direction} ${target:,.2f}\n\n"
        f"You'll be notified when the price crosses this level.",
        reply_markup=main_menu_kb(),
    )


@router.callback_query(F.data.startswith("alert_del:"))
async def alert_delete(callback: CallbackQuery):
    alert_id = int(callback.data.split(":")[1])  # type: ignore
    await delete_alert(alert_id, callback.from_user.id)
    await callback.answer("Alert deleted")
    # Refresh alerts list
    await nav_alerts(callback)


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

@router.message(Command("alert"))
async def cmd_alert(message: Message, state: FSMContext):
    """Shortcut: /alert BTC >95000"""
    args = (message.text or "").split()
    if len(args) < 3:
        await message.answer(
            "Usage: /alert <symbol> <price>\n\n"
            "Examples:\n"
            "/alert BTC >95000  (alert when above $95K)\n"
            "/alert ETH <3000   (alert when below $3K)\n"
            "/alert SOL 200     (auto-detect direction)",
            reply_markup=main_menu_kb(),
        )
        return

    symbol = args[1].upper()
    raw_price = args[2]

    direction = None
    if raw_price.startswith(">"):
        direction = "above"
        raw_price = raw_price[1:]
    elif raw_price.startswith("<"):
        direction = "below"
        raw_price = raw_price[1:]

    try:
        target = float(raw_price.lstrip("$").replace(",", ""))
        if target <= 0:
            raise ValueError
    except (ValueError, TypeError):
        await message.answer("Invalid price. Try /alert BTC >95000")
        return

    if not direction:
        try:
            from solders.keypair import Keypair
            from bot.services.pacifica_client import PacificaClient
            client = PacificaClient(account="public", keypair=Keypair())
            trades = await client.get_trades(symbol, limit=1)
            await client.close()
            if trades:
                current = float(trades[0]["price"])
                direction = "above" if target > current else "below"
            else:
                direction = "above"
        except Exception:
            direction = "above"

    tg_id = message.from_user.id  # type: ignore
    await add_price_alert(tg_id, symbol, direction, target)

    emoji = "📈" if direction == "above" else "📉"
    await message.answer(
        f"<b>✅ Alert Set!</b>\n\n"
        f"{emoji} {symbol} {direction} ${target:,.2f}",
        reply_markup=main_menu_kb(),
    )


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
        "/close BTC — Close position\n"
        "/closeall — Close all positions\n\n"
        "<b>Alerts:</b>\n"
        "/alert BTC &gt;95000 — Price above\n"
        "/alert ETH &lt;3000 — Price below\n\n"
        "<b>Copy trading:</b>\n"
        "/copy &lt;wallet&gt; [0.5x] [max=500]\n"
        "/unfollow &lt;wallet&gt;\n\n"
        "<b>Account:</b>\n"
        "/balance — Check balance\n"
        "/positions — Open positions\n",
        reply_markup=main_menu_kb(),
    )


# ------------------------------------------------------------------
# /update — git pull + restart (admin only)
# ------------------------------------------------------------------

@router.message(Command("update"))
async def cmd_update(message: Message):
    from bot.config import ADMIN_IDS
    tg_id = message.from_user.id  # type: ignore
    if tg_id not in ADMIN_IDS:
        await message.answer("Admin only.")
        return

    import os

    # Detect Render (or other cloud) — no git available, auto-deploy handles updates
    if os.environ.get("RENDER") or not os.path.isdir(
        os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), ".git")
    ):
        await message.answer(
            "<b>Auto-deploy enabled</b>\n\n"
            "Updates are deployed automatically when code is pushed to main.\n"
            "No manual update needed.",
            reply_markup=main_menu_kb(),
        )
        return

    import subprocess
    import sys

    await message.answer("Pulling latest code...")

    try:
        result = subprocess.run(
            ["git", "pull", "origin", "main"],
            capture_output=True, text=True, timeout=30,
            cwd=os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
        )
        output = result.stdout.strip() or result.stderr.strip() or "No output"

        if result.returncode != 0:
            await message.answer(f"<b>Git pull failed</b>\n\n<code>{output}</code>")
            return

        await message.answer(
            f"<b>Updated!</b>\n\n<code>{output}</code>\n\n"
            f"Restarting bot in 2 seconds...",
            reply_markup=main_menu_kb(),
        )

        # Give Telegram time to deliver the message
        import asyncio
        await asyncio.sleep(2)

        # Restart the process
        os.execv(sys.executable, [sys.executable, "-m", "bot.main"])

    except Exception as e:
        await message.answer(f"<b>Update failed</b>\n\n{e}")
