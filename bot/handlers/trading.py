"""
Interactive trading flow — button-driven + command shortcuts.
"""

import logging

from aiogram import Router, F
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

from database.db import get_user, update_user, log_trade, get_user_settings, log_referral_fee, REFERRAL_FEE_SHARE
from bot.models.user import build_client_from_user
from bot.services.pacifica_client import PacificaAPIError
from bot.services.market_data import get_price, get_market_info, get_lot_size, get_max_leverage, usd_to_token
from bot.handlers.wallet import ensure_beta_and_builder
from bot.config import BUILDER_CODE, BUILDER_FEE_RATE
from bot.utils.keyboards import (
    market_detail_kb,
    trade_amount_kb,
    trade_leverage_kb,
    confirm_trade_kb,
    confirm_limit_kb,
    position_detail_kb,
    confirm_close_kb,
    close_all_kb,
    back_to_menu_kb,
    main_menu_kb,
)
from bot.utils.formatters import fmt_position

logger = logging.getLogger(__name__)
router = Router()

# Approximate taker fee rate on Pacifica (for referral fee tracking)
_TAKER_FEE_RATE = 0.0004  # 0.04%


async def _track_referral_fee(tg_id: int, symbol: str, notional: float):
    """If user was referred, log a referral fee for the referrer."""
    try:
        user = await get_user(tg_id)
        if not user or not user.get("referred_by"):
            return
        referrer_id = user["referred_by"]
        trading_fee = notional * _TAKER_FEE_RATE
        referrer_share = trading_fee * REFERRAL_FEE_SHARE
        if referrer_share > 0:
            await log_referral_fee(referrer_id, tg_id, symbol, notional, referrer_share)
    except Exception as e:
        logger.debug("Referral fee tracking error: %s", e)


class TradeStates(StatesGroup):
    waiting_custom_amount = State()
    waiting_custom_leverage = State()
    waiting_tp_price = State()
    waiting_sl_price = State()
    waiting_limit_price = State()
    waiting_limit_amount = State()
    waiting_auto_tp = State()
    waiting_auto_sl = State()


# Aliases for backward compat within this file
_get_price = get_price
_get_max_leverage = get_max_leverage
_get_market_info = get_market_info
_get_lot_size = get_lot_size
_usdc_to_token = usd_to_token


# ------------------------------------------------------------------
# Market detail (tap on a market from the list)
# ------------------------------------------------------------------

@router.callback_query(F.data.startswith("market:"))
async def cb_market_detail(callback: CallbackQuery):
    symbol = callback.data.split(":")[1]  # type: ignore
    await callback.answer()

    # Fetch current price from latest trade + market info
    mark = "?"
    max_lev = "?"
    try:
        from solders.keypair import Keypair
        from bot.services.pacifica_client import PacificaClient
        temp_kp = Keypair()
        client = PacificaClient(account="public", keypair=temp_kp)
        trades = await client.get_trades(symbol, limit=1)
        if trades:
            mark = trades[0].get("price", "?")
        markets = await client.get_markets_info()
        m = next((x for x in markets if x.get("symbol") == symbol), None)
        if m:
            max_lev = m.get("max_leverage", "?")
        await client.close()
    except Exception:
        pass

    await callback.message.edit_text(  # type: ignore
        f"<b>{symbol}</b>\n\n"
        f"Price: <b>${mark}</b>\n"
        f"Max leverage: {max_lev}x\n\n"
        f"What do you want to do?",
        reply_markup=market_detail_kb(symbol),
    )


# ------------------------------------------------------------------
# Orderbook
# ------------------------------------------------------------------

@router.callback_query(F.data.startswith("ob:"))
async def cb_orderbook(callback: CallbackQuery):
    symbol = callback.data.split(":")[1]  # type: ignore
    await callback.answer("Loading orderbook...")

    try:
        from solders.keypair import Keypair
        from bot.services.pacifica_client import PacificaClient
        temp_kp = Keypair()
        client = PacificaClient(account="public", keypair=temp_kp)
        ob = await client.get_orderbook(symbol)
        await client.close()
    except Exception as e:
        await callback.message.edit_text(  # type: ignore
            f"Error loading orderbook: {e}",
            reply_markup=market_detail_kb(symbol),
        )
        return

    # Pacifica format: {"s": "BTC", "l": [[bids...], [asks...]]}
    # Each level: {"p": price, "a": amount, "n": count}
    levels = ob.get("l", [])
    bids = levels[0][:5] if len(levels) > 0 else []
    asks = levels[1][:5] if len(levels) > 1 else []

    text = f"<b>📊 {symbol} Orderbook</b>\n\n"
    text += "<b>Asks (Sells)</b>\n"
    for a in reversed(asks):
        text += f"  🔴 ${a['p']}  |  {a['a']}\n"
    text += "\n<b>Bids (Buys)</b>\n"
    for b in bids:
        text += f"  🟢 ${b['p']}  |  {b['a']}\n"

    await callback.message.edit_text(  # type: ignore
        text, reply_markup=market_detail_kb(symbol),
    )


# ------------------------------------------------------------------
# Chart — candlestick image from Pacifica kline data
# ------------------------------------------------------------------

@router.callback_query(F.data.startswith("chart:"))
async def cb_chart(callback: CallbackQuery):
    symbol = callback.data.split(":")[1]  # type: ignore
    await callback.answer("Generating chart...")

    from bot.services.chart import generate_chart
    from aiogram.types import BufferedInputFile

    chart_bytes = await generate_chart(symbol, interval="1h", num_candles=48)
    if not chart_bytes:
        await callback.message.edit_text(  # type: ignore
            f"Chart unavailable for {symbol}.",
            reply_markup=market_detail_kb(symbol),
        )
        return

    # Delete the old message to avoid stale buttons above the chart
    try:
        await callback.message.delete()  # type: ignore
    except Exception:
        pass

    photo = BufferedInputFile(chart_bytes, filename=f"chart_{symbol}.png")
    await callback.bot.send_photo(  # type: ignore
        chat_id=callback.from_user.id,
        photo=photo,
        caption=f"📈 <b>{symbol}</b> — 1H candles",
        reply_markup=market_detail_kb(symbol),
    )


# ------------------------------------------------------------------
# Trading flow: Select side → amount → leverage → confirm → execute
# ------------------------------------------------------------------

@router.callback_query(F.data.startswith("trade:"))
async def cb_trade_side(callback: CallbackQuery):
    """User picked long or short."""
    parts = callback.data.split(":")  # type: ignore
    side = parts[1]  # "long" or "short"
    symbol = parts[2]
    await callback.answer()

    direction = "🟢 LONG" if side == "long" else "🔴 SHORT"
    api_side = "bid" if side == "long" else "ask"

    price = await _get_price(symbol)
    price_str = f"\nCurrent price: ${price:,.2f}" if price else ""

    # Fetch equity for contextual % buttons
    equity = None
    try:
        user = await get_user(callback.from_user.id)
        if user:
            client = build_client_from_user(user)
            try:
                info = await client.get_account_info()
                equity = float(info.get("account_equity", info.get("equity", 0)) or 0)
            except Exception:
                pass
            finally:
                await client.close()
    except Exception:
        pass

    await callback.message.edit_text(  # type: ignore
        f"<b>{direction} {symbol}</b>{price_str}\n\n"
        f"Select amount (USDC):",
        reply_markup=trade_amount_kb(symbol, api_side, equity),
    )


@router.callback_query(F.data.startswith("amt:"))
async def cb_trade_amount(callback: CallbackQuery):
    """User picked a USDC amount."""
    parts = callback.data.split(":")  # type: ignore
    side, symbol, usdc_amount = parts[1], parts[2], parts[3]
    await callback.answer()

    direction = "🟢 LONG" if side == "bid" else "🔴 SHORT"
    max_lev = await _get_max_leverage(symbol)

    await callback.message.edit_text(  # type: ignore
        f"<b>{direction} {symbol}</b>\n"
        f"Size: <b>${usdc_amount} USDC</b>\n\n"
        f"Select leverage (max {max_lev}x):",
        reply_markup=trade_leverage_kb(symbol, side, usdc_amount, max_lev),
    )


@router.callback_query(F.data.startswith("amt_custom:"))
async def cb_custom_amount(callback: CallbackQuery, state: FSMContext):
    """User wants to type a custom USDC amount."""
    parts = callback.data.split(":")  # type: ignore
    side, symbol = parts[1], parts[2]
    await callback.answer()

    await state.set_state(TradeStates.waiting_custom_amount)
    await state.update_data(side=side, symbol=symbol)

    await callback.message.edit_text(  # type: ignore
        f"<b>{'🟢 LONG' if side == 'bid' else '🔴 SHORT'} {symbol}</b>\n\n"
        f"Type your amount in USDC (e.g. 150):"
    )


@router.message(TradeStates.waiting_custom_amount)
async def msg_custom_amount(message: Message, state: FSMContext):
    data = await state.get_data()
    side = data["side"]
    symbol = data["symbol"]
    raw = (message.text or "").strip().lstrip("$")

    try:
        float(raw)
    except (ValueError, TypeError):
        await message.answer("Invalid amount. Send a number in USDC (e.g. 150):")
        return

    await state.clear()
    max_lev = await _get_max_leverage(symbol)

    direction = "🟢 LONG" if side == "bid" else "🔴 SHORT"
    await message.answer(
        f"<b>{direction} {symbol}</b>\n"
        f"Size: <b>${raw} USDC</b>\n\n"
        f"Select leverage (max {max_lev}x):",
        reply_markup=trade_leverage_kb(symbol, side, raw, max_lev),
    )


@router.callback_query(F.data.startswith("lev:"))
async def cb_trade_leverage(callback: CallbackQuery):
    """User picked leverage → show confirmation with USDC conversion."""
    parts = callback.data.split(":")  # type: ignore
    side, symbol, usdc_amount, leverage = parts[1], parts[2], parts[3], parts[4]
    await callback.answer()

    price = await _get_price(symbol)
    if price:
        notional = float(usdc_amount) * float(leverage)
        lot_size = await _get_lot_size(symbol)
        token_amount = _usdc_to_token(notional, price, lot_size)
        price_line = f"Price: ${price:,.2f}\n"
        token_line = f"Token amount: ~{token_amount} {symbol}\n"
    else:
        price_line = ""
        token_line = ""

    direction = "🟢 LONG" if side == "bid" else "🔴 SHORT"
    await callback.message.edit_text(  # type: ignore
        f"<b>Confirm Order</b>\n\n"
        f"{direction} <b>{symbol}</b>\n"
        f"Size: ${usdc_amount} USDC\n"
        f"Leverage: {leverage}x\n"
        f"Notional: ${float(usdc_amount) * float(leverage):,.0f}\n"
        f"{price_line}"
        f"{token_line}"
        f"Type: Market\n"
        f"Builder fee: 0.05%",
        reply_markup=confirm_trade_kb(side, symbol, usdc_amount, leverage),
    )


@router.callback_query(F.data.startswith("lev_custom:"))
async def cb_custom_leverage(callback: CallbackQuery, state: FSMContext):
    """User wants to type custom leverage."""
    parts = callback.data.split(":")  # type: ignore
    side, symbol, usdc_amount = parts[1], parts[2], parts[3]
    await callback.answer()

    max_lev = await _get_max_leverage(symbol)
    await state.set_state(TradeStates.waiting_custom_leverage)
    await state.update_data(side=side, symbol=symbol, usdc_amount=usdc_amount, max_lev=max_lev)

    direction = "🟢 LONG" if side == "bid" else "🔴 SHORT"
    await callback.message.edit_text(  # type: ignore
        f"<b>{direction} {symbol}</b>\n"
        f"Size: ${usdc_amount} USDC\n\n"
        f"Type your leverage (max {max_lev}x):"
    )


@router.message(TradeStates.waiting_custom_leverage)
async def msg_custom_leverage(message: Message, state: FSMContext):
    data = await state.get_data()
    side = data["side"]
    symbol = data["symbol"]
    usdc_amount = data["usdc_amount"]
    max_lev = data["max_lev"]
    raw = (message.text or "").strip().lower().rstrip("x")

    try:
        lev = float(raw)
        if lev <= 0 or lev > max_lev:
            await message.answer(f"Leverage must be between 1 and {max_lev}. Try again:")
            return
    except (ValueError, TypeError):
        await message.answer(f"Invalid. Send a number (max {max_lev}):")
        return

    await state.clear()
    leverage = str(int(lev)) if lev == int(lev) else str(lev)

    price = await _get_price(symbol)
    if price:
        notional = float(usdc_amount) * float(leverage)
        lot_size = await _get_lot_size(symbol)
        token_amount = _usdc_to_token(notional, price, lot_size)
        price_line = f"Price: ${price:,.2f}\n"
        token_line = f"Token amount: ~{token_amount} {symbol}\n"
    else:
        price_line = ""
        token_line = ""

    direction = "🟢 LONG" if side == "bid" else "🔴 SHORT"
    await message.answer(
        f"<b>Confirm Order</b>\n\n"
        f"{direction} <b>{symbol}</b>\n"
        f"Size: ${usdc_amount} USDC\n"
        f"Leverage: {leverage}x\n"
        f"Notional: ${float(usdc_amount) * float(leverage):,.0f}\n"
        f"{price_line}"
        f"{token_line}"
        f"Type: Market\n"
        f"Builder fee: 0.05%",
        reply_markup=confirm_trade_kb(side, symbol, usdc_amount, leverage),
    )


@router.callback_query(F.data.startswith("exec:"))
async def cb_execute_trade(callback: CallbackQuery):
    """Execute the trade — converts USDC to token amount."""
    parts = callback.data.split(":")  # type: ignore
    side, symbol, usdc_amount, leverage = parts[1], parts[2], parts[3], parts[4]

    tg_id = callback.from_user.id
    user = await get_user(tg_id)

    if not user or not user.get("pacifica_account"):
        await callback.answer("Link your account first!")
        return

    await callback.answer("Sending order...")

    # Auto-claim beta code + builder approval if needed
    await ensure_beta_and_builder(user)

    # Convert USDC to token amount
    price = await _get_price(symbol)
    if not price:
        await callback.message.edit_text(  # type: ignore
            f"<b>❌ Could not fetch price for {symbol}</b>",
            reply_markup=market_detail_kb(symbol),
        )
        return

    notional = float(usdc_amount) * float(leverage)
    lot_size = await _get_lot_size(symbol)
    token_amount = _usdc_to_token(notional, price, lot_size)

    # Use user's slippage setting
    settings = await get_user_settings(tg_id)
    slippage = settings.get("slippage", "0.5")

    try:
        client = build_client_from_user(user)
        resp = await client.create_market_order(
            symbol=symbol,
            side=side,
            amount=token_amount,
            slippage=slippage,
        )
        await client.close()

        logger.info("Order response: %s", resp)
        order_id = resp.get("order_id", resp.get("id", "?"))
        fill_price = resp.get("fill_price", resp.get("price", resp.get("avg_fill_price", "")))

        await log_trade(tg_id, symbol, side, token_amount, str(fill_price), "market")
        await _track_referral_fee(tg_id, symbol, notional)

        direction = "🟢 LONG" if side == "bid" else "🔴 SHORT"
        price_line = f"Fill price: <b>${fill_price}</b>\n" if fill_price else ""

        from bot.config import PACIFICA_NETWORK
        app_base = "https://app.pacifica.fi" if PACIFICA_NETWORK == "mainnet" else "https://test-app.pacifica.fi"
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📊 View on Pacifica", url=f"{app_base}/portfolio")],
            *main_menu_kb().inline_keyboard,
        ])

        await callback.message.edit_text(  # type: ignore
            f"<b>✅ Order Executed!</b>\n\n"
            f"{direction} <b>{symbol}</b>\n"
            f"Size: ${usdc_amount} USDC ({token_amount} {symbol})\n"
            f"Leverage: {leverage}x\n"
            f"Notional: ${notional:,.0f}\n"
            f"{price_line}"
            f"Order ID: <code>{order_id}</code>",
            reply_markup=kb,
        )
    except PacificaAPIError as e:
        hint = _trade_error_hint(str(e))
        await callback.message.edit_text(  # type: ignore
            f"<b>❌ Order Failed</b>\n\n{e}{hint}",
            reply_markup=market_detail_kb(symbol),
        )


def _trade_error_hint(err: str) -> str:
    """Return a helpful hint based on the Pacifica error message."""
    low = err.lower()
    if "insufficient" in low or "balance" in low or "margin" in low:
        return (
            "\n\n💡 <b>You need to deposit first:</b>\n"
            "1. Send USDC (Solana) to your wallet\n"
            "2. Tap 💳 Wallet → Deposit"
        )
    if "not found" in low or "no account" in low or "does not exist" in low:
        return "\n\n💡 Your Pacifica account may not be activated yet. Deposit USDC to get started."
    if "invalid" in low and "signature" in low:
        return "\n\n💡 Signing error — try /clear and reimport your wallet."
    return ""


# ------------------------------------------------------------------
# Execute trade + auto TP/SL flow
# ------------------------------------------------------------------

@router.callback_query(F.data.startswith("exec_tpsl:"))
async def cb_exec_trade_with_tpsl(callback: CallbackQuery, state: FSMContext):
    """Execute trade then prompt for TP/SL."""
    parts = callback.data.split(":")  # type: ignore
    side, symbol, usdc_amount, leverage = parts[1], parts[2], parts[3], parts[4]

    tg_id = callback.from_user.id
    user = await get_user(tg_id)

    if not user or not user.get("pacifica_account"):
        await callback.answer("Link your account first!")
        return

    await callback.answer("Sending order...")

    # Auto-claim beta code + builder approval if needed
    await ensure_beta_and_builder(user)

    price = await _get_price(symbol)
    if not price:
        await callback.message.edit_text(  # type: ignore
            f"<b>❌ Could not fetch price for {symbol}</b>",
            reply_markup=market_detail_kb(symbol),
        )
        return

    notional = float(usdc_amount) * float(leverage)
    lot_size = await _get_lot_size(symbol)
    token_amount = _usdc_to_token(notional, price, lot_size)

    settings = await get_user_settings(tg_id)
    slippage = settings.get("slippage", "0.5")

    try:
        client = build_client_from_user(user)
        resp = await client.create_market_order(
            symbol=symbol, side=side, amount=token_amount, slippage=slippage,
        )
        await client.close()

        logger.info("Order+TPSL response: %s", resp)
        order_id = resp.get("order_id", resp.get("id", "?"))
        fill_price = resp.get("fill_price", resp.get("price", resp.get("avg_fill_price", str(price))))

        await log_trade(tg_id, symbol, side, token_amount, str(fill_price), "market")
        await _track_referral_fee(tg_id, symbol, notional)

        # Store trade info for TP/SL flow
        await state.update_data(
            tpsl_symbol=symbol,
            tpsl_side=side,
            tpsl_fill_price=fill_price or str(price),
        )
        await state.set_state(TradeStates.waiting_auto_tp)

        direction = "🟢 LONG" if side == "bid" else "🔴 SHORT"
        price_display = fill_price or str(price)
        await callback.message.edit_text(  # type: ignore
            f"<b>✅ Order Executed!</b>\n\n"
            f"{direction} <b>{symbol}</b> @ ${price_display}\n"
            f"Size: ${usdc_amount} USDC | {leverage}x\n\n"
            f"Now set your <b>Take Profit</b> price:\n"
            f"(or type <code>skip</code> to skip)",
        )
    except PacificaAPIError as e:
        hint = _trade_error_hint(str(e))
        await callback.message.edit_text(  # type: ignore
            f"<b>❌ Order Failed</b>\n\n{e}{hint}",
            reply_markup=market_detail_kb(symbol),
        )


@router.message(TradeStates.waiting_auto_tp)
async def msg_auto_tp(message: Message, state: FSMContext):
    raw = (message.text or "").strip()
    data = await state.get_data()
    symbol = data["tpsl_symbol"]
    side = data["tpsl_side"]

    if raw.lower() in ("skip", "s", "0", "no"):
        await state.set_state(TradeStates.waiting_auto_sl)
        await message.answer(
            f"<b>TP skipped.</b>\n\n"
            f"Now set your <b>Stop Loss</b> price for {symbol}:\n"
            f"(or type <code>skip</code> to skip)",
        )
        return

    tp_price = raw.lstrip("$").replace(",", "")
    try:
        float(tp_price)
    except (ValueError, TypeError):
        await message.answer("Invalid price. Enter a number or 'skip':")
        return

    tg_id = message.from_user.id  # type: ignore
    user = await get_user(tg_id)
    try:
        client = build_client_from_user(user)
        # Normalize side to bid/ask
        normalized_side = "bid" if side.lower() in ("bid", "buy", "long") else "ask"
        logger.info("Setting TP for %s side=%s stop_price=%s", symbol, normalized_side, tp_price)
        await client.set_tpsl(symbol=symbol, side=normalized_side, take_profit={"stop_price": str(tp_price)})
        await client.close()
    except Exception as e:
        logger.error("TP set failed: %s", e)
        await message.answer(f"TP failed: {e}")

    await state.set_state(TradeStates.waiting_auto_sl)
    await message.answer(
        f"<b>✅ TP set @ ${tp_price}</b>\n\n"
        f"Now set your <b>Stop Loss</b> price for {symbol}:\n"
        f"(or type <code>skip</code> to skip)",
    )


@router.message(TradeStates.waiting_auto_sl)
async def msg_auto_sl(message: Message, state: FSMContext):
    raw = (message.text or "").strip()
    data = await state.get_data()
    symbol = data["tpsl_symbol"]
    side = data["tpsl_side"]

    await state.clear()

    if raw.lower() in ("skip", "s", "0", "no"):
        await message.answer(
            f"<b>SL skipped. Trade complete!</b>",
            reply_markup=main_menu_kb(),
        )
        return

    sl_price = raw.lstrip("$").replace(",", "")
    try:
        float(sl_price)
    except (ValueError, TypeError):
        await message.answer(
            "Invalid price. Trade placed without SL.",
            reply_markup=main_menu_kb(),
        )
        return

    tg_id = message.from_user.id  # type: ignore
    user = await get_user(tg_id)
    try:
        client = build_client_from_user(user)
        normalized_side = "bid" if side.lower() in ("bid", "buy", "long") else "ask"
        await client.set_tpsl(symbol=symbol, side=normalized_side, stop_loss={"stop_price": str(sl_price)})
        await client.close()
    except Exception as e:
        logger.error("SL set failed: %s", e)
        await message.answer(f"SL failed: {e}", reply_markup=main_menu_kb())
        return

    await message.answer(
        f"<b>✅ SL set @ ${sl_price}</b>\n\n"
        f"Trade complete with TP/SL protection!",
        reply_markup=main_menu_kb(),
    )


# ------------------------------------------------------------------
# Limit order flow: side → price → amount → leverage → confirm → exec
# ------------------------------------------------------------------

@router.callback_query(F.data.startswith("limit:"))
async def cb_limit_side(callback: CallbackQuery, state: FSMContext):
    """User picked limit buy or limit sell."""
    parts = callback.data.split(":")  # type: ignore
    side, symbol = parts[1], parts[2]
    await callback.answer()

    price = await _get_price(symbol)
    price_str = f"\nCurrent price: ${price:,.2f}" if price else ""

    direction = "📗 LIMIT BUY" if side == "bid" else "📕 LIMIT SELL"
    await state.set_state(TradeStates.waiting_limit_price)
    await state.update_data(side=side, symbol=symbol)

    await callback.message.edit_text(  # type: ignore
        f"<b>{direction} {symbol}</b>{price_str}\n\n"
        f"Enter your limit price (e.g. 95000):",
    )


@router.message(TradeStates.waiting_limit_price)
async def msg_limit_price(message: Message, state: FSMContext):
    raw = (message.text or "").strip().lstrip("$").replace(",", "")
    try:
        float(raw)
    except (ValueError, TypeError):
        await message.answer("Invalid price. Enter a number (e.g. 95000):")
        return

    data = await state.get_data()
    side = data["side"]
    symbol = data["symbol"]

    await state.set_state(TradeStates.waiting_limit_amount)
    await state.update_data(limit_price=raw)

    direction = "📗 LIMIT BUY" if side == "bid" else "📕 LIMIT SELL"
    await message.answer(
        f"<b>{direction} {symbol} @ ${raw}</b>\n\n"
        f"Enter amount in USDC (e.g. 100):",
    )


@router.message(TradeStates.waiting_limit_amount)
async def msg_limit_amount(message: Message, state: FSMContext):
    raw = (message.text or "").strip().lstrip("$").replace(",", "")
    try:
        float(raw)
    except (ValueError, TypeError):
        await message.answer("Invalid amount. Enter a number in USDC:")
        return

    data = await state.get_data()
    side = data["side"]
    symbol = data["symbol"]
    limit_price = data["limit_price"]
    await state.clear()

    # Get user default leverage
    tg_id = message.from_user.id  # type: ignore
    settings = await get_user_settings(tg_id)
    leverage = settings.get("default_leverage", "10")

    price_f = float(limit_price)
    notional = float(raw) * float(leverage)
    lot_size = await _get_lot_size(symbol)
    token_amount = _usdc_to_token(notional, price_f, lot_size)

    direction = "📗 LIMIT BUY" if side == "bid" else "📕 LIMIT SELL"
    await message.answer(
        f"<b>Confirm Limit Order</b>\n\n"
        f"{direction} <b>{symbol}</b>\n"
        f"Limit price: ${limit_price}\n"
        f"Size: ${raw} USDC\n"
        f"Leverage: {leverage}x\n"
        f"Notional: ${notional:,.0f}\n"
        f"Token amount: ~{token_amount} {symbol}\n"
        f"Type: Limit (GTC)\n"
        f"Builder fee: 0.05%",
        reply_markup=confirm_limit_kb(side, symbol, raw, limit_price, leverage),
    )


@router.callback_query(F.data.startswith("exec_limit:"))
async def cb_exec_limit(callback: CallbackQuery):
    """Execute limit order."""
    parts = callback.data.split(":")  # type: ignore
    side, symbol, usdc_amount, limit_price, leverage = parts[1], parts[2], parts[3], parts[4], parts[5]

    tg_id = callback.from_user.id
    user = await get_user(tg_id)

    if not user or not user.get("pacifica_account"):
        await callback.answer("Link your account first!")
        return

    await callback.answer("Placing limit order...")

    # Auto-claim beta code + builder approval if needed
    await ensure_beta_and_builder(user)

    price_f = float(limit_price)
    notional = float(usdc_amount) * float(leverage)
    lot_size = await _get_lot_size(symbol)
    token_amount = _usdc_to_token(notional, price_f, lot_size)

    try:
        client = build_client_from_user(user)
        resp = await client.create_limit_order(
            symbol=symbol,
            side=side,
            amount=token_amount,
            price=str(price_f),
        )
        await client.close()

        order_id = resp.get("order_id", resp.get("id", "?"))

        await log_trade(tg_id, symbol, side, token_amount, limit_price, "limit")
        await _track_referral_fee(tg_id, symbol, notional)

        direction = "📗 BUY" if side == "bid" else "📕 SELL"
        await callback.message.edit_text(  # type: ignore
            f"<b>✅ Limit Order Placed!</b>\n\n"
            f"{direction} <b>{symbol}</b>\n"
            f"Price: ${limit_price}\n"
            f"Size: ${usdc_amount} USDC ({token_amount} {symbol})\n"
            f"Leverage: {leverage}x\n"
            f"Order ID: <code>{order_id}</code>",
            reply_markup=main_menu_kb(),
        )
    except PacificaAPIError as e:
        hint = _trade_error_hint(str(e))
        await callback.message.edit_text(  # type: ignore
            f"<b>❌ Limit Order Failed</b>\n\n{e}{hint}",
            reply_markup=market_detail_kb(symbol),
        )


# ------------------------------------------------------------------
# Position management via buttons
# ------------------------------------------------------------------

@router.callback_query(F.data.startswith("pos:"))
async def cb_position_detail(callback: CallbackQuery):
    symbol = callback.data.split(":")[1]  # type: ignore
    tg_id = callback.from_user.id
    user = await get_user(tg_id)

    if not user or not user.get("pacifica_account"):
        await callback.answer("Not linked")
        return

    await callback.answer()

    try:
        client = build_client_from_user(user)
        positions = await client.get_positions()
        await client.close()
    except Exception as e:
        await callback.message.edit_text(f"Error: {e}", reply_markup=back_to_menu_kb())  # type: ignore
        return

    pos = next((p for p in positions if p.get("symbol", "").upper() == symbol.upper()), None)
    if not pos:
        await callback.message.edit_text(  # type: ignore
            f"No open position for {symbol}.",
            reply_markup=back_to_menu_kb(),
        )
        return

    # Fetch mark price and funding rate for full display
    mark_price = await _get_price(symbol)
    funding_rate = None
    try:
        from solders.keypair import Keypair as _Kp
        from bot.services.pacifica_client import PacificaClient as _PC
        _c = _PC(account="public", keypair=_Kp())
        markets = await _c.get_markets_info()
        await _c.close()
        m = next((x for x in markets if x.get("symbol") == symbol), None)
        if m:
            funding_rate = float(m.get("funding_rate", 0) or 0)
    except Exception:
        pass

    await callback.message.edit_text(  # type: ignore
        f"<b>Position Detail</b>\n\n{fmt_position(pos, mark_price=mark_price, funding_rate=funding_rate)}",
        reply_markup=position_detail_kb(symbol, pos.get("side", "bid")),
    )


@router.callback_query(F.data.startswith("share_pnl:"))
async def cb_share_pnl(callback: CallbackQuery):
    """Generate and send PnL share card as image."""
    symbol = callback.data.split(":")[1]  # type: ignore
    tg_id = callback.from_user.id
    user = await get_user(tg_id)

    if not user or not user.get("pacifica_account"):
        await callback.answer("Not linked")
        return

    await callback.answer("Generating PnL card...")

    client = build_client_from_user(user)
    try:
        positions = await client.get_positions()
    except Exception as e:
        await callback.answer(f"Error: {e}", show_alert=True)
        return
    finally:
        await client.close()

    pos = next((p for p in positions if p.get("symbol", "").upper() == symbol.upper()), None)
    if not pos:
        await callback.answer("Position not found", show_alert=True)
        return

    mark_price = await _get_price(symbol) or 0
    entry_price = float(pos.get("entry_price", 0))
    amount = abs(float(pos.get("amount", pos.get("size", 0))))
    side = pos.get("side", "bid")
    margin = float(pos.get("margin", pos.get("initial_margin", 0)) or 0)

    leverage = pos.get("leverage")
    if not leverage or leverage == "?":
        try:
            notional = entry_price * amount
            leverage = round(notional / margin) if margin > 0 else 1
        except Exception:
            leverage = 1

    # Calculate PnL
    if side == "bid":
        pnl_usd = (mark_price - entry_price) * amount
    else:
        pnl_usd = (entry_price - mark_price) * amount

    # Subtract funding
    try:
        funding = float(pos.get("funding", 0))
        pnl_usd -= funding
    except (ValueError, TypeError):
        pass

    cost_basis = entry_price * amount
    pnl_pct = (pnl_usd / cost_basis * 100) if cost_basis else 0

    from bot.services.pnl_card import generate_pnl_card
    from aiogram.types import BufferedInputFile

    card_bytes = generate_pnl_card(
        symbol=symbol,
        side=side,
        entry_price=entry_price,
        mark_price=mark_price,
        amount=amount,
        leverage=leverage,
        pnl_usd=pnl_usd,
        pnl_pct=pnl_pct,
        username=user.get("username"),
        ref_code=user.get("username") or user.get("ref_code"),
    )

    photo = BufferedInputFile(card_bytes, filename=f"pnl_{symbol}.png")

    pnl_sign = "+" if pnl_usd >= 0 else ""
    caption = (
        f"{'🟢' if pnl_usd >= 0 else '🔴'} {symbol} {'LONG' if side == 'bid' else 'SHORT'} "
        f"| {pnl_sign}${pnl_usd:,.2f} ({pnl_sign}{pnl_pct:.1f}%)\n\n"
        f"Trade on @trident_pacifica_bot"
    )

    await callback.message.answer_photo(photo=photo, caption=caption)  # type: ignore
    await callback.message.answer("What next?", reply_markup=main_menu_kb())  # type: ignore


@router.callback_query(F.data.startswith("pclose:"))
async def cb_partial_close(callback: CallbackQuery):
    """Partial close: 25%, 50%, 75%."""
    parts = callback.data.split(":")  # type: ignore
    symbol, pct = parts[1], int(parts[2])
    tg_id = callback.from_user.id
    user = await get_user(tg_id)

    if not user or not user.get("pacifica_account"):
        await callback.answer("Not linked")
        return

    await callback.answer(f"Closing {pct}%...")

    client = build_client_from_user(user)
    try:
        positions = await client.get_positions()
        pos = next((p for p in positions if p.get("symbol", "").upper() == symbol.upper()), None)

        if not pos:
            await callback.message.edit_text(  # type: ignore
                f"No position for {symbol}.", reply_markup=back_to_menu_kb(),
            )
            return

        close_side = "ask" if pos.get("side") == "bid" else "bid"
        pos_side = pos.get("side", "bid")
        entry_price = float(pos.get("entry_price", 0))
        full_amount = abs(float(pos.get("amount", pos.get("size", 0))))

        # Round partial amount to lot size
        lot_size = await _get_lot_size(symbol)
        lot = float(lot_size)
        import math
        partial_amount = math.floor(full_amount * pct / 100 / lot) * lot
        decimals = len(lot_size.split(".")[-1]) if "." in lot_size else 0
        partial_str = f"{partial_amount:.{decimals}f}" if decimals else str(int(partial_amount))

        close_price = await _get_price(symbol) or 0

        settings = await get_user_settings(tg_id)
        slippage = settings.get("slippage", "0.5")

        resp = await client.create_market_order(
            symbol=symbol, side=close_side, amount=partial_str,
            reduce_only=True, slippage=slippage,
        )

        # PnL on closed portion
        pnl_line = ""
        if close_price and entry_price:
            if pos_side == "bid":
                pnl = (close_price - entry_price) * partial_amount
            else:
                pnl = (entry_price - close_price) * partial_amount
            pnl_color = "🟢" if pnl >= 0 else "🔴"
            pnl_sign = "+" if pnl >= 0 else ""
            pnl_line = f"PnL: {pnl_color} <b>{pnl_sign}${pnl:,.2f}</b>\n"

        await log_trade(tg_id, symbol, close_side, partial_str, order_type="partial_close")

        remaining = full_amount - partial_amount
        await callback.message.edit_text(  # type: ignore
            f"<b>✅ Closed {pct}% of {symbol}</b>\n\n"
            f"Closed: {partial_str} {symbol}\n"
            f"Close price: ${close_price:,.2f}\n"
            f"{pnl_line}"
            f"Remaining: ~{remaining:.{decimals}f} {symbol}",
            reply_markup=main_menu_kb(),
        )
    except PacificaAPIError as e:
        await callback.message.edit_text(  # type: ignore
            f"<b>❌ Partial Close Failed</b>\n\n{e}", reply_markup=back_to_menu_kb(),
        )
    finally:
        await client.close()


@router.callback_query(F.data.startswith("close_pos:"))
async def cb_close_pos(callback: CallbackQuery):
    symbol = callback.data.split(":")[1]  # type: ignore
    await callback.answer()
    await callback.message.edit_text(  # type: ignore
        f"<b>Close {symbol} position?</b>\n\nThis will market close your entire position.",
        reply_markup=confirm_close_kb(symbol),
    )


@router.callback_query(F.data.startswith("exec_close:"))
async def cb_exec_close(callback: CallbackQuery):
    symbol = callback.data.split(":")[1]  # type: ignore
    tg_id = callback.from_user.id
    user = await get_user(tg_id)

    if not user or not user.get("pacifica_account"):
        await callback.answer("Not linked")
        return

    await callback.answer("Closing position...")

    # Auto-claim beta code + builder approval if needed
    await ensure_beta_and_builder(user)

    client = build_client_from_user(user)
    try:
        positions = await client.get_positions()
        pos = next((p for p in positions if p.get("symbol", "").upper() == symbol.upper()), None)

        if not pos:
            await callback.message.edit_text(  # type: ignore
                f"No position for {symbol}.", reply_markup=back_to_menu_kb(),
            )
            return

        close_side = "ask" if pos.get("side") == "bid" else "bid"
        amount_f = abs(float(pos.get("amount", pos.get("size", 0))))
        entry_price = float(pos.get("entry_price", 0))
        pos_side = pos.get("side", "bid")

        # Round amount to lot size (same as partial close) to avoid
        # float precision issues like "0.45865000000000003"
        import math
        lot_size = await _get_lot_size(symbol)
        lot = float(lot_size)
        rounded_amount = math.floor(amount_f / lot) * lot
        decimals = len(lot_size.split(".")[-1]) if "." in lot_size else 0
        amount = f"{rounded_amount:.{decimals}f}" if decimals else str(int(rounded_amount))

        # Get current price for PnL calculation
        close_price = await _get_price(symbol) or 0

        settings = await get_user_settings(tg_id)
        slippage = settings.get("slippage", "0.5")

        resp = await client.create_market_order(
            symbol=symbol, side=close_side, amount=amount,
            reduce_only=True, slippage=slippage,
        )

        # Calculate closed PnL
        pnl_line = ""
        if close_price and entry_price:
            if pos_side == "bid":
                pnl = (close_price - entry_price) * amount_f
            else:
                pnl = (entry_price - close_price) * amount_f
            pnl_color = "🟢" if pnl >= 0 else "🔴"
            pnl_sign = "+" if pnl >= 0 else ""
            pnl_line = f"PnL: {pnl_color} <b>{pnl_sign}${pnl:,.2f}</b>\n"

        await log_trade(tg_id, symbol, close_side, amount, order_type="market_close")

        direction = "LONG" if pos_side == "bid" else "SHORT"

        # Calculate leverage from position data
        leverage = pos.get("leverage")
        if not leverage or leverage == "?":
            try:
                notional = entry_price * amount_f
                if margin > 0:
                    leverage = round(notional / margin)
                else:
                    leverage = 1
            except Exception:
                leverage = 1

        # Delete the old confirmation message so everything flows top-down
        try:
            await callback.message.delete()  # type: ignore
        except Exception:
            pass

        # Send PnL card (no menu)
        if close_price and entry_price:
            try:
                from bot.services.pnl_card import generate_pnl_card
                from aiogram.types import BufferedInputFile
                import asyncio

                cost_basis = entry_price * amount_f
                pnl_pct = (pnl / cost_basis * 100) if cost_basis else 0

                card_bytes = await asyncio.to_thread(
                    generate_pnl_card,
                    symbol=symbol, side=pos_side,
                    entry_price=entry_price, mark_price=close_price,
                    amount=amount_f, leverage=leverage,
                    pnl_usd=pnl, pnl_pct=pnl_pct,
                    username=user.get("username"),
                    ref_code=user.get("username") or user.get("ref_code"),
                )
                photo = BufferedInputFile(card_bytes, filename=f"pnl_{symbol}.png")
                pnl_sign = "+" if pnl >= 0 else ""
                await callback.bot.send_photo(  # type: ignore
                    chat_id=tg_id,
                    photo=photo,
                    caption=(
                        f"{'🟢' if pnl >= 0 else '🔴'} {symbol} {direction} closed "
                        f"| {pnl_sign}${pnl:,.2f} ({pnl_sign}{pnl_pct:.1f}%)\n\n"
                        f"Trade on @trident_pacifica_bot"
                    ),
                )
            except Exception as e:
                logger.debug("PnL card generation failed: %s", e)

        # Send close summary with menu buttons AFTER the card
        await callback.bot.send_message(  # type: ignore
            chat_id=tg_id,
            text=(
                f"<b>✅ Position Closed</b>\n\n"
                f"{symbol} {direction} {leverage}x — size {amount}\n"
                f"Entry: ${entry_price:,.2f}\n"
                f"Close: ${close_price:,.2f}\n"
                f"{pnl_line}"
            ),
            reply_markup=main_menu_kb(),
        )
    except PacificaAPIError as e:
        await callback.message.edit_text(  # type: ignore
            f"<b>❌ Close Failed</b>\n\n{e}", reply_markup=back_to_menu_kb(),
        )
    finally:
        await client.close()


@router.callback_query(F.data == "exec_closeall")
async def cb_exec_closeall(callback: CallbackQuery):
    tg_id = callback.from_user.id
    user = await get_user(tg_id)

    if not user or not user.get("pacifica_account"):
        await callback.answer("Not linked")
        return

    await callback.answer("Closing all...")

    # Auto-claim beta code + builder approval if needed
    await ensure_beta_and_builder(user)

    client = build_client_from_user(user)
    try:
        positions = await client.get_positions()

        settings = await get_user_settings(tg_id)
        slippage = settings.get("slippage", "0.5")

        results = []
        for pos in positions:
            symbol = pos.get("symbol", "?")
            close_side = "ask" if pos.get("side") == "bid" else "bid"
            # Round amount to lot size to avoid float precision issues
            import math
            raw_amount = abs(float(pos.get("amount", pos.get("size", 0))))
            lot_size = await _get_lot_size(symbol)
            lot = float(lot_size)
            rounded = math.floor(raw_amount / lot) * lot
            dec = len(lot_size.split(".")[-1]) if "." in lot_size else 0
            amount = f"{rounded:.{dec}f}" if dec else str(int(rounded))
            try:
                await client.create_market_order(
                    symbol=symbol, side=close_side, amount=amount,
                    reduce_only=True, slippage=slippage,
                )
                results.append(f"✅ {symbol}")
                await log_trade(tg_id, symbol, close_side, amount, order_type="market_close")
            except PacificaAPIError as e:
                results.append(f"❌ {symbol}: {e}")

        await callback.message.edit_text(  # type: ignore
            "<b>Close All Results</b>\n\n" + "\n".join(results),
            reply_markup=main_menu_kb(),
        )
    except Exception as e:
        await callback.message.edit_text(  # type: ignore
            f"Error: {e}", reply_markup=back_to_menu_kb(),
        )
    finally:
        await client.close()


# ------------------------------------------------------------------
# TP/SL via buttons → FSM for price input
# ------------------------------------------------------------------

@router.callback_query(F.data.startswith("set_tp:"))
async def cb_set_tp(callback: CallbackQuery, state: FSMContext):
    symbol = callback.data.split(":")[1]  # type: ignore
    await callback.answer()
    await state.set_state(TradeStates.waiting_tp_price)
    await state.update_data(symbol=symbol)
    await callback.message.answer(  # type: ignore
        f"<b>Set Take Profit for {symbol}</b>\n\nSend the TP price:"
    )


@router.message(TradeStates.waiting_tp_price)
async def msg_tp_price(message: Message, state: FSMContext):
    data = await state.get_data()
    symbol = data["symbol"]
    price = message.text.strip()  # type: ignore
    await state.clear()

    tg_id = message.from_user.id  # type: ignore
    user = await get_user(tg_id)
    if not user or not user.get("pacifica_account"):
        await message.answer("Link your account first.")
        return

    try:
        client = build_client_from_user(user)
        positions = await client.get_positions()
        pos = next((p for p in positions if p.get("symbol", "").upper() == symbol.upper()), None)
        if not pos:
            await message.answer(f"No position for {symbol}.", reply_markup=back_to_menu_kb())
            await client.close()
            return

        await client.set_tpsl(symbol=symbol, side=pos["side"], take_profit={"stop_price": price})
        await client.close()
        await message.answer(
            f"<b>✅ Take Profit set</b>\n{symbol} TP @ ${price}",
            reply_markup=main_menu_kb(),
        )
    except Exception as e:
        await message.answer(f"Error: {e}", reply_markup=back_to_menu_kb())


@router.callback_query(F.data.startswith("set_sl:"))
async def cb_set_sl(callback: CallbackQuery, state: FSMContext):
    symbol = callback.data.split(":")[1]  # type: ignore
    await callback.answer()
    await state.set_state(TradeStates.waiting_sl_price)
    await state.update_data(symbol=symbol)
    await callback.message.answer(  # type: ignore
        f"<b>Set Stop Loss for {symbol}</b>\n\nSend the SL price:"
    )


@router.message(TradeStates.waiting_sl_price)
async def msg_sl_price(message: Message, state: FSMContext):
    data = await state.get_data()
    symbol = data["symbol"]
    price = message.text.strip()  # type: ignore
    await state.clear()

    tg_id = message.from_user.id  # type: ignore
    user = await get_user(tg_id)
    if not user or not user.get("pacifica_account"):
        await message.answer("Link your account first.")
        return

    try:
        client = build_client_from_user(user)
        positions = await client.get_positions()
        pos = next((p for p in positions if p.get("symbol", "").upper() == symbol.upper()), None)
        if not pos:
            await message.answer(f"No position for {symbol}.", reply_markup=back_to_menu_kb())
            await client.close()
            return

        await client.set_tpsl(symbol=symbol, side=pos["side"], stop_loss={"stop_price": price})
        await client.close()
        await message.answer(
            f"<b>✅ Stop Loss set</b>\n{symbol} SL @ ${price}",
            reply_markup=main_menu_kb(),
        )
    except Exception as e:
        await message.answer(f"Error: {e}", reply_markup=back_to_menu_kb())


# ------------------------------------------------------------------
# Quick trade shortcuts — execute immediately, no confirmation step
# ------------------------------------------------------------------

async def _quick_trade(message: Message, side: str):
    """Shared logic for /long and /short instant execution.

    ``side`` is "bid" (long) or "ask" (short).
    Usage: /long SYMBOL AMOUNT [LEVERAGE]  — default leverage 1.
    """
    side_label = "long" if side == "bid" else "short"
    direction = "🟢 LONG" if side == "bid" else "🔴 SHORT"

    args = (message.text or "").split()
    if len(args) < 3:
        await message.answer(
            f"Usage: /{side_label} &lt;symbol&gt; &lt;amount_usdc&gt; [leverage]\n"
            f"Example: /{side_label} BTC 50\n"
            f"Example: /{side_label} ETH 100 5",
            reply_markup=main_menu_kb(),
        )
        return

    symbol = args[1].upper()
    usdc_raw = args[2].lstrip("$").replace(",", "")
    lev_raw = args[3].lower().rstrip("x") if len(args) > 3 else "1"

    # Validate numbers
    try:
        usdc_amount = float(usdc_raw)
        leverage = float(lev_raw)
        if usdc_amount <= 0 or leverage <= 0:
            raise ValueError
    except (ValueError, TypeError):
        await message.answer("Invalid amount or leverage. Use numbers > 0.", reply_markup=main_menu_kb())
        return

    tg_id = message.from_user.id  # type: ignore
    user = await get_user(tg_id)
    if not user or not user.get("pacifica_account"):
        await message.answer(
            "Link your account first!\nUse /start then /link <wallet>",
            reply_markup=main_menu_kb(),
        )
        return

    # Notify the user that the order is being sent
    status_msg = await message.answer(f"{direction} {symbol} — ${usdc_raw} USDC @ {lev_raw}x — sending...")

    # Auto-claim beta code + builder approval if needed
    await ensure_beta_and_builder(user)

    # Get price
    price = await _get_price(symbol)
    if not price:
        await status_msg.edit_text(
            f"<b>Could not fetch price for {symbol}.</b> Is it a valid market?",
            reply_markup=main_menu_kb(),
        )
        return

    # Convert USDC to token amount
    notional = usdc_amount * leverage
    lot_size = await _get_lot_size(symbol)
    token_amount = _usdc_to_token(notional, price, lot_size)

    # User slippage
    settings = await get_user_settings(tg_id)
    slippage = settings.get("slippage", "0.5")

    try:
        client = build_client_from_user(user)
        resp = await client.create_market_order(
            symbol=symbol,
            side=side,
            amount=token_amount,
            slippage=slippage,
        )
        await client.close()

        logger.info("Quick %s order response: %s", side_label, resp)
        order_id = resp.get("order_id", resp.get("id", "?"))
        fill_price = resp.get("fill_price", resp.get("price", resp.get("avg_fill_price", "")))

        await log_trade(tg_id, symbol, side, token_amount, str(fill_price), "market")
        await _track_referral_fee(tg_id, symbol, notional)

        leverage_str = str(int(leverage)) if leverage == int(leverage) else str(leverage)
        price_line = f"Fill price: <b>${fill_price}</b>\n" if fill_price else ""

        from bot.config import PACIFICA_NETWORK
        app_base = "https://app.pacifica.fi" if PACIFICA_NETWORK == "mainnet" else "https://test-app.pacifica.fi"
        portfolio_url = f"{app_base}/portfolio"

        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📊 View on Pacifica", url=portfolio_url)],
            *main_menu_kb().inline_keyboard,
        ])

        await status_msg.edit_text(
            f"<b>✅ Order Executed!</b>\n\n"
            f"{direction} <b>{symbol}</b>\n"
            f"Size: ${usdc_raw} USDC ({token_amount} {symbol})\n"
            f"Leverage: {leverage_str}x\n"
            f"Notional: ${notional:,.0f}\n"
            f"{price_line}"
            f"Order ID: <code>{order_id}</code>",
            reply_markup=kb,
        )
    except PacificaAPIError as e:
        hint = _trade_error_hint(str(e))
        await status_msg.edit_text(
            f"<b>❌ Order Failed</b>\n\n{e}{hint}",
            reply_markup=main_menu_kb(),
        )
    except Exception as e:
        logger.exception("Quick trade error")
        await status_msg.edit_text(
            f"<b>❌ Unexpected Error</b>\n\n{e}",
            reply_markup=main_menu_kb(),
        )


@router.message(Command("long"))
async def cmd_long(message: Message):
    """Instant long — /long SYMBOL AMOUNT [LEVERAGE]. No confirmation step."""
    await _quick_trade(message, side="bid")


@router.message(Command("short"))
async def cmd_short(message: Message):
    """Instant short — /short SYMBOL AMOUNT [LEVERAGE]. No confirmation step."""
    await _quick_trade(message, side="ask")


@router.message(Command("close"))
async def cmd_close(message: Message):
    args = (message.text or "").split()
    if len(args) < 2:
        await message.answer("Usage: /close <symbol>", reply_markup=main_menu_kb())
        return
    symbol = args[1].upper()
    await message.answer(
        f"<b>Close {symbol} position?</b>",
        reply_markup=confirm_close_kb(symbol),
    )


@router.message(Command("closeall"))
async def cmd_closeall(message: Message):
    await message.answer(
        "<b>Close ALL positions?</b>\n\nThis cannot be undone.",
        reply_markup=close_all_kb(),
    )


@router.message(Command("markets"))
async def cmd_markets(message: Message):
    """Shortcut to show markets."""
    try:
        from solders.keypair import Keypair
        from bot.services.pacifica_client import PacificaClient
        temp_kp = Keypair()
        client = PacificaClient(account="public", keypair=temp_kp)
        markets = await client.get_markets_info()

        prices: dict[str, str] = {}
        for sym in ["BTC", "ETH", "SOL", "TRUMP", "HYPE", "DOGE", "XRP", "SUI", "LINK", "AVAX"]:
            try:
                trades = await client.get_trades(sym, limit=1)
                if trades:
                    prices[sym] = trades[0].get("price", "?")
            except Exception:
                pass
        await client.close()
    except Exception as e:
        await message.answer(f"Could not load markets: {e}", reply_markup=back_to_menu_kb())
        return

    from bot.utils.keyboards import markets_kb
    await message.answer(
        f"<b>📊 Markets</b> ({len(markets)} pairs)\n\nTap to trade:",
        reply_markup=markets_kb(markets, prices),
    )


# ------------------------------------------------------------------
# Quick ticker lookup — typing just "BTC" shows the market detail
# ------------------------------------------------------------------

# Known symbols cache (populated lazily)
_known_symbols: set[str] | None = None


async def _get_known_symbols() -> set[str]:
    global _known_symbols
    if _known_symbols is None:
        try:
            from solders.keypair import Keypair
            from bot.services.pacifica_client import PacificaClient
            client = PacificaClient(account="public", keypair=Keypair())
            markets = await client.get_markets_info()
            await client.close()
            _known_symbols = {m.get("symbol", "").upper() for m in markets}
        except Exception:
            _known_symbols = set()
    return _known_symbols


@router.message()
async def msg_ticker_lookup(message: Message, state: FSMContext):
    """Catch-all: if user types a known ticker, show market detail."""
    # Don't interfere with FSM states
    current_state = await state.get_state()
    if current_state:
        return

    text = (message.text or "").strip().upper()
    # Only match single words that look like tickers (2-10 chars, letters only)
    if not text or not text.isalpha() or len(text) < 2 or len(text) > 10:
        return

    symbols = await _get_known_symbols()
    if text not in symbols:
        return

    # Show market detail
    mark = "?"
    max_lev = "?"
    try:
        from solders.keypair import Keypair
        from bot.services.pacifica_client import PacificaClient
        client = PacificaClient(account="public", keypair=Keypair())
        trades = await client.get_trades(text, limit=1)
        if trades:
            mark = trades[0].get("price", "?")
        markets = await client.get_markets_info()
        m = next((x for x in markets if x.get("symbol") == text), None)
        if m:
            max_lev = m.get("max_leverage", "?")
        await client.close()
    except Exception:
        pass

    await message.answer(
        f"<b>{text}</b>\n\n"
        f"Price: <b>${mark}</b>\n"
        f"Max leverage: {max_lev}x\n\n"
        f"What do you want to do?",
        reply_markup=market_detail_kb(text),
    )
