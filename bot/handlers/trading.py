"""
Interactive trading flow — button-driven + command shortcuts.
"""

import logging

from aiogram import Router, F
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

from database.db import get_user, log_trade, get_user_settings, log_referral_fee, REFERRAL_FEE_SHARE
from bot.models.user import build_client_from_user
from bot.services.pacifica_client import PacificaAPIError
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


async def _get_price(symbol: str) -> float | None:
    """Fetch latest trade price for a symbol."""
    try:
        from solders.keypair import Keypair
        from bot.services.pacifica_client import PacificaClient
        client = PacificaClient(account="public", keypair=Keypair())
        trades = await client.get_trades(symbol, limit=1)
        await client.close()
        if trades:
            return float(trades[0]["price"])
    except Exception:
        pass
    return None


async def _get_max_leverage(symbol: str) -> int:
    """Fetch max leverage for a symbol."""
    max_lev, _, _ = await _get_market_info(symbol)
    return max_lev


async def _get_market_info(symbol: str) -> tuple[int, str, str]:
    """Fetch max leverage, tick_size, and lot_size for a symbol."""
    try:
        from solders.keypair import Keypair
        from bot.services.pacifica_client import PacificaClient
        client = PacificaClient(account="public", keypair=Keypair())
        markets = await client.get_markets_info()
        await client.close()
        m = next((x for x in markets if x.get("symbol") == symbol), None)
        if m:
            return (
                int(m.get("max_leverage", 50)),
                str(m.get("tick_size", "1")),
                str(m.get("lot_size", "0.01")),
            )
    except Exception:
        pass
    return 50, "1", "0.01"


# Cache lot sizes per symbol (populated on first market info fetch)
_lot_sizes: dict[str, str] = {}


async def _get_lot_size(symbol: str) -> str:
    """Get lot size for a symbol, using cache or fetching from API."""
    if symbol in _lot_sizes:
        return _lot_sizes[symbol]
    _, _, lot = await _get_market_info(symbol)
    _lot_sizes[symbol] = lot
    return lot


def _usdc_to_token(usdc_amount: float, price: float, lot_size: str = "0.01") -> str:
    """Convert USDC notional to token amount, rounded down to lot size."""
    if price <= 0:
        return "0"
    raw = usdc_amount / price
    lot = float(lot_size)
    # Round DOWN to lot size (avoid exceeding balance)
    import math
    rounded = math.floor(raw / lot) * lot
    if lot >= 1:
        return str(int(rounded))
    else:
        decimals = len(lot_size.split(".")[-1]) if "." in lot_size else 0
        return f"{rounded:.{decimals}f}"


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

    await callback.message.edit_text(  # type: ignore
        f"<b>{direction} {symbol}</b>{price_str}\n\n"
        f"Select amount (USDC):",
        reply_markup=trade_amount_kb(symbol, api_side),
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
        await callback.message.edit_text(  # type: ignore
            f"<b>✅ Order Executed!</b>\n\n"
            f"{direction} <b>{symbol}</b>\n"
            f"Size: ${usdc_amount} USDC ({token_amount} {symbol})\n"
            f"Leverage: {leverage}x\n"
            f"Notional: ${notional:,.0f}\n"
            f"{price_line}"
            f"Order ID: <code>{order_id}</code>",
            reply_markup=main_menu_kb(),
        )
    except PacificaAPIError as e:
        await callback.message.edit_text(  # type: ignore
            f"<b>❌ Order Failed</b>\n\n{e}",
            reply_markup=market_detail_kb(symbol),
        )


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
        await callback.message.edit_text(  # type: ignore
            f"<b>❌ Order Failed</b>\n\n{e}",
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

    # Set TP
    tg_id = message.from_user.id  # type: ignore
    user = await get_user(tg_id)
    try:
        client = build_client_from_user(user)
        await client.set_tpsl(symbol=symbol, side=side, take_profit={"stop_price": tp_price})
        await client.close()
    except Exception as e:
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
        await client.set_tpsl(symbol=symbol, side=side, stop_loss={"stop_price": sl_price})
        await client.close()
    except Exception as e:
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

    price_f = float(limit_price)
    notional = float(usdc_amount) * float(leverage)
    lot_size = await _get_lot_size(symbol)
    token_amount = _usdc_to_token(notional, price_f, lot_size)

    # Convert price to tick level (tick_level = price in integer form)
    # Pacifica uses tick levels — we need market info to properly convert
    try:
        max_lev, tick_size, _ = await _get_market_info(symbol)
        tick_level = int(round(price_f / float(tick_size)))
    except Exception:
        tick_level = int(price_f)  # fallback

    try:
        client = build_client_from_user(user)
        resp = await client.create_limit_order(
            symbol=symbol,
            side=side,
            amount=token_amount,
            tick_level=tick_level,
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
        await callback.message.edit_text(  # type: ignore
            f"<b>❌ Limit Order Failed</b>\n\n{e}",
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

    try:
        client = build_client_from_user(user)
        positions = await client.get_positions()
        pos = next((p for p in positions if p.get("symbol", "").upper() == symbol.upper()), None)

        if not pos:
            await callback.message.edit_text(  # type: ignore
                f"No position for {symbol}.", reply_markup=back_to_menu_kb(),
            )
            await client.close()
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
        await client.close()

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

    try:
        client = build_client_from_user(user)
        positions = await client.get_positions()
        pos = next((p for p in positions if p.get("symbol", "").upper() == symbol.upper()), None)

        if not pos:
            await callback.message.edit_text(  # type: ignore
                f"No position for {symbol}.", reply_markup=back_to_menu_kb(),
            )
            await client.close()
            return

        close_side = "ask" if pos.get("side") == "bid" else "bid"
        amount_f = abs(float(pos.get("amount", pos.get("size", 0))))
        amount = str(amount_f)
        entry_price = float(pos.get("entry_price", 0))
        pos_side = pos.get("side", "bid")

        # Get current price for PnL calculation
        close_price = await _get_price(symbol) or 0

        settings = await get_user_settings(tg_id)
        slippage = settings.get("slippage", "0.5")

        resp = await client.create_market_order(
            symbol=symbol, side=close_side, amount=amount,
            reduce_only=True, slippage=slippage,
        )
        await client.close()

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
        await callback.message.edit_text(  # type: ignore
            f"<b>✅ Position Closed</b>\n\n"
            f"{symbol} {direction} — size {amount}\n"
            f"Entry: ${entry_price:,.2f}\n"
            f"Close: ${close_price:,.2f}\n"
            f"{pnl_line}",
            reply_markup=main_menu_kb(),
        )
    except PacificaAPIError as e:
        await callback.message.edit_text(  # type: ignore
            f"<b>❌ Close Failed</b>\n\n{e}", reply_markup=back_to_menu_kb(),
        )


@router.callback_query(F.data == "exec_closeall")
async def cb_exec_closeall(callback: CallbackQuery):
    tg_id = callback.from_user.id
    user = await get_user(tg_id)

    if not user or not user.get("pacifica_account"):
        await callback.answer("Not linked")
        return

    await callback.answer("Closing all...")

    try:
        client = build_client_from_user(user)
        positions = await client.get_positions()

        settings = await get_user_settings(tg_id)
        slippage = settings.get("slippage", "0.5")

        results = []
        for pos in positions:
            symbol = pos.get("symbol", "?")
            close_side = "ask" if pos.get("side") == "bid" else "bid"
            amount = str(abs(float(pos.get("amount", pos.get("size", 0)))))
            try:
                await client.create_market_order(
                    symbol=symbol, side=close_side, amount=amount,
                    reduce_only=True, slippage=slippage,
                )
                results.append(f"✅ {symbol}")
                await log_trade(tg_id, symbol, close_side, amount, order_type="market_close")
            except PacificaAPIError as e:
                results.append(f"❌ {symbol}: {e}")

        await client.close()
        await callback.message.edit_text(  # type: ignore
            "<b>Close All Results</b>\n\n" + "\n".join(results),
            reply_markup=main_menu_kb(),
        )
    except Exception as e:
        await callback.message.edit_text(  # type: ignore
            f"Error: {e}", reply_markup=back_to_menu_kb(),
        )


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
# Quick command shortcuts (still work alongside buttons)
# ------------------------------------------------------------------

@router.message(Command("long"))
async def cmd_long(message: Message):
    args = (message.text or "").split()
    if len(args) < 3:
        await message.answer(
            "Usage: /long <symbol> <$amount> [leverage]\n"
            "Example: /long BTC 100 10x\n\nOr use the buttons!",
            reply_markup=main_menu_kb(),
        )
        return

    symbol = args[1].upper()
    usdc = args[2].lstrip("$")
    leverage = args[3].lower().rstrip("x") if len(args) > 3 else "1"

    price = await _get_price(symbol)
    notional = float(usdc) * float(leverage)
    if price:
        lot_size = await _get_lot_size(symbol)
        token_amount = _usdc_to_token(notional, price, lot_size)
        price_line = f"Price: ${price:,.2f}\nToken amount: ~{token_amount} {symbol}\n"
    else:
        price_line = ""

    await message.answer(
        f"<b>Confirm Order</b>\n\n"
        f"🟢 LONG <b>{symbol}</b>\n"
        f"Size: ${usdc} USDC\n"
        f"Leverage: {leverage}x\n"
        f"Notional: ${notional:,.0f}\n"
        f"{price_line}"
        f"Type: Market\n"
        f"Builder fee: 0.05%",
        reply_markup=confirm_trade_kb("bid", symbol, usdc, leverage),
    )


@router.message(Command("short"))
async def cmd_short(message: Message):
    args = (message.text or "").split()
    if len(args) < 3:
        await message.answer(
            "Usage: /short <symbol> <$amount> [leverage]\n"
            "Example: /short ETH 200 20x\n\nOr use the buttons!",
            reply_markup=main_menu_kb(),
        )
        return

    symbol = args[1].upper()
    usdc = args[2].lstrip("$")
    leverage = args[3].lower().rstrip("x") if len(args) > 3 else "1"

    price = await _get_price(symbol)
    notional = float(usdc) * float(leverage)
    if price:
        lot_size = await _get_lot_size(symbol)
        token_amount = _usdc_to_token(notional, price, lot_size)
        price_line = f"Price: ${price:,.2f}\nToken amount: ~{token_amount} {symbol}\n"
    else:
        price_line = ""

    await message.answer(
        f"<b>Confirm Order</b>\n\n"
        f"🔴 SHORT <b>{symbol}</b>\n"
        f"Size: ${usdc} USDC\n"
        f"Leverage: {leverage}x\n"
        f"Notional: ${notional:,.0f}\n"
        f"{price_line}"
        f"Type: Market\n"
        f"Builder fee: 0.05%",
        reply_markup=confirm_trade_kb("ask", symbol, usdc, leverage),
    )


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
