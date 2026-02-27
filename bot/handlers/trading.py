"""
Interactive trading flow — button-driven + command shortcuts.
"""

import logging

from aiogram import Router, F
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

from database.db import get_user, log_trade
from bot.models.user import build_client_from_user
from bot.services.pacifica_client import PacificaAPIError
from bot.utils.keyboards import (
    market_detail_kb,
    trade_amount_kb,
    trade_leverage_kb,
    confirm_trade_kb,
    position_detail_kb,
    confirm_close_kb,
    close_all_kb,
    back_to_menu_kb,
    main_menu_kb,
)
from bot.utils.formatters import fmt_position

logger = logging.getLogger(__name__)
router = Router()


class TradeStates(StatesGroup):
    waiting_custom_amount = State()
    waiting_custom_leverage = State()
    waiting_tp_price = State()
    waiting_sl_price = State()


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
    try:
        from solders.keypair import Keypair
        from bot.services.pacifica_client import PacificaClient
        client = PacificaClient(account="public", keypair=Keypair())
        markets = await client.get_markets_info()
        await client.close()
        m = next((x for x in markets if x.get("symbol") == symbol), None)
        if m:
            return int(m.get("max_leverage", 50))
    except Exception:
        pass
    return 50


def _usdc_to_token(usdc_amount: float, price: float, lot_size: str = "0.00001") -> str:
    """Convert USDC notional to token amount, rounded to lot size."""
    if price <= 0:
        return "0"
    raw = usdc_amount / price
    # Round to lot_size precision
    lot = float(lot_size)
    if lot >= 1:
        rounded = round(raw / lot) * lot
        return str(int(rounded))
    else:
        decimals = len(lot_size.split(".")[-1]) if "." in lot_size else 0
        rounded = round(raw / lot) * lot
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
        token_amount = _usdc_to_token(notional, price)
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
        token_amount = _usdc_to_token(notional, price)
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
    token_amount = _usdc_to_token(notional, price)

    try:
        client = build_client_from_user(user)
        resp = await client.create_market_order(
            symbol=symbol,
            side=side,
            amount=token_amount,
        )
        await client.close()

        order_id = resp.get("order_id", resp.get("id", "?"))
        fill_price = resp.get("fill_price", resp.get("price", "?"))

        await log_trade(tg_id, symbol, side, token_amount, str(fill_price), "market")

        direction = "🟢 LONG" if side == "bid" else "🔴 SHORT"
        await callback.message.edit_text(  # type: ignore
            f"<b>✅ Order Executed!</b>\n\n"
            f"{direction} <b>{symbol}</b>\n"
            f"Size: ${usdc_amount} USDC ({token_amount} {symbol})\n"
            f"Leverage: {leverage}x\n"
            f"Notional: ${notional:,.0f}\n"
            f"Fill price: ${fill_price}\n"
            f"Order ID: <code>{order_id}</code>",
            reply_markup=main_menu_kb(),
        )
    except PacificaAPIError as e:
        await callback.message.edit_text(  # type: ignore
            f"<b>❌ Order Failed</b>\n\n{e}",
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

    await callback.message.edit_text(  # type: ignore
        f"<b>Position Detail</b>\n\n{fmt_position(pos)}",
        reply_markup=position_detail_kb(symbol, pos.get("side", "bid")),
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
        amount = str(abs(float(pos.get("amount", pos.get("size", 0)))))

        resp = await client.create_market_order(
            symbol=symbol, side=close_side, amount=amount, reduce_only=True,
        )
        await client.close()

        await log_trade(tg_id, symbol, close_side, amount, order_type="market_close")

        await callback.message.edit_text(  # type: ignore
            f"<b>✅ Position Closed</b>\n\n{symbol} — size {amount}",
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

        results = []
        for pos in positions:
            symbol = pos.get("symbol", "?")
            close_side = "ask" if pos.get("side") == "bid" else "bid"
            amount = str(abs(float(pos.get("amount", pos.get("size", 0)))))
            try:
                await client.create_market_order(
                    symbol=symbol, side=close_side, amount=amount, reduce_only=True,
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
        token_amount = _usdc_to_token(notional, price)
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
        token_amount = _usdc_to_token(notional, price)
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
