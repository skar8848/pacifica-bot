"""
Advanced trading handlers — Phase 1 & 2 features.
Risk calculator, funding rates, trailing stop, DCA, scaled orders.
"""

import json
import logging

from aiogram import Router, F
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State

from database.db import (
    get_user, add_trailing_stop, get_active_trailing_stops, cancel_trailing_stop,
    add_dca_config, get_active_dca_configs, cancel_dca,
    add_scaled_order,
    add_twap_order, get_active_twap_orders, cancel_twap,
    add_onchain_watch, remove_onchain_watch, get_onchain_watches,
)
from bot.services.market_data import get_price, get_market_info, usd_to_token, get_lot_size
from bot.services.funding_monitor import get_all_funding_rates

logger = logging.getLogger(__name__)
router = Router()


# ── FSM States ──
class AdvancedStates(StatesGroup):
    # Trailing stop
    waiting_trail_symbol = State()
    waiting_trail_percent = State()
    # DCA
    waiting_dca_symbol = State()
    waiting_dca_params = State()
    # Scaled
    waiting_scale_symbol = State()
    waiting_scale_params = State()


# ══════════════════════════════════════════════════════════════
# RISK CALCULATOR
# ══════════════════════════════════════════════════════════════

@router.message(Command("calc"))
async def cmd_calc(message: Message):
    """
    /calc SYMBOL ENTRY SL RISK%
    e.g. /calc BTC 100000 97000 2
    """
    user = await get_user(message.from_user.id)
    if not user or not user.get("pacifica_account"):
        await message.answer("Setup your wallet first with /start")
        return

    parts = (message.text or "").split()
    if len(parts) < 5:
        await message.answer(
            "<b>Risk Calculator</b>\n\n"
            "Usage: <code>/calc SYMBOL ENTRY SL RISK%</code>\n\n"
            "Example: <code>/calc BTC 100000 97000 2</code>\n"
            "→ Risk 2% of account if BTC drops from 100k to 97k\n\n"
            "Parameters:\n"
            "• SYMBOL — Trading pair (e.g. BTC, ETH, SOL)\n"
            "• ENTRY — Entry price\n"
            "• SL — Stop loss price\n"
            "• RISK% — % of account equity to risk",
        )
        return

    try:
        symbol = parts[1].upper()
        if not symbol.endswith("-PERP"):
            symbol += "-PERP"
        entry = float(parts[2])
        sl = float(parts[3])
        risk_pct = float(parts[4])
    except (ValueError, IndexError):
        await message.answer("Invalid parameters. Usage: <code>/calc SYMBOL ENTRY SL RISK%</code>")
        return

    if entry <= 0 or sl <= 0 or risk_pct <= 0:
        await message.answer("All values must be positive.")
        return

    # Get account equity
    from bot.services.wallet_manager import decrypt_private_key
    from bot.services.pacifica_client import PacificaClient

    try:
        kp = decrypt_private_key(user["agent_wallet_encrypted"])
        client = PacificaClient(account=user["pacifica_account"], keypair=kp)
        try:
            info = await client.get_account_info()
            equity = float(info.get("equity", 0))
        finally:
            await client.close()
    except Exception:
        equity = 0

    if equity <= 0:
        await message.answer("Could not fetch account equity. Deposit funds first.")
        return

    # Calculations
    risk_amount = equity * (risk_pct / 100)
    price_diff = abs(entry - sl)
    price_diff_pct = (price_diff / entry) * 100

    # Position size in USD (notional)
    position_size_usd = risk_amount / (price_diff / entry)

    # Max leverage needed
    max_lev, _, lot_size = await get_market_info(symbol)
    needed_margin = position_size_usd  # at 1x
    leverage_needed = max(1, round(position_size_usd / equity))
    leverage_needed = min(leverage_needed, max_lev)

    # Position size in tokens
    current_price = await get_price(symbol) or entry
    token_amount = usd_to_token(position_size_usd, current_price, lot_size)

    # Reward (2:1 and 3:1 R:R targets)
    is_long = entry > sl
    if is_long:
        tp_2r = entry + (price_diff * 2)
        tp_3r = entry + (price_diff * 3)
    else:
        tp_2r = entry - (price_diff * 2)
        tp_3r = entry - (price_diff * 3)

    text = (
        f"<b>Risk Calculator</b>\n\n"
        f"<b>{symbol}</b> {'LONG' if is_long else 'SHORT'}\n"
        f"{'─' * 28}\n\n"
        f"<b>Account</b>\n"
        f"Equity: <code>${equity:,.2f}</code>\n"
        f"Risk: <code>{risk_pct}%</code> = <code>${risk_amount:,.2f}</code>\n\n"
        f"<b>Position</b>\n"
        f"Entry: <code>${entry:,.2f}</code>\n"
        f"Stop Loss: <code>${sl:,.2f}</code> ({price_diff_pct:.1f}% away)\n"
        f"Size: <code>${position_size_usd:,.2f}</code> ({token_amount} tokens)\n"
        f"Leverage: <code>{leverage_needed}x</code>\n"
        f"Margin needed: <code>${position_size_usd / leverage_needed:,.2f}</code>\n\n"
        f"<b>Targets</b>\n"
        f"2:1 R:R → <code>${tp_2r:,.2f}</code> (+${risk_amount * 2:,.2f})\n"
        f"3:1 R:R → <code>${tp_3r:,.2f}</code> (+${risk_amount * 3:,.2f})\n"
    )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text=f"{'Long' if is_long else 'Short'} {symbol.replace('-PERP', '')} {leverage_needed}x",
                callback_data=f"trade:{'long' if is_long else 'short'}:{symbol}",
            ),
        ],
        [InlineKeyboardButton(text="Menu", callback_data="nav:menu")],
    ])

    await message.answer(text, reply_markup=kb)


# ══════════════════════════════════════════════════════════════
# FUNDING RATES
# ══════════════════════════════════════════════════════════════

@router.message(Command("funding"))
async def cmd_funding(message: Message):
    """/funding [SYMBOL] — Show current funding rates."""
    parts = (message.text or "").split()
    specific = None
    if len(parts) > 1:
        specific = parts[1].upper()
        if not specific.endswith("-PERP"):
            specific += "-PERP"

    rates = await get_all_funding_rates()
    if not rates:
        await message.answer("Could not fetch funding rates.")
        return

    if specific:
        rate = next((r for r in rates if r["symbol"] == specific), None)
        if not rate:
            await message.answer(f"No funding data for {specific}")
            return

        fr = rate["funding_rate"]
        nfr = rate["next_funding_rate"]
        annual = fr * 24 * 365 * 100

        emoji = "\U0001f7e2" if fr < 0 else ("\U0001f534" if fr > 0.0005 else "\u26aa")
        text = (
            f"<b>Funding Rate — {specific}</b>\n\n"
            f"{emoji} Current: <code>{fr*100:+.4f}%</code>/hr\n"
            f"Next: <code>{nfr*100:+.4f}%</code>/hr\n"
            f"Annualized: <code>{annual:+.1f}%</code>\n"
            f"OI: <code>${rate['open_interest']:,.0f}</code>\n\n"
            f"{'Longs pay shorts' if fr > 0 else 'Shorts pay longs' if fr < 0 else 'Neutral'}"
        )
        await message.answer(text)
        return

    # Show all markets
    lines = ["<b>Funding Rates</b>\n"]
    for r in rates[:15]:
        fr = r["funding_rate"]
        emoji = "\U0001f7e2" if fr < 0 else ("\U0001f534" if fr > 0.0005 else "\u26aa")
        sym = r["symbol"].replace("-PERP", "")
        lines.append(f"{emoji} <code>{sym:>6}</code>  <code>{fr*100:+.4f}%</code>/hr")

    lines.append("\n<i>Positive = longs pay shorts</i>")
    lines.append("Detail: <code>/funding BTC</code>")
    await message.answer("\n".join(lines))


@router.callback_query(F.data == "cmd:funding")
async def cb_funding(callback: CallbackQuery):
    await callback.answer()
    await callback.message.answer("/funding — use the command to see rates")


# ══════════════════════════════════════════════════════════════
# TRAILING STOP
# ══════════════════════════════════════════════════════════════

@router.message(Command("trail"))
async def cmd_trailing_stop(message: Message):
    """
    /trail SYMBOL PERCENT
    e.g. /trail BTC 3 — trail 3% behind BTC position
    """
    user = await get_user(message.from_user.id)
    if not user or not user.get("pacifica_account"):
        await message.answer("Setup your wallet first with /start")
        return

    parts = (message.text or "").split()
    if len(parts) < 3:
        # Show active trailing stops + usage
        stops = await get_active_trailing_stops(message.from_user.id)
        text = "<b>Trailing Stop Loss</b>\n\n"
        if stops:
            for s in stops:
                text += (
                    f"• <b>{s['symbol']}</b> {s['side'].upper()} — "
                    f"trail {s['trail_percent']}%, "
                    f"peak ${s['peak_price']:,.2f}, "
                    f"trigger ${s['callback_price']:,.2f}\n"
                )
            text += "\n"
        text += (
            "Usage: <code>/trail SYMBOL PERCENT</code>\n"
            "Example: <code>/trail BTC 3</code>\n\n"
            "The stop follows price by PERCENT%. "
            "When price reverses by that amount, position closes."
        )
        await message.answer(text)
        return

    symbol = parts[1].upper()
    if not symbol.endswith("-PERP"):
        symbol += "-PERP"

    try:
        trail_pct = float(parts[2])
    except ValueError:
        await message.answer("Invalid percentage.")
        return

    if trail_pct <= 0 or trail_pct > 50:
        await message.answer("Trail percent must be between 0.1 and 50.")
        return

    # Get current position
    from bot.services.wallet_manager import decrypt_private_key
    from bot.services.pacifica_client import PacificaClient

    kp = decrypt_private_key(user["agent_wallet_encrypted"])
    client = PacificaClient(account=user["pacifica_account"], keypair=kp)

    try:
        positions = await client.get_positions()
        pos = next((p for p in (positions or []) if p.get("symbol") == symbol), None)
    finally:
        await client.close()

    if not pos:
        await message.answer(f"No open position for {symbol}")
        return

    side = pos.get("side", "long").lower()
    entry_price = float(pos.get("entry_price", 0))
    current_price = await get_price(symbol) or entry_price

    if side in ("long", "buy"):
        peak = current_price
        callback = current_price * (1 - trail_pct / 100)
    else:
        peak = current_price
        callback = current_price * (1 + trail_pct / 100)

    stop_id = await add_trailing_stop(
        telegram_id=message.from_user.id,
        symbol=symbol,
        side=side,
        trail_percent=trail_pct,
        entry_price=entry_price,
        peak_price=peak,
        callback_price=callback,
    )

    text = (
        f"<b>Trailing Stop Set</b>\n\n"
        f"<b>{symbol}</b> {side.upper()}\n"
        f"Trail: <code>{trail_pct}%</code>\n"
        f"Current: <code>${current_price:,.2f}</code>\n"
        f"Trigger if: <code>${callback:,.2f}</code>\n"
        f"Entry: <code>${entry_price:,.2f}</code>\n\n"
        f"The stop will follow price. Checking every 5s."
    )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Cancel", callback_data=f"trail_cancel:{stop_id}")],
        [InlineKeyboardButton(text="View Positions", callback_data="nav:positions")],
    ])

    await message.answer(text, reply_markup=kb)


@router.callback_query(F.data.startswith("trail_cancel:"))
async def cb_cancel_trail(callback: CallbackQuery):
    stop_id = int(callback.data.split(":")[1])
    await cancel_trailing_stop(stop_id, callback.from_user.id)
    await callback.answer("Trailing stop cancelled")
    await callback.message.edit_text("<b>Trailing stop cancelled.</b>")


# ══════════════════════════════════════════════════════════════
# DCA BOT
# ══════════════════════════════════════════════════════════════

@router.message(Command("dca"))
async def cmd_dca(message: Message):
    """
    /dca SYMBOL SIDE TOTAL ORDERS INTERVAL LEVERAGE
    e.g. /dca BTC long 500 10 4h 5
    → DCA $500 into BTC long, 10 orders, every 4 hours, 5x leverage
    """
    user = await get_user(message.from_user.id)
    if not user or not user.get("pacifica_account"):
        await message.answer("Setup your wallet first with /start")
        return

    parts = (message.text or "").split()
    if len(parts) < 5:
        # Show active DCAs + usage
        dcas = await get_active_dca_configs(message.from_user.id)
        text = "<b>DCA Bot</b>\n\n"
        if dcas:
            for d in dcas:
                progress = f"{d['orders_executed']}/{d['orders_total']}"
                text += (
                    f"• <b>{d['symbol']}</b> {d['side'].upper()} — "
                    f"${d['amount_per_order']:,.0f} x {d['orders_total']}, "
                    f"progress: {progress}\n"
                )
            text += "\n"
        text += (
            "Usage: <code>/dca SYMBOL SIDE TOTAL ORDERS INTERVAL [LEVERAGE]</code>\n\n"
            "Example: <code>/dca BTC long 500 10 4h 5</code>\n"
            "→ Buy $50 of BTC every 4h, 10 times, at 5x\n\n"
            "Intervals: 1m, 5m, 15m, 1h, 4h, 1d\n"
            "Cancel: <code>/dca_stop</code>"
        )
        await message.answer(text)
        return

    try:
        symbol = parts[1].upper()
        if not symbol.endswith("-PERP"):
            symbol += "-PERP"
        side = parts[2].lower()
        if side not in ("long", "short"):
            raise ValueError("Side must be long or short")
        total = float(parts[3])
        orders = int(parts[4])
        interval_str = parts[5] if len(parts) > 5 else "1h"
        leverage = int(parts[6]) if len(parts) > 6 else 1
    except (ValueError, IndexError) as e:
        await message.answer(f"Invalid parameters: {e}")
        return

    # Parse interval
    interval_map = {
        "1m": 60, "5m": 300, "15m": 900, "30m": 1800,
        "1h": 3600, "4h": 14400, "1d": 86400,
    }
    interval_seconds = interval_map.get(interval_str)
    if not interval_seconds:
        await message.answer(f"Invalid interval: {interval_str}. Use: {', '.join(interval_map.keys())}")
        return

    amount_per = total / orders

    dca_id = await add_dca_config(
        telegram_id=message.from_user.id,
        symbol=symbol,
        side=side,
        mode="time",
        total_amount_usd=total,
        amount_per_order=amount_per,
        orders_total=orders,
        leverage=leverage,
        interval_seconds=interval_seconds,
    )

    duration_hrs = (interval_seconds * orders) / 3600
    text = (
        f"<b>DCA Started</b>\n\n"
        f"<b>{symbol}</b> {side.upper()} {leverage}x\n"
        f"Total: <code>${total:,.2f}</code>\n"
        f"Per order: <code>${amount_per:,.2f}</code>\n"
        f"Orders: <code>{orders}</code>\n"
        f"Every: <code>{interval_str}</code>\n"
        f"Duration: <code>~{duration_hrs:.1f}h</code>\n\n"
        f"First order executing now..."
    )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Stop DCA", callback_data=f"dca_stop:{dca_id}")],
        [InlineKeyboardButton(text="Menu", callback_data="nav:menu")],
    ])

    await message.answer(text, reply_markup=kb)


@router.message(Command("dca_stop"))
async def cmd_dca_stop(message: Message):
    dcas = await get_active_dca_configs(message.from_user.id)
    if not dcas:
        await message.answer("No active DCA configs.")
        return

    for d in dcas:
        await cancel_dca(d["id"], message.from_user.id)

    await message.answer(f"<b>Stopped {len(dcas)} DCA config(s).</b>")


@router.callback_query(F.data.startswith("dca_stop:"))
async def cb_dca_stop(callback: CallbackQuery):
    dca_id = int(callback.data.split(":")[1])
    await cancel_dca(dca_id, callback.from_user.id)
    await callback.answer("DCA stopped")
    await callback.message.edit_text("<b>DCA stopped.</b>")


# ══════════════════════════════════════════════════════════════
# SCALED ORDERS (Laddered Entry)
# ══════════════════════════════════════════════════════════════

@router.message(Command("scale"))
async def cmd_scale(message: Message):
    """
    /scale SYMBOL SIDE TOTAL LOW HIGH LEVELS [LEVERAGE]
    e.g. /scale BTC long 1000 95000 100000 5 10
    → 5 limit buys from $95k-$100k totaling $1000 at 10x
    """
    user = await get_user(message.from_user.id)
    if not user or not user.get("pacifica_account"):
        await message.answer("Setup your wallet first with /start")
        return

    parts = (message.text or "").split()
    if len(parts) < 7:
        await message.answer(
            "<b>Scaled Orders (Laddered Entry)</b>\n\n"
            "Usage: <code>/scale SYMBOL SIDE TOTAL LOW HIGH LEVELS [LEVERAGE]</code>\n\n"
            "Example: <code>/scale BTC long 1000 95000 100000 5 10</code>\n"
            "→ 5 limit buys from $95k to $100k, $200 each, at 10x\n\n"
            "Distribution is even across the price range."
        )
        return

    try:
        symbol = parts[1].upper()
        if not symbol.endswith("-PERP"):
            symbol += "-PERP"
        side = parts[2].lower()
        total = float(parts[3])
        price_low = float(parts[4])
        price_high = float(parts[5])
        levels = int(parts[6])
        leverage = int(parts[7]) if len(parts) > 7 else 1
    except (ValueError, IndexError) as e:
        await message.answer(f"Invalid parameters: {e}")
        return

    if price_low >= price_high:
        await message.answer("LOW price must be less than HIGH price.")
        return
    if levels < 2 or levels > 20:
        await message.answer("Levels must be between 2 and 20.")
        return

    # Place the limit orders
    from bot.services.wallet_manager import decrypt_private_key
    from bot.services.pacifica_client import PacificaClient

    kp = decrypt_private_key(user["agent_wallet_encrypted"])
    client = PacificaClient(account=user["pacifica_account"], keypair=kp)

    amount_per_level = total / levels
    price_step = (price_high - price_low) / (levels - 1)
    _, tick_size, lot_size = await get_market_info(symbol)

    placed = 0
    order_lines = []

    try:
        for i in range(levels):
            price = price_low + (price_step * i)
            token_amount = usd_to_token(amount_per_level, price, lot_size)

            if float(token_amount) <= 0:
                continue

            order_side = "buy" if side == "long" else "sell"
            tick_level = int(round(price / float(tick_size))) if float(tick_size) > 0 else int(price)

            try:
                await client.create_limit_order(
                    symbol=symbol,
                    side=order_side,
                    amount=token_amount,
                    tick_level=tick_level,
                )
                placed += 1
                order_lines.append(f"  ${price:,.2f} — {token_amount} tokens (${amount_per_level:,.0f})")
            except Exception as e:
                order_lines.append(f"  ${price:,.2f} — FAILED: {e}")
    finally:
        await client.close()

    await add_scaled_order(
        telegram_id=message.from_user.id,
        symbol=symbol, side=side, total_amount_usd=total,
        price_low=price_low, price_high=price_high,
        num_levels=levels, leverage=leverage,
    )

    text = (
        f"<b>Scaled Orders Placed</b>\n\n"
        f"<b>{symbol}</b> {side.upper()} {leverage}x\n"
        f"Total: <code>${total:,.2f}</code>\n"
        f"Range: <code>${price_low:,.2f}</code> → <code>${price_high:,.2f}</code>\n"
        f"Placed: <code>{placed}/{levels}</code>\n\n"
        f"<b>Orders:</b>\n"
    )
    text += "\n".join(order_lines[:10])
    if len(order_lines) > 10:
        text += f"\n  ... and {len(order_lines) - 10} more"

    await message.answer(text)


# ══════════════════════════════════════════════════════════════
# DAILY LOSS LIMIT
# ══════════════════════════════════════════════════════════════

@router.message(Command("maxloss"))
async def cmd_maxloss(message: Message):
    """
    /maxloss AMOUNT — Set daily max loss (USDC). Bot closes all if breached.
    /maxloss off — Disable.
    """
    user = await get_user(message.from_user.id)
    if not user:
        await message.answer("Setup your wallet first with /start")
        return

    parts = (message.text or "").split()
    if len(parts) < 2:
        from database.db import get_user_settings
        settings = await get_user_settings(message.from_user.id)
        current = settings.get("max_daily_loss")
        status = f"<code>${current:,.2f}</code>" if current else "OFF"
        await message.answer(
            f"<b>Daily Loss Limit</b>\n\n"
            f"Current: {status}\n\n"
            f"Set: <code>/maxloss 500</code>\n"
            f"Disable: <code>/maxloss off</code>\n\n"
            f"When daily realized losses exceed this limit, "
            f"all positions are closed and new trades are blocked until next day."
        )
        return

    val = parts[1].lower()
    from database.db import set_user_setting

    if val == "off":
        await set_user_setting(message.from_user.id, "max_daily_loss", None)
        await message.answer("<b>Daily loss limit disabled.</b>")
    else:
        try:
            limit = float(val)
            await set_user_setting(message.from_user.id, "max_daily_loss", limit)
            await message.answer(f"<b>Daily loss limit set to ${limit:,.2f}</b>")
        except ValueError:
            await message.answer("Invalid amount. Use a number or 'off'.")


# ══════════════════════════════════════════════════════════════
# FUNDING ALERT TOGGLE
# ══════════════════════════════════════════════════════════════

@router.message(Command("fundalert"))
async def cmd_fundalert(message: Message):
    """/fundalert on|off — Toggle funding rate alerts on positions."""
    parts = (message.text or "").split()
    if len(parts) < 2 or parts[1].lower() not in ("on", "off"):
        from database.db import get_user_settings
        settings = await get_user_settings(message.from_user.id)
        status = "ON" if settings.get("funding_alerts", True) else "OFF"
        await message.answer(
            f"<b>Funding Rate Alerts</b>\n\n"
            f"Status: {status}\n"
            f"Toggle: <code>/fundalert on</code> or <code>/fundalert off</code>\n\n"
            f"Get alerts when your positions are paying high funding rates."
        )
        return

    from database.db import set_user_setting
    enabled = parts[1].lower() == "on"
    await set_user_setting(message.from_user.id, "funding_alerts", enabled)
    await message.answer(f"<b>Funding alerts {'enabled' if enabled else 'disabled'}.</b>")


# ══════════════════════════════════════════════════════════════
# DASHBOARD LINK
# ══════════════════════════════════════════════════════════════

@router.message(Command("dashboard"))
async def cmd_dashboard(message: Message):
    """/dashboard — Link to the Trident web dashboard."""
    await message.answer(
        "<b>Trident Dashboard</b>\n\n"
        "View detailed analytics, markets, leaderboards, and more:\n\n"
        '<a href="https://trident-dashboard-phi.vercel.app">trident-dashboard-phi.vercel.app</a>',
        disable_web_page_preview=False,
    )


# ══════════════════════════════════════════════════════════════
# TWAP ORDERS
# ══════════════════════════════════════════════════════════════

@router.message(Command("twap"))
async def cmd_twap(message: Message):
    """
    /twap SYMBOL SIDE TOTAL SLICES DURATION [LEVERAGE]
    e.g. /twap BTC long 5000 20 2h 5
    → Buy $5000 BTC in 20 slices over 2 hours at 5x
    """
    user = await get_user(message.from_user.id)
    if not user or not user.get("pacifica_account"):
        await message.answer("Setup your wallet first with /start")
        return

    parts = (message.text or "").split()
    if len(parts) < 5:
        twaps = await get_active_twap_orders(message.from_user.id)
        text = "<b>TWAP Orders</b>\n\n"
        if twaps:
            for t in twaps:
                progress = f"{t['slices_executed']}/{t['num_slices']}"
                vwap = t.get('avg_price', 0)
                text += (
                    f"#{t['id']} <b>{t['symbol']}</b> {t['side'].upper()} — "
                    f"${t['total_amount_usd']:,.0f}, {progress} slices"
                )
                if vwap:
                    text += f", VWAP ${vwap:,.2f}"
                text += "\n"
            text += "\n"
        text += (
            "Usage: <code>/twap SYMBOL SIDE TOTAL SLICES DURATION [LEVERAGE]</code>\n\n"
            "Example: <code>/twap BTC long 5000 20 2h 5</code>\n"
            "Split $5000 into 20 equal orders over 2 hours at 5x\n\n"
            "Duration: 5m, 15m, 30m, 1h, 2h, 4h, 8h, 1d\n"
            "Cancel: <code>/twap_stop</code>"
        )
        await message.answer(text)
        return

    try:
        symbol = parts[1].upper()
        if not symbol.endswith("-PERP"):
            symbol += "-PERP"
        side = parts[2].lower()
        if side not in ("long", "short"):
            raise ValueError("Side must be long or short")
        total = float(parts[3])
        slices = int(parts[4])
        duration_str = parts[5] if len(parts) > 5 else "1h"
        leverage = int(parts[6]) if len(parts) > 6 else 1
    except (ValueError, IndexError) as e:
        await message.answer(f"Invalid parameters: {e}")
        return

    if slices < 2 or slices > 100:
        await message.answer("Slices must be between 2 and 100.")
        return

    # Parse duration to total seconds, then calculate interval
    duration_map = {
        "5m": 300, "15m": 900, "30m": 1800, "1h": 3600,
        "2h": 7200, "4h": 14400, "8h": 28800, "1d": 86400,
    }
    total_seconds = duration_map.get(duration_str)
    if not total_seconds:
        await message.answer(f"Invalid duration: {duration_str}. Use: {', '.join(duration_map.keys())}")
        return

    interval = total_seconds // slices
    if interval < 10:
        await message.answer("Too many slices for the duration. Minimum 10s between slices.")
        return

    amount_per = total / slices

    twap_id = await add_twap_order(
        telegram_id=message.from_user.id,
        symbol=symbol,
        side=side,
        total_amount_usd=total,
        num_slices=slices,
        interval_seconds=interval,
        amount_per_slice=amount_per,
        leverage=leverage,
        randomize=True,
    )

    text = (
        f"<b>TWAP Started</b>\n\n"
        f"<b>{symbol}</b> {side.upper()} {leverage}x\n"
        f"Total: <code>${total:,.2f}</code>\n"
        f"Slices: <code>{slices}</code> x <code>${amount_per:,.2f}</code>\n"
        f"Interval: <code>~{interval}s</code> (+/- 20% random)\n"
        f"Duration: <code>{duration_str}</code>\n\n"
        f"First slice executing now..."
    )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Stop TWAP", callback_data=f"twap_stop:{twap_id}")],
        [InlineKeyboardButton(text="Menu", callback_data="nav:menu")],
    ])

    await message.answer(text, reply_markup=kb)


@router.message(Command("twap_stop"))
async def cmd_twap_stop(message: Message):
    twaps = await get_active_twap_orders(message.from_user.id)
    if not twaps:
        await message.answer("No active TWAP orders.")
        return

    for t in twaps:
        await cancel_twap(t["id"], message.from_user.id)

    await message.answer(f"<b>Stopped {len(twaps)} TWAP order(s).</b>")


@router.callback_query(F.data.startswith("twap_stop:"))
async def cb_twap_stop(callback: CallbackQuery):
    twap_id = int(callback.data.split(":")[1])
    await cancel_twap(twap_id, callback.from_user.id)
    await callback.answer("TWAP stopped")
    await callback.message.edit_text("<b>TWAP order stopped.</b>")


# ══════════════════════════════════════════════════════════════
# ON-CHAIN WHALE TRACKER
# ══════════════════════════════════════════════════════════════

@router.message(Command("watch"))
async def cmd_watch(message: Message):
    """
    /watch <wallet> [min_usd] [label] — Watch on-chain wallet activity.
    /watch list — Show watched wallets.
    /watch remove <wallet> — Stop watching.
    """
    parts = (message.text or "").split()
    if len(parts) < 2:
        watches = await get_onchain_watches(message.from_user.id)
        text = "<b>On-Chain Whale Tracker</b>\n\n"
        if watches:
            for w in watches:
                addr = w["wallet_address"]
                label = w.get("label") or ""
                min_tx = w.get("min_tx_usd", 10000)
                label_str = f" ({label})" if label else ""
                text += f"<code>{addr[:8]}...{addr[-4:]}</code>{label_str} — min ${min_tx:,.0f}\n"
            text += "\n"
        text += (
            "Usage:\n"
            "<code>/watch WALLET</code> — watch with $10k min\n"
            "<code>/watch WALLET 50000</code> — watch with $50k min\n"
            "<code>/watch WALLET 10000 MyWhale</code> — with label\n"
            "<code>/watch remove WALLET</code> — stop watching\n\n"
            "Get alerts for USDC transfers on Solana."
        )
        await message.answer(text)
        return

    if parts[1].lower() == "list":
        watches = await get_onchain_watches(message.from_user.id)
        if not watches:
            await message.answer("No watched wallets. Add one with /watch <wallet>")
            return

        text = "<b>Watched Wallets</b>\n\n"
        for w in watches:
            addr = w["wallet_address"]
            label = w.get("label") or ""
            min_tx = w.get("min_tx_usd", 10000)
            label_str = f" ({label})" if label else ""
            text += (
                f"<code>{addr[:8]}...{addr[-4:]}</code>{label_str}\n"
                f"  Min alert: ${min_tx:,.0f}\n"
                f"  <code>/watch remove {addr[:16]}...</code>\n\n"
            )
        await message.answer(text)
        return

    if parts[1].lower() == "remove" and len(parts) > 2:
        wallet = parts[2].strip()
        removed = await remove_onchain_watch(message.from_user.id, wallet)
        if removed:
            await message.answer(f"Stopped watching <code>{wallet[:8]}...</code>")
        else:
            await message.answer("Wallet not found in your watch list.")
        return

    # Add new watch
    wallet = parts[1].strip()
    if len(wallet) < 20:
        await message.answer("Invalid wallet address.")
        return

    min_usd = 10000.0
    label = None
    if len(parts) > 2:
        try:
            min_usd = float(parts[2])
        except ValueError:
            label = parts[2]
    if len(parts) > 3:
        label = parts[3]

    added = await add_onchain_watch(message.from_user.id, wallet, label, min_usd)
    if added:
        label_str = f" ({label})" if label else ""
        await message.answer(
            f"<b>Now Watching</b>\n\n"
            f"<code>{wallet[:8]}...{wallet[-4:]}</code>{label_str}\n"
            f"Min alert: ${min_usd:,.0f} USDC\n\n"
            f"You'll get alerts for significant on-chain transfers."
        )
    else:
        await message.answer("Already watching this wallet.")


# ------------------------------------------------------------------
# /grid — start grid trading bot
# ------------------------------------------------------------------

@router.message(Command("grid"))
async def cmd_grid(message: Message):
    user = await get_user(message.from_user.id)
    if not user or not user.get("pacifica_account"):
        await message.answer("Please /start first.")
        return

    parts = (message.text or "").split()
    # /grid SYMBOL LOW HIGH GRIDS TOTAL_USD
    if len(parts) < 6:
        await message.answer(
            "<b>Grid Trading Bot</b>\n\n"
            "Places buy/sell orders across a price range.\n"
            "Profits from price oscillation within the range.\n\n"
            "<b>Usage:</b>\n"
            "<code>/grid BTC 60000 70000 10 $100</code>\n\n"
            "Symbol: BTC\n"
            "Range: $60,000 — $70,000\n"
            "Grids: 10 levels\n"
            "Total: $100 allocated\n\n"
            "Stop: <code>/grid_stop</code>"
        )
        return

    symbol = parts[1].upper()
    if not symbol.endswith("-PERP"):
        symbol += "-PERP"
    try:
        price_low = float(parts[2])
        price_high = float(parts[3])
        num_grids = int(parts[4])
        total_usd = float(parts[5].lstrip("$"))
    except ValueError:
        await message.answer("Invalid numbers. Usage: <code>/grid BTC 60000 70000 10 $100</code>")
        return

    if price_low >= price_high:
        await message.answer("Low price must be less than high price.")
        return
    if num_grids < 2 or num_grids > 50:
        await message.answer("Grid count must be 2-50.")
        return
    if total_usd < 5:
        await message.answer("Minimum total: $5.")
        return

    current = await get_price(symbol)
    if not current:
        await message.answer(f"Can't find price for {symbol}.")
        return

    from bot.services.grid_engine import add_grid_config
    grid_id = await add_grid_config(
        message.from_user.id, symbol, price_low, price_high, num_grids, total_usd,
    )

    amount_per = total_usd / num_grids
    grid_step = (price_high - price_low) / num_grids

    await message.answer(
        f"<b>Grid Bot Started</b>\n\n"
        f"ID: #{grid_id}\n"
        f"Symbol: {symbol}\n"
        f"Range: ${price_low:,.0f} — ${price_high:,.0f}\n"
        f"Grids: {num_grids} (step: ${grid_step:,.0f})\n"
        f"Per grid: ~${amount_per:,.1f}\n"
        f"Current price: ${current:,.2f}\n\n"
        f"The bot will buy low / sell high within the range.\n"
        f"Stop: <code>/grid_stop</code>"
    )


@router.message(Command("grid_stop"))
async def cmd_grid_stop(message: Message):
    tg_id = message.from_user.id
    from bot.services.grid_engine import get_active_grids, cancel_grid
    grids = await get_active_grids(tg_id)

    if not grids:
        await message.answer("No active grid bots.")
        return

    for g in grids:
        await cancel_grid(g["id"], tg_id)

    pnl_total = sum(g.get("realized_pnl", 0) for g in grids)
    await message.answer(
        f"Stopped <b>{len(grids)}</b> grid bot(s).\n"
        f"Total realized PnL: <code>${pnl_total:,.2f}</code>"
    )


# ------------------------------------------------------------------
# /arb — funding rate arbitrage scanner
# ------------------------------------------------------------------

@router.message(Command("arb"))
async def cmd_arb(message: Message):
    await message.answer("Scanning funding spreads HL vs Pacifica...")

    try:
        from bot.services.funding_arb import scan_funding_spreads
        spreads = await scan_funding_spreads()
    except Exception as e:
        await message.answer(f"Error: {e}")
        return

    if not spreads:
        await message.answer("No funding data available.")
        return

    # Sort by absolute spread
    spreads.sort(key=lambda x: abs(x["spread"]), reverse=True)

    lines = ["<b>Funding Arb Opportunities</b>\n", "<b>HL vs Pacifica (hourly rates)</b>\n"]
    for s in spreads[:10]:
        symbol = s["symbol"]
        hl = s["hl_rate"]
        pac = s["pacifica_rate"]
        spread = s["spread"]
        annual = abs(spread) * 24 * 365 * 100

        if spread > 0:
            direction = f"long Pac / short HL"
        else:
            direction = f"long HL / short Pac"

        emoji = "🟢" if abs(spread) >= 0.0005 else "⚪"
        lines.append(
            f"{emoji} <b>{symbol}</b>\n"
            f"  HL: {hl*100:+.4f}% | Pac: {pac*100:+.4f}%\n"
            f"  Spread: <b>{spread*100:.4f}%/hr</b> (~{annual:.0f}% APR)\n"
            f"  → {direction}\n"
        )

    if not any(abs(s["spread"]) >= 0.0001 for s in spreads):
        lines.append("\n<i>No significant spreads right now.</i>")

    await message.answer("\n".join(lines))


# ------------------------------------------------------------------
# /gaps — cross-exchange price comparison
# ------------------------------------------------------------------

@router.message(Command("gaps"))
async def cmd_gaps(message: Message):
    parts = (message.text or "").split()

    # /gaps stats — show gap tracking statistics
    if len(parts) > 1 and parts[1].lower() == "stats":
        try:
            from bot.services.gap_monitor import get_gap_stats
            stats = get_gap_stats()
        except Exception as e:
            await message.answer(f"Error: {e}")
            return

        if not stats:
            await message.answer("No gap data collected yet. Stats build over time.")
            return

        lines = ["<b>Gap Tracking Statistics</b>\n"]
        for s in stats[:10]:
            emoji = "\U0001f534" if abs(s["avg_gap"]) >= 0.5 else "\U0001f7e1" if abs(s["avg_gap"]) >= 0.2 else "\u26aa"
            lines.append(
                f"{emoji} <b>{s['symbol']}</b>\n"
                f"  Avg: <code>{s['avg_gap']:.4f}%</code> | "
                f"Max: <code>{s['max_gap']:.4f}%</code> | "
                f"Min: <code>{s['min_gap']:.4f}%</code>\n"
                f"  Now: <code>{s['current_gap']:.4f}%</code> | "
                f"Samples: {s['samples']}"
            )
        lines.append("\n<i>Use /gaps for live snapshot</i>")
        await message.answer("\n".join(lines))
        return

    await message.answer("Comparing prices HL vs Pacifica...")

    try:
        from bot.services.gap_monitor import _get_hl_prices, _get_pacifica_data
        import asyncio
        loop = asyncio.get_running_loop()
        hl_prices = await loop.run_in_executor(None, _get_hl_prices)
        pac_data = await _get_pacifica_data()
    except Exception as e:
        await message.answer(f"Error: {e}")
        return

    gaps = []
    for symbol, pac in pac_data.items():
        pac_price = pac.get("price", 0)
        hl_price = hl_prices.get(symbol, 0)
        if not pac_price or not hl_price:
            continue
        gap_pct = (hl_price - pac_price) / pac_price * 100
        gaps.append((symbol, pac_price, hl_price, gap_pct))

    gaps.sort(key=lambda x: abs(x[3]), reverse=True)

    lines = ["<b>Price Gaps — HL vs Pacifica</b>\n"]
    for symbol, pac_p, hl_p, gap in gaps[:15]:
        emoji = "\U0001f534" if abs(gap) >= 0.5 else "\U0001f7e1" if abs(gap) >= 0.2 else "\u26aa"
        lines.append(
            f"{emoji} <b>{symbol}</b>: ${hl_p:,.2f} (HL) vs ${pac_p:,.2f} (Pac) — "
            f"<b>{gap:+.3f}%</b>"
        )

    lines.append("\n<i>Use /gaps stats for tracking data</i>")
    await message.answer("\n".join(lines))


# ------------------------------------------------------------------
# /pulse — momentum detector signals
# ------------------------------------------------------------------

@router.message(Command("pulse"))
async def cmd_pulse(message: Message):
    """Show active momentum signals from the Pulse detector."""
    try:
        from bot.services.pulse_detector import get_active_signals
        signals = get_active_signals()
    except Exception as e:
        await message.answer(f"Error: {e}")
        return

    if not signals:
        await message.answer(
            "<b>Pulse Momentum Detector</b>\n\n"
            "No active signals right now.\n"
            "Signals are detected automatically every 60s.\n\n"
            "<i>Types: FIRST_JUMP, CONTRIB_EXPLOSION, "
            "IMMEDIATE_MOVER, NEW_ENTRY_DEEP, DEEP_CLIMBER</i>"
        )
        return

    lines = [f"<b>Pulse — {len(signals)} Active Signal(s)</b>\n"]
    for s in signals[:10]:
        tier = s.get("tier", "?")
        symbol = s.get("symbol", "?")
        confidence = s.get("confidence", 0)
        direction = s.get("direction", "?")
        oi_change = s.get("oi_change_pct", 0)
        vol_ratio = s.get("volume_ratio", 0)

        dir_emoji = "\U0001f7e2" if direction == "LONG" else "\U0001f534" if direction == "SHORT" else "\u26aa"
        lines.append(
            f"{dir_emoji} <b>{symbol}</b> — {tier}\n"
            f"   Confidence: {confidence} | Direction: {direction}\n"
            f"   OI: {oi_change:+.1f}% | Vol: {vol_ratio:.1f}x avg"
        )

    lines.append("\n<i>Auto-scanning every 60s</i>")
    await message.answer("\n".join(lines))


# ------------------------------------------------------------------
# /radar — opportunity scanner
# ------------------------------------------------------------------

@router.message(Command("radar"))
async def cmd_radar(message: Message):
    """Show latest Radar scan results."""
    try:
        from bot.services.radar_scanner import get_latest_scan
        results = get_latest_scan()
    except Exception as e:
        await message.answer(f"Error: {e}")
        return

    if not results:
        await message.answer(
            "<b>Radar Opportunity Scanner</b>\n\n"
            "No scan data yet. Radar runs every 15 minutes.\n\n"
            "<i>Scores assets 0-400 on: market structure, "
            "technicals, funding, BTC macro</i>"
        )
        return

    lines = ["<b>Radar — Latest Scan</b>\n"]
    for i, r in enumerate(results[:10], 1):
        symbol = r.get("symbol", "?")
        score = r.get("score", 0)
        direction = r.get("direction", "?")
        price = r.get("price", 0)
        funding = r.get("funding", 0)
        oi_change = r.get("oi_change_pct", 0)
        rsi = r.get("rsi", 50)

        dir_emoji = "\U0001f7e2" if direction == "LONG" else "\U0001f534" if direction == "SHORT" else "\u26aa"
        score_bar = "\U0001f525" if score >= 300 else "\u26a1" if score >= 200 else "\u2022"

        lines.append(
            f"{score_bar} {dir_emoji} <b>{symbol}</b> — {direction} (Score: {score}/400)\n"
            f"   Price: ${price:,.2f} | OI: {oi_change:+.1f}%\n"
            f"   Funding: {funding*100:+.4f}%/hr | RSI: {rsi:.0f}"
        )

    total = r.get("total_scanned", len(results)) if results else 0
    lines.append(f"\n<i>Scanned {total} assets | Next scan in ~15 min</i>")
    await message.answer("\n".join(lines))
