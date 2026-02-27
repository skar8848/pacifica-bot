"""
Telegram message formatting — adapted to real Pacifica API response formats.
"""


def _fmt_price(val) -> str:
    """Format a price value with appropriate decimals."""
    try:
        f = float(val)
        if abs(f) >= 1000:
            return f"{f:,.2f}"
        elif abs(f) >= 1:
            return f"{f:.2f}"
        else:
            return f"{f:.6f}"
    except (ValueError, TypeError):
        return str(val)


def fmt_position(pos: dict) -> str:
    symbol = pos.get("symbol", "?")
    side = pos.get("side", "?")
    amount = pos.get("amount", "0")
    entry = pos.get("entry_price", "?")
    mark = pos.get("mark_price", "")
    liq = pos.get("liquidation_price", "?")
    funding = pos.get("funding", "0")
    upnl = pos.get("unrealized_pnl", pos.get("pnl", ""))
    leverage = pos.get("leverage", "")
    notional = pos.get("notional_value", pos.get("notional", ""))
    isolated = pos.get("isolated", False)

    direction = "LONG" if side == "bid" else "SHORT"
    emoji = "🟢" if side == "bid" else "🔴"
    mode = "Isolated" if isolated else "Cross"

    text = f"{emoji} <b>{symbol}</b> {direction}"
    if leverage:
        text += f" ({leverage}x)"
    text += "\n"
    text += f"  Size: {amount}\n"
    if notional:
        text += f"  Notional: ${_fmt_price(notional)}\n"
    text += f"  Entry: ${_fmt_price(entry)}\n"
    if mark:
        text += f"  Mark: ${_fmt_price(mark)}\n"
    if upnl:
        try:
            pnl_f = float(upnl)
            pnl_color = "🟢" if pnl_f >= 0 else "🔴"
            pnl_sign = "+" if pnl_f >= 0 else ""
            text += f"  PnL: {pnl_color} <b>{pnl_sign}${pnl_f:,.2f}</b>\n"
        except (ValueError, TypeError):
            text += f"  PnL: {upnl}\n"
    # Hide liquidation price if it's negative (cross mode artifact)
    try:
        liq_f = float(liq)
        if liq_f > 0:
            text += f"  Liq: ${_fmt_price(liq)}\n"
    except (ValueError, TypeError):
        text += f"  Liq: ${liq}\n"
    text += f"  Funding: {funding} | {mode}\n"
    return text


def fmt_order(order: dict) -> str:
    symbol = order.get("symbol", "?")
    side = order.get("side", "?")
    amount = order.get("amount", "?")
    price = order.get("price", order.get("limit_price", "?"))
    order_type = order.get("order_type", order.get("type", "?"))
    order_id = order.get("order_id", order.get("id", "?"))
    status = order.get("status", "")

    direction = "BUY" if side == "bid" else "SELL"
    emoji = "🟢" if side == "bid" else "🔴"

    return (
        f"{emoji} <b>{symbol}</b> {direction} {order_type}\n"
        f"  Amount: {amount} @ ${price}\n"
        f"  Status: {status}\n"
        f"  ID: <code>{order_id}</code>\n"
    )


def fmt_trade_summary(
    action: str,
    symbol: str,
    side: str,
    amount: str,
    leverage: str = "",
    price: str = "",
) -> str:
    direction = "LONG" if side == "bid" else "SHORT"
    emoji = "🟢" if side == "bid" else "🔴"
    parts = [f"{emoji} <b>{action} {symbol} {direction}</b>", f"Amount: {amount}"]
    if leverage:
        parts.append(f"Leverage: {leverage}x")
    if price:
        parts.append(f"Price: ${price}")
    return "\n".join(parts)


def fmt_balance(info: dict) -> str:
    """Format account info from GET /account response."""
    balance = info.get("balance", "?")
    equity = info.get("account_equity", "?")
    available = info.get("available_to_spend", "?")
    margin_used = info.get("total_margin_used", "?")
    withdrawable = info.get("available_to_withdraw", "?")
    positions = info.get("positions_count", 0)
    orders = info.get("orders_count", 0)
    taker_fee = info.get("taker_fee", "?")
    maker_fee = info.get("maker_fee", "?")

    return (
        "<b>💰 Account Balance</b>\n\n"
        f"  Balance: <b>${balance}</b>\n"
        f"  Equity: ${equity}\n"
        f"  Available: ${available}\n"
        f"  Withdrawable: ${withdrawable}\n"
        f"  Margin Used: ${margin_used}\n\n"
        f"  Positions: {positions} | Orders: {orders}\n"
        f"  Fees: maker {maker_fee} / taker {taker_fee}\n"
    )


def fmt_pnl(trades: list) -> str:
    if not trades:
        return "<b>📉 PnL</b>\n\nNo recent trades."

    lines = ["<b>📉 Recent Trades</b>\n"]
    for t in trades[:15]:
        symbol = t.get("symbol", "?")
        side = t.get("side", "?")
        price = t.get("price", "?")
        amount = t.get("amount", "?")
        event = t.get("event_type", "")
        lines.append(f"  {symbol} {side} {amount} @ ${price} ({event})")

    return "\n".join(lines)


def fmt_market_info(market: dict) -> str:
    """Format a single market from GET /info."""
    symbol = market.get("symbol", "?")
    max_lev = market.get("max_leverage", "?")
    tick = market.get("tick_size", "?")
    lot = market.get("lot_size", "?")
    min_order = market.get("min_order_size", "?")
    max_order = market.get("max_order_size", "?")
    funding = market.get("funding_rate", "?")

    return (
        f"<b>{symbol}</b>\n"
        f"  Max leverage: {max_lev}x\n"
        f"  Tick: {tick} | Lot: {lot}\n"
        f"  Min order: ${min_order}\n"
        f"  Funding: {funding}\n"
    )


def fmt_orderbook(ob: dict, symbol: str) -> str:
    """Format orderbook from GET /book — format: {s, l: [{p, a, n}...], ...}."""
    longs = ob.get("l", [])[:5]   # bids
    shorts = ob.get("s", [])      # this is the symbol actually
    # The real format: "l" = bids (longs), second array = asks (shorts)
    # Data structure: {"s": "BTC", "l": [bids...], and asks in second position}
    # Let me handle both arrays
    all_levels = ob if isinstance(ob, list) else None

    # Actual format from API: {"s": symbol, "l": [[bid_levels], [ask_levels]]}
    # where "l" contains two arrays
    levels = ob.get("l", [])
    bids = levels[0] if len(levels) > 0 else []
    asks = levels[1] if len(levels) > 1 else []

    text = f"<b>📊 {symbol} Orderbook</b>\n\n"

    text += "<b>Asks (Sells)</b>\n"
    for a in reversed(asks[:5]):
        text += f"  🔴 ${a['p']}  |  {a['a']}\n"

    text += "\n<b>Bids (Buys)</b>\n"
    for b in bids[:5]:
        text += f"  🟢 ${b['p']}  |  {b['a']}\n"

    return text


def fmt_leaderboard(traders: list) -> str:
    if not traders:
        return "No leaderboard data."

    text = "<b>🏆 Top Traders</b>\n\n"
    for i, t in enumerate(traders[:10], 1):
        addr = t.get("address", "?")
        pnl_all = float(t.get("pnl_all_time", 0))
        equity = float(t.get("equity_current", 0))
        vol_30d = float(t.get("volume_30d", 0))
        name = t.get("username") or f"{addr[:6]}...{addr[-4:]}"

        pnl_sign = "+" if pnl_all >= 0 else ""
        text += (
            f"<b>{i}.</b> <code>{addr[:8]}...</code>\n"
            f"   PnL: {pnl_sign}{pnl_all:,.0f} | Equity: ${equity:,.0f}\n"
            f"   Vol 30d: ${vol_30d:,.0f}\n\n"
        )
    return text
