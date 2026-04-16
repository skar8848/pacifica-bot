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


def fmt_position(pos: dict, mark_price: float | None = None, funding_rate: float | None = None) -> str:
    symbol = pos.get("symbol", "?")
    side = pos.get("side", "?")
    amount = pos.get("amount", "0")
    entry = pos.get("entry_price", "?")
    liq = pos.get("liquidation_price", "?")
    funding_paid = pos.get("funding", "0")
    isolated = pos.get("isolated", False)

    direction = "LONG" if side == "bid" else "SHORT"
    emoji = "🟢" if side == "bid" else "🔴"
    mode = "Isolated" if isolated else "Cross"

    text = f"{emoji} <b>{symbol}</b> {direction}\n"
    text += f"  Size: {amount}\n"
    text += f"  Entry: ${_fmt_price(entry)}\n"

    if mark_price:
        text += f"  Mark: ${_fmt_price(mark_price)}\n"

    # Liquidation price — hide if unrealistic
    try:
        liq_f = float(liq)
        entry_f = float(entry)
        if 0 < liq_f < entry_f * 10:
            text += f"  Liq: ${_fmt_price(liq)}\n"
    except (ValueError, TypeError):
        pass

    # Funding: paid / hourly rate / annualized
    try:
        fp = float(funding_paid)
        funding_str = f"${fp:,.2f}"
    except (ValueError, TypeError):
        funding_str = str(funding_paid)

    if funding_rate is not None:
        hourly_pct = funding_rate * 100
        annual_pct = funding_rate * 8760 * 100  # 24 * 365
        text += f"  Funding: {funding_str} | {hourly_pct:.4f}%/h | {annual_pct:.1f}%/y\n"
    else:
        text += f"  Funding: {funding_str}\n"

    # PnL — calculated from mark price, including funding costs
    if mark_price:
        try:
            amt_f = float(amount)
            entry_f = float(entry)
            if side == "bid":
                price_pnl = (mark_price - entry_f) * amt_f
            else:
                price_pnl = (entry_f - mark_price) * amt_f
            # Subtract funding costs (funding_paid is cumulative cost)
            try:
                funding_cost = float(funding_paid)
            except (ValueError, TypeError):
                funding_cost = 0
            pnl_f = price_pnl - funding_cost
            pnl_color = "🟢" if pnl_f >= 0 else "🔴"
            pnl_sign = "+" if pnl_f >= 0 else ""
            text += f"  PnL: {pnl_color} <b>{pnl_sign}${pnl_f:,.2f}</b>\n"
        except (ValueError, TypeError):
            pass

    text += f"  {mode}\n"
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

    # Sort by all-time PnL (descending)
    traders = sorted(traders, key=lambda t: float(t.get("pnl_all_time", 0)), reverse=True)

    text = "<b>🏆 Top Traders (by PnL)</b>\n\n"
    for i, t in enumerate(traders[:10], 1):
        addr = t.get("address", "?")
        pnl_all = float(t.get("pnl_all_time", 0))
        equity = float(t.get("equity_current", 0))
        emoji = "🟢" if pnl_all >= 0 else "🔴"
        pnl_sign = "+" if pnl_all >= 0 else ""
        short = f"{addr[:6]}...{addr[-4:]}"
        text += (
            f"{emoji} <b>{i}.</b> {short}\n"
            f"   PnL: <b>{pnl_sign}${pnl_all:,.0f}</b> | Equity: ${equity:,.0f}\n\n"
        )
    return text
