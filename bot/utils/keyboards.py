"""
Inline keyboards — full interactive UX.
"""

from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton


# ------------------------------------------------------------------
# Main menu (shown after /start and accessible everywhere)
# ------------------------------------------------------------------

def main_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="📊 Markets", callback_data="nav:markets"),
                InlineKeyboardButton(text="⚡ Trade", callback_data="nav:trade"),
            ],
            [
                InlineKeyboardButton(text="📈 Positions", callback_data="nav:positions"),
                InlineKeyboardButton(text="📋 Orders", callback_data="nav:orders"),
            ],
            [
                InlineKeyboardButton(text="💰 Balance", callback_data="nav:balance"),
                InlineKeyboardButton(text="📉 PnL", callback_data="nav:pnl"),
            ],
            [
                InlineKeyboardButton(text="👥 Copy Trading", callback_data="nav:copy"),
                InlineKeyboardButton(text="⚙️ Settings", callback_data="nav:settings"),
            ],
        ]
    )


def back_to_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Menu", callback_data="nav:menu")],
        ]
    )


# ------------------------------------------------------------------
# Markets
# ------------------------------------------------------------------

def markets_kb(markets: list, prices: dict | None = None) -> InlineKeyboardMarkup:
    """Build market list from /info data. Optional prices dict {symbol: price_str}."""
    # Show top markets first (by max_leverage as proxy for popularity)
    top_symbols = ["BTC", "ETH", "SOL", "TRUMP", "HYPE", "DOGE", "XRP", "SUI", "LINK", "AVAX"]
    prices = prices or {}

    rows = []
    shown = set()
    # Top markets first
    for sym in top_symbols:
        m = next((x for x in markets if x.get("symbol") == sym), None)
        if m:
            price_str = f"  ${prices[sym]}" if sym in prices else ""
            lev = m.get("max_leverage", "?")
            rows.append([
                InlineKeyboardButton(
                    text=f"{sym}{price_str}  ({lev}x)",
                    callback_data=f"market:{sym}",
                )
            ])
            shown.add(sym)

    # "More markets" button for the rest
    remaining = len(markets) - len(shown)
    if remaining > 0:
        rows.append([
            InlineKeyboardButton(text=f"📋 All Markets ({len(markets)} total)", callback_data="nav:markets_all"),
        ])

    rows.append([
        InlineKeyboardButton(text="🏆 Leaderboard", callback_data="nav:leaderboard"),
        InlineKeyboardButton(text="🔄 Refresh", callback_data="nav:markets"),
    ])
    rows.append([InlineKeyboardButton(text="◀️ Menu", callback_data="nav:menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def markets_all_kb(markets: list) -> InlineKeyboardMarkup:
    """Full market list."""
    rows = []
    for m in sorted(markets, key=lambda x: x.get("symbol", "")):
        sym = m.get("symbol", "?")
        lev = m.get("max_leverage", "?")
        rows.append([
            InlineKeyboardButton(
                text=f"{sym}  ({lev}x)",
                callback_data=f"market:{sym}",
            )
        ])
    rows.append([
        InlineKeyboardButton(text="◀️ Markets", callback_data="nav:markets"),
        InlineKeyboardButton(text="◀️ Menu", callback_data="nav:menu"),
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def market_detail_kb(symbol: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="🟢 Long", callback_data=f"trade:long:{symbol}"),
                InlineKeyboardButton(text="🔴 Short", callback_data=f"trade:short:{symbol}"),
            ],
            [
                InlineKeyboardButton(text="📊 Orderbook", callback_data=f"ob:{symbol}"),
                InlineKeyboardButton(text="👥 Copy Top Trader", callback_data="nav:copy"),
            ],
            [
                InlineKeyboardButton(text="◀️ Markets", callback_data="nav:markets"),
                InlineKeyboardButton(text="◀️ Menu", callback_data="nav:menu"),
            ],
        ]
    )


# ------------------------------------------------------------------
# Trading flow (step by step via buttons)
# ------------------------------------------------------------------

def trade_amount_kb(symbol: str, side: str) -> InlineKeyboardMarkup:
    """Quick USDC amount selection."""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="$25", callback_data=f"amt:{side}:{symbol}:25"),
                InlineKeyboardButton(text="$50", callback_data=f"amt:{side}:{symbol}:50"),
                InlineKeyboardButton(text="$100", callback_data=f"amt:{side}:{symbol}:100"),
            ],
            [
                InlineKeyboardButton(text="$250", callback_data=f"amt:{side}:{symbol}:250"),
                InlineKeyboardButton(text="$500", callback_data=f"amt:{side}:{symbol}:500"),
                InlineKeyboardButton(text="$1000", callback_data=f"amt:{side}:{symbol}:1000"),
            ],
            [
                InlineKeyboardButton(text="Custom ✏️", callback_data=f"amt_custom:{side}:{symbol}"),
                InlineKeyboardButton(text="◀️ Back", callback_data=f"market:{symbol}"),
            ],
        ]
    )


def trade_leverage_kb(symbol: str, side: str, amount: str, max_lev: int = 50) -> InlineKeyboardMarkup:
    """Leverage selection — adapts buttons to the symbol's max leverage."""
    presets = [1, 2, 5, 10, 20, 50]
    available = [x for x in presets if x <= max_lev]

    rows = []
    # Split into rows of 3
    for i in range(0, len(available), 3):
        row = [
            InlineKeyboardButton(
                text=f"{x}x",
                callback_data=f"lev:{side}:{symbol}:{amount}:{x}",
            )
            for x in available[i : i + 3]
        ]
        rows.append(row)

    rows.append([
        InlineKeyboardButton(text="Custom ✏️", callback_data=f"lev_custom:{side}:{symbol}:{amount}"),
        InlineKeyboardButton(text="◀️ Back", callback_data=f"trade:{'long' if side == 'bid' else 'short'}:{symbol}"),
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def confirm_trade_kb(side: str, symbol: str, amount: str, leverage: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✅ Confirm & Send",
                    callback_data=f"exec:{side}:{symbol}:{amount}:{leverage}",
                ),
            ],
            [
                InlineKeyboardButton(text="❌ Cancel", callback_data=f"market:{symbol}"),
            ],
        ]
    )


# ------------------------------------------------------------------
# Positions
# ------------------------------------------------------------------

def positions_kb(positions: list) -> InlineKeyboardMarkup:
    rows = []
    for pos in positions:
        symbol = pos.get("symbol", "?")
        side = pos.get("side", "bid")
        direction = "🟢" if side == "bid" else "🔴"
        pnl = pos.get("unrealized_pnl", pos.get("pnl", "0"))
        rows.append([
            InlineKeyboardButton(
                text=f"{direction} {symbol}  PnL: {pnl}",
                callback_data=f"pos:{symbol}",
            )
        ])
    if not rows:
        rows.append([InlineKeyboardButton(text="No positions — Trade now!", callback_data="nav:trade")])
    rows.append([
        InlineKeyboardButton(text="🔄 Refresh", callback_data="nav:positions"),
        InlineKeyboardButton(text="◀️ Menu", callback_data="nav:menu"),
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def position_detail_kb(symbol: str, side: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="🎯 Set TP", callback_data=f"set_tp:{symbol}"),
                InlineKeyboardButton(text="🛑 Set SL", callback_data=f"set_sl:{symbol}"),
            ],
            [
                InlineKeyboardButton(text="❌ Close Position", callback_data=f"close_pos:{symbol}"),
            ],
            [
                InlineKeyboardButton(text="◀️ Positions", callback_data="nav:positions"),
                InlineKeyboardButton(text="◀️ Menu", callback_data="nav:menu"),
            ],
        ]
    )


def confirm_close_kb(symbol: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Close Now", callback_data=f"exec_close:{symbol}"),
                InlineKeyboardButton(text="❌ Cancel", callback_data=f"pos:{symbol}"),
            ],
        ]
    )


def close_all_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Close ALL", callback_data="exec_closeall"),
                InlineKeyboardButton(text="❌ Cancel", callback_data="nav:positions"),
            ],
        ]
    )


# ------------------------------------------------------------------
# Copy trading
# ------------------------------------------------------------------

def copy_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="➕ Copy a Wallet", callback_data="copy:add"),
                InlineKeyboardButton(text="👥 My Masters", callback_data="copy:masters"),
            ],
            [
                InlineKeyboardButton(text="📜 Copy Log", callback_data="copy:log"),
            ],
            [
                InlineKeyboardButton(text="◀️ Menu", callback_data="nav:menu"),
            ],
        ]
    )


def copy_settings_kb(wallet: str) -> InlineKeyboardMarkup:
    w = wallet[:8]  # shorten for callback data limit (64 bytes)
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="1x", callback_data=f"cm:{w}:1.0"),
                InlineKeyboardButton(text="0.5x", callback_data=f"cm:{w}:0.5"),
                InlineKeyboardButton(text="0.25x", callback_data=f"cm:{w}:0.25"),
            ],
            [
                InlineKeyboardButton(text="Max $500", callback_data=f"cx:{w}:500"),
                InlineKeyboardButton(text="Max $1K", callback_data=f"cx:{w}:1000"),
                InlineKeyboardButton(text="Max $5K", callback_data=f"cx:{w}:5000"),
            ],
            [
                InlineKeyboardButton(text="✅ Start Copying", callback_data=f"copy_go:{wallet}"),
                InlineKeyboardButton(text="❌ Cancel", callback_data="nav:copy"),
            ],
        ]
    )


# ------------------------------------------------------------------
# Settings
# ------------------------------------------------------------------

def settings_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="🔗 Link Wallet", callback_data="set:link"),
                InlineKeyboardButton(text="🔑 Agent Wallet", callback_data="set:agent"),
            ],
            [
                InlineKeyboardButton(text="📊 Network Info", callback_data="set:network"),
            ],
            [
                InlineKeyboardButton(text="◀️ Menu", callback_data="nav:menu"),
            ],
        ]
    )
