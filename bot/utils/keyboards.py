"""
Inline keyboards — full interactive UX.
"""

from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from bot.services.solana_client import is_devnet


# ------------------------------------------------------------------
# Main menu (shown after /start and accessible everywhere)
# ------------------------------------------------------------------

def main_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="📊 Trade", callback_data="nav:markets"),
                InlineKeyboardButton(text="💳 Wallet", callback_data="nav:wallet"),
            ],
            [
                InlineKeyboardButton(text="📈 Positions", callback_data="nav:positions"),
                InlineKeyboardButton(text="📋 Orders", callback_data="nav:orders"),
            ],
            [
                InlineKeyboardButton(text="💰 Balance", callback_data="nav:balance"),
                InlineKeyboardButton(text="📜 History", callback_data="nav:history"),
            ],
            [
                InlineKeyboardButton(text="👥 Copy Trading", callback_data="nav:copy"),
                InlineKeyboardButton(text="🔔 Alerts", callback_data="nav:alerts"),
            ],
            [
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
    top_symbols = ["BTC", "ETH", "SOL", "TRUMP", "HYPE", "DOGE", "XRP", "SUI", "LINK", "AVAX"]
    prices = prices or {}

    rows = []
    shown = set()
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
                InlineKeyboardButton(text="📗 Limit Buy", callback_data=f"limit:bid:{symbol}"),
                InlineKeyboardButton(text="📕 Limit Sell", callback_data=f"limit:ask:{symbol}"),
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
                InlineKeyboardButton(
                    text="✅ Send + Set TP/SL",
                    callback_data=f"exec_tpsl:{side}:{symbol}:{amount}:{leverage}",
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
                InlineKeyboardButton(text="Close 25%", callback_data=f"pclose:{symbol}:25"),
                InlineKeyboardButton(text="Close 50%", callback_data=f"pclose:{symbol}:50"),
                InlineKeyboardButton(text="Close 75%", callback_data=f"pclose:{symbol}:75"),
            ],
            [
                InlineKeyboardButton(text="❌ Close 100%", callback_data=f"close_pos:{symbol}"),
            ],
            [
                InlineKeyboardButton(text="◀️ Positions", callback_data="nav:positions"),
                InlineKeyboardButton(text="◀️ Menu", callback_data="nav:menu"),
            ],
        ]
    )


def confirm_limit_kb(side: str, symbol: str, amount: str, price: str, leverage: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✅ Place Limit Order",
                    callback_data=f"exec_limit:{side}:{symbol}:{amount}:{price}:{leverage}",
                ),
            ],
            [
                InlineKeyboardButton(text="❌ Cancel", callback_data=f"market:{symbol}"),
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
                InlineKeyboardButton(text="◀️ Menu", callback_data="nav:menu"),
            ],
        ]
    )


def copy_settings_kb(wallet: str) -> InlineKeyboardMarkup:
    w = wallet[:8]
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
# Onboarding
# ------------------------------------------------------------------

def onboarding_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="📥 Import Wallet", callback_data="onboard:import"),
            ],
            [
                InlineKeyboardButton(text="🆕 Generate New Wallet", callback_data="onboard:generate"),
            ],
        ]
    )


# ------------------------------------------------------------------
# Wallet dashboard
# ------------------------------------------------------------------

def wallet_kb(sol_balance: float = 0, usdc_balance: float = 0) -> InlineKeyboardMarkup:
    """Wallet action buttons. Shows devnet-only buttons when appropriate."""
    rows = []

    # Devnet-only actions
    if is_devnet():
        devnet_row = []
        if sol_balance < 0.05:
            devnet_row.append(InlineKeyboardButton(text="☀️ SOL Faucet", callback_data="wallet:airdrop"))
        devnet_row.append(InlineKeyboardButton(text="🚰 USDC Faucet", callback_data="wallet:faucet"))
        if devnet_row:
            rows.append(devnet_row)

    # Deposit / Withdraw
    rows.append([
        InlineKeyboardButton(text="💰 Deposit", callback_data="wallet:deposit"),
        InlineKeyboardButton(text="📤 Withdraw", callback_data="wallet:withdraw"),
    ])

    # Utility
    rows.append([
        InlineKeyboardButton(text="🔑 Export Key", callback_data="wallet:export"),
        InlineKeyboardButton(text="🔄 Refresh", callback_data="wallet:refresh"),
    ])

    rows.append([InlineKeyboardButton(text="◀️ Menu", callback_data="nav:menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def wallet_deposit_kb(usdc_balance: float) -> InlineKeyboardMarkup:
    """Deposit amount selection buttons."""
    rows = []

    # Quick amounts (only show if user has enough)
    quick = [100, 500, 1000, 5000]
    row = []
    for amt in quick:
        if usdc_balance >= amt:
            row.append(InlineKeyboardButton(text=f"${amt:,}", callback_data=f"dep:{amt}"))
        if len(row) == 3:
            rows.append(row)
            row = []
    if row:
        rows.append(row)

    # All + Custom
    rows.append([
        InlineKeyboardButton(text=f"All (${usdc_balance:,.0f})", callback_data=f"dep:{int(usdc_balance)}"),
        InlineKeyboardButton(text="Custom ✏️", callback_data="dep:custom"),
    ])

    rows.append([
        InlineKeyboardButton(text="◀️ Wallet", callback_data="nav:wallet"),
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def wallet_withdraw_kb(pac_balance: float) -> InlineKeyboardMarkup:
    """Withdraw amount selection buttons."""
    rows = []

    quick = [100, 500, 1000, 5000]
    row = []
    for amt in quick:
        if pac_balance >= amt:
            row.append(InlineKeyboardButton(text=f"${amt:,}", callback_data=f"wdraw:{amt}"))
        if len(row) == 3:
            rows.append(row)
            row = []
    if row:
        rows.append(row)

    rows.append([
        InlineKeyboardButton(text=f"All (${pac_balance:,.0f})", callback_data=f"wdraw:{int(pac_balance)}"),
        InlineKeyboardButton(text="Custom ✏️", callback_data="wdraw:custom"),
    ])

    rows.append([
        InlineKeyboardButton(text="◀️ Wallet", callback_data="nav:wallet"),
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


# ------------------------------------------------------------------
# Price alerts
# ------------------------------------------------------------------

def alerts_kb(alerts: list) -> InlineKeyboardMarkup:
    rows = []
    for a in alerts[:10]:
        symbol = a.get("symbol", "?")
        direction = "above" if a.get("direction") == "above" else "below"
        price = a.get("target_price", "?")
        alert_id = a.get("id", 0)
        emoji = "📈" if direction == "above" else "📉"
        rows.append([
            InlineKeyboardButton(
                text=f"{emoji} {symbol} {direction} ${price}",
                callback_data=f"alert_del:{alert_id}",
            )
        ])
    if not rows:
        rows.append([InlineKeyboardButton(text="No alerts — set one!", callback_data="nav:markets")])
    rows.append([
        InlineKeyboardButton(text="➕ New Alert", callback_data="alert:new"),
        InlineKeyboardButton(text="◀️ Menu", callback_data="nav:menu"),
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


# ------------------------------------------------------------------
# Settings
# ------------------------------------------------------------------

def settings_kb(settings: dict | None = None) -> InlineKeyboardMarkup:
    s = settings or {}
    slip = s.get("slippage", "0.5")
    lev = s.get("default_leverage", "10")

    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="💳 My Wallet", callback_data="set:wallet"),
                InlineKeyboardButton(text="🔄 Switch Wallet", callback_data="set:import"),
            ],
            [
                InlineKeyboardButton(text=f"Slippage: {slip}%", callback_data="set:slippage_menu"),
                InlineKeyboardButton(text=f"Leverage: {lev}x", callback_data="set:leverage_menu"),
            ],
            [
                InlineKeyboardButton(text="🔑 Activate Beta", callback_data="wallet:claim_beta"),
                InlineKeyboardButton(text="🔗 Referral", callback_data="set:referral"),
            ],
            [
                InlineKeyboardButton(text="📊 Network Info", callback_data="set:network"),
            ],
            [
                InlineKeyboardButton(text="◀️ Menu", callback_data="nav:menu"),
            ],
        ]
    )


def slippage_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="0.1%", callback_data="set:slippage:0.1"),
                InlineKeyboardButton(text="0.3%", callback_data="set:slippage:0.3"),
                InlineKeyboardButton(text="0.5%", callback_data="set:slippage:0.5"),
                InlineKeyboardButton(text="1%", callback_data="set:slippage:1"),
            ],
            [
                InlineKeyboardButton(text="2%", callback_data="set:slippage:2"),
                InlineKeyboardButton(text="3%", callback_data="set:slippage:3"),
                InlineKeyboardButton(text="5%", callback_data="set:slippage:5"),
            ],
            [InlineKeyboardButton(text="◀️ Settings", callback_data="nav:settings")],
        ]
    )


def leverage_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="1x", callback_data="set:deflev:1"),
                InlineKeyboardButton(text="2x", callback_data="set:deflev:2"),
                InlineKeyboardButton(text="5x", callback_data="set:deflev:5"),
            ],
            [
                InlineKeyboardButton(text="10x", callback_data="set:deflev:10"),
                InlineKeyboardButton(text="20x", callback_data="set:deflev:20"),
                InlineKeyboardButton(text="50x", callback_data="set:deflev:50"),
            ],
            [InlineKeyboardButton(text="◀️ Settings", callback_data="nav:settings")],
        ]
    )
