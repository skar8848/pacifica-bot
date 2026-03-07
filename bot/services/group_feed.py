"""
Trident Feed — posts alerts to a public Telegram group.

Events posted:
- Liquidation warnings (from liquidation_monitor)
- Whale activity (from whale_monitor)
- Funding rate spikes
- Big trades / PnL cards from leaders
- Pacifica market events (new listings, etc.)

The group ID is stored in DB (set via /setgroup admin command)
and can also be overridden via ALERT_GROUP_ID env var.
"""

import logging

from aiogram import Bot

from database.db import get_db

logger = logging.getLogger(__name__)

# Runtime group ID — loaded from DB or env on first use
_group_id: int | None = None
_loaded = False


async def _load_group_id() -> int | None:
    """Load group ID from DB, falling back to env var."""
    global _group_id, _loaded

    if _loaded:
        return _group_id

    # Try DB first
    db = await get_db()
    async with db.execute(
        "SELECT value FROM bot_settings WHERE key = 'alert_group_id'"
    ) as cursor:
        row = await cursor.fetchone()
        if row:
            try:
                _group_id = int(row[0])
                _loaded = True
                return _group_id
            except (ValueError, TypeError):
                pass

    # Fall back to env
    from bot.config import ALERT_GROUP_ID
    if ALERT_GROUP_ID:
        _group_id = ALERT_GROUP_ID

    _loaded = True
    return _group_id


async def set_group_id(group_id: int):
    """Save group ID to DB."""
    global _group_id, _loaded
    db = await get_db()
    await db.execute(
        """INSERT INTO bot_settings (key, value) VALUES ('alert_group_id', ?)
           ON CONFLICT(key) DO UPDATE SET value = ?""",
        (str(group_id), str(group_id)),
    )
    await db.commit()
    _group_id = group_id
    _loaded = True
    logger.info("Alert group ID set to %s", group_id)


async def get_group_id() -> int | None:
    return await _load_group_id()


# ------------------------------------------------------------------
# Post helpers
# ------------------------------------------------------------------

async def post_to_group(bot: Bot, text: str, **kwargs) -> bool:
    """Post a message to the alert group. Returns True if sent."""
    gid = await _load_group_id()
    if not gid:
        return False

    try:
        await bot.send_message(gid, text, disable_web_page_preview=True, **kwargs)
        return True
    except Exception as e:
        logger.warning("Failed to post to group %s: %s", gid, e)
        return False


async def post_liquidation_alert(
    bot: Bot, username: str, symbol: str, side: str,
    distance_pct: float, liq_price: float, mark_price: float,
):
    """Post a liquidation warning to the group."""
    emoji = "\U0001f6a8" if distance_pct <= 3 else "\u26a0\ufe0f"
    side_label = "LONG" if side == "bid" else "SHORT"
    text = (
        f"{emoji} <b>Liquidation Warning</b>\n\n"
        f"@{username} — <b>{symbol}</b> {side_label}\n"
        f"Distance: <b>{distance_pct:.1f}%</b>\n"
        f"Liq price: <code>${liq_price:,.2f}</code>\n"
        f"Mark: <code>${mark_price:,.2f}</code>"
    )
    await post_to_group(bot, text)


async def post_whale_alert(
    bot: Bot, address: str, oi_change: float,
    equity: float, pnl_all: float, username: str | None = None,
):
    """Post a whale OI change alert to the group."""
    direction = "opened" if oi_change > 0 else "closed"
    abs_change = abs(oi_change)
    name = f"@{username}" if username else f"<code>{address[:8]}...</code>"

    if abs_change >= 1_000_000:
        change_str = f"${abs_change / 1_000_000:.2f}M"
    elif abs_change >= 1_000:
        change_str = f"${abs_change / 1_000:.1f}K"
    else:
        change_str = f"${abs_change:,.0f}"

    emoji = "\U0001f433" if abs_change >= 100_000 else "\U0001f40b"
    text = (
        f"{emoji} <b>Whale Alert</b>\n\n"
        f"{name} {direction} <b>{change_str}</b> in positions\n"
        f"Equity: ${equity:,.0f} | All-time PnL: ${pnl_all:,.0f}"
    )
    await post_to_group(bot, text)


async def post_funding_spike(
    bot: Bot, symbol: str, funding_rate: float,
):
    """Post a funding rate spike alert."""
    hourly_pct = funding_rate * 100
    annual_pct = funding_rate * 24 * 365 * 100
    direction = "Longs pay shorts" if funding_rate > 0 else "Shorts pay longs"
    sym = symbol.replace("-PERP", "")
    text = (
        f"\U0001f4b8 <b>Funding Spike — {sym}</b>\n\n"
        f"Rate: <code>{hourly_pct:+.4f}%</code>/hr ({annual_pct:+.1f}% APR)\n"
        f"{direction}"
    )
    await post_to_group(bot, text)


async def post_leader_trade(
    bot: Bot, username: str, symbol: str, side: str,
    amount_usd: float, leverage: str | int = "?",
):
    """Post when a public leader opens a trade."""
    side_label = "LONG" if side in ("bid", "buy", "long") else "SHORT"
    emoji = "\U0001f7e2" if side_label == "LONG" else "\U0001f534"
    text = (
        f"{emoji} <b>Leader Trade</b>\n\n"
        f"@{username} opened <b>{symbol}</b> {side_label} {leverage}x\n"
        f"Size: <code>${amount_usd:,.0f}</code>"
    )
    await post_to_group(bot, text)


async def post_leader_pnl(
    bot: Bot, username: str, symbol: str, side: str,
    pnl_usd: float, pnl_pct: float,
):
    """Post when a leader closes with notable PnL."""
    side_label = "LONG" if side in ("bid", "buy", "long") else "SHORT"
    emoji = "\U0001f7e2" if pnl_usd >= 0 else "\U0001f534"
    sign = "+" if pnl_usd >= 0 else ""
    text = (
        f"{emoji} <b>Leader PnL</b>\n\n"
        f"@{username} closed <b>{symbol}</b> {side_label}\n"
        f"PnL: <code>{sign}${pnl_usd:,.2f}</code> ({sign}{pnl_pct:.1f}%)"
    )
    await post_to_group(bot, text)
