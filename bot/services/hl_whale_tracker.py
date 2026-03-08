"""
Hyperliquid Whale Tracker — monitors whale wallets on Hyperliquid
and posts alerts to the Telegram group when positions change.

Approach:
  1. Maintain a list of tracked HL wallets (admin-managed + auto-discovered)
  2. Snapshot positions every CHECK_INTERVAL seconds
  3. Detect opens/closes/size changes above threshold
  4. Post alerts to group feed
"""

import asyncio
import logging
import json
from urllib.request import Request, urlopen

from aiogram import Bot

from database.db import get_db

logger = logging.getLogger(__name__)

_running = False
CHECK_INTERVAL = 60  # seconds
MIN_POSITION_USD = 100_000  # Only alert on positions >= $100K
MIN_ACCOUNT_VALUE = 50_000  # Only track accounts >= $50K

HL_API_URL = "https://api.hyperliquid.xyz/info"

# In-memory snapshot of last known positions per wallet
_snapshots: dict[str, dict[str, dict]] = {}


# ------------------------------------------------------------------
# DB helpers
# ------------------------------------------------------------------

async def _init_hl_tables():
    db = await get_db()
    await db.executescript("""
        CREATE TABLE IF NOT EXISTS hl_tracked_wallets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            address TEXT NOT NULL UNIQUE,
            label TEXT,
            auto_discovered INTEGER DEFAULT 0,
            active INTEGER DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    await db.commit()


async def add_hl_wallet(address: str, label: str | None = None, auto: bool = False) -> bool:
    db = await get_db()
    try:
        await db.execute(
            "INSERT INTO hl_tracked_wallets (address, label, auto_discovered) VALUES (?, ?, ?)",
            (address.lower(), label, int(auto)),
        )
        await db.commit()
        return True
    except Exception:
        return False


async def remove_hl_wallet(address: str) -> bool:
    db = await get_db()
    cursor = await db.execute(
        "DELETE FROM hl_tracked_wallets WHERE address = ?", (address.lower(),)
    )
    await db.commit()
    return cursor.rowcount > 0


async def get_hl_tracked_wallets() -> list[dict]:
    db = await get_db()
    async with db.execute(
        "SELECT address, label FROM hl_tracked_wallets WHERE active = 1"
    ) as cursor:
        return [dict(r) for r in await cursor.fetchall()]


# ------------------------------------------------------------------
# Hyperliquid API
# ------------------------------------------------------------------

def _hl_post(payload: dict) -> dict | list:
    """Synchronous HL API call (run in executor)."""
    req = Request(
        HL_API_URL,
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urlopen(req, timeout=10) as resp:
        return json.loads(resp.read())


async def hl_get_positions(address: str) -> dict:
    """Get clearinghouse state for an address."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(
        None, _hl_post, {"type": "clearinghouseState", "user": address}
    )


async def hl_get_recent_trades(coin: str, limit: int = 100) -> list:
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(
        None, _hl_post, {"type": "recentTrades", "coin": coin, "limit": limit}
    )


async def hl_get_all_mids() -> dict[str, float]:
    """Get all mid prices."""
    loop = asyncio.get_event_loop()
    data = await loop.run_in_executor(None, _hl_post, {"type": "allMids"})
    return {k: float(v) for k, v in data.items()}


# ------------------------------------------------------------------
# Auto-discovery: find whales from recent large trades
# ------------------------------------------------------------------

async def discover_whales() -> list[str]:
    """Scan recent trades on major coins to find whale wallets."""
    wallets = set()
    for coin in ["BTC", "ETH", "SOL"]:
        try:
            trades = await hl_get_recent_trades(coin, limit=100)
            for t in trades:
                for u in t.get("users", []):
                    wallets.add(u)
        except Exception as e:
            logger.debug("Failed to get %s trades: %s", coin, e)

    new_whales = []
    for addr in wallets:
        try:
            state = await hl_get_positions(addr)
            av = float(state.get("marginSummary", {}).get("accountValue", "0"))
            if av >= MIN_ACCOUNT_VALUE:
                added = await add_hl_wallet(addr, auto=True)
                if added:
                    new_whales.append(addr)
                    logger.info("Auto-discovered HL whale: %s ($%,.0f)", addr, av)
        except Exception:
            pass

    return new_whales


# ------------------------------------------------------------------
# Position change detection
# ------------------------------------------------------------------

def _parse_positions(state: dict) -> dict[str, dict]:
    """Parse clearinghouse state into {coin: {side, size_usd, entry, leverage}}."""
    result = {}
    for p in state.get("assetPositions", []):
        pos = p.get("position", p)
        coin = pos.get("coin", "")
        if not coin:
            continue
        szi = float(pos.get("szi", "0"))
        ntl = abs(float(pos.get("positionValue", "0")))
        entry = pos.get("entryPx", "0")
        lev = pos.get("leverage", {})
        lev_val = lev.get("value", "?") if isinstance(lev, dict) else str(lev)
        upnl = float(pos.get("unrealizedPnl", "0"))

        result[coin] = {
            "side": "LONG" if szi > 0 else "SHORT",
            "size_usd": ntl,
            "size_token": abs(szi),
            "entry": entry,
            "leverage": lev_val,
            "unrealized_pnl": upnl,
        }
    return result


def _fmt_usd(val: float) -> str:
    abs_val = abs(val)
    if abs_val >= 1_000_000:
        return f"{'−' if val < 0 else ''}${abs_val / 1_000_000:.2f}M"
    if abs_val >= 1_000:
        return f"{'−' if val < 0 else ''}${abs_val / 1_000:.1f}K"
    return f"{'−' if val < 0 else ''}${abs_val:,.0f}"


async def _check_wallet(bot: Bot, address: str, label: str | None):
    """Check a wallet for position changes and alert."""
    try:
        state = await hl_get_positions(address)
    except Exception as e:
        logger.debug("HL fetch failed for %s: %s", address, e)
        return

    account_value = float(state.get("marginSummary", {}).get("accountValue", "0"))
    current = _parse_positions(state)
    prev = _snapshots.get(address, {})
    _snapshots[address] = current

    # Skip first snapshot (no comparison)
    if not prev:
        return

    display = label or f"{address[:6]}...{address[-4:]}"

    # Detect new positions
    for coin, pos in current.items():
        if pos["size_usd"] < MIN_POSITION_USD:
            continue

        if coin not in prev:
            # New position opened
            text = (
                f"\U0001f433 <b>HL Whale Alert</b>\n\n"
                f"<b>{display}</b> opened <b>{coin}</b> {pos['side']}\n"
                f"Size: <b>{_fmt_usd(pos['size_usd'])}</b> ({pos['leverage']}x)\n"
                f"Entry: ${pos['entry']}\n"
                f"Account: {_fmt_usd(account_value)}"
            )
            await _post_hl_alert(bot, text)

        elif prev[coin]["side"] != pos["side"]:
            # Side flipped
            text = (
                f"\U0001f504 <b>HL Whale Flip</b>\n\n"
                f"<b>{display}</b> flipped <b>{coin}</b>\n"
                f"{prev[coin]['side']} \u2192 {pos['side']}\n"
                f"Size: <b>{_fmt_usd(pos['size_usd'])}</b> ({pos['leverage']}x)\n"
                f"Account: {_fmt_usd(account_value)}"
            )
            await _post_hl_alert(bot, text)

        else:
            # Check for significant size increase (>50%)
            prev_size = prev[coin]["size_usd"]
            if prev_size > 0:
                change_pct = (pos["size_usd"] - prev_size) / prev_size * 100
                change_usd = pos["size_usd"] - prev_size
                if abs(change_usd) >= MIN_POSITION_USD and abs(change_pct) >= 50:
                    action = "added to" if change_usd > 0 else "reduced"
                    text = (
                        f"\U0001f4ca <b>HL Whale Size Change</b>\n\n"
                        f"<b>{display}</b> {action} <b>{coin}</b> {pos['side']}\n"
                        f"{_fmt_usd(prev_size)} \u2192 {_fmt_usd(pos['size_usd'])} "
                        f"({change_pct:+.0f}%)\n"
                        f"Account: {_fmt_usd(account_value)}"
                    )
                    await _post_hl_alert(bot, text)

    # Detect closed positions
    for coin, pos in prev.items():
        if pos["size_usd"] < MIN_POSITION_USD:
            continue
        if coin not in current:
            pnl = pos.get("unrealized_pnl", 0)
            pnl_str = f"\nLast uPnL: {_fmt_usd(pnl)}" if pnl else ""
            text = (
                f"\U0001f510 <b>HL Whale Close</b>\n\n"
                f"<b>{display}</b> closed <b>{coin}</b> {pos['side']}\n"
                f"Was: {_fmt_usd(pos['size_usd'])}{pnl_str}\n"
                f"Account: {_fmt_usd(account_value)}"
            )
            await _post_hl_alert(bot, text)


async def _post_hl_alert(bot: Bot, text: str):
    """Post to the alert group."""
    from bot.services.group_feed import post_to_group
    await post_to_group(bot, text)


# ------------------------------------------------------------------
# Start / Stop
# ------------------------------------------------------------------

_discovery_counter = 0
DISCOVERY_INTERVAL = 20  # Run discovery every 20 cycles (~20 min)


async def start_hl_whale_tracker(bot: Bot):
    global _running, _discovery_counter
    await _init_hl_tables()
    _running = True
    logger.info("HL whale tracker started (interval=%ds)", CHECK_INTERVAL)

    # Seed with default whales if empty
    tracked = await get_hl_tracked_wallets()
    if not tracked:
        logger.info("No HL wallets tracked, running initial discovery...")
        found = await discover_whales()
        logger.info("Discovered %d HL whales", len(found))

    while _running:
        try:
            wallets = await get_hl_tracked_wallets()
            for w in wallets:
                await _check_wallet(bot, w["address"], w.get("label"))
                await asyncio.sleep(0.5)  # Rate limit

            # Periodic discovery of new whales
            _discovery_counter += 1
            if _discovery_counter >= DISCOVERY_INTERVAL:
                _discovery_counter = 0
                await discover_whales()

        except Exception as e:
            logger.error("HL whale tracker error: %s", e)

        await asyncio.sleep(CHECK_INTERVAL)


def stop_hl_whale_tracker():
    global _running
    _running = False
    logger.info("HL whale tracker stopped.")
