"""
Funding rate arbitrage engine — compares funding rates between Hyperliquid
and Pacifica, posts spread alerts, and can auto-execute delta-neutral
positions to capture funding spread.

Strategy:
  - Long on the exchange where longs *receive* funding (cheaper rate)
  - Short on the exchange where shorts *receive* funding (more expensive rate)
  - Collect the spread as profit while remaining delta-neutral

Background loop runs every 5 minutes via start_funding_arb(bot) / stop_funding_arb().
"""

import asyncio
import json
import logging
from datetime import datetime
from urllib.request import Request, urlopen

from aiogram import Bot

from bot.services.pacifica_client import PacificaClient
from bot.services.market_data import get_price, get_lot_size, usd_to_token
from bot.services.wallet_manager import decrypt_private_key
from database.db import get_db, get_user

logger = logging.getLogger(__name__)

_running = False
SCAN_INTERVAL = 300  # 5 minutes

# Thresholds (hourly rates)
ALERT_THRESHOLD = 0.0001  # 0.01% per hour  (~87% annualized)
EXIT_THRESHOLD = 0.00002  # 0.002% per hour  (~17% annualized) — close when spread narrows

HL_API_URL = "https://api.hyperliquid.xyz/info"

# In-memory cache of last posted alert time per symbol to avoid spam
_last_alert: dict[str, datetime] = {}
ALERT_COOLDOWN = 1800  # 30 minutes between alerts for the same symbol


# ------------------------------------------------------------------
# DB helpers
# ------------------------------------------------------------------

async def _init_arb_tables():
    """Create the arb_positions table if it doesn't exist."""
    db = await get_db()
    await db.executescript("""
        CREATE TABLE IF NOT EXISTS arb_positions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            telegram_id INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            long_exchange TEXT NOT NULL,
            short_exchange TEXT NOT NULL,
            size_usd REAL NOT NULL,
            entry_spread REAL NOT NULL,
            current_spread REAL DEFAULT 0,
            accumulated_pnl REAL DEFAULT 0,
            active INTEGER DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    await db.commit()


async def _get_active_arb_positions(telegram_id: int | None = None) -> list[dict]:
    db = await get_db()
    if telegram_id:
        q = "SELECT * FROM arb_positions WHERE telegram_id = ? AND active = 1"
        params = (telegram_id,)
    else:
        q = "SELECT * FROM arb_positions WHERE active = 1"
        params = ()
    async with db.execute(q, params) as cursor:
        return [dict(r) for r in await cursor.fetchall()]


async def _create_arb_position(
    telegram_id: int, symbol: str, long_exchange: str,
    short_exchange: str, size_usd: float, entry_spread: float,
) -> int:
    db = await get_db()
    cursor = await db.execute(
        """INSERT INTO arb_positions
           (telegram_id, symbol, long_exchange, short_exchange, size_usd, entry_spread)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (telegram_id, symbol, long_exchange, short_exchange, size_usd, entry_spread),
    )
    await db.commit()
    return cursor.lastrowid  # type: ignore


async def _close_arb_position(position_id: int, accumulated_pnl: float):
    db = await get_db()
    await db.execute(
        "UPDATE arb_positions SET active = 0, accumulated_pnl = ? WHERE id = ?",
        (accumulated_pnl, position_id),
    )
    await db.commit()


async def _update_arb_spread(position_id: int, current_spread: float, accumulated_pnl: float):
    db = await get_db()
    await db.execute(
        "UPDATE arb_positions SET current_spread = ?, accumulated_pnl = ? WHERE id = ?",
        (current_spread, accumulated_pnl, position_id),
    )
    await db.commit()


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


async def _hl_get_funding_rates() -> dict[str, float]:
    """Fetch current hourly funding rates for all HL perps.

    Returns: {symbol: hourly_funding_rate} e.g. {"BTC": 0.00005, ...}
    """
    loop = asyncio.get_event_loop()
    data = await loop.run_in_executor(None, _hl_post, {"type": "metaAndAssetCtxs"})

    if not isinstance(data, list) or len(data) < 2:
        logger.warning("Unexpected HL metaAndAssetCtxs response format")
        return {}

    meta = data[0]  # {universe: [{name: "BTC", ...}, ...]}
    asset_ctxs = data[1]  # [{funding: "0.00005", ...}, ...]

    universe = meta.get("universe", [])
    if len(universe) != len(asset_ctxs):
        logger.warning("HL universe/assetCtxs length mismatch: %d vs %d",
                        len(universe), len(asset_ctxs))
        return {}

    rates = {}
    for coin_info, ctx in zip(universe, asset_ctxs):
        symbol = coin_info.get("name", "")
        funding_str = ctx.get("funding", "0")
        if symbol and funding_str:
            try:
                rates[symbol] = float(funding_str)
            except (ValueError, TypeError):
                pass

    return rates


# ------------------------------------------------------------------
# Pacifica funding rates
# ------------------------------------------------------------------

async def _pac_get_funding_rates() -> dict[str, float]:
    """Fetch current hourly funding rates from Pacifica.

    Returns: {symbol: hourly_funding_rate} e.g. {"BTC": 0.00003, ...}
    """
    from solders.keypair import Keypair as _Kp
    client = PacificaClient(account="public", keypair=_Kp())
    try:
        prices = await client.get_prices()
        if not isinstance(prices, list):
            return {}

        rates = {}
        for p in prices:
            symbol = p.get("symbol", "")
            funding = p.get("funding", "0")
            if symbol:
                # Normalize symbol: strip "-PERP" suffix if present
                clean = symbol.replace("-PERP", "").replace("-perp", "")
                try:
                    rates[clean] = float(funding)
                except (ValueError, TypeError):
                    pass
        return rates
    finally:
        await client.close()


# ------------------------------------------------------------------
# Spread scanning
# ------------------------------------------------------------------

async def scan_funding_spreads() -> list[dict]:
    """Compare funding rates between HL and Pacifica.

    Returns sorted list of dicts:
        {symbol, pacifica_rate, hl_rate, spread, abs_spread, annualized_pct, direction}

    direction = "long_pac_short_hl" means: long on Pacifica, short on Hyperliquid
                (Pacifica rate is lower / more negative, so longs pay less there)
    """
    try:
        hl_rates, pac_rates = await asyncio.gather(
            _hl_get_funding_rates(),
            _pac_get_funding_rates(),
        )
    except Exception as e:
        logger.error("Failed to fetch funding rates: %s", e)
        return []

    if not hl_rates or not pac_rates:
        return []

    # Find common symbols
    common = set(hl_rates.keys()) & set(pac_rates.keys())
    if not common:
        logger.debug("No common symbols between HL (%d) and Pacifica (%d)",
                      len(hl_rates), len(pac_rates))
        return []

    spreads = []
    for symbol in common:
        hl_rate = hl_rates[symbol]
        pac_rate = pac_rates[symbol]

        # Spread = HL rate - Pacifica rate
        # If positive: HL longs pay more → long Pacifica, short HL
        # If negative: Pacifica longs pay more → long HL, short Pacifica
        spread = hl_rate - pac_rate
        abs_spread = abs(spread)

        if spread >= 0:
            direction = "long_pac_short_hl"
        else:
            direction = "long_hl_short_pac"

        # Annualized: hourly rate * 24 * 365
        annualized_pct = abs_spread * 24 * 365 * 100

        spreads.append({
            "symbol": symbol,
            "pacifica_rate": pac_rate,
            "hl_rate": hl_rate,
            "spread": spread,
            "abs_spread": abs_spread,
            "annualized_pct": annualized_pct,
            "direction": direction,
        })

    # Sort by absolute spread descending
    spreads.sort(key=lambda x: x["abs_spread"], reverse=True)
    return spreads


# ------------------------------------------------------------------
# Alert formatting
# ------------------------------------------------------------------

def _fmt_rate(rate: float) -> str:
    """Format an hourly funding rate as a percentage string."""
    return f"{rate * 100:+.4f}%"


def _direction_label(direction: str) -> tuple[str, str]:
    """Return (long_label, short_label) for a direction string."""
    if direction == "long_pac_short_hl":
        return "Pacifica", "Hyperliquid"
    return "Hyperliquid", "Pacifica"


async def post_funding_spread_alert(bot: Bot, spreads: list[dict]):
    """Post the top funding spread opportunities to the group chat."""
    from bot.services.group_feed import post_to_group

    # Filter to only actionable spreads above threshold
    actionable = [s for s in spreads if s["abs_spread"] >= ALERT_THRESHOLD]
    if not actionable:
        return

    # Respect cooldown per symbol
    now = datetime.utcnow()
    to_alert = []
    for s in actionable[:5]:
        last = _last_alert.get(s["symbol"])
        if last and (now - last).total_seconds() < ALERT_COOLDOWN:
            continue
        to_alert.append(s)

    if not to_alert:
        return

    # Build the message
    lines = [
        "<b>Funding Arbitrage Scanner</b>",
        "",
        "Top cross-exchange funding spreads:",
        "",
    ]

    for i, s in enumerate(to_alert, 1):
        long_on, short_on = _direction_label(s["direction"])
        bar = _spread_bar(s["annualized_pct"])

        lines.append(
            f"<b>{i}. {s['symbol']}</b>"
        )
        lines.append(
            f"   HL: <code>{_fmt_rate(s['hl_rate'])}</code>/hr  |  "
            f"Pac: <code>{_fmt_rate(s['pacifica_rate'])}</code>/hr"
        )
        lines.append(
            f"   Spread: <code>{_fmt_rate(s['abs_spread'])}</code>/hr "
            f"({s['annualized_pct']:.1f}% APR)"
        )
        lines.append(
            f"   {bar} Long {long_on} / Short {short_on}"
        )
        lines.append("")

        _last_alert[s["symbol"]] = now

    lines.append(
        "<i>Rates are hourly. Positive spread = arb opportunity.</i>"
    )

    text = "\n".join(lines)
    await post_to_group(bot, text)
    logger.info("Posted funding arb alert for %d symbols", len(to_alert))


def _spread_bar(annualized_pct: float) -> str:
    """Generate a visual bar for the spread magnitude."""
    if annualized_pct >= 500:
        return "\U0001f525\U0001f525\U0001f525"  # fire x3
    if annualized_pct >= 200:
        return "\U0001f525\U0001f525"
    if annualized_pct >= 100:
        return "\U0001f525"
    return "\u26a1"  # lightning bolt


# ------------------------------------------------------------------
# Auto-execution
# ------------------------------------------------------------------

async def open_arb_position(
    telegram_id: int, symbol: str, size_usd: float, direction: str,
) -> dict | None:
    """Open a delta-neutral arb position across both exchanges.

    Currently only executes the Pacifica leg (HL execution is manual / future).
    Records the position in DB for tracking.

    Returns position dict on success, None on failure.
    """
    user = await get_user(telegram_id)
    if not user or not user.get("pacifica_account") or not user.get("agent_wallet_encrypted"):
        logger.warning("Cannot open arb for %s: wallet not configured", telegram_id)
        return None

    long_exchange, short_exchange = (
        ("pacifica", "hyperliquid") if direction == "long_pac_short_hl"
        else ("hyperliquid", "pacifica")
    )

    # Determine Pacifica side
    pac_side = "buy" if long_exchange == "pacifica" else "sell"

    try:
        kp = decrypt_private_key(user["agent_wallet_encrypted"])
        client = PacificaClient(
            account=user["pacifica_account"],
            keypair=kp,
        )
        try:
            price = await get_price(symbol)
            if not price:
                logger.error("Cannot get price for %s", symbol)
                return None

            lot_size = await get_lot_size(symbol)
            token_amount = usd_to_token(size_usd, price, lot_size)
            if float(token_amount) <= 0:
                logger.error("Token amount is zero for %s ($%.2f at $%.2f)",
                             symbol, size_usd, price)
                return None

            # Execute Pacifica leg
            result = await client.create_market_order(
                symbol=symbol,
                side=pac_side,
                amount=token_amount,
            )
            logger.info("Arb Pacifica leg executed: %s %s %s (result=%s)",
                        pac_side, token_amount, symbol, result)

        finally:
            await client.close()

    except Exception as e:
        logger.error("Failed to execute Pacifica arb leg for %s %s: %s",
                     telegram_id, symbol, e)
        return None

    # Fetch current spread for entry record
    spreads = await scan_funding_spreads()
    entry_spread = 0.0
    for s in spreads:
        if s["symbol"] == symbol:
            entry_spread = s["abs_spread"]
            break

    # Record in DB
    pos_id = await _create_arb_position(
        telegram_id=telegram_id,
        symbol=symbol,
        long_exchange=long_exchange,
        short_exchange=short_exchange,
        size_usd=size_usd,
        entry_spread=entry_spread,
    )

    return {
        "id": pos_id,
        "symbol": symbol,
        "long_exchange": long_exchange,
        "short_exchange": short_exchange,
        "size_usd": size_usd,
        "entry_spread": entry_spread,
    }


async def close_arb_position(position: dict, bot: Bot | None = None) -> bool:
    """Close an active arb position by unwinding the Pacifica leg.

    Returns True on success.
    """
    telegram_id = position["telegram_id"]
    symbol = position["symbol"]

    user = await get_user(telegram_id)
    if not user or not user.get("pacifica_account") or not user.get("agent_wallet_encrypted"):
        logger.warning("Cannot close arb for %s: wallet not configured", telegram_id)
        return False

    # Close the Pacifica leg (opposite side)
    pac_was_long = position["long_exchange"] == "pacifica"
    close_side = "sell" if pac_was_long else "buy"

    try:
        kp = decrypt_private_key(user["agent_wallet_encrypted"])
        client = PacificaClient(
            account=user["pacifica_account"],
            keypair=kp,
        )
        try:
            price = await get_price(symbol)
            if not price:
                logger.error("Cannot get price for %s to close arb", symbol)
                return False

            lot_size = await get_lot_size(symbol)
            token_amount = usd_to_token(position["size_usd"], price, lot_size)
            if float(token_amount) <= 0:
                logger.error("Token amount is zero when closing arb %s", symbol)
                return False

            result = await client.create_market_order(
                symbol=symbol,
                side=close_side,
                amount=token_amount,
            )
            logger.info("Arb Pacifica close executed: %s %s %s (result=%s)",
                        close_side, token_amount, symbol, result)

        finally:
            await client.close()

    except Exception as e:
        logger.error("Failed to close Pacifica arb leg for %s %s: %s",
                     telegram_id, symbol, e)
        return False

    # Mark position as closed
    accumulated_pnl = position.get("accumulated_pnl", 0)
    await _close_arb_position(position["id"], accumulated_pnl)

    # Notify user
    if bot:
        try:
            text = (
                f"<b>Funding Arb Closed</b>\n\n"
                f"<b>{symbol}</b>\n"
                f"Long: {position['long_exchange'].title()} / "
                f"Short: {position['short_exchange'].title()}\n"
                f"Size: <code>${position['size_usd']:,.2f}</code>\n"
                f"Entry spread: <code>{_fmt_rate(position['entry_spread'])}</code>/hr\n"
                f"Accumulated PnL: <code>${accumulated_pnl:,.2f}</code>"
            )
            await bot.send_message(telegram_id, text)
        except Exception:
            pass

    return True


# ------------------------------------------------------------------
# Position monitoring (spread check + auto-close)
# ------------------------------------------------------------------

async def _monitor_arb_positions(bot: Bot):
    """Check active arb positions and close if spread has narrowed."""
    positions = await _get_active_arb_positions()
    if not positions:
        return

    spreads = await scan_funding_spreads()
    spread_map = {s["symbol"]: s for s in spreads}

    for pos in positions:
        symbol = pos["symbol"]
        current = spread_map.get(symbol)

        if not current:
            # Symbol no longer available — skip this cycle
            logger.debug("Arb position %s: symbol %s not in spread scan", pos["id"], symbol)
            continue

        current_spread = current["abs_spread"]

        # Estimate accumulated PnL (hourly rate * size * hours since last check)
        # This is an approximation — actual PnL depends on position sizes and
        # how much funding was actually collected on each exchange
        hours_elapsed = SCAN_INTERVAL / 3600.0
        spread_pnl = current_spread * pos["size_usd"] * hours_elapsed
        accumulated = (pos.get("accumulated_pnl") or 0) + spread_pnl

        await _update_arb_spread(pos["id"], current_spread, accumulated)

        # Auto-close if spread has narrowed below exit threshold
        if current_spread < EXIT_THRESHOLD:
            logger.info("Arb position %s (%s) spread narrowed to %.6f — closing",
                        pos["id"], symbol, current_spread)
            await close_arb_position(pos, bot)

            # Notify user about auto-close
            try:
                text = (
                    f"<b>Funding Arb Auto-Closed</b>\n\n"
                    f"<b>{symbol}</b> spread narrowed below exit threshold.\n"
                    f"Entry spread: <code>{_fmt_rate(pos['entry_spread'])}</code>/hr\n"
                    f"Exit spread: <code>{_fmt_rate(current_spread)}</code>/hr\n"
                    f"Est. accumulated PnL: <code>${accumulated:,.2f}</code>"
                )
                await bot.send_message(pos["telegram_id"], text)
            except Exception:
                pass


# ------------------------------------------------------------------
# Start / Stop
# ------------------------------------------------------------------

async def start_funding_arb(bot: Bot):
    """Start the funding arbitrage scanner background loop."""
    global _running
    await _init_arb_tables()
    _running = True
    logger.info("Funding arb engine started (scan every %ds, alert threshold=%.4f%%/hr)",
                SCAN_INTERVAL, ALERT_THRESHOLD * 100)

    while _running:
        try:
            # Scan spreads
            spreads = await scan_funding_spreads()
            if spreads:
                logger.debug("Scanned %d symbols, top spread: %s %.6f",
                             len(spreads), spreads[0]["symbol"], spreads[0]["abs_spread"])

                # Post alert if any above threshold
                await post_funding_spread_alert(bot, spreads)

            # Monitor existing arb positions
            await _monitor_arb_positions(bot)

        except Exception as e:
            logger.error("Funding arb loop error: %s", e, exc_info=True)

        await asyncio.sleep(SCAN_INTERVAL)


def stop_funding_arb():
    """Stop the funding arbitrage scanner."""
    global _running
    _running = False
    logger.info("Funding arb engine stopped.")
