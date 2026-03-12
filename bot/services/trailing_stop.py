"""
Trailing-stop / Guard service — 2-phase tiered trailing stop inspired by Nunchi.

Phase 1 ("Let it Breathe"):
  - Wide retrace tolerance using the user's trail_percent
  - Auto-cut at 90 min if position hasn't graduated to Phase 2
  - Weak-peak early cut at 45 min if peak ROE < 3%

Phase 2 ("Lock the Bag"):
  - Tiered profit floors that ratchet up as ROE grows
  - Once a tier is reached the floor only goes UP, never down
  - Price below current floor → close position
"""

import asyncio
import logging
import time

from aiogram import Bot
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from bot.services.pacifica_client import PacificaClient
from bot.services.market_data import get_price, usd_to_token, get_lot_size
from bot.services.wallet_manager import decrypt_private_key
from database.db import get_db

logger = logging.getLogger(__name__)

_running = False
CHECK_INTERVAL = 5  # seconds — needs to be fast for trailing stops

# Phase 2 graduation threshold (ROE %)
PHASE2_ROE_THRESHOLD = 5.0

# Phase 1 time limits (minutes)
WEAK_PEAK_MINUTES = 45
WEAK_PEAK_ROE = 3.0
AUTO_CUT_MINUTES = 90

# Tiered profit floors: (roe_threshold, floor_profit_pct)
TIERS = [
    (5.0,   0.0),   # Tier 1: ROE >= 5%   -> floor at breakeven
    (10.0,  5.0),   # Tier 2: ROE >= 10%  -> floor at 5%
    (20.0,  12.0),  # Tier 3: ROE >= 20%  -> floor at 12%
    (35.0,  25.0),  # Tier 4: ROE >= 35%  -> floor at 25%
    (50.0,  40.0),  # Tier 5: ROE >= 50%  -> floor at 40%
    (100.0, 75.0),  # Tier 6: ROE >= 100% -> floor at 75%
]


# ---------------------------------------------------------------------------
# DB migration — add guard columns to trailing_stops
# ---------------------------------------------------------------------------

async def _migrate_guard_columns():
    """Add Phase/Tier columns if they don't already exist (safe to call repeatedly)."""
    db = await get_db()
    migrations = [
        "ALTER TABLE trailing_stops ADD COLUMN phase INTEGER DEFAULT 1",
        "ALTER TABLE trailing_stops ADD COLUMN current_tier INTEGER DEFAULT 0",
        "ALTER TABLE trailing_stops ADD COLUMN phase_start_time REAL",
        "ALTER TABLE trailing_stops ADD COLUMN peak_roe REAL DEFAULT 0",
    ]
    for sql in migrations:
        try:
            await db.execute(sql)
        except Exception:
            pass  # column already exists
    await db.commit()


# ---------------------------------------------------------------------------
# DB helper — update guard state
# ---------------------------------------------------------------------------

async def _update_guard_state(
    stop_id: int,
    phase: int,
    current_tier: int,
    peak_roe: float,
    phase_start_time: float,
):
    db = await get_db()
    await db.execute(
        """UPDATE trailing_stops
           SET phase = ?, current_tier = ?, peak_roe = ?, phase_start_time = ?
           WHERE id = ?""",
        (phase, current_tier, peak_roe, phase_start_time, stop_id),
    )
    await db.commit()


# ---------------------------------------------------------------------------
# Service lifecycle
# ---------------------------------------------------------------------------

async def start_trailing_stop_service(bot: Bot):
    global _running
    _running = True

    # Run migration on startup
    await _migrate_guard_columns()

    logger.info("Guard trailing-stop service started (check every %ss)", CHECK_INTERVAL)

    while _running:
        try:
            await _check_trailing_stops(bot)
        except Exception as e:
            logger.error("Trailing stop error: %s", e)
        await asyncio.sleep(CHECK_INTERVAL)


def stop_trailing_stop_service():
    global _running
    _running = False


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

async def _check_trailing_stops(bot: Bot):
    db = await get_db()
    async with db.execute(
        "SELECT * FROM trailing_stops WHERE active = 1"
    ) as cursor:
        stops = [dict(r) for r in await cursor.fetchall()]

    if not stops:
        return

    for stop in stops:
        try:
            await _process_trailing_stop(bot, stop)
        except Exception as e:
            logger.debug("Trailing stop check failed for %s: %s", stop.get("symbol"), e)


# ---------------------------------------------------------------------------
# ROE helper
# ---------------------------------------------------------------------------

def _calc_roe(side: str, current_price: float, entry_price: float) -> float:
    """Return ROE as a percentage (positive = profitable)."""
    if side.lower() in ("long", "buy"):
        return (current_price - entry_price) / entry_price * 100
    else:
        return (entry_price - current_price) / entry_price * 100


# ---------------------------------------------------------------------------
# Core processing
# ---------------------------------------------------------------------------

async def _process_trailing_stop(bot: Bot, stop: dict):
    symbol = stop["symbol"]
    tg_id = stop["telegram_id"]
    side = stop["side"]
    trail_pct = stop["trail_percent"]
    peak_price = stop["peak_price"]
    callback_price = stop["callback_price"]
    entry_price = stop["entry_price"]

    current_price = await get_price(symbol)
    if not current_price:
        return

    # --- Calculate ROE ---
    roe = _calc_roe(side, current_price, entry_price)

    # --- Read guard state (backward-compatible defaults) ---
    phase = stop.get("phase", 1) or 1
    current_tier = stop.get("current_tier", 0) or 0
    phase_start = stop.get("phase_start_time") or stop.get("created_at_epoch")
    if not phase_start:
        # Fallback: use current time (first run after migration)
        phase_start = time.time()
        await _update_guard_state(stop["id"], phase, current_tier, 0.0, phase_start)
    peak_roe = stop.get("peak_roe", 0) or 0.0

    # Track peak ROE
    if roe > peak_roe:
        peak_roe = roe

    db = await get_db()

    # ===================================================================
    # PHASE 1 — "Let it Breathe"
    # ===================================================================
    if phase == 1:
        elapsed_min = (time.time() - phase_start) / 60

        # --- Check Phase 1 -> Phase 2 graduation ---
        if roe >= PHASE2_ROE_THRESHOLD:
            phase = 2
            current_tier = 1  # Start at tier 1 (breakeven)
            logger.info(
                "Guard Phase 2: %s %s graduated (ROE %.1f%%) for user %s",
                symbol, side, roe, tg_id,
            )
            await _update_guard_state(stop["id"], phase, current_tier, peak_roe, phase_start)
            await bot.send_message(
                tg_id,
                f"\U0001f6e1 <b>Guard Phase 2 Active</b>\n\n"
                f"<b>{symbol}</b> \u2014 profit protection enabled\n"
                f"ROE: <code>{roe:.1f}%</code>\n"
                f"Floor: breakeven (entry price)",
            )
            return  # process phase 2 on next tick

        # --- Weak-peak early cut: 45 min and peak ROE < 3% ---
        if elapsed_min >= WEAK_PEAK_MINUTES and peak_roe < WEAK_PEAK_ROE:
            reason = f"weak peak ({peak_roe:.1f}% max after {elapsed_min:.0f}min)"
            logger.info("Guard weak-peak cut: %s %s — %s (user %s)", symbol, side, reason, tg_id)
            await _execute_trailing_close(bot, stop, current_price, reason=reason)
            return

        # --- Auto-cut at 90 minutes without graduation ---
        if elapsed_min >= AUTO_CUT_MINUTES:
            reason = f"no graduation after {elapsed_min:.0f}min (peak ROE: {peak_roe:.1f}%)"
            logger.info("Guard auto-cut: %s %s — %s (user %s)", symbol, side, reason, tg_id)
            await _execute_trailing_close(bot, stop, current_price, reason=reason)
            return

        # --- Standard trailing stop check (Phase 1 uses user's trail_percent) ---
        triggered = False

        if side.lower() in ("long", "buy"):
            if current_price > peak_price:
                new_peak = current_price
                new_callback = current_price * (1 - trail_pct / 100)
                await db.execute(
                    "UPDATE trailing_stops SET peak_price = ?, callback_price = ?, peak_roe = ? WHERE id = ?",
                    (new_peak, new_callback, peak_roe, stop["id"]),
                )
                await db.commit()
            elif current_price <= callback_price:
                triggered = True
        else:
            if current_price < peak_price:
                new_peak = current_price
                new_callback = current_price * (1 + trail_pct / 100)
                await db.execute(
                    "UPDATE trailing_stops SET peak_price = ?, callback_price = ?, peak_roe = ? WHERE id = ?",
                    (new_peak, new_callback, peak_roe, stop["id"]),
                )
                await db.commit()
            elif current_price >= callback_price:
                triggered = True

        if triggered:
            reason = f"trail {trail_pct}% hit in Phase 1"
            await _execute_trailing_close(bot, stop, current_price, reason=reason)
            return

        # Persist peak_roe even if peak_price didn't change
        if peak_roe != (stop.get("peak_roe", 0) or 0.0):
            await db.execute(
                "UPDATE trailing_stops SET peak_roe = ? WHERE id = ?",
                (peak_roe, stop["id"]),
            )
            await db.commit()

    # ===================================================================
    # PHASE 2 — "Lock the Bag"
    # ===================================================================
    elif phase == 2:
        # Determine best tier from current ROE
        new_tier = current_tier
        for i, (threshold, _floor) in enumerate(TIERS):
            if roe >= threshold:
                new_tier = i + 1

        # Tier only goes up
        new_tier = max(new_tier, current_tier)

        # Notify on tier upgrade
        if new_tier > current_tier:
            _, floor_pct = TIERS[new_tier - 1]
            current_tier = new_tier
            logger.info(
                "Guard Tier %d: %s %s (ROE %.1f%%, floor %.0f%%) for user %s",
                current_tier, symbol, side, roe, floor_pct, tg_id,
            )
            await bot.send_message(
                tg_id,
                f"\U0001f6e1 <b>Guard Tier {current_tier}</b>\n\n"
                f"<b>{symbol}</b> ROE: <code>{roe:.1f}%</code>\n"
                f"Profit floor: <code>{floor_pct:.0f}%</code>",
            )

        # Calculate floor price from current tier
        if current_tier > 0:
            _, floor_pct = TIERS[current_tier - 1]
            if side.lower() in ("long", "buy"):
                floor_price = entry_price * (1 + floor_pct / 100)
            else:
                floor_price = entry_price * (1 - floor_pct / 100)

            # Check if price dropped below floor
            if side.lower() in ("long", "buy") and current_price <= floor_price:
                reason = f"hit tier {current_tier} floor ({floor_pct:.0f}% profit)"
                logger.info("Guard floor hit: %s %s — %s (user %s)", symbol, side, reason, tg_id)
                await _execute_trailing_close(bot, stop, current_price, reason=reason)
                return
            elif side.lower() not in ("long", "buy") and current_price >= floor_price:
                reason = f"hit tier {current_tier} floor ({floor_pct:.0f}% profit)"
                logger.info("Guard floor hit: %s %s — %s (user %s)", symbol, side, reason, tg_id)
                await _execute_trailing_close(bot, stop, current_price, reason=reason)
                return

        # Persist updated state
        await _update_guard_state(stop["id"], phase, current_tier, peak_roe, phase_start)


# ---------------------------------------------------------------------------
# Close execution
# ---------------------------------------------------------------------------

async def _execute_trailing_close(
    bot: Bot,
    stop: dict,
    trigger_price: float,
    reason: str | None = None,
):
    tg_id = stop["telegram_id"]
    symbol = stop["symbol"]
    db = await get_db()

    # Mark as triggered
    await db.execute(
        "UPDATE trailing_stops SET active = 0, triggered_at = CURRENT_TIMESTAMP WHERE id = ?",
        (stop["id"],),
    )
    await db.commit()

    # Get user credentials to close position
    async with db.execute(
        "SELECT pacifica_account, agent_wallet_encrypted FROM users WHERE telegram_id = ?",
        (tg_id,),
    ) as cursor:
        user = await cursor.fetchone()

    if not user or not user[0] or not user[1]:
        return

    account = user[0]
    encrypted_key = user[1]

    try:
        kp = decrypt_private_key(encrypted_key)
        client = PacificaClient(account=account, keypair=kp)

        try:
            # Get current position to determine close side & size
            positions = await client.get_positions()
            pos = next((p for p in (positions or []) if p.get("symbol") == symbol), None)
            if not pos:
                return

            close_side = "sell" if stop["side"].lower() in ("long", "buy") else "buy"
            amount = str(pos.get("amount", "0"))

            result = await client.create_market_order(
                symbol=symbol,
                side=close_side,
                amount=amount,
                reduce_only=True,
                slippage="1",
            )

            entry = float(stop.get("entry_price", 0))
            pnl = (trigger_price - entry) * float(amount) if stop["side"].lower() in ("long", "buy") \
                else (entry - trigger_price) * float(amount)
            pnl_pct = ((trigger_price - entry) / entry * 100) if stop["side"].lower() in ("long", "buy") \
                else ((entry - trigger_price) / entry * 100)

            emoji = "\U0001f7e2" if pnl >= 0 else "\U0001f534"
            text = (
                f"<b>\U0001f6e1 Guard Triggered!</b>\n\n"
                f"{emoji} <b>{symbol}</b> {stop['side'].upper()} closed\n"
                f"Entry: <code>${entry:,.2f}</code>\n"
                f"Exit: <code>${trigger_price:,.2f}</code>\n"
                f"PnL: <code>${pnl:,.2f}</code> ({pnl_pct:+.2f}%)\n"
            )
            if reason:
                text += f"\nReason: {reason}"

            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="View Positions", callback_data="nav:positions")],
            ])

            await bot.send_message(tg_id, text, reply_markup=kb)
        finally:
            await client.close()
    except Exception as e:
        logger.error("Trailing stop close failed for %s %s: %s", tg_id, symbol, e)
