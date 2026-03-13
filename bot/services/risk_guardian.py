"""
Risk Guardian — graduated portfolio-level risk control.

State machine
-------------
  OPEN     Normal trading.  Entries and exits are both permitted.
  COOLDOWN Defensive mode.  Exits allowed; new entries are blocked.
           Triggered by:
             - 2 consecutive losses, OR
             - daily PnL loss >= 50% of the daily loss limit.
           Auto-expires after COOLDOWN_DURATION seconds.
  CLOSED   All trading halted.
           Triggered by:
             - Any loss recorded while in COOLDOWN, OR
             - Daily loss limit breach.
           Resets automatically at midnight UTC.

Circuit breaker
---------------
If any tracked asset drops more than CIRCUIT_BREAKER_DROP_PCT percent within
the last CIRCUIT_BREAKER_WINDOW seconds, the gate transitions to CLOSED
immediately regardless of current state.

Public API (safe to call without the service loop running):
  get_gate_state()            -> dict
  can_enter()                 -> bool
  can_exit()                  -> bool
  record_trade_result(pnl)    -> None
  get_guardian_history()      -> list[dict]
"""

import asyncio
import logging
import time
from collections import deque
from typing import Optional

from aiogram import Bot

from bot.services.market_data import get_price

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

COOLDOWN_DURATION = 1800          # 30 minutes in seconds
DEFAULT_DAILY_LOSS_LIMIT = 500.0  # USD

CIRCUIT_BREAKER_DROP_PCT = 50.0   # % drop within window triggers CLOSED
CIRCUIT_BREAKER_WINDOW = 60       # seconds

CHECK_INTERVAL = 30               # service loop interval in seconds

# Symbols to watch for circuit-breaker extreme moves
CIRCUIT_BREAKER_SYMBOLS = ["BTC", "ETH", "SOL"]

# Max state transition records stored for dashboard
MAX_HISTORY = 100

# ---------------------------------------------------------------------------
# States
# ---------------------------------------------------------------------------

STATE_OPEN = "OPEN"
STATE_COOLDOWN = "COOLDOWN"
STATE_CLOSED = "CLOSED"

# ---------------------------------------------------------------------------
# Module-level state
# ---------------------------------------------------------------------------

_running = False

_state: str = STATE_OPEN
_state_reason: str = "Initial state"
_state_since: float = time.time()

_consecutive_losses: int = 0
_daily_pnl: float = 0.0
_daily_loss_limit: float = DEFAULT_DAILY_LOSS_LIMIT
_cooldown_expires: Optional[float] = None

# Last UTC date (YYYY-MM-DD) for midnight reset
_last_reset_date: str = ""

# State transition history for dashboard (most-recent last)
_guardian_history: list[dict] = []

# Circuit-breaker price history: {symbol: deque[(timestamp, price)]}
_cb_prices: dict[str, deque] = {s: deque(maxlen=300) for s in CIRCUIT_BREAKER_SYMBOLS}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _utc_date() -> str:
    import datetime
    return datetime.datetime.utcnow().strftime("%Y-%m-%d")


def _record_transition(new_state: str, reason: str) -> None:
    global _state, _state_reason, _state_since
    old_state = _state
    _state = new_state
    _state_reason = reason
    _state_since = time.time()
    entry = {
        "from": old_state,
        "to": new_state,
        "reason": reason,
        "timestamp": _state_since,
    }
    _guardian_history.append(entry)
    if len(_guardian_history) > MAX_HISTORY:
        _guardian_history[:] = _guardian_history[-MAX_HISTORY:]
    logger.info("Risk guardian: %s -> %s | %s", old_state, new_state, reason)


def _maybe_reset_daily() -> None:
    """Reset daily PnL and re-open gate at midnight UTC."""
    global _daily_pnl, _consecutive_losses, _last_reset_date
    today = _utc_date()
    if today != _last_reset_date:
        _last_reset_date = today
        old_pnl = _daily_pnl
        _daily_pnl = 0.0
        _consecutive_losses = 0
        if _state == STATE_CLOSED:
            _record_transition(STATE_OPEN, f"Midnight UTC reset (daily PnL was ${old_pnl:.2f})")
        logger.info("Risk guardian: daily reset for %s", today)


def _check_cooldown_expiry() -> None:
    """Promote COOLDOWN -> OPEN if the timer has expired."""
    global _cooldown_expires
    if _state == STATE_COOLDOWN and _cooldown_expires and time.time() >= _cooldown_expires:
        _cooldown_expires = None
        _record_transition(STATE_OPEN, "Cooldown period expired")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_gate_state() -> dict:
    """
    Return a snapshot of the current gate state.

    Safe to call without the service loop running.

    Returns:
        {
            "state":               str,            # OPEN | COOLDOWN | CLOSED
            "reason":              str,
            "state_since":         float,           # epoch seconds
            "consecutive_losses":  int,
            "daily_pnl":           float,           # USD, negative = loss
            "daily_limit":         float,           # USD loss limit
            "cooldown_expires":    float | None,    # epoch seconds
            "timestamp":           float,
        }
    """
    _maybe_reset_daily()
    _check_cooldown_expiry()
    return {
        "state": _state,
        "reason": _state_reason,
        "state_since": _state_since,
        "consecutive_losses": _consecutive_losses,
        "daily_pnl": round(_daily_pnl, 4),
        "daily_limit": _daily_loss_limit,
        "cooldown_expires": _cooldown_expires,
        "timestamp": time.time(),
    }


def can_enter() -> bool:
    """Return True only when the gate is OPEN."""
    _maybe_reset_daily()
    _check_cooldown_expiry()
    return _state == STATE_OPEN


def can_exit() -> bool:
    """Return True when the gate is OPEN or COOLDOWN."""
    _maybe_reset_daily()
    _check_cooldown_expiry()
    return _state in (STATE_OPEN, STATE_COOLDOWN)


def record_trade_result(pnl: float) -> None:
    """
    Update internal counters after a trade closes.

    Call this from any order-execution service once a position is fully
    closed and the realised PnL is known.

    Args:
        pnl: Realised PnL in USD (positive = profit, negative = loss).
    """
    global _consecutive_losses, _daily_pnl, _cooldown_expires

    _maybe_reset_daily()
    _daily_pnl += pnl

    if pnl < 0:
        _consecutive_losses += 1
    else:
        _consecutive_losses = 0

    loss_so_far = -_daily_pnl  # positive = money lost today
    half_limit = _daily_loss_limit * 0.5

    if _state == STATE_OPEN:
        if _consecutive_losses >= 2 or loss_so_far >= half_limit:
            reason = (
                f"{_consecutive_losses} consecutive losses"
                if _consecutive_losses >= 2
                else f"daily loss ${loss_so_far:.2f} >= 50% of limit"
            )
            _cooldown_expires = time.time() + COOLDOWN_DURATION
            _record_transition(STATE_COOLDOWN, reason)

    elif _state == STATE_COOLDOWN:
        if pnl < 0:
            _record_transition(
                STATE_CLOSED,
                f"Loss ${abs(pnl):.2f} during cooldown period",
            )
        # Also close if daily limit breached during cooldown
        elif loss_so_far >= _daily_loss_limit:
            _record_transition(
                STATE_CLOSED,
                f"Daily loss limit ${_daily_loss_limit:.2f} reached",
            )

    # Hard close regardless of state if daily limit breached
    if _state != STATE_CLOSED and loss_so_far >= _daily_loss_limit:
        _record_transition(
            STATE_CLOSED,
            f"Daily loss limit ${_daily_loss_limit:.2f} breached",
        )

    logger.info(
        "Risk guardian: trade pnl=$%.2f | consecutive_losses=%d | daily_pnl=$%.2f | state=%s",
        pnl,
        _consecutive_losses,
        _daily_pnl,
        _state,
    )


def get_guardian_history() -> list[dict]:
    """
    Return the list of state transitions for the dashboard.

    Each entry: {from, to, reason, timestamp}.
    """
    return list(_guardian_history)


# ---------------------------------------------------------------------------
# Circuit breaker logic
# ---------------------------------------------------------------------------

async def _update_cb_prices() -> None:
    """Fetch current prices for circuit-breaker symbols and store with timestamp."""
    for symbol in CIRCUIT_BREAKER_SYMBOLS:
        try:
            price = await get_price(symbol)
            if price:
                _cb_prices[symbol].append((time.time(), price))
        except Exception as exc:
            logger.debug("CB price fetch failed for %s: %s", symbol, exc)


def _check_circuit_breaker() -> Optional[str]:
    """
    Scan recent price history for extreme moves.

    Returns a description string if a circuit breaker fires, else None.
    """
    now = time.time()
    for symbol, history in _cb_prices.items():
        if not history:
            continue
        # Collect prices within the window
        window = [(ts, px) for ts, px in history if now - ts <= CIRCUIT_BREAKER_WINDOW]
        if len(window) < 2:
            continue
        oldest_px = window[0][1]
        newest_px = window[-1][1]
        if oldest_px <= 0:
            continue
        change_pct = abs((newest_px - oldest_px) / oldest_px) * 100.0
        if change_pct >= CIRCUIT_BREAKER_DROP_PCT:
            return (
                f"Circuit breaker: {symbol} moved {change_pct:.1f}% in "
                f"{CIRCUIT_BREAKER_WINDOW}s (${oldest_px:,.2f} -> ${newest_px:,.2f})"
            )
    return None


# ---------------------------------------------------------------------------
# Service loop
# ---------------------------------------------------------------------------

async def _tick(bot: Bot) -> None:
    """One service loop iteration."""
    _maybe_reset_daily()
    _check_cooldown_expiry()

    await _update_cb_prices()

    cb_reason = _check_circuit_breaker()
    if cb_reason and _state != STATE_CLOSED:
        _record_transition(STATE_CLOSED, cb_reason)
        try:
            from bot.config import ADMIN_IDS
            for admin_id in ADMIN_IDS:
                await bot.send_message(
                    admin_id,
                    f"<b>RISK GUARDIAN — CIRCUIT BREAKER FIRED</b>\n\n"
                    f"{cb_reason}\n\n"
                    f"All trading halted until midnight UTC reset.",
                )
        except Exception as exc:
            logger.error("Risk guardian: failed to send CB alert: %s", exc)


async def start_risk_guardian(bot: Bot) -> None:
    """Start the Risk Guardian background loop."""
    global _running, _last_reset_date
    _running = True
    _last_reset_date = _utc_date()
    logger.info(
        "Risk guardian started (check every %ds, daily limit=$%.2f)",
        CHECK_INTERVAL,
        _daily_loss_limit,
    )

    while _running:
        try:
            await _tick(bot)
        except Exception as exc:
            logger.error("Risk guardian error: %s", exc)
        await asyncio.sleep(CHECK_INTERVAL)


def stop_risk_guardian() -> None:
    """Stop the Risk Guardian background loop."""
    global _running
    _running = False
    logger.info("Risk guardian stopped.")
