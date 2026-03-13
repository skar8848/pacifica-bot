"""
Volatility regime classifier with asymmetric hysteresis.

Computes annualized realized volatility from BTC 1-minute log returns and
classifies the market into four regimes:

  CALM    — sigma_ann < 0.15   (risk_multiplier = 1.0)
  NORMAL  — 0.15 <= sigma < 0.40  (risk_multiplier = 1.5)
  HIGH    — 0.40 <= sigma < 0.80  (risk_multiplier = 2.5)
  EXTREME — sigma >= 0.80         (risk_multiplier = 5.0)

Hysteresis rules:
  - Upward transitions (CALM → NORMAL → HIGH → EXTREME) are immediate.
  - Downward transitions require DOWNWARD_CONFIRM_ROUNDS consecutive rounds
    in the lower bin before the regime is lowered.

Portfolio drawdown is tracked from the all-time equity peak observed since
service start, and amplified using DRAWDOWN_BANDS.

Public API (safe to call without the service loop running):
  get_regime()         -> dict
  get_regime_history() -> list[dict]
"""

import asyncio
import json
import logging
import math
import time
from collections import deque
from typing import Any
from urllib.request import Request, urlopen

from aiogram import Bot

from bot.services.market_data import get_price

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CHECK_INTERVAL = 60        # seconds between price samples
ROLLING_WINDOW = 30        # number of prices used for vol calculation
DOWNWARD_CONFIRM_ROUNDS = 3  # consecutive lower-bin readings before demotion

HL_API = "https://api.hyperliquid.xyz/info"

# Regime definitions: name -> (upper_bound_exclusive, risk_multiplier)
# Upper bound of EXTREME is +inf — handled specially below.
REGIME_BOUNDS: list[tuple[str, float, float]] = [
    ("CALM",    0.15, 1.0),
    ("NORMAL",  0.40, 1.5),
    ("HIGH",    0.80, 2.5),
    ("EXTREME", math.inf, 5.0),
]

# Drawdown amplifier bands: (drawdown_pct_upper, amplifier)
# If drawdown >= 2.5%, amplifier = inf (halt signal)
DRAWDOWN_BANDS: list[tuple[float, float]] = [
    (0.5,   1.0),           # < 0.5% DD
    (1.5,   1.5),           # 0.5–1.5% DD
    (2.5,   2.0),           # 1.5–2.5% DD
    (999.0, math.inf),      # > 2.5% DD — halt
]

# ---------------------------------------------------------------------------
# Module-level state
# ---------------------------------------------------------------------------

_running = False

# Rolling price buffer for vol calculation
_price_buffer: deque[float] = deque(maxlen=ROLLING_WINDOW)

# Current regime index into REGIME_BOUNDS
_regime_idx: int = 0

# Pending downward-confirmation counter
_downward_pending_idx: int | None = None
_downward_pending_rounds: int = 0

# Portfolio equity tracking (populated when account info is available)
_equity_peak: float = 0.0
_equity_current: float = 0.0

# History deque for dashboard (last 60 readings)
_regime_history: deque[dict] = deque(maxlen=60)

# Last computed sigma
_last_sigma: float = 0.0


# ---------------------------------------------------------------------------
# Hyperliquid helpers (sync fetch, run in executor)
# ---------------------------------------------------------------------------

def _hl_post_sync(payload: dict) -> Any:
    req = Request(
        HL_API,
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urlopen(req, timeout=15) as resp:
        return json.loads(resp.read())


async def _hl_fetch(payload: dict) -> Any:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, _hl_post_sync, payload)


async def _fetch_btc_price_hl() -> float | None:
    """Fetch BTC mark price from Hyperliquid metaAndAssetCtxs."""
    try:
        data = await _hl_fetch({"type": "metaAndAssetCtxs"})
        universe: list[dict] = data[0].get("universe", [])
        ctxs: list[dict] = data[1]
        for asset_info, ctx in zip(universe, ctxs):
            if asset_info.get("name", "").upper() == "BTC":
                px = ctx.get("markPx")
                if px is not None:
                    return float(px)
    except Exception as exc:
        logger.debug("HL BTC price fetch failed: %s", exc)
    return None


async def _get_btc_price() -> float | None:
    """Try Pacifica first, fall back to Hyperliquid."""
    price = await get_price("BTC")
    if price:
        return price
    return await _fetch_btc_price_hl()


# ---------------------------------------------------------------------------
# Volatility calculation
# ---------------------------------------------------------------------------

def _compute_sigma(prices: list[float]) -> float:
    """
    Compute annualized realized volatility from a list of prices.

    Uses log returns between consecutive prices and annualises with
    sqrt(525_600) — the number of 1-minute periods in a year.

    Returns 0.0 if fewer than 2 prices are provided.
    """
    if len(prices) < 2:
        return 0.0
    log_returns = [
        math.log(prices[i] / prices[i - 1])
        for i in range(1, len(prices))
        if prices[i - 1] > 0 and prices[i] > 0
    ]
    if not log_returns:
        return 0.0
    n = len(log_returns)
    mean = sum(log_returns) / n
    variance = sum((r - mean) ** 2 for r in log_returns) / n
    return math.sqrt(variance) * math.sqrt(525_600)


# ---------------------------------------------------------------------------
# Regime helpers
# ---------------------------------------------------------------------------

def _sigma_to_idx(sigma: float) -> int:
    """Return the regime index that best describes the given sigma."""
    for i, (_, upper, _) in enumerate(REGIME_BOUNDS):
        if sigma < upper:
            return i
    return len(REGIME_BOUNDS) - 1


def _drawdown_amplifier(drawdown_pct: float) -> float:
    """Return the amplifier for the given drawdown percentage."""
    for upper, amp in DRAWDOWN_BANDS:
        if drawdown_pct < upper:
            return amp
    return math.inf  # > 999% — shouldn't happen in practice


# ---------------------------------------------------------------------------
# Core classification tick
# ---------------------------------------------------------------------------

def _classify(sigma: float) -> None:
    """Apply hysteresis logic and update _regime_idx."""
    global _regime_idx, _downward_pending_idx, _downward_pending_rounds

    target_idx = _sigma_to_idx(sigma)

    if target_idx > _regime_idx:
        # Immediate upward transition
        _regime_idx = target_idx
        _downward_pending_idx = None
        _downward_pending_rounds = 0
        logger.info(
            "Regime UP -> %s (sigma=%.4f)", REGIME_BOUNDS[_regime_idx][0], sigma
        )
    elif target_idx < _regime_idx:
        # Downward: requires DOWNWARD_CONFIRM_ROUNDS consecutive rounds
        if _downward_pending_idx == target_idx:
            _downward_pending_rounds += 1
        else:
            _downward_pending_idx = target_idx
            _downward_pending_rounds = 1

        if _downward_pending_rounds >= DOWNWARD_CONFIRM_ROUNDS:
            old_name = REGIME_BOUNDS[_regime_idx][0]
            _regime_idx = target_idx
            _downward_pending_idx = None
            _downward_pending_rounds = 0
            logger.info(
                "Regime DOWN %s -> %s (sigma=%.4f, confirmed %d rounds)",
                old_name, REGIME_BOUNDS[_regime_idx][0], sigma, DOWNWARD_CONFIRM_ROUNDS,
            )
        else:
            logger.debug(
                "Regime downward pending: %s (%d/%d) sigma=%.4f",
                REGIME_BOUNDS[target_idx][0],
                _downward_pending_rounds,
                DOWNWARD_CONFIRM_ROUNDS,
                sigma,
            )
    else:
        # No change — reset pending counter if target drifted back
        if _downward_pending_idx is not None and target_idx != _downward_pending_idx:
            _downward_pending_idx = None
            _downward_pending_rounds = 0


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_regime() -> dict:
    """
    Return the current regime snapshot.

    Safe to call without the service loop running — returns defaults if no
    data has been collected yet.

    Returns:
        {
            "regime":       str,   # CALM | NORMAL | HIGH | EXTREME
            "multiplier":   float, # risk_multiplier for current regime
            "sigma":        float, # latest annualized realized vol
            "drawdown_pct": float, # portfolio drawdown from peak (%)
            "drawdown_amp": float, # drawdown amplifier (inf = halt)
            "timestamp":    float, # epoch seconds of last update
        }
    """
    name, _upper, multiplier = REGIME_BOUNDS[_regime_idx]

    drawdown_pct = 0.0
    if _equity_peak > 0 and _equity_current < _equity_peak:
        drawdown_pct = (_equity_peak - _equity_current) / _equity_peak * 100.0

    drawdown_amp = _drawdown_amplifier(drawdown_pct)

    last_reading = _regime_history[-1] if _regime_history else None
    ts = last_reading["timestamp"] if last_reading else 0.0

    return {
        "regime": name,
        "multiplier": multiplier,
        "sigma": _last_sigma,
        "drawdown_pct": round(drawdown_pct, 4),
        "drawdown_amp": drawdown_amp,
        "timestamp": ts,
    }


def get_regime_history() -> list[dict]:
    """
    Return up to the last 60 regime readings for the dashboard chart.

    Each entry matches the structure returned by get_regime() plus a
    "regime_idx" field for convenience.
    """
    return list(_regime_history)


# ---------------------------------------------------------------------------
# Service loop
# ---------------------------------------------------------------------------

async def _tick() -> None:
    """Fetch BTC price, compute vol, classify, update history."""
    global _last_sigma, _equity_peak, _equity_current

    price = await _get_btc_price()
    if price is None:
        logger.debug("Regime classifier: BTC price unavailable this tick")
        return

    _price_buffer.append(price)

    # Use BTC price as a proxy for portfolio equity when no account info
    if _equity_peak == 0.0:
        _equity_peak = price
    _equity_current = price
    if price > _equity_peak:
        _equity_peak = price

    if len(_price_buffer) < 2:
        return  # Not enough data yet

    sigma = _compute_sigma(list(_price_buffer))
    _last_sigma = sigma

    _classify(sigma)

    name, _upper, multiplier = REGIME_BOUNDS[_regime_idx]
    drawdown_pct = 0.0
    if _equity_peak > 0 and _equity_current < _equity_peak:
        drawdown_pct = (_equity_peak - _equity_current) / _equity_peak * 100.0

    snapshot = {
        "regime": name,
        "regime_idx": _regime_idx,
        "multiplier": multiplier,
        "sigma": round(sigma, 6),
        "drawdown_pct": round(drawdown_pct, 4),
        "drawdown_amp": _drawdown_amplifier(drawdown_pct),
        "timestamp": time.time(),
    }
    _regime_history.append(snapshot)
    logger.debug(
        "Regime: %s | sigma=%.4f | dd=%.2f%%", name, sigma, drawdown_pct
    )


async def start_regime_classifier(bot: Bot) -> None:
    """Start the regime classifier background loop (passive — no alerts sent)."""
    global _running
    _running = True
    logger.info(
        "Regime classifier started (window=%d prices, interval=%ds)",
        ROLLING_WINDOW,
        CHECK_INTERVAL,
    )

    while _running:
        try:
            await _tick()
        except Exception as exc:
            logger.error("Regime classifier error: %s", exc)
        await asyncio.sleep(CHECK_INTERVAL)


def stop_regime_classifier() -> None:
    """Stop the regime classifier background loop."""
    global _running
    _running = False
    logger.info("Regime classifier stopped.")
