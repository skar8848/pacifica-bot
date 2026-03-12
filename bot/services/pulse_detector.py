"""
Pulse Detector — momentum detection inspired by Nunchi's Pulse system.

Polls Hyperliquid every 60 seconds to detect sudden capital inflows and
momentum shifts.  Classifies signals into 5 actionable tiers plus 3
informational tiers and posts alerts to the Telegram group.

Signal taxonomy (actionable):
  FIRST_JUMP        (100) — First asset in a sector with OI+vol breakout
  CONTRIB_EXPLOSION  (95) — Simultaneous extreme OI (+15%) AND volume (5x)
  IMMEDIATE_MOVER    (80) — Either extreme OI OR extreme volume
  NEW_ENTRY_DEEP     (65) — OI +8% but volume normal (stealth entry)
  DEEP_CLIMBER       (55) — Sustained OI growth 5%+ over 3+ scans

Informational (no entry):
  VOLUME_SURGE       (70) — 4h volume / average > 3x
  OI_BREAKOUT        (60) — OI jumps 8%+ above baseline
  FUNDING_FLIP       (50) — Funding reverses or accelerates 50%+
"""

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any
from urllib.request import Request, urlopen

from aiogram import Bot

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

HL_API = "https://api.hyperliquid.xyz/info"
CHECK_INTERVAL = 60  # seconds

# Detection thresholds
OI_EXTREME_PCT = 15          # % change for extreme OI breakout
OI_MODERATE_PCT = 8          # % change for moderate OI breakout
OI_SUSTAINED_PCT = 5         # % per window for DEEP_CLIMBER
VOLUME_EXTREME_RATIO = 5.0   # multiple of average volume
VOLUME_SURGE_RATIO = 3.0     # informational threshold
DEEP_CLIMBER_MIN_SCANS = 3   # consecutive scans with sustained OI growth

# Baseline window sizes
OI_BASELINE_WINDOW = 10      # last N samples for OI average
VOLUME_BASELINE_WINDOW = 30  # last N samples for volume average
FUNDING_BASELINE_WINDOW = 30
HISTORY_MAX = 30             # max samples to keep

# Cooldowns
PER_ASSET_COOLDOWN = 600       # 10 min between alerts for same asset
PER_TIER_COOLDOWN = 900        # 15 min for lower-tier signals
ERRATIC_THRESHOLD = 5          # signals in last hour to mark erratic
ERRATIC_WINDOW = 3600          # 1 hour

# Signal history
MAX_SIGNAL_HISTORY = 20

# ---------------------------------------------------------------------------
# Sector map
# ---------------------------------------------------------------------------

SECTORS: dict[str, list[str]] = {
    "L1": ["BTC", "ETH", "SOL", "AVAX", "SUI", "APT", "SEI", "TIA"],
    "DEFI": ["AAVE", "UNI", "LINK", "MKR", "SNX", "CRV", "COMP"],
    "MEME": ["DOGE", "SHIB", "PEPE", "WIF", "BONK", "FLOKI"],
    "AI": ["RENDER", "FET", "OCEAN", "AGIX"],
    "GAMING": ["IMX", "GALA", "AXS", "SAND"],
}

_asset_to_sector: dict[str, str] = {}
for _sec, _coins in SECTORS.items():
    for _c in _coins:
        _asset_to_sector[_c] = _sec


def _get_sector(asset: str) -> str | None:
    return _asset_to_sector.get(asset)


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

class SignalTier(str, Enum):
    FIRST_JUMP = "FIRST_JUMP"
    CONTRIB_EXPLOSION = "CONTRIB_EXPLOSION"
    IMMEDIATE_MOVER = "IMMEDIATE_MOVER"
    VOLUME_SURGE = "VOLUME_SURGE"
    NEW_ENTRY_DEEP = "NEW_ENTRY_DEEP"
    OI_BREAKOUT = "OI_BREAKOUT"
    DEEP_CLIMBER = "DEEP_CLIMBER"
    FUNDING_FLIP = "FUNDING_FLIP"


TIER_CONFIDENCE: dict[SignalTier, int] = {
    SignalTier.FIRST_JUMP: 100,
    SignalTier.CONTRIB_EXPLOSION: 95,
    SignalTier.IMMEDIATE_MOVER: 80,
    SignalTier.VOLUME_SURGE: 70,
    SignalTier.NEW_ENTRY_DEEP: 65,
    SignalTier.OI_BREAKOUT: 60,
    SignalTier.DEEP_CLIMBER: 55,
    SignalTier.FUNDING_FLIP: 50,
}

TIER_DESCRIPTION: dict[SignalTier, str] = {
    SignalTier.FIRST_JUMP: "First mover in {sector} sector \u2014 smart money front-running",
    SignalTier.CONTRIB_EXPLOSION: "All guns blazing \u2014 simultaneous OI + volume explosion",
    SignalTier.IMMEDIATE_MOVER: "Something is happening \u2014 extreme {metric} detected",
    SignalTier.VOLUME_SURGE: "Volume surge \u2014 4h volume 3x+ above average",
    SignalTier.NEW_ENTRY_DEEP: "Stealth entry \u2014 OI growing quietly, volume normal",
    SignalTier.OI_BREAKOUT: "OI breakout \u2014 open interest jumped 8%+ above baseline",
    SignalTier.DEEP_CLIMBER: "Slow build-up \u2014 sustained OI growth over {scans} scans",
    SignalTier.FUNDING_FLIP: "Funding flip \u2014 rate reversed or accelerated sharply",
}

# Tiers that are actionable (post with full detail)
ACTIONABLE_TIERS = {
    SignalTier.FIRST_JUMP,
    SignalTier.CONTRIB_EXPLOSION,
    SignalTier.IMMEDIATE_MOVER,
    SignalTier.NEW_ENTRY_DEEP,
    SignalTier.DEEP_CLIMBER,
}

TIER_EMOJI: dict[SignalTier, str] = {
    SignalTier.FIRST_JUMP: "\U0001f534",         # red circle
    SignalTier.CONTRIB_EXPLOSION: "\U0001f7e0",   # orange circle
    SignalTier.IMMEDIATE_MOVER: "\U0001f7e1",     # yellow circle
    SignalTier.VOLUME_SURGE: "\U0001f4ca",        # bar chart
    SignalTier.NEW_ENTRY_DEEP: "\U0001f7e3",      # purple circle
    SignalTier.OI_BREAKOUT: "\U0001f535",          # blue circle
    SignalTier.DEEP_CLIMBER: "\U0001f7e2",        # green circle
    SignalTier.FUNDING_FLIP: "\U0001f504",        # counterclockwise arrows
}


@dataclass
class AssetBaseline:
    oi_history: list[float] = field(default_factory=list)
    volume_history: list[float] = field(default_factory=list)
    funding_history: list[float] = field(default_factory=list)
    price_history: list[float] = field(default_factory=list)
    last_update: float = 0.0
    consecutive_oi_growth: int = 0


@dataclass
class PulseSignal:
    tier: SignalTier
    asset: str
    confidence: int
    direction: str          # LONG / SHORT / NEUTRAL
    oi_current: float
    oi_baseline: float
    oi_change_pct: float
    volume_current: float
    volume_baseline: float
    volume_ratio: float
    funding_rate: float
    price: float
    price_change_pct: float
    sector: str | None
    description: str
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> dict[str, Any]:
        return {
            "tier": self.tier.value,
            "asset": self.asset,
            "confidence": self.confidence,
            "direction": self.direction,
            "oi_current": self.oi_current,
            "oi_baseline": self.oi_baseline,
            "oi_change_pct": round(self.oi_change_pct, 2),
            "volume_current": self.volume_current,
            "volume_baseline": self.volume_baseline,
            "volume_ratio": round(self.volume_ratio, 2),
            "funding_rate": self.funding_rate,
            "price": self.price,
            "price_change_pct": round(self.price_change_pct, 2),
            "sector": self.sector,
            "description": self.description,
            "timestamp": self.timestamp,
        }


# ---------------------------------------------------------------------------
# Module state
# ---------------------------------------------------------------------------

_running = False
_baselines: dict[str, AssetBaseline] = {}
_signal_history: list[PulseSignal] = []
_alert_timestamps: dict[str, list[float]] = {}   # asset -> list of alert times
_last_alert_time: dict[str, float] = {}           # asset -> last alert epoch
_sector_first_jump: dict[str, str] = {}           # sector -> asset that fired first (per cycle)


# ---------------------------------------------------------------------------
# Hyperliquid API (sync, run in executor)
# ---------------------------------------------------------------------------

def _hl_post(payload: dict) -> Any:
    """Synchronous HL API call."""
    req = Request(
        HL_API,
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urlopen(req, timeout=15) as resp:
        return json.loads(resp.read())


async def _hl_fetch(payload: dict) -> Any:
    """Async wrapper around sync HL post."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, _hl_post, payload)


async def _fetch_meta_and_ctxs() -> tuple[dict, list[dict]]:
    """Fetch meta + asset contexts from Hyperliquid."""
    data = await _hl_fetch({"type": "metaAndAssetCtxs"})
    meta = data[0]   # {universe: [{name, szDecimals, ...}, ...]}
    ctxs = data[1]   # [{funding, openInterest, prevDayPx, dayNtlVlm, ...}, ...]
    return meta, ctxs


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def _fmt_usd(val: float) -> str:
    abs_val = abs(val)
    sign = "\u2212" if val < 0 else ""
    if abs_val >= 1_000_000_000:
        return f"{sign}${abs_val / 1_000_000_000:.2f}B"
    if abs_val >= 1_000_000:
        return f"{sign}${abs_val / 1_000_000:.2f}M"
    if abs_val >= 1_000:
        return f"{sign}${abs_val / 1_000:.1f}K"
    return f"{sign}${abs_val:,.0f}"


def _fmt_pct(val: float) -> str:
    return f"{val:+.1f}%"


# ---------------------------------------------------------------------------
# Direction inference
# ---------------------------------------------------------------------------

def _infer_direction(funding_rate: float, price_change_pct: float) -> str:
    if funding_rate > 0 and price_change_pct > 0:
        return "LONG"
    if funding_rate < 0 and price_change_pct < 0:
        return "SHORT"
    return "NEUTRAL"


# ---------------------------------------------------------------------------
# Cooldown & erratic checks
# ---------------------------------------------------------------------------

def _is_erratic(asset: str, now: float) -> bool:
    """Return True if asset has fired too many signals recently."""
    times = _alert_timestamps.get(asset, [])
    recent = [t for t in times if now - t < ERRATIC_WINDOW]
    _alert_timestamps[asset] = recent  # prune old entries
    return len(recent) >= ERRATIC_THRESHOLD


def _check_cooldown(asset: str, tier: SignalTier, now: float) -> bool:
    """Return True if the alert should be suppressed by cooldown."""
    # FIRST_JUMP always fires
    if tier == SignalTier.FIRST_JUMP:
        return False

    last = _last_alert_time.get(asset, 0)

    # Per-asset cooldown
    if now - last < PER_ASSET_COOLDOWN:
        return True

    # Lower tiers get additional cooldown
    if tier not in ACTIONABLE_TIERS and now - last < PER_TIER_COOLDOWN:
        return True

    return False


def _record_alert(asset: str, now: float):
    """Record that we sent an alert for this asset."""
    _last_alert_time[asset] = now
    _alert_timestamps.setdefault(asset, []).append(now)


# ---------------------------------------------------------------------------
# Baseline management
# ---------------------------------------------------------------------------

def _update_baseline(
    asset: str, oi: float, volume: float, funding: float, price: float
) -> AssetBaseline:
    """Update rolling baseline for an asset and return it."""
    bl = _baselines.get(asset)
    if bl is None:
        bl = AssetBaseline()
        _baselines[asset] = bl

    bl.oi_history.append(oi)
    bl.volume_history.append(volume)
    bl.funding_history.append(funding)
    bl.price_history.append(price)

    # Trim to max
    if len(bl.oi_history) > HISTORY_MAX:
        bl.oi_history = bl.oi_history[-HISTORY_MAX:]
    if len(bl.volume_history) > HISTORY_MAX:
        bl.volume_history = bl.volume_history[-HISTORY_MAX:]
    if len(bl.funding_history) > HISTORY_MAX:
        bl.funding_history = bl.funding_history[-HISTORY_MAX:]
    if len(bl.price_history) > HISTORY_MAX:
        bl.price_history = bl.price_history[-HISTORY_MAX:]

    bl.last_update = time.time()
    return bl


def _get_baseline_oi(bl: AssetBaseline) -> float:
    window = bl.oi_history[-OI_BASELINE_WINDOW - 1:-1]  # exclude current
    return sum(window) / len(window) if window else 0.0


def _get_baseline_volume(bl: AssetBaseline) -> float:
    window = bl.volume_history[-VOLUME_BASELINE_WINDOW - 1:-1]
    return sum(window) / len(window) if window else 0.0


def _get_baseline_funding(bl: AssetBaseline) -> float:
    window = bl.funding_history[-FUNDING_BASELINE_WINDOW - 1:-1]
    return sum(window) / len(window) if window else 0.0


# ---------------------------------------------------------------------------
# Detection logic
# ---------------------------------------------------------------------------

def _detect_signals(
    asset: str,
    oi: float,
    volume: float,
    funding: float,
    price: float,
    bl: AssetBaseline,
) -> list[PulseSignal]:
    """Run detection logic and return any triggered signals (highest tier first)."""
    signals: list[PulseSignal] = []

    baseline_oi = _get_baseline_oi(bl)
    baseline_vol = _get_baseline_volume(bl)
    baseline_funding = _get_baseline_funding(bl)

    # Need at least a few samples before triggering
    if len(bl.oi_history) < 3:
        return signals

    # Compute metrics
    oi_change_pct = ((oi - baseline_oi) / baseline_oi * 100) if baseline_oi > 0 else 0.0
    volume_ratio = (volume / baseline_vol) if baseline_vol > 0 else 0.0

    # Price change vs previous scan
    prev_price = bl.price_history[-2] if len(bl.price_history) >= 2 else price
    price_change_pct = ((price - prev_price) / prev_price * 100) if prev_price > 0 else 0.0

    direction = _infer_direction(funding, price_change_pct)
    sector = _get_sector(asset)

    # Track consecutive OI growth for DEEP_CLIMBER
    if oi_change_pct >= OI_SUSTAINED_PCT:
        bl.consecutive_oi_growth += 1
    else:
        bl.consecutive_oi_growth = 0

    # Funding flip detection
    funding_flipped = False
    if len(bl.funding_history) >= 3:
        prev_funding = bl.funding_history[-2]
        if prev_funding != 0:
            # Direction reversal
            if (prev_funding > 0 and funding < 0) or (prev_funding < 0 and funding > 0):
                funding_flipped = True
            # Acceleration 50%+
            funding_change = abs(funding - prev_funding) / abs(prev_funding) * 100
            if funding_change >= 50:
                funding_flipped = True

    def _make_signal(tier: SignalTier, desc: str) -> PulseSignal:
        return PulseSignal(
            tier=tier,
            asset=asset,
            confidence=TIER_CONFIDENCE[tier],
            direction=direction,
            oi_current=oi,
            oi_baseline=baseline_oi,
            oi_change_pct=oi_change_pct,
            volume_current=volume,
            volume_baseline=baseline_vol,
            volume_ratio=volume_ratio,
            funding_rate=funding,
            price=price,
            price_change_pct=price_change_pct,
            sector=sector,
            description=desc,
        )

    # --- Actionable tiers (highest first) ---

    extreme_oi = oi_change_pct >= OI_EXTREME_PCT
    extreme_vol = volume_ratio >= VOLUME_EXTREME_RATIO

    if extreme_oi and extreme_vol:
        # Check if first in sector
        if sector and sector not in _sector_first_jump:
            _sector_first_jump[sector] = asset
            desc = TIER_DESCRIPTION[SignalTier.FIRST_JUMP].format(sector=sector)
            signals.append(_make_signal(SignalTier.FIRST_JUMP, desc))
        else:
            desc = TIER_DESCRIPTION[SignalTier.CONTRIB_EXPLOSION]
            signals.append(_make_signal(SignalTier.CONTRIB_EXPLOSION, desc))

    elif extreme_oi or extreme_vol:
        metric = "OI" if extreme_oi else "volume"
        desc = TIER_DESCRIPTION[SignalTier.IMMEDIATE_MOVER].format(metric=metric)
        signals.append(_make_signal(SignalTier.IMMEDIATE_MOVER, desc))

    elif oi_change_pct >= OI_MODERATE_PCT and volume_ratio < 2.0:
        desc = TIER_DESCRIPTION[SignalTier.NEW_ENTRY_DEEP]
        signals.append(_make_signal(SignalTier.NEW_ENTRY_DEEP, desc))

    elif bl.consecutive_oi_growth >= DEEP_CLIMBER_MIN_SCANS and oi_change_pct >= OI_SUSTAINED_PCT:
        desc = TIER_DESCRIPTION[SignalTier.DEEP_CLIMBER].format(
            scans=bl.consecutive_oi_growth
        )
        signals.append(_make_signal(SignalTier.DEEP_CLIMBER, desc))

    # --- Informational tiers (independent, can stack) ---

    if volume_ratio >= VOLUME_SURGE_RATIO and not extreme_vol:
        desc = TIER_DESCRIPTION[SignalTier.VOLUME_SURGE]
        signals.append(_make_signal(SignalTier.VOLUME_SURGE, desc))

    if oi_change_pct >= OI_MODERATE_PCT and not extreme_oi and volume_ratio >= 2.0:
        # OI breakout that didn't qualify for higher tiers
        desc = TIER_DESCRIPTION[SignalTier.OI_BREAKOUT]
        signals.append(_make_signal(SignalTier.OI_BREAKOUT, desc))

    if funding_flipped:
        desc = TIER_DESCRIPTION[SignalTier.FUNDING_FLIP]
        signals.append(_make_signal(SignalTier.FUNDING_FLIP, desc))

    return signals


# ---------------------------------------------------------------------------
# Alert formatting
# ---------------------------------------------------------------------------

def _format_alert(sig: PulseSignal) -> str:
    """Format a PulseSignal into a Telegram HTML message."""
    emoji = TIER_EMOJI.get(sig.tier, "\u26a1")
    sector_str = f" ({sig.sector} sector)" if sig.sector else ""

    lines = [
        f"{emoji} <b>{sig.tier.value}</b> \u2014 {sig.asset}{sector_str}",
        f"Confidence: {sig.confidence} | Direction: {sig.direction}",
        "",
        f"OI: {_fmt_usd(sig.oi_baseline)} \u2192 {_fmt_usd(sig.oi_current)} ({_fmt_pct(sig.oi_change_pct)})",
        f"Volume: {_fmt_usd(sig.volume_current)} ({sig.volume_ratio:.1f}x avg)",
        f"Funding: {sig.funding_rate:+.4f}%/hr",
        f"Price: ${sig.price:,.2f} ({_fmt_pct(sig.price_change_pct)})",
        "",
        sig.description,
    ]
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Core scan
# ---------------------------------------------------------------------------

async def _scan_pulse(bot: Bot):
    """Run one scan cycle across all Hyperliquid assets."""
    now = time.time()

    try:
        meta, ctxs = await _fetch_meta_and_ctxs()
    except Exception as e:
        logger.error("Failed to fetch HL meta/ctxs: %s", e)
        return

    universe = meta.get("universe", [])
    if len(universe) != len(ctxs):
        logger.warning(
            "Universe/ctxs length mismatch: %d vs %d", len(universe), len(ctxs)
        )
        return

    # Reset per-cycle sector tracker
    _sector_first_jump.clear()

    cycle_signals: list[PulseSignal] = []

    for asset_info, ctx in zip(universe, ctxs):
        asset = asset_info.get("name", "")
        if not asset:
            continue

        try:
            oi = float(ctx.get("openInterest", "0"))
            funding = float(ctx.get("funding", "0"))
            volume = float(ctx.get("dayNtlVlm", "0"))
            price = float(ctx.get("markPx", "0"))
        except (ValueError, TypeError):
            continue

        # Skip assets with negligible OI
        if oi < 10_000:
            continue

        bl = _update_baseline(asset, oi, volume, funding, price)
        signals = _detect_signals(asset, oi, volume, funding, price, bl)

        for sig in signals:
            # Erratic filter
            if _is_erratic(asset, now):
                logger.debug("Suppressed erratic signal: %s %s", sig.tier.value, asset)
                continue

            # Cooldown filter
            if _check_cooldown(asset, sig.tier, now):
                logger.debug("Suppressed cooldown signal: %s %s", sig.tier.value, asset)
                continue

            cycle_signals.append(sig)

    # Post alerts and record
    for sig in cycle_signals:
        logger.info(
            "Pulse signal: %s %s (confidence=%d, dir=%s, OI=%s, vol_ratio=%.1fx)",
            sig.tier.value,
            sig.asset,
            sig.confidence,
            sig.direction,
            _fmt_pct(sig.oi_change_pct),
            sig.volume_ratio,
        )

        text = _format_alert(sig)
        try:
            from bot.services.group_feed import post_to_group
            await post_to_group(bot, text, parse_mode="HTML")
        except Exception as e:
            logger.error("Failed to post pulse alert for %s: %s", sig.asset, e)

        _record_alert(sig.asset, now)

        # Store in history
        _signal_history.append(sig)
        if len(_signal_history) > MAX_SIGNAL_HISTORY:
            _signal_history[:] = _signal_history[-MAX_SIGNAL_HISTORY:]

    if cycle_signals:
        logger.info("Pulse scan complete: %d signal(s) detected", len(cycle_signals))


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_active_signals() -> list[dict]:
    """Return the last N pulse signals as dicts (for /pulse command)."""
    return [s.to_dict() for s in _signal_history]


# ---------------------------------------------------------------------------
# Start / Stop
# ---------------------------------------------------------------------------

async def start_pulse_detector(bot: Bot):
    """Start the pulse detector background loop."""
    global _running
    _running = True
    logger.info("Pulse detector started (check every %ds)", CHECK_INTERVAL)

    while _running:
        try:
            await _scan_pulse(bot)
        except Exception as e:
            logger.error("Pulse scan error: %s", e)
        await asyncio.sleep(CHECK_INTERVAL)


def stop_pulse_detector():
    """Stop the pulse detector background loop."""
    global _running
    _running = False
    logger.info("Pulse detector stopped.")
