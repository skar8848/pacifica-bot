"""
Cross-exchange price & funding gap monitor.

Compares Pacifica and Hyperliquid prices/funding in real-time
and posts alerts to the Telegram group when significant gaps appear.
"""

import asyncio
import json
import logging
import time
from urllib.request import Request, urlopen

from aiogram import Bot

from bot.services.group_feed import post_to_group

logger = logging.getLogger(__name__)

_running = False

CHECK_INTERVAL = 60          # price gap check every 60s
FUNDING_POST_INTERVAL = 1800 # funding comparison every 30min
PRICE_GAP_THRESHOLD = 0.005  # 0.5%
FUNDING_GAP_THRESHOLD = 0.00005  # 0.005% per hour
COOLDOWN_SECONDS = 1800      # 30 min cooldown per symbol

# {symbol: last_alert_timestamp}
_price_cooldowns: dict[str, float] = {}
_last_funding_post: float = 0.0

# Gap tracking: {symbol: [{"gap_pct": float, "timestamp": float}, ...]}
_gap_history: dict[str, list[dict]] = {}
MAX_HISTORY_SAMPLES = 360  # ~6 hours at 60s intervals

# Symbol mapping: Pacifica uses "BTC-PERP", HL uses "BTC"
_PAC_SUFFIX = "-PERP"


# ------------------------------------------------------------------
# Hyperliquid API helpers
# ------------------------------------------------------------------

def _hl_post(payload: dict) -> dict | list:
    """Synchronous POST to Hyperliquid info endpoint."""
    req = Request(
        "https://api.hyperliquid.xyz/info",
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urlopen(req, timeout=10) as resp:
        return json.loads(resp.read())


def _get_hl_prices() -> dict[str, float]:
    """Fetch mid prices for all assets on Hyperliquid.

    Returns: {symbol: price} e.g. {"BTC": 67750.0, "ETH": 2450.0}
    """
    data = _hl_post({"type": "allMids"})
    result: dict[str, float] = {}
    for coin, price_str in data.items():
        try:
            result[coin] = float(price_str)
        except (ValueError, TypeError):
            continue
    return result


def _get_hl_funding() -> dict[str, float]:
    """Fetch current hourly funding rates from Hyperliquid.

    Returns: {symbol: hourly_rate} e.g. {"BTC": 0.0001, "ETH": -0.0002}
    """
    data = _hl_post({"type": "metaAndAssetCtxs"})
    meta = data[0]
    asset_ctxs = data[1]

    universe = meta.get("universe", [])
    result: dict[str, float] = {}
    for i, asset_info in enumerate(universe):
        name = asset_info.get("name", "")
        if i < len(asset_ctxs):
            try:
                funding = float(asset_ctxs[i].get("funding", "0"))
                result[name] = funding
            except (ValueError, TypeError, AttributeError):
                continue
    return result


# ------------------------------------------------------------------
# Pacifica data helper
# ------------------------------------------------------------------

async def _get_pacifica_data() -> dict[str, dict]:
    """Fetch prices and funding rates from Pacifica.

    Returns: {symbol: {"price": float, "funding": float}}
    where symbol is the base asset (e.g. "BTC", not "BTC-PERP")
    """
    from bot.services.funding_monitor import get_all_funding_rates

    rates = await get_all_funding_rates()
    result: dict[str, dict] = {}
    for r in rates:
        raw_symbol = r["symbol"]
        # Normalise: strip "-PERP" suffix so we can match with HL
        symbol = raw_symbol.replace(_PAC_SUFFIX, "")
        result[symbol] = {
            "price": r["mark_price"],
            "funding": r["funding_rate"],
        }
    return result


# ------------------------------------------------------------------
# Cooldown helper
# ------------------------------------------------------------------

def _is_cooled_down(symbol: str) -> bool:
    """Return True if enough time has passed since the last alert for this symbol."""
    last = _price_cooldowns.get(symbol, 0.0)
    return (time.time() - last) >= COOLDOWN_SECONDS


def _mark_alerted(symbol: str):
    _price_cooldowns[symbol] = time.time()


# ------------------------------------------------------------------
# Main check logic
# ------------------------------------------------------------------

async def _check_gaps(bot: Bot):
    """Compare prices and funding between HL and Pacifica, post alerts."""
    global _last_funding_post

    # Fetch data from both exchanges concurrently
    loop = asyncio.get_running_loop()

    hl_prices_future = loop.run_in_executor(None, _get_hl_prices)
    hl_funding_future = loop.run_in_executor(None, _get_hl_funding)
    pac_data = await _get_pacifica_data()

    hl_prices = await hl_prices_future
    hl_funding = await hl_funding_future

    if not hl_prices or not pac_data:
        logger.debug("Skipping gap check — missing data (HL=%d, Pac=%d)",
                      len(hl_prices), len(pac_data))
        return

    # Find overlapping symbols
    common = set(hl_prices.keys()) & set(pac_data.keys())
    if not common:
        logger.debug("No overlapping symbols between HL and Pacifica")
        return

    # --- Price gap alerts ---
    gap_lines: list[str] = []
    for symbol in sorted(common):
        hl_price = hl_prices[symbol]
        pac_price = pac_data[symbol]["price"]

        if hl_price <= 0 or pac_price <= 0:
            continue

        gap_pct = (hl_price - pac_price) / pac_price  # positive = HL more expensive

        # Record gap sample for statistics
        if symbol not in _gap_history:
            _gap_history[symbol] = []
        _gap_history[symbol].append({"gap_pct": gap_pct, "timestamp": time.time()})
        # Trim to max samples
        if len(_gap_history[symbol]) > MAX_HISTORY_SAMPLES:
            _gap_history[symbol] = _gap_history[symbol][-MAX_HISTORY_SAMPLES:]

        if abs(gap_pct) < PRICE_GAP_THRESHOLD:
            continue

        if not _is_cooled_down(symbol):
            continue

        if gap_pct > 0:
            direction = "buy Pacifica, sell HL"
        else:
            direction = "buy HL, sell Pacifica"

        # Trend detection based on history
        trend_tag = ""
        history = _gap_history.get(symbol, [])
        if len(history) >= 10:
            recent_avg = sum(abs(s["gap_pct"]) for s in history[-10:]) / 10
            current_abs = abs(gap_pct)
            if recent_avg > 0:
                if current_abs > 2 * recent_avg:
                    trend_tag = " \u26a0\ufe0f WIDENING"
                elif current_abs < 0.5 * recent_avg:
                    trend_tag = " \u2705 NARROWING"

        gap_lines.append(
            f"<b>{symbol}</b>: ${hl_price:,.2f} (HL) vs ${pac_price:,.2f} (Pacifica)\n"
            f"Gap: {gap_pct:+.2%} \u2014 {direction}{trend_tag}"
        )
        _mark_alerted(symbol)

    if gap_lines:
        text = "\U0001f4ca <b>Price Gap Alert</b>\n\n" + "\n\n".join(gap_lines)
        await post_to_group(bot, text)
        logger.info("Posted price gap alert for %d symbol(s)", len(gap_lines))

    # --- Funding rate comparison (periodic) ---
    now = time.time()
    if now - _last_funding_post < FUNDING_POST_INTERVAL:
        return

    funding_lines: list[str] = []
    for symbol in sorted(common):
        hl_rate = hl_funding.get(symbol)
        pac_rate = pac_data[symbol].get("funding")

        if hl_rate is None or pac_rate is None:
            continue

        spread = abs(hl_rate - pac_rate)
        if spread < FUNDING_GAP_THRESHOLD:
            continue

        funding_lines.append(
            f"<b>{symbol}</b>: {hl_rate * 100:+.4f}% (HL) vs "
            f"{pac_rate * 100:+.4f}% (Pac) \u2192 Spread: {spread * 100:.4f}%"
        )

    # Build top-gap summary for the periodic post
    top_gap_text = ""
    gap_stats = get_gap_stats()
    if gap_stats:
        # Only include symbols with samples from the last 30 minutes
        cutoff = now - FUNDING_POST_INTERVAL
        recent_stats = [
            s for s in gap_stats
            if any(
                h["timestamp"] >= cutoff
                for h in _gap_history.get(s["symbol"], [])
            )
        ]
        top3 = sorted(recent_stats, key=lambda s: s["avg_gap"], reverse=True)[:3]
        if top3:
            parts = [f"{s['symbol']} avg {s['avg_gap']:.2%}" for s in top3]
            top_gap_text = "\n\n\U0001f50d <b>Top gaps (last 30m):</b> " + ", ".join(parts)

    if funding_lines:
        text = (
            "\U0001f4b0 <b>Funding Rate Comparison (HL vs Pacifica)</b>\n\n"
            + "\n".join(funding_lines)
            + top_gap_text
        )
        await post_to_group(bot, text)
        logger.info("Posted funding comparison for %d symbol(s)", len(funding_lines))
    elif top_gap_text:
        # Post gap stats even if no funding lines exceed threshold
        await post_to_group(bot, top_gap_text.strip())
        logger.info("Posted top gap stats (no funding spread)")

    _last_funding_post = now


# ------------------------------------------------------------------
# Public gap statistics
# ------------------------------------------------------------------

def get_gap_stats(symbol: str | None = None) -> list[dict]:
    """Return gap statistics for tracked symbols.

    Returns list of dicts sorted by avg_gap descending, each containing:
        symbol, avg_gap, max_gap, min_gap, current_gap, samples,
        time_span_s, direction
    """
    targets = {}
    if symbol is not None:
        history = _gap_history.get(symbol)
        if history:
            targets[symbol] = history
    else:
        targets = _gap_history

    stats: list[dict] = []
    for sym, samples in targets.items():
        if not samples:
            continue

        abs_gaps = [abs(s["gap_pct"]) for s in samples]
        avg_gap = sum(abs_gaps) / len(abs_gaps)
        max_gap = max(abs_gaps)
        min_gap = min(abs_gaps)
        current = samples[-1]["gap_pct"]

        # Time span covered by history
        time_span_s = samples[-1]["timestamp"] - samples[0]["timestamp"]

        # Direction label from latest sample
        if current > 0:
            direction = "HL > Pacifica"
        elif current < 0:
            direction = "Pacifica > HL"
        else:
            direction = "equal"

        stats.append({
            "symbol": sym,
            "avg_gap": avg_gap,
            "max_gap": max_gap,
            "min_gap": min_gap,
            "current_gap": current,
            "samples": len(samples),
            "time_span_s": time_span_s,
            "direction": direction,
        })

    stats.sort(key=lambda s: s["avg_gap"], reverse=True)
    return stats


# ------------------------------------------------------------------
# Start / Stop
# ------------------------------------------------------------------

async def start_gap_monitor(bot: Bot):
    """Start the cross-exchange gap monitor loop."""
    global _running
    _running = True
    logger.info(
        "Gap monitor started (price check every %ds, funding post every %ds)",
        CHECK_INTERVAL,
        FUNDING_POST_INTERVAL,
    )

    while _running:
        try:
            await _check_gaps(bot)
        except Exception as e:
            logger.error("Gap monitor error: %s", e, exc_info=True)
        await asyncio.sleep(CHECK_INTERVAL)


def stop_gap_monitor():
    """Stop the gap monitor loop."""
    global _running
    _running = False
    logger.info("Gap monitor stopped.")
