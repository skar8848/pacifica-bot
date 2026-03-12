"""
Radar Scanner — Opportunity scanner inspired by Nunchi's Radar system.

Scans all Hyperliquid perpetual assets every 15 minutes, scores them
on market structure, technicals, funding, and BTC macro context, then
posts the top opportunities to the Telegram group.

Scoring breakdown (0-400 points):
  - Market Structure: 0-140  (35%)
  - Technicals:       0-120  (30%)
  - Funding:          0-80   (20%)
  - BTC Macro:        0-60   (15%)
"""

import asyncio
import json
import logging
import statistics
from urllib.request import Request, urlopen

from aiogram import Bot

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

HL_API_URL = "https://api.hyperliquid.xyz/info"

SCAN_INTERVAL = 900  # 15 minutes
MIN_OI_USD = 1_000_000  # Filter out assets with OI < $1M
MIN_VOLUME_USD = 100_000  # Filter out assets with 24h volume < $100K
SCORE_THRESHOLD = 200  # Only post if top opportunity >= this score
TOP_N = 5  # Show top N opportunities in each alert
MAX_HISTORY = 20  # ~5 hours of price/OI history at 15-min intervals

_running = False

# ---------------------------------------------------------------------------
# In-memory state
# ---------------------------------------------------------------------------

_price_history: dict[str, list[float]] = {}  # {symbol: [price1, price2, ...]}
_oi_history: dict[str, list[float]] = {}  # {symbol: [oi1, oi2, ...]}
_latest_results: list[dict] = []  # Updated each scan (public API)

# ---------------------------------------------------------------------------
# Hyperliquid API helpers
# ---------------------------------------------------------------------------


def _hl_post(payload: dict) -> dict | list:
    """Synchronous HL API call (run in executor)."""
    req = Request(
        HL_API_URL,
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urlopen(req, timeout=15) as resp:
        return json.loads(resp.read())


async def _fetch_radar_data() -> tuple[dict, dict]:
    """Fetch all needed data from HL in parallel."""
    loop = asyncio.get_running_loop()

    meta_task = loop.run_in_executor(None, _hl_post, {"type": "metaAndAssetCtxs"})
    mids_task = loop.run_in_executor(None, _hl_post, {"type": "allMids"})

    meta_data = await meta_task
    mids = await mids_task

    return meta_data, mids


# ---------------------------------------------------------------------------
# RSI calculation
# ---------------------------------------------------------------------------


def _simple_rsi(prices: list[float], period: int = 14) -> float:
    """Calculate RSI from a list of prices."""
    if len(prices) < period + 1:
        return 50.0  # neutral if not enough data

    gains = []
    losses = []
    for i in range(1, len(prices)):
        diff = prices[i] - prices[i - 1]
        if diff > 0:
            gains.append(diff)
            losses.append(0)
        else:
            gains.append(0)
            losses.append(abs(diff))

    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period

    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


# ---------------------------------------------------------------------------
# Scoring functions
# ---------------------------------------------------------------------------


def _score_volume(volume_24h: float, median_volume: float) -> int:
    """Volume score: 0-50 points."""
    if median_volume <= 0:
        return 0
    volume_ratio = volume_24h / median_volume
    return min(50, int(volume_ratio * 15))


def _score_oi(current_oi: float, baseline_oi: float) -> int:
    """OI score: 0-50 points."""
    if baseline_oi <= 0:
        return 0
    oi_ratio = current_oi / baseline_oi
    return min(50, max(0, int((oi_ratio - 1) * 200)))


def _score_liquidity(mark_price: float) -> int:
    """Liquidity score based on price as proxy: 0-40 points."""
    if mark_price > 1000:
        return 40
    if mark_price > 100:
        return 30
    if mark_price > 10:
        return 20
    return 10


def _score_rsi(rsi: float) -> tuple[int, str]:
    """RSI score: 0-40 points. Returns (score, bias)."""
    if rsi < 30:
        return 40, "LONG"
    if rsi > 70:
        return 40, "SHORT"
    if rsi < 40:
        return 20, "LONG"
    if rsi > 60:
        return 20, "SHORT"
    return 0, "NEUTRAL"


def _score_trend(price: float, avg_20: float) -> tuple[int, float]:
    """Trend score: 0-40 points. Returns (score, trend_pct)."""
    if avg_20 <= 0:
        return 5, 0.0
    trend_pct = (price - avg_20) / avg_20 * 100
    if abs(trend_pct) > 5:
        return 40, trend_pct
    if abs(trend_pct) > 2:
        return 25, trend_pct
    return 5, trend_pct


def _score_momentum(change_1h_pct: float) -> int:
    """Momentum score: 0-40 points."""
    if abs(change_1h_pct) > 3:
        return 40
    if abs(change_1h_pct) > 1:
        return 25
    return 5


def _score_funding_extremity(funding_rate: float) -> int:
    """Funding extremity score: 0-40 points. funding_rate is hourly %."""
    if abs(funding_rate) > 0.01:
        return 40
    if abs(funding_rate) > 0.005:
        return 25
    return 5


def _score_funding_direction(funding_rate: float, change_1h_pct: float) -> int:
    """Funding direction vs price score: 0-40 points."""
    # High positive funding + price rising = potential short squeeze setup
    if funding_rate > 0.005 and change_1h_pct > 0:
        return 40
    # High negative funding + price falling = potential long squeeze setup
    if funding_rate < -0.005 and change_1h_pct < 0:
        return 40
    return 10


def _score_btc_alignment(asset_change: float, btc_change: float) -> int:
    """BTC trend alignment: 0-30 points."""
    if abs(btc_change) < 0.1:
        return 20  # BTC neutral
    same_direction = (asset_change > 0 and btc_change > 0) or (
        asset_change < 0 and btc_change < 0
    )
    return 30 if same_direction else 10


def _score_btc_strength(btc_change: float) -> int:
    """BTC trend strength: 0-30 points."""
    if abs(btc_change) > 1:
        return 30
    if abs(btc_change) > 0.3:
        return 20
    return 10


# ---------------------------------------------------------------------------
# Direction inference
# ---------------------------------------------------------------------------


def _infer_direction(
    rsi: float, trend_pct: float, funding_rate: float
) -> str:
    """Accumulate direction votes and return LONG / SHORT / NEUTRAL."""
    long_votes = 0
    short_votes = 0

    if rsi < 35:
        long_votes += 2  # Oversold = long
    if rsi > 65:
        short_votes += 2  # Overbought = short
    if trend_pct > 2:
        long_votes += 1  # Uptrend
    if trend_pct < -2:
        short_votes += 1  # Downtrend
    if funding_rate > 0.005:
        short_votes += 1  # Crowded longs = short opportunity
    if funding_rate < -0.005:
        long_votes += 1  # Crowded shorts = long opportunity

    if long_votes > short_votes:
        return "LONG"
    if short_votes > long_votes:
        return "SHORT"
    return "NEUTRAL"


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------


def _fmt_usd(val: float) -> str:
    """Format USD value with appropriate suffix."""
    abs_val = abs(val)
    sign = "\u2212" if val < 0 else ""
    if abs_val >= 1_000_000_000:
        return f"{sign}${abs_val / 1_000_000_000:.2f}B"
    if abs_val >= 1_000_000:
        return f"{sign}${abs_val / 1_000_000:.2f}M"
    if abs_val >= 1_000:
        return f"{sign}${abs_val / 1_000:.1f}K"
    return f"{sign}${abs_val:,.0f}"


def _fmt_price(price: float) -> str:
    """Format price with sensible decimal places."""
    if price >= 1000:
        return f"${price:,.0f}"
    if price >= 1:
        return f"${price:,.2f}"
    if price >= 0.01:
        return f"${price:,.4f}"
    return f"${price:,.6f}"


def _rsi_label(rsi: float) -> str:
    """Human-readable RSI label."""
    if rsi < 30:
        return "oversold"
    if rsi > 70:
        return "overbought"
    if rsi < 40:
        return "low"
    if rsi > 60:
        return "high"
    return "neutral"


# ---------------------------------------------------------------------------
# Core scan logic
# ---------------------------------------------------------------------------


def _score_asset(
    symbol: str,
    mark_price: float,
    volume_24h: float,
    current_oi: float,
    funding_rate: float,
    median_volume: float,
    btc_change_1h: float,
) -> dict | None:
    """Score a single asset across all criteria. Returns dict or None if skipped."""

    # Update price history
    if symbol not in _price_history:
        _price_history[symbol] = []
    _price_history[symbol].append(mark_price)
    if len(_price_history[symbol]) > MAX_HISTORY:
        _price_history[symbol] = _price_history[symbol][-MAX_HISTORY:]

    # Update OI history
    if symbol not in _oi_history:
        _oi_history[symbol] = []
    _oi_history[symbol].append(current_oi)
    if len(_oi_history[symbol]) > MAX_HISTORY:
        _oi_history[symbol] = _oi_history[symbol][-MAX_HISTORY:]

    prices = _price_history[symbol]
    oi_hist = _oi_history[symbol]

    # --- Technicals ---
    rsi = _simple_rsi(prices)

    avg_20 = statistics.mean(prices) if len(prices) >= 2 else mark_price
    trend_score, trend_pct = _score_trend(mark_price, avg_20)

    # 1h change: use price from ~4 scans ago (4 * 15 min = 60 min)
    if len(prices) >= 5:
        price_1h_ago = prices[-5]
    elif len(prices) >= 2:
        price_1h_ago = prices[0]
    else:
        price_1h_ago = mark_price
    change_1h_pct = (
        ((mark_price - price_1h_ago) / price_1h_ago * 100)
        if price_1h_ago > 0
        else 0.0
    )

    # OI baseline: from ~4 scans ago (1h)
    if len(oi_hist) >= 5:
        baseline_oi = oi_hist[-5]
    elif len(oi_hist) >= 2:
        baseline_oi = oi_hist[0]
    else:
        baseline_oi = current_oi

    oi_change_pct = (
        ((current_oi - baseline_oi) / baseline_oi * 100) if baseline_oi > 0 else 0.0
    )

    # --- Market Structure (0-140) ---
    vol_score = _score_volume(volume_24h, median_volume)
    oi_score = _score_oi(current_oi, baseline_oi)
    liq_score = _score_liquidity(mark_price)
    market_structure = vol_score + oi_score + liq_score

    # --- Technicals (0-120) ---
    rsi_score, rsi_bias = _score_rsi(rsi)
    mom_score = _score_momentum(change_1h_pct)
    technicals = rsi_score + trend_score + mom_score

    # --- Funding (0-80) ---
    fund_ext = _score_funding_extremity(funding_rate)
    fund_dir = _score_funding_direction(funding_rate, change_1h_pct)
    funding_total = fund_ext + fund_dir

    # --- BTC Macro (0-60) ---
    btc_align = _score_btc_alignment(change_1h_pct, btc_change_1h)
    btc_str = _score_btc_strength(btc_change_1h)
    btc_macro = btc_align + btc_str

    total_score = market_structure + technicals + funding_total + btc_macro

    # --- Direction ---
    direction = _infer_direction(rsi, trend_pct, funding_rate)

    # Volume ratio for display
    volume_ratio = volume_24h / median_volume if median_volume > 0 else 0.0

    return {
        "symbol": symbol,
        "score": total_score,
        "direction": direction,
        "price": mark_price,
        "oi": current_oi,
        "oi_change_pct": oi_change_pct,
        "funding_rate": funding_rate,
        "rsi": rsi,
        "rsi_label": _rsi_label(rsi),
        "change_1h_pct": change_1h_pct,
        "volume_24h": volume_24h,
        "volume_ratio": volume_ratio,
        "trend_pct": trend_pct,
        "btc_aligned": abs(btc_change_1h) >= 0.1
        and (
            (change_1h_pct > 0 and btc_change_1h > 0)
            or (change_1h_pct < 0 and btc_change_1h < 0)
        ),
        # Sub-scores for debugging / future use
        "breakdown": {
            "market_structure": market_structure,
            "technicals": technicals,
            "funding": funding_total,
            "btc_macro": btc_macro,
        },
    }


async def _run_radar_scan(bot: Bot):
    """Execute one full radar scan and post results."""
    global _latest_results

    try:
        meta_data, mids = await _fetch_radar_data()
    except Exception as e:
        logger.error("Radar: failed to fetch HL data: %s", e)
        return

    # Parse meta + asset contexts
    # meta_data is [meta, [assetCtx, ...]]
    if not isinstance(meta_data, list) or len(meta_data) < 2:
        logger.error("Radar: unexpected metaAndAssetCtxs format")
        return

    meta = meta_data[0]  # {universe: [{name, szDecimals, ...}, ...]}
    asset_ctxs = meta_data[1]  # [{funding, openInterest, prevDayPx, dayNtlVlm, ...}, ...]
    universe = meta.get("universe", [])

    if len(universe) != len(asset_ctxs):
        logger.warning(
            "Radar: universe/ctx length mismatch (%d vs %d)",
            len(universe),
            len(asset_ctxs),
        )

    # Build map: symbol -> context
    assets: list[dict] = []
    for i, info in enumerate(universe):
        if i >= len(asset_ctxs):
            break
        ctx = asset_ctxs[i]
        symbol = info.get("name", "")
        if not symbol:
            continue

        try:
            mark_px = float(ctx.get("markPx", "0"))
            open_interest = float(ctx.get("openInterest", "0"))
            funding = float(ctx.get("funding", "0"))
            day_volume = float(ctx.get("dayNtlVlm", "0"))
            # OI in USD
            oi_usd = open_interest * mark_px
        except (ValueError, TypeError):
            continue

        # Filter noise
        if oi_usd < MIN_OI_USD or day_volume < MIN_VOLUME_USD:
            continue

        assets.append(
            {
                "symbol": symbol,
                "mark_px": mark_px,
                "oi_usd": oi_usd,
                "funding": funding,
                "day_volume": day_volume,
            }
        )

    if not assets:
        logger.warning("Radar: no qualifying assets after filtering")
        return

    # Compute median volume for relative scoring
    volumes = [a["day_volume"] for a in assets]
    median_volume = statistics.median(volumes) if volumes else 1.0

    # BTC change (for macro context) — use our accumulated history
    btc_prices = _price_history.get("BTC", [])
    if len(btc_prices) >= 5:
        btc_price_1h = btc_prices[-5]
    elif len(btc_prices) >= 2:
        btc_price_1h = btc_prices[0]
    else:
        # Get current BTC price from mids
        btc_mid = float(mids.get("BTC", "0")) if isinstance(mids, dict) else 0.0
        btc_price_1h = btc_mid  # no history yet, change = 0

    btc_current = float(mids.get("BTC", "0")) if isinstance(mids, dict) else 0.0
    btc_change_1h = (
        ((btc_current - btc_price_1h) / btc_price_1h * 100)
        if btc_price_1h > 0
        else 0.0
    )

    # Score all assets
    scored: list[dict] = []
    for a in assets:
        try:
            result = _score_asset(
                symbol=a["symbol"],
                mark_price=a["mark_px"],
                volume_24h=a["day_volume"],
                current_oi=a["oi_usd"],
                funding_rate=a["funding"],
                median_volume=median_volume,
                btc_change_1h=btc_change_1h,
            )
            if result:
                scored.append(result)
        except Exception as e:
            logger.debug("Radar: failed to score %s: %s", a["symbol"], e)

    # Sort by score descending
    scored.sort(key=lambda x: x["score"], reverse=True)

    # Store for public API
    _latest_results = scored[:TOP_N]

    top = scored[:TOP_N]
    top_score = top[0]["score"] if top else 0

    logger.info(
        "Radar: scanned %d assets, %d above threshold, top: %s (%d)",
        len(scored),
        sum(1 for s in scored if s["score"] >= SCORE_THRESHOLD),
        top[0]["symbol"] if top else "none",
        top_score,
    )

    # Only post if at least one opportunity meets the threshold
    if top_score < SCORE_THRESHOLD:
        logger.info("Radar: no opportunities above threshold (%d)", SCORE_THRESHOLD)
        return

    # Filter to only those above threshold for the alert
    worthy = [s for s in top if s["score"] >= SCORE_THRESHOLD]
    if not worthy:
        return

    await _post_radar_alert(bot, worthy, len(scored))


async def _post_radar_alert(bot: Bot, opportunities: list[dict], total_scanned: int):
    """Format and post the radar scan results to group."""
    lines = ["\U0001f4e1 <b>Radar Scan \u2014 Top Opportunities</b>\n"]

    for i, opp in enumerate(opportunities, 1):
        direction = opp["direction"]
        if direction == "LONG":
            emoji = "\U0001f7e2"
        elif direction == "SHORT":
            emoji = "\U0001f534"
        else:
            emoji = "\u26aa"

        btc_icon = "\u2713" if opp["btc_aligned"] else "\u25cb"
        btc_label = f"BTC aligned {btc_icon}"

        # RSI display
        rsi_val = opp["rsi"]
        rsi_display = f"RSI: {rsi_val:.0f}"
        if opp["rsi_label"] != "neutral":
            rsi_display += f" ({opp['rsi_label']})"

        # OI change display
        oi_change = opp["oi_change_pct"]
        oi_str = f"{_fmt_usd(opp['oi'])}"
        if abs(oi_change) >= 0.1:
            oi_str += f" ({oi_change:+.1f}%)"

        # Volume display
        vol_ratio = opp["volume_ratio"]
        vol_str = f"{vol_ratio:.1f}x avg" if vol_ratio >= 0.1 else "low"

        # Funding display
        funding_pct = opp["funding_rate"] * 100  # already hourly rate
        funding_str = f"{funding_pct:+.4f}%/hr"

        line = (
            f"{i}. {emoji} <b>{opp['symbol']}</b> \u2014 {direction} "
            f"(Score: {opp['score']}/400)\n"
            f"   Price: {_fmt_price(opp['price'])} | "
            f"OI: {oi_str}\n"
            f"   Funding: {funding_str} | "
            f"{rsi_display}\n"
            f"   Volume: {vol_str} | "
            f"{btc_label}"
        )
        lines.append(line)

    lines.append(f"\nScan: {total_scanned} assets | Next: 15 min")

    text = "\n\n".join(lines)

    from bot.services.group_feed import post_to_group

    await post_to_group(bot, text)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def get_latest_scan() -> list[dict]:
    """Return the most recent radar scan results (for /radar command)."""
    return list(_latest_results)


# ---------------------------------------------------------------------------
# Start / Stop
# ---------------------------------------------------------------------------


async def start_radar_scanner(bot: Bot):
    """Start the radar scanner loop."""
    global _running
    _running = True
    logger.info("Radar scanner started (scan every %ds)", SCAN_INTERVAL)

    # Wait 30s before first scan (let other services initialize)
    await asyncio.sleep(30)

    while _running:
        try:
            await _run_radar_scan(bot)
        except Exception as e:
            logger.error("Radar scan error: %s", e)
        await asyncio.sleep(SCAN_INTERVAL)


def stop_radar_scanner():
    """Stop the radar scanner loop."""
    global _running
    _running = False
    logger.info("Radar scanner stopped.")
