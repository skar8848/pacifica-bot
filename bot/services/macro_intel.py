"""
Macro Intelligence — aggregates macro context for the dashboard.

Sources (all public, no API keys needed):
- Yahoo Finance: BTC SMA50/200, QQQ, XLP, DXY, VIX
- CoinGecko: stablecoin market caps & peg status
- Alternative.me: Fear & Greed Index
- Mempool.space: Bitcoin hashrate
- Yahoo Finance: BTC ETF flows (IBIT, FBTC, GBTC, etc.)

Provides:
- Macro verdict (BUY / CASH / NEUTRAL)
- ETF flow direction
- Stablecoin health
- Fear & Greed
- Technical trend (SMA cross)
"""

import asyncio
import logging
import time
from typing import Any

import aiohttp

logger = logging.getLogger(__name__)

# In-memory cache
_cache: dict[str, Any] = {}
_cache_ts: dict[str, float] = {}

CACHE_TTL = 300  # 5 min


def _cached(key: str) -> Any | None:
    if key in _cache and time.time() - _cache_ts.get(key, 0) < CACHE_TTL:
        return _cache[key]
    return None


def _set_cache(key: str, data: Any):
    _cache[key] = data
    _cache_ts[key] = time.time()


# ---------------------------------------------------------------------------
# Yahoo Finance helper
# ---------------------------------------------------------------------------

async def _yahoo_quote(session: aiohttp.ClientSession, symbol: str) -> dict | None:
    """Fetch a quote from Yahoo Finance chart API."""
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
    params = {"interval": "1d", "range": "60d"}
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        async with session.get(url, params=params, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()
            result = data.get("chart", {}).get("result", [{}])[0]
            meta = result.get("meta", {})
            quotes = result.get("indicators", {}).get("quote", [{}])[0]
            closes = quotes.get("close", [])
            volumes = quotes.get("volume", [])
            return {
                "symbol": symbol,
                "price": meta.get("regularMarketPrice", 0),
                "prev_close": meta.get("previousClose", meta.get("chartPreviousClose", 0)),
                "closes": [c for c in closes if c is not None],
                "volumes": [v for v in volumes if v is not None],
            }
    except Exception as e:
        logger.debug("Yahoo quote %s failed: %s", symbol, e)
        return None


# ---------------------------------------------------------------------------
# Macro Signals
# ---------------------------------------------------------------------------

async def get_macro_signals() -> dict:
    """
    Compute macro signals verdict.

    Returns: {verdict, signals: {technical, fear_greed, liquidity, macro_regime, flow_structure}, timestamp}
    """
    cached = _cached("macro_signals")
    if cached:
        return cached

    async with aiohttp.ClientSession() as session:
        # Fetch all quotes in parallel
        tasks = {
            "btc": _yahoo_quote(session, "BTC-USD"),
            "qqq": _yahoo_quote(session, "QQQ"),
            "xlp": _yahoo_quote(session, "XLP"),
            "dxy": _yahoo_quote(session, "DX-Y.NYB"),
            "vix": _yahoo_quote(session, "^VIX"),
        }
        results = {}
        for key, coro in tasks.items():
            try:
                results[key] = await coro
            except Exception:
                results[key] = None

        # Fear & Greed
        fear_greed = await _fetch_fear_greed(session)

        # Hashrate
        hashrate = await _fetch_hashrate(session)

    signals: dict[str, dict] = {}
    bullish_count = 0
    total_count = 0

    # 1. Technical Trend (BTC SMA50 vs SMA200)
    btc = results.get("btc")
    if btc and len(btc["closes"]) >= 50:
        closes = btc["closes"]
        sma50 = sum(closes[-50:]) / 50
        sma200 = sum(closes[-min(200, len(closes)):]) / min(200, len(closes))
        price = btc["price"]
        mayer = price / sma200 if sma200 > 0 else 0

        if price > sma50 and sma50 > sma200:
            status = "BULLISH"
            bullish_count += 1
        elif price < sma50 and sma50 < sma200:
            status = "BEARISH"
        else:
            status = "NEUTRAL"
        total_count += 1

        signals["technical"] = {
            "status": status,
            "btc_price": price,
            "sma50": round(sma50, 2),
            "sma200": round(sma200, 2),
            "mayer_multiple": round(mayer, 3),
            "sparkline": closes[-20:],
        }
    else:
        signals["technical"] = {"status": "UNKNOWN"}

    # 2. Fear & Greed
    if fear_greed is not None:
        total_count += 1
        fg_status = "EXTREME FEAR" if fear_greed < 25 else "FEAR" if fear_greed < 40 else "NEUTRAL" if fear_greed < 60 else "GREED" if fear_greed < 75 else "EXTREME GREED"
        if fear_greed >= 40:
            bullish_count += 1
        signals["fear_greed"] = {"status": fg_status, "value": fear_greed}
    else:
        signals["fear_greed"] = {"status": "UNKNOWN", "value": None}

    # 3. Macro Regime (QQQ vs XLP — risk-on vs defensive)
    qqq = results.get("qqq")
    xlp = results.get("xlp")
    if qqq and xlp and len(qqq["closes"]) >= 20 and len(xlp["closes"]) >= 20:
        qqq_roc = (qqq["closes"][-1] - qqq["closes"][-20]) / qqq["closes"][-20] * 100
        xlp_roc = (xlp["closes"][-1] - xlp["closes"][-20]) / xlp["closes"][-20] * 100
        total_count += 1
        if qqq_roc > xlp_roc and qqq_roc > 0:
            status = "RISK-ON"
            bullish_count += 1
        elif xlp_roc > qqq_roc:
            status = "DEFENSIVE"
        else:
            status = "NEUTRAL"
        signals["macro_regime"] = {
            "status": status,
            "qqq_roc20": round(qqq_roc, 2),
            "xlp_roc20": round(xlp_roc, 2),
        }
    else:
        signals["macro_regime"] = {"status": "UNKNOWN"}

    # 4. DXY / VIX
    dxy = results.get("dxy")
    vix = results.get("vix")
    if dxy and dxy["price"]:
        signals["dxy"] = {"value": dxy["price"], "trend": "WEAK" if dxy["price"] < 100 else "STRONG"}
        total_count += 1
        if dxy["price"] < 100:
            bullish_count += 1  # weak dollar = risk-on
    else:
        signals["dxy"] = {"value": None, "trend": "UNKNOWN"}

    if vix and vix["price"]:
        signals["vix"] = {"value": round(vix["price"], 2), "status": "LOW" if vix["price"] < 20 else "ELEVATED" if vix["price"] < 30 else "HIGH"}
        total_count += 1
        if vix["price"] < 20:
            bullish_count += 1
    else:
        signals["vix"] = {"value": None, "status": "UNKNOWN"}

    # 5. Hashrate
    if hashrate is not None:
        signals["hashrate"] = hashrate
        total_count += 1
        if hashrate.get("status") == "GROWING":
            bullish_count += 1

    # Verdict
    if total_count == 0:
        verdict = "UNKNOWN"
    elif bullish_count / total_count >= 0.57:
        verdict = "BUY"
    elif bullish_count / total_count <= 0.3:
        verdict = "CASH"
    else:
        verdict = "NEUTRAL"

    result = {
        "verdict": verdict,
        "bullish_count": bullish_count,
        "total_count": total_count,
        "signals": signals,
        "timestamp": time.time(),
    }
    _set_cache("macro_signals", result)
    return result


# ---------------------------------------------------------------------------
# ETF Flows
# ---------------------------------------------------------------------------

ETF_TICKERS = [
    ("IBIT", "BlackRock"),
    ("FBTC", "Fidelity"),
    ("ARKB", "ARK/21Shares"),
    ("BITB", "Bitwise"),
    ("GBTC", "Grayscale"),
    ("HODL", "VanEck"),
    ("BRRR", "Valkyrie"),
    ("EZBC", "Franklin"),
    ("BTCO", "Invesco"),
    ("BTCW", "WisdomTree"),
]


async def get_etf_flows() -> dict:
    """
    Estimate BTC ETF inflows/outflows from volume and price action.

    Returns: {summary: {net_direction, total_volume, inflow_count, outflow_count}, etfs: [...]}
    """
    cached = _cached("etf_flows")
    if cached:
        return cached

    async with aiohttp.ClientSession() as session:
        tasks = [_yahoo_quote(session, ticker) for ticker, _ in ETF_TICKERS]
        raw = await asyncio.gather(*tasks, return_exceptions=True)

    etfs = []
    total_volume = 0
    total_est_flow = 0
    inflow_count = 0
    outflow_count = 0

    for i, (ticker, issuer) in enumerate(ETF_TICKERS):
        data = raw[i] if not isinstance(raw[i], Exception) else None
        if not data or not data.get("price"):
            continue

        price = data["price"]
        prev = data.get("prev_close", price)
        change_pct = ((price - prev) / prev * 100) if prev > 0 else 0

        volumes = data.get("volumes", [])
        vol = volumes[-1] if volumes else 0
        avg_vol = sum(volumes[-5:]) / max(len(volumes[-5:]), 1) if volumes else 0
        vol_ratio = vol / avg_vol if avg_vol > 0 else 1

        # Heuristic: positive price change + high volume = inflow
        if change_pct > 0.1 and vol_ratio > 0.8:
            direction = "inflow"
            est_flow = vol * price * 0.3  # rough estimate
            inflow_count += 1
        elif change_pct < -0.1:
            direction = "outflow"
            est_flow = -vol * price * 0.3
            outflow_count += 1
        else:
            direction = "neutral"
            est_flow = 0

        total_volume += vol * price
        total_est_flow += est_flow

        etfs.append({
            "ticker": ticker,
            "issuer": issuer,
            "price": round(price, 2),
            "change_pct": round(change_pct, 2),
            "volume": vol,
            "avg_volume": round(avg_vol),
            "volume_ratio": round(vol_ratio, 2),
            "direction": direction,
            "est_flow": round(est_flow),
        })

    net_direction = "NET INFLOW" if total_est_flow > 0 else "NET OUTFLOW" if total_est_flow < 0 else "NEUTRAL"
    if not etfs:
        net_direction = "UNAVAILABLE"

    result = {
        "summary": {
            "etf_count": len(etfs),
            "total_volume": round(total_volume),
            "total_est_flow": round(total_est_flow),
            "net_direction": net_direction,
            "inflow_count": inflow_count,
            "outflow_count": outflow_count,
        },
        "etfs": etfs,
        "timestamp": time.time(),
    }
    _set_cache("etf_flows", result)
    return result


# ---------------------------------------------------------------------------
# Stablecoin Health
# ---------------------------------------------------------------------------

STABLECOINS = ["tether", "usd-coin", "dai", "first-digital-usd", "ethena-usde"]


async def get_stablecoin_health() -> dict:
    """
    Check stablecoin peg health and volumes.

    Returns: {summary: {health, total_mcap, depegged_count}, stablecoins: [...]}
    """
    cached = _cached("stablecoin_health")
    if cached:
        return cached

    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",
        "ids": ",".join(STABLECOINS),
        "order": "market_cap_desc",
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    return {"summary": {"health": "UNAVAILABLE"}, "stablecoins": [], "timestamp": time.time()}
                data = await resp.json()
    except Exception as e:
        logger.debug("Stablecoin fetch failed: %s", e)
        return {"summary": {"health": "UNAVAILABLE"}, "stablecoins": [], "timestamp": time.time()}

    coins = []
    total_mcap = 0
    total_vol = 0
    depegged = 0

    for c in data:
        price = c.get("current_price", 1.0)
        deviation = abs(price - 1.0) / 1.0 * 100

        if deviation <= 0.5:
            peg_status = "ON PEG"
        elif deviation <= 1.0:
            peg_status = "SLIGHT DEPEG"
        else:
            peg_status = "DEPEGGED"
            depegged += 1

        mcap = c.get("market_cap", 0) or 0
        vol = c.get("total_volume", 0) or 0
        total_mcap += mcap
        total_vol += vol

        coins.append({
            "id": c.get("id"),
            "symbol": (c.get("symbol") or "").upper(),
            "name": c.get("name"),
            "price": price,
            "deviation": round(deviation, 3),
            "peg_status": peg_status,
            "market_cap": mcap,
            "volume_24h": vol,
            "change_24h": c.get("price_change_percentage_24h", 0),
        })

    health = "HEALTHY" if depegged == 0 else "CAUTION" if depegged == 1 else "WARNING"

    result = {
        "summary": {
            "health": health,
            "total_mcap": total_mcap,
            "total_volume": total_vol,
            "depegged_count": depegged,
            "coin_count": len(coins),
        },
        "stablecoins": coins,
        "timestamp": time.time(),
    }
    _set_cache("stablecoin_health", result)
    return result


# ---------------------------------------------------------------------------
# Fear & Greed
# ---------------------------------------------------------------------------

async def _fetch_fear_greed(session: aiohttp.ClientSession) -> int | None:
    try:
        async with session.get(
            "https://api.alternative.me/fng/?limit=1",
            timeout=aiohttp.ClientTimeout(total=8),
        ) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()
            return int(data["data"][0]["value"])
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Hashrate
# ---------------------------------------------------------------------------

async def _fetch_hashrate(session: aiohttp.ClientSession) -> dict | None:
    try:
        async with session.get(
            "https://mempool.space/api/v1/mining/hashrate/1m",
            timeout=aiohttp.ClientTimeout(total=8),
        ) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()
            hashrates = data.get("hashrates", [])
            if len(hashrates) < 2:
                return None
            current = hashrates[-1].get("avgHashrate", 0)
            prev = hashrates[-max(30, len(hashrates))].get("avgHashrate", 0)
            change = ((current - prev) / prev * 100) if prev > 0 else 0
            status = "GROWING" if change > 2 else "STABLE" if change > -2 else "DECLINING"
            return {"status": status, "change_30d": round(change, 1), "current": current}
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Combined Intel (all 3 in one call for efficiency)
# ---------------------------------------------------------------------------

async def get_full_intel() -> dict:
    """Return all macro intel in one shot."""
    macro, etf, stable = await asyncio.gather(
        get_macro_signals(),
        get_etf_flows(),
        get_stablecoin_health(),
        return_exceptions=True,
    )
    return {
        "macro": macro if not isinstance(macro, Exception) else {"verdict": "UNKNOWN", "signals": {}},
        "etf": etf if not isinstance(etf, Exception) else {"summary": {"net_direction": "UNAVAILABLE"}, "etfs": []},
        "stablecoins": stable if not isinstance(stable, Exception) else {"summary": {"health": "UNAVAILABLE"}, "stablecoins": []},
        "timestamp": time.time(),
    }
