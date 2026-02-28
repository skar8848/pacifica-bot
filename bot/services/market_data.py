"""
Shared market data service — single source of truth for prices, lot sizes, market info.

Replaces duplicate _get_price / _get_lot_size / _usd_to_token across
trading.py, copy_engine.py, and portfolio.py.
"""

import math
import logging

from solders.keypair import Keypair

from bot.services.pacifica_client import PacificaClient

logger = logging.getLogger(__name__)

# Shared read-only client (no signing needed)
_client: PacificaClient | None = None


async def _get_client() -> PacificaClient:
    global _client
    if _client is None or (_client._session and _client._session.closed):
        _client = PacificaClient(account="public", keypair=Keypair())
    return _client


async def close():
    """Shut down the shared client (call on bot shutdown)."""
    global _client
    if _client:
        await _client.close()
        _client = None


# ------------------------------------------------------------------
# Price
# ------------------------------------------------------------------

async def get_price(symbol: str) -> float | None:
    """Fetch latest price — tries /trades then /info/prices fallback."""
    client = await _get_client()

    try:
        trades = await client.get_trades(symbol, limit=1)
        if trades:
            return float(trades[0]["price"])
    except Exception:
        pass

    try:
        prices = await client.get_prices()
        if isinstance(prices, list):
            p = next((x for x in prices if x.get("symbol") == symbol), None)
            if p:
                return float(
                    p.get("mark_price") or p.get("index_price") or p.get("price", 0)
                )
        elif isinstance(prices, dict) and symbol in prices:
            return float(prices[symbol])
    except Exception:
        pass

    return None


# ------------------------------------------------------------------
# Market info (leverage, tick, lot)
# ------------------------------------------------------------------

# Simple cache: {symbol: (max_leverage, tick_size, lot_size)}
_market_cache: dict[str, tuple[int, str, str]] = {}


async def get_markets_info() -> list:
    """Fetch all markets from API."""
    client = await _get_client()
    return await client.get_markets_info()


async def get_market_info(symbol: str) -> tuple[int, str, str]:
    """Return (max_leverage, tick_size, lot_size) for a symbol."""
    if symbol in _market_cache:
        return _market_cache[symbol]

    try:
        markets = await get_markets_info()
        # Cache all symbols from this call
        for m in markets:
            sym = m.get("symbol", "")
            if sym:
                entry = (
                    int(m.get("max_leverage", 50)),
                    str(m.get("tick_size", "1")),
                    str(m.get("lot_size", "0.01")),
                )
                _market_cache[sym] = entry

        if symbol in _market_cache:
            return _market_cache[symbol]
    except Exception:
        pass

    return 50, "1", "0.01"


async def get_lot_size(symbol: str) -> str:
    _, _, lot = await get_market_info(symbol)
    return lot


async def get_max_leverage(symbol: str) -> int:
    lev, _, _ = await get_market_info(symbol)
    return lev


# ------------------------------------------------------------------
# Unit conversion helpers
# ------------------------------------------------------------------

def token_to_usd(token_amount: float, price: float) -> float:
    return token_amount * price


def usd_to_token(usd_amount: float, price: float, lot_size: str = "0.01") -> str:
    """Convert USD to token amount, rounded down to lot size."""
    if price <= 0:
        return "0"
    raw = usd_amount / price
    lot = float(lot_size)
    rounded = math.floor(raw / lot) * lot
    if lot >= 1:
        return str(int(rounded))
    decimals = len(lot_size.split(".")[-1]) if "." in lot_size else 0
    return f"{rounded:.{decimals}f}"


def round_to_lot(amount: float, lot_size: str) -> str:
    """Round a token amount down to lot size."""
    lot = float(lot_size)
    rounded = math.floor(amount / lot) * lot
    if lot >= 1:
        return str(int(rounded))
    decimals = len(lot_size.split(".")[-1]) if "." in lot_size else 0
    return f"{rounded:.{decimals}f}"
