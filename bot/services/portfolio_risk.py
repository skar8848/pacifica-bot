"""
Portfolio risk manager — correlation groups and exposure limits.

Prevents concentrated correlated bets by enforcing:
  - At most MAX_PER_GROUP open positions within the same correlation group.
  - At most MAX_SAME_DIRECTION total positions in the same direction (long/short).
  - Margin utilisation warnings above MARGIN_WARN and hard blocks above MARGIN_BLOCK.

Symbols not found in any group are placed in an "uncategorized" bucket that
does not count against group limits but does count against direction limits.

Public API (safe to call without any background loop):
  check_entry(symbol, side, positions)   -> dict
  get_portfolio_exposure(positions)      -> dict
  get_correlation_groups()               -> dict
"""

import logging
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Correlation groups
# ---------------------------------------------------------------------------

CORRELATION_GROUPS: dict[str, list[str]] = {
    "large_cap": ["BTC", "ETH"],
    "l2":        ["ARB", "OP", "STRK", "MANTA", "BLAST", "ZK"],
    "alt_l1":    ["SOL", "AVAX", "SUI", "APT", "SEI", "TIA", "INJ", "NEAR"],
    "defi":      ["AAVE", "UNI", "LINK", "MKR", "SNX", "CRV", "PENDLE"],
    "meme":      ["DOGE", "SHIB", "PEPE", "WIF", "BONK", "FLOKI", "BRETT", "FARTCOIN"],
    "ai":        ["FET", "RNDR", "TAO", "WLD", "ARKM"],
    "gaming":    ["IMX", "GALA", "AXS", "PIXEL"],
    "rwa":       ["ONDO", "PENDLE"],
}

# Reverse lookup: symbol -> group name
_SYMBOL_TO_GROUP: dict[str, str] = {
    symbol: group
    for group, symbols in CORRELATION_GROUPS.items()
    for symbol in symbols
}

# ---------------------------------------------------------------------------
# Limits
# ---------------------------------------------------------------------------

MAX_PER_GROUP = 2         # max concurrent positions in the same correlation group
MAX_SAME_DIRECTION = 4    # max total longs OR shorts across the portfolio
MARGIN_WARN = 0.70        # warn when margin utilisation >= 70%
MARGIN_BLOCK = 0.90       # block new entries when margin utilisation >= 90%

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_LONG_SIDES = {"long", "buy"}
_SHORT_SIDES = {"short", "sell"}


def _normalise_side(side: str) -> str:
    """Return 'long' or 'short', or raise ValueError."""
    side_lower = side.lower().strip()
    if side_lower in _LONG_SIDES:
        return "long"
    if side_lower in _SHORT_SIDES:
        return "short"
    raise ValueError(f"Unknown side: {side!r}. Expected long/buy or short/sell.")


def _get_group(symbol: str) -> str:
    """Return the correlation group for a symbol, or 'uncategorized'."""
    return _SYMBOL_TO_GROUP.get(symbol.upper(), "uncategorized")


def _extract_symbol(pos: dict) -> str:
    """Extract the bare symbol from a position dict (strips -PERP suffix if present)."""
    raw = pos.get("symbol", "")
    return raw.replace("-PERP", "").replace("-perp", "").upper()


def _extract_side(pos: dict) -> str | None:
    """Return 'long' or 'short' from a position dict, or None if unrecognised."""
    raw = pos.get("side", "")
    try:
        return _normalise_side(raw)
    except ValueError:
        return None


def _position_margin(pos: dict) -> float:
    """Return the margin (collateral) used by a position in USD."""
    # Pacifica positions may expose 'margin', 'collateral', or 'notional/leverage'
    margin = pos.get("margin") or pos.get("collateral")
    if margin is not None:
        return float(margin)
    # Fallback: notional / leverage
    try:
        notional = float(pos.get("notional") or 0)
        leverage = float(pos.get("leverage") or 1)
        if notional > 0 and leverage > 0:
            return notional / leverage
    except (TypeError, ValueError, ZeroDivisionError):
        pass
    return 0.0


def _total_margin(positions: list[dict]) -> float:
    return sum(_position_margin(p) for p in positions)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def check_entry(
    symbol: str,
    side: str,
    positions: list[dict],
    total_equity: float = 0.0,
) -> dict[str, Any]:
    """
    Decide whether a new entry is allowed given the current open positions.

    Args:
        symbol:        Bare Pacifica symbol, e.g. "BTC" (no -PERP suffix needed).
        side:          "long" / "buy" or "short" / "sell".
        positions:     List of currently open position dicts from the Pacifica API.
        total_equity:  Account equity in USD.  When > 0, margin utilisation is
                       checked against MARGIN_WARN / MARGIN_BLOCK.

    Returns:
        {
            "allowed":          bool,
            "reason":           str,
            "group":            str,          # correlation group of the symbol
            "current_count":    int,          # existing positions in same group
            "direction_count":  int,          # existing positions in same direction
            "margin_util":      float | None, # 0.0–1.0, None if equity unknown
        }
    """
    symbol = symbol.upper().replace("-PERP", "")

    try:
        norm_side = _normalise_side(side)
    except ValueError as exc:
        return {
            "allowed": False,
            "reason": str(exc),
            "group": _get_group(symbol),
            "current_count": 0,
            "direction_count": 0,
            "margin_util": None,
        }

    group = _get_group(symbol)

    # Count positions in same correlation group (excluding uncategorized)
    if group != "uncategorized":
        group_count = sum(
            1 for p in positions
            if _get_group(_extract_symbol(p)) == group
        )
    else:
        group_count = 0

    # Count positions in same direction
    direction_count = sum(
        1 for p in positions
        if _extract_side(p) == norm_side
    )

    # Margin utilisation check
    margin_util: float | None = None
    if total_equity > 0:
        used_margin = _total_margin(positions)
        margin_util = used_margin / total_equity

        if margin_util >= MARGIN_BLOCK:
            return {
                "allowed": False,
                "reason": (
                    f"Margin utilisation {margin_util*100:.1f}% >= "
                    f"block threshold {MARGIN_BLOCK*100:.0f}%"
                ),
                "group": group,
                "current_count": group_count,
                "direction_count": direction_count,
                "margin_util": margin_util,
            }

    # Group limit
    if group != "uncategorized" and group_count >= MAX_PER_GROUP:
        return {
            "allowed": False,
            "reason": (
                f"Group '{group}' already has {group_count}/{MAX_PER_GROUP} positions"
            ),
            "group": group,
            "current_count": group_count,
            "direction_count": direction_count,
            "margin_util": margin_util,
        }

    # Direction limit
    if direction_count >= MAX_SAME_DIRECTION:
        return {
            "allowed": False,
            "reason": (
                f"Already {direction_count} {norm_side} positions "
                f"(max {MAX_SAME_DIRECTION})"
            ),
            "group": group,
            "current_count": group_count,
            "direction_count": direction_count,
            "margin_util": margin_util,
        }

    # All clear — build reason with any soft warning
    reason = "OK"
    if margin_util is not None and margin_util >= MARGIN_WARN:
        reason = (
            f"WARN: margin utilisation {margin_util*100:.1f}% >= "
            f"{MARGIN_WARN*100:.0f}% — consider reducing exposure"
        )

    return {
        "allowed": True,
        "reason": reason,
        "group": group,
        "current_count": group_count,
        "direction_count": direction_count,
        "margin_util": margin_util,
    }


def get_portfolio_exposure(
    positions: list[dict],
    total_equity: float = 0.0,
) -> dict[str, Any]:
    """
    Build a full exposure breakdown of the current portfolio.

    Args:
        positions:    List of open position dicts from the Pacifica API.
        total_equity: Account equity in USD (optional; enables margin_util).

    Returns:
        {
            "groups": {
                group_name: {
                    "symbols":   list[str],
                    "count":     int,
                    "at_limit":  bool,
                }
            },
            "direction": {
                "long":  int,
                "short": int,
            },
            "margin_util":   float | None,
            "total_margin":  float,
            "position_count": int,
        }
    """
    # Per-group breakdown
    group_map: dict[str, dict[str, Any]] = {}
    for p in positions:
        sym = _extract_symbol(p)
        grp = _get_group(sym)
        if grp not in group_map:
            group_map[grp] = {"symbols": [], "count": 0, "at_limit": False}
        group_map[grp]["symbols"].append(sym)
        group_map[grp]["count"] += 1

    # Mark groups at their limit
    for grp, info in group_map.items():
        if grp != "uncategorized":
            info["at_limit"] = info["count"] >= MAX_PER_GROUP

    # Direction counts
    longs = sum(1 for p in positions if _extract_side(p) == "long")
    shorts = sum(1 for p in positions if _extract_side(p) == "short")

    # Margin
    used_margin = _total_margin(positions)
    margin_util: float | None = None
    if total_equity > 0:
        margin_util = used_margin / total_equity

    return {
        "groups": group_map,
        "direction": {"long": longs, "short": shorts},
        "margin_util": margin_util,
        "total_margin": round(used_margin, 4),
        "position_count": len(positions),
    }


def get_correlation_groups() -> dict[str, list[str]]:
    """
    Return the full CORRELATION_GROUPS mapping.

    Intended for dashboard display so the UI can show which assets belong
    to each group.
    """
    return {group: list(symbols) for group, symbols in CORRELATION_GROUPS.items()}
