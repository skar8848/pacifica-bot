"""
Copy trading engine — polls master wallets and replicates positions.

Sizing modes:
  - fixed_usd: fixed dollar amount per trade (e.g. $10)
  - pct_equity: percentage of user's equity per trade (e.g. 5%)
  - proportional: multiply master's position size (e.g. 0.5x)
"""

import asyncio
import math
import logging
from typing import Any

from aiogram import Bot
from solders.keypair import Keypair

from bot.config import COPY_POLL_INTERVAL
from bot.services.pacifica_client import PacificaClient, PacificaAPIError
from bot.models.user import build_client_from_user
from database.db import (
    get_active_copy_configs,
    get_user,
    log_trade,
)

logger = logging.getLogger(__name__)

_running = False
# Track last known positions per master: {master_wallet: {symbol: position_dict}}
_master_snapshots: dict[str, dict[str, dict]] = {}

# Shared read-only client for fetching prices / market info
_ro_client: PacificaClient | None = None


async def _get_ro_client() -> PacificaClient:
    """Get or create a shared read-only client (no signing needed)."""
    global _ro_client
    if _ro_client is None or (_ro_client._session and _ro_client._session.closed):
        _ro_client = PacificaClient(account="public", keypair=Keypair())
    return _ro_client


async def _get_price(symbol: str) -> float:
    """Fetch latest trade price for a symbol."""
    client = await _get_ro_client()
    trades = await client.get_trades(symbol, limit=1)
    if trades:
        return float(trades[0]["price"])
    return 0.0


async def _get_lot_size(symbol: str) -> str:
    """Fetch lot size for a symbol from market info."""
    client = await _get_ro_client()
    markets = await client.get_markets_info()
    m = next((x for x in markets if x.get("symbol") == symbol), None)
    if m:
        return str(m.get("lot_size", "0.01"))
    return "0.01"


def _token_to_usd(token_amount: float, price: float) -> float:
    """Convert token amount to USD notional."""
    return token_amount * price


def _usd_to_token(usd_amount: float, price: float, lot_size: str) -> str:
    """Convert USD amount to token amount, rounded down to lot size."""
    if price <= 0:
        return "0"
    raw = usd_amount / price
    lot = float(lot_size)
    rounded = math.floor(raw / lot) * lot
    if lot >= 1:
        return str(int(rounded))
    decimals = len(lot_size.split(".")[-1]) if "." in lot_size else 0
    return f"{rounded:.{decimals}f}"


def _round_to_lot(amount: float, lot_size: str) -> str:
    """Round a token amount down to lot size."""
    lot = float(lot_size)
    rounded = math.floor(amount / lot) * lot
    if lot >= 1:
        return str(int(rounded))
    decimals = len(lot_size.split(".")[-1]) if "." in lot_size else 0
    return f"{rounded:.{decimals}f}"


async def start_copy_engine(bot: Bot):
    global _running
    _running = True
    logger.info("Copy engine started (poll every %ss)", COPY_POLL_INTERVAL)

    while _running:
        try:
            await _poll_cycle(bot)
        except Exception as e:
            logger.error("Copy engine error: %s", e)
        await asyncio.sleep(COPY_POLL_INTERVAL)


def stop_copy_engine():
    global _running
    _running = False


async def _poll_cycle(bot: Bot):
    configs = await get_active_copy_configs()
    if not configs:
        return

    # Group configs by master wallet
    masters: dict[str, list[dict]] = {}
    for cfg in configs:
        masters.setdefault(cfg["master_wallet"], []).append(cfg)

    for master_wallet, followers in masters.items():
        try:
            await _check_master(bot, master_wallet, followers)
        except Exception as e:
            logger.error("Error checking master %s: %s", master_wallet, e)


async def _check_master(bot: Bot, master_wallet: str, followers: list[dict]):
    # Fetch master's current positions (read-only)
    client = await _get_ro_client()

    try:
        positions = await client.get_positions(master_wallet)
    except PacificaAPIError:
        logger.warning("Could not fetch positions for master %s", master_wallet)
        return

    # Build current snapshot: {symbol: position}
    current: dict[str, dict] = {}
    for pos in positions:
        symbol = pos.get("symbol", "")
        if symbol:
            current[symbol] = pos

    prev = _master_snapshots.get(master_wallet, {})
    _master_snapshots[master_wallet] = current

    # Detect changes
    # New positions (opened)
    for symbol, pos in current.items():
        if symbol not in prev:
            await _replicate_open(bot, master_wallet, pos, followers)
        else:
            # Check if side changed (flipped position)
            if pos.get("side") != prev[symbol].get("side"):
                await _replicate_close(bot, master_wallet, prev[symbol], followers)
                await _replicate_open(bot, master_wallet, pos, followers)

    # Closed positions
    for symbol, pos in prev.items():
        if symbol not in current:
            await _replicate_close(bot, master_wallet, pos, followers)


async def _get_total_copy_exposure(user_client: PacificaClient) -> float:
    """Calculate the user's total USD exposure across all open positions.

    This is used to enforce max_total_usd — protecting against a master
    spamming hundreds of small trades across different tokens.
    """
    try:
        positions = await user_client.get_positions()
        total = 0.0
        for pos in positions:
            amount = abs(float(pos.get("amount", pos.get("size", 0))))
            entry = float(pos.get("entry_price", 0))
            total += amount * entry
        return total
    except Exception:
        return 0.0


async def _calculate_copy_amount(
    cfg: dict, symbol: str, master_size: float, price: float, lot_size: str,
    user_client: PacificaClient,
) -> str | None:
    """Calculate the copy trade amount based on sizing mode.

    Returns the token amount as a string, or None to skip this trade.
    """
    sizing_mode = cfg.get("sizing_mode", "fixed_usd")
    max_usd = cfg.get("max_position_usd", 1000)
    max_total_usd = cfg.get("max_total_usd", 5000)
    min_trade_usd = cfg.get("min_trade_usd", 0)

    # Check minimum trade filter: skip if master's trade is too small
    master_usd = _token_to_usd(master_size, price)
    if min_trade_usd > 0 and master_usd < min_trade_usd:
        return None

    copy_usd = 0.0

    if sizing_mode == "fixed_usd":
        copy_usd = cfg.get("fixed_amount_usd", 10.0)

    elif sizing_mode == "pct_equity":
        pct = cfg.get("pct_equity", 5.0)
        try:
            account = user_client.account
            info = await user_client.get_account_info(account)
            equity = float(info.get("equity", 0))
            copy_usd = equity * (pct / 100.0)
        except Exception as e:
            logger.warning("Could not fetch equity for %s: %s", cfg["telegram_id"], e)
            return None

    elif sizing_mode == "proportional":
        multiplier = cfg.get("size_multiplier", 1.0)
        copy_token = master_size * multiplier
        copy_usd = _token_to_usd(copy_token, price)

    else:
        # Fallback to proportional
        multiplier = cfg.get("size_multiplier", 1.0)
        copy_token = master_size * multiplier
        copy_usd = _token_to_usd(copy_token, price)

    # Apply max per-position cap
    if copy_usd > max_usd:
        copy_usd = max_usd

    # Apply max total exposure cap — check all existing positions
    current_exposure = await _get_total_copy_exposure(user_client)
    remaining_budget = max_total_usd - current_exposure
    if remaining_budget <= 0:
        logger.info(
            "User %s at total cap ($%.0f/$%.0f), skipping copy",
            cfg["telegram_id"], current_exposure, max_total_usd,
        )
        return None
    if copy_usd > remaining_budget:
        copy_usd = remaining_budget

    # Convert USD to token amount
    amount_str = _usd_to_token(copy_usd, price, lot_size)

    # Skip if amount is zero or negligible
    if float(amount_str) <= 0:
        return None

    return amount_str


async def _replicate_open(
    bot: Bot, master_wallet: str, master_pos: dict, followers: list[dict]
):
    symbol = master_pos.get("symbol", "?")
    side = master_pos.get("side", "bid")
    master_size = abs(float(master_pos.get("amount", master_pos.get("size", 0))))

    # Fetch price and lot size once for all followers
    try:
        price = await _get_price(symbol)
        lot_size = await _get_lot_size(symbol)
    except Exception:
        price = 0.0
        lot_size = "0.01"

    if price <= 0:
        logger.warning("Could not get price for %s, skipping copy", symbol)
        return

    for cfg in followers:
        tg_id = cfg["telegram_id"]
        client = None

        try:
            user = await get_user(tg_id)
            if not user or not user.get("pacifica_account"):
                continue

            client = build_client_from_user(user)

            # Calculate copy amount based on sizing mode
            amount = await _calculate_copy_amount(
                cfg, symbol, master_size, price, lot_size, client,
            )

            if amount is None:
                continue  # Skipped (min filter or zero amount)

            await client.create_market_order(
                symbol=symbol,
                side=side,
                amount=amount,
            )

            await log_trade(
                tg_id, symbol, side, amount,
                order_type="copy_open",
                is_copy_trade=True,
                master_wallet=master_wallet,
            )

            direction = "LONG" if side == "bid" else "SHORT"
            copy_usd = float(amount) * price
            sizing_mode = cfg.get("sizing_mode", "fixed_usd")
            mode_label = sizing_mode.replace("_", " ").title()

            await bot.send_message(
                tg_id,
                f"<b>Copy Trade Executed</b>\n\n"
                f"Master: <code>{master_wallet[:8]}...</code>\n"
                f"{symbol} {direction} {amount} (~${copy_usd:,.0f})\n"
                f"Mode: {mode_label}",
            )
        except Exception as e:
            logger.error("Copy open failed for user %s: %s", tg_id, e)
            try:
                await bot.send_message(
                    tg_id,
                    f"Copy trade failed for {symbol}: {e}",
                )
            except Exception:
                pass
        finally:
            if client:
                await client.close()


async def _replicate_close(
    bot: Bot, master_wallet: str, master_pos: dict, followers: list[dict]
):
    symbol = master_pos.get("symbol", "?")

    for cfg in followers:
        tg_id = cfg["telegram_id"]
        client = None

        try:
            user = await get_user(tg_id)
            if not user or not user.get("pacifica_account"):
                continue

            client = build_client_from_user(user)

            # Get follower's actual position for this symbol
            positions = await client.get_positions()
            pos = next(
                (p for p in positions if p.get("symbol", "").upper() == symbol.upper()),
                None,
            )
            if not pos:
                continue

            pos_size = abs(float(pos.get("amount", pos.get("size", 0))))
            lot_size = await _get_lot_size(symbol)
            amount = _round_to_lot(pos_size, lot_size)
            actual_close_side = "ask" if pos.get("side") == "bid" else "bid"

            if float(amount) <= 0:
                continue

            await client.create_market_order(
                symbol=symbol,
                side=actual_close_side,
                amount=amount,
                reduce_only=True,
            )

            await log_trade(
                tg_id, symbol, actual_close_side, amount,
                order_type="copy_close",
                is_copy_trade=True,
                master_wallet=master_wallet,
            )

            await bot.send_message(
                tg_id,
                f"<b>Copy Trade — Position Closed</b>\n\n"
                f"Master: <code>{master_wallet[:8]}...</code>\n"
                f"{symbol} closed (size: {amount})",
            )
        except Exception as e:
            logger.error("Copy close failed for user %s: %s", tg_id, e)
        finally:
            if client:
                await client.close()
