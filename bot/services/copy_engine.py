"""
Copy trading engine — polls master wallets and replicates positions.

Sizing modes:
  - fixed_usd: fixed dollar amount per trade (e.g. $10)
  - pct_equity: percentage of user's equity per trade (e.g. 5%)
  - proportional: multiply master's position size (e.g. 0.5x)
"""

import asyncio
import logging

from aiogram import Bot

from bot.config import COPY_POLL_INTERVAL
from bot.services.pacifica_client import PacificaAPIError
from bot.services.market_data import get_price, get_lot_size, token_to_usd, usd_to_token, round_to_lot, _get_client
from bot.models.user import build_client_from_user
from database.db import (
    get_active_copy_configs,
    get_user,
    get_user_by_wallet,
    log_trade,
    open_follower_position,
    close_follower_position,
    get_leader_profile,
)

logger = logging.getLogger(__name__)

_running = False
_master_snapshots: dict[str, dict[str, dict]] = {}
_leader_wallet_cache: dict[str, int | None] = {}


_user_by_wallet_cache: dict[str, dict | None] = {}


async def _get_user_by_wallet_cached(wallet: str) -> dict | None:
    """Get user by wallet with caching."""
    if wallet in _user_by_wallet_cache:
        return _user_by_wallet_cache[wallet]
    user = await get_user_by_wallet(wallet)
    _user_by_wallet_cache[wallet] = user
    return user


async def _find_leader_by_wallet(wallet: str) -> int | None:
    """Check if a master wallet belongs to a registered leader. Cached."""
    if wallet in _leader_wallet_cache:
        return _leader_wallet_cache[wallet]
    user = await _get_user_by_wallet_cached(wallet)
    if user:
        profile = await get_leader_profile(user["telegram_id"])
        if profile:
            _leader_wallet_cache[wallet] = user["telegram_id"]
            return user["telegram_id"]
    _leader_wallet_cache[wallet] = None
    return None


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

    masters: dict[str, list[dict]] = {}
    for cfg in configs:
        masters.setdefault(cfg["master_wallet"], []).append(cfg)

    for master_wallet, followers in masters.items():
        try:
            await _check_master(bot, master_wallet, followers)
        except Exception as e:
            logger.error("Error checking master %s: %s", master_wallet, e)


async def _check_master(bot: Bot, master_wallet: str, followers: list[dict]):
    client = await _get_client()

    try:
        positions = await client.get_positions(master_wallet)
    except PacificaAPIError:
        logger.warning("Could not fetch positions for master %s", master_wallet)
        return

    current: dict[str, dict] = {}
    for pos in positions:
        symbol = pos.get("symbol", "")
        if symbol:
            current[symbol] = pos

    if master_wallet not in _master_snapshots:
        # First poll for this master — save snapshot only, don't replicate.
        # Existing positions are the baseline, not new trades.
        _master_snapshots[master_wallet] = current
        logger.info("Initial snapshot for master %s: %d positions", master_wallet, len(current))
        return

    prev = _master_snapshots[master_wallet]
    _master_snapshots[master_wallet] = current

    for symbol, pos in current.items():
        if symbol not in prev:
            await _replicate_open(bot, master_wallet, pos, followers)
        elif pos.get("side") != prev[symbol].get("side"):
            await _replicate_close(bot, master_wallet, prev[symbol], followers)
            await _replicate_open(bot, master_wallet, pos, followers)

    for symbol, pos in prev.items():
        if symbol not in current:
            await _replicate_close(bot, master_wallet, pos, followers)


async def _get_total_copy_exposure(user_client) -> float:
    """Total USD exposure across all open positions."""
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
    user_client,
) -> str | None:
    """Calculate copy trade amount based on sizing mode. Returns None to skip."""
    sizing_mode = cfg.get("sizing_mode", "fixed_usd")
    max_usd = cfg.get("max_position_usd", 1000)
    max_total_usd = cfg.get("max_total_usd", 5000)
    min_trade_usd = cfg.get("min_trade_usd", 0)

    master_usd = token_to_usd(master_size, price)
    if min_trade_usd > 0 and master_usd < min_trade_usd:
        return None

    copy_usd = 0.0

    if sizing_mode == "fixed_usd":
        copy_usd = cfg.get("fixed_amount_usd", 10.0)

    elif sizing_mode == "pct_equity":
        pct = cfg.get("pct_equity", 5.0)
        try:
            info = await user_client.get_account_info(user_client.account)
            equity = float(info.get("equity", 0))
            copy_usd = equity * (pct / 100.0)
        except Exception as e:
            logger.warning("Could not fetch equity for %s: %s", cfg["telegram_id"], e)
            return None

    else:  # proportional (or fallback)
        multiplier = cfg.get("size_multiplier", 1.0)
        copy_usd = token_to_usd(master_size * multiplier, price)

    copy_usd = min(copy_usd, max_usd)

    current_exposure = await _get_total_copy_exposure(user_client)
    remaining = max_total_usd - current_exposure
    if remaining <= 0:
        logger.info(
            "User %s at total cap ($%.0f/$%.0f), skipping",
            cfg["telegram_id"], current_exposure, max_total_usd,
        )
        return None
    copy_usd = min(copy_usd, remaining)

    amount_str = usd_to_token(copy_usd, price, lot_size)
    if float(amount_str) <= 0:
        return None
    return amount_str


async def _replicate_open(
    bot: Bot, master_wallet: str, master_pos: dict, followers: list[dict]
):
    symbol = master_pos.get("symbol", "?")
    side = master_pos.get("side", "bid")
    master_size = abs(float(master_pos.get("amount", master_pos.get("size", 0))))

    price = await get_price(symbol) or 0.0
    lot_size = await get_lot_size(symbol)

    if price <= 0:
        logger.warning("Could not get price for %s, skipping copy", symbol)
        return

    # Post leader trade to group feed (once per master open, not per follower)
    master_user_data = await _get_user_by_wallet_cached(master_wallet)
    if master_user_data and master_user_data.get("username"):
        master_usd = token_to_usd(master_size, price)
        from bot.services.group_feed import post_leader_trade
        await post_leader_trade(
            bot, master_user_data["username"], symbol, side, master_usd,
        )

    for cfg in followers:
        tg_id = cfg["telegram_id"]
        client = None
        try:
            user = await get_user(tg_id)
            if not user or not user.get("pacifica_account"):
                continue

            client = build_client_from_user(user)
            amount = await _calculate_copy_amount(
                cfg, symbol, master_size, price, lot_size, client,
            )
            if amount is None:
                continue

            await client.create_market_order(symbol=symbol, side=side, amount=amount)

            await log_trade(
                tg_id, symbol, side, amount,
                order_type="copy_open", is_copy_trade=True, master_wallet=master_wallet,
            )

            # Track follower PnL for leader profit sharing
            leader_user = await _find_leader_by_wallet(master_wallet)
            if leader_user:
                side_label = "long" if side == "bid" else "short"
                await open_follower_position(
                    follower_id=tg_id,
                    leader_id=leader_user,
                    symbol=symbol,
                    side=side_label,
                    entry_price=price,
                    amount=float(amount),
                )

            direction = "LONG" if side == "bid" else "SHORT"
            copy_usd = float(amount) * price
            mode_label = cfg.get("sizing_mode", "fixed_usd").replace("_", " ").title()

            # Show username if leader, otherwise wallet snippet
            master_label = f"<code>{master_wallet[:8]}...</code>"
            if master_user_data and master_user_data.get("username"):
                master_label = f"@{master_user_data['username']}"

            await bot.send_message(
                tg_id,
                f"<b>Copy Trade Executed</b>\n\n"
                f"Master: {master_label}\n"
                f"{symbol} {direction} {amount} (~${copy_usd:,.0f})\n"
                f"Mode: {mode_label}",
            )
        except Exception as e:
            logger.error("Copy open failed for user %s: %s", tg_id, e)
            try:
                await bot.send_message(tg_id, f"Copy trade failed for {symbol}: {e}")
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
            positions = await client.get_positions()
            pos = next(
                (p for p in positions if p.get("symbol", "").upper() == symbol.upper()),
                None,
            )
            if not pos:
                continue

            pos_size = abs(float(pos.get("amount", pos.get("size", 0))))
            lot = await get_lot_size(symbol)
            amount = round_to_lot(pos_size, lot)
            close_side = "ask" if pos.get("side") == "bid" else "bid"

            if float(amount) <= 0:
                continue

            await client.create_market_order(
                symbol=symbol, side=close_side, amount=amount, reduce_only=True,
            )

            await log_trade(
                tg_id, symbol, close_side, amount,
                order_type="copy_close", is_copy_trade=True, master_wallet=master_wallet,
            )

            # Close follower PnL tracking & calculate profit share
            leader_user = await _find_leader_by_wallet(master_wallet)
            pnl_info = None
            if leader_user:
                exit_price = await get_price(symbol) or 0
                if exit_price:
                    pnl_info = await close_follower_position(
                        follower_id=tg_id,
                        leader_id=leader_user,
                        symbol=symbol,
                        exit_price=exit_price,
                    )

            pnl_str = ""
            if pnl_info:
                pnl = pnl_info["pnl"]
                share = pnl_info["profit_share"]
                emoji = "+" if pnl >= 0 else ""
                pnl_str = f"\nPnL: <code>{emoji}${pnl:,.2f}</code>"
                if share > 0:
                    pnl_str += f"\nLeader fee: <code>${share:,.2f}</code>"

                # Post leader PnL to group feed
                master_user_data_close = await _get_user_by_wallet_cached(master_wallet)
                if master_user_data_close and master_user_data_close.get("username"):
                    entry = pnl_info["entry"]
                    exit_p = pnl_info["exit"]
                    pnl_pct = ((exit_p - entry) / entry * 100) if entry else 0
                    side_raw = master_pos.get("side", "bid")
                    if abs(pnl) >= 10:
                        from bot.services.group_feed import post_leader_pnl
                        await post_leader_pnl(
                            bot, master_user_data_close["username"],
                            symbol, side_raw, pnl, pnl_pct,
                        )

            master_label = f"<code>{master_wallet[:8]}...</code>"
            master_user_data = await _get_user_by_wallet_cached(master_wallet)
            if master_user_data and master_user_data.get("username"):
                master_label = f"@{master_user_data['username']}"

            await bot.send_message(
                tg_id,
                f"<b>Copy Trade — Position Closed</b>\n\n"
                f"Master: {master_label}\n"
                f"{symbol} closed (size: {amount}){pnl_str}",
            )
        except Exception as e:
            logger.error("Copy close failed for user %s: %s", tg_id, e)
        finally:
            if client:
                await client.close()
