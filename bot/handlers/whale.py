"""
Whale tracking & alerts handlers.

Commands:
  /track <address>   — Track a wallet
  /untrack <address>  — Stop tracking a wallet
  /tracked            — List tracked wallets
  /whales on|off      — Enable/disable whale alerts
  /lookup <address>   — View a wallet's stats & recent activity
"""

import logging

from aiogram import Router, types
from aiogram.filters import Command

from bot.services.whale_monitor import (
    add_tracked_wallet,
    remove_tracked_wallet,
    get_tracked_wallets,
    set_whale_alerts,
    get_last_snapshot,
    get_wallet_history,
)
from bot.services.market_data import _get_client

logger = logging.getLogger(__name__)
router = Router()


def _fmt_usd(val: float) -> str:
    abs_val = abs(val)
    if abs_val >= 1_000_000:
        return f"{'−' if val < 0 else ''}${abs_val / 1_000_000:.2f}M"
    if abs_val >= 1_000:
        return f"{'−' if val < 0 else ''}${abs_val / 1_000:.1f}K"
    return f"{'−' if val < 0 else ''}${abs_val:.0f}"


def _short(addr: str) -> str:
    if len(addr) <= 12:
        return addr
    return f"{addr[:6]}...{addr[-4:]}"


# ------------------------------------------------------------------
# /track <address> [label]
# ------------------------------------------------------------------

@router.message(Command("track"))
async def cmd_track(msg: types.Message):
    parts = (msg.text or "").split(maxsplit=2)
    if len(parts) < 2:
        await msg.answer(
            "Usage: <code>/track &lt;wallet_address&gt; [label]</code>\n\n"
            "Example: <code>/track 7xKXt...3fPq whale_joe</code>"
        )
        return

    address = parts[1].strip()
    label = parts[2].strip() if len(parts) > 2 else None

    if len(address) < 20:
        await msg.answer("That doesn't look like a valid Solana address.")
        return

    added = await add_tracked_wallet(msg.from_user.id, address, label)
    if added:
        await msg.answer(
            f"✅ Now tracking <b>{label or _short(address)}</b>\n\n"
            f"<code>{address}</code>\n\n"
            f"You'll get notified when this wallet opens or closes large positions."
        )
    else:
        await msg.answer(f"You're already tracking {_short(address)}.")


# ------------------------------------------------------------------
# /untrack <address>
# ------------------------------------------------------------------

@router.message(Command("untrack"))
async def cmd_untrack(msg: types.Message):
    parts = (msg.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await msg.answer("Usage: <code>/untrack &lt;wallet_address&gt;</code>")
        return

    address = parts[1].strip()
    removed = await remove_tracked_wallet(msg.from_user.id, address)
    if removed:
        await msg.answer(f"❌ Stopped tracking {_short(address)}")
    else:
        await msg.answer(f"You weren't tracking {_short(address)}.")


# ------------------------------------------------------------------
# /tracked — list tracked wallets
# ------------------------------------------------------------------

@router.message(Command("tracked"))
async def cmd_tracked(msg: types.Message):
    wallets = await get_tracked_wallets(msg.from_user.id)
    if not wallets:
        await msg.answer(
            "You're not tracking any wallets yet.\n\n"
            "Use <code>/track &lt;address&gt;</code> to start."
        )
        return

    lines = ["🔔 <b>Tracked Wallets</b>\n"]
    for w in wallets:
        addr = w["wallet_address"]
        label = w.get("label") or _short(addr)
        snap = await get_last_snapshot(addr)
        if snap:
            pnl = snap.get("pnl_all_time", 0)
            equity = snap.get("equity", 0)
            oi = snap.get("oi", 0)
            lines.append(
                f"• <b>{label}</b>\n"
                f"  PnL: {_fmt_usd(pnl)} | Eq: {_fmt_usd(equity)} | OI: {_fmt_usd(oi)}\n"
                f"  <code>{addr}</code>"
            )
        else:
            lines.append(f"• <b>{label}</b>\n  <code>{addr}</code>\n  <i>No data yet</i>")

    await msg.answer("\n".join(lines))


# ------------------------------------------------------------------
# /whales on|off — toggle whale alerts
# ------------------------------------------------------------------

@router.message(Command("whales"))
async def cmd_whales(msg: types.Message):
    parts = (msg.text or "").split(maxsplit=1)
    if len(parts) < 2 or parts[1].strip().lower() not in ("on", "off"):
        await msg.answer(
            "🐋 <b>Whale Alerts</b>\n\n"
            "Get notified when whales ($100K+ equity) open or close large positions.\n\n"
            "<code>/whales on</code>  — Enable alerts\n"
            "<code>/whales off</code> — Disable alerts"
        )
        return

    enabled = parts[1].strip().lower() == "on"
    await set_whale_alerts(msg.from_user.id, enabled)
    if enabled:
        await msg.answer("🐋 Whale alerts <b>enabled</b>. You'll get notified of big moves.")
    else:
        await msg.answer("🐋 Whale alerts <b>disabled</b>.")


# ------------------------------------------------------------------
# /lookup <address> — view wallet stats
# ------------------------------------------------------------------

@router.message(Command("lookup"))
async def cmd_lookup(msg: types.Message):
    parts = (msg.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await msg.answer("Usage: <code>/lookup &lt;wallet_address&gt;</code>")
        return

    address = parts[1].strip()

    # Try leaderboard data
    try:
        client = await _get_client()
        entries = await client.get_leaderboard(limit=100)
        trader = next((e for e in entries if e.get("address") == address), None)
    except Exception:
        trader = None

    if not trader:
        # Check snapshots
        snap = await get_last_snapshot(address)
        if snap:
            await msg.answer(
                f"📊 <b>Wallet {_short(address)}</b>\n\n"
                f"Last seen data:\n"
                f"PnL (all-time): <b>{_fmt_usd(snap.get('pnl_all_time', 0))}</b>\n"
                f"Equity: <b>{_fmt_usd(snap.get('equity', 0))}</b>\n"
                f"Open Interest: <b>{_fmt_usd(snap.get('oi', 0))}</b>\n\n"
                f"<code>{address}</code>"
            )
        else:
            await msg.answer(
                f"Wallet {_short(address)} not found on the leaderboard.\n"
                f"Only top 100 traders are tracked."
            )
        return

    username = trader.get("username") or _short(address)
    pnl_all = float(trader.get("pnl_all_time", 0))
    pnl_30d = float(trader.get("pnl_30d", 0))
    pnl_7d = float(trader.get("pnl_7d", 0))
    pnl_1d = float(trader.get("pnl_1d", 0))
    equity = float(trader.get("equity_current", 0))
    oi = float(trader.get("oi_current", 0))
    vol_all = float(trader.get("volume_all_time", 0))
    vol_30d = float(trader.get("volume_30d", 0))

    # Rank
    sorted_entries = sorted(entries, key=lambda x: float(x.get("pnl_all_time", 0)), reverse=True)
    rank = next((i + 1 for i, e in enumerate(sorted_entries) if e.get("address") == address), "?")

    pnl_color = "🟢" if pnl_all >= 0 else "🔴"

    text = (
        f"📊 <b>{username}</b>  #{rank}\n"
        f"<code>{address}</code>\n\n"
        f"💰 Equity: <b>{_fmt_usd(equity)}</b>\n"
        f"📊 Open Interest: <b>{_fmt_usd(oi)}</b>\n\n"
        f"<b>PnL</b>\n"
        f"  All-time: {pnl_color} <b>{_fmt_usd(pnl_all)}</b>\n"
        f"  30D: {_fmt_usd(pnl_30d)}\n"
        f"  7D: {_fmt_usd(pnl_7d)}\n"
        f"  24h: {_fmt_usd(pnl_1d)}\n\n"
        f"<b>Volume</b>\n"
        f"  All-time: {_fmt_usd(vol_all)}\n"
        f"  30D: {_fmt_usd(vol_30d)}\n\n"
        f"Track: <code>/track {address}</code>"
    )

    await msg.answer(text)


# ------------------------------------------------------------------
# /setcode <code> — add a beta code at runtime (admin only)
# ------------------------------------------------------------------

@router.message(Command("setcode"))
async def cmd_setcode(msg: types.Message):
    from bot.config import ADMIN_IDS
    if msg.from_user.id not in ADMIN_IDS:
        await msg.answer("Admin only.")
        return

    parts = (msg.text or "").split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        await msg.answer(
            "Usage: <code>/setcode YOUR_BETA_CODE</code>\n\n"
            "Adds a beta/referral code that will be used for new users automatically.\n"
            "No redeploy needed."
        )
        return

    code = parts[1].strip()

    from database.db import add_beta_code
    added = await add_beta_code(code, added_by=msg.from_user.id)

    if added:
        # Clear from dead codes cache if it was there
        from bot.handlers.wallet import _dead_codes
        _dead_codes.discard(code)

        await msg.answer(
            f"✅ Beta code <code>{code}</code> added!\n\n"
            f"It will be used first for new users. No redeploy needed."
        )
    else:
        await msg.answer(f"Code <code>{code}</code> already exists.")


# ------------------------------------------------------------------
# /codes — list all beta codes (admin only)
# ------------------------------------------------------------------

@router.message(Command("codes"))
async def cmd_codes(msg: types.Message):
    from bot.config import ADMIN_IDS
    if msg.from_user.id not in ADMIN_IDS:
        await msg.answer("Admin only.")
        return

    from database.db import get_all_beta_codes
    from bot.config import BETA_CODE_POOL

    db_codes = await get_all_beta_codes()

    lines = ["🔑 <b>Beta Codes</b>\n"]

    if db_codes:
        lines.append("<b>Runtime (DB):</b>")
        for c in db_codes:
            status = "✅" if c["active"] else "❌"
            lines.append(
                f"  {status} <code>{c['code']}</code> — {c['uses']} uses"
            )

    lines.append(f"\n<b>Env pool ({len(BETA_CODE_POOL)}):</b>")
    from bot.handlers.wallet import _dead_codes
    for code in BETA_CODE_POOL:
        status = "❌" if code in _dead_codes else "✅"
        lines.append(f"  {status} <code>{code}</code>")

    lines.append(f"\n<i>/setcode CODE to add a new code</i>")
    await msg.answer("\n".join(lines))
