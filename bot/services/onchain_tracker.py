"""
On-chain whale tracker — monitors Solana transactions for watched wallets
and alerts users when significant transfers/trades are detected.

Uses Solana RPC getSignaturesForAddress + getTransaction to detect:
- Large USDC transfers (deposits/withdrawals to Pacifica)
- Position changes via Pacifica program interactions
"""

import asyncio
import logging
import time
from typing import Any

import aiohttp
from aiogram import Bot

from bot.config import SOLANA_RPC_URL
from database.db import get_all_onchain_addresses

logger = logging.getLogger(__name__)

_running = False
CHECK_INTERVAL = 30  # seconds
_last_signatures: dict[str, str] = {}  # wallet -> last seen signature

# USDC mint on Solana mainnet
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"


async def start_onchain_tracker(bot: Bot):
    global _running
    _running = True
    logger.info("On-chain whale tracker started (check every %ss)", CHECK_INTERVAL)

    while _running:
        try:
            await _check_watched_wallets(bot)
        except Exception as e:
            logger.error("On-chain tracker error: %s", e)
        await asyncio.sleep(CHECK_INTERVAL)


def stop_onchain_tracker():
    global _running
    _running = False


async def _solana_rpc(method: str, params: list) -> Any:
    """Make a Solana JSON-RPC call."""
    async with aiohttp.ClientSession() as session:
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params,
        }
        async with session.post(SOLANA_RPC_URL, json=payload) as resp:
            data = await resp.json()
            if "error" in data:
                logger.debug("RPC error: %s", data["error"])
                return None
            return data.get("result")


async def _get_recent_signatures(wallet: str, limit: int = 5) -> list[dict]:
    """Get recent transaction signatures for a wallet."""
    result = await _solana_rpc(
        "getSignaturesForAddress",
        [wallet, {"limit": limit, "commitment": "confirmed"}],
    )
    return result or []


async def _get_transaction(signature: str) -> dict | None:
    """Get full transaction details."""
    result = await _solana_rpc(
        "getTransaction",
        [signature, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}],
    )
    return result


def _extract_transfer_info(tx: dict) -> dict | None:
    """Extract USDC transfer info from a parsed transaction."""
    if not tx:
        return None

    meta = tx.get("meta")
    if not meta or meta.get("err"):
        return None

    # Check pre/post token balances for USDC changes
    pre_balances = meta.get("preTokenBalances", [])
    post_balances = meta.get("postTokenBalances", [])

    usdc_changes = []
    for post in post_balances:
        if post.get("mint") != USDC_MINT:
            continue
        owner = post.get("owner", "")
        post_amount = float(post.get("uiTokenAmount", {}).get("uiAmount", 0) or 0)

        pre_amount = 0
        for pre in pre_balances:
            if pre.get("mint") == USDC_MINT and pre.get("owner") == owner:
                pre_amount = float(pre.get("uiTokenAmount", {}).get("uiAmount", 0) or 0)
                break

        change = post_amount - pre_amount
        if abs(change) > 0:
            usdc_changes.append({"owner": owner, "change": change})

    if not usdc_changes:
        return None

    # Find the largest USDC change
    largest = max(usdc_changes, key=lambda x: abs(x["change"]))
    return {
        "owner": largest["owner"],
        "usdc_change": largest["change"],
        "signature": tx.get("transaction", {}).get("signatures", [""])[0],
        "block_time": tx.get("blockTime", 0),
    }


async def _check_watched_wallets(bot: Bot):
    """Check all watched wallets for new transactions."""
    watches = await get_all_onchain_addresses()
    if not watches:
        return

    for wallet, subscribers in watches.items():
        try:
            sigs = await _get_recent_signatures(wallet, limit=3)
            if not sigs:
                continue

            last_seen = _last_signatures.get(wallet)
            new_sigs = []

            for sig_info in sigs:
                sig = sig_info.get("signature", "")
                if sig == last_seen:
                    break
                new_sigs.append(sig_info)

            if sigs:
                _last_signatures[wallet] = sigs[0].get("signature", "")

            if not last_seen or not new_sigs:
                continue

            for sig_info in new_sigs:
                sig = sig_info.get("signature", "")
                try:
                    tx = await _get_transaction(sig)
                    if not tx:
                        continue

                    transfer = _extract_transfer_info(tx)
                    if not transfer:
                        continue

                    usdc = transfer["usdc_change"]
                    abs_usdc = abs(usdc)

                    for tg_id, min_tx in subscribers:
                        if abs_usdc < min_tx:
                            continue

                        direction = "received" if usdc > 0 else "sent"
                        emoji = "🟢" if usdc > 0 else "🔴"
                        short_wallet = f"{wallet[:6]}...{wallet[-4:]}"
                        short_sig = f"{sig[:8]}...{sig[-4:]}"

                        text = (
                            f"<b>On-Chain Alert</b>\n\n"
                            f"{emoji} <b>{short_wallet}</b>\n"
                            f"{direction} <code>${abs_usdc:,.2f}</code> USDC\n\n"
                            f"Tx: <code>{short_sig}</code>\n"
                            f"<a href='https://solscan.io/tx/{sig}'>View on Solscan</a>"
                        )

                        try:
                            await bot.send_message(tg_id, text, disable_web_page_preview=True)
                        except Exception:
                            pass

                except Exception as e:
                    logger.debug("Failed to process tx %s: %s", sig[:16], e)

            # Small delay between wallets to respect rate limits
            await asyncio.sleep(0.5)

        except Exception as e:
            logger.debug("On-chain check failed for %s: %s", wallet[:8], e)
