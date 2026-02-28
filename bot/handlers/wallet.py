"""
Wallet dashboard — balances, faucet, deposit, withdraw, export key, airdrop.
"""

import logging
import asyncio

from aiogram import Router, F
from aiogram.types import Message, CallbackQuery
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

from database.db import get_user, update_user
from bot.services.wallet_manager import decrypt_private_key
from bot.services.solana_client import (
    get_sol_balance,
    get_usdc_balance,
    request_faucet,
    deposit_to_pacifica,
    request_sol_airdrop,
    is_devnet,
    explorer_url,
    SolanaRPCError,
)
from bot.services.pacifica_client import PacificaAPIError
from bot.config import PACIFICA_NETWORK
from bot.utils.keyboards import (
    wallet_kb,
    wallet_deposit_kb,
    wallet_withdraw_kb,
    back_to_menu_kb,
    main_menu_kb,
)

logger = logging.getLogger(__name__)
router = Router()


# Cache of dead codes so we don't retry them every time
_dead_codes: set[str] = set()


async def _try_claim_beta(client, tg_id: int) -> bool:
    """Try claiming beta access. Tries DB codes first, then env pool."""
    from bot.config import BETA_CODE_POOL
    from database.db import get_active_beta_codes, increment_beta_code_uses, deactivate_beta_code

    # Build code list: DB codes (newest first) + env pool
    db_codes = await get_active_beta_codes()
    all_codes = db_codes + [c for c in BETA_CODE_POOL if c not in db_codes]

    for code in all_codes:
        if code in _dead_codes:
            continue
        try:
            await client.claim_beta_code(code)
            logger.info("Claimed beta code '%s' for user %s", code, tg_id)
            await increment_beta_code_uses(code)
            return True
        except Exception as e:
            err = str(e).lower()
            if "already" in err:
                return True  # User already has beta
            if "limit" in err or "not found" in err or "invalid code" in err:
                _dead_codes.add(code)
                await deactivate_beta_code(code)
                logger.info("Beta code '%s' is dead (deactivated)", code)
                continue
            if "invalid message" in err:
                # Signing error — not a dead code, but skip to next
                logger.warning("Signing error for code '%s': %s", code, e)
                continue
            logger.debug("Beta code '%s' failed for %s: %s", code, tg_id, e)
            continue
    logger.warning("All beta codes exhausted for user %s", tg_id)
    return False


async def ensure_beta_and_builder(user: dict) -> None:
    """Claim beta + approve builder code if not done yet.

    Single source of truth — called before trades and during onboarding.
    """
    if user.get("builder_approved"):
        return

    tg_id = user["telegram_id"]
    try:
        from bot.models.user import build_client_from_user
        from bot.config import BUILDER_CODE, BUILDER_FEE_RATE

        client = build_client_from_user(user)
        try:
            await _try_claim_beta(client, tg_id)

            if BUILDER_CODE:
                try:
                    await client.approve_builder_code(BUILDER_CODE, BUILDER_FEE_RATE)
                    await update_user(tg_id, builder_approved=1)
                    logger.info("Approved builder code for %s", tg_id)
                except Exception as e:
                    err = str(e).lower()
                    if "already" in err:
                        await update_user(tg_id, builder_approved=1)
                    elif "not found" not in err:
                        logger.debug("Builder approve failed: %s", e)
        finally:
            await client.close()
    except Exception as e:
        logger.debug("Beta/builder setup failed for %s: %s", tg_id, e)


async def _auto_claim_beta(tg_id: int):
    """Background task: claim beta code + approve builder code with retries."""
    for attempt in range(3):
        await asyncio.sleep(5 + attempt * 5)
        try:
            u = await get_user(tg_id)
            if not u:
                return
            await ensure_beta_and_builder(u)
            return
        except Exception as e:
            logger.debug("Auto-setup attempt %d for %s: %s", attempt + 1, tg_id, e)
    logger.warning("Auto-setup failed after 3 attempts for %s", tg_id)


class WalletStates(StatesGroup):
    waiting_deposit_amount = State()
    waiting_withdraw_amount = State()
    waiting_beta_code = State()


# ------------------------------------------------------------------
# Wallet dashboard
# ------------------------------------------------------------------

@router.callback_query(F.data == "nav:wallet")
async def nav_wallet(callback: CallbackQuery):
    """Show wallet dashboard with all balances."""
    await callback.answer("Loading wallet...")
    user = await get_user(callback.from_user.id)

    if not user or not user.get("pacifica_account"):
        await callback.message.edit_text(  # type: ignore
            "Set up your wallet first — /start",
            reply_markup=back_to_menu_kb(),
        )
        return

    wallet = user["pacifica_account"]
    short_wallet = f"{wallet[:6]}...{wallet[-4:]}"

    # Fetch balances in parallel
    sol_bal = 0.0
    usdc_bal = 0.0
    pac_bal = "—"

    try:
        sol_bal, usdc_bal = await asyncio.gather(
            get_sol_balance(wallet),
            get_usdc_balance(wallet),
        )
    except Exception as e:
        logger.debug("Balance fetch error: %s", e)

    # Pacifica balance from API
    try:
        from bot.models.user import build_client_from_user
        client = build_client_from_user(user)
        try:
            info = await client.get_account_info()
            pac_bal = f"${info.get('balance', '0')}"
        except PacificaAPIError as e:
            if "not found" in str(e).lower():
                pac_bal = "$0 (not deposited)"
            else:
                pac_bal = f"Error"
        finally:
            await client.close()
    except Exception:
        pac_bal = "—"

    text = (
        f"<b>Wallet</b>\n\n"
        f"Address: <code>{short_wallet}</code>\n"
        f"Network: <code>{PACIFICA_NETWORK}</code>\n\n"
        f"<b>Balances:</b>\n"
        f"  SOL: <b>{sol_bal:.4f} SOL</b>\n"
        f"  USDC (wallet): <b>{usdc_bal:,.2f}</b>\n"
        f"  USDC (Pacifica): <b>{pac_bal}</b>\n"
    )

    await callback.message.edit_text(  # type: ignore
        text,
        reply_markup=wallet_kb(sol_bal, usdc_bal),
    )


# ------------------------------------------------------------------
# Faucet (devnet only) — Mint 10K mock USDC
# ------------------------------------------------------------------

@router.callback_query(F.data == "wallet:faucet")
async def wallet_faucet(callback: CallbackQuery):
    if not is_devnet():
        await callback.answer("Faucet only available on devnet!", show_alert=True)
        return

    user = await get_user(callback.from_user.id)
    if not user or not user.get("agent_wallet_encrypted"):
        await callback.answer("Set up your wallet first! Use /start", show_alert=True)
        return

    await callback.answer("Minting 10K mock USDC...")

    try:
        keypair = decrypt_private_key(user["agent_wallet_encrypted"])
        sig = await request_faucet(keypair)

        url = explorer_url(sig)
        await callback.message.edit_text(  # type: ignore
            f"<b>Faucet Success!</b>\n\n"
            f"Minted <b>10,000 USDC</b> to your wallet.\n\n"
            f"Tx: <a href='{url}'>{sig[:16]}...</a>\n\n"
            f"You can now deposit USDC into Pacifica to start trading.",
            reply_markup=wallet_kb(0, 10000),
            disable_web_page_preview=True,
        )
    except SolanaRPCError as e:
        wallet = user.get("pacifica_account", "")
        acct_url = f"https://explorer.solana.com/address/{wallet}?cluster=devnet" if wallet else ""
        link = f"\n\n<a href='{acct_url}'>View wallet on Explorer</a>" if acct_url else ""
        await callback.message.edit_text(  # type: ignore
            f"<b>Faucet Failed</b>\n\n{e}{link}",
            reply_markup=wallet_kb(0, 0),
            disable_web_page_preview=True,
        )
    except Exception as e:
        logger.error("Faucet error: %s", e, exc_info=True)
        await callback.message.edit_text(  # type: ignore
            f"<b>Faucet Error</b>\n\n{e}",
            reply_markup=wallet_kb(0, 0),
        )


# ------------------------------------------------------------------
# SOL Faucet (devnet only)
# ------------------------------------------------------------------

@router.callback_query(F.data == "wallet:airdrop")
async def wallet_airdrop(callback: CallbackQuery):
    if not is_devnet():
        await callback.answer("Airdrop only available on devnet!", show_alert=True)
        return

    user = await get_user(callback.from_user.id)
    if not user or not user.get("pacifica_account"):
        await callback.answer("Set up your wallet first! Use /start", show_alert=True)
        return

    await callback.answer("Requesting 1 SOL from faucet...")

    try:
        sig = await request_sol_airdrop(user["pacifica_account"], 1.0)
        url = explorer_url(sig)
        await callback.message.edit_text(  # type: ignore
            f"<b>SOL Faucet Success!</b>\n\n"
            f"<b>1 SOL</b> requested from devnet faucet.\n\n"
            f"Tx: <a href='{url}'>{sig[:16]}...</a>\n\n"
            f"It may take a few seconds to arrive.",
            reply_markup=wallet_kb(1, 0),
            disable_web_page_preview=True,
        )
    except SolanaRPCError as e:
        await callback.message.edit_text(  # type: ignore
            f"<b>SOL Faucet Failed</b>\n\n{e}\n\n"
            f"Devnet faucets can be rate-limited. Try again later.",
            reply_markup=wallet_kb(0, 0),
        )
    except Exception as e:
        logger.error("Airdrop error: %s", e, exc_info=True)
        await callback.message.edit_text(  # type: ignore
            f"<b>SOL Faucet Error</b>\n\n{e}",
            reply_markup=wallet_kb(0, 0),
        )


# ------------------------------------------------------------------
# Deposit USDC into Pacifica
# ------------------------------------------------------------------

@router.callback_query(F.data == "wallet:deposit")
async def wallet_deposit(callback: CallbackQuery):
    await callback.answer()
    user = await get_user(callback.from_user.id)
    if not user or not user.get("pacifica_account"):
        await callback.answer("No wallet set up!", show_alert=True)
        return

    try:
        usdc_bal = await get_usdc_balance(user["pacifica_account"])
    except Exception:
        usdc_bal = 0.0

    if usdc_bal <= 0:
        text = (
            "<b>Deposit USDC</b>\n\n"
            "You don't have any USDC in your wallet.\n"
        )
        if is_devnet():
            text += "Use the faucet first to get 10K mock USDC!"
        await callback.message.edit_text(  # type: ignore
            text, reply_markup=wallet_kb(0, 0),
        )
        return

    await callback.message.edit_text(  # type: ignore
        f"<b>Deposit USDC into Pacifica</b>\n\n"
        f"Wallet USDC: <b>{usdc_bal:,.2f}</b>\n\n"
        f"Select amount to deposit:",
        reply_markup=wallet_deposit_kb(usdc_bal),
    )


@router.callback_query(F.data.startswith("dep:"))
async def wallet_deposit_exec(callback: CallbackQuery):
    """Execute deposit with selected amount."""
    amount_str = callback.data.split(":")[1]  # type: ignore
    await callback.answer(f"Depositing {amount_str} USDC...")

    user = await get_user(callback.from_user.id)
    if not user or not user.get("agent_wallet_encrypted"):
        await callback.answer("No wallet!", show_alert=True)
        return

    try:
        amount = float(amount_str)
    except ValueError:
        await callback.answer("Invalid amount", show_alert=True)
        return

    try:
        keypair = decrypt_private_key(user["agent_wallet_encrypted"])
        sig = await deposit_to_pacifica(keypair, amount)

        url = explorer_url(sig)

        # Launch background task to claim beta code + builder code with retries
        import asyncio
        tg_id = callback.from_user.id

        asyncio.create_task(_auto_claim_beta(callback.from_user.id))

        from bot.utils.keyboards import InlineKeyboardMarkup, InlineKeyboardButton
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="📊 Trade", callback_data="nav:markets"),
                InlineKeyboardButton(text="💳 Wallet", callback_data="nav:wallet"),
            ],
            [
                InlineKeyboardButton(text="🔑 Activate Beta", callback_data="wallet:claim_beta"),
                InlineKeyboardButton(text="◀️ Menu", callback_data="nav:menu"),
            ],
        ])

        await callback.message.edit_text(  # type: ignore
            f"<b>Deposit Submitted!</b>\n\n"
            f"Amount: <b>{amount:,.2f} USDC</b>\n\n"
            f"Tx: <a href='{url}'>{sig[:16]}...</a>\n\n"
            f"Beta code activation in progress...\n"
            f"If trading fails, tap <b>Activate Beta</b> below.",
            reply_markup=kb,
            disable_web_page_preview=True,
        )
    except SolanaRPCError as e:
        await callback.message.edit_text(  # type: ignore
            f"<b>Deposit Failed</b>\n\n{e}\n\n"
            f"Check that you have enough USDC and SOL for fees.",
            reply_markup=wallet_kb(0, 0),
        )
    except Exception as e:
        logger.error("Deposit error: %s", e, exc_info=True)
        await callback.message.edit_text(  # type: ignore
            f"<b>Deposit Error</b>\n\n{e}",
            reply_markup=wallet_kb(0, 0),
        )


@router.callback_query(F.data == "dep:custom")
async def wallet_deposit_custom(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.set_state(WalletStates.waiting_deposit_amount)
    await callback.message.edit_text(  # type: ignore
        "<b>Deposit USDC</b>\n\nType the amount to deposit (e.g. 2500):",
    )


@router.message(WalletStates.waiting_deposit_amount)
async def msg_deposit_amount(message: Message, state: FSMContext):
    raw = (message.text or "").strip().lstrip("$").replace(",", "")
    await state.clear()

    try:
        amount = float(raw)
        if amount <= 0:
            raise ValueError
    except (ValueError, TypeError):
        await message.answer("Invalid amount. Try again from the wallet menu.", reply_markup=main_menu_kb())
        return

    user = await get_user(message.from_user.id)  # type: ignore
    if not user or not user.get("agent_wallet_encrypted"):
        await message.answer("No wallet set up!", reply_markup=main_menu_kb())
        return

    try:
        keypair = decrypt_private_key(user["agent_wallet_encrypted"])
        sig = await deposit_to_pacifica(keypair, amount)

        url = explorer_url(sig)

        from bot.utils.keyboards import InlineKeyboardMarkup, InlineKeyboardButton
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="📊 Trade", callback_data="nav:markets"),
                InlineKeyboardButton(text="💳 Wallet", callback_data="nav:wallet"),
            ],
            [
                InlineKeyboardButton(text="🔑 Activate Beta", callback_data="wallet:claim_beta"),
                InlineKeyboardButton(text="◀️ Menu", callback_data="nav:menu"),
            ],
        ])

        await message.answer(
            f"<b>Deposit Submitted!</b>\n\n"
            f"Amount: <b>{amount:,.2f} USDC</b>\n\n"
            f"Tx: <a href='{url}'>{sig[:16]}...</a>\n\n"
            f"Beta code activation in progress...\n"
            f"If trading fails, tap <b>Activate Beta</b> below.",
            reply_markup=kb,
            disable_web_page_preview=True,
        )

        asyncio.create_task(_auto_claim_beta(message.from_user.id))
    except Exception as e:
        await message.answer(f"<b>Deposit Failed</b>\n\n{e}", reply_markup=main_menu_kb())


# ------------------------------------------------------------------
# Withdraw USDC from Pacifica
# ------------------------------------------------------------------

@router.callback_query(F.data == "wallet:withdraw")
async def wallet_withdraw(callback: CallbackQuery):
    await callback.answer()
    user = await get_user(callback.from_user.id)
    if not user or not user.get("pacifica_account"):
        await callback.answer("No wallet set up!", show_alert=True)
        return

    # Get Pacifica balance
    pac_bal = 0.0
    try:
        from bot.models.user import build_client_from_user
        client = build_client_from_user(user)
        try:
            info = await client.get_account_info()
            pac_bal = float(info.get("available_to_withdraw", 0) or 0)
        finally:
            await client.close()
    except Exception:
        pass

    if pac_bal <= 0:
        await callback.message.edit_text(  # type: ignore
            "<b>Withdraw USDC</b>\n\n"
            "No withdrawable balance on Pacifica.",
            reply_markup=wallet_kb(0, 0),
        )
        return

    await callback.message.edit_text(  # type: ignore
        f"<b>Withdraw from Pacifica</b>\n\n"
        f"Available: <b>${pac_bal:,.2f}</b>\n\n"
        f"Select amount to withdraw:",
        reply_markup=wallet_withdraw_kb(pac_bal),
    )


@router.callback_query(F.data.startswith("wdraw:"))
async def wallet_withdraw_exec(callback: CallbackQuery):
    """Execute withdraw via Pacifica REST API."""
    amount_str = callback.data.split(":")[1]  # type: ignore
    await callback.answer(f"Withdrawing {amount_str} USDC...")

    user = await get_user(callback.from_user.id)
    if not user or not user.get("agent_wallet_encrypted"):
        await callback.answer("No wallet!", show_alert=True)
        return

    try:
        amount = float(amount_str)
    except ValueError:
        await callback.answer("Invalid amount", show_alert=True)
        return

    try:
        from bot.models.user import build_client_from_user
        client = build_client_from_user(user)
        try:
            result = await client.request_withdraw(str(amount))
            await callback.message.edit_text(  # type: ignore
                f"<b>Withdraw Submitted!</b>\n\n"
                f"Amount: <b>{amount:,.2f} USDC</b>\n\n"
                f"Your withdrawal is being processed.",
                reply_markup=main_menu_kb(),
            )
        finally:
            await client.close()
    except Exception as e:
        logger.error("Withdraw error: %s", e, exc_info=True)
        await callback.message.edit_text(  # type: ignore
            f"<b>Withdraw Failed</b>\n\n{e}",
            reply_markup=wallet_kb(0, 0),
        )


@router.callback_query(F.data == "wdraw:custom")
async def wallet_withdraw_custom(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.set_state(WalletStates.waiting_withdraw_amount)
    await callback.message.edit_text(  # type: ignore
        "<b>Withdraw USDC</b>\n\nType the amount to withdraw (e.g. 500):",
    )


@router.message(WalletStates.waiting_withdraw_amount)
async def msg_withdraw_amount(message: Message, state: FSMContext):
    raw = (message.text or "").strip().lstrip("$").replace(",", "")
    await state.clear()

    try:
        amount = float(raw)
        if amount <= 0:
            raise ValueError
    except (ValueError, TypeError):
        await message.answer("Invalid amount.", reply_markup=main_menu_kb())
        return

    user = await get_user(message.from_user.id)  # type: ignore
    if not user or not user.get("agent_wallet_encrypted"):
        await message.answer("No wallet set up!", reply_markup=main_menu_kb())
        return

    try:
        from bot.models.user import build_client_from_user
        client = build_client_from_user(user)
        try:
            await client.request_withdraw(str(amount))
            await message.answer(
                f"<b>Withdraw Submitted!</b>\n\n"
                f"Amount: <b>{amount:,.2f} USDC</b>",
                reply_markup=main_menu_kb(),
            )
        finally:
            await client.close()
    except Exception as e:
        await message.answer(f"<b>Withdraw Failed</b>\n\n{e}", reply_markup=main_menu_kb())


# ------------------------------------------------------------------
# Claim Beta Code (manual)
# ------------------------------------------------------------------

@router.callback_query(F.data == "wallet:claim_beta")
async def wallet_claim_beta(callback: CallbackQuery, state: FSMContext):
    """Manually claim the Pacifica beta code + approve builder code."""
    user = await get_user(callback.from_user.id)
    if not user or not user.get("pacifica_account"):
        await callback.answer("Set up your wallet first!", show_alert=True)
        return

    await callback.answer("Claiming beta code...")

    from bot.models.user import build_client_from_user
    from bot.config import BUILDER_CODE, BUILDER_FEE_RATE
    from bot.utils.keyboards import InlineKeyboardMarkup, InlineKeyboardButton

    beta_claimed = False
    results = []

    try:
        client = build_client_from_user(user)
        try:
            # Try code pool
            beta_claimed = await _try_claim_beta(client, callback.from_user.id)
            if beta_claimed:
                results.append("✅ Beta access activated!")
            else:
                results.append("All codes exhausted")

            # Approve builder code
            if BUILDER_CODE:
                try:
                    await client.approve_builder_code(BUILDER_CODE, BUILDER_FEE_RATE)
                    await update_user(callback.from_user.id, builder_approved=1)
                    results.append(f"✅ Builder code <code>{BUILDER_CODE}</code> approved!")
                except Exception as e:
                    err = str(e).lower()
                    if "already" in err:
                        results.append("✅ Builder code already approved")
                        await update_user(callback.from_user.id, builder_approved=1)
                    elif "not found" in err:
                        results.append(f"⏳ Builder code <code>{BUILDER_CODE}</code> not registered on Pacifica yet")
                    else:
                        results.append(f"❌ Builder code: {str(e)}")
        finally:
            await client.close()
    except Exception as e:
        results.append(f"❌ Error: {e}")

    # If beta wasn't claimed, ask user for their own referral code
    if not beta_claimed:
        await state.set_state(WalletStates.waiting_beta_code)
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Cancel", callback_data="nav:menu")],
        ])
        await callback.message.edit_text(  # type: ignore
            "<b>Beta Activation</b>\n\n"
            "All built-in codes are used up.\n\n"
            "Please paste your Pacifica referral code below.\n"
            "Get one from the Pacifica Discord.",
            reply_markup=kb,
        )
        return

    footer = "You can trade now!"
    await callback.message.edit_text(  # type: ignore
        f"<b>Beta Activation</b>\n\n" + "\n".join(results) + f"\n\n{footer}",
        reply_markup=main_menu_kb(),
    )


@router.message(WalletStates.waiting_beta_code)
async def wallet_beta_code_input(message: Message, state: FSMContext):
    """User entered a custom beta/referral code."""
    code = (message.text or "").strip()
    await state.clear()

    if not code or len(code) < 2:
        await message.answer(
            "Invalid code. Try again via Settings > Activate Beta.",
            reply_markup=main_menu_kb(),
        )
        return

    user = await get_user(message.from_user.id)  # type: ignore
    if not user or not user.get("pacifica_account"):
        await message.answer("Set up your wallet first!", reply_markup=main_menu_kb())
        return

    from bot.models.user import build_client_from_user
    from bot.config import BUILDER_CODE, BUILDER_FEE_RATE

    results = []
    try:
        client = build_client_from_user(user)
        try:
            # Claim the user-provided code (tries whitelist then referral)
            await client.claim_beta_code(code)
            results.append(f"✅ Beta code <code>{code}</code> claimed!")

            # Now approve builder code
            try:
                await client.approve_builder_code(BUILDER_CODE, BUILDER_FEE_RATE)
                await update_user(message.from_user.id, builder_approved=1)  # type: ignore
                results.append(f"✅ Builder code <code>{BUILDER_CODE}</code> approved!")
            except Exception as e:
                err = str(e)
                if "already" in err.lower():
                    results.append("✅ Builder code already approved")
                    await update_user(message.from_user.id, builder_approved=1)  # type: ignore
                elif "not found" in err.lower():
                    results.append(f"⏳ Builder code <code>{BUILDER_CODE}</code> not registered on Pacifica yet")
                else:
                    results.append(f"❌ Builder code: {err}")
        finally:
            await client.close()
    except Exception as e:
        err = str(e)
        if "already" in err.lower():
            results.append("✅ Beta code already claimed")
        else:
            results.append(f"❌ Code <code>{code}</code> failed: {err}")
            results.append("\nCheck the code and try again via Settings > Activate Beta.")

    await message.answer(
        "<b>Beta Activation</b>\n\n" + "\n".join(results),
        reply_markup=main_menu_kb(),
    )


# ------------------------------------------------------------------
# Export Private Key
# ------------------------------------------------------------------

@router.callback_query(F.data == "wallet:export")
async def wallet_export_confirm(callback: CallbackQuery):
    """Show warning before revealing private key."""
    await callback.answer()
    from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Yes, show my key", callback_data="wallet:export_yes")],
        [InlineKeyboardButton(text="Cancel", callback_data="nav:wallet")],
    ])
    await callback.message.edit_text(  # type: ignore
        "<b>Export Private Key</b>\n\n"
        "Your private key will be shown in chat.\n"
        "It will be automatically deleted after 30 seconds.\n\n"
        "<b>Never share your private key with anyone!</b>",
        reply_markup=kb,
    )


@router.callback_query(F.data == "wallet:export_yes")
async def wallet_export_reveal(callback: CallbackQuery):
    """Reveal the private key, then auto-delete after 30s."""
    await callback.answer()
    user = await get_user(callback.from_user.id)

    if not user or not user.get("agent_wallet_encrypted"):
        await callback.answer("No wallet!", show_alert=True)
        return

    keypair = decrypt_private_key(user["agent_wallet_encrypted"])
    import base58
    privkey_b58 = base58.b58encode(bytes(keypair)).decode("ascii")

    # Send as a new message (so we can delete it later)
    msg = await callback.message.answer(  # type: ignore
        f"<b>Your Private Key</b>\n\n"
        f"<code>{privkey_b58}</code>\n\n"
        f"This message will self-destruct in 30 seconds.",
    )

    # Edit the original message
    await callback.message.edit_text(  # type: ignore
        "Key revealed above. Deleting in 30s...",
        reply_markup=main_menu_kb(),
    )

    # Auto-delete after 30 seconds
    async def _delete_later():
        await asyncio.sleep(30)
        try:
            await msg.delete()
        except Exception:
            pass

    asyncio.create_task(_delete_later())


# ------------------------------------------------------------------
# Refresh wallet
# ------------------------------------------------------------------

@router.callback_query(F.data == "wallet:refresh")
async def wallet_refresh(callback: CallbackQuery):
    """Refresh = re-render the wallet dashboard."""
    await nav_wallet(callback)
