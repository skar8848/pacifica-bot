"""
Solana on-chain client — faucet, deposit, withdraw, balance queries.

Handles direct Solana transactions for Pacifica's devnet program:
- Faucet: mint 10K mock USDC
- Deposit: transfer USDC from wallet into Pacifica
- Withdraw: pull USDC from Pacifica back to wallet
- Balance: SOL + USDC balance queries
- Airdrop: devnet SOL airdrop
"""

import struct
import logging
import base64
from typing import Any

import aiohttp
from solders.pubkey import Pubkey  # type: ignore
from solders.keypair import Keypair  # type: ignore
from solders.instruction import Instruction, AccountMeta  # type: ignore
from solders.transaction import Transaction  # type: ignore
from solders.message import Message  # type: ignore
from solders.hash import Hash  # type: ignore

from bot.config import SOLANA_RPC_URL, PACIFICA_NETWORK

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# On-chain constants (Pacifica devnet / testnet)
# ---------------------------------------------------------------------------

PACIFICA_PROGRAM = Pubkey.from_string("peRPsYCcB1J9jvrs29jiGdjkytxs8uHLmSPLKKP9ptm")
USDC_MINT = Pubkey.from_string("USDPqRbLidFGufty2s3oizmDEKdqx7ePTqzDMbf5ZKM")
MINT_AUTHORITY = Pubkey.from_string("2zPRq1Qvdq5A4Ld6WsH7usgCge4ApZRYfhhf5VAjfXxv")

TOKEN_PROGRAM = Pubkey.from_string("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA")
ASSOC_TOKEN_PROGRAM = Pubkey.from_string("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL")
SYS_PROGRAM = Pubkey.from_string("11111111111111111111111111111111")

# Anchor instruction discriminators (first 8 bytes)
FAUCET_DISC = bytes.fromhex("76904e769bd6b9ba")
DEPOSIT_DISC = bytes.fromhex("f223c68952e1f2b6")
WITHDRAW_DISC = bytes.fromhex("254c95475e24f5c3")

USDC_DECIMALS = 6


# ---------------------------------------------------------------------------
# PDA / ATA derivation
# ---------------------------------------------------------------------------

def get_ata(owner: Pubkey, mint: Pubkey) -> Pubkey:
    """Derive the Associated Token Account address."""
    ata, _bump = Pubkey.find_program_address(
        [bytes(owner), bytes(TOKEN_PROGRAM), bytes(mint)],
        ASSOC_TOKEN_PROGRAM,
    )
    return ata


def get_user_account_pda(user: Pubkey) -> Pubkey:
    """Derive the Pacifica user account PDA."""
    pda, _bump = Pubkey.find_program_address(
        [b"user_account", bytes(user)],
        PACIFICA_PROGRAM,
    )
    return pda


def get_event_authority() -> Pubkey:
    """Derive the Anchor event authority PDA."""
    pda, _bump = Pubkey.find_program_address(
        [b"__event_authority"],
        PACIFICA_PROGRAM,
    )
    return pda


def get_vault_ata() -> Pubkey:
    """Derive the vault ATA (USDC token account held by the mint authority PDA)."""
    return get_ata(MINT_AUTHORITY, USDC_MINT)


# ---------------------------------------------------------------------------
# Solana JSON-RPC helpers
# ---------------------------------------------------------------------------

async def _rpc(method: str, params: list | None = None) -> Any:
    """Low-level JSON-RPC call to Solana."""
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": params or [],
    }
    async with aiohttp.ClientSession() as session:
        async with session.post(
            SOLANA_RPC_URL,
            json=payload,
            headers={"Content-Type": "application/json"},
        ) as resp:
            data = await resp.json()

    if "error" in data:
        raise SolanaRPCError(data["error"])
    return data.get("result")


async def get_sol_balance(pubkey: str) -> float:
    """Return SOL balance for an address."""
    result = await _rpc("getBalance", [pubkey])
    return result.get("value", 0) / 1_000_000_000


async def get_usdc_balance(owner: str) -> float:
    """Return USDC balance for an owner (derives ATA automatically)."""
    ata = get_ata(Pubkey.from_string(owner), USDC_MINT)
    try:
        result = await _rpc("getTokenAccountBalance", [str(ata)])
        if result and result.get("value"):
            return float(result["value"].get("uiAmount", 0) or 0)
    except SolanaRPCError:
        pass  # ATA doesn't exist → 0 balance
    return 0.0


async def get_latest_blockhash() -> str:
    """Get a recent blockhash for transaction construction."""
    result = await _rpc("getLatestBlockhash")
    return result["value"]["blockhash"]


async def send_tx(tx_bytes: bytes) -> str:
    """Send a signed transaction. Returns signature string."""
    tx_b64 = base64.b64encode(tx_bytes).decode("ascii")
    sig = await _rpc(
        "sendTransaction",
        [tx_b64, {"encoding": "base64", "skipPreflight": False}],
    )
    return sig


async def confirm_tx(signature: str, timeout: int = 30) -> bool:
    """Poll for transaction confirmation."""
    import asyncio
    for _ in range(timeout):
        try:
            result = await _rpc("getSignatureStatuses", [[signature]])
            statuses = result.get("value", [])
            if statuses and statuses[0]:
                status = statuses[0]
                if status.get("confirmationStatus") in ("confirmed", "finalized"):
                    return True
                if status.get("err"):
                    logger.error("Tx %s failed: %s", signature, status["err"])
                    return False
        except Exception:
            pass
        await asyncio.sleep(1)
    return False


# ---------------------------------------------------------------------------
# Transaction builders
# ---------------------------------------------------------------------------

def _build_and_sign(keypair: Keypair, instructions: list[Instruction], blockhash: str) -> bytes:
    """Build, sign, and serialize a legacy transaction."""
    payer = keypair.pubkey()
    bh = Hash.from_string(blockhash)
    msg = Message.new_with_blockhash(instructions, payer, bh)
    tx = Transaction.new_unsigned(msg)
    tx.sign([keypair], bh)
    return bytes(tx)


def _create_ata_idempotent_ix(payer: Pubkey, owner: Pubkey, mint: Pubkey) -> Instruction:
    """Build a createAssociatedTokenAccountIdempotent instruction.

    This creates the ATA if it doesn't exist, or does nothing if it already exists.
    Instruction index 1 = idempotent variant (won't fail if account exists).
    """
    ata = get_ata(owner, mint)
    accounts = [
        AccountMeta(payer, is_signer=True, is_writable=True),
        AccountMeta(ata, is_signer=False, is_writable=True),
        AccountMeta(owner, is_signer=False, is_writable=False),
        AccountMeta(mint, is_signer=False, is_writable=False),
        AccountMeta(SYS_PROGRAM, is_signer=False, is_writable=False),
        AccountMeta(TOKEN_PROGRAM, is_signer=False, is_writable=False),
    ]
    # Instruction data: single byte 0x01 = CreateIdempotent
    return Instruction(ASSOC_TOKEN_PROGRAM, bytes([1]), accounts)


async def request_faucet(keypair: Keypair) -> str:
    """Mint 10,000 mock USDC via Pacifica's devnet faucet.

    Prepends a createAssociatedTokenAccountIdempotent instruction
    to ensure the user's USDC ATA exists before the faucet mints into it.

    Faucet instruction: discriminator only (no args), 8 accounts.
    Account layout (verified from on-chain txs):
      [0] user (signer, writable)
      [1] user_account_pda (writable) — PDA([b"user_account", user], program)
      [2] user_usdc_ata (writable, receives minted tokens)
      [3] usdc_mint (writable, supply changes)
      [4] mint_authority
      [5] associated_token_program
      [6] token_program
      [7] system_program
    """
    user = keypair.pubkey()
    user_account = get_user_account_pda(user)
    user_ata = get_ata(user, USDC_MINT)

    # 1) Create ATA if it doesn't exist (idempotent — safe to call even if exists)
    create_ata_ix = _create_ata_idempotent_ix(user, user, USDC_MINT)

    # 2) Faucet instruction
    faucet_accounts = [
        AccountMeta(user, is_signer=True, is_writable=True),
        AccountMeta(user_account, is_signer=False, is_writable=True),
        AccountMeta(user_ata, is_signer=False, is_writable=True),
        AccountMeta(USDC_MINT, is_signer=False, is_writable=True),
        AccountMeta(MINT_AUTHORITY, is_signer=False, is_writable=False),
        AccountMeta(ASSOC_TOKEN_PROGRAM, is_signer=False, is_writable=False),
        AccountMeta(TOKEN_PROGRAM, is_signer=False, is_writable=False),
        AccountMeta(SYS_PROGRAM, is_signer=False, is_writable=False),
    ]
    faucet_ix = Instruction(PACIFICA_PROGRAM, FAUCET_DISC, faucet_accounts)

    blockhash = await get_latest_blockhash()
    raw = _build_and_sign(keypair, [create_ata_ix, faucet_ix], blockhash)

    sig = await send_tx(raw)
    logger.info("Faucet tx: %s", sig)
    return sig


async def deposit_to_pacifica(keypair: Keypair, amount_usdc: float) -> str:
    """Deposit USDC from wallet into Pacifica exchange.

    Instruction: discriminator + u64 amount (6 decimals), 10 accounts.
    Account layout (verified from on-chain deposit tx):
      [0] user (signer, writable)
      [1] user_usdc_ata (writable, source of transfer)
      [2] mint_authority (writable, stores exchange state)
      [3] vault_ata (writable, destination of transfer)
      [4] token_program
      [5] associated_token_program
      [6] usdc_mint
      [7] system_program
      [8] event_authority (Anchor CPI event)
      [9] self_program
    """
    user = keypair.pubkey()
    user_ata = get_ata(user, USDC_MINT)
    vault = get_vault_ata()
    event_auth = get_event_authority()

    amount_raw = int(amount_usdc * 10**USDC_DECIMALS)
    ix_data = DEPOSIT_DISC + struct.pack("<Q", amount_raw)

    # Ensure user ATA exists before deposit
    create_ata_ix = _create_ata_idempotent_ix(user, user, USDC_MINT)

    deposit_accounts = [
        AccountMeta(user, is_signer=True, is_writable=True),
        AccountMeta(user_ata, is_signer=False, is_writable=True),
        AccountMeta(MINT_AUTHORITY, is_signer=False, is_writable=True),
        AccountMeta(vault, is_signer=False, is_writable=True),
        AccountMeta(TOKEN_PROGRAM, is_signer=False, is_writable=False),
        AccountMeta(ASSOC_TOKEN_PROGRAM, is_signer=False, is_writable=False),
        AccountMeta(USDC_MINT, is_signer=False, is_writable=False),
        AccountMeta(SYS_PROGRAM, is_signer=False, is_writable=False),
        AccountMeta(event_auth, is_signer=False, is_writable=False),
        AccountMeta(PACIFICA_PROGRAM, is_signer=False, is_writable=False),
    ]

    deposit_ix = Instruction(PACIFICA_PROGRAM, ix_data, deposit_accounts)
    blockhash = await get_latest_blockhash()
    raw = _build_and_sign(keypair, [create_ata_ix, deposit_ix], blockhash)

    sig = await send_tx(raw)
    logger.info("Deposit tx: %s (%.2f USDC)", sig, amount_usdc)
    return sig


async def withdraw_from_pacifica(keypair: Keypair, amount_usdc: float) -> str:
    """Withdraw USDC from Pacifica back to wallet.

    Uses the withdraw discriminator. Account layout mirrors deposit
    but with 11 accounts (includes mint_authority twice: as state and as vault auth).
    Note: This layout is best-guess based on the deposit pattern.
    """
    user = keypair.pubkey()
    user_ata = get_ata(user, USDC_MINT)
    vault = get_vault_ata()
    event_auth = get_event_authority()

    amount_raw = int(amount_usdc * 10**USDC_DECIMALS)
    ix_data = WITHDRAW_DISC + struct.pack("<Q", amount_raw)

    accounts = [
        AccountMeta(user, is_signer=True, is_writable=True),
        AccountMeta(user_ata, is_signer=False, is_writable=True),
        AccountMeta(MINT_AUTHORITY, is_signer=False, is_writable=True),
        AccountMeta(vault, is_signer=False, is_writable=True),
        AccountMeta(TOKEN_PROGRAM, is_signer=False, is_writable=False),
        AccountMeta(ASSOC_TOKEN_PROGRAM, is_signer=False, is_writable=False),
        AccountMeta(USDC_MINT, is_signer=False, is_writable=False),
        AccountMeta(SYS_PROGRAM, is_signer=False, is_writable=False),
        AccountMeta(event_auth, is_signer=False, is_writable=False),
        AccountMeta(PACIFICA_PROGRAM, is_signer=False, is_writable=False),
    ]

    ix = Instruction(PACIFICA_PROGRAM, ix_data, accounts)
    blockhash = await get_latest_blockhash()
    raw = _build_and_sign(keypair, [ix], blockhash)

    sig = await send_tx(raw)
    logger.info("Withdraw tx: %s (%.2f USDC)", sig, amount_usdc)
    return sig


async def send_sol(from_keypair: Keypair, to_pubkey: str, amount_sol: float) -> str:
    """Send SOL from one wallet to another. Returns signature."""
    from_pub = from_keypair.pubkey()
    to_pub = Pubkey.from_string(to_pubkey)
    lamports = int(amount_sol * 1_000_000_000)

    # Build a system transfer instruction (index=2, then u64 lamports)
    ix = Instruction(
        SYS_PROGRAM,
        struct.pack("<IQ", 2, lamports),
        [
            AccountMeta(from_pub, is_signer=True, is_writable=True),
            AccountMeta(to_pub, is_signer=False, is_writable=True),
        ],
    )

    blockhash = await get_latest_blockhash()
    raw = _build_and_sign(from_keypair, [ix], blockhash)
    sig = await send_tx(raw)
    logger.info("SOL transfer: %s -> %s (%.4f SOL) tx=%s", from_pub, to_pubkey, amount_sol, sig)
    return sig


async def request_sol_airdrop(pubkey: str, amount_sol: float = 1.0) -> str:
    """Request SOL airdrop on devnet. Returns signature."""
    lamports = int(amount_sol * 1_000_000_000)
    sig = await _rpc("requestAirdrop", [pubkey, lamports])
    return sig


def is_devnet() -> bool:
    """Check if we're running on devnet (for enabling faucet/airdrop)."""
    return PACIFICA_NETWORK in ("devnet", "testnet")


# ---------------------------------------------------------------------------
# Explorer URL helper
# ---------------------------------------------------------------------------

def explorer_url(signature: str) -> str:
    """Build a Solana Explorer URL for a transaction."""
    cluster = "devnet" if is_devnet() else "mainnet-beta"
    return f"https://explorer.solana.com/tx/{signature}?cluster={cluster}"


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

# Known Pacifica program errors
PACIFICA_ERRORS = {
    0x66: "Faucet cooldown — try again in a few minutes",
    0x1: "Insufficient funds",
    0x0: "Generic program error",
}


class SolanaRPCError(Exception):
    def __init__(self, error: dict | str):
        if isinstance(error, dict):
            self.code = error.get("code", -1)
            msg = error.get("message", str(error))
            # Parse custom program errors for better messages
            data = error.get("data", {})
            if isinstance(data, dict):
                logs = data.get("logs", [])
                # Extract custom program error code
                for log in logs:
                    if "custom program error:" in str(log):
                        try:
                            hex_code = str(log).split("custom program error: ")[1].strip()
                            err_code = int(hex_code, 16)
                            friendly = PACIFICA_ERRORS.get(err_code, f"Program error {hex_code}")
                            msg = friendly
                            break
                        except (ValueError, IndexError):
                            pass
        else:
            self.code = -1
            msg = str(error)
        super().__init__(msg)
