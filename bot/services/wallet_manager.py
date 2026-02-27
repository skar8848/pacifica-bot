"""
Agent wallet generation and encrypted storage.
"""

from cryptography.fernet import Fernet
from solders.keypair import Keypair
import base58

from bot.config import ENCRYPTION_KEY


def _get_fernet() -> Fernet:
    if not ENCRYPTION_KEY:
        raise RuntimeError("ENCRYPTION_KEY not set in .env")
    return Fernet(ENCRYPTION_KEY.encode())


def generate_agent_wallet() -> tuple[str, str]:
    """Generate a new agent wallet.

    Returns:
        (public_key_str, encrypted_private_key_b64)
    """
    kp = Keypair()
    public_key = str(kp.pubkey())
    private_key_b58 = base58.b58encode(bytes(kp)).decode("ascii")

    fernet = _get_fernet()
    encrypted = fernet.encrypt(private_key_b58.encode()).decode("ascii")

    return (public_key, encrypted)


def decrypt_private_key(encrypted: str) -> Keypair:
    """Decrypt an encrypted private key back into a Keypair."""
    fernet = _get_fernet()
    private_key_b58 = fernet.decrypt(encrypted.encode()).decode("ascii")
    return Keypair.from_base58_string(private_key_b58)


async def get_user_keypair(telegram_id: int) -> Keypair:
    """Load and decrypt the agent wallet keypair for a user."""
    from database.db import get_user

    user = await get_user(telegram_id)
    if not user or not user["agent_wallet_encrypted"]:
        raise ValueError(f"No agent wallet found for user {telegram_id}")
    return decrypt_private_key(user["agent_wallet_encrypted"])
