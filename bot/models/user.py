"""
Helper to build a PacificaClient from a user's DB record.
"""

from solders.keypair import Keypair

from bot.services.pacifica_client import PacificaClient
from bot.services.wallet_manager import decrypt_private_key
from bot.config import BUILDER_CODE


def build_client_from_user(user: dict) -> PacificaClient:
    """Build an authenticated PacificaClient from a user DB row.

    The user must have pacifica_account and agent_wallet_encrypted set.
    """
    if not user.get("pacifica_account"):
        raise ValueError("User has not linked their Pacifica account yet.")
    if not user.get("agent_wallet_encrypted"):
        raise ValueError("User has no agent wallet.")

    keypair: Keypair = decrypt_private_key(user["agent_wallet_encrypted"])

    return PacificaClient(
        account=user["pacifica_account"],
        keypair=keypair,
        agent_wallet=user["agent_wallet_public"],
        builder_code=BUILDER_CODE,
    )
