"""
Helper to build a PacificaClient from a user's DB record.
"""

from solders.keypair import Keypair

from bot.services.pacifica_client import PacificaClient
from bot.services.wallet_manager import decrypt_private_key
from bot.config import BUILDER_CODE


def build_client_from_user(user: dict) -> PacificaClient:
    """Build an authenticated PacificaClient from a user DB row.

    Supports two modes:
    1. Imported wallet (no agent): signs directly with the wallet key
    2. Agent wallet: account = linked wallet, signs with agent key
    """
    if not user.get("pacifica_account"):
        raise ValueError("User has not set up their wallet yet.")
    if not user.get("agent_wallet_encrypted"):
        raise ValueError("User has no wallet key.")

    keypair: Keypair = decrypt_private_key(user["agent_wallet_encrypted"])

    # If agent_wallet_public is set and differs from pacifica_account,
    # we're using the agent wallet pattern (account + separate signer)
    agent_pub = user.get("agent_wallet_public")
    if agent_pub and agent_pub != user["pacifica_account"]:
        return PacificaClient(
            account=user["pacifica_account"],
            keypair=keypair,
            agent_wallet=agent_pub,
            builder_code=BUILDER_CODE,
        )

    # Direct wallet: signing with the main wallet key, no agent
    return PacificaClient(
        account=user["pacifica_account"],
        keypair=keypair,
        agent_wallet=None,
        builder_code=BUILDER_CODE,
    )
