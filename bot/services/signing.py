"""
Pacifica Ed25519 signing — ported from the official Python SDK.
https://github.com/pacifica-fi/python-sdk
"""

import json
import base58
from solders.keypair import Keypair


def sign_message(
    header: dict, payload: dict, keypair: Keypair
) -> tuple[str, str]:
    """Sign a Pacifica API message.

    Args:
        header: Must contain 'type', 'timestamp', 'expiry_window'.
        payload: Operation-specific data (symbol, amount, side, etc.).
        keypair: Ed25519 keypair (solders).

    Returns:
        (compact_json_message, base58_signature)
    """
    message = prepare_message(header, payload)
    message_bytes = message.encode("utf-8")
    signature = keypair.sign_message(message_bytes)
    return (message, base58.b58encode(bytes(signature)).decode("ascii"))


def prepare_message(header: dict, payload: dict) -> str:
    """Build the canonical JSON message to sign.

    Merges header + {"data": payload}, sorts keys recursively,
    then serialises as compact JSON (no whitespace).
    """
    if not all(k in header for k in ("type", "timestamp", "expiry_window")):
        raise ValueError("Header must have type, timestamp, and expiry_window")

    data = {**header, "data": payload}
    data = sort_json_keys(data)
    return json.dumps(data, separators=(",", ":"))


def sort_json_keys(value):
    """Recursively sort dictionary keys alphabetically."""
    if isinstance(value, dict):
        return {k: sort_json_keys(v) for k, v in sorted(value.items())}
    elif isinstance(value, list):
        return [sort_json_keys(item) for item in value]
    return value
