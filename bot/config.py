import os
from dotenv import load_dotenv

load_dotenv()

# --- Network ---
PACIFICA_NETWORK = os.getenv("PACIFICA_NETWORK", "testnet")

_NETWORK_URLS = {
    "devnet": {
        "rest": "https://test-api.pacifica.fi/api/v1",
        "ws": "wss://test-ws.pacifica.fi/ws",
    },
    "testnet": {
        "rest": "https://test-api.pacifica.fi/api/v1",
        "ws": "wss://test-ws.pacifica.fi/ws",
    },
    "mainnet": {
        "rest": "https://api.pacifica.fi/api/v1",
        "ws": "wss://ws.pacifica.fi/ws",
    },
}

PACIFICA_REST_URL = _NETWORK_URLS[PACIFICA_NETWORK]["rest"]
PACIFICA_WS_URL = _NETWORK_URLS[PACIFICA_NETWORK]["ws"]

# --- Builder ---
BUILDER_CODE = os.getenv("BUILDER_CODE", "Trident")
BUILDER_FEE_RATE = os.getenv("BUILDER_FEE_RATE", "0.0005")

# --- Telegram ---
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")

# --- Encryption ---
ENCRYPTION_KEY = os.getenv("ENCRYPTION_KEY", "")

# --- Solana RPC ---
SOLANA_RPC_URL = os.getenv("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com")

# --- Database ---
DATABASE_PATH = os.getenv("DATABASE_PATH", "database/pacifica_bot.db")

# --- Pacifica access ---
PACIFICA_REFERRAL_CODE = os.getenv("PACIFICA_REFERRAL_CODE", "DPEXZ83PUFZ7")

# --- Bot ---
BOT_USERNAME = os.getenv("BOT_USERNAME", "trident_pacifica_bot")

# --- Defaults ---
DEFAULT_SLIPPAGE = "0.5"
DEFAULT_EXPIRY_WINDOW = 5_000
COPY_POLL_INTERVAL = 5  # seconds
GAS_CHECK_INTERVAL = 300  # seconds (5 min)
LOW_SOL_THRESHOLD = 0.01  # SOL
