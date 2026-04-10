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
BUILDER_CODE = os.getenv("BUILDER_CODE", "") or ""
BUILDER_FEE_RATE = os.getenv("BUILDER_FEE_RATE", "0.0005")

# --- Telegram ---
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")

# --- Encryption ---
ENCRYPTION_KEY = os.getenv("ENCRYPTION_KEY", "")

# --- Solana RPC ---
SOLANA_RPC_URL = os.getenv("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com")

# --- Database ---
DATABASE_URL = os.getenv("DATABASE_URL", "")  # PostgreSQL connection string (required on Render)
DATABASE_PATH = os.getenv("DATABASE_PATH", "database/pacifica_bot.db")  # SQLite fallback (local dev)

# --- Pacifica access ---
PACIFICA_REFERRAL_CODE = os.getenv("PACIFICA_REFERRAL_CODE", "Pacifica")

# Pool of beta codes — tried in order, auto-skips exhausted ones
# Override via env: BETA_CODE_POOL=CODE1,CODE2,CODE3
BETA_CODE_POOL = [c.strip() for c in os.getenv("BETA_CODE_POOL", "").split(",") if c.strip()] or [
    # Our own generated codes (priority)
    "WVEN1W39T7W3HFH0",
    "X4VA9AHBEFKA4N6Z",
    "JV81QZBNFNC6PYD8",
]

# --- Bot ---
BOT_USERNAME = os.getenv("BOT_USERNAME", "trident_pacifica_bot")
ADMIN_IDS = [int(x) for x in os.getenv("ADMIN_IDS", "6994676998").split(",") if x.strip()]

# --- SOL Dispenser (devnet) ---
DISPENSER_PRIVATE_KEY = os.getenv("DISPENSER_PRIVATE_KEY", "")
DISPENSER_SOL_AMOUNT = 0.02  # SOL sent to each new wallet
DISPENSER_USDC_AMOUNT = 3_000  # USDC sent to each new wallet (devnet)

# --- Alert Group ---
ALERT_GROUP_ID = int(os.getenv("ALERT_GROUP_ID", "0")) or None

# --- Defaults ---
DEFAULT_SLIPPAGE = "0.5"
DEFAULT_EXPIRY_WINDOW = 5_000
COPY_POLL_INTERVAL = 5  # seconds
GAS_CHECK_INTERVAL = 300  # seconds (5 min)
LOW_SOL_THRESHOLD = 0.01  # SOL
