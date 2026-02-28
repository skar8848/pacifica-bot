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
DATABASE_PATH = os.getenv("DATABASE_PATH", "database/pacifica_bot.db")

# --- Pacifica access ---
PACIFICA_REFERRAL_CODE = os.getenv("PACIFICA_REFERRAL_CODE", "Pacifica")

# Pool of beta codes — tried in order, auto-skips exhausted ones
BETA_CODE_POOL = [c.strip() for c in os.getenv("BETA_CODE_POOL", "").split(",") if c.strip()] or [
    "BZZGJ4W6ZSA15WZZ",
    "R4BHWY8659MQC448",
    "9ERRZMXZCBPQX054",
    "E4XN1XRN4CDNSV7S",
    "VYF57MR1WG4535FX",
    "5VZH6MNKSK5A1DQW",
    "0D3RFDA1Y5A4MJ4P",
    "CWRSTZFH2DVT7H1N",
    "XESJ4K5TYKZ9X63D",
    "PGFFDFM9EQP6SCS2",
    "ARG1ZZMY369183F8",
    "CT9E519YVMHDCQ9C",
    "Z9JCW64C8KNTA4H1",
    "J9YFSBENTXPG0QCM",
    "FSBFFVHVERQ8C96R",
    "9NQQ431VN5EWF8J2",
    "0PNEZ6BNEWE4WYXS",
    "4WX2K3JY3QX0RH81",
    "243X5NX142Q4VHSW",
    "CQ8MT6V1YERC4X3A",
    "W5K4M3MD9J16PTJS",
    "3NRTNR3HVCKPYY2R",
    "49AGMJPGQTZ70MS8",
    "GNN5XT3H4EGHKNHH",
    "3EG8BFMVHQV202EG",
    "KHXYGMSGJWFE0BWT",
    "AFT8Q1D26R0CCAAJ",
    "CTZ1S6TBAYK33BAB",
    "9PAK81M7D7GNP0FQ",
    "YZJBQM6DTYRMJNVP",
]

# --- Bot ---
BOT_USERNAME = os.getenv("BOT_USERNAME", "trident_pacifica_bot")
ADMIN_IDS = [int(x) for x in os.getenv("ADMIN_IDS", "6994676998").split(",") if x.strip()]

# --- SOL Dispenser (devnet) ---
DISPENSER_PRIVATE_KEY = os.getenv("DISPENSER_PRIVATE_KEY", "")
DISPENSER_SOL_AMOUNT = 0.02  # SOL sent to each new wallet
DISPENSER_USDC_AMOUNT = 3_000  # USDC sent to each new wallet (devnet)

# --- Defaults ---
DEFAULT_SLIPPAGE = "0.5"
DEFAULT_EXPIRY_WINDOW = 5_000
COPY_POLL_INTERVAL = 5  # seconds
GAS_CHECK_INTERVAL = 300  # seconds (5 min)
LOW_SOL_THRESHOLD = 0.01  # SOL
