# Trident — Telegram Trading Bot for Pacifica

[@trident_pacifica_bot](https://t.me/trident_pacifica_bot) — Trade perpetual futures on [Pacifica](https://pacifica.fi) (Solana perp DEX) directly from Telegram.

## Features

**Trading**
- Market & limit orders with leverage (1–50x)
- Quick commands: `/long BTC 100 5x`, `/short ETH 200 10x`
- TP/SL with smart price hints
- Partial close (25/50/75%) or close all
- Equity-based sizing (10%/25%/50% of your balance)
- PnL share cards on close

**Copy Trading**
- Browse top traders sorted by PnL
- One-tap copy setup with sizing modes (fixed $, % equity, ratio)
- Auto-retry with increasing slippage for volatile tokens
- Mirror existing positions or wait for new trades
- Unfollow with confirmation

**Social**
- Referral system with fee sharing (10% referrer / 5% rebate)
- Unique @username per wallet (permanent reservation)
- PnL cards with branding and referral link
- Leaderboard with `/inspect` trader profiles

**Monitoring**
- Liquidation alerts (75/85/90% margin lost)
- Price alerts
- Whale tracking
- Funding rate monitoring
- 22 background services running concurrently

## Quick Start

```bash
git clone https://github.com/skar8848/pacifica-bot.git
cd pacifica-bot
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

cp .env.example .env
# Edit .env with your values (see below)

python -m bot.main
```

## Environment Variables

| Variable | Required | Description |
|---|---|---|
| `TELEGRAM_BOT_TOKEN` | Yes | From [@BotFather](https://t.me/BotFather) |
| `DATABASE_URL` | Yes | PostgreSQL connection string ([Neon](https://neon.tech) free tier works) |
| `ENCRYPTION_KEY` | Yes | Fernet key: `python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"` |
| `PACIFICA_NETWORK` | No | `testnet` (default) or `mainnet` |
| `BUILDER_CODE` | No | Builder program code |
| `BUILDER_FEE_RATE` | No | Builder fee rate (default: `0.0005`) |
| `SOLANA_RPC_URL` | No | Solana RPC endpoint |
| `RENDER_EXTERNAL_URL` | No | Auto-set by Render; used for keep-alive ping |
| `DISPENSER_PRIVATE_KEY` | No | Devnet: auto-sends SOL+USDC to new wallets |
| `ADMIN_IDS` | No | Comma-separated Telegram user IDs |

## Commands

| Command | Description |
|---|---|
| `/start` | Onboarding + wallet setup |
| `/long BTC 100 5x` | Instant market long |
| `/short ETH 200` | Instant market short (1x default) |
| `/close SOL` | Close a position |
| `/closeall` | Close all positions |
| `/balance` | Full balance overview (SOL + USDC + Pacifica) |
| `/positions` | Open positions with PnL |
| `/orders` | Pending orders |
| `/history` | Trade history |
| `/copy <wallet>` | Start copy trading setup |
| `/inspect <wallet>` | Trader profile + PnL stats |
| `/top` | Leaderboard (sort: pnl, pnl7d, volume) |
| `/unfollow <wallet>` | Stop copying |
| `/masters` | List followed traders |
| `/tp <symbol> <price>` | Set take profit |
| `/sl <symbol> <price>` | Set stop loss |
| `/username <name>` | Change username |
| `/clear` | Reset account |

## Architecture

```
bot/
├── main.py                 # Entry point, health server, 22 background tasks
├── handlers/               # Telegram command handlers
│   ├── start.py            # Onboarding, referrals, navigation
│   ├── trading.py          # Market/limit orders, TP/SL, close
│   ├── portfolio.py        # Positions, balance, history
│   ├── copy_trade.py       # Copy trading, leaderboard, inspect
│   ├── wallet.py           # Deposit, withdraw, faucet
│   ├── whale.py            # Whale tracking
│   └── advanced.py         # DCA, trailing stops, grid trading
├── services/               # Background engines + API clients
│   ├── pacifica_client.py  # Pacifica REST API (signed requests)
│   ├── copy_engine.py      # Copy trading poll loop (5s)
│   ├── trailing_stop.py    # Guard 2-phase trailing stops
│   ├── pnl_card.py         # PNG share card generation
│   └── ...                 # 40+ service modules
├── models/user.py          # Client builder from DB user
└── utils/                  # Keyboards, formatters
database/
└── db.py                   # PostgreSQL (asyncpg) + SQLite compat wrapper
```

**Stack**: Python 3.12 · aiogram 3 · asyncpg · solders · Pillow · mplfinance

## Deployment

Deployed on **Render** with auto-deploy from GitHub.

```bash
# Docker
docker compose up -d

# Or Render / Railway / Fly.io — just set env vars
```

## Testing

```bash
pip install -r requirements-dev.txt
pytest tests/ -v    # 35 tests, ~1.5s
```

## License

MIT
