# Pacifica Telegram Trading Bot

Telegram bot for trading perpetual futures on [Pacifica](https://pacifica.fi) (Solana perp DEX).

## Features

- **Trading**: Long/Short/Limit/Close via Telegram commands
- **Copy Trading**: Mirror positions from top traders
- **Builder Code**: `Pacifica` on every order for fee generation
- **Gas Alerts**: Low SOL balance warnings

## Setup

```bash
# Clone and install
cd pacifica-bot
pip install -r requirements.txt

# Configure
cp .env.example .env
# Edit .env with your values

# Run
python -m bot.main
```

## Commands

| Command | Description |
|---|---|
| `/start` | Setup & onboarding |
| `/link <wallet>` | Link Pacifica account |
| `/long <symbol> <amount> [leverage]` | Market long |
| `/short <symbol> <amount> [leverage]` | Market short |
| `/limit <buy\|sell> <symbol> <amount> <price>` | Limit order |
| `/close <symbol>` | Close position |
| `/closeall` | Close all positions |
| `/tp <symbol> <price>` | Set take profit |
| `/sl <symbol> <price>` | Set stop loss |
| `/cancel <order_id> <symbol>` | Cancel order |
| `/cancelall` | Cancel all orders |
| `/positions` | Show open positions |
| `/orders` | Show open orders |
| `/pnl` | PnL summary |
| `/balance` | Account balance |
| `/history` | Trade history |
| `/copy <wallet> [mult] [max=amt]` | Copy a trader |
| `/unfollow <wallet>` | Stop copying |
| `/masters` | List followed traders |
| `/copylog` | Copy trade history |

## Docker

```bash
docker compose up -d
```

## Architecture

- **aiogram 3** — Async Telegram framework
- **solders** — Ed25519 signing (Solana)
- **aiosqlite** — Async SQLite
- **Fernet** — Agent wallet encryption at rest
