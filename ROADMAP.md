# Trident — Pacifica Trading Bot Roadmap

## Hackathon Submission: March 16 – April 16, 2026

### Tracks Covered
- **Track 1: Trading Applications & Bots** — Core trading engine
- **Track 2: Analysis & Data** — PnL tracking, portfolio analytics
- **Track 3: Social & Gamification** — PnL cards, leaderboards, copy trading, referrals

### Special Awards Targeted
- **Best User Experience** — One-tap onboarding, inline trading, PnL cards
- **Most Innovative Application** — First Telegram perp bot on Pacifica + social layer

---

## Current Features (v1 — Live on Testnet)

### Trading Engine
- [x] Market orders (Long/Short) with inline button flow
- [x] Leverage selection (1x–50x) with custom input
- [x] USDC-denominated sizing with auto token conversion
- [x] Limit orders with tick level conversion
- [x] Take Profit / Stop Loss setting
- [x] Partial close (25%, 50%, 75%) and full close
- [x] Close All positions in one tap
- [x] Slippage configuration per user
- [x] Builder code "Pacifica" auto-injected on every order (0.05% fee)
- [x] Beta code auto-claim before first trade

### Wallet Management
- [x] One-tap wallet generation (encrypted, stored locally)
- [x] Wallet import via /import command
- [x] Deposit/Withdraw via Pacifica
- [x] Export private key
- [x] SOL + USDC auto-dispenser (devnet)
- [x] Wallet uniqueness check (1 wallet per account)

### Portfolio & Analytics
- [x] Real-time positions view with PnL (including funding costs)
- [x] Open orders display
- [x] Account balance and equity
- [x] Parallel price fetching for fast refresh

### Social & Referral
- [x] Referral system with deeplinks (t.me/bot?start=ref_username)
- [x] Custom @username system (unique per wallet)
- [x] Fee sharing: 10% referrer / 5% referee rebate
- [x] Referral dashboard with stats and claim
- [x] **PnL share cards** — PNG images with trade details, branding, referral link
- [x] Auto PnL card on position close

### Infrastructure
- [x] Deployed on Render (auto-deploy from GitHub)
- [x] Health check endpoint for Render free tier
- [x] Devnet/Testnet/Mainnet toggle via env
- [x] Encrypted wallet storage (Fernet)

---

## Phase 2 — Hackathon Sprint (March 16 – April 16)

### Trading Enhancements
- [ ] **DCA into perp positions** — Schedule limit orders to average in
- [ ] **Funding rate alerts** — Notify when funding spikes (arb opportunity)
- [ ] **Liquidation proximity warnings** — Alert at 75%, 50%, 25% margin distance
- [ ] **Quick trade mode** — `/long SOL 100 10x` one-command execution
- [ ] **Two-mode UX** — Simple (quick buttons) vs Advanced (full parameters)

### Copy Trading (Priority)
- [ ] **Copy engine** — Poll master wallets, replicate positions
- [ ] **Follow/unfollow** via `/copy <wallet>` command
- [ ] **Size multiplier** — Copy at 0.5x, 1x, 2x of master's sizing
- [ ] **Max position cap** — Limit exposure per copied trade
- [ ] **Copy trade notifications** — Alert when master opens/closes
- [ ] **Copy trade log** — History of all replicated trades

### Social & Gamification (High Priority)
- [ ] **Group chat integration** — Add bot to TG groups for shared trading
- [ ] **Per-group leaderboard** — Rank traders within a group
- [ ] **Global leaderboard** — All-time top traders on Trident
- [ ] **Trading streaks** — Track consecutive profitable days
- [ ] **Achievement badges** — "First Trade", "10x Gainer", "100 Trades", etc.
- [ ] **PnL card customization** — Multiple themes/styles
- [ ] **Squad competitions** — Teams compete for best collective PnL

### Analytics Dashboard
- [ ] **Win rate tracker** — % of profitable trades
- [ ] **PnL calendar** — Green/red heatmap by day
- [ ] **Risk dashboard** — Current exposure, margin usage, distances to liquidation
- [ ] **Trade history export** — CSV download of all trades
- [ ] **Builder analytics** — Volume generated via builder code

### Web Dashboard (Vercel)
- [ ] **Landing page** — Feature showcase, how-to, CTA to Telegram bot
- [ ] **Live stats** — Total volume, users, trades via builder API
- [ ] **Leaderboard page** — Public trader rankings
- [ ] **Documentation** — API, commands, getting started guide

---

## Phase 3 — Post-Hackathon / Mainnet

### Mainnet Launch
- [ ] Switch to mainnet Pacifica API
- [ ] Remove dispenser (users deposit SOL directly)
- [ ] Persistent database (PostgreSQL on Render/Supabase)
- [ ] Production monitoring and alerting

### Gasless Trading (V2)
- [ ] SOL balance monitoring with low-balance alerts
- [ ] Fee delegation with small markup (1-2%)
- [ ] Deposit abstraction (accept USDC from any chain)

### Advanced Features
- [ ] **Whale tracking** — Monitor large Pacifica trades, alert in Telegram
- [ ] **Price alerts** — Set alerts for specific price levels
- [ ] **Trailing stop loss** — Dynamic SL that follows price
- [ ] **Multi-wallet support** — Switch between trading wallets
- [ ] **Vault strategies** — Community-managed trading vaults
- [ ] **Cross-DEX arbitrage** — Price comparison with other Solana perp DEXs

---

## Design System

### Color Palette
| Color | Hex | Usage |
|---|---|---|
| Background Dark | `#0D1117` | Main background |
| Card Background | `#161B22` | Cards, panels |
| Accent Green | `#10B981` | Profit, Long, Success |
| Accent Red | `#EF4444` | Loss, Short, Error |
| Text White | `#FFFFFF` | Primary text |
| Text Dim | `#8B949E` | Secondary/label text |
| Accent Blue | `#38BDF8` | Links, usernames |
| Pacifica Purple | `#8B5CF6` | Branding, referral links |

### Typography
- **Headlines**: Bold, 32px+
- **Body**: Regular, 16-22px
- **Labels**: 14px, dimmed color
- **Monospace**: Code, wallet addresses, order IDs

### PnL Card Layout (800x480 PNG)
```
┌─────────────────────────────────────┐
│  TRIDENT on Pacifica    [LONG BTC]  │
│─────────────────────────────────────│
│                                     │
│          +$1,234.56                 │
│           (+12.34%)                 │
│     ═══════════════                 │
│                                     │
│  Entry      Mark      Size    Lev   │
│  $95,000   $96,234   0.013   10x   │
│─────────────────────────────────────│
│  Entry ────────────────→ Now        │
│  $95,000                 $96,234    │
│─────────────────────────────────────│
│  @username   Powered by Pacifica    │
│              t.me/trident_...       │
└─────────────────────────────────────┘
```

---

## Competitive Positioning

| Feature | Trident | Pocket Pro (dYdX) | Hyperbot (HL) | Trojan |
|---|---|---|---|---|
| Platform | Pacifica (Solana) | dYdX (Arbitrum) | Hyperliquid | Solana DEXs |
| Perp Trading | ✅ | ✅ | ✅ | ❌ (spot only) |
| Copy Trading | ✅ (planned) | ✅ | ✅ | ✅ |
| PnL Cards | ✅ | ✅ | ❌ | ✅ |
| Leaderboards | ✅ (planned) | ✅ | ❌ | ❌ |
| Group Chat | ✅ (planned) | ✅ | ❌ | ❌ |
| Builder Fees | ✅ (native) | ❌ | ❌ | ❌ |
| Referral System | ✅ | ✅ | ❌ | ✅ |
| Free Tier | ✅ | ✅ | ❌ (paid) | ✅ |
| Solana Native | ✅ | ❌ | ❌ | ✅ |

### Key Differentiators
1. **First and only Telegram perp bot on Pacifica** — No competition
2. **Builder Program integration** — Every trade generates revenue via builder code
3. **Solana-native** — Fast, cheap, aligned with Pacifica's infrastructure
4. **Social-first approach** — PnL cards, leaderboards, group trading from day one
5. **Copy trading on Solana perps** — Unique in the ecosystem
