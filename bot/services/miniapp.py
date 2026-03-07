"""
Telegram Mini App — serves an inline web app for the bot.

Provides:
- Portfolio overview (positions, PnL, equity)
- Quick trading interface
- Leader board
- Settings

The mini app is served from the bot's health check aiohttp server.
"""

import json
import hashlib
import hmac
import logging
from urllib.parse import parse_qs, unquote

from aiohttp import web

from bot.config import TELEGRAM_BOT_TOKEN, PACIFICA_REST_URL
from database.db import get_user, get_leader_profile, get_leader_performance

logger = logging.getLogger(__name__)


def validate_init_data(init_data: str) -> dict | None:
    """Validate Telegram WebApp initData using HMAC-SHA256."""
    if not init_data or not TELEGRAM_BOT_TOKEN:
        return None

    parsed = dict(parse_qs(init_data, keep_blank_values=True))
    received_hash = parsed.pop("hash", [None])[0]
    if not received_hash:
        return None

    # Flatten single-value lists
    flat = {k: v[0] if len(v) == 1 else v for k, v in parsed.items()}

    # Build data check string (sorted key=value, newline-separated)
    data_check = "\n".join(f"{k}={flat[k]}" for k in sorted(flat.keys()))

    # HMAC: secret_key = HMAC_SHA256("WebAppData", bot_token)
    secret = hmac.new(b"WebAppData", TELEGRAM_BOT_TOKEN.encode(), hashlib.sha256).digest()
    computed = hmac.new(secret, data_check.encode(), hashlib.sha256).hexdigest()

    if not hmac.compare_digest(computed, received_hash):
        return None

    # Extract user
    user_str = flat.get("user")
    if user_str:
        try:
            return json.loads(unquote(user_str))
        except (json.JSONDecodeError, TypeError):
            pass

    return flat


# ------------------------------------------------------------------
# Mini App HTML
# ------------------------------------------------------------------

MINIAPP_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Trident</title>
<script src="https://telegram.org/js/telegram-web-app.js"></script>
<style>
  :root {
    --bg: #0a121c;
    --card: #0c1218;
    --primary: #55c3e9;
    --green: #02c77b;
    --red: #eb365a;
    --text: #e8edf2;
    --muted: #586878;
    --border: rgba(255,255,255,0.08);
  }
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body {
    font-family: -apple-system, 'Inter', sans-serif;
    background: var(--bg);
    color: var(--text);
    padding: 16px;
    min-height: 100vh;
  }
  .card {
    background: var(--card);
    border: 1px solid var(--border);
    border-radius: 12px;
    padding: 16px;
    margin-bottom: 12px;
  }
  .card h3 { color: var(--primary); font-size: 14px; margin-bottom: 8px; text-transform: uppercase; letter-spacing: 1px; }
  .stat-row { display: flex; justify-content: space-between; padding: 6px 0; border-bottom: 1px solid var(--border); }
  .stat-row:last-child { border-bottom: none; }
  .stat-label { color: var(--muted); font-size: 13px; }
  .stat-value { font-weight: 600; font-size: 14px; }
  .green { color: var(--green); }
  .red { color: var(--red); }
  .positions { margin-top: 8px; }
  .pos-item {
    display: flex; justify-content: space-between; align-items: center;
    padding: 10px 0; border-bottom: 1px solid var(--border);
  }
  .pos-item:last-child { border-bottom: none; }
  .pos-symbol { font-weight: 700; font-size: 15px; }
  .pos-side { font-size: 12px; padding: 2px 6px; border-radius: 4px; font-weight: 600; }
  .pos-long { background: rgba(2, 199, 123, 0.15); color: var(--green); }
  .pos-short { background: rgba(235, 54, 90, 0.15); color: var(--red); }
  .btn {
    display: block; width: 100%; padding: 12px; border: none; border-radius: 10px;
    font-size: 15px; font-weight: 600; cursor: pointer; margin-top: 8px;
    transition: opacity 0.2s;
  }
  .btn:active { opacity: 0.7; }
  .btn-primary { background: var(--primary); color: #0a121c; }
  .btn-row { display: flex; gap: 8px; margin-top: 8px; }
  .btn-row .btn { flex: 1; }
  .loader { text-align: center; padding: 40px; color: var(--muted); }
  .tabs { display: flex; gap: 4px; margin-bottom: 16px; }
  .tab {
    flex: 1; text-align: center; padding: 8px; border-radius: 8px;
    font-size: 13px; font-weight: 600; cursor: pointer;
    background: var(--card); border: 1px solid var(--border); color: var(--muted);
  }
  .tab.active { background: var(--primary); color: #0a121c; border-color: var(--primary); }
  #error { color: var(--red); text-align: center; padding: 20px; display: none; }
</style>
</head>
<body>
<div class="tabs">
  <div class="tab active" onclick="showTab('portfolio')">Portfolio</div>
  <div class="tab" onclick="showTab('positions')">Positions</div>
  <div class="tab" onclick="showTab('leaders')">Leaders</div>
</div>

<div id="portfolio-tab">
  <div class="loader" id="loading">Loading...</div>
  <div id="portfolio-content" style="display:none">
    <div class="card">
      <h3>Account</h3>
      <div class="stat-row"><span class="stat-label">Equity</span><span class="stat-value" id="equity">-</span></div>
      <div class="stat-row"><span class="stat-label">Balance</span><span class="stat-value" id="balance">-</span></div>
      <div class="stat-row"><span class="stat-label">Unrealized PnL</span><span class="stat-value" id="upnl">-</span></div>
      <div class="stat-row"><span class="stat-label">Margin Used</span><span class="stat-value" id="margin">-</span></div>
    </div>
  </div>
</div>

<div id="positions-tab" style="display:none">
  <div class="card">
    <h3>Open Positions</h3>
    <div class="positions" id="pos-list">
      <div class="loader">Loading...</div>
    </div>
  </div>
</div>

<div id="leaders-tab" style="display:none">
  <div class="card">
    <h3>Top Leaders</h3>
    <div id="leaders-list">
      <div class="loader">Loading...</div>
    </div>
  </div>
</div>

<div id="error"></div>

<script>
const tg = window.Telegram.WebApp;
tg.ready();
tg.expand();
tg.setHeaderColor('#0a121c');
tg.setBackgroundColor('#0a121c');

const API = '__API_URL__';
let userData = null;
let accountAddr = null;

async function init() {
  try {
    const resp = await fetch('/miniapp/auth', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({init_data: tg.initData})
    });
    const data = await resp.json();
    if (data.error) { showError(data.error); return; }
    userData = data;
    accountAddr = data.pacifica_account;
    if (!accountAddr) { showError('No wallet linked. Use /start in the bot.'); return; }
    loadPortfolio();
  } catch(e) { showError('Connection error'); }
}

async function loadPortfolio() {
  try {
    const resp = await fetch(API + '/account?account=' + accountAddr);
    const json = await resp.json();
    const d = json.data || json;
    document.getElementById('equity').textContent = '$' + fmt(d.equity);
    document.getElementById('balance').textContent = '$' + fmt(d.balance);
    const upnl = parseFloat(d.unrealized_pnl || 0);
    const upnlEl = document.getElementById('upnl');
    upnlEl.textContent = (upnl >= 0 ? '+$' : '-$') + fmt(Math.abs(upnl));
    upnlEl.className = 'stat-value ' + (upnl >= 0 ? 'green' : 'red');
    document.getElementById('margin').textContent = '$' + fmt(d.margin_used);
    document.getElementById('loading').style.display = 'none';
    document.getElementById('portfolio-content').style.display = 'block';
  } catch(e) { showError('Failed to load portfolio'); }
}

async function loadPositions() {
  const el = document.getElementById('pos-list');
  try {
    const resp = await fetch(API + '/positions?account=' + accountAddr);
    const json = await resp.json();
    const positions = json.data || json;
    if (!positions || positions.length === 0) {
      el.innerHTML = '<div style="text-align:center;padding:20px;color:var(--muted)">No open positions</div>';
      return;
    }
    el.innerHTML = positions.map(p => {
      const side = p.side === 'bid' ? 'LONG' : 'SHORT';
      const cls = p.side === 'bid' ? 'pos-long' : 'pos-short';
      const entry = parseFloat(p.entry_price || 0);
      const amt = Math.abs(parseFloat(p.amount || 0));
      const lev = p.leverage || '?';
      return '<div class="pos-item">' +
        '<div><span class="pos-symbol">' + p.symbol + '</span> ' +
        '<span class="pos-side ' + cls + '">' + side + ' ' + lev + 'x</span></div>' +
        '<div style="text-align:right"><div style="font-size:13px">$' + fmt(entry) + '</div>' +
        '<div style="font-size:12px;color:var(--muted)">' + amt + ' tokens</div></div></div>';
    }).join('');
  } catch(e) { el.innerHTML = '<div style="color:var(--red)">Error loading positions</div>'; }
}

async function loadLeaders() {
  const el = document.getElementById('leaders-list');
  try {
    const resp = await fetch('/miniapp/leaders');
    const leaders = await resp.json();
    if (!leaders || leaders.length === 0) {
      el.innerHTML = '<div style="text-align:center;padding:20px;color:var(--muted)">No leaders yet</div>';
      return;
    }
    el.innerHTML = leaders.map((l, i) => {
      const pnl = l.total_pnl || 0;
      return '<div class="pos-item"><div>' +
        '<span class="pos-symbol">' + (i+1) + '. ' + l.display_name + '</span><br>' +
        '<span style="font-size:12px;color:var(--muted)">' + l.total_followers + ' followers | ' + l.profit_share_pct + '% share</span></div>' +
        '<div style="text-align:right"><span class="' + (pnl >= 0 ? 'green' : 'red') + '">' +
        (pnl >= 0 ? '+' : '') + '$' + fmt(pnl) + '</span><br>' +
        '<span style="font-size:12px;color:var(--muted)">' + (l.win_rate || 0).toFixed(0) + '% WR</span></div></div>';
    }).join('');
  } catch(e) { el.innerHTML = '<div style="color:var(--red)">Error loading leaders</div>'; }
}

function showTab(name) {
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  ['portfolio','positions','leaders'].forEach(t => {
    document.getElementById(t + '-tab').style.display = t === name ? 'block' : 'none';
  });
  event.target.classList.add('active');
  if (name === 'positions') loadPositions();
  if (name === 'leaders') loadLeaders();
}

function fmt(n) { return parseFloat(n || 0).toLocaleString('en-US', {minimumFractionDigits: 2, maximumFractionDigits: 2}); }
function showError(msg) {
  document.getElementById('loading').style.display = 'none';
  const el = document.getElementById('error');
  el.textContent = msg;
  el.style.display = 'block';
}

init();
</script>
</body>
</html>"""


def get_miniapp_html() -> str:
    """Return the mini app HTML with API URL injected."""
    return MINIAPP_HTML.replace("__API_URL__", PACIFICA_REST_URL)


# ------------------------------------------------------------------
# aiohttp routes (added to health server)
# ------------------------------------------------------------------

async def miniapp_page(request: web.Request) -> web.Response:
    """Serve the mini app HTML."""
    return web.Response(text=get_miniapp_html(), content_type="text/html")


async def miniapp_auth(request: web.Request) -> web.Response:
    """Validate Telegram initData and return user info."""
    try:
        body = await request.json()
        init_data = body.get("init_data", "")
    except Exception:
        return web.json_response({"error": "Invalid request"}, status=400)

    tg_user = validate_init_data(init_data)
    if not tg_user:
        return web.json_response({"error": "Invalid authentication"}, status=401)

    tg_id = tg_user.get("id")
    if not tg_id:
        return web.json_response({"error": "No user ID"}, status=400)

    user = await get_user(int(tg_id))
    if not user:
        return web.json_response({"error": "User not found. Use /start in the bot."}, status=404)

    return web.json_response({
        "telegram_id": user["telegram_id"],
        "pacifica_account": user.get("pacifica_account"),
        "username": user.get("username"),
    })


async def miniapp_leaders(request: web.Request) -> web.Response:
    """Return public leaders with performance stats."""
    from database.db import get_public_leaders
    leaders = await get_public_leaders()

    result = []
    for ldr in leaders[:20]:
        perf = await get_leader_performance(ldr["telegram_id"])
        result.append({
            "display_name": ldr["display_name"],
            "profit_share_pct": ldr.get("profit_share_pct", 10),
            "total_followers": ldr.get("total_followers", 0),
            "total_pnl": perf.get("total_pnl", 0),
            "win_rate": perf.get("win_rate", 0),
            "total_trades": perf.get("total_trades", 0),
        })

    return web.json_response(result)


def register_miniapp_routes(app: web.Application):
    """Register mini app routes on the aiohttp app."""
    app.router.add_get("/miniapp", miniapp_page)
    app.router.add_post("/miniapp/auth", miniapp_auth)
    app.router.add_get("/miniapp/leaders", miniapp_leaders)
