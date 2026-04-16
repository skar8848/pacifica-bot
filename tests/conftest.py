"""
Pytest fixtures for Trident integration tests.

Provides:
- In-memory mock database (dict-backed, same interface as database.db functions)
- Mock Pacifica API client (canned responses)
- Mock bot / dispatcher helpers
- Auto-patching of database.db and related modules
"""

import asyncio
import copy
import json as _json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio

# ---------------------------------------------------------------------------
# pytest-asyncio configuration
# ---------------------------------------------------------------------------

pytest_plugins = ("pytest_asyncio",)


# ---------------------------------------------------------------------------
# In-memory mock database
# ---------------------------------------------------------------------------

class MockDB:
    """Dict-backed database that mirrors every function exported by database.db."""

    REFERRAL_FEE_SHARE = 0.10
    REFEREE_FEE_REBATE = 0.05

    def __init__(self):
        self.users: dict[int, dict] = {}          # telegram_id -> user dict
        self.trade_log: list[dict] = []
        self.referral_fees: list[dict] = []
        self.reserved_usernames: dict[str, int] = {}  # lower_name -> telegram_id
        self.price_alerts: list[dict] = []
        self._alert_seq = 1

    # -- users ---------------------------------------------------------------

    async def get_user(self, telegram_id: int) -> dict | None:
        u = self.users.get(telegram_id)
        return copy.deepcopy(u) if u else None

    async def create_user(
        self,
        telegram_id: int,
        agent_wallet_public: str | None,
        agent_wallet_encrypted: str,
    ) -> dict:
        self.users[telegram_id] = {
            "telegram_id": telegram_id,
            "agent_wallet_public": agent_wallet_public,
            "agent_wallet_encrypted": agent_wallet_encrypted,
            "pacifica_account": None,
            "username": None,
            "ref_code": None,
            "referred_by": None,
            "builder_approved": 0,
            "settings": "{}",
        }
        return copy.deepcopy(self.users[telegram_id])

    async def update_user(self, telegram_id: int, **fields):
        if telegram_id in self.users:
            self.users[telegram_id].update(fields)

    async def get_user_by_wallet(self, wallet: str, exclude_tg_id: int | None = None) -> dict | None:
        for u in self.users.values():
            if u.get("pacifica_account") == wallet:
                if exclude_tg_id and u["telegram_id"] == exclude_tg_id:
                    continue
                return copy.deepcopy(u)
        return None

    # -- referrals -----------------------------------------------------------

    async def get_or_create_ref_code(self, telegram_id: int) -> str:
        u = self.users.get(telegram_id)
        if u and u.get("ref_code"):
            return u["ref_code"]
        code = f"REF{telegram_id}"
        if u:
            u["ref_code"] = code
        return code

    async def get_user_by_ref_code(self, code: str) -> dict | None:
        for u in self.users.values():
            if u.get("ref_code") == code or u.get("username") == code:
                return copy.deepcopy(u)
        return None

    async def count_referrals(self, telegram_id: int) -> int:
        return sum(1 for u in self.users.values() if u.get("referred_by") == telegram_id)

    async def is_username_taken(self, username: str, exclude_tg_id: int | None = None) -> bool:
        lower = username.lower()
        # Check reserved
        if lower in self.reserved_usernames:
            owner = self.reserved_usernames[lower]
            if exclude_tg_id and owner == exclude_tg_id:
                return False
            return True
        for u in self.users.values():
            if (u.get("username") or "").lower() == lower:
                if exclude_tg_id and u["telegram_id"] == exclude_tg_id:
                    continue
                return True
        return False

    async def reserve_username(self, username: str, telegram_id: int):
        self.reserved_usernames[username.lower()] = telegram_id

    # -- settings ------------------------------------------------------------

    async def get_user_settings(self, telegram_id: int) -> dict:
        u = self.users.get(telegram_id)
        if not u:
            return {}
        try:
            return _json.loads(u.get("settings") or "{}")
        except Exception:
            return {}

    async def set_user_setting(self, telegram_id: int, key: str, value):
        settings = await self.get_user_settings(telegram_id)
        settings[key] = value
        await self.update_user(telegram_id, settings=_json.dumps(settings))

    # -- trade log -----------------------------------------------------------

    async def log_trade(
        self,
        telegram_id: int,
        symbol: str,
        side: str,
        amount: str,
        price: str = "",
        order_type: str = "market",
        is_copy_trade: bool = False,
        master_wallet: str = "",
        client_order_id: str = "",
    ):
        self.trade_log.append({
            "telegram_id": telegram_id,
            "symbol": symbol,
            "side": side,
            "amount": amount,
            "price": price,
            "order_type": order_type,
            "is_copy_trade": is_copy_trade,
            "master_wallet": master_wallet,
            "client_order_id": client_order_id,
        })

    # -- referral fees -------------------------------------------------------

    async def log_referral_fee(
        self,
        referrer_id: int,
        referee_id: int,
        symbol: str,
        trade_volume: float,
        fee_earned: float,
    ):
        self.referral_fees.append({
            "referrer_id": referrer_id,
            "referee_id": referee_id,
            "symbol": symbol,
            "trade_volume": trade_volume,
            "fee_earned": fee_earned,
            "claimed": 0,
        })

    async def get_referral_stats(self, telegram_id: int) -> dict:
        ref_count = await self.count_referrals(telegram_id)
        total_earned = sum(f["fee_earned"] for f in self.referral_fees if f["referrer_id"] == telegram_id)
        unclaimed = sum(
            f["fee_earned"]
            for f in self.referral_fees
            if f["referrer_id"] == telegram_id and not f["claimed"]
        )
        total_volume = sum(f["trade_volume"] for f in self.referral_fees if f["referrer_id"] == telegram_id)
        return {
            "ref_count": ref_count,
            "total_earned": total_earned,
            "unclaimed": unclaimed,
            "total_volume": total_volume,
        }

    async def claim_referral_fees(self, telegram_id: int) -> float:
        unclaimed = sum(
            f["fee_earned"]
            for f in self.referral_fees
            if f["referrer_id"] == telegram_id and not f["claimed"]
        )
        for f in self.referral_fees:
            if f["referrer_id"] == telegram_id and not f["claimed"]:
                f["claimed"] = 1
        return unclaimed

    # -- price alerts --------------------------------------------------------

    async def get_active_alerts(self, telegram_id: int | None = None) -> list[dict]:
        return [
            a for a in self.price_alerts
            if a.get("active") and not a.get("triggered")
            and (telegram_id is None or a["telegram_id"] == telegram_id)
        ]

    async def add_price_alert(self, telegram_id: int, symbol: str, direction: str, target_price: float) -> int:
        alert_id = self._alert_seq
        self._alert_seq += 1
        self.price_alerts.append({
            "id": alert_id,
            "telegram_id": telegram_id,
            "symbol": symbol,
            "direction": direction,
            "target_price": target_price,
            "active": 1,
            "triggered": 0,
        })
        return alert_id

    async def delete_alert(self, alert_id: int, telegram_id: int):
        self.price_alerts = [
            a for a in self.price_alerts
            if not (a["id"] == alert_id and a["telegram_id"] == telegram_id)
        ]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def mock_db():
    """Return a fresh MockDB instance."""
    return MockDB()


@pytest.fixture()
def patch_db(mock_db):
    """Patch all database.db functions with the mock_db equivalents.

    Patches at BOTH the source module (database.db) AND every handler module
    that uses ``from database.db import ...``, because Python binds the name
    at import time.

    Returns the MockDB so tests can inspect state.
    """
    # (function_name, mock_db_method) — maps db function names to mock methods
    fn_map = {
        "get_user": mock_db.get_user,
        "create_user": mock_db.create_user,
        "update_user": mock_db.update_user,
        "get_user_by_wallet": mock_db.get_user_by_wallet,
        "get_or_create_ref_code": mock_db.get_or_create_ref_code,
        "get_user_by_ref_code": mock_db.get_user_by_ref_code,
        "count_referrals": mock_db.count_referrals,
        "is_username_taken": mock_db.is_username_taken,
        "reserve_username": mock_db.reserve_username,
        "get_user_settings": mock_db.get_user_settings,
        "set_user_setting": mock_db.set_user_setting,
        "log_trade": mock_db.log_trade,
        "log_referral_fee": mock_db.log_referral_fee,
        "get_referral_stats": mock_db.get_referral_stats,
        "claim_referral_fees": mock_db.claim_referral_fees,
        "get_active_alerts": mock_db.get_active_alerts,
        "add_price_alert": mock_db.add_price_alert,
        "delete_alert": mock_db.delete_alert,
    }

    # Modules that import db functions directly
    handler_modules = [
        "database.db",
        "bot.handlers.start",
        "bot.handlers.trading",
        "bot.handlers.wallet",
        "bot.handlers.portfolio",
        "bot.handlers.copy_trade",
    ]

    stack = []
    for fn_name, mock_fn in fn_map.items():
        for mod in handler_modules:
            target = f"{mod}.{fn_name}"
            try:
                p = patch(target, side_effect=mock_fn)
                p.start()
                stack.append(p)
            except AttributeError:
                pass  # this module doesn't import this function — skip

    # Also patch the constants that are imported by value
    for mod in handler_modules:
        try:
            p = patch(f"{mod}.REFERRAL_FEE_SHARE", 0.10)
            p.start()
            stack.append(p)
        except AttributeError:
            pass
        try:
            p = patch(f"{mod}.REFEREE_FEE_REBATE", 0.05)
            p.start()
            stack.append(p)
        except AttributeError:
            pass

    yield mock_db

    for p in stack:
        p.stop()


# ---------------------------------------------------------------------------
# Mock Pacifica client
# ---------------------------------------------------------------------------

def make_mock_pacifica_client(
    balance: float = 1000.0,
    equity: float = 1050.0,
    positions: list | None = None,
    fill_price: float = 50000.0,
    order_id: str = "mock-order-123",
):
    """Build an AsyncMock that mimics PacificaClient methods.

    Override keyword arguments to change canned responses.
    """
    client = AsyncMock()
    client.close = AsyncMock()

    # Account info
    client.get_account_info = AsyncMock(return_value={
        "balance": balance,
        "account_equity": equity,
        "free_collateral": balance * 0.8,
        "initial_margin": 200.0,
        "maintenance_margin": 100.0,
    })

    # Positions
    client.get_positions = AsyncMock(return_value=positions or [])

    # Market order
    client.create_market_order = AsyncMock(return_value={
        "order_id": order_id,
        "fill_price": fill_price,
        "status": "filled",
        "symbol": "BTC",
        "side": "bid",
        "amount": "0.01",
    })

    # Limit order
    client.create_limit_order = AsyncMock(return_value={
        "order_id": order_id,
        "status": "open",
    })

    # TP/SL
    client.set_tpsl = AsyncMock(return_value={"status": "ok"})

    # Cancel
    client.cancel_order = AsyncMock(return_value={"status": "cancelled"})
    client.cancel_all_orders = AsyncMock(return_value={"status": "ok"})

    # Builder/referral
    client.approve_builder_code = AsyncMock(return_value={"status": "ok"})
    client.claim_referral_code = AsyncMock(return_value={"status": "ok"})

    # Open orders
    client.get_open_orders = AsyncMock(return_value=[])

    # Trades (for market detail)
    client.get_trades = AsyncMock(return_value=[{"price": str(fill_price)}])
    client.get_markets_info = AsyncMock(return_value=[
        {"symbol": "BTC", "max_leverage": 50},
        {"symbol": "ETH", "max_leverage": 50},
        {"symbol": "SOL", "max_leverage": 20},
    ])

    return client


@pytest.fixture()
def mock_client():
    """Return a default mock Pacifica client."""
    return make_mock_pacifica_client()


@pytest.fixture()
def patch_client(mock_client):
    """Patch build_client_from_user to return the mock client."""
    with patch("bot.models.user.build_client_from_user", return_value=mock_client):
        yield mock_client


# ---------------------------------------------------------------------------
# Mock market data functions
# ---------------------------------------------------------------------------

@pytest.fixture()
def patch_market_data():
    """Patch market_data helper functions with simple canned values."""
    patches = []

    async def mock_get_price(symbol: str):
        prices = {"BTC": 50000.0, "ETH": 3000.0, "SOL": 150.0}
        return prices.get(symbol)

    async def mock_get_max_leverage(symbol: str):
        levs = {"BTC": 50, "ETH": 50, "SOL": 20}
        return levs.get(symbol, 10)

    async def mock_get_market_info(symbol: str):
        info = {
            "BTC": (50, "0.1", "0.001"),
            "ETH": (50, "0.01", "0.01"),
            "SOL": (20, "0.001", "0.1"),
        }
        return info.get(symbol, (10, "0.01", "0.01"))

    async def mock_get_lot_size(symbol: str):
        lots = {"BTC": "0.001", "ETH": "0.01", "SOL": "0.1"}
        return lots.get(symbol, "0.01")

    def mock_usd_to_token(usd_amount, price, lot_size="0.01"):
        """Simplified version of usd_to_token for tests."""
        import math
        if price <= 0:
            return "0"
        raw = usd_amount / price
        lot = float(lot_size)
        rounded = math.floor(raw / lot) * lot
        if lot >= 1:
            return str(int(rounded))
        decimals = len(lot_size.split(".")[-1]) if "." in lot_size else 0
        return f"{rounded:.{decimals}f}"

    targets = {
        # Source module
        "bot.services.market_data.get_price": mock_get_price,
        "bot.services.market_data.get_max_leverage": mock_get_max_leverage,
        "bot.services.market_data.get_market_info": mock_get_market_info,
        "bot.services.market_data.get_lot_size": mock_get_lot_size,
        # Imports in trading.py (from ... import names)
        "bot.handlers.trading.get_price": mock_get_price,
        "bot.handlers.trading.get_max_leverage": mock_get_max_leverage,
        "bot.handlers.trading.get_market_info": mock_get_market_info,
        "bot.handlers.trading.get_lot_size": mock_get_lot_size,
        "bot.handlers.trading.usd_to_token": mock_usd_to_token,
        # Module-level aliases (_get_price = get_price, etc.)
        "bot.handlers.trading._get_price": mock_get_price,
        "bot.handlers.trading._get_max_leverage": mock_get_max_leverage,
        "bot.handlers.trading._get_market_info": mock_get_market_info,
        "bot.handlers.trading._get_lot_size": mock_get_lot_size,
        "bot.handlers.trading._usdc_to_token": mock_usd_to_token,
    }

    for target, fn in targets.items():
        p = patch(target, side_effect=fn)
        patches.append(p)
        p.start()

    yield

    for p in patches:
        p.stop()


# ---------------------------------------------------------------------------
# Mock wallet manager
# ---------------------------------------------------------------------------

@pytest.fixture()
def patch_wallet():
    """Patch wallet generation/import to avoid real crypto."""
    fake_pub = "FakeWa11etPubKey1234567890abcdef1234567890ab"
    fake_enc = "gAAAAABmocked_encrypted_private_key"

    with patch(
        "bot.services.wallet_manager.generate_wallet",
        return_value=(fake_pub, fake_enc),
    ), patch(
        "bot.services.wallet_manager.import_wallet",
        return_value=(fake_pub, fake_enc),
    ), patch(
        "bot.handlers.start.generate_wallet",
        return_value=(fake_pub, fake_enc),
    ), patch(
        "bot.handlers.start.import_wallet",
        return_value=(fake_pub, fake_enc),
    ):
        yield fake_pub, fake_enc


# ---------------------------------------------------------------------------
# Telegram message / callback helpers
# ---------------------------------------------------------------------------

def make_message(
    text: str = "",
    user_id: int = 12345,
    first_name: str = "TestUser",
    chat_id: int = 12345,
) -> MagicMock:
    """Create a mock aiogram Message object."""
    msg = MagicMock()
    msg.text = text
    msg.from_user = MagicMock()
    msg.from_user.id = user_id
    msg.from_user.first_name = first_name
    msg.chat = MagicMock()
    msg.chat.id = chat_id
    msg.answer = AsyncMock()
    msg.delete = AsyncMock()
    msg.reply = AsyncMock()
    return msg


def make_callback(
    data: str = "",
    user_id: int = 12345,
    first_name: str = "TestUser",
    chat_id: int = 12345,
) -> MagicMock:
    """Create a mock aiogram CallbackQuery object."""
    cb = MagicMock()
    cb.data = data
    cb.from_user = MagicMock()
    cb.from_user.id = user_id
    cb.from_user.first_name = first_name
    cb.message = MagicMock()
    cb.message.chat = MagicMock()
    cb.message.chat.id = chat_id
    cb.message.edit_text = AsyncMock()
    cb.message.delete = AsyncMock()
    cb.answer = AsyncMock()
    cb.bot = MagicMock()
    cb.bot.send_photo = AsyncMock()
    return cb


def make_state(data: dict | None = None) -> MagicMock:
    """Create a mock FSMContext."""
    state = MagicMock()
    _data: dict = data or {}
    _current_state: list = [None]

    async def get_data():
        return _data.copy()

    async def update_data(**kwargs):
        _data.update(kwargs)

    async def set_data(new_data):
        nonlocal _data
        _data = new_data

    async def set_state(new_state):
        _current_state[0] = new_state

    async def get_state():
        return _current_state[0]

    async def clear():
        nonlocal _data
        _data = {}
        _current_state[0] = None

    state.get_data = get_data
    state.update_data = update_data
    state.set_data = set_data
    state.set_state = set_state
    state.get_state = get_state
    state.clear = clear

    return state
