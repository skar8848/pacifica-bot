"""
Async Pacifica REST client — wraps signing + HTTP for all trading endpoints.

All Pacifica responses are wrapped: {"success": bool, "data": ..., "error": ...}
This client unwraps automatically and returns the inner "data" value.
"""

import time
import uuid
import logging
from typing import Any

import aiohttp
from solders.keypair import Keypair

from bot.config import (
    PACIFICA_REST_URL,
    BUILDER_CODE,
    DEFAULT_SLIPPAGE,
    DEFAULT_EXPIRY_WINDOW,
)
from bot.services.signing import sign_message

logger = logging.getLogger(__name__)


class PacificaClient:
    """Unified async client for the Pacifica perp DEX API."""

    def __init__(
        self,
        account: str,
        keypair: Keypair,
        agent_wallet: str | None = None,
        builder_code: str = BUILDER_CODE,
    ):
        self.account = account
        self.keypair = keypair
        self.agent_wallet = agent_wallet
        self.builder_code = builder_code
        self._session: aiohttp.ClientSession | None = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.close()

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                headers={"Content-Type": "application/json"}
            )
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _make_header(self, msg_type: str, expiry: int = DEFAULT_EXPIRY_WINDOW) -> dict:
        return {
            "timestamp": int(time.time() * 1_000),
            "expiry_window": expiry,
            "type": msg_type,
        }

    def _build_request(self, header: dict, payload: dict) -> dict:
        """Sign and build the final flat request body."""
        _, signature = sign_message(header, payload, self.keypair)
        return {
            "account": self.account,
            "agent_wallet": self.agent_wallet,
            "signature": signature,
            "timestamp": header["timestamp"],
            "expiry_window": header["expiry_window"],
            **payload,
        }

    @staticmethod
    def _unwrap(raw: dict) -> Any:
        """Unwrap Pacifica's standard {"success": bool, "data": ...} envelope."""
        if isinstance(raw, dict) and "success" in raw:
            if not raw["success"]:
                error_msg = raw.get("error", "Unknown error")
                code = raw.get("code", 400)
                raise PacificaAPIError(code, {"error": error_msg})
            return raw.get("data")
        return raw

    async def _post(self, endpoint: str, body: dict) -> Any:
        session = await self._get_session()
        url = f"{PACIFICA_REST_URL}{endpoint}"
        logger.debug("POST %s", url)
        async with session.post(url, json=body) as resp:
            raw = await resp.json()
            if resp.status >= 400 and not isinstance(raw, dict):
                raise PacificaAPIError(resp.status, raw)
            return self._unwrap(raw)

    async def _get(self, endpoint: str, params: dict | None = None) -> Any:
        session = await self._get_session()
        url = f"{PACIFICA_REST_URL}{endpoint}"
        logger.debug("GET %s %s", url, params)
        async with session.get(url, params=params) as resp:
            raw = await resp.json()
            if resp.status >= 400 and not isinstance(raw, dict):
                raise PacificaAPIError(resp.status, raw)
            return self._unwrap(raw)

    # ------------------------------------------------------------------
    # Order creation (all include builder_code automatically)
    # ------------------------------------------------------------------

    async def create_market_order(
        self,
        symbol: str,
        side: str,
        amount: str,
        slippage: str = DEFAULT_SLIPPAGE,
        reduce_only: bool = False,
        client_order_id: str | None = None,
    ) -> dict:
        header = self._make_header("create_market_order")
        payload: dict[str, Any] = {
            "symbol": symbol,
            "side": side,
            "amount": amount,
            "slippage_percent": slippage,
            "reduce_only": reduce_only,
            "client_order_id": client_order_id or str(uuid.uuid4()),
        }
        if self.builder_code:
            payload["builder_code"] = self.builder_code
        return await self._post("/orders/create_market", self._build_request(header, payload))

    async def create_limit_order(
        self,
        symbol: str,
        side: str,
        amount: str,
        tick_level: int,
        tif: str = "gtc",
        reduce_only: bool = False,
        client_order_id: str | None = None,
    ) -> dict:
        header = self._make_header("create_order")
        payload: dict[str, Any] = {
            "symbol": symbol,
            "side": side,
            "amount": amount,
            "tick_level": tick_level,
            "tif": tif,
            "reduce_only": reduce_only,
            "client_order_id": client_order_id or str(uuid.uuid4()),
        }
        if self.builder_code:
            payload["builder_code"] = self.builder_code
        return await self._post("/orders/create", self._build_request(header, payload))

    async def create_stop_order(
        self,
        symbol: str,
        side: str,
        amount: str,
        stop_price: str,
        limit_price: str | None = None,
        reduce_only: bool = False,
        client_order_id: str | None = None,
    ) -> dict:
        header = self._make_header("create_stop_order")
        payload: dict[str, Any] = {
            "symbol": symbol,
            "side": side,
            "amount": amount,
            "stop_price": stop_price,
            "reduce_only": reduce_only,
            "client_order_id": client_order_id or str(uuid.uuid4()),
        }
        if self.builder_code:
            payload["builder_code"] = self.builder_code
        if limit_price:
            payload["limit_price"] = limit_price
        return await self._post("/orders/stop/create", self._build_request(header, payload))

    async def set_tpsl(
        self,
        symbol: str,
        side: str,
        take_profit: dict | None = None,
        stop_loss: dict | None = None,
    ) -> dict:
        header = self._make_header("set_position_tpsl")
        payload: dict[str, Any] = {
            "symbol": symbol,
            "side": side,
        }
        if self.builder_code:
            payload["builder_code"] = self.builder_code
        if take_profit:
            tp = {
                "stop_price": take_profit["stop_price"],
                "limit_price": take_profit.get("limit_price", take_profit["stop_price"]),
                "client_order_id": take_profit.get("client_order_id", str(uuid.uuid4())),
            }
            payload["take_profit"] = tp
        if stop_loss:
            sl = {
                "stop_price": stop_loss["stop_price"],
                "limit_price": stop_loss.get("limit_price", stop_loss["stop_price"]),
                "client_order_id": stop_loss.get("client_order_id", str(uuid.uuid4())),
            }
            payload["stop_loss"] = sl
        return await self._post("/positions/tpsl", self._build_request(header, payload))

    async def cancel_order(self, order_id: str, symbol: str) -> dict:
        header = self._make_header("cancel_order")
        payload = {"order_id": order_id, "symbol": symbol}
        return await self._post("/orders/cancel", self._build_request(header, payload))

    async def cancel_all_orders(self, symbol: str | None = None) -> dict:
        header = self._make_header("cancel_all_orders")
        payload: dict[str, Any] = {}
        if symbol:
            payload["symbol"] = symbol
        return await self._post("/orders/cancel_all", self._build_request(header, payload))

    # ------------------------------------------------------------------
    # Account actions (signed)
    # ------------------------------------------------------------------

    async def approve_builder_code(self, builder_code: str, max_fee_rate: str) -> dict:
        header = self._make_header("approve_builder_code")
        payload = {"builder_code": builder_code, "max_fee_rate": max_fee_rate}
        return await self._post(
            "/account/builder_codes/approve",
            self._build_request(header, payload),
        )

    async def claim_referral_code(self, code: str) -> dict:
        """Claim a referral code for beta/whitelist access."""
        header = self._make_header("claim_referral_code")
        payload = {"code": code}
        return await self._post(
            "/referral/user/code/claim",
            self._build_request(header, payload),
        )

    async def request_withdraw(self, amount: str) -> dict:
        """Request a USDC withdrawal from Pacifica."""
        header = self._make_header("withdraw")
        payload = {"amount": amount}
        return await self._post(
            "/account/withdraw",
            self._build_request(header, payload),
        )

    async def register_agent_wallet(self, agent_wallet_public: str) -> dict:
        header = self._make_header("register_agent_wallet")
        payload = {"agent_wallet": agent_wallet_public}
        return await self._post(
            "/account/agent_wallets/register",
            self._build_request(header, payload),
        )

    # ------------------------------------------------------------------
    # Read-only endpoints (correct paths, auto-unwrapped)
    # ------------------------------------------------------------------

    async def get_markets_info(self) -> list:
        """GET /info — list of all tradable symbols with tick/lot/leverage info."""
        return await self._get("/info")

    async def get_positions(self, account: str | None = None) -> list:
        """GET /positions?account=..."""
        return await self._get("/positions", {"account": account or self.account})

    async def get_account_info(self, account: str | None = None) -> dict:
        """GET /account?account=..."""
        return await self._get("/account", {"account": account or self.account})

    async def get_open_orders(self, account: str | None = None) -> list:
        """GET /orders?account=..."""
        return await self._get("/orders", {"account": account or self.account})

    async def get_orderbook(self, symbol: str) -> dict:
        """GET /book?symbol=..."""
        return await self._get("/book", {"symbol": symbol})

    async def get_trades(self, symbol: str, limit: int = 20) -> list:
        """GET /trades?symbol=...&limit=... — public recent trades for a symbol."""
        return await self._get("/trades", {"symbol": symbol, "limit": limit})

    async def get_trades_history(
        self,
        account: str | None = None,
        symbol: str | None = None,
        limit: int = 50,
    ) -> list:
        """GET /trades/history?account=..."""
        params: dict[str, Any] = {"account": account or self.account, "limit": limit}
        if symbol:
            params["symbol"] = symbol
        return await self._get("/trades/history", params)

    async def get_orders_history(
        self,
        account: str | None = None,
        limit: int = 50,
    ) -> list:
        """GET /orders/history?account=..."""
        return await self._get(
            "/orders/history",
            {"account": account or self.account, "limit": limit},
        )

    async def get_builder_codes_approvals(self, account: str | None = None) -> list:
        return await self._get(
            "/account/builder_codes/approvals",
            {"account": account or self.account},
        )

    async def get_leaderboard(self) -> list:
        return await self._get("/leaderboard")


class PacificaAPIError(Exception):
    def __init__(self, status: int, data: Any):
        self.status = status
        self.data = data
        msg = data.get("error", str(data)) if isinstance(data, dict) else str(data)
        super().__init__(f"Pacifica API {status}: {msg}")
