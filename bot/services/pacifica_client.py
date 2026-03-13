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
            raw = await self._parse_response(resp)
            if resp.status >= 400 and not isinstance(raw, dict):
                raise PacificaAPIError(resp.status, raw)
            return self._unwrap(raw)

    async def _get(self, endpoint: str, params: dict | None = None) -> Any:
        session = await self._get_session()
        url = f"{PACIFICA_REST_URL}{endpoint}"
        logger.debug("GET %s %s", url, params)
        async with session.get(url, params=params) as resp:
            raw = await self._parse_response(resp)
            if resp.status >= 400 and not isinstance(raw, dict):
                raise PacificaAPIError(resp.status, raw)
            return self._unwrap(raw)

    @staticmethod
    async def _parse_response(resp) -> Any:
        """Parse response, handling text/plain error bodies from Pacifica."""
        ct = resp.content_type or ""
        if "json" in ct:
            return await resp.json()
        text = await resp.text()
        try:
            import json
            return json.loads(text)
        except (json.JSONDecodeError, ValueError):
            if resp.status >= 400:
                raise PacificaAPIError(resp.status, text)
            return text

    # ------------------------------------------------------------------
    # Order creation (all include builder_code automatically)
    # ------------------------------------------------------------------

    @staticmethod
    def _normalize_side(side: str) -> str:
        """Convert long/short/buy/sell to bid/ask for Pacifica API."""
        s = side.lower()
        if s in ("long", "buy", "bid"):
            return "bid"
        if s in ("short", "sell", "ask"):
            return "ask"
        return s

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
            "side": self._normalize_side(side),
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
        price: str | int | float,
        tif: str = "gtc",
        reduce_only: bool = False,
        client_order_id: str | None = None,
    ) -> dict:
        header = self._make_header("create_order")
        payload: dict[str, Any] = {
            "symbol": symbol,
            "side": self._normalize_side(side),
            "amount": amount,
            "price": str(price),
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
            "side": self._normalize_side(side),
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
        payload: dict[str, Any] = {
            "all_symbols": symbol is None,
            "exclude_reduce_only": False,
        }
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
        """Claim a referral code (e.g. 'Pacifica') via /referral/user/code/claim.

        When using agent wallet mode, we claim for the SIGNER (agent wallet)
        because Pacifica checks beta access on the signer, not the account.
        We also try to claim for the main account for completeness.
        """
        header = self._make_header("claim_referral_code")
        payload = {"code": code}

        if self.agent_wallet:
            # 1. Claim for the signer (agent wallet) — Pacifica checks beta on signer
            logger.info(
                "Claiming referral for signer %s (account=%s)",
                self.agent_wallet, self.account,
            )
            _, sig1 = sign_message(header, payload, self.keypair)
            body_signer = {
                "account": self.agent_wallet,
                "agent_wallet": None,
                "signature": sig1,
                "timestamp": header["timestamp"],
                "expiry_window": header["expiry_window"],
                **payload,
            }
            result = await self._post("/referral/user/code/claim", body_signer)
            logger.info("Referral claimed for signer %s", self.agent_wallet)

            # 2. Also claim for the main account (best effort, uses a second code)
            try:
                header2 = self._make_header("claim_referral_code")
                await self._post(
                    "/referral/user/code/claim",
                    self._build_request(header2, payload),
                )
                logger.info("Referral also claimed for account %s", self.account)
            except Exception as e:
                logger.debug("Main account referral claim (secondary): %s", e)

            return result

        return await self._post(
            "/referral/user/code/claim",
            self._build_request(header, payload),
        )

    async def claim_whitelist_code(self, code: str) -> dict:
        """Claim an access/invite code (e.g. '8V48ZGS7468AM5WD') via /whitelist/claim.

        Access codes use a different endpoint than referral codes.
        """
        header = self._make_header("claim_access_code")
        payload = {"code": code}

        if self.agent_wallet:
            logger.info(
                "Claiming whitelist for signer %s (account=%s)",
                self.agent_wallet, self.account,
            )
            _, sig1 = sign_message(header, payload, self.keypair)
            body_signer = {
                "account": self.agent_wallet,
                "agent_wallet": None,
                "signature": sig1,
                "timestamp": header["timestamp"],
                "expiry_window": header["expiry_window"],
                **payload,
            }
            result = await self._post("/whitelist/claim", body_signer)
            logger.info("Whitelist claimed for signer %s", self.agent_wallet)

            # Also claim for the main account (best effort)
            try:
                header2 = self._make_header("claim_access_code")
                await self._post(
                    "/whitelist/claim",
                    self._build_request(header2, payload),
                )
                logger.info("Whitelist also claimed for account %s", self.account)
            except Exception as e:
                logger.debug("Main account whitelist claim (secondary): %s", e)

            return result

        return await self._post(
            "/whitelist/claim",
            self._build_request(header, payload),
        )

    async def claim_beta_code(self, code: str) -> dict:
        """Try claiming a code — tries whitelist first, then referral.

        Pacifica has two separate endpoints:
        - /whitelist/claim for access/invite codes (16-char alphanumeric)
        - /referral/user/code/claim for referral codes (short names like 'Pacifica')
        """
        # Try whitelist endpoint first
        try:
            result = await self.claim_whitelist_code(code)
            logger.info("Code '%s' claimed via whitelist endpoint", code)
            return result
        except PacificaAPIError as e:
            if e.status == 404 or "not found" in str(e).lower():
                logger.debug("Code '%s' not on whitelist, trying referral...", code)
            else:
                raise

        # Fall back to referral endpoint
        result = await self.claim_referral_code(code)
        logger.info("Code '%s' claimed via referral endpoint", code)
        return result

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

    async def get_leaderboard(self, limit: int = 100) -> list:
        """GET /leaderboard?limit=... — limit must be 10, 100, or 25000."""
        return await self._get("/leaderboard", {"limit": limit})

    async def get_kline(
        self,
        symbol: str,
        interval: str = "1h",
        start_time: int | None = None,
        end_time: int | None = None,
    ) -> list:
        """GET /kline — OHLCV candle data.

        interval: 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 8h, 12h, 1d
        start_time / end_time: milliseconds epoch
        """
        import time as _time
        if end_time is None:
            end_time = int(_time.time() * 1000)
        if start_time is None:
            # Default: last 48 candles
            intervals_ms = {
                "1m": 60_000, "3m": 180_000, "5m": 300_000, "15m": 900_000,
                "30m": 1_800_000, "1h": 3_600_000, "2h": 7_200_000,
                "4h": 14_400_000, "8h": 28_800_000, "12h": 43_200_000, "1d": 86_400_000,
            }
            ms = intervals_ms.get(interval, 3_600_000)
            start_time = end_time - ms * 48
        params = {
            "symbol": symbol, "interval": interval,
            "start_time": start_time, "end_time": end_time,
        }
        return await self._get("/kline", params)

    async def get_prices(self) -> list:
        """GET /info/prices — all current market prices."""
        return await self._get("/info/prices")


class PacificaAPIError(Exception):
    def __init__(self, status: int, data: Any):
        self.status = status
        self.data = data
        msg = data.get("error", str(data)) if isinstance(data, dict) else str(data)
        super().__init__(f"Pacifica API {status}: {msg}")
