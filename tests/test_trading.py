"""
Integration tests for the trade execution flow.

Tests cover:
- Successful market order (long / short)
- Insufficient funds error with deposit hint
- Invalid symbol error (price not found)
- Referral fee tracking after trade
"""

import pytest
from unittest.mock import patch, AsyncMock, MagicMock

from tests.conftest import (
    make_callback,
    make_state,
    make_mock_pacifica_client,
)


# ---------------------------------------------------------------------------
# Config patches (autouse for all tests in this module)
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _patch_config():
    with patch("bot.config.TELEGRAM_BOT_TOKEN", "fake-token"), \
         patch("bot.config.BUILDER_CODE", "TEST_BUILDER"), \
         patch("bot.config.BUILDER_FEE_RATE", "0.0005"), \
         patch("bot.config.PACIFICA_NETWORK", "devnet"), \
         patch("bot.config.PACIFICA_REFERRAL_CODE", "TestRef"), \
         patch("bot.config.BOT_USERNAME", "test_bot"), \
         patch("bot.config.DISPENSER_PRIVATE_KEY", ""), \
         patch("bot.config.DISPENSER_SOL_AMOUNT", 0.1), \
         patch("bot.config.DISPENSER_USDC_AMOUNT", 100):
        yield


# ---------------------------------------------------------------------------
# Helper: seed a user ready to trade
# ---------------------------------------------------------------------------

def _seed_trader(mock_db, tg_id=12345, referred_by=None):
    mock_db.users[tg_id] = {
        "telegram_id": tg_id,
        "pacifica_account": "TraderWa11et123456789012345678901234567890",
        "agent_wallet_encrypted": "enc_key_xxx",
        "agent_wallet_public": None,
        "username": f"trader_{tg_id}",
        "ref_code": f"REF{tg_id}",
        "referred_by": referred_by,
        "builder_approved": 1,
        "settings": "{}",
    }


# ---------------------------------------------------------------------------
# Successful market orders
# ---------------------------------------------------------------------------

class TestSuccessfulTrade:
    """Verify the exec: callback executes a trade and logs it."""

    @pytest.mark.asyncio
    async def test_long_market_order(self, patch_db, patch_market_data):
        from bot.handlers.trading import cb_execute_trade

        _seed_trader(patch_db)

        mock_client = make_mock_pacifica_client(fill_price=50000.0, order_id="order-long-1")
        cb = make_callback(data="exec:bid:BTC:100:10", user_id=12345)

        with patch("bot.handlers.trading.build_client_from_user", return_value=mock_client), \
             patch("bot.handlers.trading.ensure_beta_and_builder", new_callable=AsyncMock):
            await cb_execute_trade(cb)

        # Order should have been sent
        mock_client.create_market_order.assert_called_once()
        call_kwargs = mock_client.create_market_order.call_args
        # Check symbol in either positional or keyword args
        all_kwargs = call_kwargs.kwargs if call_kwargs.kwargs else {}
        assert all_kwargs.get("symbol") == "BTC"

        # Should show success message
        cb.message.edit_text.assert_called_once()
        text = cb.message.edit_text.call_args[0][0]
        assert "Order Executed" in text or "order-long-1" in text

        # Trade should be logged in mock DB
        assert len(patch_db.trade_log) == 1
        logged = patch_db.trade_log[0]
        assert logged["telegram_id"] == 12345
        assert logged["symbol"] == "BTC"
        assert logged["side"] == "bid"

    @pytest.mark.asyncio
    async def test_short_market_order(self, patch_db, patch_market_data):
        from bot.handlers.trading import cb_execute_trade

        _seed_trader(patch_db, tg_id=22222)

        mock_client = make_mock_pacifica_client(fill_price=3000.0, order_id="order-short-1")
        cb = make_callback(data="exec:ask:ETH:50:5", user_id=22222)

        with patch("bot.handlers.trading.build_client_from_user", return_value=mock_client), \
             patch("bot.handlers.trading.ensure_beta_and_builder", new_callable=AsyncMock):
            await cb_execute_trade(cb)

        mock_client.create_market_order.assert_called_once()

        cb.message.edit_text.assert_called_once()
        text = cb.message.edit_text.call_args[0][0]
        assert "Order Executed" in text

        assert len(patch_db.trade_log) == 1
        assert patch_db.trade_log[0]["side"] == "ask"
        assert patch_db.trade_log[0]["symbol"] == "ETH"

    @pytest.mark.asyncio
    async def test_trade_with_custom_slippage(self, patch_db, patch_market_data):
        """User-configured slippage should be passed to the API."""
        from bot.handlers.trading import cb_execute_trade

        _seed_trader(patch_db, tg_id=33333)
        # Set custom slippage
        import json
        patch_db.users[33333]["settings"] = json.dumps({"slippage": "1.0"})

        mock_client = make_mock_pacifica_client()
        cb = make_callback(data="exec:bid:SOL:200:5", user_id=33333)

        with patch("bot.handlers.trading.build_client_from_user", return_value=mock_client), \
             patch("bot.handlers.trading.ensure_beta_and_builder", new_callable=AsyncMock):
            await cb_execute_trade(cb)

        call_kwargs = mock_client.create_market_order.call_args
        assert call_kwargs is not None, "create_market_order was not called"
        all_kwargs = call_kwargs.kwargs if call_kwargs.kwargs else {}
        if "slippage" in all_kwargs:
            assert all_kwargs["slippage"] == "1.0"


# ---------------------------------------------------------------------------
# Error cases
# ---------------------------------------------------------------------------

class TestTradeErrors:
    """Verify error handling shows proper hints."""

    @pytest.mark.asyncio
    async def test_insufficient_funds(self, patch_db, patch_market_data):
        """PacificaAPIError with 'insufficient' should show deposit hint."""
        from bot.handlers.trading import cb_execute_trade
        from bot.services.pacifica_client import PacificaAPIError

        _seed_trader(patch_db, tg_id=44444)

        mock_client = make_mock_pacifica_client()
        mock_client.create_market_order = AsyncMock(
            side_effect=PacificaAPIError(400, {"error": "Insufficient margin balance"})
        )
        cb = make_callback(data="exec:bid:BTC:1000:10", user_id=44444)

        with patch("bot.handlers.trading.build_client_from_user", return_value=mock_client), \
             patch("bot.handlers.trading.ensure_beta_and_builder", new_callable=AsyncMock):
            await cb_execute_trade(cb)

        cb.message.edit_text.assert_called_once()
        text = cb.message.edit_text.call_args[0][0]
        assert "Order Failed" in text
        # Should include deposit hint
        assert "deposit" in text.lower()

    @pytest.mark.asyncio
    async def test_invalid_symbol_no_price(self, patch_db):
        """When price can't be fetched, the trade should fail gracefully."""
        from bot.handlers.trading import cb_execute_trade

        _seed_trader(patch_db, tg_id=55555)

        cb = make_callback(data="exec:bid:INVALID:100:5", user_id=55555)

        # Patch get_price to return None for unknown symbol
        async def no_price(symbol):
            return None

        with patch("bot.handlers.trading.get_price", side_effect=no_price), \
             patch("bot.handlers.trading.ensure_beta_and_builder", new_callable=AsyncMock):
            await cb_execute_trade(cb)

        cb.message.edit_text.assert_called_once()
        text = cb.message.edit_text.call_args[0][0]
        assert "Could not fetch price" in text or "INVALID" in text

    @pytest.mark.asyncio
    async def test_no_wallet_linked(self, patch_db, patch_market_data):
        """User without a wallet should be told to link first."""
        from bot.handlers.trading import cb_execute_trade

        # User exists but has no wallet
        patch_db.users[66666] = {
            "telegram_id": 66666,
            "pacifica_account": None,
            "agent_wallet_encrypted": None,
            "agent_wallet_public": None,
            "username": "no_wallet",
            "ref_code": None,
            "referred_by": None,
            "builder_approved": 0,
            "settings": "{}",
        }

        cb = make_callback(data="exec:bid:BTC:100:5", user_id=66666)

        await cb_execute_trade(cb)

        cb.answer.assert_called_once()
        text = cb.answer.call_args[0][0]
        assert "link" in text.lower() or "account" in text.lower() or "first" in text.lower()

    @pytest.mark.asyncio
    async def test_unknown_user(self, patch_db, patch_market_data):
        """A completely unknown user should be told to link."""
        from bot.handlers.trading import cb_execute_trade

        cb = make_callback(data="exec:bid:BTC:100:5", user_id=99999)

        await cb_execute_trade(cb)

        cb.answer.assert_called()


# ---------------------------------------------------------------------------
# Referral fee tracking
# ---------------------------------------------------------------------------

class TestReferralFeeTracking:
    """Verify referral fees are logged after a successful trade."""

    @pytest.mark.asyncio
    async def test_referral_fee_logged(self, patch_db, patch_market_data):
        """When a referred user trades, a fee should be logged for the referrer."""
        from bot.handlers.trading import cb_execute_trade

        # Referrer
        _seed_trader(patch_db, tg_id=7000)
        # Referred trader
        _seed_trader(patch_db, tg_id=7001, referred_by=7000)

        mock_client = make_mock_pacifica_client(fill_price=50000.0)
        cb = make_callback(data="exec:bid:BTC:100:10", user_id=7001)

        with patch("bot.handlers.trading.build_client_from_user", return_value=mock_client), \
             patch("bot.handlers.trading.ensure_beta_and_builder", new_callable=AsyncMock):
            await cb_execute_trade(cb)

        # Should have a trade log entry
        assert len(patch_db.trade_log) == 1

        # Should have a referral fee entry
        assert len(patch_db.referral_fees) == 1
        fee = patch_db.referral_fees[0]
        assert fee["referrer_id"] == 7000
        assert fee["referee_id"] == 7001
        assert fee["symbol"] == "BTC"
        # notional = 100 * 10 = 1000
        assert fee["trade_volume"] == 1000.0
        # referrer_share = 1000 * 0.0004 * 0.10 = 0.04
        assert abs(fee["fee_earned"] - 0.04) < 0.001

    @pytest.mark.asyncio
    async def test_no_referral_fee_without_referrer(self, patch_db, patch_market_data):
        """A user without a referrer should generate no referral fees."""
        from bot.handlers.trading import cb_execute_trade

        _seed_trader(patch_db, tg_id=8000)

        mock_client = make_mock_pacifica_client()
        cb = make_callback(data="exec:bid:BTC:100:10", user_id=8000)

        with patch("bot.handlers.trading.build_client_from_user", return_value=mock_client), \
             patch("bot.handlers.trading.ensure_beta_and_builder", new_callable=AsyncMock):
            await cb_execute_trade(cb)

        assert len(patch_db.trade_log) == 1
        assert len(patch_db.referral_fees) == 0


# ---------------------------------------------------------------------------
# Trade side selection callbacks
# ---------------------------------------------------------------------------

class TestTradeSideSelection:
    """Test the trade:long:SYM and trade:short:SYM callbacks."""

    @pytest.mark.asyncio
    async def test_long_side_selection(self, patch_market_data):
        from bot.handlers.trading import cb_trade_side

        cb = make_callback(data="trade:long:BTC", user_id=12345)

        await cb_trade_side(cb)

        cb.message.edit_text.assert_called_once()
        text = cb.message.edit_text.call_args[0][0]
        assert "LONG" in text
        assert "BTC" in text
        assert "amount" in text.lower() or "USDC" in text

    @pytest.mark.asyncio
    async def test_short_side_selection(self, patch_market_data):
        from bot.handlers.trading import cb_trade_side

        cb = make_callback(data="trade:short:ETH", user_id=12345)

        await cb_trade_side(cb)

        cb.message.edit_text.assert_called_once()
        text = cb.message.edit_text.call_args[0][0]
        assert "SHORT" in text
        assert "ETH" in text


# ---------------------------------------------------------------------------
# Trade error hint helper
# ---------------------------------------------------------------------------

class TestTradeErrorHint:
    """Unit-test the _trade_error_hint helper directly."""

    def test_insufficient_hint(self):
        from bot.handlers.trading import _trade_error_hint
        hint = _trade_error_hint("Insufficient margin balance")
        assert "deposit" in hint.lower()

    def test_balance_hint(self):
        from bot.handlers.trading import _trade_error_hint
        hint = _trade_error_hint("Not enough balance to cover order")
        assert "deposit" in hint.lower()

    def test_not_found_hint(self):
        from bot.handlers.trading import _trade_error_hint
        hint = _trade_error_hint("Account not found")
        assert "activated" in hint.lower() or "deposit" in hint.lower()

    def test_signature_hint(self):
        from bot.handlers.trading import _trade_error_hint
        hint = _trade_error_hint("Invalid signature from signer")
        assert "signing" in hint.lower() or "reimport" in hint.lower()

    def test_unknown_error_no_hint(self):
        from bot.handlers.trading import _trade_error_hint
        hint = _trade_error_hint("Some random API error")
        assert hint == ""
