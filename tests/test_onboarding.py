"""
Integration tests for the /start -> wallet -> username onboarding flow.

Tests cover:
- New user sees onboarding prompt
- Import wallet flow
- Generate wallet flow
- Username validation (too short, taken, valid)
- Referral deeplink parsing
"""

import pytest
from unittest.mock import patch, AsyncMock, MagicMock

from tests.conftest import make_message, make_callback, make_state


# We need to patch heavy imports that the handler module pulls in at import time.
# Patch config values before importing handlers.
@pytest.fixture(autouse=True)
def _patch_config():
    """Provide safe config defaults so the handler module can be imported."""
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
# /start — new user (no DB record)
# ---------------------------------------------------------------------------

class TestStartNewUser:
    """When a brand-new user sends /start they should see the wallet setup prompt."""

    @pytest.mark.asyncio
    async def test_new_user_sees_onboarding(self, patch_db):
        from bot.handlers.start import cmd_start

        msg = make_message(text="/start", user_id=111)
        state = make_state()

        await cmd_start(msg, state)

        msg.answer.assert_called_once()
        text = msg.answer.call_args[0][0]
        assert "Trident" in text or "Import" in text or "wallet" in text.lower()

    @pytest.mark.asyncio
    async def test_new_user_with_referral_deeplink(self, patch_db):
        """A deeplink like /start ref_ABC123 should store the ref_code in FSM."""
        from bot.handlers.start import cmd_start

        state = make_state()
        msg = make_message(text="/start ref_ABC123", user_id=222)

        await cmd_start(msg, state)

        data = await state.get_data()
        assert data.get("ref_code") == "ABC123"


# ---------------------------------------------------------------------------
# /start — existing user (already onboarded)
# ---------------------------------------------------------------------------

class TestStartExistingUser:
    """Existing users with a wallet and username should see the main menu."""

    @pytest.mark.asyncio
    async def test_existing_user_sees_menu(self, patch_db, patch_client):
        from bot.handlers.start import cmd_start

        # Seed an existing user
        patch_db.users[333] = {
            "telegram_id": 333,
            "pacifica_account": "SomeWa11etAddr",
            "agent_wallet_encrypted": "enc_key",
            "agent_wallet_public": None,
            "username": "veteran",
            "ref_code": "VETREF",
            "referred_by": None,
            "builder_approved": 1,
            "settings": "{}",
        }

        msg = make_message(text="/start", user_id=333, first_name="Vet")
        state = make_state()

        await cmd_start(msg, state)

        msg.answer.assert_called_once()
        text = msg.answer.call_args[0][0]
        # Should greet the user and show wallet info
        assert "veteran" in text or "Vet" in text

    @pytest.mark.asyncio
    async def test_existing_user_without_username_prompted(self, patch_db):
        """User with wallet but no username should be asked to set one."""
        from bot.handlers.start import cmd_start, OnboardUsernameStates

        patch_db.users[444] = {
            "telegram_id": 444,
            "pacifica_account": "Wa11etNoName",
            "agent_wallet_encrypted": "enc",
            "agent_wallet_public": None,
            "username": None,
            "ref_code": None,
            "referred_by": None,
            "builder_approved": 0,
            "settings": "{}",
        }

        msg = make_message(text="/start", user_id=444)
        state = make_state()

        await cmd_start(msg, state)

        # Should transition to username prompt state
        current = await state.get_state()
        assert current == OnboardUsernameStates.waiting_username

    @pytest.mark.asyncio
    async def test_existing_user_referral_applied(self, patch_db):
        """Existing user clicking a ref link gets the referrer recorded."""
        from bot.handlers.start import cmd_start

        # Referrer
        patch_db.users[500] = {
            "telegram_id": 500,
            "pacifica_account": "ReferrerWallet",
            "agent_wallet_encrypted": "enc",
            "agent_wallet_public": None,
            "username": "referrer",
            "ref_code": "REFCODE",
            "referred_by": None,
            "builder_approved": 1,
            "settings": "{}",
        }
        # Existing user without referrer, but with wallet+username
        patch_db.users[501] = {
            "telegram_id": 501,
            "pacifica_account": "ExistingWallet",
            "agent_wallet_encrypted": "enc",
            "agent_wallet_public": None,
            "username": "existing",
            "ref_code": None,
            "referred_by": None,
            "builder_approved": 1,
            "settings": "{}",
        }

        msg = make_message(text="/start ref_REFCODE", user_id=501)
        state = make_state()

        # Patch client to avoid real API call in the /start for existing user
        with patch("bot.models.user.build_client_from_user") as mock_build:
            client_mock = AsyncMock()
            client_mock.get_account_info = AsyncMock(return_value={"balance": 100, "account_equity": 100})
            client_mock.get_positions = AsyncMock(return_value=[])
            client_mock.close = AsyncMock()
            mock_build.return_value = client_mock

            await cmd_start(msg, state)

        assert patch_db.users[501]["referred_by"] == 500


# ---------------------------------------------------------------------------
# Import wallet flow
# ---------------------------------------------------------------------------

class TestImportWallet:
    """Test the import wallet callback and private key handling."""

    @pytest.mark.asyncio
    async def test_import_callback_sets_state(self):
        from bot.handlers.start import onboard_import, ImportStates

        cb = make_callback(data="onboard:import", user_id=600)
        state = make_state()

        await onboard_import(cb, state)

        current = await state.get_state()
        assert current == ImportStates.waiting_private_key
        cb.answer.assert_called_once()

    @pytest.mark.asyncio
    async def test_import_invalid_key_short(self, patch_db, patch_wallet):
        """A key that's too short should be rejected."""
        from bot.handlers.start import msg_import_key

        msg = make_message(text="short", user_id=600)
        state = make_state()

        await msg_import_key(msg, state)

        msg.answer.assert_called_once()
        text = msg.answer.call_args[0][0]
        assert "doesn't look like" in text.lower() or "valid" in text.lower()

    @pytest.mark.asyncio
    async def test_import_valid_key(self, patch_db, patch_wallet):
        """A valid-looking key should create the user and transition to username."""
        from bot.handlers.start import msg_import_key, OnboardUsernameStates

        fake_pub, fake_enc = patch_wallet
        fake_key = "5" * 88  # looks like a base58 key

        msg = make_message(text=fake_key, user_id=700)
        state = make_state()

        # Patch the background tasks that run after wallet setup
        with patch("bot.handlers.start._auto_dispense_sol", new_callable=AsyncMock), \
             patch("bot.handlers.start._auto_claim_setup", new_callable=AsyncMock):
            await msg_import_key(msg, state)

        # Key message should be deleted for security
        msg.delete.assert_called_once()

        # User should be created in the mock DB
        user = await patch_db.get_user(700)
        assert user is not None
        assert user["pacifica_account"] == fake_pub

        # State should transition to username prompt
        current = await state.get_state()
        assert current == OnboardUsernameStates.waiting_username

    @pytest.mark.asyncio
    async def test_import_wallet_already_used(self, patch_db, patch_wallet):
        """Importing a wallet that belongs to another user should be rejected."""
        from bot.handlers.start import msg_import_key

        fake_pub, fake_enc = patch_wallet

        # Another user already has this wallet
        patch_db.users[800] = {
            "telegram_id": 800,
            "pacifica_account": fake_pub,
            "agent_wallet_encrypted": "enc",
            "agent_wallet_public": None,
            "username": "other_user",
            "ref_code": None,
            "referred_by": None,
            "builder_approved": 0,
            "settings": "{}",
        }

        fake_key = "5" * 88
        msg = make_message(text=fake_key, user_id=900)
        state = make_state()

        await msg_import_key(msg, state)

        msg.answer.assert_called()
        text = msg.answer.call_args[0][0]
        assert "already linked" in text.lower() or "already" in text.lower()


# ---------------------------------------------------------------------------
# Generate wallet flow
# ---------------------------------------------------------------------------

class TestGenerateWallet:
    """Test the generate wallet callback."""

    @pytest.mark.asyncio
    async def test_generate_creates_wallet(self, patch_db, patch_wallet):
        from bot.handlers.start import onboard_generate, OnboardUsernameStates

        fake_pub, fake_enc = patch_wallet
        cb = make_callback(data="onboard:generate", user_id=1000)
        state = make_state()

        with patch("bot.handlers.start._auto_dispense_sol", new_callable=AsyncMock), \
             patch("bot.handlers.start._auto_claim_setup", new_callable=AsyncMock):
            await onboard_generate(cb, state)

        user = await patch_db.get_user(1000)
        assert user is not None
        assert user["pacifica_account"] == fake_pub

        current = await state.get_state()
        assert current == OnboardUsernameStates.waiting_username

    @pytest.mark.asyncio
    async def test_generate_existing_wallet_rejected(self, patch_db, patch_wallet):
        """User who already has a wallet should not generate a new one."""
        from bot.handlers.start import onboard_generate

        patch_db.users[1100] = {
            "telegram_id": 1100,
            "pacifica_account": "AlreadyHasWa11et",
            "agent_wallet_encrypted": "enc",
            "agent_wallet_public": None,
            "username": "existing",
            "ref_code": None,
            "referred_by": None,
            "builder_approved": 0,
            "settings": "{}",
        }

        cb = make_callback(data="onboard:generate", user_id=1100)
        state = make_state()

        await onboard_generate(cb, state)

        cb.message.edit_text.assert_called_once()
        text = cb.message.edit_text.call_args[0][0]
        assert "already" in text.lower()


# ---------------------------------------------------------------------------
# Username validation
# ---------------------------------------------------------------------------

class TestUsernameValidation:
    """Test the onboarding username step."""

    @pytest.mark.asyncio
    async def test_username_too_short(self, patch_db):
        from bot.handlers.start import msg_onboard_username

        msg = make_message(text="ab", user_id=1200)
        state = make_state()

        await msg_onboard_username(msg, state)

        msg.answer.assert_called_once()
        text = msg.answer.call_args[0][0]
        assert "invalid" in text.lower() or "3-15" in text

    @pytest.mark.asyncio
    async def test_username_too_long(self, patch_db):
        from bot.handlers.start import msg_onboard_username

        msg = make_message(text="a" * 20, user_id=1200)
        state = make_state()

        await msg_onboard_username(msg, state)

        msg.answer.assert_called_once()
        text = msg.answer.call_args[0][0]
        assert "invalid" in text.lower() or "3-15" in text

    @pytest.mark.asyncio
    async def test_username_invalid_chars(self, patch_db):
        from bot.handlers.start import msg_onboard_username

        msg = make_message(text="bad name!", user_id=1200)
        state = make_state()

        await msg_onboard_username(msg, state)

        msg.answer.assert_called_once()
        text = msg.answer.call_args[0][0]
        assert "invalid" in text.lower() or "3-15" in text

    @pytest.mark.asyncio
    async def test_username_taken(self, patch_db):
        """A username already used by another user should be rejected."""
        from bot.handlers.start import msg_onboard_username

        patch_db.users[1300] = {
            "telegram_id": 1300,
            "pacifica_account": "W1",
            "agent_wallet_encrypted": "enc",
            "agent_wallet_public": None,
            "username": "alpha",
            "ref_code": None,
            "referred_by": None,
            "builder_approved": 0,
            "settings": "{}",
        }

        msg = make_message(text="alpha", user_id=1400)
        state = make_state()

        await msg_onboard_username(msg, state)

        msg.answer.assert_called_once()
        text = msg.answer.call_args[0][0]
        assert "taken" in text.lower()

    @pytest.mark.asyncio
    async def test_username_valid(self, patch_db):
        """A valid, untaken username should be saved and the user welcomed."""
        from bot.handlers.start import msg_onboard_username

        patch_db.users[1500] = {
            "telegram_id": 1500,
            "pacifica_account": "ValidWallet123",
            "agent_wallet_encrypted": "enc",
            "agent_wallet_public": None,
            "username": None,
            "ref_code": None,
            "referred_by": None,
            "builder_approved": 0,
            "settings": "{}",
        }

        msg = make_message(text="trader_one", user_id=1500)
        state = make_state()

        await msg_onboard_username(msg, state)

        # Username should be saved
        assert patch_db.users[1500]["username"] == "trader_one"
        # Ref code should be generated
        assert patch_db.users[1500]["ref_code"] is not None

        # FSM state should transition to referral prompt (or be cleared if already referred)
        current = await state.get_state()
        assert current is None or "waiting_referral" in str(current)

        # Welcome/referral message should contain the username
        msg.answer.assert_called_once()
        text = msg.answer.call_args[0][0]
        assert "trader_one" in text

    @pytest.mark.asyncio
    async def test_username_case_insensitive_taken(self, patch_db):
        """Username check should be case-insensitive."""
        from bot.handlers.start import msg_onboard_username

        patch_db.users[1600] = {
            "telegram_id": 1600,
            "pacifica_account": "W2",
            "agent_wallet_encrypted": "enc",
            "agent_wallet_public": None,
            "username": "Trader",
            "ref_code": None,
            "referred_by": None,
            "builder_approved": 0,
            "settings": "{}",
        }

        msg = make_message(text="trader", user_id=1700)
        state = make_state()

        await msg_onboard_username(msg, state)

        text = msg.answer.call_args[0][0]
        assert "taken" in text.lower()


# ---------------------------------------------------------------------------
# Referral deeplink during wallet setup
# ---------------------------------------------------------------------------

class TestReferralDuringOnboarding:
    """Verify that ref_code from deeplink is applied after wallet setup."""

    @pytest.mark.asyncio
    async def test_referral_applied_after_import(self, patch_db, patch_wallet):
        from bot.handlers.start import msg_import_key

        fake_pub, fake_enc = patch_wallet

        # Create the referrer
        patch_db.users[2000] = {
            "telegram_id": 2000,
            "pacifica_account": "ReferrerW",
            "agent_wallet_encrypted": "enc",
            "agent_wallet_public": None,
            "username": "referrer2",
            "ref_code": "XYZREF",
            "referred_by": None,
            "builder_approved": 1,
            "settings": "{}",
        }

        fake_key = "5" * 88
        msg = make_message(text=fake_key, user_id=2001)
        # FSM state has ref_code from the /start deeplink
        state = make_state(data={"ref_code": "XYZREF"})

        with patch("bot.handlers.start._auto_dispense_sol", new_callable=AsyncMock), \
             patch("bot.handlers.start._auto_claim_setup", new_callable=AsyncMock):
            await msg_import_key(msg, state)

        user = await patch_db.get_user(2001)
        assert user is not None
        assert user["referred_by"] == 2000

    @pytest.mark.asyncio
    async def test_self_referral_ignored(self, patch_db, patch_wallet):
        """A user should not be able to refer themselves."""
        from bot.handlers.start import msg_import_key

        fake_pub, fake_enc = patch_wallet

        # The user IS the referrer (same telegram_id)
        patch_db.users[3000] = {
            "telegram_id": 3000,
            "pacifica_account": "SelfRefW",
            "agent_wallet_encrypted": "enc",
            "agent_wallet_public": None,
            "username": "selfref",
            "ref_code": "SELFREF",
            "referred_by": None,
            "builder_approved": 1,
            "settings": "{}",
        }

        fake_key = "5" * 88
        msg = make_message(text=fake_key, user_id=3000)
        state = make_state(data={"ref_code": "SELFREF"})

        with patch("bot.handlers.start._auto_dispense_sol", new_callable=AsyncMock), \
             patch("bot.handlers.start._auto_claim_setup", new_callable=AsyncMock):
            await msg_import_key(msg, state)

        user = await patch_db.get_user(3000)
        # referred_by should remain None (self-referral blocked)
        assert user["referred_by"] is None
