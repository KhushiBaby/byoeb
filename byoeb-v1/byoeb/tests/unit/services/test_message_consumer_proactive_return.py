"""
Unit tests for MessageConsmerService routing (byoeb.services.chat.message_consumer).

These tests cover proactive return / onboarding routing: users with incomplete
profile (user_type=None, user_language=None) or replying to an onboarding-step
bot message (USER_TYPE / LANGUAGE_SELECTION / CONSENT) must be routed to
onboarding (onboard_convs), not to the "already registered" flow (conversations).

Scenarios 1–3 from PR_PROACTIVE_RETURN_SCENARIO_VALIDATION.md. The existing
test_onboarding.py in this package tests the onboarding flow module
(byoeb.services.user.onboarding), not message consumer routing.

Tests use asyncio.run() so they run without pytest-asyncio (e.g. from repo root).
"""

import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock

from byoeb_core.models.byoeb.user import User
from byoeb_core.models.byoeb.message_context import (
    ByoebMessageContext,
    MessageContext,
    ReplyContext,
    MessageTypes,
)
from byoeb.services.channel.base import BaseChannelService
from byoeb.services.chat.message_consumer import MessageConsmerService
from byoeb.services.databases.mongo_db import UserMongoDBService, MessageMongoDBService
import byoeb.services.chat.constants as constants

class DummyChannelService(BaseChannelService):
    def __init__(self):
        self._prepared = None
        self._sent = None

    def prepare_requests(self, byoeb_message):
        return [{}]

    def prepare_reaction_requests(self, message_reactions):
        return []

    async def send_requests(self, requests):
        return ["RESP"], ["MSG-ID-1"]

    def create_conv(self, byoeb_user_message, responses):
        return [ByoebMessageContext(channel_type="whatsapp")]

    def create_cross_conv(self, byoeb_user_message, byoeb_expert_message, user_responses, expert_responses):
        return []

    async def amark_read(self, messages):
        return []


def make_user(
    phone_number_id: str = "919000000001",
    user_id: str = "user-1",
    user_type: str | None = "asha",
    user_language: str | None = "en",
) -> User:
    return User(
        user_id=user_id,
        phone_number_id=phone_number_id,
        user_type=user_type,
        user_language=user_language,
        user_name="Test",
        test_user=False,
        experts={},
        audience=[],
        additional_info={},
        created_timestamp=0,
        activity_timestamp=0,
        last_conversations=[],
    )


def make_message(
    user: User,
    message_source_text: str = "hello",
    message_id: str = "msg-1",
    reply_id: str | None = None,
) -> ByoebMessageContext:
    return ByoebMessageContext(
        channel_type="whatsapp",
        message_category="user_to_bot",
        user=user,
        message_context=MessageContext(
            message_id=message_id,
            message_type=MessageTypes.REGULAR_TEXT.value,
            message_source_text=message_source_text,
            additional_info={},
        ),
        reply_context=ReplyContext(reply_id=reply_id) if reply_id is not None else ReplyContext(),
    )


def make_bot_message(message_category: str, message_id: str = "bot-1") -> ByoebMessageContext:
    """Minimal bot message for reply context (message_category = user_type / language_selection / consent)."""
    user = make_user(phone_number_id="919000000000", user_id="bot-user")
    return ByoebMessageContext(
        channel_type="whatsapp",
        message_category=message_category,
        user=user,
        message_context=MessageContext(
            message_id=message_id,
            message_type=MessageTypes.REGULAR_TEXT.value,
            message_source_text="",
            additional_info={},
        ),
        reply_context=ReplyContext(),
    )


@pytest.fixture
def mock_user_db():
    db = MagicMock(spec=UserMongoDBService)
    db.get_users = AsyncMock(return_value=[])
    return db


@pytest.fixture
def mock_message_db():
    db = MagicMock(spec=MessageMongoDBService)
    db.get_bot_messages_by_ids = AsyncMock(return_value=[])
    return db


@pytest.fixture
def service(mock_user_db, mock_message_db):
    return MessageConsmerService(
        config=MagicMock(),
        user_db_service=mock_user_db,
        message_db_service=mock_message_db,
        channel_service=DummyChannelService(),
    )


async def _create_conversations(service: MessageConsmerService, messages: list) -> tuple:
    """Call private __create_conversations (used by consume)."""
    method = getattr(service, "_MessageConsmerService__create_conversations")
    return await method(messages)


# ---------- Scenario 1: user_type=None -> onboarding ----------


def test_user_with_user_type_none_and_non_onboarding_message_goes_to_onboarding(
    service, mock_user_db
):
    """Scenario 1: User exists with user_type=None; non-onboarding message -> must go to onboarding."""
    async def run():
        user = make_user(phone_number_id="919111111111", user_id="u1", user_type=None, user_language="en")
        mock_user_db.get_users = AsyncMock(return_value=[user])
        msg = make_message(user, message_source_text="hello", message_id="m1")
        msg.user = user
        conversations, onboard_convs = await _create_conversations(service, [msg])
        assert len(onboard_convs) == 1, "Expected message to be routed to onboarding when user_type is None"
        assert onboard_convs[0].message_context.message_id == "m1"
        assert len(conversations) == 0
    asyncio.run(run())


def test_user_with_user_type_none_and_onboarding_message_with_user_id_goes_to_conversations(
    service, mock_user_db
):
    """When user has user_type=None but sends onboarding message and has user_id -> already registered path (conversations)."""
    async def run():
        user = make_user(phone_number_id="919222222222", user_id="u2", user_type=None, user_language="en")
        mock_user_db.get_users = AsyncMock(return_value=[user])
        msg = make_message(user, message_source_text="onboard asha", message_id="m2")
        msg.user = user
        conversations, onboard_convs = await _create_conversations(service, [msg])
        assert len(conversations) == 1, "Already registered user sending onboarding message -> conversations"
        assert len(onboard_convs) == 0
    asyncio.run(run())


# ---------- Scenario 2: user_language=None -> onboarding ----------


def test_user_with_user_language_none_and_non_onboarding_message_goes_to_onboarding(
    service, mock_user_db
):
    """Scenario 2: User exists with user_language=None; non-onboarding message -> must go to onboarding."""
    async def run():
        user = make_user(
            phone_number_id="919333333333", user_id="u3", user_type="asha", user_language=None
        )
        mock_user_db.get_users = AsyncMock(return_value=[user])
        msg = make_message(user, message_source_text="what is antra?", message_id="m3")
        msg.user = user
        conversations, onboard_convs = await _create_conversations(service, [msg])
        assert len(onboard_convs) == 1, "Expected message to be routed to onboarding when user_language is None"
        assert onboard_convs[0].message_context.message_id == "m3"
        assert len(conversations) == 0
    asyncio.run(run())


def test_user_with_user_language_none_and_onboarding_message_with_user_id_goes_to_conversations(
    service, mock_user_db
):
    """When user has user_language=None but sends onboarding message and has user_id -> conversations."""
    async def run():
        user = make_user(
            phone_number_id="919444444444", user_id="u4", user_type="asha", user_language=None
        )
        mock_user_db.get_users = AsyncMock(return_value=[user])
        msg = make_message(user, message_source_text="onboard-asha", message_id="m4")
        msg.user = user
        conversations, onboard_convs = await _create_conversations(service, [msg])
        assert len(conversations) == 1
        assert len(onboard_convs) == 0
    asyncio.run(run())


# ---------- Scenario 3: Reply to CONSENT / USER_TYPE / LANGUAGE_SELECTION -> onboarding ----------


def test_user_with_complete_info_replying_to_consent_goes_to_onboarding(
    service, mock_user_db, mock_message_db
):
    """Scenario 3: User has user_type and user_language set but replies to CONSENT bot message -> onboarding."""
    async def run():
        user = make_user(
            phone_number_id="919555555555", user_id="u5", user_type="asha", user_language="en"
        )
        mock_user_db.get_users = AsyncMock(return_value=[user])
        bot_msg = make_bot_message(message_category=constants.CONSENT, message_id="bot-consent")
        mock_message_db.get_bot_messages_by_ids = AsyncMock(return_value=[bot_msg])
        msg = make_message(
            user, message_source_text="Yes", message_id="m5", reply_id="bot-consent"
        )
        msg.user = user
        conversations, onboard_convs = await _create_conversations(service, [msg])
        assert len(onboard_convs) == 1, "Reply to CONSENT step -> must go to onboarding"
        assert len(conversations) == 0
    asyncio.run(run())


def test_user_with_complete_info_replying_to_user_type_goes_to_onboarding(
    service, mock_user_db, mock_message_db
):
    """Scenario 3: User replies to USER_TYPE bot message -> onboarding."""
    async def run():
        user = make_user(
            phone_number_id="919666666666", user_id="u6", user_type="asha", user_language="en"
        )
        mock_user_db.get_users = AsyncMock(return_value=[user])
        bot_msg = make_bot_message(message_category=constants.USER_TYPE, message_id="bot-ut")
        mock_message_db.get_bot_messages_by_ids = AsyncMock(return_value=[bot_msg])
        msg = make_message(user, message_source_text="Asha", message_id="m6", reply_id="bot-ut")
        msg.user = user
        conversations, onboard_convs = await _create_conversations(service, [msg])
        assert len(onboard_convs) == 1
        assert len(conversations) == 0
    asyncio.run(run())


def test_user_with_complete_info_replying_to_language_selection_goes_to_onboarding(
    service, mock_user_db, mock_message_db
):
    """Scenario 3: User replies to LANGUAGE_SELECTION bot message -> onboarding."""
    async def run():
        user = make_user(
            phone_number_id="919777777777", user_id="u7", user_type="asha", user_language="en"
        )
        mock_user_db.get_users = AsyncMock(return_value=[user])
        bot_msg = make_bot_message(
            message_category=constants.LANGUAGE_SELECTION, message_id="bot-lang"
        )
        mock_message_db.get_bot_messages_by_ids = AsyncMock(return_value=[bot_msg])
        msg = make_message(user, message_source_text="English", message_id="m7", reply_id="bot-lang")
        msg.user = user
        conversations, onboard_convs = await _create_conversations(service, [msg])
        assert len(onboard_convs) == 1
        assert len(conversations) == 0
    asyncio.run(run())


# ---------- Scenario 4 (success): Complete user, no onboarding-step reply -> conversations ----------


def test_user_with_complete_info_and_onboarding_message_goes_to_conversations(
    service, mock_user_db
):
    """Scenario 4: User has user_type and user_language, sends onboarding message, no reply -> already registered (conversations)."""
    async def run():
        user = make_user(
            phone_number_id="919888888888", user_id="u8", user_type="asha", user_language="en"
        )
        mock_user_db.get_users = AsyncMock(return_value=[user])
        msg = make_message(user, message_source_text="onboard asha", message_id="m8")
        msg.user = user
        conversations, onboard_convs = await _create_conversations(service, [msg])
        assert len(conversations) == 1, "Complete user sending onboarding message -> already registered (conversations)"
        assert len(onboard_convs) == 0
    asyncio.run(run())


# ---------- User not found -> onboarding ----------


def test_user_not_found_goes_to_onboarding(service, mock_user_db):
    """User not in DB (get_users returns empty) -> message goes to onboarding."""
    async def run():
        mock_user_db.get_users = AsyncMock(return_value=[])
        user = make_user(phone_number_id="919999999999", user_id="u9", user_type="asha", user_language="en")
        msg = make_message(user, message_source_text="hello", message_id="m9")
        msg.user = user
        conversations, onboard_convs = await _create_conversations(service, [msg])
        assert len(onboard_convs) == 1
        assert len(conversations) == 0
    asyncio.run(run())
