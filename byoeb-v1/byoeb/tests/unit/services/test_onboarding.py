import pytest
import types
from unittest.mock import MagicMock

import byoeb.services.user.onboarding as mod

from byoeb_core.models.byoeb.message_context import (
    ByoebMessageContext,
    MessageContext,
    ReplyContext,
    MessageTypes,
)
from byoeb_core.models.byoeb.user import User

import byoeb.services.chat.constants as chat_const
import byoeb.services.user.constants as user_const

def make_user(phone="919000000000", lang="en", utype=None):
    return User(
        user_id="dummy",
        user_name="Test",
        phone_number_id=phone,
        user_language=lang,
        user_type=utype,
        test_user=(utype == "others"),
        experts={},
        audience=[],
        additional_info={},
        created_timestamp=0,
        activity_timestamp=0,
    )

def make_msg(user, category=None, text="", message_id="m1"):
    reply_ctx = None
    if category is not None:
        reply_ctx = ReplyContext(reply_id=message_id, message_category=category)
    return ByoebMessageContext(
        channel_type="whatsapp",
        message_category=category or "unknown",
        user=user,
        message_context=MessageContext(
            message_type=MessageTypes.REGULAR_TEXT.value,
            message_source_text=text,
            additional_info={},
        ),
        reply_context=reply_ctx,
    )

class FakeWhatsAppService:
    def __init__(self, channel_client_factory=None):
        self._prepared = None
        self._sent = None

    def prepare_requests(self, byoeb_message):
        self._prepared = byoeb_message
        return ["REQ"]

    async def send_requests(self, requests):
        self._sent = requests
        return (["RESP"], ["MSG-ID-1"])

    def create_conv(self, byoeb_message, responses):
        return [{"message_id": "conv-1"}]


@pytest.fixture
def mock_services(monkeypatch):
    """Monkeypatch external services used by handle_unknown_user."""
    monkeypatch.setattr(mod, "WhatsAppService", FakeWhatsAppService)

    class MsgDB:
        def __init__(self):
            self.created = []
            self.executed = []
        def message_create_queries(self, convs):
            self.created.append(convs)
            return [{"insert": "messages"}]
        async def execute_queries(self, q):
            self.executed.append(q)

    class UserDB:
        def __init__(self):
            self.created = []
            self.updated = []
            self.executed = []
        def user_create_query(self, user):
            self.created.append(user)
            return {"insert": "user"}
        def user_update_query(self, user):
            self.updated.append(user)
            return {"update": "user"}
        async def execute_queries(self, q):
            self.executed.append(q)

    msg_db = MsgDB()
    user_db = UserDB()

    channel_factory = MagicMock()

    monkeypatch.setattr(mod, "create_audio", lambda lang, utype: (b"ogg", "audio/ogg"))

    return msg_db, user_db, channel_factory

def test_get_language_code():
    assert mod.get_language_code("English") == "en"
    assert mod.get_language_code("हिंदी") == "hi"
    assert mod.get_language_code("मराठी") == "mr"
    assert mod.get_language_code("తెలుగు") == "te"
    assert mod.get_language_code("Klingon") is None

def test_get_consent():
    assert mod.get_consent("Yes") is True
    assert mod.get_consent("हाँ") is True
    assert mod.get_consent("अवును") is None
    assert mod.get_consent("No") is False

def test_get_user_type():
    assert mod.get_user_type("Asha") == "asha"
    assert mod.get_user_type("ANM") == "anm"
    assert mod.get_user_type("Others") == "others"
    assert mod.get_user_type("Random") is None

@pytest.mark.asyncio
async def test_first_message_triggers_language_selection(mock_services):
    """Onboarding-like first message (e.g. 'onboard asha') creates user and sends language selection."""
    msg_db, user_db, channel_factory = mock_services

    user = make_user(phone="919111111111")
    msg = make_msg(user, category=None, text="onboard asha")

    await mod.handle_unknown_user([msg], msg_db, user_db, channel_factory)

    assert len(user_db.created) == 1
    assert len(msg_db.created) == 1
    assert len(msg_db.executed) == 1


@pytest.mark.asyncio
async def test_first_message_non_onboarding_sends_register_prompt_no_user(mock_services):
    """First message that is not onboarding-like (e.g. 'hi') sends register prompt and does not create user."""
    msg_db, user_db, channel_factory = mock_services

    user = make_user(phone="919111111111")
    msg = make_msg(user, category=None, text="hi")

    await mod.handle_unknown_user([msg], msg_db, user_db, channel_factory)

    assert len(user_db.created) == 0
    # Register prompt is sent via channel; msg_db may or may not record it depending on flow
    assert len(msg_db.executed) == 0

@pytest.mark.asyncio
async def test_register_prompt_reply_onboarding_like_sends_language_selection(mock_services):
    """Reply to register prompt with onboarding text runs language selection (not get_language_code)."""
    msg_db, user_db, channel_factory = mock_services

    user = make_user(phone="919555555555")
    msg = make_msg(
        user,
        category=chat_const.REGISTER_PROMPT,
        text="onboard asha",
    )

    await mod.handle_unknown_user([msg], msg_db, user_db, channel_factory)

    assert len(user_db.created) == 1
    assert len(msg_db.created) == 1
    assert len(msg_db.executed) == 1


@pytest.mark.asyncio
async def test_register_prompt_reply_not_onboarding_resends_prompt_no_db(mock_services):
    """Reply to register prompt with non-onboarding text re-sends prompt without user/message DB writes."""
    msg_db, user_db, channel_factory = mock_services

    user = make_user(phone="919666666666")
    msg = make_msg(
        user,
        category=chat_const.REGISTER_PROMPT,
        text="hello there",
    )

    await mod.handle_unknown_user([msg], msg_db, user_db, channel_factory)

    assert len(user_db.created) == 0
    assert len(user_db.updated) == 0
    assert len(msg_db.executed) == 0


@pytest.mark.asyncio
async def test_language_selection_sends_user_type_buttons_and_updates_user_lang(mock_services):
    msg_db, user_db, channel_factory = mock_services

    user = make_user(phone="919222222222")
    msg = make_msg(
        user,
        category=chat_const.LANGUAGE_SELECTION,
        text="English"
    )

    await mod.handle_unknown_user([msg], msg_db, user_db, channel_factory)

    assert len(user_db.updated) == 1
    assert user_db.updated[0].user_language == "en"

    assert len(msg_db.created) == 1
    assert len(msg_db.executed) == 1


@pytest.mark.asyncio
async def test_user_type_selection_sends_consent_and_updates_user_type(mock_services):
    msg_db, user_db, channel_factory = mock_services

    user = make_user(phone="919333333333", lang="en")
    msg = make_msg(
        user,
        category=chat_const.USER_TYPE,
        text="Others"
    )

    await mod.handle_unknown_user([msg], msg_db, user_db, channel_factory)

    assert len(user_db.updated) == 1
    assert user_db.updated[0].user_type == "others"

    assert len(msg_db.created) == 1


@pytest.mark.asyncio
async def test_consent_yes_sends_initial_message_and_updates_user(mock_services, monkeypatch):
    msg_db, user_db, channel_factory = mock_services

    user = make_user(phone="919444444444", lang="en", utype="others")
    msg = make_msg(
        user,
        category=chat_const.CONSENT,
        text="Yes"
    )

    monkeypatch.setattr(mod, "create_audio", lambda lang, utype: (b"ogg", "audio/ogg"))

    await mod.handle_unknown_user([msg], msg_db, user_db, channel_factory)

    assert len(user_db.updated) == 1
    assert user_db.updated[0].additional_info.get(user_const.CONSENT) is True

    assert len(msg_db.created) == 0
