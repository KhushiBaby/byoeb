from datetime import datetime, timezone
from typing import Optional
import uuid
import time
from byoeb.services.user.onboarding import map_user_type
import pytest
import tenacity

from byoeb_core.models.byoeb.user import User
from byoeb_core.models.whatsapp.incoming.interactive_message import (
    ButtonReplyModel,
    ChangeModel as InteractiveChange,
    ContactModel as InteractiveContact,
    ContextModel as InteractiveContext,
    EntryModel as InteractiveEntry,
    InteractiveModel,
    ListReplyModel,
    MessageModel as InteractiveMessage,
    MetadataModel as InteractiveMetadata,
    ValueModel as InteractiveValue,
    WhatsAppInteractiveMessageBody,
)
from byoeb_core.models.whatsapp.incoming.regular_message import (
    Change as RegularChange,
    Contact as RegularContact,
    Entry as RegularEntry,
    Message as RegularMessage,
    Metadata as RegularMetadata,
    Profile as RegularProfile,
    TextMessage,
    Value as RegularValue,
    WhatsAppRegularMessageBody,
)
from byoeb.constants.onboarding_text import CONSENT_DICT, LANGUAGE_NAME_TO_CODE, MESSAGE_DICT
from byoeb.constants.user_enums import UserType
from byoeb.services.user.constants import CONSENT

def get_message_timestamp() -> str:
    return str(int(time.time()))

def generate_message_id() -> str:
    return f"wamid.{uuid.uuid4().hex}"

def _regular_webhook(*, message: RegularMessage, username: str, phone_number_id: str, bot_phone_number_id: str) -> WhatsAppRegularMessageBody:
    return WhatsAppRegularMessageBody(object="whatsapp_business_account", entry=[
        RegularEntry(id="211506508713627", changes=[
            RegularChange(field="messages", value=RegularValue(
                messaging_product="whatsapp",
                metadata=RegularMetadata(phone_number_id=bot_phone_number_id, display_phone_number="1234567890"),
                contacts=[RegularContact(profile=RegularProfile(name=username), wa_id=phone_number_id)],
                messages=[message],
            ))
        ])
    ])

def _interactive_webhook(*, message: InteractiveMessage, username: str, phone_number_id: str, bot_phone_number_id: str) -> WhatsAppInteractiveMessageBody:
    return WhatsAppInteractiveMessageBody(object="whatsapp_business_account", entry=[
        InteractiveEntry(id="211506508713627", changes=[
            InteractiveChange(field="messages", value=InteractiveValue(
                messaging_product="whatsapp",
                metadata=InteractiveMetadata(phone_number_id=bot_phone_number_id, display_phone_number="1234567890"),
                contacts=[InteractiveContact(profile={"name": username}, wa_id=phone_number_id)],
                messages=[message],
            ))
        ])
    ])

def _dump_payload(body) -> dict:
    return body.model_dump(by_alias=True, exclude_none=True)

def _text_message_payload(*, message_id: str, timestamp: str, text: str, username: str, phone_number_id: str, bot_phone_number_id: str):
    msg = RegularMessage(id=message_id, timestamp=timestamp, type="text", text=TextMessage(body=text))
    msg.from_ = phone_number_id
    return _dump_payload(_regular_webhook(message=msg, username=username, phone_number_id=phone_number_id, bot_phone_number_id=bot_phone_number_id))

def _interactive_list_reply_payload( *, message_id: str, timestamp: str, context_id: str, selection_id: str, title: str, description: str, username: str, phone_number_id: str, bot_phone_number_id: str):
    msg = InteractiveMessage(
        id=message_id,
        timestamp=timestamp,
        type="interactive",
        context=InteractiveContext(id=context_id),
        interactive=InteractiveModel(type="list_reply", list_reply=ListReplyModel(id=selection_id, title=title, description=description)),
    )
    msg.from_ = phone_number_id
    return _dump_payload(_interactive_webhook(message=msg, username=username, phone_number_id=phone_number_id, bot_phone_number_id=bot_phone_number_id))

def _interactive_button_reply_payload(*, message_id: str, timestamp: str, context_id: str, button_id: str, title: str, username: str, phone_number_id: str, bot_phone_number_id: str):
    msg = InteractiveMessage(
        id=message_id,
        timestamp=timestamp,
        type="interactive",
        context=InteractiveContext(id=context_id),
        interactive=InteractiveModel(type="button_reply", button_reply=ButtonReplyModel(id=button_id, title=title)),
    )
    msg.from_ = phone_number_id
    return _dump_payload(_interactive_webhook(message=msg, username=username, phone_number_id=phone_number_id, bot_phone_number_id=bot_phone_number_id))

@tenacity.retry(wait=tenacity.wait_fixed(5), stop=tenacity.stop_after_delay(120))
def _wait_for_next_context_id(auth_session, envs, phone: str, sent_timestamp: str, reply_to_message_id: Optional[str] = None, prompt_substring: Optional[str] = None) -> str:
    bot_messages = auth_session.get(f"{envs.base_url}/get_bot_messages?timestamp={sent_timestamp}", timeout=30).json()
    for msg in bot_messages:
        if msg["user"]["phone_number_id"] != phone:
            continue
        if reply_to_message_id and msg.get("reply_context", {}).get("reply_id") != reply_to_message_id:
            continue
        if prompt_substring \
            and prompt_substring not in msg.get("message_context", {}).get("message_source_text", "") \
            and prompt_substring not in msg.get("message_context", {}).get("message_english_text", ""):
            continue
        return msg["message_context"]["message_id"]
    raise RuntimeError("No matching bot message found yet")

@tenacity.retry(wait=tenacity.wait_fixed(2), stop=tenacity.stop_after_delay(60))
def _validate_user(envs, auth_session, phone_number_id: str, user_type: UserType, lang_code: str, begin: datetime):
    response = auth_session.post(f"{envs.base_url}/get_users", json=[phone_number_id], timeout=30)
    response.raise_for_status()
    users = response.json()
    assert len(users) == 1

    user = User(**users[0])
    assert user is not None
    assert user.user_language == lang_code
    assert user.user_type == user_type.value
    assert user.created_timestamp is not None and user.created_timestamp > begin
    assert user.tenant_id == envs.auth_tenant_id
    assert user.additional_info and CONSENT in user.additional_info and user.additional_info[CONSENT] is True

@pytest.mark.parametrize("language_display,user_type_choice,consent_yes_choice,expect_user_type", [
    ("English", "Others", "Yes", UserType.OTHERS),
    ("हिंदी", "आशा", "हाँ", UserType.ASHA),
    ("हिंदी", "अन्य", "हाँ", UserType.OTHERS),
    ("मराठी", "इतर", "होय", UserType.OTHERS),
    ("తెలుగు", "ఇతరులు", "అవును", UserType.OTHERS),
])
async def test_whatsapp_onboarding_flow(language_display: str, user_type_choice: str, consent_yes_choice: str, expect_user_type: UserType, envs, auth_me, auth_session, whatsapp_webhook):
    phone_number_id = auth_me.phone_number_id
    username = auth_me.username
    context_id: str | None = None
    auth_session.delete(f"{envs.base_url}/delete_users", json=[phone_number_id], timeout=30).raise_for_status()

    lang_code = LANGUAGE_NAME_TO_CODE[language_display]
    user_type_prompt_substring = MESSAGE_DICT[lang_code]["text"].strip()[:16]
    consent_prompt_substring = CONSENT_DICT[map_user_type(expect_user_type.value)][lang_code]["text"].strip()[:16]

    START = "start"
    LANGUAGE_SELECTED = "language_selected"
    USER_TYPE_SELECTED = "user_type_selected"
    CONSENTED = "consented"
    VALIDATE_USER = "validate_user"
    ASKED_QUESTION = "asked_question"
    DONE = "done"

    begin = datetime.now(timezone.utc)

    state = START
    while state != DONE:
        print("-------------------------STATE", f"({state})", "-------------------------")
        timestamp = get_message_timestamp()
        time.sleep(1)  # ensure unique timestamp for each message
        if state == START:
            message_id = generate_message_id()
            payload = _text_message_payload(message_id=message_id, timestamp=timestamp, text="onboard-asha", username=username, phone_number_id=phone_number_id, bot_phone_number_id=envs.whatsapp_phone_number_id)
            whatsapp_webhook(payload)
            context_id = _wait_for_next_context_id(auth_session=auth_session, envs=envs, phone=phone_number_id, sent_timestamp=timestamp, reply_to_message_id=message_id, prompt_substring="Select your language")
            state = LANGUAGE_SELECTED
        elif state == LANGUAGE_SELECTED:
            assert context_id is not None, f"Missing context_id before state={state!r}"
            message_id = generate_message_id()
            payload = _interactive_list_reply_payload(message_id=message_id, timestamp=timestamp, context_id=context_id, selection_id=language_display, title=language_display, description="", username=username, phone_number_id=phone_number_id, bot_phone_number_id=envs.whatsapp_phone_number_id)
            whatsapp_webhook(payload)
            context_id = _wait_for_next_context_id(auth_session=auth_session, envs=envs, phone=phone_number_id, sent_timestamp=timestamp, reply_to_message_id=message_id, prompt_substring=user_type_prompt_substring)
            state = USER_TYPE_SELECTED
        elif state == USER_TYPE_SELECTED:
            assert context_id is not None, f"Missing context_id before state={state!r}"
            message_id = generate_message_id()
            payload = _interactive_button_reply_payload(message_id=message_id, timestamp=timestamp, context_id=context_id, button_id="others", title=user_type_choice, username=username, phone_number_id=phone_number_id, bot_phone_number_id=envs.whatsapp_phone_number_id)
            whatsapp_webhook(payload)
            context_id = _wait_for_next_context_id(auth_session=auth_session, envs=envs, phone=phone_number_id, sent_timestamp=timestamp, reply_to_message_id=message_id, prompt_substring=consent_prompt_substring)
            state = CONSENTED
        elif state == CONSENTED:
            assert context_id is not None, f"Missing context_id before state={state!r}"
            message_id = generate_message_id()
            payload = _interactive_button_reply_payload(message_id=message_id, timestamp=timestamp, context_id=context_id, button_id="yes", title=consent_yes_choice, username=username, phone_number_id=phone_number_id, bot_phone_number_id=envs.whatsapp_phone_number_id)
            whatsapp_webhook(payload)
            state = VALIDATE_USER
        elif state == VALIDATE_USER:
            _validate_user(envs=envs, auth_session=auth_session, phone_number_id=phone_number_id, user_type=expect_user_type, lang_code=lang_code, begin=begin)
            state = ASKED_QUESTION
        elif state == ASKED_QUESTION:
            message_id = generate_message_id()
            payload = _text_message_payload(message_id=message_id, timestamp=timestamp, text="What is the antara injection?", username=username, phone_number_id=phone_number_id, bot_phone_number_id=envs.whatsapp_phone_number_id)
            whatsapp_webhook(payload)
            _wait_for_next_context_id(auth_session=auth_session, envs=envs, phone=phone_number_id, sent_timestamp=timestamp, reply_to_message_id=message_id, prompt_substring="Antara")
            state = DONE
        else:
            raise RuntimeError(f"Unknown state: {state!r}")