import uuid
import time
import requests
import os
import sys
import pytest

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
    ValueModel as InteractiveValue,
    WhatsAppInteractiveMessageBody,
)
from byoeb_core.models.whatsapp.incoming.regular_message import (
    Change as RegularChange,
    Contact as RegularContact,
    Entry as RegularEntry,
    Message as RegularMessage,
    Profile as RegularProfile,
    TextMessage,
    Value as RegularValue,
    WhatsAppRegularMessageBody,
)
from byoeb.constants.onboarding_text import CONSENT_DICT, LANGUAGE_NAME_TO_CODE, MESSAGE_DICT
from byoeb.constants.user_enums import UserType

# Endpoint
BASE_URL = os.getenv("RECIEVE_URL")
PHONE_NUMBER_ID = os.getenv("PHONE_NUMBER_ID")
USER_NAME = os.getenv("USER_NAME", "byoeb-user")
if BASE_URL is None or PHONE_NUMBER_ID is None:
    print("Environment variables are missing (RECIEVE_URL / PHONE_NUMBER_ID)")
    sys.exit(1)

def get_current_timestamp() -> str:
    return str(int(time.time()))


def _created_timestamp_seconds(created_timestamp) -> float:
    """Convert created_timestamp (datetime, int, float, or string) to seconds since epoch for comparison."""
    if created_timestamp is None:
        return 0.0
    if hasattr(created_timestamp, "timestamp"):
        return created_timestamp.timestamp()
    try:
        t = float(created_timestamp)
        return t if t < 1e12 else t / 1000.0
    except (TypeError, ValueError):
        return 0.0

def generate_message_id() -> str:
    return f"wamid.{uuid.uuid4().hex}"

def _regular_webhook(*, message: RegularMessage) -> WhatsAppRegularMessageBody:
    return WhatsAppRegularMessageBody(object="whatsapp_business_account", entry=[
        RegularEntry(id="211506508713627", changes=[
            RegularChange(field="messages", value=RegularValue(
                messaging_product="whatsapp",
                contacts=[RegularContact(profile=RegularProfile(name=USER_NAME), wa_id=PHONE_NUMBER_ID)],
                messages=[message],
            ))
        ])
    ])

def _interactive_webhook(*, message: InteractiveMessage) -> WhatsAppInteractiveMessageBody:
    return WhatsAppInteractiveMessageBody(object="whatsapp_business_account", entry=[
        InteractiveEntry(id="211506508713627", changes=[
            InteractiveChange(field="messages", value=InteractiveValue(
                messaging_product="whatsapp",
                contacts=[InteractiveContact(profile={"name": USER_NAME}, wa_id=PHONE_NUMBER_ID)],
                messages=[message],
            ))
        ])
    ])

def _dump_payload(body) -> dict:
    return body.model_dump(by_alias=True, exclude_none=True)

def _text_message_payload(*, message_id: str, timestamp: str, text: str):
    msg = RegularMessage(id=message_id, timestamp=timestamp, type="text", text=TextMessage(body=text))
    msg.from_ = PHONE_NUMBER_ID
    return _dump_payload(_regular_webhook(message=msg))

def _interactive_list_reply_payload( *, message_id: str, timestamp: str, context_id: str, selection_id: str, title: str, description: str = ""):
    msg = InteractiveMessage(
        id=message_id,
        timestamp=timestamp,
        type="interactive",
        context=InteractiveContext(id=context_id),
        interactive=InteractiveModel(type="list_reply", list_reply=ListReplyModel(id=selection_id, title=title, description=description)),
    )
    msg.from_ = PHONE_NUMBER_ID
    return _dump_payload(_interactive_webhook(message=msg))

def _interactive_button_reply_payload(*, message_id: str, timestamp: str, context_id: str, button_id: str, title: str):
    msg = InteractiveMessage(
        id=message_id,
        timestamp=timestamp,
        type="interactive",
        context=InteractiveContext(id=context_id),
        interactive=InteractiveModel(type="button_reply", button_reply=ButtonReplyModel(id=button_id, title=title)),
    )
    msg.from_ = PHONE_NUMBER_ID
    return _dump_payload(_interactive_webhook(message=msg))

# Timeouts for integration tests (server + queue may be slow)
WAIT_FOR_BOT_TIMEOUT_S = 240
WAIT_FOR_BOT_POLL_INTERVAL_S = 5
HTTP_REQUEST_TIMEOUT_S = 60
GET_USERS_POLL_TIMEOUT_S = 120
GET_USERS_POLL_INTERVAL_S = 2


def _wait_for_next_context_id(*, url: str, reply_to_message_id: str, sent_timestamp: str, prompt_substring: str, timeout_s: int = WAIT_FOR_BOT_TIMEOUT_S, poll_interval_s: int = WAIT_FOR_BOT_POLL_INTERVAL_S, initial_delay_s: float = 0) -> str:
    if initial_delay_s > 0:
        time.sleep(initial_delay_s)
    deadline = time.time() + timeout_s
    while True:
        bot_messages = requests.get(url, timeout=HTTP_REQUEST_TIMEOUT_S).json()
        for msg in bot_messages:
            if (
                msg.get("reply_context", {}).get("reply_id") == reply_to_message_id
                and msg.get("outgoing_timestamp") not in (None, "None", "")
                and int(str(msg["outgoing_timestamp"])) > int(sent_timestamp)
                and prompt_substring in (msg.get("message_context", {}).get("message_source_text") or "")
            ):
                return msg["message_context"]["message_id"]

        if time.time() >= deadline:
            raise TimeoutError(f"Timed out waiting for bot prompt containing {prompt_substring!r}")

        time.sleep(poll_interval_s)

@pytest.mark.parametrize(
    "language_display,user_type_choice,consent_yes_choice",
    [
        ("English", "Others", "Yes"),
        ("हिंदी", "अन्य", "हाँ"),
        ("मराठी", "इतर", "होय"),
        ("తెలుగు", "ఇతరులు", "అవును"),
    ],
)
def test_whatsapp_onboarding_flow(language_display: str, user_type_choice: str, consent_yes_choice: str):
    context_id = None
    delete_url = BASE_URL.replace("receive", "delete_users")
    requests.delete(delete_url, json=[PHONE_NUMBER_ID], timeout=HTTP_REQUEST_TIMEOUT_S).raise_for_status()

    lang_code = LANGUAGE_NAME_TO_CODE[language_display]
    user_type_prompt_substring = MESSAGE_DICT[lang_code]["text"].strip()[:16]
    consent_prompt_substring = CONSENT_DICT[UserType.ASHA.value][lang_code]["text"].strip()[:16]

    START = "start"
    LANGUAGE_SELECTED = "language_selected"
    USER_TYPE_SELECTED = "user_type_selected"
    CONSENTED = "consented"
    VALIDATE_USER = "validate_user"
    ASKED_QUESTION = "asked_question"
    DONE = "done"

    begin = time.time()

    state = START
    while state != DONE:
        print("-------------------------STATE", f"({state})", "-------------------------")
        if state == START:
            message_id = generate_message_id()
            timestamp = get_current_timestamp()
            payload = _text_message_payload(message_id=message_id, timestamp=timestamp, text="onboard-asha")

            requests.post(BASE_URL, json=payload, timeout=HTTP_REQUEST_TIMEOUT_S).raise_for_status()
            url = BASE_URL.replace("receive", "get_bot_messages?timestamp=") + str(timestamp)

            context_id = _wait_for_next_context_id(url=url, reply_to_message_id=message_id, sent_timestamp=timestamp, prompt_substring="Select your language")

            state = LANGUAGE_SELECTED
        elif state == LANGUAGE_SELECTED:
            assert context_id is not None, f"Missing context_id before state={state!r}"
            message_id = generate_message_id()
            timestamp = get_current_timestamp()
            payload = _interactive_list_reply_payload(message_id=message_id, timestamp=timestamp, context_id=context_id, selection_id=language_display, title=language_display, description="")

            requests.post(BASE_URL, json=payload, timeout=HTTP_REQUEST_TIMEOUT_S).raise_for_status()
            url = BASE_URL.replace("receive", "get_bot_messages?timestamp=") + str(timestamp)

            context_id = _wait_for_next_context_id(url=url, reply_to_message_id=message_id, sent_timestamp=timestamp, prompt_substring=user_type_prompt_substring, initial_delay_s=3)

            state = USER_TYPE_SELECTED
        elif state == USER_TYPE_SELECTED:
            assert context_id is not None, f"Missing context_id before state={state!r}"
            message_id = generate_message_id()
            timestamp = get_current_timestamp()
            payload = _interactive_button_reply_payload(message_id=message_id, timestamp=timestamp, context_id=context_id, button_id="others", title=user_type_choice)

            requests.post(BASE_URL, json=payload, timeout=HTTP_REQUEST_TIMEOUT_S).raise_for_status()
            url = BASE_URL.replace("receive", "get_bot_messages?timestamp=") + str(timestamp)

            context_id = _wait_for_next_context_id(url=url, reply_to_message_id=message_id, sent_timestamp=timestamp, prompt_substring=consent_prompt_substring)

            state = CONSENTED
        elif state == CONSENTED:
            assert context_id is not None, f"Missing context_id before state={state!r}"
            message_id = generate_message_id()
            timestamp = get_current_timestamp()
            payload = _interactive_button_reply_payload(message_id=message_id, timestamp=timestamp, context_id=context_id, button_id="yes", title=consent_yes_choice)
            requests.post(BASE_URL, json=payload, timeout=HTTP_REQUEST_TIMEOUT_S).raise_for_status()
            state = VALIDATE_USER
        elif state == VALIDATE_USER:
            get_url = BASE_URL.replace("receive", "get_users")
            user = None
            get_users_deadline = time.time() + GET_USERS_POLL_TIMEOUT_S
            while time.time() < get_users_deadline:
                response = requests.post(get_url, json=[PHONE_NUMBER_ID], timeout=HTTP_REQUEST_TIMEOUT_S)
                response.raise_for_status()
                users = response.json()
                if len(users) == 1:
                    user = User(**users[0])
                    break
                time.sleep(GET_USERS_POLL_INTERVAL_S)
            else:
                raise TimeoutError("Timed out waiting for get_users to return one user")

            assert user is not None
            assert user.user_language == lang_code
            assert user.user_type == UserType.OTHERS.value
            assert _created_timestamp_seconds(user.created_timestamp) > begin
            state = ASKED_QUESTION
        elif state == ASKED_QUESTION:
            message_id = generate_message_id()
            timestamp = get_current_timestamp()
            payload = _text_message_payload(message_id=message_id, timestamp=timestamp, text="What is a antra injection?")
            requests.post(BASE_URL, json=payload, timeout=HTTP_REQUEST_TIMEOUT_S).raise_for_status()
            state = DONE
        else:
            raise RuntimeError(f"Unknown state: {state!r}")
