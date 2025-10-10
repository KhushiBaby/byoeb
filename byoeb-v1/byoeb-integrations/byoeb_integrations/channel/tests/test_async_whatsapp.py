import os
import asyncio
import uuid
import pytest
import byoeb_core.convertor.audio_convertor as ac
import json
import byoeb_integrations.channel.whatsapp.validate_message as wa_validate
import byoeb_integrations.channel.whatsapp.convert_message as wa_convert
from byoeb_core.models.whatsapp.requests import message_request as wa_message
from byoeb_core.models.whatsapp.requests import interactive_message_request as wa_interactive
from byoeb_core.models.whatsapp.requests import template_message_request as wa_template
from byoeb_core.models.whatsapp.requests import media_request as wa_media
from byoeb_core.models.whatsapp.message_context import WhatsappMessageReplyContext
from byoeb_integrations.channel.whatsapp.meta.async_whatsapp_client import AsyncWhatsAppClient, StatusCode, WhatsAppMessageTypes
from byoeb_integrations import test_environment_path
from dotenv import load_dotenv
from byoeb_core.models.whatsapp.response.message_response import (
    WhatsAppResponse, 
    WhatsAppResponseStatus, 
    MediaMessage,
    Contact,
    Message
)
from byoeb_core.models.whatsapp.requests import (
    WhatsAppMessage,
    WhatsAppInteractiveMessage,
    WhatsAppTemplateMessage,
    WhatsAppMediaMessage, 
    WhatsAppAudio,
    WhatsAppVideo,
    WhatsAppReadMessage,
    MediaData
)
from byoeb_core.models.whatsapp.response.acknowledment_response import WhatsAppAcknowledgment
from types import SimpleNamespace

DUMMY_TOKEN = "dummy_auth_token_123456789"

def _make_ok_send_response(kind: str):
    return SimpleNamespace(
        response_status=SimpleNamespace(status="200"),
        messages=[SimpleNamespace(id="wamid.FAKE_MESSAGE_ID")],
        contacts=[SimpleNamespace(wa_id="918837701828")],
        media_message=SimpleNamespace(id="FAKE_MEDIA_ID") if kind in {"audio","video","image","document"} else None,
    )

@pytest.fixture(autouse=True)
def mock_whatsapp_when_dummy(monkeypatch):
    if os.getenv("USE_REAL_WHATSAPP") == "1":
        return

    from byoeb_integrations.channel.whatsapp.meta.async_whatsapp_client import AsyncWhatsAppClient

    async def fake_asend_batch_messages(self, batch_request, message_type):
        kind = message_type
        return [_make_ok_send_response(kind) for _ in (batch_request or [None])]

    async def fake__upload_media(self, data, mime_type):
        return 200, SimpleNamespace(id="FAKE_UPLOAD_ID"), None

    async def fake_adownload_media(self, media_id):
        return 200, SimpleNamespace(data=b"FAKE_BYTES"), None

    async def fake_adelete_media(self, media_id):
        return SimpleNamespace(success=True)

    async def fake_amark_as_read(self, message_id):
        return SimpleNamespace(success=True)

    monkeypatch.setattr(AsyncWhatsAppClient, "asend_batch_messages", fake_asend_batch_messages, raising=True)
    monkeypatch.setattr(AsyncWhatsAppClient, "_upload_media", fake__upload_media, raising=True)
    monkeypatch.setattr(AsyncWhatsAppClient, "adownload_media", fake_adownload_media, raising=True)
    monkeypatch.setattr(AsyncWhatsAppClient, "adelete_media", fake_adelete_media, raising=True)
    monkeypatch.setattr(AsyncWhatsAppClient, "amark_as_read", fake_amark_as_read, raising=True)

@pytest.fixture(autouse=True)
def mock_azure_tts(monkeypatch):
    if os.getenv("USE_REAL_AZURE_TTS") == "1":
        return

    from byoeb_integrations.translators.speech.azure.async_azure_speech_translator import (
        AsyncAzureSpeechTranslator,
    )

    def fake_init(self, *args, **kwargs):
        pass

    async def fake_atext_to_speech(self, input_text: str, source_language: str = "en"):
        wav = ac.text_to_wav_bytes(input_text)
        return ac.wav_to_ogg_opus_bytes(wav)

    monkeypatch.setattr(AsyncAzureSpeechTranslator, "__init__", fake_init, raising=True)
    monkeypatch.setattr(AsyncAzureSpeechTranslator, "atext_to_speech", fake_atext_to_speech, raising=True)

load_dotenv(test_environment_path)
WHATSAPP_AUTH_TOKEN = "dummy_auth_token_123456789"
WHATSAPP_PHONE_NUMBER_ID = "123456789012345"

@pytest.fixture(scope="session")
def event_loop():
    """Create and reuse a single event loop for all tests in the session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop

test_numbers = ["918837701828", "918904954952"]

async def atest_meta_batch_text_message():
    whatsapp_client = AsyncWhatsAppClient(
        phone_number_id=WHATSAPP_PHONE_NUMBER_ID,
        bearer_token=WHATSAPP_AUTH_TOKEN,
        reuse_client=True
    )
    message_type = WhatsAppMessageTypes.TEXT.value
    text_message = "Hello how are you"
    text = wa_message.Text(body=text_message)
    batch_request = []
    for number in test_numbers:
        whatsapp_text_message = wa_message.WhatsAppMessage(
            messaging_product=whatsapp_client.get_product_name(),
            to=number,
            type=message_type,
            text=text
        )
        batch_request.append(whatsapp_text_message.model_dump())
    batch_reponses = await whatsapp_client.asend_batch_messages(batch_request, message_type)
    
    batch_reaction_request = []
    for response in batch_reponses:
        assert response is not None
        if (response.response_status.status != "202" 
            and response.response_status.status != "200"
        ):
            continue
        message_id = response.messages[0].id
        contact = response.contacts[0].wa_id
        whatsapp_text_message = wa_message.WhatsAppMessage(
            messaging_product=whatsapp_client.get_product_name(),
            to=contact,
            type=WhatsAppMessageTypes.REACTION.value,
            reaction=wa_message.Reaction(
                message_id=message_id,
                emoji="👍"
            )
        )
        batch_reaction_request.append(whatsapp_text_message.model_dump())
    
    batch_reaction_response = await whatsapp_client.asend_batch_messages(batch_reaction_request, WhatsAppMessageTypes.REACTION.value)

    batch_reply_request = []
    for response in batch_reponses:
        assert response is not None
        if (response.response_status.status != '202' 
            and response.response_status.status != '200'
        ):
            continue
        message_id = response.messages[0].id
        contact = response.contacts[0].wa_id
        whatsapp_text_message = wa_message.WhatsAppMessage(
            messaging_product=whatsapp_client.get_product_name(),
            to=contact,
            type=WhatsAppMessageTypes.TEXT.value,
            text=wa_message.Text(body="I am fine, thank you!"),
            context=WhatsappMessageReplyContext(
                message_id=message_id
            )
        )
        batch_reply_request.append(whatsapp_text_message.model_dump(by_alias=True))
    batch_reply_response = await whatsapp_client.asend_batch_messages(batch_reply_request, message_type)
    await whatsapp_client._close()
    
async def atest_meta_batch_send_interactive_reply_message():
    
    def get_button(title):
        poll_id = str(uuid.uuid4())
        return wa_interactive.InteractiveActionButton(
            reply=wa_interactive.InteractiveReply(
                id=poll_id,
                title=title
            )
        )
    whatsapp_client = AsyncWhatsAppClient(
        phone_number_id=WHATSAPP_PHONE_NUMBER_ID,
        bearer_token=WHATSAPP_AUTH_TOKEN,
        reuse_client=True
    )
    message_type = WhatsAppMessageTypes.INTERACTIVE.value
    batch_request = []
    for number in test_numbers:
        whatsapp_text_message = wa_interactive.WhatsAppInteractiveMessage(
            messaging_product=whatsapp_client.get_product_name(),
            to=number,
            type=message_type,
            interactive=wa_interactive.Interactive(
                body=wa_interactive.InteractiveBody(
                    text="Do you like this product?"
                ),
                action=wa_interactive.InteractiveAction(
                    buttons=[get_button("Yes"), get_button("No")]
                )
            )
        )
        print(json.dumps(whatsapp_text_message.model_dump(exclude_none=True)))
        batch_request.append(whatsapp_text_message.model_dump())
    # whatsapp_text_response = await whatsapp_client.asend_batch_messages(batch_request, message_type)
    await whatsapp_client._close()

async def atest_meta_batch_send_interactive_list_message():
    def get_section(description):
        return wa_interactive.InteractiveActionSection(
            title=description,
            rows=[
                get_section_row("O1"),
                get_section_row("O2"),
                get_section_row("O3")
            ]
        )

    def get_section_row(description):
        return wa_interactive.InteractiveSectionRow(
            id=str(uuid.uuid4()),
            title=" ",
            description=description
        )
    whatsapp_client = AsyncWhatsAppClient(
        phone_number_id=WHATSAPP_PHONE_NUMBER_ID,
        bearer_token=WHATSAPP_AUTH_TOKEN,
        reuse_client=True
    )
    batch_request = []
    for number in test_numbers:
        message_type = WhatsAppMessageTypes.INTERACTIVE.value
        interactive_type = wa_interactive.InteractiveMessageTypes.LIST.value
        whatsapp_text_message = wa_interactive.WhatsAppInteractiveMessage(
            messaging_product=whatsapp_client.get_product_name(),
            to=number,
            type=message_type,
            interactive=wa_interactive.Interactive(
                type=interactive_type,
                body=wa_interactive.InteractiveBody(
                    text="Select an option"
                ),
                action=wa_interactive.InteractiveAction(
                    button="Button Options",
                    sections=[
                        get_section("S1"),
                    ]
                )
            )
        )
        batch_request.append(whatsapp_text_message.model_dump())

    whatsapp_text_response = await whatsapp_client.asend_batch_messages(batch_request, message_type)
    await whatsapp_client._close()

async def atest_meta_batch_send_template_message():
    whatsapp_client = AsyncWhatsAppClient(
        phone_number_id=WHATSAPP_PHONE_NUMBER_ID,
        bearer_token=WHATSAPP_AUTH_TOKEN,
        reuse_client=True
    )
    template_name = "question_of_the_week"
    text = "नवजात शिशु के शरीर में 300 हड्डियाँ होती हैं।"
    message_type = WhatsAppMessageTypes.TEMPLATE.value
    component = wa_template.TemplateComponent(
        type="body",
        parameters=[
            wa_template.TemplateParameter(
                type="text",
                text=text
            )
        ]
    )
    template = wa_template.Template(
        name =template_name,
        language=wa_template.TemplateLanguage(
            code="hi",
        ),
        components=[component]
    )
    batch_request = []
    for number in test_numbers:
        whatsapp_text_message = wa_template.WhatsAppTemplateMessage(
            messaging_product=whatsapp_client.get_product_name(),
            to=number,
            type=message_type,
            template=template
        )
        batch_request.append(whatsapp_text_message.model_dump())
        print(json.dumps(whatsapp_text_message.model_dump(exclude_none=True)))
    whatsapp_text_response = await whatsapp_client.asend_batch_messages(batch_request, message_type)
    assert whatsapp_text_response is not None
    assert whatsapp_text_response[0].response_status.status == "200"
    await whatsapp_client._close()

async def atest_audio_download():
    whatsapp_client = AsyncWhatsAppClient(
        phone_number_id=WHATSAPP_PHONE_NUMBER_ID,
        bearer_token=WHATSAPP_AUTH_TOKEN,
        reuse_client=True
    )
    message_type = wa_media.WhatsAppMediaTypes.AUDIO.value
    audio_bytes = ac.text_to_wav_bytes("Hello, how are you?")
    audio_bytes = ac.wav_to_ogg_opus_bytes(audio_bytes)
    media_type=wa_media.FileMediaType.AUDIO_OGG.value
    status, response, err = await whatsapp_client._upload_media(audio_bytes, media_type)
    audio_id = '1593203484905159'
    status, audio_data, err = await whatsapp_client.adownload_media(audio_id)
    assert audio_data.data is not None
    ack = await whatsapp_client.adelete_media(audio_id)
    assert ack.success is True
    await whatsapp_client._close()

async def atest_mark_as_read():
    whatsapp_client = AsyncWhatsAppClient(
        phone_number_id=WHATSAPP_PHONE_NUMBER_ID,
        bearer_token=WHATSAPP_AUTH_TOKEN,
        reuse_client=True
    )
    id = "wamid.HBgMOTE4ODM3NzAxODI4FQIAEhggODdBQjNFNTFBQzRCNEY1MjU1QTcwMEI4RTRBNkNGQUEA"
    ack = await whatsapp_client.amark_as_read(id)
    assert ack.success is True
    await whatsapp_client._close()

async def atest_send_video_message():
    # Get the directory of the current script
    current_dir = os.path.dirname(os.path.abspath(__file__))
    video_path = os.path.join(current_dir, 'asha.mp4')
    video_path = os.path.normpath(video_path)
    video_bytes = None
    with open(video_path, 'rb') as file:
        video_bytes = file.read()
    whatsapp_client = AsyncWhatsAppClient(
        phone_number_id=WHATSAPP_PHONE_NUMBER_ID,
        bearer_token=WHATSAPP_AUTH_TOKEN,
        reuse_client=True
    )
    number = "918837701828"
    message_type = wa_media.WhatsAppMediaTypes.VIDEO.value
    media_type=wa_media.FileMediaType.VIDEO_MP4.value
    video_message = wa_media.WhatsAppMediaMessage(
        messaging_product=whatsapp_client.get_product_name(),
        to=number,
        type=message_type,
        media=wa_media.MediaData(
            data=video_bytes,
            mime_type=media_type
        )
    )
    whatsapp_responses = await whatsapp_client.asend_batch_messages([video_message.model_dump()], message_type)
    media_id = whatsapp_responses[0].media_message.id
    print(whatsapp_responses)
    # ack = await whatsapp_client.adelete_media(media_id)
    # assert ack.success is True
    await whatsapp_client._close()

async def atest_batch_send_audio_message():
    from byoeb_integrations.translators.speech.azure.async_azure_speech_translator import AsyncAzureSpeechTranslator
    from azure.identity import get_bearer_token_provider, DefaultAzureCredential
    speech_translator_resource_id = os.getenv('SPEECH_TRANSLATOR_RESOURCE_ID')
    speech_translator_region = os.getenv('SPEECH_TRANSLATOR_REGION')
    token_provider = get_bearer_token_provider(
        DefaultAzureCredential(), "https://cognitiveservices.azure.com/.default"
    )
    message_type = wa_media.WhatsAppMediaTypes.AUDIO.value
    text = "नमस्कार क्या हालचाल हैं?"
    async_azure_speech_translator = AsyncAzureSpeechTranslator(
        region=speech_translator_region,
        token_provider=token_provider,
        resource_id=speech_translator_resource_id,
    )
    audio_bytes = await async_azure_speech_translator.atext_to_speech(
        input_text=text,
        source_language="hi",
    )
    with open("test_audio.ogg", "wb") as f:
        f.write(audio_bytes)
    # audio_bytes = ac.text_to_wav_bytes("अंतरा एक गर्भनिरोधक इंजेक्शन है जो जन्म नियंत्रण के लिए उपयोग किया जाता है। यह एक प्रतिवर्ती गर्भनिरोधक विधि है जो सरकारी स्वास्थ्य केंद्रों और अस्पतालों में मुफ्त में उपलब्ध है।")
    # audio_bytes = ac.convert_wav_bytes_to_aac(audio_bytes)
    media_type=wa_media.FileMediaType.AUDIO_OGG.value
    whatsapp_client = AsyncWhatsAppClient(
        phone_number_id=WHATSAPP_PHONE_NUMBER_ID,
        bearer_token=WHATSAPP_AUTH_TOKEN,
        reuse_client=True
    )
    batch_request = []
    for number in test_numbers:
        whatsapp_media_message = wa_media.WhatsAppMediaMessage(
            messaging_product=whatsapp_client.get_product_name(),
            to=number,
            type=message_type,
            media=wa_media.MediaData(
                data=audio_bytes,
                mime_type=media_type
            )
        )
        batch_request.append(whatsapp_media_message.model_dump())
    whatsapp_responses = await whatsapp_client.asend_batch_messages(batch_request, message_type)
    media_id = whatsapp_responses[0].media_message.id
    print(whatsapp_responses)
    # ack = await whatsapp_client.adelete_media(media_id)
    # assert ack.success is True
    await whatsapp_client._close()

def test_template_message():
    message = '{"object": "whatsapp_business_account", "entry": [{"id": "423299570870294", "changes": [{"value": {"messaging_product": "whatsapp", "metadata": {"display_phone_number": "15551355272", "phone_number_id": "421395191063010"}, "contacts": [{"profile": {"name": "rahul5982439"}, "wa_id": "918837701828"}], "messages": [{"context": {"from": "15551355272", "id": "wamid.HBgMOTE4ODM3NzAxODI4FQIAERgSRjZCQThENDNGREY0MjdEMTczAA=="}, "from": "918837701828", "id": "wamid.HBgMOTE4ODM3NzAxODI4FQIAEhggQTNDMDU5QzQwMUNBOUQyMDM4OTAxQTZGQUFFNUE1RDEA", "timestamp": "1732167477", "type": "button", "button": {"payload": "\u0909\u0924\u094d\u0924\u0930 \u0926\u093f\u0916\u093e\u0907\u090f", "text": "\u0909\u0924\u094d\u0924\u0930 \u0926\u093f\u0916\u093e\u0907\u090f"}}]}, "field": "messages"}]}]}'
    is_wa, message_type = wa_validate.validate_whatsapp_message(message)
    wa_convert.convert_template_message(message)
    assert is_wa is True
    assert message_type == "template"

def test_regular_message():
    message_1 = '{"object": "whatsapp_business_account", "entry": [{"id": "423299570870294", "changes": [{"value": {"messaging_product": "whatsapp", "metadata": {"display_phone_number": "15551355272", "phone_number_id": "421395191063010"}, "contacts": [{"profile": {"name": "rahul5982439"}, "wa_id": "918837701828"}], "messages": [{"from": "918837701828", "id": "wamid.HBgMOTE4ODM3NzAxODI4FQIAEhggMTU1NEI5QjRBNTlCNUZCQTk0QzlBNDY2NDRDNEYyMzkA", "timestamp": "1732167417", "text": {"body": "Hello"}, "type": "text"}]}, "field": "messages"}]}]}'
    is_wa, message_type = wa_validate.validate_whatsapp_message(message_1)
    wa_convert.convert_regular_message(message_1)
    assert is_wa is True
    assert message_type == "regular"
    message_2 = '{"object": "whatsapp_business_account", "entry": [{"id": "423299570870294", "changes": [{"value": {"messaging_product": "whatsapp", "metadata": {"display_phone_number": "15551355272", "phone_number_id": "421395191063010"}, "contacts": [{"profile": {"name": "rahul5982439"}, "wa_id": "918837701828"}], "messages": [{"from": "918837701828", "id": "wamid.HBgMOTE4ODM3NzAxODI4FQIAEhggNEJFQzc1OEFBNTlDQTQxOTI2MDQ3RTNGM0E4OTQxRDEA", "timestamp": "1732167457", "type": "audio", "audio": {"mime_type": "audio/ogg; codecs=opus", "sha256": "w26HXHrAYkyu19h0AStMV+9ojTl5mQwAQmBHPedekX0=", "id": "543799451804859", "voice": true}}]}, "field": "messages"}]}]}'
    is_wa, message_type = wa_validate.validate_whatsapp_message(message_2)
    wa_convert.convert_regular_message(message_2)
    assert is_wa is True
    assert message_type == "regular"

def test_interactive_message():
    message_1 = '{"object": "whatsapp_business_account", "entry": [{"id": "423299570870294", "changes": [{"value": {"messaging_product": "whatsapp", "metadata": {"display_phone_number": "15551355272", "phone_number_id": "421395191063010"}, "contacts": [{"profile": {"name": "rahul5982439"}, "wa_id": "918837701828"}], "messages": [{"context": {"from": "15551355272", "id": "wamid.HBgMOTE4ODM3NzAxODI4FQIAERgSNkY1QUIwQTMzNjc2MkU1OUZGAA=="}, "from": "918837701828", "id": "wamid.HBgMOTE4ODM3NzAxODI4FQIAEhggODg4REFBQUI4NzM1NUI1MDc2NjA2ODMwQjFFQkU3QzUA", "timestamp": "1732167674", "type": "interactive", "interactive": {"type": "button_reply", "button_reply": {"id": "804dcad1-def3-4e37-b391-9aa51fe7f5c3", "title": "Yes"}}}]}, "field": "messages"}]}]}'
    is_wa, message_type = wa_validate.validate_whatsapp_message(message_1)
    wa_convert.convert_interactive_message(message_1)
    assert is_wa is True
    assert message_type == "interactive"
    message_2 = '{"object": "whatsapp_business_account", "entry": [{"id": "423299570870294", "changes": [{"value": {"messaging_product": "whatsapp", "metadata": {"display_phone_number": "15551355272", "phone_number_id": "421395191063010"}, "contacts": [{"profile": {"name": "rahul5982439"}, "wa_id": "918837701828"}], "messages": [{"context": {"from": "15551355272", "id": "wamid.HBgMOTE4ODM3NzAxODI4FQIAERgSMzU5QTYwNzNBQUUzNjdDNjAwAA=="}, "from": "918837701828", "id": "wamid.HBgMOTE4ODM3NzAxODI4FQIAEhggNURBMjI2MUI1REREOUU1RTk0OTRDNzY4NkJEMjQxM0UA", "timestamp": "1732167786", "type": "interactive", "interactive": {"type": "list_reply", "list_reply": {"id": "f79a74e4-3d50-4cc3-ae50-86efff88f3f3", "title": " ", "description": "O1"}}}]}, "field": "messages"}]}]}'
    is_wa, message_type = wa_validate.validate_whatsapp_message(message_2)
    byoeb_message = wa_convert.convert_interactive_message(message_2)
    assert byoeb_message.reply_context.reply_id == "wamid.HBgMOTE4ODM3NzAxODI4FQIAERgSMzU5QTYwNzNBQUUzNjdDNjAwAA=="
    assert is_wa is True
    assert message_type == "interactive"

def test_status_message():
    message = '{"object": "whatsapp_business_account", "entry": [{"id": "423299570870294", "changes": [{"value": {"messaging_product": "whatsapp", "metadata": {"display_phone_number": "15551355272", "phone_number_id": "421395191063010"}, "statuses": [{"id": "wamid.HBgMOTE4ODM3NzAxODI4FQIAERgSQjM1RjY0M0QyMkU4OUU3OTc3AA==", "status": "delivered", "timestamp": "1733170072", "recipient_id": "918837701828", "conversation": {"id": "bbc3027b762dec0714d75f270a6e9e9e", "origin": {"type": "service"}}, "pricing": {"billable": true, "pricing_model": "CBP", "category": "service"}}]}, "field": "messages"}]}]}'
    is_wa, message_type = wa_validate.validate_whatsapp_message(message)
    byoeb_message = wa_convert.convert_status_message(message)
    json_data = byoeb_message.model_dump_json()
    assert is_wa is True
    assert byoeb_message.message_id == "wamid.HBgMOTE4ODM3NzAxODI4FQIAERgSQjM1RjY0M0QyMkU4OUU3OTc3AA=="
    assert message_type == "status"

import byoeb_integrations.channel.whatsapp.request_payload as wa_request_payload
from byoeb_core.models.byoeb.message_context import ByoebMessageContext
def test_text_request_payload():
    byoeb_message = '{"channel_type": "whatsapp", "message_category": "Bot_to_user_response", "user": {"user_id": "6dfd0676f602bdf2cd545160efd99e01", "user_name": null, "user_region": null, "user_language": "en", "user_type": "byoebuser", "phone_number_id": "918837701828", "test_user": false, "experts": { "primary": ["918904954952"] }, "audience": [], "created_timestamp": 1732451468, "activity_timestamp": 1732451468}, "message_context": {"message_id": null, "message_type": "regular_text", "message_source_text": "Hello! How can I assist you today?", "message_english_text": "Hello! How can I assist you today?", "media_info": null, "additional_info": null}, "reply_context": {"reply_id": "wamid.HBgMOTE4ODM3NzAxODI4FQIAEhggNzc0MUZCRkREOTEzNEY4NkRENURCRDMzOTQ1MEYyNzQA", "reply_type": "regular_text", "reply_source_text": "Hi", "reply_english_text": "Hi", "media_info": null, "additional_info": null}, "cross_conversation_id": null, "cross_conversation_context": null, "incoming_timestamp": null, "outgoing_timestamp": null}'
    byoeb_message = ByoebMessageContext.model_validate(json.loads(byoeb_message))
    payload = wa_request_payload.get_whatsapp_text_request_from_byoeb_message(byoeb_message)
    print(json.dumps(payload))

def test_meta_batch_text_message(event_loop):
    event_loop.run_until_complete(atest_meta_batch_text_message())

def test_meta_batch_send_interactive_reply_message(event_loop):
    event_loop.run_until_complete(atest_meta_batch_send_interactive_reply_message())

def test_meta_batch_send_interactive_list_message(event_loop):
    event_loop.run_until_complete(atest_meta_batch_send_interactive_list_message())

def test_meta_batch_send_template_message(event_loop):
    event_loop.run_until_complete(atest_meta_batch_send_template_message())

def test_batch_send_audio_message(event_loop):
    event_loop.run_until_complete(atest_batch_send_audio_message())

def test_audio_download(event_loop):
    event_loop.run_until_complete(atest_audio_download())
import pytest
import json
import byoeb_integrations.channel.whatsapp.validate_message as wa_validate
from types import SimpleNamespace

# 1. Invalid JSON input (should trigger json.loads)
def test_validate_regular_message_invalid_json(monkeypatch):
    # valid JSON string, but wrong structure for model_validate
    invalid_json_message = '{"foo": "bar"}'
    result = wa_validate.validate_regular_message(invalid_json_message)
    assert result is False


# 2. Regular message with unsupported type (should return False)
def test_validate_regular_message_unsupported_type():
    message = {
        "entry": [{"changes": [{"value": {"messages": [{"type": "unsupported"}]}}]}]
    }
    assert wa_validate.validate_regular_message(message) is False

# 3. Regular message raising exception (simulate model_validate raising)
def test_validate_regular_message_exception(monkeypatch):
    monkeypatch.setattr(
        "byoeb_integrations.channel.whatsapp.validate_message.incoming_message.WhatsAppRegularMessageBody",
        SimpleNamespace(model_validate=lambda x: (_ for _ in ()).throw(Exception("fail")))
    )
    message = {"any": "data"}
    assert wa_validate.validate_regular_message(message) is False

# 4. Template message invalid type
def test_validate_template_message_invalid_type():
    message = {
        "entry": [{"changes": [{"value": {"messages": [{"type": "text"}]}}]}]
    }
    assert wa_validate.validate_template_message(message) is False

# 5. Interactive message invalid type
def test_validate_interactive_message_invalid_type():
    message = {
        "entry": [{"changes": [{"value": {"messages": [{"type": "text"}]}}]}]
    }
    assert wa_validate.validate_interactive_message(message) is False

# 6. Status message missing statuses
def test_validate_status_message_none_statuses():
    message = {
        "entry": [{"changes": [{"value": {"statuses": None}}]}]
    }
    assert wa_validate.validate_status_message(message) is False

# 7. validate_whatsapp_message fallback to False
def test_validate_whatsapp_message_unknown_type():
    message = {
        "entry": [{"changes": [{"value": {"messages": [{"type": "unknown"}]}}]}]
    }
    is_valid, message_type = wa_validate.validate_whatsapp_message(message)
    assert is_valid is False
    assert message_type is None

# 8. JSON string input for validate_regular_message
def test_validate_regular_message_json_string():
    message_dict = {
        "entry": [{"changes": [{"value": {"messages": [{"type": "text"}]}}]}]
    }
    message_json = json.dumps(message_dict)
    assert wa_validate.validate_regular_message(message_json) is True

import aiohttp
@pytest.mark.asyncio
async def test_prepare_data():
    client = AsyncWhatsAppClient(
        phone_number_id=WHATSAPP_PHONE_NUMBER_ID,
        bearer_token=WHATSAPP_AUTH_TOKEN,
        reuse_client=True
    )
    data = {"key1": "value1", "key2": "value2"}
    file_content = b"file bytes"
    files = {
        "file1": ["test.txt", file_content, "text/plain"]
    }
    
    # Call private method
    form_data = client._AsyncWhatsAppClient__prepare_data(data, files)

    # Check that form_data is indeed aiohttp.FormData
    assert isinstance(form_data, aiohttp.FormData)

    # Extract field names
    field_names = [field[0].get("name") for field in form_data._fields]

    # Assert that all keys are present
    assert "key1" in field_names
    assert "key2" in field_names
    assert "file1" in field_names
@pytest.mark.asyncio
async def test_prepare_data_notdata():
    client = AsyncWhatsAppClient(
        phone_number_id=WHATSAPP_PHONE_NUMBER_ID,
        bearer_token=WHATSAPP_AUTH_TOKEN,
        reuse_client=True
    )
    form_data = client._AsyncWhatsAppClient__prepare_data(None, None)

    # Check that form_data is indeed aiohttp.FormData
    assert isinstance(form_data, aiohttp.FormData)
def test__get_headers__():
    client = AsyncWhatsAppClient(
    phone_number_id=WHATSAPP_PHONE_NUMBER_ID,
    bearer_token=WHATSAPP_AUTH_TOKEN,
    reuse_client=True
    )

    headers =client.__get_headers__("text")
    headers =client.__get_headers__(None)
@pytest.mark.asyncio
async def test__get_session__():
    client = AsyncWhatsAppClient(
    phone_number_id=WHATSAPP_PHONE_NUMBER_ID,
    bearer_token=WHATSAPP_AUTH_TOKEN,
    reuse_client=True
    )
    client._session=None
    await client._AsyncWhatsAppClient__get_session()
@pytest.mark.asyncio
async def test__get_session___reuseclientfalse():
    client = AsyncWhatsAppClient(
    phone_number_id=WHATSAPP_PHONE_NUMBER_ID,
    bearer_token=WHATSAPP_AUTH_TOKEN,
    reuse_client=False
    )
    client._session=None
    await client._AsyncWhatsAppClient__get_session()
@pytest.mark.asyncio
async def test__upload__():
    client = AsyncWhatsAppClient(
        phone_number_id=WHATSAPP_PHONE_NUMBER_ID,
        bearer_token=WHATSAPP_AUTH_TOKEN,
        reuse_client=True
    )
    mock_session = AsyncMock()
    client._session = mock_session
    data = {"key1": "value1", "key2": "value2"}
    file_content = b"file bytes"
    files = {
        "file1": ["test.txt", file_content, "text/plain"]
    }
    

    form_data = client._AsyncWhatsAppClient__prepare_data(data, files)
    await client.__upload__("xyz.com",form_data)
@pytest.mark.asyncio
async def test__delete__():
    client = AsyncWhatsAppClient(
        phone_number_id=WHATSAPP_PHONE_NUMBER_ID,
        bearer_token=WHATSAPP_AUTH_TOKEN,
        reuse_client=True
    )
    mock_session = AsyncMock()
    client._session = mock_session
    await client.__delete__("xyz.com")
@pytest.mark.asyncio
async def test__get__():
    client = AsyncWhatsAppClient(
        phone_number_id=WHATSAPP_PHONE_NUMBER_ID,
        bearer_token=WHATSAPP_AUTH_TOKEN,
        reuse_client=True
    )
    mock_session = AsyncMock()
    client._session = mock_session
    await client.__get__("xyz.com")

@pytest.mark.asyncio
async def test__post__():
    client = AsyncWhatsAppClient(
        phone_number_id=WHATSAPP_PHONE_NUMBER_ID,
        bearer_token=WHATSAPP_AUTH_TOKEN,
        reuse_client=True
    )
    mock_session = AsyncMock()
    client._session = mock_session
    await client.__post__("xyz.com", payload=None, data=None,content_type = "application/json")   
@pytest.mark.asyncio
async def test__post__valueerror():
    client = AsyncWhatsAppClient(
        phone_number_id=WHATSAPP_PHONE_NUMBER_ID,
        bearer_token=WHATSAPP_AUTH_TOKEN,
        reuse_client=True
    )
    data = {"key1": "value1", "key2": "value2"}
    file_content = b"file bytes"
    files = {
        "file1": ["test.txt", file_content, "text/plain"]
    }
    

    form_data = client._AsyncWhatsAppClient__prepare_data(data, files)
    mock_session = AsyncMock()
    client._session = mock_session
    with pytest.raises(ValueError, match="Only one of payload or data should be provided."):
       await client.__post__("xyz.com", payload="XYZ", data=form_data,content_type = "application/json") 

@pytest.mark.parametrize("msg_type, expected_func", [
    (WhatsAppMessageTypes.TEXT.value, "asend_text_message"),
    (WhatsAppMessageTypes.REACTION.value, "asend_reaction"),
    (WhatsAppMessageTypes.INTERACTIVE.value, "asend_interactive_message"),
    (WhatsAppMessageTypes.TEMPLATE.value, "asend_template_message"),
    (WhatsAppMessageTypes.AUDIO.value, "asend_audio_message"),
    (WhatsAppMessageTypes.VIDEO.value, "asend_video_message"),
    (WhatsAppMessageTypes.READ.value, "amark_as_read"),
])
def test_get_send_function(msg_type, expected_func):
    whatsapp_client = AsyncWhatsAppClient(
        phone_number_id=WHATSAPP_PHONE_NUMBER_ID,
        bearer_token=WHATSAPP_AUTH_TOKEN,
        reuse_client=True
    )

    func = whatsapp_client.get_send_function(msg_type)
    assert func == getattr(whatsapp_client, expected_func)

@pytest.mark.asyncio
async def test_get_send_function_invalid_type():
    whatsapp_client = AsyncWhatsAppClient(
        phone_number_id=WHATSAPP_PHONE_NUMBER_ID,
        bearer_token=WHATSAPP_AUTH_TOKEN,
        reuse_client=True,
    )

    func = whatsapp_client.get_send_function("INVALID_TYPE")

    # should return None
    assert func is None


import asyncio
from unittest.mock import AsyncMock, patch


@pytest.mark.asyncio
async def test_asend_text_message_success():
    dummy_payload = {
        "messaging_product": "whatsapp",
        "to": "1234567890",
        "type": "text",
        "text": {"body": "Hello world"}
    }

    # Dummy response matching WhatsAppResponse fields
    dummy_response = {
        "messaging_product": "whatsapp",
        "contacts": [{"input": "1234567890", "wa_id": "9876543210"}],
        "messages": [{"id": "message_1"}]
    }

    class DummyAsyncWhatsAppClient(AsyncWhatsAppClient):
        async def __post__(self, url, payload=None, data=None, content_type="application/json"):
            # Always return success with dummy_response
            return 200, dummy_response, None

    client = DummyAsyncWhatsAppClient("dummy_phone", "dummy_token")

    response: WhatsAppResponse = await client.asend_text_message(dummy_payload)

    assert response.response_status.status == "200"
    assert response.messaging_product == "whatsapp"
    assert response.contacts[0].input == "1234567890"
    assert response.messages[0].id == "message_1"

@pytest.mark.asyncio
async def test_close_with_session():
    whatsapp_client = AsyncWhatsAppClient(
        phone_number_id=WHATSAPP_PHONE_NUMBER_ID,
        bearer_token=WHATSAPP_AUTH_TOKEN,
        reuse_client=True
    )


    mock_session = AsyncMock()
    whatsapp_client._session = mock_session

    await whatsapp_client._close()


    mock_session.close.assert_awaited_once()

    assert whatsapp_client._session is None


@pytest.mark.asyncio
async def test_aexit_with_session():
    whatsapp_client = AsyncWhatsAppClient(
        phone_number_id=WHATSAPP_PHONE_NUMBER_ID,
        bearer_token=WHATSAPP_AUTH_TOKEN,
        reuse_client=True
    )
    mock_session = AsyncMock()
    whatsapp_client._session = mock_session
    await whatsapp_client.__aexit__("abs", "abc", "ab")
    whatsapp_client._session=None
    await whatsapp_client.__aexit__("abs", "abc", "ab")

@pytest.mark.asyncio
async def test_aenter_with_session():
    whatsapp_client = AsyncWhatsAppClient(
        phone_number_id=WHATSAPP_PHONE_NUMBER_ID,
        bearer_token=WHATSAPP_AUTH_TOKEN,
        reuse_client=True
    )
    mock_session = AsyncMock()
    whatsapp_client._session = mock_session
    x=await whatsapp_client.__aenter__()
@pytest.mark.asyncio
async def test_asend_template_message_success():
    dummy_payload = {
        "messaging_product": "whatsapp",
        "to": "1234567890",
        "type": "template",
        "template": {
            "name": "hello_world",
            "language": {"code": "en_US"},
            "components": []
        }
    }

    dummy_response = {
        "messaging_product": "whatsapp",
        "contacts": [{"input": "1234567890", "wa_id": "9876543210"}],
        "messages": [{"id": "message_1"}]
    }

    class DummyAsyncWhatsAppClient(AsyncWhatsAppClient):
        async def __post__(self, url, payload=None, data=None, content_type="application/json"):
            return 200, dummy_response, None

    client = DummyAsyncWhatsAppClient("dummy_phone", "dummy_token")
    response: WhatsAppResponse = await client.asend_template_message(dummy_payload)

    assert isinstance(response, WhatsAppResponse)
    assert response.messaging_product == "whatsapp"
    assert response.contacts[0].wa_id == "9876543210"
    assert response.messages[0].id == "message_1"
    assert response.response_status.status == "200"
@pytest.mark.asyncio
async def test_asend_audio_message_upload_error(monkeypatch):
    """Covers branch where _upload_media returns an error."""

    # Dummy client
    client = AsyncWhatsAppClient("dummy_phone", "dummy_token")

    # Monkeypatch _upload_media to return an error
    async def fake_upload(data, mime_type):
        return 500, None, "upload failed"
    monkeypatch.setattr(client, "_upload_media", fake_upload)

    # Create a payload without "audio", forcing _upload_media to be called
    payload = WhatsAppMediaMessage(
        messaging_product=client.get_product_name(),
        to="1234567890",
        type=WhatsAppMessageTypes.AUDIO.value,
        media=MediaData(
            data=b"fakebytes",
            mime_type="audio/ogg"
        )
    ).model_dump()

    resp = await client.asend_audio_message(payload)

    # Since upload failed, should return WhatsAppResponse with error
    assert resp.response_status.status == "500"
    assert resp.response_status.error == "upload failed"
    assert resp.messages == []

@pytest.mark.asyncio
async def test_asend_reaction_failure(monkeypatch):
    """Covers error path in asend_reaction when __post__ returns failure."""
    client = AsyncWhatsAppClient("dummy_phone", "dummy_token")

    # Monkeypatch __post__ to simulate a 500 error
    async def fake_post(url, payload=None, data=None, content_type="application/json"):
        return 500, None, "server error"
    monkeypatch.setattr(client, "__post__", fake_post)

    payload = WhatsAppMessage(
        messaging_product=client.get_product_name(),
        to="1234567890",
        type="reaction",
        reaction={"message_id": "fakeid", "emoji": "👍"}
    ).model_dump()

    response: WhatsAppResponse = await client.asend_reaction(payload)

    assert isinstance(response, WhatsAppResponse)
    assert response.response_status.status == "500"
    assert response.response_status.error == "server error"
    assert response.messages == []
    assert response.contacts == []


@pytest.mark.asyncio
async def test_asend_video_message_upload_error(monkeypatch):
    """Covers branch where _upload_media returns an error for video messages."""

    client = AsyncWhatsAppClient("dummy_phone", "dummy_token")

    # Patch _upload_media to simulate failure
    async def fake_upload(data, mime_type):
        return 400, None, "video upload failed"
    monkeypatch.setattr(client, "_upload_media", fake_upload)

    # Create payload without "video" so upload is triggered
    payload = WhatsAppMediaMessage(
        messaging_product=client.get_product_name(),
        to="1234567890",
        type=WhatsAppMessageTypes.VIDEO.value,
        media=MediaData(
            data=b"fakebytes",
            mime_type="video/mp4"
        )
    ).model_dump()

    resp = await client.asend_video_message(payload)

    # Verify failure response
    assert resp.response_status.status == "400"
    assert resp.response_status.error == "video upload failed"
    assert resp.messages == []


@pytest.mark.asyncio
async def test_asend_interactive_message_failure(monkeypatch):
    """Covers asend_interactive_message branch when __post__ fails."""
    client = AsyncWhatsAppClient("dummy_phone", "dummy_token")

    # Monkeypatch __post__ to simulate failure
    async def fake_post(url, payload=None, data=None, content_type="application/json"):
        return 500, None, "server error"
    monkeypatch.setattr(client, "__post__", fake_post)

    # Minimal valid interactive message payload
    payload = WhatsAppInteractiveMessage(
        messaging_product=client.get_product_name(),
        to="1234567890",
        type=WhatsAppMessageTypes.INTERACTIVE.value,
        interactive={"type": "button", "body": {"text": "Test"}}
    ).model_dump()

    response: WhatsAppResponse = await client.asend_interactive_message(payload)

    assert isinstance(response, WhatsAppResponse)
    assert response.response_status.status == "500"
    assert response.response_status.error == "server error"
    assert response.messages == []
    assert response.contacts == []


import pytest
from byoeb_integrations.channel.whatsapp.meta.async_whatsapp_client import (
    AsyncWhatsAppClient,
    WhatsAppResponse,
)

@pytest.mark.asyncio
async def test_asend_text_message_failure(monkeypatch):
    client = AsyncWhatsAppClient("dummy_phone", "dummy_token")

    # Fake __post__ returns failure
    async def fake_post(url, payload=None, data=None, content_type="application/json"):
        return 500, None, "server error"
    monkeypatch.setattr(client, "__post__", fake_post)

    # Minimal valid text payload
    payload = {
        "messaging_product": "whatsapp",
        "to": "1234567890",
        "type": "text",
        "text": {"body": "Hello!"}
    }

    response: WhatsAppResponse = await client.asend_text_message(payload)

    # Should take the "if" branch
    assert isinstance(response, WhatsAppResponse)
    assert response.response_status.status == "500"
    assert response.response_status.error == "server error"
    assert response.contacts == []
    assert response.messages == []



if __name__ == "__main__":
    # event_loop = asyncio.get_event_loop()
    # # event_loop.run_until_complete(atest_meta_batch_send_interactive_reply_message())
    # event_loop.run_until_complete(atest_meta_batch_send_template_message())
    # event_loop.close()
    # test_template_message()
    # test_regular_message()
    # test_interactive_message()
    # test_status_message()
    asyncio.run(atest_send_video_message())

