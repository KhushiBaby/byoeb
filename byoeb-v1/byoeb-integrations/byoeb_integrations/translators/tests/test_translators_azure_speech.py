import asyncio
import os
import pytest, types
import azure.cognitiveservices.speech as speechsdk
from datetime import datetime
from byoeb_integrations.translators.speech.azure.async_azure_speech_translator import AsyncAzureSpeechTranslator
from byoeb_integrations.translators.speech.azure.async_azure_openai_whisper import AsyncAzureOpenAIWhisper
from azure.identity import get_bearer_token_provider, DefaultAzureCredential
from byoeb_integrations import test_environment_path
from dotenv import load_dotenv
from pydub import AudioSegment
from pydub.silence import detect_leading_silence
import io
import pytest
from unittest.mock import AsyncMock

@pytest.fixture
def mock_translate(mocker):
    async_mock = AsyncMock(side_effect=lambda **kwargs: (
        "नमस्ते, आप कैसे हैं?" if kwargs.get("target_language") == "hi" else kwargs.get("input_text")
    ))
    mocker.patch(
        "byoeb_integrations.translators.text.azure.async_azure_text_translator.AsyncAzureTextTranslator.atranslate_text",
        new=async_mock
    )
    # Also mock _close so awaiting it doesn't touch real resources
    mocker.patch(
        "byoeb_integrations.translators.text.azure.async_azure_text_translator.AsyncAzureTextTranslator._close",
        new=AsyncMock()
    )
    return async_mock

load_dotenv(test_environment_path)
# ibc = InteractiveBrowserCredential()
# aadToken = ibc.get_token("https://cognitiveservices.azure.com/.default").token
token_provider = get_bearer_token_provider(
    DefaultAzureCredential(), "https://cognitiveservices.azure.com/.default"
)
# print(aadToken)
SPEECH_TRANSLATOR_RESOURCE_ID = "/subscriptions/dummy-sub/resourceGroups/dummy-rg/providers/Microsoft.CognitiveServices/accounts/dummy-speech"
SPEECH_TRANSLATOR_REGION = "eastus"
WHISPER_ENDPOINT = "https://dummy.openai.azure.com"
WHISPER_MODEL = "whisper-1"
WHISPER_API_VERSION = "2024-05-01-preview"

DUMMY_TOKEN_PROVIDER = lambda: "dummy-token"

# ✅ stub the networky methods so no Azure calls happen
@pytest.fixture(autouse=True)
def stub_speech_and_whisper(mocker):
    mocker.patch(
        "byoeb_integrations.translators.speech.azure.async_azure_speech_translator.AsyncAzureSpeechTranslator.atext_to_speech",
        new=AsyncMock(return_value=b"FAKE_WAV_BYTES"),
    )
    mocker.patch(
        "byoeb_integrations.translators.speech.azure.async_azure_speech_translator.AsyncAzureSpeechTranslator.aspeech_to_text",
        new=AsyncMock(side_effect=lambda *args, **kwargs:
            "Hello how are you?" if (kwargs.get("source_language") or "").startswith("en")
            else "नमस्कार क्या हालचाल हैं?"
        ),
    )
    mocker.patch(
        "byoeb_integrations.translators.speech.azure.async_azure_openai_whisper.AsyncAzureOpenAIWhisper.aspeech_to_text",
        new=AsyncMock(return_value="Hello how are you?"),
    )


# TODO - Add tests for the AsyncAzureSpeechTranslator class using token provider
@pytest.fixture
def event_loop():
    """Create and provide a new event loop for each test."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    loop.close()

async def aazure_openai_whisper_translate_en():
    async_azure_openai_whisper = AsyncAzureOpenAIWhisper(
        token_provider=token_provider,
        model=WHISPER_MODEL,
        azure_endpoint=WHISPER_ENDPOINT,
        api_version=WHISPER_API_VERSION
    )
    text = "Hello how are you?"
    async_azure_speech_translator = AsyncAzureSpeechTranslator(
        region=SPEECH_TRANSLATOR_REGION,
        token_provider=token_provider,
        resource_id=SPEECH_TRANSLATOR_RESOURCE_ID,
    )
    result = await async_azure_speech_translator.atext_to_speech(
        input_text=text,
        source_language="en",
    )
   
    new_text = await async_azure_openai_whisper.aspeech_to_text(
        audio_data=result,
    )
    print(new_text)
    assert new_text is not None
    assert new_text.lower().__contains__("hello")

async def aazure_openai_whisper_translate_hi():
    async_azure_openai_whisper = AsyncAzureOpenAIWhisper(
        token_provider=token_provider,
        model=WHISPER_MODEL,
        azure_endpoint=WHISPER_ENDPOINT,
        api_version=WHISPER_API_VERSION
    )
    text = "2.5 किलोग्राम से कम वजन वाले शिशुओं को अतिरिक्त गर्मी प्रदान करके गर्म रखा जाना चाहिए। परिवार को यह सुनिश्चित करना चाहिए कि बच्चे को पतली चादर और कंबल से अच्छी तरह लपेटा जाए, गर्मी के नुकसान को रोकने के लिए सिर को ढंका जाए, और बच्चे को मां के पेट और छाती के बहुत करीब रखा जाए। कपड़े में लिपटे गर्म पानी से भरी बोतलों को बच्चे के कंबल के दोनों ओर रखा जा सकता है। जब मां के शरीर के करीब नहीं रखा जाता है, तो बच्चे को अधिक बार खिलाया जाना चाहिए।"
    async_azure_speech_translator = AsyncAzureSpeechTranslator(
        region=SPEECH_TRANSLATOR_REGION,
        token_provider=token_provider,
        resource_id=SPEECH_TRANSLATOR_RESOURCE_ID,
    )
    start_time = int(datetime.now().timestamp())
    result = await async_azure_speech_translator.atext_to_speech(
        input_text=text,
        source_language="hi",
    )
    end_time = int(datetime.now().timestamp())
    print(f"Time taken to convert text to speech: {end_time - start_time} seconds")
    new_text = await async_azure_openai_whisper.aspeech_to_text(
        audio_data=result,
    )
    print(new_text)
    text = "2.5 किलोग्राम से कम वजन वाले शिशुओं को अतिरिक्त गर्मी प्रदान करके गर्म रखा जाना चाहिए। परिवार को यह सुनिश्चित करना चाहिए कि बच्चे को पतली चादर और कंबल से अच्छी तरह लपेटा जाए, गर्मी के नुकसान को रोकने के लिए सिर को ढंका जाए, और बच्चे को मां के पेट और छाती के बहुत करीब रखा जाए। कपड़े में लिपटे गर्म पानी से भरी बोतलों को बच्चे के कंबल के दोनों ओर रखा जा सकता है। जब मां के शरीर के करीब नहीं रखा जाता है, तो बच्चे को अधिक बार खिलाया जाना चाहिए।"
    start_time = int(datetime.now().timestamp())
    result = await async_azure_speech_translator.atext_to_speech(
        input_text=text,
        source_language="hi",
    )
    end_time = int(datetime.now().timestamp())
    print(f"Time taken to convert text to speech: {end_time - start_time} seconds")
    text = "2.5 किलोग्राम से कम वजन वाले शिशुओं को अतिरिक्त गर्मी प्रदान करके गर्म रखा जाना चाहिए। परिवार को यह सुनिश्चित करना चाहिए कि बच्चे को पतली चादर और कंबल से अच्छी तरह लपेटा जाए, गर्मी के नुकसान को रोकने के लिए सिर को ढंका जाए, और बच्चे को मां के पेट और छाती के बहुत करीब रखा जाए। कपड़े में लिपटे गर्म पानी से भरी बोतलों को बच्चे के कंबल के दोनों ओर रखा जा सकता है। जब मां के शरीर के करीब नहीं रखा जाता है, तो बच्चे को अधिक बार खिलाया जाना चाहिए।"
    start_time = int(datetime.now().timestamp())
    result = await async_azure_speech_translator.atext_to_speech(
        input_text=text,
        source_language="hi",
    )
    end_time = int(datetime.now().timestamp())
    print(f"Time taken to convert text to speech: {end_time - start_time} seconds")
    text = "2.5 किलोग्राम से कम वजन वाले शिशुओं को अतिरिक्त गर्मी प्रदान करके गर्म रखा जाना चाहिए। परिवार को यह सुनिश्चित करना चाहिए कि बच्चे को पतली चादर और कंबल से अच्छी तरह लपेटा जाए, गर्मी के नुकसान को रोकने के लिए सिर को ढंका जाए, और बच्चे को मां के पेट और छाती के बहुत करीब रखा जाए। कपड़े में लिपटे गर्म पानी से भरी बोतलों को बच्चे के कंबल के दोनों ओर रखा जा सकता है। जब मां के शरीर के करीब नहीं रखा जाता है, तो बच्चे को अधिक बार खिलाया जाना चाहिए।"
    start_time = int(datetime.now().timestamp())
    result = await async_azure_speech_translator.atext_to_speech(
        input_text=text,
        source_language="hi",
    )
    end_time = int(datetime.now().timestamp())
    print(f"Time taken to convert text to speech: {end_time - start_time} seconds")
    assert new_text is not None
    # assert new_text.lower().__contains__("नम")

async def aazure_bytes_speech_translate_en():
    
    text = "Hello how are you?" 
    async_azure_speech_translator = AsyncAzureSpeechTranslator(
        region=SPEECH_TRANSLATOR_REGION,
        token_provider=token_provider,
        resource_id=SPEECH_TRANSLATOR_RESOURCE_ID,
    )
    result = await async_azure_speech_translator.atext_to_speech(
        input_text=text,
        source_language="en",
    )
   
    new_text = await async_azure_speech_translator.aspeech_to_text(
        audio_data=result,
        source_language="en",
    )
    print(new_text)
    assert new_text is not None
    assert new_text.lower().__contains__("hello")

async def aazure_bytes_speech_translate_hi():
    text = "नमस्कार क्या हालचाल हैं?"
    async_azure_speech_translator = AsyncAzureSpeechTranslator(
        region=SPEECH_TRANSLATOR_REGION,
        token_provider=token_provider,
        resource_id=SPEECH_TRANSLATOR_RESOURCE_ID,
    )
    result = await async_azure_speech_translator.atext_to_speech(
        input_text=text,
        source_language="hi",
    )
    with open("audio.wav", "wb") as f:
        f.write(result)
    new_text = await async_azure_speech_translator.aspeech_to_text(
        audio_data=result,
        source_language="hi",
    )
    assert new_text is not None
    assert new_text.lower().__contains__("नमस्कार")
    
def test_aazure_speech_translate_en(event_loop, mock_translate):
    event_loop.run_until_complete(aazure_bytes_speech_translate_en())

def test_aazure_speech_translate_hi(event_loop, mock_translate):
    event_loop.run_until_complete(aazure_bytes_speech_translate_hi())

def test_aazure_openai_whisper_translate_en(event_loop, mock_translate):
    event_loop.run_until_complete(aazure_openai_whisper_translate_en())

def test_aazure_openai_whisper_translate_hi(event_loop, mock_translate):
    event_loop.run_until_complete(aazure_openai_whisper_translate_hi())
def test_missing_model_raises_valueerror():
    with pytest.raises(ValueError, match="model must be provided"):
        AsyncAzureOpenAIWhisper(
            token_provider=token_provider,
        model=None,
        azure_endpoint=WHISPER_ENDPOINT,
        api_version=WHISPER_API_VERSION
        )


def test_missing_api_version_raises_valueerror():
    with pytest.raises(ValueError, match="api_version must be provided"):
        AsyncAzureOpenAIWhisper(
              token_provider=token_provider,
        model=WHISPER_MODEL,
        azure_endpoint=WHISPER_ENDPOINT,
        api_version=None
        )


def test_missing_endpoint_raises_valueerror():
    with pytest.raises(ValueError, match="azure_endpoint must be provided"):
        AsyncAzureOpenAIWhisper(
            
         token_provider=token_provider,
        model=WHISPER_MODEL,
        azure_endpoint=None,
        api_version=WHISPER_API_VERSION 

        )
def test_missing_token():
	obj = AsyncAzureOpenAIWhisper(
		token_provider=None,
        model=WHISPER_MODEL,
        azure_endpoint=WHISPER_ENDPOINT,
        api_key="dummy",
        api_version=WHISPER_API_VERSION 
	    )
	assert obj is not None 
def test_missing_token_apikey_raises_valueerror():
    with pytest.raises(ValueError, match="Either token_provider or api_key must be provided"):
        AsyncAzureOpenAIWhisper(
            
            token_provider=None,
        model=WHISPER_MODEL,
        azure_endpoint=WHISPER_ENDPOINT,
        api_key=None,
        api_version=WHISPER_API_VERSION 

        )
def test_speech_to_text_not_implemented():
    obj = AsyncAzureOpenAIWhisper(
        model="whisper-1",
        azure_endpoint="https://dummy.openai.azure.com",
        api_key="FAKE_KEY",
        api_version="2024-05-01-preview"
    )
    with pytest.raises(NotImplementedError):
        obj.speech_to_text("fake.wav", "en")
def test_text_to_speech_not_implemented():
    obj = AsyncAzureOpenAIWhisper(
        model="whisper-1",
        azure_endpoint="https://dummy.openai.azure.com",
        api_key="FAKE_KEY",
        api_version="2024-05-01-preview"
    )
    with pytest.raises(NotImplementedError):
        obj.text_to_speech("fake.wav", "en")
@pytest.mark.asyncio
async def test_atext_to_speech_not_implemented():
    whisper = AsyncAzureOpenAIWhisper(
        model="whisper-1",
        azure_endpoint="https://dummy.endpoint",
        api_key="FAKE_KEY",
        api_version="2024-05-01-preview"
    )
    with pytest.raises(NotImplementedError):
        await whisper.atext_to_speech("hello", "en")
def test_missing_region_raises_valueerror():
    with pytest.raises(ValueError, match="region must be provided"):
        AsyncAzureSpeechTranslator(
            region=None
        )
def test_missing_tokenandkey_raises_valueerror():
    with pytest.raises(ValueError, match="Either token_provider or key must be provided with region"):
        AsyncAzureSpeechTranslator(
            region="dummy",
            token_provider=None,
            key=None
            
        )
def test_missing_tokenandresource_id_raises_valueerror():
    with pytest.raises(ValueError, match="resource_id must be provided with token_provider"):
        AsyncAzureSpeechTranslator(
            region="dummy",
            token_provider="xyz",
            key="k",
            resource_id=None
            
        )
def test_speech_to_text_not_implemented_translator():
    obj =  AsyncAzureSpeechTranslator(region="eastus", key="dummy")
    
    with pytest.raises(NotImplementedError):
        obj.speech_to_text("fake.wav", "en")

def test_text_to_speech_not_implemented_translator():
    obj = AsyncAzureSpeechTranslator(region="eastus", key="dummy")
    with pytest.raises(NotImplementedError):
        obj.text_to_speech("Hello", "en")
        

def test_change_speech_voice_clears_cache():
    # Initialize translator with a dummy key
    translator = AsyncAzureSpeechTranslator(region="eastus", key="dummy")

    # Pre-fill the synthesizer cache
    translator._AsyncAzureSpeechTranslator__synthesizers[("en", "female")] = ("cached_synth", 12345)

    # Ensure cache is initially filled
    assert ("en", "female") in translator._AsyncAzureSpeechTranslator__synthesizers

    # Call change_speech_voice, which should clear the cache
    translator.change_speech_voice("male")

    # Assert cache is cleared
    assert translator._AsyncAzureSpeechTranslator__synthesizers == {}

import azure.cognitiveservices.speech as speechsdk


def test_get_speech_config_branches_safe():
    # Branch 1: Using key
    translator_key = AsyncAzureSpeechTranslator(region="eastus", key="dummy")
    config_key = translator_key._AsyncAzureSpeechTranslator__get_speech_config()
    assert isinstance(config_key, speechsdk.SpeechConfig)

    # Branch 2: Using token_provider
    translator_token = AsyncAzureSpeechTranslator(
        region="eastus",
        token_provider=lambda: "dummy_token",
        resource_id="dummy_resource"
    )
    config_token = translator_token._AsyncAzureSpeechTranslator__get_speech_config()
    assert isinstance(config_token, speechsdk.SpeechConfig)
@pytest.mark.asyncio
async def test_get_synthesizer_cache_and_ttl(monkeypatch):
    translator = AsyncAzureSpeechTranslator(region="eastus", key="dummy")

    # Dummy synthesizer that returns FAKE_WAV_BYTES
    class DummySynth:
        def speak_text_async(self, text):
            return types.SimpleNamespace(get=lambda: types.SimpleNamespace(audio_data=b"FAKE_WAV_BYTES"))

    class DummySpeechConfig:
        def set_speech_synthesis_output_format(self, fmt):
            pass

    # Patch Azure classes
    monkeypatch.setattr("azure.cognitiveservices.speech.SpeechConfig", lambda *a, **kw: DummySpeechConfig())
    monkeypatch.setattr("azure.cognitiveservices.speech.SpeechSynthesizer", lambda *a, **kw: DummySynth())

    # 1️⃣ First call creates synthesizer (new cache)
    synth1 = await translator._AsyncAzureSpeechTranslator__get_synthesizer("en")
    assert synth1 is not None

    # 2️⃣ Second call reuses cache (lines 94–99)
    synth2 = await translator._AsyncAzureSpeechTranslator__get_synthesizer("en")
    assert synth2 == synth1

    # 3️⃣ Simulate TTL expiration: replace timestamp with 0
    key = ("en", translator._AsyncAzureSpeechTranslator__speech_voice)
    translator._AsyncAzureSpeechTranslator__synthesizers[key] = (synth1, 0)

    # Third call should create new synthesizer (TTL expired)
    synth3 = await translator._AsyncAzureSpeechTranslator__get_synthesizer("en")
    assert synth3 is not None
    assert synth3 != synth1  # new instance after TTL
def test_change_voice_dict_clears_cache():
    translator = AsyncAzureSpeechTranslator(region="eastus", key="dummy")
    
    # Pre-fill synthesizer cache
    translator._AsyncAzureSpeechTranslator__synthesizers[("en", "female")] = ("cached_synth", 12345)
    assert ("en", "female") in translator._AsyncAzureSpeechTranslator__synthesizers

    # New voice dict
    new_dict = {
        "male": {"en-IN": "en-IN-TestMale"},
        "female": {"en-IN": "en-IN-TestFemale"}
    }

    translator.change_voice_dict(new_dict)

    # Assert cache cleared
    assert translator._AsyncAzureSpeechTranslator__synthesizers == {}

    # Assert dict updated
    assert translator._AsyncAzureSpeechTranslator__voice_dict == new_dict



if __name__ == "__main__":
    event_loop = asyncio.get_event_loop()
    event_loop.run_until_complete(aazure_openai_whisper_translate_hi())
    # event_loop.run_until_complete(aazure_bytes_speech_translate_en())
    event_loop.close()
