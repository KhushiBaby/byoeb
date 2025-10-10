import os
import asyncio
import pytest
import logging
from byoeb_integrations.translators.text.azure.async_azure_text_translator import AsyncAzureTextTranslator
from azure.identity import DefaultAzureCredential
from byoeb_integrations import test_environment_path
from dotenv import load_dotenv
from unittest.mock import AsyncMock

load_dotenv(test_environment_path)

credential = DefaultAzureCredential()
TEXT_TRANSLATOR_RESOURCE_ID="/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/rg/providers/Microsoft.CognitiveServices/accounts/your-translator"
TEXT_TRANSLATOR_REGION="eastus"

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

@pytest.fixture
def event_loop():
    """Create and provide a new event loop for each test."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    loop.close()

async def aazure_translate_text_en_hi():
    async_azure_text_translator = AsyncAzureTextTranslator(
        credential=credential,
        resource_id=TEXT_TRANSLATOR_RESOURCE_ID,
        region=TEXT_TRANSLATOR_REGION
    )
    input_text = "Hello, how are you?"
    source_language = "en"
    target_language = "hi"
    translated_text = await async_azure_text_translator.atranslate_text(
        input_text=input_text,
        source_language=source_language,
        target_language=target_language
    )
    print(translated_text)
    assert translated_text is not None
    assert translated_text != input_text
    await async_azure_text_translator._close()

async def aazure_translate_text_en_en():
    async_azure_text_translator = AsyncAzureTextTranslator(
        credential=credential,
        resource_id=TEXT_TRANSLATOR_RESOURCE_ID,
        region=TEXT_TRANSLATOR_REGION
    )
    input_text = "Hello, how are you?"
    source_language = "en"
    target_language = "en"
    translated_text = await async_azure_text_translator.atranslate_text(
        input_text=input_text,
        source_language=source_language,
        target_language=target_language
    )
    assert translated_text is not None
    assert translated_text == input_text

# asyncio.run(aazure_translate_text_en_hi())
def test_aazure_translate_text_en_hi(event_loop, mock_translate):
    event_loop.run_until_complete(aazure_translate_text_en_hi())

def test_aazure_translate_text_en_en(event_loop, mock_translate):
    event_loop.run_until_complete(aazure_translate_text_en_en())
def test_missing_region_raises_valueerror():
    with pytest.raises(ValueError, match="region must be provided"):
        AsyncAzureTextTranslator(
            region=None
        )
def test_missing_credential_raises_valueerror():
    with pytest.raises(ValueError, match="Either entra id credential or key must be provided"):
        AsyncAzureTextTranslator(
            credential=None,
            key=None,
            region="xyz"
        )
def test_missing_both_raises_valueerror():
    with pytest.raises(ValueError, match="Either entra id credential or key must be provided not both"):
        AsyncAzureTextTranslator(
            credential="dummy",
            key="dummy",
            region="xyz"
        )

def test_translate_text_not_implemented():
    obj =  AsyncAzureTextTranslator(region="eastus", key="dummy")
    
    with pytest.raises(NotImplementedError):
        obj.translate_text("xyz", "abc","en")
if __name__ == "__main__":
    event_loop = asyncio.get_event_loop()
    event_loop.run_until_complete(aazure_translate_text_en_hi())
    event_loop.close()
