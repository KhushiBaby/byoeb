import os
os.environ["AZURE_OPENAI_API_KEY"] = "sk-xxxx"

import hashlib
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from azure.core.exceptions import ResourceNotFoundError
from azure.identity import DefaultAzureCredential
from dotenv import load_dotenv

from byoeb_integrations import test_environment_path
from byoeb_integrations.vector_stores.azure_vector_search.azure_vector_search import (
    AzureVectorStore,
    AzureVectorSearchType,
)

pytestmark = pytest.mark.asyncio

load_dotenv(test_environment_path)


texts = [
    "Photosynthesis is the process by which green plants convert sunlight into chemical energy, producing oxygen and glucose.",
    "Chlorophyll, the green pigment in plants, absorbs light energy from the sun to drive photosynthesis.",
    "The two main stages of photosynthesis are the light-dependent reactions and the Calvin cycle.",
    "During the light-dependent reactions, sunlight is used to split water molecules, releasing oxygen and storing energy in ATP and NADPH.",
    "The Calvin cycle, also called the light-independent reaction, uses ATP and NADPH to convert carbon dioxide into glucose.",
    "Plants take in carbon dioxide through tiny openings called stomata and release oxygen as a byproduct of photosynthesis.",
    "Photosynthesis occurs in the chloroplasts, organelles found in plant cells that contain chlorophyll.",
    "Without photosynthesis, life on Earth would not exist as it provides oxygen and food for most living organisms.",
    "The equation for photosynthesis is: 6CO2 + 6H2O + light energy → C6H12O6 + 6O2.",
    "Photosynthesis is essential for maintaining atmospheric oxygen levels and reducing carbon dioxide in the environment.",
    "Algae and some bacteria, like cyanobacteria, also perform photosynthesis, contributing to global oxygen production.",
    "The rate of photosynthesis is influenced by factors such as light intensity, temperature, and carbon dioxide concentration.",
    "In desert plants, CAM photosynthesis allows them to conserve water by absorbing CO2 at night.",
    "C4 photosynthesis, used by crops like corn and sugarcane, improves efficiency in hot climates.",
    "Artificial photosynthesis is being studied to create clean energy by mimicking natural processes.",
    "The oxygen produced during photosynthesis supports aerobic respiration in animals and humans.",
    "Photosynthesis evolved over 2.5 billion years ago, leading to the Great Oxygenation Event.",
    "The process of photosynthesis plays a crucial role in the carbon cycle, recycling carbon between organisms and the atmosphere.",
    "Scientists study photosynthesis to improve crop yields and develop sustainable energy solutions.",
    "Deforestation and pollution negatively impact photosynthesis by reducing plant populations and increasing greenhouse gases."
]

languages_translation_prompts = {"hi": "You are an english to hindi translator."}

SERVICE_NAME = "dummy-search-service"
INDEX_NAME = "dummy_index"

# -------------------------
# Stubs (pure in-memory)
# -------------------------
class _DummyEmbed:
    """Embedding stub that matches the async interface your store expects."""
    def get_text_embedding(self, text: str):
        return [0.0] * 3072

    async def aget_text_embedding(self, text: str):
        return [0.0] * 3072

    def __call__(self, texts):
        return [[0.0] * 3072 for _ in texts]


async def _fake_agenerate_response(*args, **kwargs):
    """LLM stub: returns a tuple of strings (as your code expects)."""
    tagged = (
        "<q_1>What is photosynthesis?</q_1>"
        "<q_2>How does chlorophyll work?</q_2>"
        "<q_3>Why is photosynthesis important?</q_3>"
    )
    return tagged, tagged


@pytest.fixture
def embedding_fn_stub():
    return _DummyEmbed()


@pytest.fixture
def llm_client_stub():
    ns = SimpleNamespace()
    ns.agenerate_response = AsyncMock(side_effect=_fake_agenerate_response)
    return ns


@pytest.fixture(autouse=True)
def mock_search_clients(mocker):
    """
    Patch Azure Search clients where AzureVectorStore imports them.
    Ensures: no network, no real credentials, deterministic results.
    """
    path = "byoeb_integrations.vector_stores.azure_vector_search.azure_vector_search"

    mock_index_client = mocker.Mock(name="SearchIndexClient")
    mock_search_client = mocker.Mock(name="SearchClient")

    # Constructors return mocks
    mocker.patch(f"{path}.SearchIndexClient", return_value=mock_index_client)
    mocker.patch(f"{path}.SearchClient", return_value=mock_search_client)

    # Force "index not found" initially so code creates it
    mock_index_client.get_index.side_effect = ResourceNotFoundError("not found")
    mock_index_client.create_or_update_index.return_value = None
    mock_index_client.delete_index.return_value = None

    # Upload returns per-doc result objects
    mock_search_client.upload_documents.return_value = [{"key": "doc-1", "status": True}]

    # Search returns hits; ensure related_questions is a dict (model expects dict)

    class FakeAsyncPaged:
        def __init__(self, items: list[dict]): self._items = items

        def __aiter__(self): return self._async_generator()

        async def _async_generator(self):
            for item in self._items:
                yield item


    def _fake_search(*args, **kwargs):
        async def wrapper(): return FakeAsyncPaged([
            {
                "id": "1",
                "text": "Photosynthesis basics",
                "metadata": {"source": "0"},
                "related_questions": {},
            },
            {
                "id": "2",
                "text": "Chlorophyll overview",
                "metadata": {"source": "1"},
                "related_questions": {},
            },
        ])
        return wrapper()

    mock_search_client.search.side_effect = _fake_search
    return mock_index_client, mock_search_client

@pytest.mark.asyncio
async def test_azure_vector_search_upload_documents(embedding_fn_stub, llm_client_stub):
    ids = [hashlib.md5(text.encode()).hexdigest() for text in texts]
    metadatas = [
        {
            "source": str(i),
            "creation_timestamp": str(int(datetime.now().timestamp())),
            "update_timestamp": str(int(datetime.now().timestamp())),
        }
        for i in range(len(texts))
    ]

    azure_vector_search = AzureVectorStore(
        SERVICE_NAME,
        INDEX_NAME,
        embedding_fn_stub,
        credential=DefaultAzureCredential()
    )
    async for _ in azure_vector_search.aadd_chunks(
        ids=ids,
        data_chunks=texts,
        metadata=metadatas,
        llm_client=llm_client_stub,
        languages_translation_prompts=languages_translation_prompts,
        show_progress=True
    ): ...

@pytest.mark.asyncio
async def test_azure_vector_search_query(embedding_fn_stub):
    query_texts = [
        "What is photosynthesis?",
        "Explain chlorophyll",
        "What is photosynthesis?",
        "Explain chlorophyll"
    ]
    azure_vector_search = AzureVectorStore(
        SERVICE_NAME,
        INDEX_NAME,
        embedding_fn_stub,
        credential=DefaultAzureCredential()
    )
    for query_text in query_texts:
        start_time = datetime.now().timestamp()
        results = await azure_vector_search.aretrieve_top_k_chunks(
            text=query_text,
            k=3,
            search_type=AzureVectorSearchType.DENSE.value,
            select=["id", "text", "metadata", "related_questions"],
            vector_field="text_vector_3072"
        )
        print("Results: ", results)
        end_time = datetime.now().timestamp()
        print("Execution Time: ", end_time - start_time)

def test_azure_vector_search_rebuild(embedding_fn_stub):
    azure_vector_search = AzureVectorStore(
        SERVICE_NAME,
        INDEX_NAME,
        embedding_fn_stub,
        credential=DefaultAzureCredential()
    )
    azure_vector_search.delete_store()
    azure_vector_search.create_store()
