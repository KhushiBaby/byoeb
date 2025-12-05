import os
from typing import List
from byoeb_core.models.vector_stores.chunk import Chunk
from chromadb import Documents, EmbeddingFunction, Embeddings
from byoeb_integrations.vector_stores.chroma.base import ChromaDBVectorStore
from byoeb_integrations.vector_stores.llama_index.llama_index_chroma_store import LlamaIndexChromaDBStore
from llama_index.core.embeddings.mock_embed_model import MockEmbedding
from llama_index.core.schema import TextNode

os.environ["AZURE_OPENAI_API_KEY"] = "sk-xxxx"
os.environ["CHROMA_TELEMETRY_DISABLED"] = "true"

class MockEmbeddingFunction(EmbeddingFunction):
    def __init__(self):
        self.__embedding_fn = MockEmbedding(embed_dim=4)

    def __call__(self, input: Documents) -> Embeddings:
        return [self.__embedding_fn.get_text_embedding(doc) for doc in input]


def test_chroma_vector_store_ops(tmp_path):
    embedding_fn = MockEmbeddingFunction()

    # IMPORTANT: use `new=` with a real function so the signature is (self, input)
    chromavs = ChromaDBVectorStore(str(tmp_path / "vdb"), "test", embedding_function=embedding_fn)
    chromavs.add_chunks(["hello", "world"], [{"a": 1}, {"b": 2}], ["1", "2"])

    responses: List[Chunk] = chromavs.retrieve_top_k_chunks("hello", 1)
    assert responses and responses[0].text == "hello"
    chromavs.delete_store()

def test_llama_index_chroma_vector_store_ops(tmp_path):
    embed_model = MockEmbedding(embed_dim=4)

    chromavs = LlamaIndexChromaDBStore(str(tmp_path / "vdb"), "test", embedding_function=embed_model)
    chromavs.add_chunks(["hello", "world"], [{"a": 1}, {"b": 2}], ["1", "2"])

    responses: List[Chunk] = chromavs.retrieve_top_k_chunks("hello", 1)
    assert responses and responses[0].text == "hello"

    count_before = chromavs.collection.count()
    chromavs.delete_store()
    chromavs.create_store()
    chromavs.add_chunks(["hello", "world"], [{"a": 1}, {"b": 2}], ["1", "2"])
    count_after = chromavs.collection.count()
    assert count_after == count_before