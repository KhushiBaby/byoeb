from typing import Dict, List, Optional

import pytest

from byoeb.services.knowledge_base.kb_service import KBService
from byoeb_core.media_storage.base import BaseMediaStorage
from byoeb_core.models.media_storage.file_data import FileData, FileMetadata
from byoeb_core.vector_stores.base import BaseVectorStore, VectorStoreMetadata
from llama_index.core.schema import TextNode
from llama_index.core.embeddings.mock_embed_model import MockEmbedding


class InMemoryMediaStorage(BaseMediaStorage):
    def __init__(self, files: Dict[str, FileData]):
        self._files = files

    async def aget_all_files_properties(self) -> List[FileMetadata]:
        return [file.metadata for file in self._files.values() if file.metadata is not None]

    async def aupload_file(self, file_name: str, file_path: str):
        return 201

    async def adownload_file(self, file_name: str) -> Optional[FileData]:
        return self._files[file_name] if file_name in self._files else None

    async def aget_file_properties(self, file_name: str):
        file = self._files.get(file_name)
        return file.metadata if file else None

    async def adelete_file(self, blob_name: str):
        self._files.pop(blob_name, None)
        return 204


class DummyVectorStore(BaseVectorStore):
    def __init__(self):
        self.chunks: List[dict] = []
        self.create_called = False
        self.retrieve_calls = 0
        self.embedding_fn = MockEmbedding(embed_dim=4)

    async def get_metadata(self) -> VectorStoreMetadata:
        return VectorStoreMetadata(store_type="dummy", collection="dummy-collection", count=len(self.chunks), capabilities={})

    def create_store(self):
        self.create_called = True

    def delete_store(self):
        self.chunks.clear()

    def add_chunks(self, data_chunks, metadata, ids, **kwargs):
        inserted_ids = []
        for text, md, cid in zip(data_chunks, metadata, ids or []):
            self.chunks.append({"id": cid, "text": text, "metadata": md})
            inserted_ids.append(cid)
        return inserted_ids

    async def aadd_chunks(self, data_chunks, metadata, ids, **kwargs):
        for id in self.add_chunks(data_chunks=data_chunks, metadata=metadata, ids=ids, **kwargs):
            yield id

    def update_chunks(self, data_chunks, metadata, ids, **kwargs):
        raise NotImplementedError

    def delete_chunks(self, ids, **kwargs):
        n = len([None for chunk in self.chunks if chunk["id"] in ids])
        self.chunks = [chunk for chunk in self.chunks if chunk["id"] not in ids]
        return n

    async def adelete_chunks(self, ids, **kwargs):
        return self.delete_chunks(ids, **kwargs)

    def retrieve_top_k_chunks(self, text, k, **kwargs):
        return []

    async def aretrieve_top_k_chunks(self, text, k, **kwargs):
        return []

    async def aretrieve_similar_chunks(self, text: str):
        self.retrieve_calls += 1
        return []

    async def get_count(self) -> int:
        return len(self.chunks)


class PartiallyFailingMediaStorage(InMemoryMediaStorage):
    def __init__(self, files: Dict[str, FileData], fail_names: set[str]):
        super().__init__(files)
        self.fail_names = set(fail_names)
        self.download_attempts: Dict[str, int] = {name: 0 for name in files}

    async def adownload_file(self, file_name: str) -> Optional[FileData]:
        self.download_attempts[file_name] = self.download_attempts.get(file_name, 0) + 1
        if file_name in self.fail_names:
            return None
        return await super().adownload_file(file_name)


def _file_data(text: str, name: str) -> FileData:
    return FileData(
        data=text.encode("utf-8"),
        metadata=FileMetadata(
            file_name=name,
            file_type=".txt",
            creation_time="2024-01-01T00:00:00Z",
        ),
    )


@pytest.mark.asyncio
async def test_create_kb_with_no_files_returns_zero():
    vector_store = DummyVectorStore()
    storage = InMemoryMediaStorage({})
    service = KBService(vector_store=vector_store, media_storage=storage)

    count = await service.upload(files=[])

    assert count == 0
    assert vector_store.create_called is True
    assert vector_store.chunks == []


@pytest.mark.asyncio
async def test_create_kb_skips_failed_downloads_and_ingests_successful_files():
    files = {
        "good.txt": _file_data("Good chunk text.", "good.txt"),
        "bad.txt": _file_data("Bad chunk text.", "bad.txt"),
    }
    storage = PartiallyFailingMediaStorage(files, fail_names={"bad.txt"})
    vector_store = DummyVectorStore()
    service = KBService(vector_store=vector_store, media_storage=storage)

    count = await service.upload(files=[f.metadata for f in files.values() if f.metadata is not None])

    assert count == 1
    assert len(vector_store.chunks) == 1
    assert "Good chunk text." in vector_store.chunks[0]["text"]
    assert storage.download_attempts["bad.txt"] == 1


@pytest.mark.asyncio
async def test_gather_similar_chunks_short_circuits_when_threshold_one():
    vector_store = DummyVectorStore()
    storage = InMemoryMediaStorage({})
    service = KBService(vector_store=vector_store, media_storage=storage, upsert_t=1.0)

    similar_chunks: List[str] = []
    chunks = [TextNode(text="alpha"), TextNode(text="beta")]

    progress = 0
    async for _ in service._gather_similar_chunks(chunks, similar_chunks):
        progress += 1

    assert progress == len(chunks)
    assert similar_chunks == []
    assert vector_store.retrieve_calls == 0
