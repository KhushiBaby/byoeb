import asyncio
import logging
import hashlib
from datetime import datetime, timezone
from byoeb.kb_app.configuration.dependency_setup import (
    amedia_storage,
    vector_store,
    llm_client
)
from typing import AsyncIterator, List, Optional

from tenacity import retry, stop_after_attempt, wait_fixed
from tqdm import tqdm
from byoeb_core.data_parser.llama_index_text_parser import LLamaIndexTextParser, LLamaIndexTextSplitterType
from byoeb_core.llms.base import BaseLLM
from byoeb_core.media_storage.base import BaseMediaStorage
from byoeb_core.models.media_storage.file_data import FileMetadata, FileData
from byoeb_core.models.vector_stores.chunk import Chunk
from byoeb_core.vector_stores.base import BaseVectorStore
from llama_index.core.schema import BaseNode, TextNode

logger = logging.getLogger("kb_service")
text_parser = LLamaIndexTextParser(
        chunk_size=300,
        chunk_overlap=50,
    )
class KBService:
    def __init__(self, vector_store: BaseVectorStore, media_storage: BaseMediaStorage, llm_client: Optional[BaseLLM] = None, text_parser_instance=None, upsert_t: float = 0.95):
        self.vector_store = vector_store
        self.media_storage = media_storage
        self.llm_client = llm_client
        self.text_parser = text_parser_instance or text_parser
        self.upsert_t = upsert_t

    async def _gather_similar_chunks(self, chunks: List[BaseNode], out: List[str], n_concurrency=4) -> AsyncIterator[tuple[str, list[Chunk], int]]:
        if self.upsert_t >= 1.00:
            for c in chunks:
                yield c.node_id, [], 0
            return

        sem = asyncio.Semaphore(n_concurrency)

        @retry(stop=stop_after_attempt(5), wait=wait_fixed(15))
        async def run(id: str, text: str):
            async with sem:
                return id, await self.vector_store.aretrieve_similar_chunks(text=text)

        tasks = []
        for c in chunks:
            text = c.text if isinstance(c, TextNode) else str(c)
            tasks.append(run(c.node_id, text))

        for task in asyncio.as_completed(tasks):
            id, chunks = await task
            n_evicted = 0
            for chunk in chunks:
                assert isinstance(chunk, Chunk)
                if chunk.similarity >= self.upsert_t:
                    out.append(chunk.chunk_id)
                    n_evicted += 1
            yield id, chunks, n_evicted

    async def _add_nodes_to_vector_store(self, chunks: List[BaseNode], similar_chunks: List[str], llm_client: Optional[BaseLLM] = None, batch_size: Optional[int] = None) -> AsyncIterator[str]:
        from byoeb.kb_app.configuration.config import prompt_config

        if not chunks:
            logger.info("No chunks to ingest")
            return

        now_ts = str(int(datetime.now(timezone.utc).timestamp()))

        data_chunks = []
        metadata_list = []
        insert_ids = []

        for c in chunks:
            text = c.text if isinstance(c, TextNode) else str(c)
            file_name = c.metadata.get("file_name", c.metadata.get("source", "unknown")) if isinstance(c, BaseNode) else "unknown"
            md = {
                "source": file_name,
                "creation_timestamp": now_ts,
                "update_timestamp": now_ts,
            }

            cid = getattr(c, "chunk_id", None) or getattr(c, "node_id", None) or hashlib.md5(text.encode()).hexdigest()

            data_chunks.append(text)
            metadata_list.append(md)
            insert_ids.append(cid)

        if similar_chunks:
            try:
                await self.vector_store.adelete_chunks(ids=similar_chunks)
            except NotImplementedError:
                logger.info("Vector store does not support deletes; inserting matched chunks instead")

        if not insert_ids:
            logger.info("No new chunks to insert after applying upsert threshold")
            return

        bs = batch_size or 32
        try:
            async for id in self.vector_store.aadd_chunks(
                data_chunks=data_chunks,
                metadata=metadata_list,
                ids=insert_ids,
                llm_client=llm_client or self.llm_client,
                languages_translation_prompts=prompt_config.get("languages_translation_prompts", {})
            ):
                yield id
            logger.info(f"✅ Uploaded {len(insert_ids)} chunks to {type(self.vector_store).__name__} (upserted {len(similar_chunks)})")
        except AttributeError:
            logger.debug("vector_store has no aadd_chunks; falling back to sync add_chunks")
            for id in self.vector_store.add_chunks(data_chunks=data_chunks, metadata=metadata_list, ids=insert_ids, batch_size=bs):
                yield id

    async def _abulk_download_files(self, all_files: List[FileMetadata]) -> List[FileData]:
        def create_batches(batch_size=5):
            return [all_files[i:i + batch_size] for i in range(0, len(all_files), batch_size)]

        async def get_batch_results(batch):
            tasks = []
            for file in batch:
                logger.debug(f"  📥 Queuing download for: {file.file_name}")
                task = self.media_storage.adownload_file(file.file_name)
                tasks.append(task)
            return await asyncio.gather(*tasks)

        files_data = []
        batches = create_batches(5)
        logger.info(f"📦 Processing {len(batches)} batches of files")

        for batch_idx, batch in enumerate(batches, 1):
            logger.info(f"  Processing batch {batch_idx}/{len(batches)} ({len(batch)} files)")
            batch_results = await get_batch_results(batch)

            for idx, response in enumerate(batch_results):
                file_name = batch[idx].file_name if idx < len(batch) else "unknown"
                if response is None:
                    logger.warning(f"  ⚠️  Failed to download {file_name}: (file not found)")
                    continue

                try:
                    response = FileData(**response.model_dump())
                    files_data.append(response)
                    logger.debug(f"  ✅ Downloaded {file_name} ({len(response.data)} bytes)")
                except Exception as e:
                    logger.error(f"  ❌ Error processing {file_name}: {str(e)}")

        logger.info(f"✅ Successfully downloaded {len(files_data)}/{len(all_files)} files")
        return files_data

    async def upload(self, files: List[FileMetadata]):
        self.vector_store.create_store()

        logger.info(f"📥 Starting KB upload for {len(files)} files")
        if not files:
            logger.info("📄 No files provided for upload")
            return 0

        files_data = await self._abulk_download_files(files)
        logger.info(f"✅ Downloaded {len(files_data)} files successfully")
        if not files_data:
            logger.info("📄 No files downloaded successfully; skipping ingestion")
            return 0

        logger.info("🔤 Parsing files into chunks")
        try:
            chunks = self.text_parser.get_chunks_from_collection(
                files_data,
                splitter_type=LLamaIndexTextSplitterType.SENTENCE
            )
            logger.info(f"✅ Created {len(chunks)} chunks from {len(files_data)} files")
        except Exception as e:
            logger.error(f"❌ Error parsing chunks: {str(e)}", exc_info=True)
            raise


        chunk_filenames = {chunk.node_id: chunk.metadata["file_name"] for chunk in chunks}
        progress_values = {f.metadata.file_name if f.metadata else "Unknown": 0 for f in files_data}
        for chunk in chunks:
            progress_values[chunk_filenames[chunk.node_id]] += 1

        logger.info("🔄 Retrieving similar chunks for upserting")
        similar_chunks: List[str] = []
        progress = {k: tqdm(total=v, desc=k, position=i) for i, (k, v) in enumerate(progress_values.items())}
        try:
            evicted_totals = {k: 0 for k in progress_values.keys()}
            async for id, buf, n_evicted in self._gather_similar_chunks(chunks, similar_chunks):
                file_name = chunk_filenames[id]
                bar = progress[file_name]
                evicted_totals[file_name] += n_evicted
                bar.set_description("%s (%d found)" % (file_name, evicted_totals[file_name]))
                bar.update(len(buf))
        finally:
            for bar in progress.values():
                bar.close()

        logger.info("💾 Upserting chunks to vector store")
        progress = {k: tqdm(total=v, desc=k, position=i) for i, (k, v) in enumerate(progress_values.items())}
        try:
            async for id in self._add_nodes_to_vector_store(chunks, similar_chunks):
                progress[chunk_filenames[id]].update(1)
        except Exception as e:
            logger.error(f"❌ Error upserting nodes to vector store: {str(e)}", exc_info=True)
            raise
        finally:
            for bar in progress.values():
                bar.close()

        collection_count = await self.vector_store.get_count()
        logger.info(f"📊 Final collection count: {collection_count}")
        return collection_count


def _get_default_kb_service():
    return KBService(vector_store=vector_store, media_storage=amedia_storage, llm_client=llm_client)


async def upload(files: List[FileMetadata]):
    svc = _get_default_kb_service()
    return await svc.upload(files)
