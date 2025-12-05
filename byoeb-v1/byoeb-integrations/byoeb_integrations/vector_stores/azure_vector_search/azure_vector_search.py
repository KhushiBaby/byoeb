import asyncio
import logging
from enum import Enum
from typing import Any, AsyncIterator, Coroutine, List, Optional
from tenacity import retry, stop_after_attempt, stop_after_delay, wait_exponential, wait_fixed
from tqdm.asyncio import tqdm
from byoeb_core.vector_stores.base import BaseVectorStore, VectorStoreMetadata
from byoeb_core.llms.base import BaseLLM
from azure.core.exceptions import HttpResponseError
from azure.search.documents import SearchIndexingBufferedSender
from azure.search.documents.aio import SearchClient
from azure.search.documents.indexes import SearchIndexClient
from azure.search.documents.indexes.models import (
    SearchIndex,
    SearchField,
    SimpleField,
    SearchableField,
    ComplexField,
    SearchFieldDataType,
    VectorSearch,
    HnswAlgorithmConfiguration,
    HnswParameters,
    VectorSearchAlgorithmKind,
    VectorSearchAlgorithmMetric,
    VectorSearchProfile,
    BM25SimilarityAlgorithm,
)
from azure.search.documents.models import VectorizableTextQuery, IndexAction
from azure.search.documents.indexes.models import AzureOpenAIVectorizer, AzureOpenAIVectorizerParameters
from byoeb_core.models.vector_stores.azure.azure_search import AzureSearchNode, Metadata
from byoeb_integrations.vector_stores.related_questions import aget_related_questions
from byoeb_core.models.vector_stores.chunk import Chunk, Chunk_metadata

logger = logging.getLogger(__name__)

class AzureVectorSearchType(Enum):
    BM25 = "bm25"
    DENSE = "dense"
    HYBRID = "hybrid"

class AzureVectorStore(BaseVectorStore):
    def __init__(
        self,
        service_name: str,
        index_name: str,
        embedding_function,
        api_key: Optional[str] = None,
        credential = None,
        vectorizer_params: Optional[AzureOpenAIVectorizerParameters] = None  # used when creating a new index
    ):
        if not service_name:
            raise ValueError("service_name is required")
        if not index_name:
            raise ValueError("index_name is required")
        if not embedding_function:
            raise ValueError("embedding_function is required")
        if not api_key and not credential:
            raise ValueError("api_key or credential is required")
        if api_key and credential:
            raise ValueError("only one of api_key or credential is required")
        if api_key:
            raise NotImplementedError("api_key is not supported yet")
    
        self.__service_name = service_name
        self.__index_name = index_name
        self.__embedding_function = embedding_function
        self.__credential = credential
        self.__endpoint = f"https://{self.__service_name}.search.windows.net"
        self.search_client = SearchClient(
            endpoint=self.__endpoint,
            index_name=self.__index_name,
            credential=credential
        )
        self.search_index_client = SearchIndexClient(
            endpoint=self.__endpoint,
            credential=credential
        )
        self.vectorizer_params = vectorizer_params

    def index_definition(self):
        return SearchIndex(
            name=self.__index_name,
            fields=[
                SimpleField(name="id", type=SearchFieldDataType.String, key=True, searchable=False, filterable=True, retrievable=True, stored=True, sortable=True, facetable=False),
                SearchableField(name="text", type=SearchFieldDataType.String, analyzer_name="standard.lucene", searchable=True, filterable=False, retrievable=True, stored=True, sortable=False, facetable=False),
                SearchField(
                    name="text_vector_3072",
                    type=SearchFieldDataType.Collection(SearchFieldDataType.Single),
                    searchable=True,
                    filterable=False,
                    retrievable=False,
                    stored=True,
                    sortable=False,
                    facetable=False,
                    vector_search_dimensions=3072,
                    vector_search_profile_name="default-vector-profile"
                ),
                ComplexField(name="metadata", fields=[
                    SimpleField(name="source", type=SearchFieldDataType.String, searchable=False, filterable=True, retrievable=True, stored=True, sortable=True, facetable=True),
                    SimpleField(name="creation_timestamp", type=SearchFieldDataType.String, searchable=False, filterable=True, retrievable=True, stored=True, sortable=True, facetable=False),
                    SimpleField(name="update_timestamp", type=SearchFieldDataType.String, searchable=False, filterable=True, retrievable=True, stored=True, sortable=True, facetable=False),
                ]),
                ComplexField(name="related_questions", fields=[
                    SearchField(name=lang, type=SearchFieldDataType.Collection(SearchFieldDataType.String), searchable=False, filterable=False, retrievable=True, stored=True, sortable=False, facetable=False)
                    for lang in ["en", "hi", "mr", "te"]
                ]),
            ],
            similarity=BM25SimilarityAlgorithm(),
            vector_search=VectorSearch(algorithms=[
                HnswAlgorithmConfiguration(name="default-hnsw-config", kind=VectorSearchAlgorithmKind.HNSW, parameters=HnswParameters(
                    metric=VectorSearchAlgorithmMetric.COSINE, m=4, ef_construction=400, ef_search=500
                ))
            ], profiles=[
                VectorSearchProfile(name="default-vector-profile", algorithm_configuration_name="default-hnsw-config", vectorizer_name="azure-openai-vectorizer")
            ], vectorizers=[
                AzureOpenAIVectorizer(vectorizer_name="azure-openai-vectorizer", parameters=self.vectorizer_params)
            ])
        )

    def fails(self, error: IndexAction):
        print("Failed to upload document")
        print(error.additional_properties)

    async def __prepare_azure_node(
        self,
        id,
        chunk,
        metadata,
        llm_client: BaseLLM,
        languages_translation_prompts: dict,
        system_prompt
    ) -> AzureSearchNode:
        related_questions = None
        if llm_client is not None:
            related_questions = await aget_related_questions(
                chunk,
                llm_client,
                languages_translation_prompts,
                system_prompt,
            )
        azure_doc = AzureSearchNode(
            id=id,
            text=chunk,
            metadata=Metadata(
                source=metadata["source"],
                creation_timestamp=metadata["creation_timestamp"],
                update_timestamp=metadata["update_timestamp"],
            ),
            text_vector_3072=await self.__embedding_function.aget_text_embedding(chunk),
            related_questions=related_questions,
        )
        return azure_doc

    def add_chunks(
        self,
        data_chunks: list, 
        metadata: list,
        ids: list = None,
        **kwargs
    ):
        raise NotImplementedError

    async def aadd_chunks(
        self,
        data_chunks: list,
        metadata: list,
        ids: list,
        llm_client: BaseLLM =None,
        languages_translation_prompts: dict = None,
        system_prompt = None,
        **kwargs
    ) -> AsyncIterator[str]:
        if languages_translation_prompts is not None and llm_client is None:
            raise ValueError("llm_client is required when languages are provided")
        
        sem = asyncio.Semaphore(16)
        lock = asyncio.Lock()
        locking_id = None

        @retry(stop=stop_after_attempt(5), wait=wait_fixed(15))
        async def run(id: Any, coro: Coroutine):
            # any error causes run() to be blocked until the errored task
            # succeeds to a retry. this is needed so we dont bog down due
            # to several tasks retrying all at once.
            nonlocal locking_id
            if locking_id != id:
                async with lock: ...
            async with sem:
                try:
                    result = await coro
                except:
                    if locking_id != id:
                        await lock.acquire()
                        locking_id = id
                    raise
                if locking_id == id:
                    locking_id = None
                    lock.release()
                return result

        def flush(nodes: list[Chunk]):
            with SearchIndexingBufferedSender(
                endpoint=self.__endpoint,
                index_name=self.__index_name,
                credential=self.__credential,
                on_error=self.fails
            ) as batch_client:
                current_documents = [node.model_dump(exclude_none=True, exclude_defaults=True) for node in nodes]
                batch_client.upload_documents(documents=current_documents)

        tasks = [run(id, self.__prepare_azure_node(
            id=id,
            chunk=chunk,
            metadata=metadata,
            llm_client=llm_client,
            languages_translation_prompts=languages_translation_prompts,
            system_prompt=system_prompt
        )) for id, chunk, metadata in zip(ids, data_chunks, metadata)]

        batch_nodes = []
        uploaded = 0
        for task in asyncio.as_completed(tasks):
            chunk = await task
            assert isinstance(chunk, AzureSearchNode)
            batch_nodes.append(chunk)
            if len(batch_nodes) >= 16:
                flush(batch_nodes)
                uploaded += len(batch_nodes)
                batch_nodes = []
            yield str(chunk.id)
        if len(batch_nodes) > 0:
            flush(batch_nodes)
            uploaded += len(batch_nodes)
            batch_nodes = []

    def update_chunks(
        self,
        data_chunks: list, 
        metadata: list,
        ids: list,
        **kwargs
    ):
        raise NotImplementedError

    def delete_chunks(self, ids: list, batch_size: int = 100, **kwargs):
        raise NotImplementedError

    async def adelete_chunks(self, ids: list, batch_size: int = 100, **kwargs) -> int:
        if not ids:
            logger.info("No chunk ids supplied for deletion; skipping")
            return 0

        total_batches = (len(ids) + batch_size - 1) // batch_size
        logger.info(f"Deleting {len(ids)} chunks from Azure index '{self.__index_name}' in {total_batches} batches")

        deleted = 0

        def on_error(error: IndexAction):
            try:
                doc_ref = getattr(error, "key", None) or getattr(error, "document", None) or "unknown"
                logger.error(f"Failed to delete document {doc_ref}: {getattr(error, 'additional_properties', None)}")
            except Exception:
                logger.error(f"Failed to delete document: {error}")

        try:
            with SearchIndexingBufferedSender(
                endpoint=self.__endpoint,
                index_name=self.__index_name,
                credential=self.__credential,
                on_error=on_error
            ) as batch_client:
                for i in range(0, len(ids), batch_size):
                    batch_ids = ids[i:i + batch_size]
                    batch_client.delete_documents(documents=[{"id": chunk_id} for chunk_id in batch_ids])
                    deleted += len(batch_ids)
                    batch_num = (i // batch_size) + 1
                    logger.debug(f"Deleted batch {batch_num}/{total_batches} ({len(batch_ids)} chunks)")
        except HttpResponseError as e:
            logger.error(f"Error while deleting chunks from Azure index '{self.__index_name}': {e}", exc_info=True)
            raise

        logger.info(f"✅ Deleted {deleted} chunks from Azure index '{self.__index_name}'")
        return deleted

    def retrieve_top_k_chunks(
        self,
        text: str,
        k: int,
        **kwargs
    ) -> List[Chunk]:
        raise NotImplementedError

    async def aretrieve_similar_chunks(self, text: str) -> List[Chunk]:
        return await self.aretrieve_top_k_chunks(text=text, k=1, search_type=AzureVectorSearchType.DENSE.value, select=["id"], vector_field="text_vector_3072")

    async def aretrieve_top_k_chunks(
        self,
        text: str,
        k: int,
        search_type=AzureVectorSearchType.HYBRID.value,
        select=None,
        vector_field=None,
        **kwargs
    ) -> List[Chunk]:
        chunk_list: List[Chunk] = []
        results = []
        if (search_type == AzureVectorSearchType.HYBRID or search_type == AzureVectorSearchType.DENSE) and vector_field is None:
            raise ValueError("vector_field is required for dense and hybrid search types")

        if search_type == AzureVectorSearchType.BM25.value:
            results = await self.search_client.search(
                search_text=text,
                select=select,
                top=k
            )
        elif search_type == AzureVectorSearchType.DENSE.value:
            vector_query = VectorizableTextQuery(
                text=text,
                k_nearest_neighbors=10,
                fields=vector_field
            )
            results = await self.search_client.search(
                vector_queries=[vector_query],
                select=select,
                top=k
            )
        elif search_type == AzureVectorSearchType.HYBRID.value:
            vector_query = VectorizableTextQuery(
                text=text,
                k_nearest_neighbors=10,
                fields=vector_field
            )
            results = await self.search_client.search(
                search_text=text,
                vector_queries=[vector_query],
                select=select,
                top=k
            )
        else:
            raise ValueError("Invalid search type")

        async for result in results:
            azure_search_result = AzureSearchNode(**result)
            if azure_search_result.metadata is None:
                metadata = None
            else:
                metadata = Chunk_metadata(
                    source=azure_search_result.metadata.source,
                    creation_timestamp=azure_search_result.metadata.creation_timestamp,
                    update_timestamp=azure_search_result.metadata.update_timestamp
                )
            chunk = Chunk(
                chunk_id=azure_search_result.id,
                text=azure_search_result.text,
                metadata=metadata,
                related_questions=azure_search_result.related_questions,
                similarity=result.get("@search.score", 0.0)
            )
            chunk_list.append(chunk)
        return chunk_list

    async def get_count(self) -> int:
        return await self.search_client.get_document_count()

    async def get_metadata(self):
        return VectorStoreMetadata(
            store_type="azure_search",
            collection=self.__index_name,
            count=await self.get_count(),
            capabilities={
                "hybrid_search": True,
                "vector_search": True,
                "bm25_search": True,
            },
        )

    def create_store(self):
        try:
            self.search_index_client.create_index(self.index_definition())
        except HttpResponseError as e:
            if "(ResourceNameAlreadyInUse)" not in e.message:
                # there has to be a better way to do this... 
                raise

    def delete_store(self):
        self.search_index_client.delete_index(self.__index_name)
        self.search_index_client.create_index(self.index_definition())
