import logging
import os
from typing import List

from chromadb import Collection
from byoeb_core.models.vector_stores.chunk import Chunk
from byoeb_core.vector_stores.base import BaseVectorStore, VectorStoreMetadata
from byoeb_integrations.vector_stores.chroma.base import ChromaDBVectorStore
from llama_index.vector_stores.chroma import ChromaVectorStore
from llama_index.core import VectorStoreIndex
from llama_index.core.schema import TextNode

logger = logging.getLogger(__name__)

class LlamaIndexChromaDBStore(BaseVectorStore):
    collection: Collection

    def __init__(
        self,
        persist_directory: str,
        collection_name: str,
        embedding_function=None
    ):
        
        self.__persist_directory = persist_directory
        self.__collection_name = collection_name
        self.__embedding_function = embedding_function
        self.chromadb = ChromaDBVectorStore(
            self.__persist_directory,
            self.__collection_name
        )
        self.vector_store_index = None
        self.__get_or_create_store()
        
    
    def __get_or_create_store(
        self
    ):
        if self.vector_store_index is not None:
            return self.vector_store_index
        self.collection = self.chromadb.get_or_create_collection()
        os.chmod(self.__persist_directory, 0o777)
        self.vector_store = ChromaVectorStore(chroma_collection=self.collection)
        self.vector_store_index = VectorStoreIndex.from_vector_store(
            vector_store=self.vector_store,
            embed_model=self.__embedding_function
        )
        return self.vector_store_index
    
    def delete_nodes(self, ids: List[str]):
        vector_store_index = self.__get_or_create_store()
        vector_store_index.delete_nodes(ids)

    def add_chunks(
        self,
        data_chunks: list, 
        metadata: list,
        ids: list,
        **kwargs
    ):
        if len(data_chunks) != len(metadata):
            raise ValueError("Data chunks and metadata should be of the same length")
        nodes = []
        for id, text, metadata in zip(ids, data_chunks, metadata):
            text_node = TextNode(id_=id, text=text, metadata=metadata)
            nodes.append(text_node)
        vector_store_index = self.__get_or_create_store()
        vector_store_index.insert_nodes(nodes)
        return [c.node_id for c in nodes]

    async def aadd_chunks(
        self,
        data_chunks,
        metadata,
        ids,
        batch_size: int = 100,
        **kwargs
    ):
        """Async wrapper for add_chunks to avoid blocking the event loop."""
        import asyncio
        from functools import partial
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        # Use functools.partial to pass keyword arguments to add_chunks when
        # running in an executor. Passing batch_size as a positional arg caused
        # a TypeError in callers because add_chunks does not accept extra
        # positional params.
        func = partial(self.add_chunks, data_chunks=data_chunks, metadata=metadata, ids=ids, batch_size=batch_size, **kwargs)

        if loop is None:
            result = func()
        else:
            result = await loop.run_in_executor(None, func)
        for id in result:
            yield id

    def update_chunks(
        self,
        data_chunks: list,
        metadata: list,
        ids: list,
        **kwargs
    ):
        self.collection.update(documents=data_chunks, metadatas=metadata, ids=ids)
    
    def delete_chunks(
        self,
        ids: list,
        **kwargs
    ):
        self.delete_nodes(ids)
    
    async def adelete_chunks(
        self,
        ids: list,
        **kwargs
    ):
        self.delete_nodes(ids)

    def retrieve_top_k_chunks(
        self,
        text: str,
        k: int,
        **kwargs
    ) -> List[Chunk]:
        vector_store_index = self.__get_or_create_store()
        retriever = vector_store_index.as_retriever(similarity_top_k=k)
        nodes = retriever.retrieve(text)
        chunk_list: List[Chunk] = []
        for node in nodes:
            chunk = Chunk(
                chunk_id=node.node.node_id,
                text=node.node.text,
                metadata=node.node.metadata,
                similarity=node.score
            )
            chunk_list.append(chunk)
        return chunk_list
    
    async def aretrieve_top_k_chunks(
        self,
        text: str,
        k: int,
        **kwargs
    ) -> List[Chunk]:
        vector_store_index = self.__get_or_create_store()
        retriever = vector_store_index.as_retriever(similarity_top_k=k)
        nodes = await retriever.aretrieve(text)
        chunk_list: List[Chunk] = []
        for node in nodes:
            chunk = Chunk(
                chunk_id=node.node.node_id,
                text=node.node.text,
                metadata=node.node.metadata,
                similarity=node.score
            )
            chunk_list.append(chunk)
        return chunk_list

    async def aretrieve_similar_chunks(self, text: str) -> List[Chunk]:
        return await self.aretrieve_top_k_chunks(text=text, k=1)

    async def get_count(self) -> int:
        return self.collection.count()

    async def get_metadata(self) -> VectorStoreMetadata:
        return VectorStoreMetadata(
            store_type="llama_index_chroma",
            collection=self.__collection_name,
            count=await self.get_count(),
            capabilities={
                "vector_search": True,
                "metadata_filters": True,
            },
        )

    def create_store(self):
        self.__get_or_create_store()

    def delete_store(self):
        self.chromadb.delete_store()
        self.vector_store_index = None
