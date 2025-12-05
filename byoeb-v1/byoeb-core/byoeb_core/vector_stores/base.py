from abc import ABC, abstractmethod
from typing import Any, AsyncIterator, Dict, List, Optional

from pydantic import BaseModel, Field

from byoeb_core.models.vector_stores.chunk import Chunk

class VectorStoreMetadata(BaseModel):
    store_type: str
    collection: str
    count: Optional[int] = None
    capabilities: Dict[str, bool] = Field(default_factory=dict)

class BaseVectorStore(ABC):

    @abstractmethod
    async def get_metadata(self) -> VectorStoreMetadata:
        pass

    @abstractmethod
    def add_chunks(
        self,
        data_chunks: list, 
        metadata: list,
        ids: list,
        **kwargs
    ) -> List[str]:
        pass

    def aadd_chunks(
        self,
        data_chunks: list, 
        metadata: list,
        ids: list,
        **kwargs
    ) -> AsyncIterator[str]:
        raise NotImplementedError

    @abstractmethod
    def update_chunks(
        self,
        data_chunks: list, 
        metadata: list,
        ids: list,
        **kwargs
    ) -> Any:
        pass

    async def aupdate_chunks(
        self,
        data_chunks: list, 
        metadata: list,
        ids: list,
        **kwargs
    ) -> Any:
        raise NotImplementedError
    
    @abstractmethod
    def delete_chunks(
        self,
        ids: list,
        **kwargs
    ) -> Any:
        pass

    async def adelete_chunks(
        self,
        ids: list,
        **kwargs
    ) -> int:
        raise NotImplementedError

    @abstractmethod
    def retrieve_top_k_chunks(
        self,
        text: str,
        k: int,
        **kwargs
    ) -> List[Chunk]:
        pass
    
    async def aretrieve_top_k_chunks(
        self,
        text: str,
        k: int,
        **kwargs
    ) -> List[Chunk]:
        raise NotImplementedError

    @abstractmethod
    async def aretrieve_similar_chunks(self, text: str) -> List[Chunk]:
        pass

    @abstractmethod
    async def get_count(self) -> int:
        pass

    @abstractmethod
    def create_store(self):
        """
        Create vector store if it does not exist.
        """
        pass

    @abstractmethod
    def delete_store(self):
        pass
