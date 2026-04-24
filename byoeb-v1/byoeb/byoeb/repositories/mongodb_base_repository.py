from typing import List, Dict, Any, Optional, Tuple, final, AsyncIterator
from pymongo import UpdateOne
from pymongo.asynchronous.collection import AsyncCollection
from byoeb.repositories.base_repository import BaseRepository
from abc import ABC

class MongoBaseRepository(BaseRepository, ABC):

    def __init__(self, collection: AsyncCollection):
        self._collection = collection

    @final
    async def find_by_id(self, id: str) -> Optional[Dict[str, Any]]:
        return await self._collection.find_one({"_id": id})

    @final
    async def find_all(self, filter_dict: Optional[Dict[str, Any]] = None,  projection: Optional[Dict[str, Any]] = None, sort: Optional[List[Tuple[str, int]]] = None, limit: int = 0) -> AsyncIterator[Dict[str, Any]]:
        cursor = self._collection.find(filter_dict or {}, projection=projection, sort=sort, limit=limit)
        async for doc in cursor:
            yield doc

    @final
    async def count(self, filter_dict: Optional[Dict[str, Any]] = None) -> int:
        return await self._collection.count_documents(filter_dict or {})

    @final
    async def insert_one(self, document: Dict[str, Any]) -> str:
        result = await self._collection.insert_one(document)
        return str(result.inserted_id)

    @final
    async def insert_many(self, documents: List[Dict[str, Any]]) -> List[str]:
        result = await self._collection.insert_many(documents, ordered=False)
        return [str(doc_id) for doc_id in result.inserted_ids]

    @final
    async def update_one(self, filter_dict: Dict[str, Any], update_dict: Dict[str, Any]) -> bool:
        result = await self._collection.update_one(filter_dict, update_dict)
        return result.matched_count > 0

    @final
    async def update_many(self, filter_dict: Dict[str, Any], update_dict: Dict[str, Any]) -> int:
        result = await self._collection.update_many(filter_dict, update_dict)
        return result.modified_count

    @final
    async def delete_one(self, filter_dict: Dict[str, Any]) -> bool:
        result = await self._collection.delete_one(filter_dict)
        return result.deleted_count > 0

    @final
    async def delete_many(self, filter_dict: Dict[str, Any]) -> int:
        result = await self._collection.delete_many(filter_dict)
        return result.deleted_count

    @final
    async def bulk_update(self, bulk_queries: List[Tuple[Dict[str, Any], Dict[str, Any]]]) -> int:
        if not bulk_queries:
            return 0
        operations = [UpdateOne(filter=query, update=update) for query, update in bulk_queries]
        result = await self._collection.bulk_write(operations)
        return result.modified_count

