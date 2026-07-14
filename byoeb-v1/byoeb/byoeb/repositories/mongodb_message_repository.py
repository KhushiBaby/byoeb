import asyncio
from typing import Dict, Any, Optional, AsyncIterator, List, Tuple
from byoeb.repositories.message_repository import MessageRepository
from byoeb.repositories.mongodb_base_repository import MongoBaseRepository

class MongoMessageRepository(MessageRepository, MongoBaseRepository):

    def __init__(self, collection):
        super().__init__(collection)
        self._indexes_ready = asyncio.create_task(self._ensure_indexes())

    async def _ensure_indexes(self) -> None:
        await asyncio.gather(
            self._collection.create_index(
                [("message_data.incoming_timestamp", 1), ("message_data.message_category", 1)]
            ),
        )

    async def find_messages_by_time_range(self, 
                                        start_timestamp: int, 
                                        end_timestamp: int,
                                        message_categories: Optional[list[str]] = None,
                                        projection: Optional[Dict[str, Any]] = None,
                                        sort: Optional[List[Tuple[str, int]]] = None) -> AsyncIterator[Dict[str, Any]]:
        filter_dict = {
            "message_data.incoming_timestamp": {
                "$gte": start_timestamp, 
                "$lte": end_timestamp
            }
        }

        if message_categories:
            filter_dict["message_data.message_category"] = {"$in": message_categories}

        return self.find_all(filter_dict, projection, sort=sort)

    async def find_messages_by_user_ids(self, 
                                      user_ids: list[str],
                                      projection: Optional[Dict[str, Any]] = None) -> AsyncIterator[Dict[str, Any]]:
        filter_dict = {"message_data.user.user_id": {"$in": user_ids}}
        return self.find_all(filter_dict, projection)

    async def find_messages_by_message_ids(self, 
                                         message_ids: list[str],
                                         projection: Optional[Dict[str, Any]] = None) -> AsyncIterator[Dict[str, Any]]:
        filter_dict = {"message_data.message_context.message_id": {"$in": message_ids}}
        return self.find_all(filter_dict, projection)

    async def count_messages_by_time_range(self, 
                                         start_timestamp: int, 
                                         end_timestamp: int,
                                         message_categories: Optional[list[str]] = None) -> int:
        filter_dict = {
            "message_data.incoming_timestamp": {
                "$gte": start_timestamp, 
                "$lte": end_timestamp
            }
        }

        if message_categories:
            filter_dict["message_data.message_category"] = {"$in": message_categories}
        
        return await self.count(filter_dict)

    async def find_messages_by_district_and_time_range(self, 
                                                     district: str,
                                                     start_timestamp: int, 
                                                     end_timestamp: int,
                                                     message_categories: Optional[list[str]] = None) -> AsyncIterator[Dict[str, Any]]:
        filter_dict = {
            "message_data.incoming_timestamp": {
                "$gte": start_timestamp, 
                "$lte": end_timestamp
            },
            "message_data.user.user_location.district": district
        }

        if message_categories:
            filter_dict["message_data.message_category"] = {"$in": message_categories}

        return self.find_all(filter_dict)

    async def get_message_statistics_by_district(self, 
                                               start_timestamp: int, 
                                               end_timestamp: int,
                                               message_categories: Optional[list[str]] = None) -> AsyncIterator[Dict[str, Any]]:
        # This would use MongoDB aggregation pipeline
        # For now, return an empty async iterator - can be implemented with proper aggregation
        async def _empty():
            if False:
                yield {}
        return _empty()

    async def find_recent_messages_by_user(self, 
                                         user_id: str, 
                                         limit: int = 10) -> AsyncIterator[Dict[str, Any]]:
        filter_dict = {"message_data.user.user_id": user_id}
        sort = [("message_data.incoming_timestamp", -1)]  # Sort by timestamp descending
        return self.find_all(filter_dict, sort=sort, limit=limit)

    async def find_messages_by_category(self, 
                                      category: str,
                                      limit: Optional[int] = None) -> AsyncIterator[Dict[str, Any]]:
        filter_dict = {"message_data.message_category": category}
        return self.find_all(filter_dict, limit=limit or 0)
