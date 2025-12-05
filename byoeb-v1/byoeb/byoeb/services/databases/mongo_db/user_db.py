import byoeb.services.chat.constants as constants
from aiocache import Cache
from datetime import datetime, timezone
from byoeb_core.models.byoeb.user import User
from byoeb.factory import MongoDBFactory
from typing import List, Dict, Any, Optional
from byoeb.services.databases.mongo_db.base import BaseMongoDBService
import os

class UserMongoDBService(BaseMongoDBService):
    """Service class for user-related MongoDB operations."""

    def __init__(self, config, mongo_db_factory: MongoDBFactory):
        super().__init__(config, mongo_db_factory)
        self._history_length = self._config["app"]["history_length"]
        self.collection_name = self._config["databases"]["mongo_db"]["user_collection"]
        self.cache = Cache(Cache.MEMORY)
        # Note: _get_repository_factory() is now provided by BaseMongoDBService

    async def fetch_phone_numbers_for_asha_and_test_users(self) -> List[str]:
        """
        Retrieves phone numbers for all ASHA workers and test users from the database.

        Returns:
            List[str]: Phone numbers of ASHA workers and test users
        """
        repository_factory = await self._get_repository_factory()
        user_repository = await repository_factory.get_user_repository()

        # Delegate selection logic (including TEST_USERS_ONLY handling) to repository
        # users = await user_repository.find_test_users_by_types(["others", "asha"]) # all users for future reference
        users = await user_repository.find_test_users_by_types(["others"]) # only others users for now

        numbers: List[str] = []
        for user_document in users:
            phone_number = user_document.get("User", {}).get("phone_number_id")
            if phone_number:
                numbers.append(phone_number)
        return numbers


    async def hydrate_users(
        self, 
        message_documents: List[Dict[str, Any]], 
        user_objects_cache: Dict[str, Any]
    ) -> None:
        """
        Hydrate user objects for message documents.

        Args:
            message_documents: List of message documents
            user_objects_cache: Cache to store user objects
        """
        from types import SimpleNamespace

        # Collect unique user IDs from messages
        user_ids = set()
        for message_document in message_documents:
            message_data = message_document.get("message_data", {})
            user_id = message_data.get("user", {}).get("user_id")
            if user_id and user_id not in user_objects_cache:
                user_ids.add(user_id)

        if not user_ids:
            return

        # Get repository instances
        repository_factory = await self._get_repository_factory()
        user_repository = await repository_factory.get_user_repository()

        # Fetch users from database
        users_data = await user_repository.find_users_by_ids(list(user_ids))

        # Convert to user objects and cache them
        for user_document in users_data:
            user_data = user_document.get("User", {})
            user_id = user_data.get("user_id")
            if user_id:
                user_object = SimpleNamespace(**user_data)
                user_objects_cache[user_id] = user_object
    
    async def invalidate_user_cache(self, user_id: str):
        print(self.cache)
        await self.cache.delete(user_id)

    async def get_user_activity_timestamp(self, user_id: str):
        """Get the user's last activity timestamp with caching."""
        cached_data = await self.cache.get(user_id)
        if cached_data is not None and isinstance(cached_data, dict):
            user = User(**cached_data)
            activity_timestamp = user.activity_timestamp
            if activity_timestamp is None:
                activity_timestamp = user.created_timestamp 
            return activity_timestamp, True

        repository_factory = await self._get_repository_factory()
        user_repository = await repository_factory.get_user_repository()
        user_obj = await user_repository.find_by_id(user_id)

        if user_obj is None:
            return None

        user = User(**user_obj["User"])
        activity_timestamp = user.activity_timestamp
        if activity_timestamp is None:
            activity_timestamp = user.created_timestamp

        await self.cache.set(user_id, user.model_dump(), ttl=3600)
        return activity_timestamp, False

    async def get_users(self, user_ids: List[str]) -> List[User]:
        """Fetch multiple users from the database using repository."""
        repository_factory = await self._get_repository_factory()
        user_repository = await repository_factory.get_user_repository()
        users_obj = await user_repository.find_all({"_id": {"$in": user_ids}})
        try:
            return [User(**user_obj["User"]) for user_obj in users_obj]
        except Exception:
            return []
    
    async def get_users_by_type(self, user_type: str) -> List[User]:
        """Fetch users by type using repository."""
        repository_factory = await self._get_repository_factory()
        user_repository = await repository_factory.get_user_repository()
        users_obj = await user_repository.find_users_by_type(user_type)
        return [User(**user_obj["User"]) for user_obj in users_obj]
    
    def user_activity_update_query(self, user: User, qa: Optional[Dict[str, Any]] = None, skip_timestamp: bool = False):
        """Generate update query for user activity."""
        update_data = {"$set": {}}
        if not skip_timestamp:
            latest_timestamp = str(int(datetime.now(timezone.utc).timestamp()))
            update_data = {"$set": {"User.activity_timestamp": latest_timestamp}}

        if qa is None:
            return ({"_id": user.user_id}, update_data)

        last_convs = user.last_conversations
        if len(last_convs) >= self._history_length:
            last_convs.pop(0)
        last_convs.append(qa)
        update_data["$set"]["User.last_conversations"] = last_convs

        return ({"_id": user.user_id}, update_data)
    
    def user_create_query(self, user: User):
        """Generate insert query for user."""
        return ({
            "_id": user.user_id,
            "User": user.model_dump(),
            "timestamp": str(int(datetime.now(timezone.utc).timestamp()))
        })
    
    def user_update_query(self, user: User):
        """Generate update query for user."""
        update_data = {"$set": {"User": user.model_dump()}}
        return ({"_id": user.user_id}, update_data)
    
    def aggregate_queries(
        self,
        results: List[Dict[str, Any]]
    ):
        new_user_queries = {
            constants.CREATE: [],
            constants.UPDATE: [],
        }
        for queries, _, err in results:
            if err is not None or queries is None:
                continue
            user_queries = queries.get(constants.USER_DB_QUERIES, {})
            if user_queries is not None and user_queries != {}:
                user_create_queries = user_queries.get(constants.CREATE,[])
                user_update_queries = user_queries.get(constants.UPDATE,[])
                new_user_queries[constants.CREATE].extend(user_create_queries)
                new_user_queries[constants.UPDATE].extend(user_update_queries)
        
        return new_user_queries
    
    async def execute_queries(self, queries: Dict[str, Any]):
        """Execute user database queries via repository (insert_many, bulk_update)."""
        if not queries:
            return

        repository_factory = await self._get_repository_factory()
        user_repository = await repository_factory.get_user_repository()
        if queries.get("create"):
            await user_repository.insert_many(queries["create"])
        if queries.get("update"):
            await user_repository.bulk_update(queries["update"])