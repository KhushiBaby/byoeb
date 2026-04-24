"""
Factory for creating repository instances.
"""
from typing import Optional
from byoeb.repositories.dyk_repository import DykRepository
from byoeb.repositories.auth_repository import AuthRepository
from byoeb.repositories.mongodb_dyk_repository import MongoDykRepository
from byoeb.repositories.mongodb_auth_repository import MongoAuthRepository
from byoeb.repositories.message_repository import MessageRepository
from byoeb.repositories.user_repository import UserRepository
from byoeb.repositories.mongodb_message_repository import MongoMessageRepository
from byoeb.repositories.mongodb_user_repository import MongoUserRepository
from byoeb.factory import MongoDBFactory
from byoeb.chat_app.configuration.config import app_config

# Global repository factory instance
_repository_factory: Optional['RepositoryFactory'] = None


class RepositoryFactory:
    """Factory for creating repository instances."""

    def __init__(self, mongo_factory: MongoDBFactory):
        self._mongo_factory = mongo_factory
        self._dyk_repository: Optional[DykRepository] = None
        self._message_repository: Optional[MessageRepository] = None
        self._user_repository: Optional[UserRepository] = None
        self._auth_repository: Optional[AuthRepository] = None

    async def get_dyk_repository(self) -> DykRepository:
        """Get or create DYK repository instance."""
        if self._dyk_repository is None:
            mongo_db = await self._mongo_factory.get(app_config["app"]["db_provider"])
            storage_collection = mongo_db.get_collection(app_config["databases"]["mongo_db"]["dyk_storage_collection"])
            queue_collection = mongo_db.get_collection(app_config["databases"]["mongo_db"]["dyk_queue_collection"])
            self._dyk_repository = MongoDykRepository(storage_collection, queue_collection)
        return self._dyk_repository

    async def get_message_repository(self) -> MessageRepository:
        """Get or create message repository instance."""
        if self._message_repository is None:
            mongo_db = await self._mongo_factory.get(app_config["app"]["db_provider"])
            message_collection = mongo_db.get_collection(app_config["databases"]["mongo_db"]["message_collection"])
            self._message_repository = MongoMessageRepository(message_collection)
        return self._message_repository

    async def get_user_repository(self) -> UserRepository:
        """Get or create user repository instance."""
        if self._user_repository is None:
            mongo_db = await self._mongo_factory.get(app_config["app"]["db_provider"])
            user_collection = mongo_db.get_collection(app_config["databases"]["mongo_db"]["user_collection"])
            self._user_repository = MongoUserRepository(user_collection)
        return self._user_repository

    async def get_auth_repository(self) -> AuthRepository:
        """Get or create auth repository instance."""
        if self._auth_repository is None:
            mongo_db = await self._mongo_factory.get(app_config["app"]["db_provider"])
            config = app_config["databases"]["mongo_db"]
            auth_collection = mongo_db.get_collection(config["auth_user_collection"])
            tenant_collection = mongo_db.get_collection(config["auth_tenant_collection"])
            role_collection = mongo_db.get_collection(config["auth_tenant_roles_collection"])
            oauth_client_collection = mongo_db.get_collection(config["auth_oauth_client_collection"])
            oauth_code_collection = mongo_db.get_collection(config["auth_oauth_code_collection"])
            integration_collection = mongo_db.get_collection(config["auth_tenant_integrations_collection"])
            self._auth_repository = MongoAuthRepository(auth_collection, tenant_collection, role_collection, oauth_client_collection, oauth_code_collection, integration_collection)
        return self._auth_repository

    async def reset_repositories(self):
        """Reset repository instances (useful for testing)."""
        self._dyk_repository = None
        self._message_repository = None
        self._user_repository = None
        self._auth_repository = None

async def get_repository_factory() -> RepositoryFactory:
    """Get or create repository factory instance."""
    global _repository_factory
    if _repository_factory is None:
        mongo_factory = MongoDBFactory(config=app_config, scope="singleton")
        _repository_factory = RepositoryFactory(mongo_factory)
    return _repository_factory
