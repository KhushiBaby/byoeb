from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional, Dict, Any
from uuid import UUID

from byoeb.repositories.base_repository import BaseRepository
from mcp.shared.auth import OAuthClientInformationFull


class AuthRepository(BaseRepository, ABC):

    @abstractmethod
    async def find_user_by_username(self, username: str) -> Optional[Dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
    async def find_user_by_refresh_token(self, refresh_token: str, client_id: str | None = None) -> Optional[Dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
    async def find_oauth_client(self, client_id: str) -> Optional[OAuthClientInformationFull]:
        raise NotImplementedError

    @abstractmethod
    async def insert_oauth_client(self, client_info: OAuthClientInformationFull) -> None:
        raise NotImplementedError

    @abstractmethod
    async def store_auth_code(
        self,
        code: str,
        client_id: str,
        redirect_uri: str | None,
        scope: str | None,
        code_challenge: str | None,
        code_challenge_method: str | None,
        username: str,
        tenant_id: UUID,
        expires_at: datetime,
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    async def find_auth_code(self, code: str) -> Optional[Dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
    async def delete_auth_code(self, code: str) -> bool:
        raise NotImplementedError

    @abstractmethod
    async def update_user_by_username(self, username: str, updates: Dict[str, Any]) -> bool:
        raise NotImplementedError

    @abstractmethod
    async def update_user_refresh_token(self, username: str, refresh_token: str, client_id: str | None, scope: str | None) -> bool:
        raise NotImplementedError

    @abstractmethod
    async def clear_user_refresh_token(self, refresh_token: str) -> bool:
        raise NotImplementedError

    @abstractmethod
    async def rotate_refresh_token(
        self,
        username: str,
        current_refresh_token: str,
        new_refresh_token: str,
        client_id: str | None,
        scope: str | None,
    ) -> bool:
        raise NotImplementedError

    @abstractmethod
    async def update_user_roles_for_tenant(self, username: str, tenant_id: UUID, roles: list[str]) -> bool:
        raise NotImplementedError

    @abstractmethod
    async def add_user_tenant(self, username: str, tenant_id: UUID, roles: list[str]) -> bool:
        raise NotImplementedError

    @abstractmethod
    async def remove_user_tenant(self, username: str, tenant_id: UUID) -> bool:
        raise NotImplementedError

    @abstractmethod
    async def find_tenant_by_id(self, tenant_id: UUID) -> Optional[Dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
    async def update_tenant_roles(self, tenant_id: UUID, roles: Dict[str, Any]) -> bool:
        raise NotImplementedError

    @abstractmethod
    async def insert_tenant(self, tenant_doc: Dict[str, Any], roles: Dict[str, Any]) -> str:
        raise NotImplementedError

    @abstractmethod
    async def find_tenant_roles_by_id(self, tenant_id: UUID) -> Optional[Dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
    async def find_integration_by_identifier(self, platform: str, identifier: str) -> Optional[Dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
    async def find_integration_by_token(self, platform: str, token: str) -> Optional[Dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
    async def find_integrations_by_ids(self, integration_ids: list[str]) -> list[Dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
    async def find_integrations_by_tenants(self, platform: str, tenant_ids: list[UUID]) -> list[Dict[str, Any]]:
        raise NotImplementedError
