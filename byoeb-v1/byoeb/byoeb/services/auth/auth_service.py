from __future__ import annotations

import logging
import secrets
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Optional
from uuid import UUID

from pydantic import BaseModel, Field
from mcp.shared.auth import OAuthClientInformationFull

from byoeb.chat_app.configuration.config import app_config
from byoeb_core.models.byoeb.user import PhoneNumberId
from byoeb.repositories.auth_repository import AuthRepository
from byoeb.repositories.repository_factory import get_repository_factory
from byoeb.services.auth.exceptions import (
    InvalidCredentialsError,
    InvalidRoleAssignmentError,
    InvalidScopeError,
    InvalidTokenError,
    MissingPhoneNumberIdError,
    RoleAlreadyExistsError,
    RoleNotFoundError,
    TenantAccessForbiddenError,
    TenantAlreadyExistsError,
    TenantNotFoundError,
    UserAlreadyExistsError,
    UserNotFoundError,
    UserTenantConflictError,
)
from byoeb.services.auth.models import AuthPermission, AuthTenant, AuthUser, AshaTenantIntegration
from byoeb.services.auth.security import AuthTokenService, TOKEN_SERVICE, TokenClaims, PASSWORD_CTX

logger = logging.getLogger(__name__)


class TokenDetails(BaseModel):
    access_token: str = Field(..., description="Bearer token for API access")
    refresh_token: str = Field(..., description="Refresh token for obtaining new access tokens")
    token_type: str = Field(default="bearer")
    expires_in: int = Field(..., description="Token TTL in seconds")


class AuthService:
    def __init__(self, repo: AuthRepository, token_service: AuthTokenService) -> None:
        self._repo = repo
        self._token_service = token_service

    async def issue_token(self, username: str, password: str, tenant_id: UUID | None, scopes: Iterable[str] | None = None) -> TokenDetails:
        resolved_tenant = tenant_id or await self._resolve_default_tenant(username)
        user = await self.authenticate_user(username, password, resolved_tenant)
        scope = await self.validate_requested_scopes(user, scopes)
        return await self.issue_token_for_user(user.username, user.tenant_id, scope=scope)

    async def authenticate_user(self, username: str, password: str, tenant_id: UUID) -> AuthUser:
        user_doc = await self._repo.find_user_by_username(username)
        if not user_doc or not PASSWORD_CTX.verify(password, user_doc.get("password_hash")):
            raise InvalidCredentialsError()
        try:
            return await self._build_auth_user(user_doc, tenant_id)
        except (TenantAccessForbiddenError, TenantNotFoundError, UserNotFoundError) as exc:
            raise InvalidCredentialsError() from exc

    async def get_user_by_username(self, username: str, tenant_id: UUID) -> AuthUser:
        user_doc = await self._repo.find_user_by_username(username)
        if not user_doc:
            raise UserNotFoundError()
        return await self._build_auth_user(user_doc, tenant_id)

    async def resolve_user_from_token(self, token: str) -> tuple[AuthUser, TokenClaims]:
        claims = self._token_service.parse_access_token(token)
        return await self.get_user_by_username(claims.username, claims.tenant_id), claims

    async def refresh_token(self, refresh_token: str, client_id: str | None = None, scope: str | None = None) -> TokenDetails:
        user_doc = await self.find_user_doc_by_refresh_token(refresh_token, client_id=client_id)
        if not user_doc:
            raise InvalidTokenError("Invalid refresh token.")
        username = user_doc.get("username")
        if not username:
            raise InvalidTokenError("Invalid refresh token.")
        tenant_id = next((entry.get("tenant_id") for entry in user_doc.get("tenants", []) if entry.get("tenant_id")), None)
        if not tenant_id:
            raise InvalidTokenError("Invalid refresh token.")
        stored_scope = user_doc.get("refresh_scopes")
        if scope and not stored_scope:
            raise InvalidTokenError("Invalid refresh token.")
        if scope and stored_scope:
            requested_scopes = {entry for entry in scope.split() if entry}
            granted_scopes = {entry for entry in str(stored_scope).split() if entry}
            if not requested_scopes.issubset(granted_scopes):
                raise InvalidTokenError("Invalid refresh token.")
        stored_client_id = user_doc.get("refresh_client_id")
        if client_id is not None and stored_client_id is not None and client_id != stored_client_id:
            logger.warning(
                "refresh_token: client_id mismatch for user '%s' — provided '%s', stored '%s'",
                username,
                client_id,
                stored_client_id,
            )
            raise InvalidTokenError("Invalid refresh token.")
        resolved_client_id = client_id if client_id is not None else stored_client_id
        return await self.issue_token_for_refresh(
            username,
            UUID(str(tenant_id)),
            refresh_token,
            client_id=resolved_client_id,
            scope=stored_scope,
        )

    async def find_user_doc_by_refresh_token(self, refresh_token: str, client_id: str | None = None) -> Optional[Dict[str, Any]]:
        user_doc = await self._repo.find_user_by_refresh_token(refresh_token, client_id)
        if not user_doc:
            return None
        self._validate_refresh_token_expiry(user_doc)
        return user_doc

    async def update_refresh_token(self, username: str, refresh_token: str, client_id: str | None, scope: str | None) -> bool:
        return await self._repo.update_user_refresh_token(username, refresh_token, client_id, scope)

    async def issue_token_for_user(self, username: str, tenant_id: UUID, *, client_id: str | None = None, scope: str | None = None) -> TokenDetails:
        user = await self.get_user_by_username(username, tenant_id)
        permissions = await self.get_permissions_for_roles(tenant_id, user.roles)
        try:
            access_token, ttl_seconds = self._token_service.create_access_token(username, tenant_id, permissions=permissions)
        except Exception:
            logger.exception("Failed to generate access token for user '%s' in tenant '%s'.", username, tenant_id)
            raise
        refresh_token = secrets.token_urlsafe(48)
        await self._repo.update_user_refresh_token(username, refresh_token, client_id=client_id, scope=scope)
        return TokenDetails(access_token=access_token, refresh_token=refresh_token, expires_in=ttl_seconds)

    async def issue_token_for_refresh(
        self,
        username: str,
        tenant_id: UUID,
        current_refresh_token: str,
        *,
        client_id: str | None = None,
        scope: str | None = None,
    ) -> TokenDetails:
        user = await self.get_user_by_username(username, tenant_id)
        permissions = await self.get_permissions_for_roles(tenant_id, user.roles)
        try:
            access_token, ttl_seconds = self._token_service.create_access_token(username, tenant_id, permissions=permissions)
        except Exception:
            logger.exception("Failed to generate rotated access token for user '%s' in tenant '%s'.", username, tenant_id)
            raise

        new_refresh_token = secrets.token_urlsafe(48)
        rotated = await self._repo.rotate_refresh_token(
            username,
            current_refresh_token,
            new_refresh_token,
            client_id=client_id,
            scope=scope,
        )
        if not rotated:
            raise InvalidTokenError("Refresh token reuse detected.")
        return TokenDetails(access_token=access_token, refresh_token=new_refresh_token, expires_in=ttl_seconds)

    async def find_oauth_client(self, client_id: str) -> OAuthClientInformationFull | None:
        return await self._repo.find_oauth_client(client_id)

    async def register_oauth_client(self, client_info: OAuthClientInformationFull) -> None:
        await self._repo.insert_oauth_client(client_info)

    async def store_auth_code(self, code: str, client_id: str, redirect_uri: str | None, scope: str | None, code_challenge: str | None, code_challenge_method: str | None, username: str, tenant_id: UUID, expires_at: datetime) -> None:
        await self._repo.store_auth_code(code, client_id, redirect_uri, scope, code_challenge, code_challenge_method, username, tenant_id, expires_at)

    async def find_auth_code(self, code: str) -> Optional[Dict[str, Any]]:
        return await self._repo.find_auth_code(code)

    async def delete_auth_code(self, code: str) -> bool:
        return await self._repo.delete_auth_code(code)

    async def revoke_refresh_token(self, refresh_token: str) -> None:
        await self._repo.clear_user_refresh_token(refresh_token)

    def _validate_refresh_token_expiry(self, user_doc: dict[str, Any]) -> None:
        expires_at = user_doc.get("refresh_token_expires_at")
        if expires_at is None:
            return
        if isinstance(expires_at, datetime):
            if expires_at.tzinfo is None:
                expires_at = expires_at.replace(tzinfo=timezone.utc)
            if datetime.now(timezone.utc) > expires_at:
                logger.warning("refresh_token: token expired at %s (now %s)", expires_at, datetime.now(timezone.utc))
                raise InvalidTokenError("Refresh token expired.")

    async def register_user(self, tenant_id: UUID, payload) -> AuthUser:
        if payload.tenant_id != tenant_id:
            raise TenantAccessForbiddenError()
        if not await self._repo.find_tenant_by_id(payload.tenant_id):
            raise TenantNotFoundError()
        if await self._repo.find_user_by_username(payload.username):
            raise UserAlreadyExistsError()
        roles = self._normalize_roles(payload.roles)
        await self._ensure_roles_defined(payload.tenant_id, roles)
        password_hash = PASSWORD_CTX.hash(payload.password)
        user_id = uuid.uuid4()
        await self._repo.insert_one({
            "_id": user_id,
            "username": payload.username,
            "tenants": [{"tenant_id": payload.tenant_id, "roles": roles}],
            "phone_number_id": payload.phone_number_id,
            "password_hash": password_hash,
        })
        return AuthUser(
            id=user_id,
            username=payload.username,
            tenant_id=payload.tenant_id,
            roles=roles,
            phone_number_id=payload.phone_number_id,
        )

    async def update_user(
        self,
        tenant_id: UUID,
        username: str,
        roles: list[str] | None,
        password: str | None,
        phone_number_id: PhoneNumberId | None,
    ) -> AuthUser:
        user_doc = await self._repo.find_user_by_username(username)
        if not user_doc:
            raise UserNotFoundError()
        tenant_entry = self._find_tenant_entry(user_doc, tenant_id)
        updates: dict[str, object] = {}
        if roles is not None:
            cleaned_roles = self._normalize_roles(roles)
            await self._ensure_roles_defined(tenant_id, cleaned_roles)
            await self._repo.update_user_roles_for_tenant(username, tenant_id, cleaned_roles)
            tenant_entry["roles"] = cleaned_roles
        if password is not None:
            password_hash = PASSWORD_CTX.hash(password)
            updates["password_hash"] = password_hash
        if phone_number_id is not None:
            updates["phone_number_id"] = phone_number_id
        if updates:
            await self._repo.update_user_by_username(username, updates)
        return await self._build_auth_user({**user_doc, **updates}, tenant_id)

    async def create_tenant(self, name: str) -> AuthTenant:
        tenant_id = uuid.uuid4()
        if await self._repo.find_tenant_by_id(tenant_id):
            raise TenantAlreadyExistsError()
        roles = {role: list(perms) for role, perms in app_config.get("default_tenant_roles", {}).items()}
        await self._repo.insert_tenant({"_id": tenant_id, "name": name}, roles)
        return AuthTenant(id=tenant_id, name=name, roles=self._coerce_role_permissions(roles))

    async def update_tenant_roles(self, tenant_id: UUID, roles: dict[str, list[AuthPermission]]) -> AuthTenant:
        tenant = await self._repo.find_tenant_by_id(tenant_id)
        if not tenant:
            raise TenantNotFoundError()
        serialized = {role: [perm.value for perm in perms] for role, perms in roles.items()}
        if not await self._repo.update_tenant_roles(tenant_id, serialized):
            raise TenantNotFoundError()
        return AuthTenant(id=tenant_id, name=tenant.get("name", ""), roles=roles)

    async def list_tenant_roles(self, tenant_id: UUID) -> dict[str, list[str]]:
        await self._ensure_tenant_exists(tenant_id)
        roles_doc = await self._repo.find_tenant_roles_by_id(tenant_id) or {}
        return roles_doc.get("roles") or {}

    async def add_tenant_role(
        self,
        tenant_id: UUID,
        role: str,
        permissions: list[AuthPermission],
    ) -> dict[str, list[str]]:
        roles_map = await self.list_tenant_roles(tenant_id)
        if role in roles_map:
            raise RoleAlreadyExistsError()
        roles_map[role] = [perm.value for perm in permissions]
        await self._repo.update_tenant_roles(tenant_id, roles_map)
        return roles_map

    async def set_tenant_role_permissions(
        self,
        tenant_id: UUID,
        role: str,
        permissions: list[AuthPermission],
    ) -> dict[str, list[str]]:
        roles_map = await self.list_tenant_roles(tenant_id)
        if role not in roles_map:
            raise RoleNotFoundError()
        roles_map[role] = [perm.value for perm in permissions]
        await self._repo.update_tenant_roles(tenant_id, roles_map)
        return roles_map

    async def delete_tenant_role(self, tenant_id: UUID, role: str) -> dict[str, list[str]]:
        roles_map = await self.list_tenant_roles(tenant_id)
        if role not in roles_map:
            raise RoleNotFoundError()
        roles_map.pop(role, None)
        await self._repo.update_tenant_roles(tenant_id, roles_map)
        return roles_map

    async def set_user_roles(self, tenant_id: UUID, username: str, roles: list[str]) -> AuthUser:
        return await self.update_user(tenant_id, username, roles, None, None)

    async def add_user_role(self, tenant_id: UUID, username: str, role: str) -> AuthUser:
        user_doc = await self._repo.find_user_by_username(username)
        if not user_doc:
            raise UserNotFoundError()
        tenant_entry = self._find_tenant_entry(user_doc, tenant_id)
        role = self._normalize_roles([role])[0]
        await self._ensure_roles_defined(tenant_id, [role])
        roles = list(tenant_entry.get("roles", []))
        if role not in roles:
            roles.append(role)
            await self._repo.update_user_roles_for_tenant(username, tenant_id, roles)
            tenant_entry["roles"] = roles
        return await self._build_auth_user(user_doc, tenant_id)

    async def remove_user_role(self, tenant_id: UUID, username: str, role: str) -> AuthUser:
        user_doc = await self._repo.find_user_by_username(username)
        if not user_doc:
            raise UserNotFoundError()
        tenant_entry = self._find_tenant_entry(user_doc, tenant_id)
        roles = [r for r in tenant_entry.get("roles", []) if r != role]
        await self._repo.update_user_roles_for_tenant(username, tenant_id, roles)
        tenant_entry["roles"] = roles
        return await self._build_auth_user(user_doc, tenant_id)

    async def add_user_tenant(self, tenant_id: UUID, username: str, roles: list[str]) -> AuthUser:
        user_doc = await self._repo.find_user_by_username(username)
        if not user_doc:
            raise UserNotFoundError()
        await self._ensure_tenant_exists(tenant_id)
        if any(entry.get("tenant_id") == tenant_id for entry in user_doc.get("tenants", [])):
            raise UserTenantConflictError()
        roles = self._normalize_roles(roles)
        await self._ensure_roles_defined(tenant_id, roles)
        if not await self._repo.add_user_tenant(username, tenant_id, roles):
            raise UserNotFoundError()
        user_doc.setdefault("tenants", []).append({"tenant_id": tenant_id, "roles": roles})
        return await self._build_auth_user(user_doc, tenant_id)

    async def remove_user_tenant(self, tenant_id: UUID, username: str) -> None:
        user_doc = await self._repo.find_user_by_username(username)
        if not user_doc:
            raise UserNotFoundError()
        if not any(entry.get("tenant_id") == tenant_id for entry in user_doc.get("tenants", [])):
            raise UserNotFoundError("User or tenant not found.")
        await self._repo.remove_user_tenant(username, tenant_id)

    async def list_my_tenants(self, token: str) -> list[dict]:
        claims = self._token_service.parse_access_token(token)
        user_doc = await self._repo.find_user_by_username(claims.username)
        if not user_doc:
            raise UserNotFoundError()
        results: list[dict] = []
        for entry in user_doc.get("tenants", []):
            tenant_id = entry.get("tenant_id")
            if not tenant_id:
                continue
            tenant_uuid = UUID(str(tenant_id))
            tenant_doc = await self._repo.find_tenant_by_id(tenant_uuid)
            if not tenant_doc:
                continue
            results.append({
                "tenant_id": tenant_uuid,
                "name": tenant_doc.get("name", ""),
                "roles": list(entry.get("roles", [])),
            })
        return results

    async def get_permissions_for_roles(self, tenant_id: UUID, roles: Iterable[str]) -> list[str]:
        roles_doc = await self._repo.find_tenant_roles_by_id(tenant_id)
        if not roles_doc:
            raise TenantNotFoundError()
        role_map = roles_doc.get("roles") or {}
        permissions = {perm for role in roles for perm in role_map.get(role, [])}
        return list(permissions)

    async def validate_requested_scopes(self, user: AuthUser, scopes: Iterable[str] | None) -> str | None:
        if not scopes:
            return None
        requested = {scope for scope in scopes if scope}
        if not requested:
            return None
        valid_scopes = {perm.value for perm in AuthPermission}
        if requested - valid_scopes:
            raise InvalidScopeError()
        granted = set(await self.get_permissions_for_roles(user.tenant_id, user.roles))
        if requested - granted:
            raise InvalidScopeError("One or more requested scopes are not granted for this user.")
        if AuthPermission.MCP_ACCESS.value in requested and user.phone_number_id is None:
            raise MissingPhoneNumberIdError()
        return " ".join(sorted(requested))

    async def _resolve_default_tenant(self, username: str) -> UUID:
        user_doc = await self._repo.find_user_by_username(username)
        if not user_doc:
            raise InvalidCredentialsError()
        for entry in user_doc.get("tenants", []):
            tenant_id = entry.get("tenant_id")
            if tenant_id:
                return UUID(str(tenant_id))
        raise InvalidCredentialsError()

    async def _build_auth_user(self, user_doc: dict, tenant_id: UUID) -> AuthUser:
        await self._ensure_tenant_exists(tenant_id)
        user_id = user_doc.get("_id")
        if not user_id:
            raise UserNotFoundError()
        tenant_entry = self._find_tenant_entry(user_doc, tenant_id)
        roles = list(tenant_entry.get("roles", []))
        return AuthUser(
            id=user_id,
            username=user_doc.get("username", ""),
            tenant_id=tenant_id,
            roles=roles,
            phone_number_id=user_doc.get("phone_number_id"),
        )

    async def _ensure_tenant_exists(self, tenant_id: UUID) -> None:
        if not await self._repo.find_tenant_by_id(tenant_id):
            raise TenantNotFoundError()

    async def _ensure_roles_defined(self, tenant_id: UUID, roles: Iterable[str]) -> None:
        roles_doc = await self._repo.find_tenant_roles_by_id(tenant_id)
        if not roles_doc:
            raise TenantNotFoundError()
        tenant_roles = set((roles_doc.get("roles") or {}).keys())
        if not set(roles).issubset(tenant_roles):
            raise InvalidRoleAssignmentError()

    def _find_tenant_entry(self, user_doc: dict, tenant_id: UUID) -> dict:
        tenant_entry = next(
            (tenant for tenant in user_doc.get("tenants", []) if tenant.get("tenant_id") == tenant_id),
            None,
        )
        if not tenant_entry:
            raise TenantAccessForbiddenError()
        return tenant_entry

    def _coerce_role_permissions(self, roles: dict[str, list[str]]) -> dict[str, list[AuthPermission]]:
        result: dict[str, list[AuthPermission]] = {}
        for role, perms in roles.items():
            role_perms: list[AuthPermission] = []
            for perm in perms:
                try:
                    role_perms.append(AuthPermission(perm))
                except ValueError:
                    continue
            result[role] = role_perms
        return result

    def _normalize_roles(self, roles: Iterable[str]) -> list[str]:
        cleaned_roles = [role.strip() for role in roles]
        if any(not role for role in cleaned_roles):
            raise InvalidRoleAssignmentError("Role names must not be empty.")
        return cleaned_roles

    async def resolve_integration(self, platform: str, identifier: str) -> Optional[AshaTenantIntegration]:
        doc = await self._repo.find_integration_by_identifier(platform, identifier)
        if not doc:
            return None
        return AshaTenantIntegration(**doc)

    async def verify_integration_token(self, platform: str, token: str) -> bool:
        doc = await self._repo.find_integration_by_token(platform, token)
        return doc is not None

    async def fetch_integrations(self, integration_ids: list[str]) -> list[AshaTenantIntegration]:
        docs = await self._repo.find_integrations_by_ids(integration_ids)
        return [AshaTenantIntegration(**doc) for doc in docs]

    async def fetch_integrations_by_tenants(self, platform: str, tenant_ids: list[UUID]) -> list[AshaTenantIntegration]:
        docs = await self._repo.find_integrations_by_tenants(platform, tenant_ids)
        return [AshaTenantIntegration(**doc) for doc in docs]


async def get_auth_service() -> AuthService:
    repo_factory = await get_repository_factory()
    auth_repo = await repo_factory.get_auth_repository()
    return AuthService(auth_repo, TOKEN_SERVICE)
