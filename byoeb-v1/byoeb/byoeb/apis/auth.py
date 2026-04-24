import secrets
from collections import defaultdict, deque
from time import monotonic
import asyncio
import logging
from typing import Annotated, Optional
from uuid import UUID

from byoeb.services.auth.exceptions import MissingTokenError
from byoeb.services.auth.dependencies import is_public_base_url_secure
from fastapi import APIRouter, Body, Depends, Header, HTTPException, Request
from fastapi.responses import JSONResponse
from fastapi.security import OAuth2PasswordRequestForm
from pydantic import BaseModel, Field, StringConstraints

from byoeb_core.models.byoeb.user import PhoneNumberId
from byoeb.services.auth.dependencies import AccessTokenDep, AuthServiceDep, get_current_user, require_permissions
from byoeb.services.auth.models import AuthPermission, AuthTenant, AuthUser


auth_apis_router = APIRouter(prefix="/auth", tags=["Auth"])
REFRESH_TOKEN_MAX_AGE = 60 * 60 * 24 * 30
COOKIE_SECURE = is_public_base_url_secure()
audit_logger = logging.getLogger("audit")
_AUTH_RATE_LIMIT_WINDOW_SECONDS = 60.0
_AUTH_RATE_LIMIT_MAX_ATTEMPTS = 5
_auth_rate_limit_cache: dict[str, deque[float]] = defaultdict(deque)
_auth_rate_limit_lock = asyncio.Lock()

TUname = Annotated[str, StringConstraints(strip_whitespace=True, min_length=3, max_length=64)]
TPass = Annotated[str, StringConstraints(min_length=8, max_length=128)]
TRole = Annotated[str, StringConstraints(strip_whitespace=True, min_length=2, max_length=64)]


class RegisterUserRequest(BaseModel):
    username: TUname
    password: TPass
    tenant_id: UUID
    roles: list[TRole] = Field(min_length=1)
    phone_number_id: Optional[PhoneNumberId] = Field(default=None, description="Optional WhatsApp phone number ID")

class TenantSummary(BaseModel):
    tenant_id: UUID
    name: str
    roles: list[str] = Field(default_factory=list)

class UpdateUserRequest(BaseModel):
    username: TUname
    roles: Optional[list[TRole]] = None
    password: Optional[TPass] = None
    phone_number_id: Optional[PhoneNumberId] = Field(default=None, description="Optional WhatsApp phone number ID")


async def _enforce_auth_rate_limit(request: Request, route_name: str) -> None:
    client_host = request.client.host if request.client else "unknown"
    cache_key = f"{route_name}:{client_host}"
    cutoff = monotonic() - _AUTH_RATE_LIMIT_WINDOW_SECONDS
    async with _auth_rate_limit_lock:
        entries = _auth_rate_limit_cache[cache_key]
        while entries and entries[0] < cutoff:
            entries.popleft()
        if len(entries) >= _AUTH_RATE_LIMIT_MAX_ATTEMPTS:
            raise HTTPException(status_code=429, detail="Too many authentication attempts. Please retry later.")
        entries.append(monotonic())


@auth_apis_router.post("/token/issue")
async def issue_token(request: Request, auth_service: AuthServiceDep, tenant_id: Annotated[UUID | None, Header(alias="X-Tenant-ID")] = None, form_data: OAuth2PasswordRequestForm = Depends()) -> JSONResponse:
    await _enforce_auth_rate_limit(request, "issue")
    token_details = await auth_service.issue_token(form_data.username, form_data.password, tenant_id, form_data.scopes)
    csrf_token = secrets.token_urlsafe(32)
    response = JSONResponse(content={"status": "ok", "expires_in": token_details.expires_in})
    response.set_cookie("asha_auth_token", token_details.access_token, httponly=True, secure=COOKIE_SECURE, samesite="strict", max_age=token_details.expires_in)
    response.set_cookie("asha_refresh_token", token_details.refresh_token, httponly=True, secure=COOKIE_SECURE, samesite="strict", max_age=REFRESH_TOKEN_MAX_AGE)
    response.set_cookie("csrf_token", csrf_token, httponly=False, secure=COOKIE_SECURE, samesite="strict", max_age=token_details.expires_in)
    return response


@auth_apis_router.post("/token/refresh")
async def refresh_token(auth_service: AuthServiceDep, request: Request, client_id: Annotated[str | None, Header(alias="X-Client-ID")] = None) -> JSONResponse:
    await _enforce_auth_rate_limit(request, "refresh")
    refresh_token = request.cookies.get("asha_refresh_token")
    if not refresh_token:
        raise MissingTokenError()
    token_details = await auth_service.refresh_token(refresh_token, client_id=client_id)
    csrf_token = secrets.token_urlsafe(32)
    response = JSONResponse(content={"status": "ok", "expires_in": token_details.expires_in})
    response.set_cookie("asha_auth_token", token_details.access_token, httponly=True, secure=COOKIE_SECURE, samesite="strict", max_age=token_details.expires_in)
    response.set_cookie("asha_refresh_token", token_details.refresh_token, httponly=True, secure=COOKIE_SECURE, samesite="strict", max_age=REFRESH_TOKEN_MAX_AGE)
    response.set_cookie("csrf_token", csrf_token, httponly=False, secure=COOKIE_SECURE, samesite="strict", max_age=token_details.expires_in)
    return response


@auth_apis_router.post("/logout")
async def logout(auth_service: AuthServiceDep, request: Request) -> JSONResponse:
    refresh_token = request.cookies.get("asha_refresh_token")
    if refresh_token:
        await auth_service.revoke_refresh_token(refresh_token)
    response = JSONResponse(content={"status": "logged_out"})
    response.delete_cookie("asha_auth_token")
    response.delete_cookie("asha_refresh_token")
    response.delete_cookie("csrf_token")
    return response


@auth_apis_router.post("/users")
async def register_user(auth_service: AuthServiceDep, payload: RegisterUserRequest, user: AuthUser = Depends(require_permissions(AuthPermission.AUTH_USERS_WRITE))) -> AuthUser:
    registered_user = await auth_service.register_user(user.tenant_id, payload)
    audit_logger.info(
        "auth.user.registered actor=%s actor_tenant=%s target_user=%s target_tenant=%s roles=%s",
        user.username,
        user.tenant_id,
        payload.username,
        payload.tenant_id,
        payload.roles,
    )
    return registered_user


@auth_apis_router.put("/users")
async def update_user(auth_service: AuthServiceDep, payload: UpdateUserRequest, user: AuthUser = Depends(require_permissions(AuthPermission.AUTH_USERS_WRITE))) -> AuthUser:
    updated_user = await auth_service.update_user(tenant_id=user.tenant_id, username=payload.username, roles=payload.roles, password=payload.password, phone_number_id=payload.phone_number_id)
    audit_logger.info(
        "auth.user.updated actor=%s actor_tenant=%s target_user=%s roles_updated=%s password_updated=%s phone_number_updated=%s",
        user.username,
        user.tenant_id,
        payload.username,
        payload.roles is not None,
        payload.password is not None,
        payload.phone_number_id is not None,
    )
    return updated_user


@auth_apis_router.post("/tenants", dependencies=[Depends(require_permissions(AuthPermission.AUTH_TENANTS_WRITE))])
async def create_tenant(auth_service: AuthServiceDep, name: Annotated[str, StringConstraints(strip_whitespace=True, min_length=2, max_length=128), Body(..., embed=True)]) -> AuthTenant:
    tenant = await auth_service.create_tenant(name)
    audit_logger.info("auth.tenant.created tenant_id=%s tenant_name=%s", tenant.id, tenant.name)
    return tenant


@auth_apis_router.put("/tenants")
async def update_tenant_roles(auth_service: AuthServiceDep, roles: dict[str, list[AuthPermission]] = Body(..., min_length=1), user: AuthUser = Depends(require_permissions(AuthPermission.AUTH_TENANTS_WRITE))) -> AuthTenant:
    tenant = await auth_service.update_tenant_roles(user.tenant_id, roles)
    audit_logger.info("auth.tenant.roles.updated actor=%s tenant_id=%s role_count=%s", user.username, user.tenant_id, len(roles))
    return tenant


@auth_apis_router.get("/tenants/roles")
async def list_tenant_roles(auth_service: AuthServiceDep, user: AuthUser = Depends(get_current_user)) -> dict[str, list[str]]:
    return await auth_service.list_tenant_roles(user.tenant_id)


@auth_apis_router.post("/tenants/roles")
async def create_tenant_role(auth_service: AuthServiceDep, user: AuthUser = Depends(require_permissions(AuthPermission.AUTH_TENANTS_WRITE)), role: TRole = Body(...), permissions: list[AuthPermission] = Body(..., min_length=1)) -> dict[str, list[str]]:
    roles = await auth_service.add_tenant_role(user.tenant_id, role, permissions)
    audit_logger.info("auth.tenant.role.created actor=%s tenant_id=%s role=%s permissions=%s", user.username, user.tenant_id, role, [permission.value for permission in permissions])
    return roles


@auth_apis_router.put("/tenants/roles/{role}")
async def update_tenant_role_permissions(auth_service: AuthServiceDep, role: TRole, permissions: list[AuthPermission] = Body(..., min_length=1), user: AuthUser = Depends(require_permissions(AuthPermission.AUTH_TENANTS_WRITE))) -> dict[str, list[str]]:
    roles = await auth_service.set_tenant_role_permissions(user.tenant_id, role, permissions)
    audit_logger.info("auth.tenant.role.permissions.updated actor=%s tenant_id=%s role=%s permissions=%s", user.username, user.tenant_id, role, [permission.value for permission in permissions])
    return roles


@auth_apis_router.delete("/tenants/roles/{role}")
async def delete_tenant_role_permissions(auth_service: AuthServiceDep, role: TRole, user: AuthUser = Depends(require_permissions(AuthPermission.AUTH_TENANTS_WRITE))) -> dict[str, list[str]]:
    roles = await auth_service.delete_tenant_role(user.tenant_id, role)
    audit_logger.info("auth.tenant.role.deleted actor=%s tenant_id=%s role=%s", user.username, user.tenant_id, role)
    return roles


@auth_apis_router.put("/users/roles")
async def set_user_roles(auth_service: AuthServiceDep, user: AuthUser = Depends(require_permissions(AuthPermission.AUTH_USERS_WRITE)), username: TUname = Body(...), roles: list[TRole] = Body(..., min_length=1)) -> AuthUser:
    updated_user = await auth_service.set_user_roles(user.tenant_id, username, roles)
    audit_logger.info("auth.user.roles.set actor=%s tenant_id=%s target_user=%s roles=%s", user.username, user.tenant_id, username, roles)
    return updated_user


@auth_apis_router.post("/users/roles")
async def add_user_role_api(auth_service: AuthServiceDep, user: AuthUser = Depends(require_permissions(AuthPermission.AUTH_USERS_WRITE)), username: TUname = Body(...), role: TRole = Body(...)) -> AuthUser:
    updated_user = await auth_service.add_user_role(user.tenant_id, username, role)
    audit_logger.info("auth.user.role.added actor=%s tenant_id=%s target_user=%s role=%s", user.username, user.tenant_id, username, role)
    return updated_user


@auth_apis_router.delete("/users/roles")
async def remove_user_role_api(auth_service: AuthServiceDep, user: AuthUser = Depends(require_permissions(AuthPermission.AUTH_USERS_WRITE)), username: TUname = Body(...), role: TRole = Body(...)) -> AuthUser:
    updated_user = await auth_service.remove_user_role(user.tenant_id, username, role)
    audit_logger.info("auth.user.role.removed actor=%s tenant_id=%s target_user=%s role=%s", user.username, user.tenant_id, username, role)
    return updated_user


@auth_apis_router.post("/users/tenants")
async def add_user_tenant_api(auth_service: AuthServiceDep, user: AuthUser = Depends(require_permissions(AuthPermission.AUTH_USERS_WRITE)), username: TUname = Body(...), roles: list[TRole] = Body(..., min_length=1)) -> AuthUser:
    updated_user = await auth_service.add_user_tenant(user.tenant_id, username, roles)
    audit_logger.info("auth.user.tenant.added actor=%s tenant_id=%s target_user=%s roles=%s", user.username, user.tenant_id, username, roles)
    return updated_user


@auth_apis_router.delete("/users/tenants")
async def remove_user_tenant_api(auth_service: AuthServiceDep, user: AuthUser = Depends(require_permissions(AuthPermission.AUTH_USERS_WRITE)), username: TUname = Body(...)) -> dict[str, str]:
    await auth_service.remove_user_tenant(user.tenant_id, username)
    audit_logger.info("auth.user.tenant.removed actor=%s tenant_id=%s target_user=%s", user.username, user.tenant_id, username)
    return {"status": "removed"}


@auth_apis_router.get("/tenants")
async def list_my_tenants(auth_service: AuthServiceDep, token: AccessTokenDep) -> list[TenantSummary]:
    if token is None:
        raise MissingTokenError()
    results = await auth_service.list_my_tenants(token)
    return [TenantSummary(**entry) for entry in results]


@auth_apis_router.get("/me")
async def me(user: AuthUser = Depends(get_current_user)) -> AuthUser:
    return user
