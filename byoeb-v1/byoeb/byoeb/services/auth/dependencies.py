import hashlib
import hmac
import json
from typing import Annotated
from uuid import UUID

from fastapi import Depends, Header, HTTPException, Request, status
from fastapi.security import APIKeyCookie, HTTPAuthorizationCredentials, HTTPBearer
from fastmcp.server.dependencies import get_access_token
from pydantic import StringConstraints

from urllib.parse import urlparse

from byoeb.chat_app.configuration import config as env_config
from byoeb.services.auth.auth_service import AuthService, get_auth_service
from byoeb.services.auth.exceptions import (
    InvalidTenantClaimError,
    MissingTokenError,
    PermissionDeniedError,
)
from byoeb.services.auth.models import AuthPermission, AuthUser, AshaTenantIntegration
from byoeb.services.auth.security import TOKEN_SERVICE


cookie_scheme = APIKeyCookie(name="asha_auth_token", auto_error=False)
bearer_scheme = HTTPBearer(auto_error=False)
def get_public_base_url() -> str: return (env_config.env_public_base_url or "http://127.0.0.1:8000").rstrip("/")
def is_public_base_url_secure() -> bool: return urlparse(get_public_base_url()).scheme == "https"

def get_access_token_cookie(token: Annotated[str | None, Depends(cookie_scheme)] = None, bearer: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer_scheme)] = None) -> str | None:
    if token:
        return token
    if bearer and bearer.scheme.lower() == "bearer":
        return bearer.credentials
    return None


AuthServiceDep = Annotated[AuthService, Depends(get_auth_service)]
AccessTokenDep = Annotated[str | None, Depends(get_access_token_cookie)]


async def get_current_user(auth_service: AuthServiceDep, token: AccessTokenDep) -> AuthUser:
    if token is None:
        raise MissingTokenError()
    user, claims = await auth_service.resolve_user_from_token(token)
    return user


def require_permissions(*required_permissions: AuthPermission):
    required = {perm.value for perm in required_permissions}
    async def _require_permissions(auth_service: AuthServiceDep, token: AccessTokenDep, user: AuthUser = Depends(get_current_user)) -> AuthUser:
        granted: set[str]
        if token:
            claims = TOKEN_SERVICE.parse_access_token(token)
            granted = set(claims.permissions or [])
        else:
            granted = set()
        if not granted:
            granted = set(await auth_service.get_permissions_for_roles(user.tenant_id, user.roles))
        if not granted.intersection(required):
            raise PermissionDeniedError()
        return user

    return _require_permissions


async def require_csrf_token(request: Request, csrf_header: Annotated[str | None, Header(alias="X-CSRF-Token")] = None) -> None:
    if request.method not in {"POST", "PUT", "DELETE", "PATCH"}:
        return
    csrf_cookie = request.cookies.get("csrf_token")
    if not csrf_cookie or not csrf_header or not hmac.compare_digest(csrf_cookie, csrf_header):
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Invalid CSRF token")


def require_mcp_tenant_header() -> UUID:
    access_token = get_access_token()
    if access_token is None:
        raise MissingTokenError("Not authenticated")
    claims = access_token.claims or {}
    tenant_claim = claims.get("tenant_id")
    if not tenant_claim:
        raise InvalidTenantClaimError("Invalid token subject")
    try:
        return UUID(str(tenant_claim))
    except ValueError:
        raise InvalidTenantClaimError()


async def verify_whatsapp_signature(request: Request, signature_header: Annotated[str, Header(alias="X-Hub-Signature-256", description="Signature used to verify the sender. Refer [Facebook GraphAPI Webhook documentation](https://developers.facebook.com/docs/graph-api/webhooks/getting-started#verification-requests)."), StringConstraints(pattern=r"^sha256=.+")] ) -> AshaTenantIntegration:
    raw_body = await request.body()
    try:
        body = json.loads(raw_body)
        # Extract phone_number_id from the first entry's metadata
        phone_number_id = body.get("entry", [{}])[0].get("changes", [{}])[0].get("value", {}).get("metadata", {}).get("phone_number_id")
    except (json.JSONDecodeError, IndexError, KeyError):
        phone_number_id = None

    if not phone_number_id:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid WhatsApp payload: phone_number_id not found")

    auth_service = await get_auth_service()
    integration = await auth_service.resolve_integration("whatsapp", phone_number_id)
    if not integration or "app_secret" not in integration.credentials:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"Integration not configured for phone_number_id: {phone_number_id}")

    secret = integration.credentials["app_secret"]
    signature = signature_header.split("=", 1)[1]
    expected = hmac.new(secret.encode("utf-8"), raw_body, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(signature, expected):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid signature")
    return integration


def get_active_phone_id() -> str | None:
    require_mcp_tenant_header()
    access_token = get_access_token()
    if access_token is None:
        raise MissingTokenError("Not authenticated")
    return (access_token.claims or {}).get("phone_number_id")
