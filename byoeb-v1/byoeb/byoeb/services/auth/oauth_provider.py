from __future__ import annotations

import hmac
import secrets
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from functools import cached_property
from typing import Any
from uuid import UUID

import asyncio
from authlib.oauth2.rfc6749 import AuthorizationServer, ClientMixin, JsonRequest, OAuth2Payload, OAuth2Request, TokenMixin
from authlib.oauth2.rfc6749.grants import AuthorizationCodeGrant, RefreshTokenGrant
from authlib.oauth2.rfc7636 import CodeChallenge
from pydantic import AnyHttpUrl
from starlette.requests import Request
from starlette.responses import JSONResponse, RedirectResponse, Response
from starlette.routing import Route

from fastmcp.server.auth.auth import AccessToken, AuthProvider
from mcp.server.auth.handlers.metadata import MetadataHandler
from mcp.server.auth.handlers.register import RegistrationHandler
from mcp.server.auth.routes import build_metadata, create_protected_resource_routes, cors_middleware
from mcp.server.auth.settings import ClientRegistrationOptions, RevocationOptions
from mcp.shared.auth import OAuthClientInformationFull

from byoeb.services.auth.auth_service import get_auth_service
from byoeb.services.auth.exceptions import AuthError
from byoeb.services.auth.models import AuthPermission
from byoeb.services.auth.security import TOKEN_SERVICE


_AUTH_CODE_TTL_SECONDS = 5 * 60


class _OAuthHelper:

    def __init__(self):
        self._loop: asyncio.AbstractEventLoop | None = None

    async def get_client(self, client_id: str) -> OAuthClient | None:
        auth_service = await get_auth_service()
        client_info = await auth_service.find_oauth_client(client_id)
        if not client_info:
            return None
        return OAuthClient.from_info(client_info)

    async def save_token(self, token: dict[str, Any], request: OAuth2Request) -> None:
        user = request.user
        if not user:
            return
        refresh_token = token.get("refresh_token")
        if refresh_token:
            client_id = request.client.get_client_id() if request.client else None
            scope = token.get("scope")
            auth_service = await get_auth_service()
            await auth_service.update_refresh_token(user.username, refresh_token, client_id, scope)

    async def store_auth_code(self, code: str, request: OAuth2Request) -> None:
        auth_service = await get_auth_service()
        expires_at = datetime.now(timezone.utc) + timedelta(seconds=_AUTH_CODE_TTL_SECONDS)
        await auth_service.store_auth_code(
            code=code,
            client_id=request.client.get_client_id(),
            redirect_uri=request.payload.redirect_uri,
            scope=request.payload.scope,
            code_challenge=request.payload.data.get("code_challenge"),
            code_challenge_method=request.payload.data.get("code_challenge_method"),
            username=request.user.username,
            tenant_id=request.user.tenant_id,
            expires_at=expires_at,
        )

    async def query_auth_code(self, code: str, client: OAuthClient) -> OAuthAuthorizationCode | None:
        auth_service = await get_auth_service()
        record = await auth_service.find_auth_code(code)
        if not record or record.get("client_id") != client.get_client_id():
            return None
        return OAuthAuthorizationCode(
            code=record["code"],
            client_id=record["client_id"],
            redirect_uri=record["redirect_uri"],
            scope=record.get("scope"),
            expires_at=record["expires_at"],
            code_challenge=record.get("code_challenge"),
            code_challenge_method=record.get("code_challenge_method"),
            username=record["subject_username"],
            tenant_id=record["subject_tenant_id"],
        )

    async def delete_auth_code(self, authorization_code: OAuthAuthorizationCode) -> None:
        auth_service = await get_auth_service()
        await auth_service.delete_auth_code(authorization_code.code)

    async def authenticate_code_user(self, authorization_code: OAuthAuthorizationCode):
        auth_service = await get_auth_service()
        return await auth_service.get_user_by_username(authorization_code.username, authorization_code.tenant_id)

    async def authenticate_refresh_token(self, refresh_token: str) -> OAuthRefreshToken | None:
        auth_service = await get_auth_service()
        user_doc = await auth_service.find_user_doc_by_refresh_token(refresh_token)
        if not user_doc:
            return None
        username = user_doc.get("username")
        tenant_id = next((entry.get("tenant_id") for entry in user_doc.get("tenants", []) if entry.get("tenant_id")), None)
        if not username or not tenant_id:
            return None
        return OAuthRefreshToken(
            token=refresh_token,
            client_id=user_doc.get("refresh_client_id"),
            scope=user_doc.get("refresh_scopes"),
            username=username,
            tenant_id=UUID(str(tenant_id)),
        )

    async def authenticate_refresh_user(self, refresh_token: OAuthRefreshToken):
        auth_service = await get_auth_service()
        return await auth_service.get_user_by_username(refresh_token.username, refresh_token.tenant_id)

    async def revoke_refresh_token(self, refresh_token: OAuthRefreshToken) -> None:
        auth_service = await get_auth_service()
        await auth_service.revoke_refresh_token(refresh_token.token)

    def ensure_loop(self) -> None:
        if self._loop is None:
            self._loop = asyncio.get_running_loop()

    def run_coroutine(self, coro):
        if self._loop is None:
            raise RuntimeError("OAuth provider event loop not initialized")
        return asyncio.run_coroutine_threadsafe(coro, self._loop).result()


class StarletteOAuth2Payload(OAuth2Payload):
    def __init__(self, data: Any):
        self._data = data

    @property
    def data(self):
        return self._data

    @cached_property
    def datalist(self):
        values = defaultdict(list)
        if hasattr(self._data, "multi_items"):
            for key, value in self._data.multi_items():
                values[key].append(value)
            return values
        if hasattr(self._data, "getlist"):
            for key in self._data:
                values[key].extend(self._data.getlist(key))
            return values
        for key, value in self._data.items():
            if isinstance(value, list):
                values[key].extend(value)
            else:
                values[key].append(value)
        return values


class StarletteOAuth2Request(OAuth2Request):
    def __init__(self, request: Request, form_data: Any | None):
        super().__init__(method=request.method, uri=str(request.url), headers=dict(request.headers))
        self._request = request
        self._form_data = form_data or {}
        payload_data = self._form_data if self._form_data else request.query_params
        self.payload = StarletteOAuth2Payload(payload_data)

    @property
    def args(self):
        return self._request.query_params

    @property
    def form(self):
        return self._form_data


@dataclass
class OAuthClient(ClientMixin):
    client_id: str
    client_secret: str | None
    redirect_uris: list[str]
    grant_types: list[str]
    response_types: list[str]
    scope: str | None
    token_endpoint_auth_method: str | None

    @classmethod
    def from_info(cls, info: OAuthClientInformationFull) -> "OAuthClient":
        return cls(
            client_id=info.client_id or "",
            client_secret=info.client_secret,
            redirect_uris=[str(uri) for uri in (info.redirect_uris or [])],
            grant_types=list(info.grant_types or []),
            response_types=list(info.response_types or []),
            scope=info.scope,
            token_endpoint_auth_method=info.token_endpoint_auth_method,
        )

    def get_client_id(self):
        return self.client_id

    def get_default_redirect_uri(self):
        return self.redirect_uris[0] if self.redirect_uris else ""

    def get_redirect_uris(self):
        return self.redirect_uris

    def get_allowed_scope(self, scope):
        if not self.scope:
            return scope or ""
        allowed = set(self.scope.split())
        if not scope:
            return " ".join(sorted(allowed))
        return " ".join([entry for entry in scope.split() if entry in allowed])

    def check_redirect_uri(self, redirect_uri):
        return redirect_uri in self.redirect_uris

    def check_client_secret(self, client_secret):
        return hmac.compare_digest(client_secret or "", self.client_secret or "")

    def check_endpoint_auth_method(self, method, endpoint):
        if endpoint != "token":
            return True
        expected = self.token_endpoint_auth_method or ("none" if not self.client_secret else "client_secret_post")
        return method == expected

    def check_response_type(self, response_type):
        return response_type in self.response_types

    def check_grant_type(self, grant_type):
        return grant_type in self.grant_types


@dataclass
class OAuthAuthorizationCode:
    code: str
    client_id: str
    redirect_uri: str | None
    scope: str | None
    expires_at: datetime
    code_challenge: str | None
    code_challenge_method: str | None
    username: str
    tenant_id: UUID

    def get_redirect_uri(self): return self.redirect_uri
    def get_scope(self): return self.scope


@dataclass
class OAuthRefreshToken(TokenMixin):
    token: str
    client_id: str | None
    scope: str | None
    username: str
    tenant_id: UUID

    def check_client(self, client): return self.client_id is None or self.client_id == client.get_client_id()
    def get_scope(self): return self.scope
    def get_expires_in(self): return None
    def is_expired(self): return False
    def is_revoked(self): return False
    def get_user(self): return {"username": self.username, "tenant_id": self.tenant_id}
    def get_client(self): return None

class OAuth2AuthorizationServer(AuthorizationServer):
    def __init__(self, provider: "OAuthProvider", scopes_supported: list[str] | None):
        super().__init__(scopes_supported=scopes_supported)
        self._helper: _OAuthHelper = provider._helper

    def query_client(self, client_id):
        return self._helper.run_coroutine(self._helper.get_client(client_id))

    def save_token(self, token, request):
        return self._helper.run_coroutine(self._helper.save_token(token, request))

    def send_signal(self, name, *args, **kwargs):
        # No signal system wired up; treat hooks as no-ops.
        return None

    def create_oauth2_request(self, request):
        if isinstance(request, OAuth2Request):
            return request
        return StarletteOAuth2Request(request, None)

    def create_json_request(self, request):
        return JsonRequest(request.method, str(request.url), headers=dict(request.headers))

    def handle_response(self, status, body, headers):
        header_map = dict(headers or [])
        if isinstance(body, dict):
            return JSONResponse(body, status_code=status, headers=header_map)
        return Response(body, status_code=status, headers=header_map)


class OAuth2AuthorizationCodeGrant(AuthorizationCodeGrant):
    TOKEN_ENDPOINT_AUTH_METHODS = ["none", "client_secret_basic", "client_secret_post"]

    def __init__(self, request, server):
        super().__init__(request, server)
        self._helper: _OAuthHelper = server._helper

    def save_authorization_code(self, code, request): return self._helper.run_coroutine(self._helper.store_auth_code(code, request))
    def query_authorization_code(self, code, client): return self._helper.run_coroutine(self._helper.query_auth_code(code, client))
    def delete_authorization_code(self, authorization_code): return self._helper.run_coroutine(self._helper.delete_auth_code(authorization_code))
    def authenticate_user(self, authorization_code): return self._helper.run_coroutine(self._helper.authenticate_code_user(authorization_code))


class OAuth2RefreshTokenGrant(RefreshTokenGrant):
    INCLUDE_NEW_REFRESH_TOKEN = True
    TOKEN_ENDPOINT_AUTH_METHODS = ["none", "client_secret_basic", "client_secret_post"]

    def __init__(self, request, server):
        super().__init__(request, server)
        self._helper: _OAuthHelper = server._helper

    def authenticate_refresh_token(self, refresh_token): return self._helper.run_coroutine(self._helper.authenticate_refresh_token(refresh_token))
    def authenticate_user(self, refresh_token): return self._helper.run_coroutine(self._helper.authenticate_refresh_user(refresh_token))
    def revoke_old_credential(self, refresh_token): return self._helper.run_coroutine(self._helper.revoke_refresh_token(refresh_token))


class OAuthProvider(AuthProvider):

    def __init__(self, *, base_url: AnyHttpUrl | str, scopes_required: list[AuthPermission], scopes_default: list[AuthPermission], revocation_options: RevocationOptions | None = None):
        super().__init__(base_url=base_url, required_scopes=[s.value for s in scopes_required])
        self._helper = _OAuthHelper()
        self._client_registration_options = ClientRegistrationOptions(enabled=True, valid_scopes=[p.value for p in AuthPermission], default_scopes=[s.value for s in scopes_default])
        self._revocation_options = revocation_options or RevocationOptions()
        self._server = OAuth2AuthorizationServer(self, self._client_registration_options.valid_scopes or self.required_scopes)
        self._server.register_grant(OAuth2AuthorizationCodeGrant, [CodeChallenge(required=True)])
        self._server.register_grant(OAuth2RefreshTokenGrant)
        self._server.register_token_generator("default", self._token_generator)

    async def verify_token(self, token: str) -> AccessToken | None:
        self._helper.ensure_loop()
        auth_service = await get_auth_service()
        try:
            user, claims = await auth_service.resolve_user_from_token(token)
            permissions = await auth_service.get_permissions_for_roles(user.tenant_id, user.roles)
        except AuthError:
            return None
        phone_number_id = str(user.phone_number_id) if user.phone_number_id is not None else None
        return AccessToken(
            token=token,
            client_id=user.username,
            scopes=permissions,
            expires_at=claims.expires_at,
            resource_owner=user.username,
            claims={
                "tenant_id": str(user.tenant_id),
                "roles": user.roles,
                "permissions": permissions,
                "phone_number_id": phone_number_id,
            },
        )

    def get_routes(self, mcp_path: str | None = None) -> list[Route]:
        assert self.base_url is not None
        metadata = build_metadata(AnyHttpUrl(str(self.base_url).rstrip("/") + "/oauth"), None, self._client_registration_options, self._revocation_options)
        metadata.token_endpoint_auth_methods_supported.append("none")
        routes = [
            Route("/.well-known/oauth-authorization-server", endpoint=cors_middleware(MetadataHandler(metadata).handle, ["GET", "OPTIONS"]), methods=["GET", "OPTIONS"]),
            Route("/oauth/authorize", endpoint=self._authorize, methods=["GET", "POST"]),
            Route("/oauth/token", endpoint=cors_middleware(self._token, ["POST", "OPTIONS"]), methods=["POST", "OPTIONS"]),
        ]
        if self._client_registration_options.enabled:
            registration_handler = RegistrationHandler(self, options=self._client_registration_options)
            routes.append(Route("/oauth/register", endpoint=cors_middleware(registration_handler.handle, ["POST", "OPTIONS"]), methods=["POST", "OPTIONS"]))
        return routes

    def create_resource_routes(self, resource_path: str, scopes: list[AuthPermission]) -> list[Route]:
        resource_url = self._get_resource_url(resource_path)
        if not resource_url: raise RuntimeError("Cannot create resource URL")
        return create_protected_resource_routes(resource_url=resource_url, authorization_servers=[self.base_url], scopes_supported=[s.value for s in scopes])

    async def get_client(self, client_id: str) -> OAuthClientInformationFull | None:
        auth_service = await get_auth_service()
        return await auth_service.find_oauth_client(client_id)

    async def register_client(self, client_info: OAuthClientInformationFull) -> None:
        self._helper.ensure_loop()
        auth_service = await get_auth_service()
        await auth_service.register_oauth_client(client_info)

    async def _authorize(self, request: Request) -> Response:
        self._helper.ensure_loop()
        if request.method == "GET":
            query = request.url.query
            auth_path = request.url.path
            location = "/login"
            if query:
                location = f"{location}?{query}&auth_path={auth_path}"
            else:
                location = f"{location}?auth_path={auth_path}"
            return RedirectResponse(location, status_code=302)
        # CSRF protection: reject cross-origin POST requests.
        # The OAuth login form on /login always sets Origin (same-origin form submit).
        # An attacker-controlled page targeting this endpoint would have a different Origin.
        # We only reject when Origin/Referer is present and does not match, to avoid breaking
        # clients that suppress these headers for legitimate reasons.
        from urllib.parse import urlparse
        from byoeb.services.auth.dependencies import get_public_base_url
        public_base = get_public_base_url()
        public_origin = "{0.scheme}://{0.netloc}".format(urlparse(public_base))
        request_origin = request.headers.get("origin")
        request_referer = request.headers.get("referer", "")
        source = request_origin or request_referer
        if source and not (source == public_origin or source.startswith(public_origin + "/")):
            return JSONResponse(
                {"error": "invalid_request", "error_description": "Cross-origin request not allowed."},
                status_code=403,
            )
        form = await request.form()
        username = form.get("username")
        password = form.get("password")
        tenant_id = form.get("tenant_id")
        if not username or not password or not tenant_id:
            return JSONResponse({"error": "invalid_request", "error_description": "Missing credentials."}, status_code=400)
        try:
            auth_service = await get_auth_service()
            user = await auth_service.authenticate_user(username, password, UUID(str(tenant_id)))
            scope = form.get("scope") or ""
            scopes = set(scope.split())
            await auth_service.validate_requested_scopes(user, scopes)
            oauth_request = StarletteOAuth2Request(request, form)
            return await asyncio.to_thread(self._server.create_authorization_response, oauth_request, grant_user=user)
        except AuthError as exc:
            return JSONResponse(exc.payload(), status_code=exc.status_code, headers=dict(exc.headers or {}))

    async def _token(self, request: Request) -> Response:
        self._helper.ensure_loop()
        oauth_request = StarletteOAuth2Request(request, await request.form())
        return await asyncio.to_thread(self._server.create_token_response, oauth_request)

    def _token_generator(self, grant_type, client, user=None, scope=None, expires_in=None, include_refresh_token=True):
        granted_scope = scope
        granted_permissions: list[str] | None = None
        if user:
            auth_service = self._helper.run_coroutine(get_auth_service())
            granted_permissions = self._helper.run_coroutine(auth_service.get_permissions_for_roles(user.tenant_id, user.roles))
            if scope:
                allowed = set(granted_permissions)
                granted_scope = " ".join([entry for entry in scope.split() if entry in allowed]) or None
        access_token, ttl_seconds = TOKEN_SERVICE.create_access_token(user.username, user.tenant_id, permissions=granted_permissions)
        token = {"access_token": access_token, "token_type": "Bearer", "expires_in": ttl_seconds}
        if granted_scope:
            token["scope"] = granted_scope
        if include_refresh_token:
            token["refresh_token"] = secrets.token_urlsafe(48)
        return token
