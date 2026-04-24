from datetime import datetime, timedelta, timezone
from typing import Any, Tuple
from uuid import UUID

from authlib.jose import JsonWebToken
from authlib.jose.errors import ExpiredTokenError, JoseError
from passlib.context import CryptContext
from pydantic import BaseModel, ConfigDict, Field

from byoeb.services.auth.exceptions import InvalidTokenError, TokenExpiredError
from byoeb.chat_app.configuration import config as env_config


class TokenClaims(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    username: str = Field(alias="sub")
    tenant_id: UUID
    expires_at: int = Field(alias="exp")
    issued_at: int | None = Field(default=None, alias="iat")
    issuer: str | None = Field(default=None, alias="iss")
    audience: str | None = Field(default=None, alias="aud")
    permissions: list[str] | None = Field(default=None)


class AuthTokenService:

    def __init__(self, *, secret: str, algorithm: str, ttl_seconds: int, issuer: str | None, audience: str | None, leeway_seconds: int) -> None:
        self._secret = secret
        self._algorithm = algorithm
        self._ttl_seconds = ttl_seconds
        self._issuer = issuer
        self._audience = audience
        self._leeway_seconds = leeway_seconds
        self._jwt = JsonWebToken([algorithm])

    def create_access_token(self, subject: str, tenant_id: UUID, permissions: list[str] | None = None) -> Tuple[str, int]:
        now = datetime.now(timezone.utc)
        expires_at = now + timedelta(seconds=self._ttl_seconds)
        payload: dict[str, Any] = {
            "sub": subject,
            "tenant_id": str(tenant_id),
            "exp": int(expires_at.timestamp()),
            "iat": int(now.timestamp()),
        }
        if self._issuer:
            payload["iss"] = self._issuer
        if self._audience:
            payload["aud"] = self._audience
        if permissions is not None:
            payload["permissions"] = permissions
        token = self._jwt.encode({"alg": self._algorithm}, payload, self._secret)
        return token.decode("utf-8"), self._ttl_seconds

    def parse_access_token(self, token: str) -> TokenClaims:
        claims_options: dict[str, dict[str, object]] = {
            "exp": {"essential": True},
            "sub": {"essential": True},
            "tenant_id": {"essential": True},
        }
        if self._issuer:
            claims_options["iss"] = {"essential": True, "value": self._issuer}
        if self._audience:
            claims_options["aud"] = {"essential": True, "value": self._audience}
        try:
            claims = self._jwt.decode(token, self._secret, claims_options=claims_options)
            claims.validate(leeway=self._leeway_seconds)
        except ExpiredTokenError as exc:
            raise TokenExpiredError() from exc
        except JoseError as exc:
            raise InvalidTokenError() from exc
        return TokenClaims.model_validate(dict(claims))


secret = env_config.env_auth_token_secret or ""
if len(secret) < 32:
    raise RuntimeError(
        "AUTH_TOKEN_SECRET must be at least 32 characters. "
        "Generate one with: python -c \"import secrets; print(secrets.token_hex(32))\""
    )

PASSWORD_CTX = CryptContext(schemes=["argon2"], deprecated="auto")
TOKEN_SERVICE = AuthTokenService(
    secret=secret,
    algorithm=env_config.env_auth_token_algorithm or "HS256",
    ttl_seconds=int(env_config.env_auth_token_ttl_seconds or "3600"),
    issuer=env_config.env_auth_token_issuer or None,
    audience=env_config.env_auth_token_audience or None,
    leeway_seconds=int(env_config.env_auth_token_leeway_seconds or "0"),
)
