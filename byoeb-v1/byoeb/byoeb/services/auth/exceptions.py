from __future__ import annotations

from dataclasses import dataclass
from http import HTTPStatus
from typing import Mapping


@dataclass
class AuthError(Exception):
    status_code: int
    error_code: str
    detail: str
    headers: Mapping[str, str] | None = None

    def payload(self) -> dict[str, str]:
        return {"error": self.error_code, "error_description": self.detail}


def _unauthorized_headers() -> Mapping[str, str]:
    return {"WWW-Authenticate": "Bearer"}


class InvalidCredentialsError(AuthError):
    def __init__(self, detail: str | None = None) -> None:
        super().__init__(status_code=HTTPStatus.UNAUTHORIZED, error_code="invalid_credentials", detail=detail or "Invalid username or password.", headers=_unauthorized_headers())


class MissingTokenError(AuthError):
    def __init__(self, detail: str | None = None) -> None:
        super().__init__(status_code=HTTPStatus.UNAUTHORIZED, error_code="missing_token", detail=detail or "Authentication token is required.", headers=_unauthorized_headers())


class InvalidTokenError(AuthError):
    def __init__(self, detail: str | None = None) -> None:
        super().__init__(status_code=HTTPStatus.UNAUTHORIZED, error_code="invalid_token", detail=detail or "Invalid or expired token.", headers=_unauthorized_headers())


class TokenExpiredError(AuthError):
    def __init__(self, detail: str | None = None) -> None:
        super().__init__(status_code=HTTPStatus.UNAUTHORIZED, error_code="token_expired", detail=detail or "Token has expired.", headers=_unauthorized_headers())


class PermissionDeniedError(AuthError):
    def __init__(self, detail: str | None = None) -> None:
        super().__init__(status_code=HTTPStatus.FORBIDDEN, error_code="permission_denied", detail=detail or "Permission access forbidden.")


class TenantAccessForbiddenError(AuthError):
    def __init__(self, detail: str | None = None) -> None:
        super().__init__(status_code=HTTPStatus.FORBIDDEN, error_code="tenant_forbidden", detail=detail or "Tenant access forbidden.")


class TenantNotFoundError(AuthError):
    def __init__(self, detail: str | None = None) -> None:
        super().__init__(status_code=HTTPStatus.NOT_FOUND, error_code="tenant_not_found", detail=detail or "Tenant not found.")


class UserNotFoundError(AuthError):
    def __init__(self, detail: str | None = None) -> None:
        super().__init__(status_code=HTTPStatus.NOT_FOUND, error_code="user_not_found", detail=detail or "User not found.")


class RoleNotFoundError(AuthError):
    def __init__(self, detail: str | None = None) -> None:
        super().__init__(status_code=HTTPStatus.NOT_FOUND, error_code="role_not_found", detail=detail or "Role not found.")


class RoleAlreadyExistsError(AuthError):
    def __init__(self, detail: str | None = None) -> None:
        super().__init__(status_code=HTTPStatus.CONFLICT, error_code="role_exists", detail=detail or "Role already exists.")


class TenantAlreadyExistsError(AuthError):
    def __init__(self, detail: str | None = None) -> None:
        super().__init__(status_code=HTTPStatus.CONFLICT, error_code="tenant_exists", detail=detail or "Tenant already exists.")


class UserAlreadyExistsError(AuthError):
    def __init__(self, detail: str | None = None) -> None:
        super().__init__(status_code=HTTPStatus.CONFLICT, error_code="user_exists", detail=detail or "Username already exists.")


class UserTenantConflictError(AuthError):
    def __init__(self, detail: str | None = None) -> None:
        super().__init__(status_code=HTTPStatus.CONFLICT, error_code="user_tenant_conflict", detail=detail or "User already belongs to this tenant.")


class InvalidRoleAssignmentError(AuthError):
    def __init__(self, detail: str | None = None) -> None:
        super().__init__(status_code=HTTPStatus.BAD_REQUEST, error_code="invalid_roles", detail=detail or "One or more roles are not defined for this tenant.")


class InvalidTenantHeaderError(AuthError):
    def __init__(self, detail: str | None = None) -> None:
        super().__init__(status_code=HTTPStatus.BAD_REQUEST, error_code="invalid_tenant_header", detail=detail or "Invalid X-Tenant-ID header.")


class InvalidTenantClaimError(AuthError):
    def __init__(self, detail: str | None = None) -> None:
        super().__init__(status_code=HTTPStatus.BAD_REQUEST, error_code="invalid_tenant_claim", detail=detail or "Invalid tenant in token.")


class MissingPhoneNumberIdError(AuthError):
    def __init__(self, detail: str | None = None) -> None:
        super().__init__(status_code=HTTPStatus.FORBIDDEN, error_code="missing_phone_number", detail=detail or "Phone number ID is missing for this user.")


class InvalidScopeError(AuthError):
    def __init__(self, detail: str | None = None) -> None:
        super().__init__(status_code=HTTPStatus.BAD_REQUEST, error_code="invalid_scope", detail=detail or "One or more requested scopes are not supported.")
