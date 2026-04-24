from pydantic import ConfigDict
from enum import Enum
from uuid import UUID
from typing import Optional, Any, Dict

from byoeb_core.models.byoeb.user import PhoneNumberId
from pydantic import BaseModel, Field


class AuthPermission(str, Enum):
    ADMIN_ACCESS = "admin:access"
    JOBS_RUN = "jobs:run"
    USERS_MANAGE = "users:manage"
    MESSAGES_READ = "messages:read"
    MCP_ACCESS = "mcp:access"
    AUTH_USERS_WRITE = "auth:users:write"
    AUTH_TENANTS_WRITE = "auth:tenants:write"


class AuthTenant(BaseModel):
    id: UUID
    name: str
    roles: dict[str, list[AuthPermission]] = Field(default_factory=dict)


class AuthUser(BaseModel):
    id: UUID
    username: str
    tenant_id: UUID
    roles: list[str] = Field(default_factory=list)
    phone_number_id: Optional[PhoneNumberId] = None

class AshaTenantIntegration(BaseModel):
    model_config = ConfigDict(validate_by_name=True, validate_by_alias=True)

    id: UUID = Field(alias="_id")
    platform: str
    identifier: str
    tenant_id: UUID
    credentials: Dict[str, Any] = Field(default_factory=dict)
