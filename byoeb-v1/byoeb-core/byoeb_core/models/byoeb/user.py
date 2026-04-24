from typing import Annotated, List, Optional, Dict, Any
from pydantic import BaseModel, Field, StringConstraints, field_validator
from datetime import datetime, timezone
from uuid import UUID

PhoneNumberId = Annotated[str, StringConstraints(pattern=r"^\d{11,13}$")]


def _require_utc_or_none(value: Optional[datetime]) -> Optional[datetime]:
    """Ensure datetime is None or timezone-aware UTC. Run with mode='after' so Pydantic coerces int/str/etc. to datetime first."""
    if value is None:
        return None
    if value.tzinfo is None:
        raise ValueError("datetime must be timezone-aware (e.g. tzinfo=timezone.utc)")
    if value.tzinfo != timezone.utc:
        return value.astimezone(timezone.utc)
    return value


class User(BaseModel):
    user_id: Optional[str] = Field(default=None, description="Unique identifier for the user", examples=["12345"])
    tenant_id: Optional[UUID] = Field(default=None, description="Tenant ID of the user")
    user_name: Optional[str] = Field(default=None, description="Name of the user", examples=["John Doe"])
    user_location: Optional[Dict] = Field(default={}, description="Region of the user", examples=["US"])
    user_language: Optional[str] = Field(default=None, description="Language preference of the user", examples=["en"])
    user_type: Optional[str] = Field(default=None, description="Type of the user, e.g., 'admin' or 'normal'")
    phone_number_id: PhoneNumberId = Field(..., description="Phone number ID of the user")
    test_user: Optional[bool] = Field(default=False, description="Indicates if the user is a test user")
    experts: Optional[Dict[str, List[Any]]] = Field(default_factory=dict, description="List of expert phone numbers associated with the user")
    audience: Optional[List[str]] = Field(default_factory=list, description="List of users associated with this user")
    created_timestamp: Optional[datetime] = Field(
        default=None,
        description="Timestamp when the user was created",
        examples=[datetime(2021, 10, 1, 0, 0, 0, tzinfo=timezone.utc)],
    )
    activity_timestamp: Optional[datetime] = Field(
        default=None,
        description="Timestamp of the user's last activity",
        examples=[datetime(2021, 10, 1, 0, 0, 0, tzinfo=timezone.utc)],
    )
    last_conversations: Optional[List[Dict[str, Any]]] = Field(default_factory=list, description="List of the user's last conversations")
    additional_info: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Any additional information related to the user")

    @field_validator("created_timestamp", "activity_timestamp", mode="after")
    @classmethod
    def _ensure_utc(cls, v: Optional[datetime]) -> Optional[datetime]:
        return _require_utc_or_none(v)
