from typing import Any, List, Optional, Dict
from byoeb_core.models.byoeb.user import PhoneNumberId, User
from fastapi import APIRouter, Body
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from byoeb_core.models.byoeb.response import ByoebStatusCodes
import byoeb.chat_app.configuration.dependency_setup as dependency_setup
from byoeb.constants.user_enums import LanguageCode, UserType
from byoeb.services.user.utils import get_user_ids_from_phone_number_ids
from byoeb.utils.utils import mcp_get_phone_number

USER_API_NAME = "user_api"
user_apis_router = APIRouter(tags=["Users"])


# -----------------------------
# Pydantic models
# -----------------------------

class UserLocation(BaseModel):
    district: str = Field(..., description="District name (required)", examples=["Jaipur"])
    block: Optional[str] = Field(None, description="Block name", examples=["Sanganer"])
    sector: Optional[str] = Field(None, description="Sector name", examples=["Sector 12"])
    sub_center: Optional[str] = Field(None, description="Sub-center name", examples=["SC-45"])


class UserRegister(BaseModel):
    phone_number_id: PhoneNumberId = Field(..., description="Phone number ID of the user")
    user_location: Optional[UserLocation] = Field(..., description="Location details (district is mandatory)")
    user_type: UserType = Field(..., description="Type of user (asha, anm, etc.)")
    user_language: LanguageCode = Field(..., description="Language code (hi, en, te, etc.)")
    user_name: Optional[str] = Field(None, description="Name of the user", examples=["Sita Devi"])
    test_user: Optional[bool] = Field(False, description="Flag to mark test users")


class UserUpdate(BaseModel):
    phone_number_id: PhoneNumberId = Field(..., description="Phone number ID of the user")
    user_name: Optional[str] = Field(None, description="Updated name of the user", examples=["John Doe"])
    user_location: Optional[Dict[str, Any]] = Field(None, description="Updated location details")
    user_language: Optional[LanguageCode] = Field(None, description="Updated language code")
    user_type: Optional[UserType] = Field(None, description="Updated type of user")
    user_name: Optional[str] = Field(None, description="Name of the user", examples=["Sita Devi"])
    test_user: Optional[bool] = Field(None, description="Flag to mark test users")


# -----------------------------
# Endpoints
# -----------------------------

@user_apis_router.post("/register_users", summary="Register one or more users")
async def register_users(
    users: List[UserRegister] = Body(..., description="List of users to register. Mandatory fields: `user_type`, `user_location` and `phone_number_id`. `district` inside `user_location` is mandatory.")
) -> List[User]:
    """
    Registers one or more users.

    - `district` inside `user_location` is **mandatory**.
    - Other fields like `block`, `sector`, `sub_center` are optional.
    - Calls the async handler `users_handler.aregister` with validated payload.
    """
    payload = [u.dict(exclude_unset=True) for u in users]
    response = await dependency_setup.users_handler.aregister(payload)
    if response.status_code == ByoebStatusCodes.OK.value:
        return [User(**x) for x in response.message]

    return JSONResponse(content=response.message, status_code=response.status_code)


@user_apis_router.post("/update_users", summary="Update one or more users")
async def update_users(
    users: List[UserUpdate] = Body(..., description="List of users to update. Only include fields that need updating.")
) -> JSONResponse:
    """
    Update one or more users.

    - `phone_number_id` is **mandatory**.
    - Include only fields that need to be updated.
    - Calls `users_handler.aupdate` internally.
    """
    payload = [u.dict(exclude_unset=True) for u in users]
    response = await dependency_setup.users_handler.aupdate(payload)
    return JSONResponse(
        content=response.message,
        status_code=response.status_code
    )
       
@user_apis_router.delete("/delete_users", summary="Delete one or more users")
async def delete_users(users: List[PhoneNumberId] = Body(..., description="List of phone_number_ids (must be numeric).")) -> JSONResponse:
    """
    Deletes users matching the provided phone_number_ids.
    """
    response = await dependency_setup.users_handler.adelete(users)
    return JSONResponse(
        status_code=response.status_code,
        content=response.message if isinstance(response.message, str) else str(response.message),
    )

@user_apis_router.post("/get_users", summary="Retrieve one or more users by phone_number_id")
async def get_users(
    phone_number_ids: List[PhoneNumberId] = Body(..., description="List of phone_number_ids.")
) -> List[User]:
    """
    Retrieve user information for one or more users.
    - Accepts multiple `phone_number_id` values as query parameters.
    """
    response = await dependency_setup.users_handler.aget(phone_number_ids)
    print("Response from aget:", response.message)

    return list(map(lambda x: User(**x), response.message))

    
# -----------------------------
# MCP Tool Registration
# -----------------------------
def user_mcps_router(mcp):
    class UserInput(BaseModel):
        name: str = Field(..., min_length=3, max_length=100)
        language: LanguageCode = Field(..., description="Supported language codes")
        state: str = Field(..., description="Name of a state in India")

    @mcp.tool
    async def asha_register_user(data: UserInput):
        """
        Register a new Asha user.
        """
        phone_number = mcp_get_phone_number()
        response = await dependency_setup.users_handler.aregister([
            dict(
                user_id=get_user_ids_from_phone_number_ids([phone_number])[0],
                user_name=data.name,
                user_location=dict(country="IN", region=data.state),
                user_language=data.language.value,
                user_type=UserType.ASHA.value,
                phone_number_id=phone_number,
            )
        ])
        return response