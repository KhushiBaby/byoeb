import logging
import byoeb.chat_app.configuration.dependency_setup as dependency_setup
from byoeb.constants.user_enums import LanguageCode, UserType
from byoeb.services.user.utils import get_user_ids_from_phone_number_ids
from byoeb.utils.utils import mcp_get_phone_number
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

USER_API_NAME = 'user_api'

user_apis_router = APIRouter()
_logger = logging.getLogger(USER_API_NAME)

@user_apis_router.post("/register_users")
async def register_users(request: Request):
    body = await request.json()
    response = await dependency_setup.users_handler.aregister(body)
    print("Response: ", response.message)
    return JSONResponse(
        content=response.message,
        status_code=response.status_code
    )

#@user_apis_router.post("/update_users")
#async def update_users():
#    return JSONResponse(content={"message": "received"}, status_code=200)
@user_apis_router.post("/update_users")
async def update_users(request: Request):
    try:
        body = await request.json()
        response = await dependency_setup.users_handler.aupdate(body)
        print("Response: ", response.message)
        return JSONResponse(
            content=response.message,
            status_code=response.status_code
        )
    except Exception as e:
        _logger.exception(f"Error in /update_users: {str(e)}")
        return JSONResponse(
            content={"error": str(e)},
            status_code=500
        )

@user_apis_router.delete("/delete_users")
async def delete_users(request: Request):
    body = await request.json()
    response = await dependency_setup.users_handler.adelete(body)
    return JSONResponse(
        content=response.message,
        status_code=response.status_code
    )

@user_apis_router.get("/get_users")
async def get_users(request: Request):
    body = await request.json()
    response = await dependency_setup.users_handler.aget(body)
    return JSONResponse(
        content=response.message,
        status_code=response.status_code
    )

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
        response = await dependency_setup.users_handler.aregister([dict(
            user_id=get_user_ids_from_phone_number_ids([phone_number])[0],
            user_name=data.name,
            user_location=dict(country="IN", region=data.state),
            user_language=data.language.value,
            user_type=UserType.ASHA.value,
            phone_number_id=phone_number
        )])
        return response
