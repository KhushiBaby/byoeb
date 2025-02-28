import logging
import byoeb.chat_app.configuration.dependency_setup as dependency_setup
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

REGISTER_API_NAME = 'register_api'

register_apis_router = APIRouter()
_logger = logging.getLogger(REGISTER_API_NAME)

@register_apis_router.get("/receive")
async def register(request: Request):
    """
    Route to handle the registration process.
    """
    # print("Received the request: ", request.query_params._dict)
    _logger.debug(msg=f"Received the request: \n{request.query_params._dict}")
    response = await dependency_setup.channel_register_handler.handle(request)
    print("Response: ", response.message)
    return JSONResponse(content=int(response.message), status_code=200)
    # return JSONResponse(content={"message": "received"}, status_code=200)
