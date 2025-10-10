import logging
import json
import byoeb.chat_app.configuration.dependency_setup as dependency_setup
from byoeb_core.models.byoeb.message_context import ByoebMessageContext, MessageContext, MessageTypes, ReplyContext
from byoeb.models.message_category import MessageCategory
from byoeb.services.user.utils import get_user_ids_from_phone_number_ids
from byoeb.utils.utils import mcp_get_phone_number
from fastapi import APIRouter, Request, Query
from fastapi.responses import JSONResponse

CHAT_API_NAME = 'chat_api'
chat_apis_router = APIRouter()
_logger = logging.getLogger(CHAT_API_NAME)

@chat_apis_router.post("/receive")
async def receive(request: Request):
    """
    Handle incoming WhatsApp messages.
    """
    body = await request.json()
    # print("Received the request: ", json.dumps(body))
    _logger.info(f"Received the request: {json.dumps(body)}")
    response = await dependency_setup.message_producer_handler.handle(body)
    _logger.info(f"Response: {response}")
    return JSONResponse(
        content=response.message,
        status_code=response.status_code
    )

@chat_apis_router.get("/get_bot_messages")
async def get_bot_messages(
    request: Request, 
    timestamp: str = Query(..., description="Unix timestamp as a string")
):
    """
    Get all messages for a specific BO.
    """
    responses = await dependency_setup.message_db_service.get_latest_bot_messages_by_timestamp(timestamp)
    byoeb_response = []
    for response in responses:
        byoeb_response.append(response.model_dump())
    return JSONResponse(
        content=byoeb_response,
        status_code=200
    )

@chat_apis_router.delete("/delete_message_collection")
async def delete_collection(
    request: Request,
):
    """
    Delete a collection from the database.
    """
    response, e = await dependency_setup.message_db_service.delete_message_collection()
    if response == True:
        return JSONResponse(
            content="Successfully deleted",
            status_code=200
        )
    elif response == False and e is None:
        return JSONResponse(
            content="Failed to delete",
            status_code=500
        )
    elif e is not None:
        return JSONResponse(
            content=f"Error: {e}",
            status_code=500
        )

def chat_mcps_router(mcp):
    @mcp.tool
    async def asha_chat(message: str) -> str:
        """
        Ask any health-related query and get a response.
        """
        phone_number = mcp_get_phone_number()
        user_id = get_user_ids_from_phone_number_ids([phone_number])[0]
        users = await dependency_setup.user_db_service.get_users([user_id])
        if len(users) == 0:
            return "Before I can answer your question, you must register yourself as an ASHA user. Shall I start with the registration?"

        user = users[0]
        ctx = ByoebMessageContext(
            channel_type="whatsapp",
            message_category="whatsapp",
            user=user,
            message_context=MessageContext(
                message_id="chat-mcps-router-for-" + user_id,
                message_type=MessageTypes.REGULAR_TEXT.value,
                message_source_text=message,
                message_english_text=message,
                media_info=None,
                additional_info=dict(query_type="asha_work_related")
            ),
            reply_context=ReplyContext(
                reply_id="reply-id-unknown",
                reply_type="acknowledgement",
                reply_source_text=message,
                reply_english_text=message,
                media_info=None,
                message_category="notification",
                additional_info=None
            ),
            cross_conversation_id=None,
            cross_conversation_context=None,
            incoming_timestamp=None,
            outgoing_timestamp=None
        )
        responses = await dependency_setup.byoeb_user_generate_response.handle_message_generate_workflow([ctx])
        for resp in responses:
            if resp.message_category == MessageCategory.BOT_TO_USER_RESPONSE.value:
                response = resp.message_context.message_source_text
                if "description" in resp.message_context.additional_info and "row_texts" in resp.message_context.additional_info:
                    response += "\n\n" + resp.message_context.additional_info["description"]
                    response += str(resp.message_context.additional_info["row_texts"])
                return response
        return "I cannot answer that."
