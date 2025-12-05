import base64
import logging
import json
import time
import uuid
from typing import Any, List, Dict, Literal, Set
import byoeb.chat_app.configuration.dependency_setup as dependency_setup
from pydantic import BaseModel, Field
from byoeb_core.models.byoeb.message_context import (
    ByoebMessageContext,
    MessageContext,
    MessageTypes,
    ReplyContext,
)
from byoeb.models.message_category import MessageCategory
from byoeb.services.user.utils import get_user_ids_from_phone_number_ids
from byoeb.utils.utils import mcp_get_phone_number
from fastapi import APIRouter, Query, Body
from fastapi.responses import JSONResponse
import byoeb.services.chat.constants as chat_constants
import byoeb.utils.utils as utils

# ---------------------------------------------------------
# Setup
# ---------------------------------------------------------

CHAT_API_NAME = "chat_api"
chat_apis_router = APIRouter(tags=["Chat"])
_logger = logging.getLogger(CHAT_API_NAME)

# ---------------------------------------------------------
# Endpoints
# ---------------------------------------------------------
@chat_apis_router.post("/receive", summary="Handle incoming WhatsApp messages")
async def receive(body: Dict[str, Any] = Body(..., description="Raw WhatsApp webhook payload")) -> JSONResponse:
    """
    Handles an incoming WhatsApp message from a user.
    The message is processed by the message_producer_handler.
    """
    _logger.info(f"Received WhatsApp request: {json.dumps(body, ensure_ascii=False)}")
    response = await dependency_setup.message_producer_handler.handle(body)
    _logger.info(f"Handler response: {response}")
    return JSONResponse(
        status_code=response.status_code,
        content=response.message if isinstance(response.message, str) else str(response.message)
    )


@chat_apis_router.get("/get_bot_messages", summary="Fetch bot messages after a given timestamp")
async def get_bot_messages(
    timestamp: int = Query(..., description="Unix timestamp to fetch messages since")
) -> List[ByoebMessageContext]:
    """
    Retrieves all bot messages stored in the database
    after the specified timestamp.
    """
    responses = await dependency_setup.message_db_service.get_latest_bot_messages_by_timestamp(str(timestamp))
    return responses


# ---------------------------------------------------------
# MCP Tool
# ---------------------------------------------------------

def chat_mcps_router(mcp):
    class AshaChatResponse(BaseModel):
        category: str = Field(description="Category of the response")
        text: str = Field(description="Response to the query")
        additional_info: list[tuple[str, Any]] = Field(default=[], description="Additional info pertaining to the query and response")

    class AdditionalInfoBuilder:
        def __init__(self):
            self._items: list[tuple[str, Any]] = []

        def add_internal_query(self, resp):
            if resp.reply_context and resp.reply_context.reply_english_text:
                self._items.append(("Internal query", resp.reply_context.reply_english_text))
            return self

        def add_description_rows(self, info):
            if "description" in info and "row_texts" in info:
                self._items.append((info["description"], info["row_texts"]))
            return self

        def add_cache_hit(self, info):
            if "cache_hit" in info:
                self._items.append(("Cache hit", info["cache_hit"]))
            return self

        def add_cache_score(self, info):
            if "cache_score" in info:
                self._items.append(("Cache score", info["cache_score"]))
            return self

        def add_history(self, features, resp):
            if (
                "history" in features
                and resp.reply_context
                and resp.reply_context.additional_info
                and "conversation_history" in resp.reply_context.additional_info
            ):
                self._items.append(("Conversation history", resp.reply_context.additional_info["conversation_history"]))
            return self

        def add_audio(self, features, info):
            if "audio" in features and "mime_type" in info and "data" in info:
                self._items.append(("Audio", (info["mime_type"], base64.b64encode(info["data"]))))
            return self

        def build(self) -> list[tuple[str, Any]]:
            return self._items

    @mcp.tool
    async def asha_chat(message: str, features: Set[Literal["audio", "history"]] = set()) -> AshaChatResponse:
        """
        Ask any health-related query and get a response.
        """
        phone_number = mcp_get_phone_number()
        user_id = get_user_ids_from_phone_number_ids([phone_number])[0]
        users = await dependency_setup.user_db_service.get_users([user_id])

        if len(users) == 0:
            return AshaChatResponse(category="unknown_user", text=(
                "Before I can answer your question, you must register yourself as an ASHA user. "
                "Shall I start with the registration?"
            ))

        user = users[0]
        message_id = f"chat-mcps-{user_id}-{uuid.uuid4()}"
        ctx = ByoebMessageContext(
            channel_type="whatsapp",
            message_category="whatsapp",
            user=user,
            message_context=MessageContext(
                message_id=message_id,
                message_type=MessageTypes.REGULAR_TEXT.value,
                message_source_text=message,
                message_english_text=message,
                media_info=None,
                additional_info=dict(query_type="asha_work_related"),
            ),
            reply_context=ReplyContext(
                reply_id="reply-id-unknown",
                reply_type="acknowledgement",
                reply_source_text=message,
                reply_english_text=message,
                media_info=None,
                message_category="notification",
                additional_info=None,
            ),
            cross_conversation_id=None,
            cross_conversation_context=None,
            incoming_timestamp=None,
            outgoing_timestamp=None,
        )

        processed_ctx = await dependency_setup.byoeb_user_process.handle_process_message_workflow([ctx])
        responses = await dependency_setup.byoeb_user_generate_response.handle_message_generate_workflow([processed_ctx]) or []

        preferred_categories = {MessageCategory.BOT_TO_USER_RESPONSE.value, MessageCategory.TEXT_IDK.value, MessageCategory.AUDIO_IDK.value}
        for resp in responses:
            if resp.message_context is None or resp.message_context.message_source_text is None:
                continue
            if resp.message_category not in preferred_categories:
                continue

            response_text = resp.message_context.message_source_text
            info = resp.message_context.additional_info or {}
            additional_info = (AdditionalInfoBuilder()
                .add_internal_query(resp)
                .add_description_rows(info)
                .add_cache_hit(info)
                .add_cache_score(info)
                .add_history(features, resp)
                .add_audio(features, info)
                .build())

            # persist QA for conversation continuity
            qa = {
                chat_constants.AUDIO_MESSAGE_ID: None,
                chat_constants.TEXT_MESSAGE_ID: resp.message_context.message_id,
                chat_constants.TIMESTAMP: str(int(time.time())),
                chat_constants.QUESTION: processed_ctx.message_context.message_english_text,
                chat_constants.ANSWER: resp.message_context.message_english_text,
            } if not utils.is_idk(resp.message_context.message_english_text) else None
            update_query = dependency_setup.user_db_service.user_activity_update_query(user, qa)
            await dependency_setup.user_db_service.execute_queries({chat_constants.UPDATE: [update_query]})
            return AshaChatResponse(category=resp.message_category, text=response_text, additional_info=additional_info)

        return AshaChatResponse(category=MessageCategory.TEXT_IDK.value, text="I cannot answer that at the moment.")
