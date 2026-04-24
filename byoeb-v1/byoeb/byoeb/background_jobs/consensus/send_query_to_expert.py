import asyncio
import os
import sys
import threading
import logging
from byoeb.services.chat import constants
from byoeb.services.chat import utils as chat_utils
from byoeb.services.databases.mongo_db.message_db import MessageMongoDBService
from byoeb.services.databases.mongo_db.user_db import UserMongoDBService
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from byoeb_core.models.byoeb.message_context import ByoebMessageContext
from byoeb_core.models.byoeb.user import User
from byoeb_core.channel.base import BaseChannel
from byoeb.services.channel.whatsapp import WhatsAppService
from byoeb_core.models.byoeb.message_context import (
    ByoebMessageContext,
    MessageContext,
    ReplyContext,
    MessageTypes
)
from byoeb.background_jobs.consensus.config import bot_config
from byoeb.chat_app.configuration.config import app_config
from byoeb.models.message_category import MessageCategory
from byoeb.models.consensus import Consensus

logger = logging.getLogger(__name__)

EXPERT_TYPE = "anm"
CONSENSUS = "consensus"
CONSENSUS_SEND_LIMIT = 30
max_last_active_duration_seconds: int = app_config["app"]["max_last_active_duration_seconds"]

def create_expert_consensus_message(
    message: ByoebMessageContext,
    expert_user: User
) -> ByoebMessageContext:
    
    expert_phone_number_id = expert_user.phone_number_id
    expert_user_id = expert_user.user_id
    expert_language = expert_user.user_language
    if expert_language == "en":
        question = message.reply_context.reply_english_text
    else:
        question = message.reply_context.reply_source_text
    consensus_header = bot_config["template_messages"]["expert"]["consensus"]["header"][expert_language]
    consensus_footer = bot_config["template_messages"]["expert"]["consensus"]["footer"][expert_language]
    additional_info = {
        "template_name": bot_config["template_messages"]["expert"]["consensus"]["template_name"],
        "template_language": expert_language,  
        "template_parameters": [question]
    }
    expert_message = consensus_header + "\n" + question + "\n\n" + consensus_footer
    new_expert_verification_message = ByoebMessageContext(
        channel_type=message.channel_type,
        message_category=MessageCategory.BOT_TO_EXPERT_CONSENSUS.value,
        user=User(
            user_id=expert_user_id,
            user_type=expert_user.user_type,
            user_language=expert_language,
            phone_number_id=expert_phone_number_id
        ),
        message_context=MessageContext(
            message_type=MessageTypes.REGULAR_TEXT.value,
            message_source_text=expert_message,
            message_english_text=expert_message,
            additional_info=additional_info
        ),
        incoming_timestamp=message.incoming_timestamp,
    )
    return new_expert_verification_message

def create_user_db_queries(cross_convs: List[ByoebMessageContext]):
    user_db_queries_list = []
    for cross_conv in cross_convs:
        user_id = cross_conv.user.user_id
        message_id = cross_conv.message_context.message_id
        update_data = {"$set": {"User.last_conversations": [{"message_id": message_id}]}}
        user_db_queries_list.append(({"_id": user_id}, update_data))
    user_db_queries = {
        constants.UPDATE: user_db_queries_list
    }
    return user_db_queries

def create_message_db_queries(
    cross_convs: List[ByoebMessageContext],
    user_message: ByoebMessageContext,
    message_db_service: MessageMongoDBService,
):
    message_db_create_queries = {
        constants.CREATE: message_db_service.message_create_queries(cross_convs),
        constants.UPDATE: message_db_service.consensus_update_query(user_message, cross_convs)
    }
    return message_db_create_queries

async def is_active_user(user_db_service: UserMongoDBService, user_id: str):
    user_timestamp, cached = await user_db_service.get_user_activity_timestamp(user_id)
    last_active_duration_seconds = chat_utils.get_last_active_duration_seconds(user_timestamp)
    logger.debug("Cached=%s", cached)
    if last_active_duration_seconds >= max_last_active_duration_seconds and cached:
        logger.info("Invalidating cache for user_id=%s", user_id)
        await user_db_service.invalidate_user_cache(user_id)
        user_timestamp, cached = await user_db_service.get_user_activity_timestamp(user_id)
        logger.debug("Cached after invalidate=%s", cached)
        last_active_duration_seconds = chat_utils.get_last_active_duration_seconds(user_timestamp)
        logger.debug("Last active duration=%s", last_active_duration_seconds)
    if last_active_duration_seconds >= max_last_active_duration_seconds:
        return False
    return True

async def send_pending_query_to_expert(
    whatsapp_service: WhatsAppService,
    message: ByoebMessageContext,
    experts: List[User],
    user_db_service: UserMongoDBService,
    message_db_service: MessageMongoDBService
):
    consensus_info = message.message_context.additional_info.get(CONSENSUS, None)
    consensus_list = []
    if consensus_info is not None:
        for consensus in consensus_info:
            consensus_list.append(Consensus(**consensus))
    if len(consensus_list) == CONSENSUS_SEND_LIMIT:
        return None
    consensus_user_ids = {consensus.user_id for consensus in consensus_list}
    filtered_experts = [expert for expert in experts if expert.user_id not in consensus_user_ids]
    selected_experts = filtered_experts[:10]
    logger.debug("Selected experts=%s", selected_experts)
    cross_convs = []
    for expert in selected_experts:
        expert_message = create_expert_consensus_message(message, expert)
        # expert_messages.append(expert_message)
        active_user = await is_active_user(user_db_service, expert_message.user.user_id)
        logger.debug("Active user=%s", active_user)
        expert_requests = whatsapp_service.prepare_requests(expert_message)
        text_message = expert_requests[0]
        template_verification_message = expert_requests[1]
        
        if not active_user:
            expert_message.message_context.message_type = MessageTypes.TEMPLATE_TEXT.value
            responses, message_ids = await whatsapp_service.send_requests([template_verification_message])
        else:
            responses, message_ids = await whatsapp_service.send_requests([text_message])
        expert_message.message_context.additional_info = None
        consensus_cross_conv = whatsapp_service.create_consensus_cross_conv(
            message,
            expert_message,
            responses[0]
        )
        cross_convs.append(consensus_cross_conv)
        
    user_db_queries = create_user_db_queries(cross_convs)
    message_db_queries = create_message_db_queries(
        cross_convs,
        message,
        message_db_service
    )
    await user_db_service.execute_queries(user_db_queries)
    await message_db_service.execute_queries(message_db_queries)
    

async def send_pending_queries_to_expert(
    user_db_service: UserMongoDBService,
    message_db_service: MessageMongoDBService,
    whatsapp_service: WhatsAppService
):
    waiting_status = constants.WAITING
    experts = await user_db_service.get_users_by_type(EXPERT_TYPE)
    experts.sort(key=lambda expert: expert.activity_timestamp or 0, reverse=True)
    # experts = [expert for expert in experts if expert.phone_number_id == "918904954952"]
    async for message in message_db_service.get_bot_messages_by_status(waiting_status):
        # if message.user.phone_number_id != "918837701828":
        #     continue
        # print(message.reply_context.reply_id)
        await send_pending_query_to_expert(
            whatsapp_service,
            message,
            experts,
            user_db_service,
            message_db_service
        )

async def main():
    from byoeb.chat_app.configuration.dependency_setup import (
        channel_client_factory,
        user_db_service,
        message_db_service,
        whatsapp_service
    )
    print(threading.get_ident())
    print("PID:", os.getpid())
    await send_pending_queries_to_expert(user_db_service, message_db_service, whatsapp_service)
    await channel_client_factory.close()

if __name__ == "__main__":
    logger.info("start thread_id=%s pid=%s", threading.get_ident(), os.getpid())
    asyncio.run(main())
    logger.info("end thread_id=%s pid=%s", threading.get_ident(), os.getpid())
    sys.exit(0)