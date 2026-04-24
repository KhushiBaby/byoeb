import asyncio
import base64
import re
import os
import sys
import threading
import datetime
import logging
from zoneinfo import ZoneInfo
from byoeb.services.chat import constants
from byoeb.services.databases.mongo_db.message_db import MessageMongoDBService
from byoeb.services.databases.mongo_db.user_db import UserMongoDBService
from typing import List, Optional
from byoeb_core.models.byoeb.message_context import ByoebMessageContext
from byoeb.services.channel.whatsapp import WhatsAppService
from byoeb_core.models.byoeb.message_context import (
    ByoebMessageContext,
    MessageContext,
    MessageTypes
)
from byoeb.background_jobs.consensus.config import bot_config
from byoeb.chat_app.configuration.config import app_config
from byoeb.models.message_category import MessageCategory
from byoeb.models.consensus import Consensus
from byoeb.background_jobs.consensus.consensus_prompt import consensus_prompt

logger = logging.getLogger(__name__)

# curr_dir = os.path.dirname(os.path.abspath(__file__))
# consensus_prompt_path = os.path.join(curr_dir, "consensus_prompt.txt")
# with open(consensus_prompt_path, "r") as f:
#     consensus_prompt = f.read()

EXPERT_TYPE = "anm"
CONSENSUS = "consensus"
CONSENSUS_THRESHOLD = 1
TIMEZONE = ZoneInfo("Asia/Kolkata")
max_last_active_duration_seconds: int = app_config["app"]["max_last_active_duration_seconds"]

def consensus_timeout(
    timestamp: float,
):
    timeout_duration = 60 * 60 * 4.5
    current_timestamp = datetime.datetime.now(datetime.timezone.utc).timestamp()
    if current_timestamp - timestamp > timeout_duration:
        return True
    return False

async def create_user_db_queries(
    message: ByoebMessageContext,
    user_db_service: UserMongoDBService,
    user_id: str,
    consensus_en_response: str
):
    message_id = message.message_context.message_id
    user = (await user_db_service.get_users([user_id]))[0]
    qa = {
            constants.TEXT_MESSAGE_ID: message_id,
            constants.TIMESTAMP: datetime.datetime.now(datetime.timezone.utc),
            constants.QUESTION: message.reply_context.reply_source_text,
            constants.QUESTION_REWRITTEN: message.reply_context.reply_english_text,
            constants.ANSWER: consensus_en_response
        }
    user_db_query = user_db_service.user_activity_update_query(user, qa, skip_timestamp=True)
    user_db_queries = {
        constants.UPDATE: [user_db_query]
    }
    return user_db_queries

def create_message_db_queries(
    has_consensus: bool,
    message: ByoebMessageContext,
    message_db_service: MessageMongoDBService,
    consensus_response: Optional[str] = None,
    consensus_en_response: Optional[str] = None,
):
    if has_consensus:
        message.message_context.additional_info[constants.STATUS] = constants.RESOLVED
        message.message_context.additional_info[constants.CONSENSUS_ANSWER_EN] = consensus_en_response
        message.message_context.additional_info[constants.CONSENSUS_ANSWER_SOURCE] = consensus_response
    else:
        message.message_context.additional_info[constants.STATUS] = constants.TIMEOUT
    status_update_query = message_db_service.idk_status_update_query(message)

    message_db_queries = {
        constants.UPDATE: message_db_service.consensus_update_query(message, []) + [status_update_query]
    }
    return message_db_queries    

async def create_user_message(
    message: ByoebMessageContext,
    response: str,
) -> ByoebMessageContext:
    from byoeb.chat_app.configuration.dependency_setup import speech_translator
    
    user_language = message.user.user_language
    translated_audio_message = await speech_translator.atext_to_speech(
        input_text=response,
        source_language=user_language,
        test_user=message.user.test_user
    )
    related_questions = message.message_context.additional_info.get(constants.RELATED_QUESTIONS)
    description = bot_config["template_messages"]["user"]["follow_up_questions_description"][user_language]
    return ByoebMessageContext(
        message_category=MessageCategory.BOT_TO_USER.value,
        user=message.user,
        channel_type=message.channel_type,
        message_context=MessageContext(
            message_type=MessageTypes.INTERACTIVE_LIST.value,
            message_source_text=response,
            additional_info={
                constants.DESCRIPTION: description,
                constants.ROW_TEXTS: related_questions,
                constants.DATA: base64.b64encode(translated_audio_message).decode("utf-8"),
                constants.MIME_TYPE: "audio/ogg",
            }
        ),
        reply_context=message.reply_context
    )
async def send_consensus_response(
    whatsapp_service: WhatsAppService,
    user_message: ByoebMessageContext,
):
    user_requests = whatsapp_service.prepare_requests(user_message)
    user_message_copy = user_message.__deepcopy__()
    user_message_copy.reply_context = None
    user_requests_no_tag = whatsapp_service.prepare_requests(user_message_copy)
    audio_no_tag_message = user_requests_no_tag[1]
    text_tag_message = user_requests[0]
    response_text, message_id_text = await whatsapp_service.send_requests([text_tag_message])
    response_audio, message_id_audio = await whatsapp_service.send_requests([audio_no_tag_message])
    responses = response_text
    message_ids = message_id_text
    return message_ids

async def agenerate_consensus_response(
    query: str,
    responses: List[str],
    lang_code: str = "en",
):
    consensus_not_found = "Consensus not reached"
    def parse_struct(text):
        pattern = r"<(.*?)>(.*?)</\1>"
        matches = re.findall(pattern, text, re.DOTALL)
        return {key: value.strip() for key, value in matches}
    from byoeb.chat_app.configuration.dependency_setup import text_translator, llm_client
    logger.debug("Query=%s", query)
    logger.debug("Responses=%s", responses)
    prompt = [
        {"role": "system", "content": str(consensus_prompt)},
    ]

    query_prompt = f'''
    Please find the consensus for the following input:
    q: {query}
    anm_answers: [{", ".join(responses)}]
    convert the <consensus_answer> to user language based on lang code {lang_code}
    as <consensus_answer_source>
    Share the output in following stucutre 
    <BEGIN_STRUCT>
    <anm_votes>xxx</anm_votes>
    <consensus_explanation>xxx</consensus_explanation>
    <consensus_answer>xxx</consensus_answer>
    <consensus_answer_source>xxx converted to user language</consensus_answer_source>
    <END_STRUCT>, do not include anything else.
    '''
    prompt.append({"role": "user", "content": str(query_prompt)})
    resp, text = await llm_client.generate_response(prompt)
    parsed_response = parse_struct(text)
    logger.debug("Parsed response=%s", parsed_response)
    consensus_answer = parsed_response.get("consensus_answer")
    consensus_answer_source = parsed_response.get("consensus_answer_source")
    if consensus_not_found in consensus_answer:
        return consensus_answer, consensus_answer_source, False
    return consensus_answer, consensus_answer_source, True

async def process_consensus_responses(
    message: ByoebMessageContext,
    message_db_service: MessageMongoDBService,
    user_db_service: UserMongoDBService,
    whatsapp_service: WhatsAppService
) -> bool:
    """
    Process consensus responses for a message.
    Returns True if consensus was found and sent to user, False otherwise.
    """
    consensus_info_list = message.message_context.additional_info.get(constants.CONSENSUS, None)
    if not consensus_info_list:
        return False
    expert_consensus_message_ids = []
    for consensus_info in consensus_info_list:
        consensus = Consensus(**consensus_info)
        expert_consensus_message_ids.append(consensus.message_id)
    expert_consensus_messages = await message_db_service.get_bot_messages_by_ids(expert_consensus_message_ids)
    response_messages = []
    updated_consensus_list = []
    for expert_consensus_message, consensus_info in zip(expert_consensus_messages, consensus_info_list):
        consensus = Consensus(**consensus_info)
        if not isinstance(expert_consensus_message, ByoebMessageContext):
            continue
        response = expert_consensus_message.message_context.additional_info.get(constants.CORRECTION_SOURCE)
        if response:  # Only add non-empty responses
            response_messages.append(response)
        consensus.status = constants.RESOLVED
        updated_consensus_list.append(consensus.model_dump())
    message.message_context.additional_info[constants.CONSENSUS] = updated_consensus_list
    if len(response_messages) < CONSENSUS_THRESHOLD:
        return False
    consensus_en_response, consensus_response, has_consensus = await agenerate_consensus_response(
        message.reply_context.reply_english_text,
        response_messages,
        message.user.user_language
    )
    if not has_consensus:
        return False
    user_message = await create_user_message(message, consensus_response)
    message_ids = await send_consensus_response(whatsapp_service, user_message)
    user_db_queries = await create_user_db_queries(message, user_db_service, user_message.user.user_id, consensus_en_response)
    message_db_queries = create_message_db_queries(has_consensus, message, message_db_service, consensus_response)
    await user_db_service.execute_queries(user_db_queries)
    await message_db_service.execute_queries(message_db_queries)
    return True
    

async def process_queries_consensus(
    message_db_service: MessageMongoDBService,
    user_db_service: UserMongoDBService,
    whatsapp_service: WhatsAppService
):
    """
    Process queries waiting for consensus responses.
    Only sends messages during business hours (8 AM - 8 PM IST).
    Only sends timeout messages if no consensus answer was found.
    """
    waiting_status = constants.WAITING
    current_time = datetime.datetime.now(TIMEZONE)
    current_hour = current_time.hour
    
    # Only process during business hours (8 AM - 8 PM IST)
    if current_hour < 8 or current_hour >= 20:
        return
    
    async for message in message_db_service.get_bot_messages_by_status(waiting_status):
        # Check current status to avoid processing messages that are already resolved or timed out
        current_status = message.message_context.additional_info.get(constants.STATUS)
        if current_status in [constants.RESOLVED, constants.TIMEOUT]:
            # Skip messages that are already resolved or timed out
            continue
        
        # First, try to process consensus responses
        # This will return True if consensus was found and sent
        consensus_found = await process_consensus_responses(
            message, message_db_service, user_db_service, whatsapp_service
        )
        # Only send timeout message if:
        # 1. Message has timed out (>4.5 hours)
        # 2. No consensus answer was found
        # If no consensus was found and message has timed out, mark it as TIMEOUT
        # but do NOT send another message to the user (they already received IDK message initially)
        if consensus_timeout(message.outgoing_timestamp) and not consensus_found:
            # Double-check status hasn't changed (race condition protection)
            current_status = message.message_context.additional_info.get(constants.STATUS)
            if current_status == constants.WAITING:
                # Just update the status to TIMEOUT without sending any message to user
                # The user already received the initial IDK message, no need to send timeout message
                message_db_queries = create_message_db_queries(False, message, message_db_service)
                await message_db_service.execute_queries(message_db_queries)

async def main():
    from byoeb.chat_app.configuration.dependency_setup import (
        channel_client_factory,
        message_db_service,
        user_db_service,
        whatsapp_service
    )
    logger.info("main started thread_id=%s pid=%s", threading.get_ident(), os.getpid())
    whatsapp_service = WhatsAppService(channel_client_factory)
    await process_queries_consensus(message_db_service, user_db_service, whatsapp_service)
    await channel_client_factory.close()

if __name__ == "__main__":
    logger.info("start thread_id=%s pid=%s", threading.get_ident(), os.getpid())
    asyncio.run(main())
    logger.info("end thread_id=%s pid=%s", threading.get_ident(), os.getpid())
    sys.exit(0)