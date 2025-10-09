import asyncio
import byoeb.services.chat.constants as constants
import byoeb.utils.utils as utils
from byoeb.models.message_category import MessageCategory
from datetime import datetime, timezone
from byoeb.chat_app.configuration.config import app_config
from byoeb.services.chat import utils as chat_utils
from byoeb.services.chat import mocks
from typing import Any, Dict, List
from byoeb_core.models.byoeb.message_context import ByoebMessageContext, MessageTypes
from byoeb.services.channel.base import BaseChannelService, MessageReaction
from byoeb.services.databases.mongo_db import UserMongoDBService, MessageMongoDBService
from byoeb.services.chat.message_handlers.base import Handler
from byoeb.services.channel.base import MessageReaction
from byoeb.chat_app.configuration.dependency_setup import app_insights_logger

class ByoebUserSendResponse(Handler):
    __max_last_active_duration_seconds: int = app_config["app"]["max_last_active_duration_seconds"]
    __reaction_enabled: bool = app_config["channel"]["reaction"]["enabled"]

    def __init__(
        self,
        user_db_service: UserMongoDBService,
        message_db_service: MessageMongoDBService,
    ):
        self._user_db_service = user_db_service
        self._message_db_service = message_db_service

    def get_channel_service(
        self,
        channel_type
    ) -> BaseChannelService:
        from byoeb.chat_app.configuration.dependency_setup import channel_client_factory
        if channel_type == "whatsapp":
            from byoeb.services.channel.whatsapp import WhatsAppService
            return WhatsAppService(channel_client_factory)
        return None

    def __prepare_db_queries(
        self,
        convs: List[ByoebMessageContext],
        byoeb_user_message: ByoebMessageContext,
    ):
        # Safety check for None user message
        if byoeb_user_message is None:
            print("[send] __prepare_db_queries: byoeb_user_message is None, returning empty queries")
            return {}
        if byoeb_user_message.reply_context.message_category == MessageCategory.AUDIO_IDK.value:
            message_db_queries = {
                constants.UPDATE: self._message_db_service.audio_idk_status_update_query(byoeb_user_message)
            }
        else:
            message_db_queries = {
                constants.CREATE: self._message_db_service.message_create_queries(convs)
            }
        audio_message_id = None
        text_message_id = None
        user_convs = chat_utils.get_user_byoeb_messages(convs)
        for user_conv in user_convs:
            if user_conv.message_context.message_type == MessageTypes.REGULAR_AUDIO.value:
                audio_message_id = user_conv.message_context.message_id
            else:
                text_message_id = user_conv.message_context.message_id
        qa = {
            constants.AUDIO_MESSAGE_ID: audio_message_id,
            constants.TEXT_MESSAGE_ID: text_message_id,
            constants.TIMESTAMP: str(int(datetime.now(timezone.utc).timestamp())),
            constants.QUESTION: byoeb_user_message.reply_context.reply_english_text,
            constants.ANSWER: byoeb_user_message.message_context.message_english_text
        }
        if utils.is_idk(byoeb_user_message.message_context.message_english_text):
            qa = None
        user_db_queries = {
            constants.UPDATE: [self._user_db_service.user_activity_update_query(byoeb_user_message.user, qa)]
        }
        return {
            constants.MESSAGE_DB_QUERIES: message_db_queries,
            constants.USER_DB_QUERIES: user_db_queries
        }
        
    async def is_active_user(self, user_id: str):
        user_timestamp, cached = await self._user_db_service.get_user_activity_timestamp(user_id)
        last_active_duration_seconds = chat_utils.get_last_active_duration_seconds(user_timestamp)
        print("Last active duration", last_active_duration_seconds)
        print("Cached", cached)
        if last_active_duration_seconds >= self.__max_last_active_duration_seconds and cached:
            print("Invalidating cache")
            await self._user_db_service.invalidate_user_cache(user_id)
            user_timestamp, cached = await self._user_db_service.get_user_activity_timestamp(user_id)
            print("Cached", cached)
            last_active_duration_seconds = chat_utils.get_last_active_duration_seconds(user_timestamp)
            print("Last active duration", last_active_duration_seconds)
        if last_active_duration_seconds >= self.__max_last_active_duration_seconds:
            return False
        return True
    
    async def __handle_expert(
        self,
        channel_service: BaseChannelService,
        expert_message_context: ByoebMessageContext
    ):
        # responses = [
        #     mocks.get_mock_whatsapp_response(expert_message_context.user.phone_number_id)
        # ]
        # return responses
        if expert_message_context is None:
            return []
        is_active_user = await self.is_active_user(expert_message_context.user.user_id)
        expert_requests = channel_service.prepare_requests(expert_message_context)
        interactive_button_message = expert_requests[0]
        template_verification_message = expert_requests[1]
        
        if not is_active_user:
            expert_message_context.message_context.message_type = MessageTypes.TEMPLATE_BUTTON.value
            responses, message_ids = await channel_service.send_requests([template_verification_message])
        else:
            responses, message_ids = await channel_service.send_requests([interactive_button_message])
        print("responses", responses)
        pending_emoji = expert_message_context.message_context.additional_info.get(constants.EMOJI)
        message_reactions = [
            MessageReaction(
                reaction=pending_emoji,
                message_id=message_id,
                phone_number_id=expert_message_context.user.phone_number_id
            )
            for message_id in message_ids if message_id is not None
        ]

        reaction_requests = channel_service.prepare_reaction_requests(message_reactions)
        await channel_service.send_requests(reaction_requests)
        return responses

    async def __handle_user(
        self,
        channel_service: BaseChannelService,
        user_message_context: ByoebMessageContext
    ):
        # responses = [
        #     mocks.get_mock_whatsapp_response(user_message_context.user.phone_number_id)
        # ]
        # return responses
        message_ids = []
        responses = []
        user_requests = channel_service.prepare_requests(user_message_context)
        if (user_message_context.message_context.message_type == MessageTypes.REGULAR_AUDIO.value
            and len(user_requests) == 2
        ):
            user_message_copy = user_message_context.__deepcopy__()
            user_message_copy.reply_context = None
            user_requests_no_tag = channel_service.prepare_requests(user_message_copy)
            text_tag_message = user_requests[0]
            audio_no_tag_message = user_requests_no_tag[1]
            start_time = datetime.now(timezone.utc).timestamp()
            response_text, message_id_text = await channel_service.send_requests([text_tag_message])
            end_time = datetime.now(timezone.utc).timestamp()
            utils.log_to_text_file(f"Successfully sent interactive message in {end_time - start_time} seconds")
            start_time = datetime.now(timezone.utc).timestamp()
            response_audio, message_id_audio = await channel_service.send_requests([audio_no_tag_message])
            end_time = datetime.now(timezone.utc).timestamp()
            utils.log_to_text_file(f"Successfully sent audio message in {end_time - start_time} seconds. Audio id: {message_id_audio, response_audio}")
            responses = response_text
            message_ids = message_id_text
        elif ((user_message_context.message_context.message_type == MessageTypes.INTERACTIVE_LIST.value
            or user_message_context.message_context.message_type == MessageTypes.INTERACTIVE_BUTTON.value)
            and len(user_requests) == 2
        ):
            user_message_copy = user_message_context.__deepcopy__()
            user_message_copy.reply_context = None
            user_requests_no_tag = channel_service.prepare_requests(user_message_copy)
            text_tag_message = user_requests[0]
            audio_no_tag_message = user_requests_no_tag[1]
            start_time = datetime.now(timezone.utc).timestamp()
            response_text, message_id_text = await channel_service.send_requests([text_tag_message])
            end_time = datetime.now(timezone.utc).timestamp()
            utils.log_to_text_file(f"Successfully sent interactive message in {end_time - start_time} seconds")
            start_time = datetime.now(timezone.utc).timestamp()
            response_audio, message_id_audio = await channel_service.send_requests([audio_no_tag_message])
            end_time = datetime.now(timezone.utc).timestamp()
            utils.log_to_text_file(f"Successfully sent audio message in {end_time - start_time} seconds. Audio id: {message_id_audio, response_audio}")
            responses = response_text
            message_ids = message_id_text
        elif user_message_context.message_context.message_type == MessageTypes.REGULAR_TEXT.value or len(user_requests) == 1:
            response, message_id = await channel_service.send_requests(user_requests)
            responses = response
            message_ids = message_id
        
        print("user responses", responses)
        if self.__reaction_enabled:
            pending_emoji = user_message_context.message_context.additional_info.get(constants.EMOJI)
            message_reactions = [
                MessageReaction(
                    reaction=pending_emoji,
                    message_id=message_id,
                    phone_number_id=user_message_context.user.phone_number_id
                )
                for message_id in message_ids if message_id is not None
            ]
            reaction_requests = channel_service.prepare_reaction_requests(message_reactions)
            await channel_service.send_requests(reaction_requests)
        return responses
    
    async def __handle_message_send_workflow(
        self,
        messages: List[ByoebMessageContext]
    ):
        # verification_status = constants.VERIFICATION_STATUS
        print("[send] __handle_message_send_workflow: start messages_count=", len(messages) if messages else 0)
        read_receipt_messages = chat_utils.get_read_receipt_byoeb_messages(messages)
        print("[send] read_receipt_messages_count=", len(read_receipt_messages) if read_receipt_messages else 0)
        byoeb_user_messages = chat_utils.get_user_byoeb_messages(messages)
        print("byoeb_user_messages", byoeb_user_messages)
        print("[send] byoeb_user_messages_count=", len(byoeb_user_messages) if byoeb_user_messages else 0)

        if not byoeb_user_messages:
            print("[send] ERROR: No user messages found, cannot proceed")
            return [], None
        byoeb_user_message = byoeb_user_messages[0]
        print("[send] byoeb_user_message_type=", type(byoeb_user_message).__name__)
        print("[send] about to access reply_context on byoeb_user_message")

        track_message_id = byoeb_user_message.reply_context.reply_id
        print("[send] track_message_id(initial)=", track_message_id)
        if byoeb_user_message.reply_context.message_category == MessageCategory.AUDIO_IDK.value:
            print("[send] AUDIO_IDK detected; using TRACK_MESSAGE_ID from additional_info")
            track_message_id = byoeb_user_message.message_context.additional_info.get(constants.TRACK_MESSAGE_ID)
        print("[send] track_message_id(final)=", track_message_id)

        start_time = datetime.now(timezone.utc).timestamp()
        print("[send] start_time=", start_time)
        print("[send] channel_type=", getattr(byoeb_user_message, "channel_type", None))
        channel_service = self.get_channel_service(byoeb_user_message.channel_type)
        print("[send] channel_service_resolved=", type(channel_service).__name__ if channel_service else None)
        print("[send] scheduling amark_read for", len(read_receipt_messages) if read_receipt_messages else 0, "messages")
        mark_read_task = channel_service.amark_read(read_receipt_messages)
        print("[send] scheduling user_task")
        user_task = self.__handle_user(channel_service, byoeb_user_message)

        byoeb_expert_messages = chat_utils.get_expert_byoeb_messages(messages)
        print("[send] byoeb_expert_messages_count=", len(byoeb_expert_messages) if byoeb_expert_messages else 0)
        if byoeb_expert_messages is None or len(byoeb_expert_messages) == 0:
            byoeb_expert_message = None
            print("[send] byoeb_expert_message=None")
        else:
            byoeb_expert_message = byoeb_expert_messages[0]
            print("[send] byoeb_expert_message set (index 0)")

        print("[send] scheduling expert_task")
        expert_task = self.__handle_expert(channel_service, byoeb_expert_message)

        print("[send] awaiting asyncio.gather for mark_read_task, user_task, expert_task")
        _, user_responses, expert_responses = await asyncio.gather(mark_read_task, user_task, expert_task)
        print("[send] gather done; user_responses_len=", len(user_responses) if user_responses else 0, "expert_responses_len=", len(expert_responses) if expert_responses else 0)

        # byoeb_user_verification_status = byoeb_expert_message.message_context.additional_info.get(verification_status)
        print("[send] extracting additional_info fields (ROW_TEXTS, QUERY_TYPE, STATUS)")
        related_questions = byoeb_user_message.message_context.additional_info.get(constants.ROW_TEXTS)
        query_type = byoeb_user_message.message_context.additional_info.get(constants.QUERY_TYPE)
        status = byoeb_user_message.message_context.additional_info.get(constants.STATUS)
        print("[send] related_questions=", related_questions)
        print("[send] query_type=", query_type)
        print("[send] status=", status)

        byoeb_user_message.message_context.additional_info = {
            # verification_status: byoeb_user_verification_status,
            constants.RELATED_QUESTIONS: related_questions,
            constants.QUERY_TYPE: query_type,
            constants.STATUS: status
        }
        print("[send] creating bot_to_user_convs via channel_service.create_conv")
        bot_to_user_convs = channel_service.create_conv(
            byoeb_user_message,
            user_responses
        )
        print("[send] bot_to_user_convs_count=", len(bot_to_user_convs) if bot_to_user_convs else 0)

        end_time = datetime.now(timezone.utc).timestamp()
        print("[send] end_time=", end_time, "duration=", end_time - start_time)
        app_insights_logger.add_log(
            event_name="message_send_workflow",
            details={
                "message_id": track_message_id,
                "time_taken": end_time - start_time
            }
        )
        print("[send] app_insights_logger.add_log called for message_send_workflow")

        # byoeb_expert_verification_status = byoeb_expert_message.message_context.additional_info.get(verification_status)
        # byoeb_expert_message.message_context.additional_info = {
        #     verification_status: byoeb_expert_verification_status
        # }
        # bot_to_expert_cross_convs = channel_service.create_cross_conv(
        #     byoeb_user_message,
        #     byoeb_expert_message,
        #     user_responses,
        #     expert_responses
        # )
        # return bot_to_user_convs + bot_to_expert_cross_convs, byoeb_user_message
        print("[send] returning from __handle_message_send_workflow")
        return bot_to_user_convs, byoeb_user_message
    
    async def handle(
        self,
        messages: List[ByoebMessageContext]
    ) -> Dict[str, Any]:
        if messages is None or len(messages) == 0:
            return {}
        try:
            start_time = datetime.now(timezone.utc).timestamp()
            convs, byoeb_user_message = await self.__handle_message_send_workflow(messages)
            # Check if we have a valid user message to process
            if byoeb_user_message is None:
                print("[send] No user message to process, returning empty queries")
                return {}
            db_queries = self.__prepare_db_queries(convs, byoeb_user_message)
            end_time = datetime.now(timezone.utc).timestamp()
            utils.log_to_text_file(f"E2E for send workflow {end_time - start_time} seconds")
            return db_queries
        except Exception as e:
            utils.log_to_text_file(f"Error in sending message to user and expert: {str(e)}")
            raise e