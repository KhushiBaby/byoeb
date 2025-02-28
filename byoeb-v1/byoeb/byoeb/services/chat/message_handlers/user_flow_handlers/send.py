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
            audio_tag_message = user_requests[1]
            text_no_tag_message = user_requests_no_tag[0]
            response_audio, message_id_audio = await channel_service.send_requests([audio_tag_message])
            response_text, message_id_text = await channel_service.send_requests([text_no_tag_message])
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
            utils.log_to_text_file(f"Successfully sent audio message in {end_time - start_time} seconds")
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
        read_receipt_messages = chat_utils.get_read_receipt_byoeb_messages(messages)
        byoeb_user_messages = chat_utils.get_user_byoeb_messages(messages)
        byoeb_user_message = byoeb_user_messages[0]
        track_message_id = byoeb_user_message.reply_context.reply_id
        if byoeb_user_message.reply_context.message_category == MessageCategory.AUDIO_IDK.value:
            track_message_id = byoeb_user_message.message_context.additional_info.get(constants.TRACK_MESSAGE_ID)
        start_time = datetime.now(timezone.utc).timestamp()
        channel_service = self.get_channel_service(byoeb_user_message.channel_type)
        mark_read_task = channel_service.amark_read(read_receipt_messages)
        user_task = self.__handle_user(channel_service, byoeb_user_message)
        byoeb_expert_messages = chat_utils.get_expert_byoeb_messages(messages)
        if byoeb_expert_messages is None or len(byoeb_expert_messages) == 0:
            byoeb_expert_message = None
        else:
            byoeb_expert_message = byoeb_expert_messages[0]
        expert_task = self.__handle_expert(channel_service, byoeb_expert_message)
        _, user_responses, expert_responses = await asyncio.gather(mark_read_task, user_task, expert_task)

        # byoeb_user_verification_status = byoeb_expert_message.message_context.additional_info.get(verification_status)
        related_questions = byoeb_user_message.message_context.additional_info.get(constants.ROW_TEXTS)
        query_type = byoeb_user_message.message_context.additional_info.get(constants.QUERY_TYPE)
        status = byoeb_user_message.message_context.additional_info.get(constants.STATUS)
        byoeb_user_message.message_context.additional_info = {
            # verification_status: byoeb_user_verification_status,
            constants.RELATED_QUESTIONS: related_questions,
            constants.QUERY_TYPE: query_type,
            constants.STATUS: status
        }
        bot_to_user_convs = channel_service.create_conv(
            byoeb_user_message,
            user_responses
        )
        end_time = datetime.now(timezone.utc).timestamp()
        app_insights_logger.add_log(
            event_name="message_send_workflow",
            details={
                "message_id": track_message_id,
                "time_taken": end_time - start_time
            }
        )
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
            db_queries = self.__prepare_db_queries(convs, byoeb_user_message)
            end_time = datetime.now(timezone.utc).timestamp()
            utils.log_to_text_file(f"E2E for send workflow {end_time - start_time} seconds")
            return db_queries
        except Exception as e:
            utils.log_to_text_file(f"Error in sending message to user and expert: {str(e)}")
            raise e