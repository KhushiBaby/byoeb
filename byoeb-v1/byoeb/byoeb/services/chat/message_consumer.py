import logging
import asyncio
import json
import hashlib
import traceback
import byoeb.utils.utils as utils
import byoeb.services.chat.constants as constants
from datetime import datetime, timezone
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from byoeb.models.message_category import MessageCategory
from byoeb.factory import ChannelClientFactory
from byoeb.chat_app.configuration.config import bot_config
from byoeb_core.models.byoeb.user import User
from byoeb_core.models.byoeb.message_context import ReplyContext
from byoeb.services.databases.mongo_db import UserMongoDBService, MessageMongoDBService
from byoeb_core.models.byoeb.message_context import ByoebMessageContext
from byoeb.chat_app.configuration.dependency_setup import app_insights_logger
from byoeb.services.user.onboarding import handle_unknown_user

class Conversation(BaseModel):
    user_message: Optional[ByoebMessageContext]
    bot_message: Optional[ByoebMessageContext]
    user: User

class MessageConsmerService:

    __timeout_seconds = 180
    def __init__(
        self,
        config,
        user_db_service: UserMongoDBService,
        message_db_service: MessageMongoDBService,
        channel_client_factory: ChannelClientFactory
    ):
        self._config = config
        self._logger = logging.getLogger(self.__class__.__name__)
        self._user_db_service = user_db_service
        self._message_db_service = message_db_service
        self._channel_client_factory = channel_client_factory
        self._regular_user_type = bot_config["regular"]["user_type"]
        self._expert_user_types = bot_config["expert"]

    # TODO: Hash can be used or better way to get user by phone number
    def __get_user(
        self,
        users: List[User],
        phone_number_id,

    ) -> User:
        return next((user for user in users if user.phone_number_id == phone_number_id), None)
    
    def __is_expert_user_type(
        self,
        user_type: str
    ):
        if user_type in self._expert_user_types.values():
            return True
        return False
            
    
    def __get_bot_message(
        self,
        messages: List[ByoebMessageContext],
        reply_id
    ) -> ByoebMessageContext:
        return next(
            (
                message for message in messages
                if reply_id is not None
                and message.message_context.message_id == reply_id
            ),
            None
        )
    
    async def __create_conversations(
        self,
        messages: List[ByoebMessageContext]
    ) -> List[ByoebMessageContext]:
        print("[__create_conversations] ▶ start")
        conversations = []
        onboard_convs = []
        start_time = datetime.now(timezone.utc).timestamp()

        phone_numbers = list(set([m.user.phone_number_id for m in messages]))
        user_ids = list(set([hashlib.md5(number.encode()).hexdigest() for number in phone_numbers]))
        print(f"[__create_conversations] phone_numbers={phone_numbers} user_ids_count={len(user_ids)}")

        byoeb_users = await self._user_db_service.get_users(user_ids)
        print(f"[__create_conversations] fetched_users={len(byoeb_users)}")

        for m in messages:
            user = self.__get_user(byoeb_users, m.user.phone_number_id)
            if user is None:
                print(f"[__create_conversations] user_not_found phone={m.user.phone_number_id} -> onboard")
                onboard_convs.append(m)
                continue

            # minimal peek
            print(f"[__create_conversations] user_found phone={m.user.phone_number_id} user_type={user.user_type} reply_id={m.reply_context.reply_id}")

            if self.__is_expert_user_type(user.user_type) and m.reply_context.reply_id is None:
                # attach last bot message to reply
                if user.last_conversations and len(user.last_conversations) > 0:
                    m.reply_context = ReplyContext(
                        reply_id=user.last_conversations[0].get("message_id")
                    )
                    print(f"[__create_conversations] expert -> set reply_id={m.reply_context.reply_id}")

        bot_message_ids = list(
            set(m.reply_context.reply_id for m in messages if m.reply_context.reply_id is not None)
        )
        print(f"[__create_conversations] bot_message_ids={bot_message_ids}")

        bot_messages = await self._message_db_service.get_bot_messages_by_ids(bot_message_ids)
        print(f"[__create_conversations] fetched_bot_messages={len(bot_messages)}")
        print("bot_messages", bot_messages)

        end_time = datetime.now(timezone.utc).timestamp()

        for m in messages:
            user = self.__get_user(byoeb_users, m.user.phone_number_id)
            if user is None:
                continue

            bot_message = self.__get_bot_message(bot_messages, m.reply_context.reply_id)
            conversation = ByoebMessageContext.model_validate(m)

            if user.user_type in self._regular_user_type:
                conversation.message_category = MessageCategory.USER_TO_BOT.value
            elif self.__is_expert_user_type(user.user_type):
                conversation.message_category = MessageCategory.EXPERT_TO_BOT.value

            conversation.user = user
            app_insights_logger.add_log(
                event_name="create_conversations",
                details={
                    "message_id": m.message_context.message_id,
                    "time_taken": end_time - start_time
                }
            )

            if bot_message is None:
                if user.user_type is None or user.user_language is None:
                    print(f"[__create_conversations] no_bot_msg + needs_onboarding -> onboard (msg_id={m.message_context.message_id})")
                    onboard_convs.append(m)
                else:
                    print(f"[__create_conversations] no_bot_msg -> conversations (msg_id={m.message_context.message_id})")
                    conversations.append(conversation)
                continue

            # carry over reply context fields
            conversation.reply_context.message_category = bot_message.message_category
            conversation.reply_context.reply_id = bot_message.message_context.message_id
            conversation.reply_context.reply_type = bot_message.message_context.message_type
            conversation.reply_context.reply_source_text = bot_message.message_context.message_source_text
            conversation.reply_context.reply_english_text = bot_message.message_context.message_english_text
            conversation.reply_context.additional_info = bot_message.message_context.additional_info
            conversation.cross_conversation_id = bot_message.cross_conversation_id
            conversation.cross_conversation_context = bot_message.cross_conversation_context

            if bot_message.message_category == MessageCategory.AUDIO_IDK.value:
                # copy more fields for audio-idk
                conversation.reply_context.additional_info[constants.BOT_AUDIO_IDK_MESSAGE_ID] = bot_message.message_context.message_id
                conversation.reply_context.reply_source_text = bot_message.reply_context.reply_source_text
                conversation.reply_context.reply_english_text = bot_message.reply_context.reply_english_text
                conversation.reply_context.reply_id = bot_message.reply_context.reply_id
                conversation.reply_context.media_info = bot_message.reply_context.media_info
                conversation.reply_context.reply_type = bot_message.reply_context.reply_type

            if (bot_message.message_category == constants.USER_TYPE
                or bot_message.message_category == constants.LANGUAGE_SELECTION
                or bot_message.message_category == constants.CONSENT):
                print(f"[__create_conversations] onboarding_flow msg_id={m.message_context.message_id} bot_cat={bot_message.message_category} -> onboard")
                onboard_convs.append(conversation)
            elif user.user_type is None or user.user_language is None:
                print(f"[__create_conversations] missing_user_fields -> onboard (msg_id={m.message_context.message_id})")
                onboard_convs.append(m)
            else:
                print(f"[__create_conversations] regular_flow -> conversations (msg_id={m.message_context.message_id})")
                conversations.append(conversation)

        print(f"[__create_conversations] ◀ end conversations={len(conversations)} onboard={len(onboard_convs)} took={end_time - start_time:.3f}s")
        return conversations, onboard_convs

    async def __process_byoebuser_conversation(self, byoeb_message: ByoebMessageContext):
        print(f"[__process_byoebuser_conversation] ▶ start msg_id={byoeb_message.message_context.message_id}")
        from byoeb.chat_app.configuration.dependency_setup import byoeb_user_process
        byoeb_message_copy = byoeb_message.model_copy(deep=True)
        try:
            queries = await asyncio.wait_for(
                byoeb_user_process.handle([byoeb_message]),
                timeout=self.__timeout_seconds
            )
            print(f"[__process_byoebuser_conversation] ◀ ok queries_count={len(queries) if queries else 0}")
            return queries, byoeb_message_copy, None
        except asyncio.TimeoutError:
            print(f"[__process_byoebuser_conversation] ✖ Timeout after {self.__timeout_seconds}s")
            app_insights_logger.add_log(
                event_name="timeout_error",
                details={
                    "user_type": byoeb_message.user.user_type,
                    "message_id": byoeb_message.message_context.message_id,
                    "message_text": byoeb_message.message_context.message_source_text,
                    "timeout_seconds": self.__timeout_seconds
                }
            )
            return None, byoeb_message_copy, "TimeoutError"
        except Exception as e:
            print(f"[__process_byoebuser_conversation] ✖ error: {e}")
            traceback.print_exc()
            return None, byoeb_message_copy, e

    async def __process_byoebexpert_conversation(
        self,
        byoeb_message: ByoebMessageContext
    ):
        print(f"[__process_byoebexpert_conversation] ▶ start msg_id={byoeb_message.message_context.message_id}")
        from byoeb.chat_app.configuration.dependency_setup import byoeb_expert_process
        byoeb_message_copy = byoeb_message.model_copy(deep=True)
        try:
            queries = await asyncio.wait_for(
                byoeb_expert_process.handle([byoeb_message]),
                timeout=self.__timeout_seconds
            )
            print(f"[__process_byoebexpert_conversation] ◀ ok queries_count={len(queries) if queries else 0}")
            return queries, byoeb_message_copy, None
        except asyncio.TimeoutError:
            print(f"[__process_byoebexpert_conversation] ✖ Timeout after {self.__timeout_seconds}s")
            app_insights_logger.add_log(
                event_name="timeout_error",
                details={
                    "user_type": byoeb_message.user.user_type,
                    "message_id": byoeb_message.message_context.message_id,
                    "message_text": byoeb_message.message_context.message_source_text,
                    "timeout_seconds": self.__timeout_seconds
                }
            )
            return None, byoeb_message_copy, "TimeoutError"
        except Exception as e:
            print(f"[__process_byoebexpert_conversation] ✖ error: {e}")
            traceback.print_exc()
            return None, byoeb_message_copy, e

    async def consume(
        self,
        messages: list
    ) -> List[ByoebMessageContext]:
        print("[consume] ▶ start")
        byoeb_messages: List[ByoebMessageContext] = []

        for raw in messages:
            json_message = json.loads(raw)
            print("raw", raw)
            byoeb_message = ByoebMessageContext.model_validate(json_message)
            byoeb_messages.append(byoeb_message)
        print(f"[consume] parsed_messages={len(byoeb_messages)}")
        print("byoeb_messages", byoeb_messages)

        start_time = datetime.now(timezone.utc).timestamp()
        conversations, onboard_convs = await self.__create_conversations(byoeb_messages)
        end_time = datetime.now(timezone.utc).timestamp()

        print(f"[consume] conversations={len(conversations)} onboard={len(onboard_convs)} create_time={end_time - start_time:.3f}s")

        # onboarding first (if any)
        if onboard_convs is not None and len(onboard_convs) > 0:
            print(f"[consume] → handle_unknown_user count={len(onboard_convs)}")
            print("onboard_convs", onboard_convs)
            await handle_unknown_user(
                messages=onboard_convs,
                message_db_service=self._message_db_service,
                user_db_service=self._user_db_service,
                channel_factory=self._channel_client_factory
            )
            print("[consume] ← handle_unknown_user done")

        # build tasks
        tasks = []
        for conv in conversations:
            conv.user.activity_timestamp = str(int(datetime.now(timezone.utc).timestamp()))
            if conv.user.user_type in self._regular_user_type:
                print(f"[consume] queue user_flow msg_id={conv.message_context.message_id}")
                tasks.append(self.__process_byoebuser_conversation(conv))
            elif self.__is_expert_user_type(conv.user.user_type):
                print(f"[consume] queue expert_flow msg_id={conv.message_context.message_id}")
                tasks.append(self.__process_byoebexpert_conversation(conv))

        results = await asyncio.gather(*tasks) if tasks else []
        print(f"[consume] tasks_done count={len(results)}")

        successfully_processed_messages = []
        for queries, processed_message, err in results:
            if err is not None or queries is None:
                print(f"[consume] task_result skipped err={err}")
                continue
            successfully_processed_messages.append(processed_message)
            print(f"[consume] task_result ok msg_id={processed_message.message_context.message_id}")

        # aggregate + write
        start_time = datetime.now(timezone.utc).timestamp()
        user_queries = self._user_db_service.aggregate_queries(results)
        message_queries = self._message_db_service.aggregate_queries(results)
        print(f"[consume] → execute user_queries={len(user_queries) if user_queries else 0} message_queries={len(message_queries) if message_queries else 0}")

        await asyncio.gather(
            self._user_db_service.execute_queries(user_queries),
            self._message_db_service.execute_queries(message_queries)
        )
        end_time = datetime.now(timezone.utc).timestamp()
        print(f"[consume] ← db_write done time={end_time - start_time:.3f}s")

        # metrics
        for m in byoeb_messages:
            app_insights_logger.add_log(
                event_name="write_to_db",
                details={"message_id": m.message_context.message_id, "time_taken": end_time - start_time}
            )
            app_insights_logger.add_log(
                event_name="overall_response_time",
                details={"message_id": m.message_context.message_id, "time_taken": end_time - m.incoming_timestamp}
            )

        successfully_processed_messages.extend(onboard_convs)
        print(f"[consume] ◀ end success_count={len(successfully_processed_messages)}")
        return successfully_processed_messages