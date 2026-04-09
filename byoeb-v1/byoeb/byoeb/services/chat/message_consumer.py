import asyncio
import hashlib
import logging
import byoeb.services.chat.constants as constants
import byoeb.utils.utils as utils
from datetime import datetime, timezone
from pydantic import BaseModel
from typing import Optional, List

from byoeb.models.message_category import MessageCategory
from byoeb.factory import ChannelClientFactory
from byoeb.chat_app.configuration.config import bot_config
from byoeb_core.models.byoeb.user import User
from byoeb_core.models.byoeb.message_context import ReplyContext
from byoeb.services.databases.mongo_db import UserMongoDBService, MessageMongoDBService
from byoeb_core.models.byoeb.message_context import ByoebMessageContext
from byoeb.application_logger.azure_app_insights import AppInsightsLogHandler
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
        # Use module path for logger to ensure proper configuration
        self._logger = logging.getLogger(__name__)
        self._logger.setLevel(logging.INFO)  # Ensure INFO level
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
    ) -> tuple[list[ByoebMessageContext], list[ByoebMessageContext]]:
        self._logger.info("[__create_conversations] ▶ start")
        conversations = []
        onboard_convs = []
        start_time = datetime.now(timezone.utc).timestamp()

        phone_numbers = list(set([m.user.phone_number_id for m in messages]))
        user_ids = list(set([hashlib.md5(number.encode()).hexdigest() for number in phone_numbers]))
        self._logger.debug("[__create_conversations] phone_numbers=%s user_ids_count=%s", phone_numbers, len(user_ids))

        try:
            byoeb_users = await self._user_db_service.get_users(user_ids)
        except Exception as e:
            self._logger.exception(
                "[__create_conversations] user lookup failed: %s", e
            )
            byoeb_users = []  # treat all as unknown; onboarding guard below handles them
        self._logger.debug("[__create_conversations] fetched_users=%s", len(byoeb_users))

        for m in messages:
            user = self.__get_user(byoeb_users, m.user.phone_number_id)
            if user is None:
                self._logger.info("[__create_conversations] user_not_found phone=%s -> onboard", utils.mask_phone(getattr(m.user, "phone_number_id", "")))
                try:
                    AppInsightsLogHandler.getLogger("onboarding_routing").info(
                        "routed_to_onboard: user_not_found",
                        extra={
                            AppInsightsLogHandler.DETAILS: {
                                "reason": "user_not_found",
                                "message_id": m.message_context.message_id,
                                "phone_number_id": utils.mask_phone(getattr(m.user, "phone_number_id", "")),
                            }
                        },
                    )
                except Exception as e:
                    self._logger.warning("[onboarding_routing] telemetry failed: %s", e)
                onboard_convs.append(m)
                continue

            # minimal peek
            self._logger.info("[__create_conversations] user_found phone=%s user_type=%s reply_id=%s", utils.mask_phone(getattr(m.user, "phone_number_id", "")), user.user_type, m.reply_context.reply_id)

            if self.__is_expert_user_type(user.user_type) and m.reply_context.reply_id is None:
                # attach last bot message to reply
                if user.last_conversations and len(user.last_conversations) > 0:
                    m.reply_context = ReplyContext(
                        reply_id=user.last_conversations[0].get("message_id")
                    )
                    self._logger.debug("[__create_conversations] expert -> set reply_id=%s", m.reply_context.reply_id)

        bot_message_ids = list(
            set(m.reply_context.reply_id for m in messages if m.reply_context.reply_id is not None)
        )
        self._logger.debug("[__create_conversations] bot_message_ids=%s", bot_message_ids)

        bot_messages = await self._message_db_service.get_bot_messages_by_ids(bot_message_ids)
        self._logger.debug("[__create_conversations] fetched_bot_messages=%s", len(bot_messages))
        self._logger.debug("bot_messages=%s", bot_messages)

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
            AppInsightsLogHandler.getLogger("create_conversations").info(f"Creating conversation {m.message_context.message_id}", extra={AppInsightsLogHandler.DETAILS: {
                "message_id": m.message_context.message_id,
                "time_taken": end_time - start_time
            }})

            if bot_message is None:
                # Check if this is an onboarding message from an already-registered user
                # If so, route to regular flow to return "already registered" response
                is_onboarding_msg = utils.is_onboard(m.message_context.message_source_text, user.user_language)
                
                if user.user_type is None or user.user_language is None:
                    if is_onboarding_msg and user.user_id is not None:
                        # User is already registered but sending onboarding message
                        self._logger.info("[__create_conversations] registered_user_onboard_msg -> conversations (msg_id=%s)", m.message_context.message_id)
                        conversations.append(conversation)
                    else:
                        # New user needs onboarding
                        self._logger.info("[__create_conversations] no_bot_msg + needs_onboarding -> onboard (msg_id=%s)", m.message_context.message_id)
                        try:
                            AppInsightsLogHandler.getLogger("onboarding_routing").info(
                                "routed_to_onboard: needs_onboarding_no_bot_msg",
                                extra={
                                    AppInsightsLogHandler.DETAILS: {
                                        "reason": "needs_onboarding_no_bot_msg",
                                        "message_id": m.message_context.message_id,
                                        "phone_number_id": utils.mask_phone(getattr(m.user, "phone_number_id", "")),
                                        "has_user_type": user.user_type is not None,
                                        "has_user_language": user.user_language is not None,
                                    }
                                },
                            )
                        except Exception as e:
                            self._logger.warning("[onboarding_routing] telemetry failed: %s", e)
                        onboard_convs.append(m)
                else:
                    self._logger.info("[__create_conversations] no_bot_msg -> conversations (msg_id=%s)", m.message_context.message_id)
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
                or bot_message.message_category == constants.REGISTER_PROMPT
                or bot_message.message_category == constants.CONSENT):
                self._logger.info("[__create_conversations] onboarding_flow msg_id=%s bot_cat=%s -> onboard", m.message_context.message_id, bot_message.message_category)
                onboard_convs.append(conversation)
            elif user.user_type is None or user.user_language is None:
                # Check if this is an onboarding message from an already-registered user
                is_onboarding_msg = utils.is_onboard(m.message_context.message_source_text, user.user_language)
                
                if is_onboarding_msg and user.user_id is not None:
                    # User is already registered but sending onboarding message
                    self._logger.info("[__create_conversations] registered_user_onboard_msg_with_reply -> conversations (msg_id=%s)", m.message_context.message_id)
                    conversations.append(conversation)
                else:
                    # Missing user fields, needs onboarding
                    self._logger.info("[__create_conversations] missing_user_fields -> onboard (msg_id=%s)", m.message_context.message_id)
                    try:
                        AppInsightsLogHandler.getLogger("onboarding_routing").info(
                            "routed_to_onboard: missing_user_fields",
                            extra={
                                AppInsightsLogHandler.DETAILS: {
                                    "reason": "missing_user_fields",
                                    "message_id": m.message_context.message_id,
                                    "phone_number_id": utils.mask_phone(getattr(m.user, "phone_number_id", "")),
                                    "user_id": getattr(user, "user_id", None),
                                }
                            },
                        )
                    except Exception as e:
                        self._logger.warning("[onboarding_routing] telemetry failed: %s", e)
                    onboard_convs.append(m)
            else:
                self._logger.info("[__create_conversations] regular_flow -> conversations (msg_id=%s)", m.message_context.message_id)
                conversations.append(conversation)

        self._logger.info("[__create_conversations] ◀ end conversations=%s onboard=%s took=%.3fs", len(conversations), len(onboard_convs), end_time - start_time)
        return conversations, onboard_convs

    async def __process_byoebuser_conversation(self, byoeb_message: ByoebMessageContext):
        self._logger.info("[__process_byoebuser_conversation] ▶ start msg_id=%s", byoeb_message.message_context.message_id)
        from byoeb.chat_app.configuration.dependency_setup import byoeb_user_process
        byoeb_message_copy = byoeb_message.model_copy(deep=True)
        try:
            queries = await asyncio.wait_for(
                byoeb_user_process.handle([byoeb_message]),
                timeout=self.__timeout_seconds
            )
            self._logger.info("[__process_byoebuser_conversation] ◀ ok queries_count=%s", len(queries) if queries else 0)
            return queries, byoeb_message_copy, None
        except asyncio.TimeoutError:
            self._logger.error("[__process_byoebuser_conversation] ✖ Timeout after %ss", self.__timeout_seconds)
            AppInsightsLogHandler.getLogger("timeout_error").info(f"Timeout after {self.__timeout_seconds}s", extra={AppInsightsLogHandler.DETAILS: {
                "user_type": byoeb_message.user.user_type,
                "message_id": byoeb_message.message_context.message_id,
                "message_text": byoeb_message.message_context.message_source_text,
                "timeout_seconds": self.__timeout_seconds
            }})
            return None, byoeb_message_copy, "TimeoutError"
        except Exception as e:
            self._logger.exception("[__process_byoebuser_conversation] ✖ error: %s", e)
            return None, byoeb_message_copy, e

    async def __process_byoebexpert_conversation(
        self,
        byoeb_message: ByoebMessageContext
    ):
        self._logger.info("[__process_byoebexpert_conversation] ▶ start msg_id=%s", byoeb_message.message_context.message_id)
        from byoeb.chat_app.configuration.dependency_setup import byoeb_expert_process
        byoeb_message_copy = byoeb_message.model_copy(deep=True)
        try:
            queries = await asyncio.wait_for(
                byoeb_expert_process.handle([byoeb_message]),
                timeout=self.__timeout_seconds
            )
            self._logger.info("[__process_byoebexpert_conversation] ◀ ok queries_count=%s", len(queries) if queries else 0)
            return queries, byoeb_message_copy, None
        except asyncio.TimeoutError:
            self._logger.error("[__process_byoebexpert_conversation] ✖ Timeout after %ss", self.__timeout_seconds)
            AppInsightsLogHandler.getLogger("timeout_error").info(f"Timeout after {self.__timeout_seconds}s", extra={AppInsightsLogHandler.DETAILS: {
                "user_type": byoeb_message.user.user_type,
                "message_id": byoeb_message.message_context.message_id,
                "message_text": byoeb_message.message_context.message_source_text,
                "timeout_seconds": self.__timeout_seconds
            }})
            return None, byoeb_message_copy, "TimeoutError"
        except Exception as e:
            self._logger.exception("[__process_byoebexpert_conversation] ✖ error: %s", e)
            return None, byoeb_message_copy, e

    async def consume(
        self,
        messages: list
    ) -> List[ByoebMessageContext]:
        self._logger.info(f"[consume] Processing {len(messages)} raw message(s)")
        byoeb_messages: List[ByoebMessageContext] = []

        for raw in messages:
            try:
                byoeb_messages.append(ByoebMessageContext.model_validate_json(raw))
            except Exception as e:
                self._logger.error("[consume] Failed to parse queue message, skipping: %s | raw_preview=%s", e, raw[:200])
                continue
            self._logger.debug("raw=%s", raw)
        self._logger.debug("byoeb_messages=%s", byoeb_messages)

        start_time = datetime.now(timezone.utc).timestamp()
        conversations, onboard_convs = await self.__create_conversations(byoeb_messages)
        end_time = datetime.now(timezone.utc).timestamp()

        self._logger.info("[consume] conversations=%s onboard=%s create_time=%.3fs", len(conversations), len(onboard_convs), end_time - start_time)

        # onboarding first (if any)
        if onboard_convs is not None and len(onboard_convs) > 0:
            self._logger.info("[consume] → handle_unknown_user count=%s", len(onboard_convs))
            self._logger.debug("onboard_convs=%s", onboard_convs)
            await handle_unknown_user(
                messages=onboard_convs,
                message_db_service=self._message_db_service,
                user_db_service=self._user_db_service,
                channel_factory=self._channel_client_factory
            )
            self._logger.info("[consume] ← handle_unknown_user done")

        # build tasks: run each conversation in its extracted trace context when available
        async def process_one(conv: ByoebMessageContext):
            msg_id = conv.message_context.message_id
            conv.user.activity_timestamp = datetime.now(timezone.utc)
            if conv.user.user_type in self._regular_user_type:
                self._logger.debug("[consume] queue user_flow msg_id=%s", msg_id)
                return await self.__process_byoebuser_conversation(conv)
            elif self.__is_expert_user_type(conv.user.user_type):
                self._logger.debug("[consume] queue expert_flow msg_id=%s", msg_id)
                return await self.__process_byoebexpert_conversation(conv)
            return None, None, None

        tasks = [process_one(conv) for conv in conversations]
        results = await asyncio.gather(*tasks) if tasks else []
        self._logger.info("[consume] tasks_done count=%s", len(results))

        successfully_processed_messages = []
        for queries, processed_message, err in results:
            if err is not None or queries is None:
                self._logger.warning("[consume] task_result skipped err=%s", err)
                continue
            successfully_processed_messages.append(processed_message)
            self._logger.info("[consume] task_result ok msg_id=%s", processed_message.message_context.message_id)

        # aggregate + write
        start_time = datetime.now(timezone.utc).timestamp()
        user_queries = self._user_db_service.aggregate_queries(results)
        message_queries = self._message_db_service.aggregate_queries(results)
        self._logger.debug(
            "[consume] → execute user_queries=%s message_queries=%s",
            len(user_queries) if user_queries else 0,
            len(message_queries) if message_queries else 0,
        )

        await asyncio.gather(
            self._user_db_service.execute_queries(user_queries),
            self._message_db_service.execute_queries(message_queries)
        )
        end_time = datetime.now(timezone.utc).timestamp()
        self._logger.debug("[consume] ← db_write done time=%.3fs", end_time - start_time)

        # metrics
        for m in byoeb_messages:
            AppInsightsLogHandler.getLogger("write_to_db").info(f"Wrote message {m.message_context.message_id} to database", extra={AppInsightsLogHandler.DETAILS: {
                "message_id": m.message_context.message_id,
                "time_taken": end_time - start_time,
                "overall_time_taken": end_time - m.incoming_timestamp
            }})

        successfully_processed_messages.extend(onboard_convs)
        self._logger.info("[consume] ◀ end success_count=%s", len(successfully_processed_messages))
        return successfully_processed_messages