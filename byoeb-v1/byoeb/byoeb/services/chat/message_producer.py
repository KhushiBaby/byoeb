import logging
import byoeb.utils.utils as utils
from byoeb.services.chat import constants
from datetime import datetime, timezone
from byoeb_core.models.byoeb.message_context import ByoebMessageContext
from byoeb_core.message_queue.base import BaseQueue
from byoeb.application_logger.azure_app_insights import AppInsightsLogHandler
from byoeb.services.databases.mongo_db.message_db import MessageMongoDBService


class MessageProducerService:
    def __init__(
        self,
        config,
        queue_client: BaseQueue,
        message_db_service: MessageMongoDBService
    ):
        self._logger = logging.getLogger(self.__class__.__name__)
        self._config = config
        self.__queue_client = queue_client
        self.__message_db_service = message_db_service
        self._message_pub_logger = AppInsightsLogHandler.getLogger("message_published")

    def __convert_whatsapp_to_byoeb_message(
        self,
        message
    ) -> ByoebMessageContext:
        import byoeb_integrations.channel.whatsapp.validate_message as wa_validator
        import byoeb_integrations.channel.whatsapp.convert_message as wa_converter
        _, message_type = wa_validator.validate_whatsapp_message(message)
        self._logger.debug("message=%s", message)
        byoeb_message = wa_converter.convert_whatsapp_to_byoeb_message(message, message_type)
        try:
            msg_ctx = getattr(byoeb_message, "message_context", None)
            if (
                msg_ctx is not None
                and getattr(msg_ctx, "message_type", None) == "interactive_list_reply"
                and not getattr(msg_ctx, "message_source_text", None)
            ):
                entry = (message or {}).get("entry", [])
                changes = entry[0].get("changes", []) if entry else []
                value = changes[0].get("value", {}) if changes else {}
                msgs = value.get("messages", [])
                interactive = msgs[0].get("interactive", {}) if msgs else {}
                list_reply = interactive.get("list_reply", {}) if interactive else {}
                selected_text = list_reply.get("title") or list_reply.get("id")
                if selected_text:
                    msg_ctx.message_source_text = selected_text
                    self._logger.info("[FALLBACK] Set message_source_text='%s' for interactive list reply", selected_text)
        except Exception as e:
            self._logger.exception("[FALLBACK] Error extracting text: %s", e)
        self._logger.debug("byoeb_message=%s", byoeb_message)
        return byoeb_message
    
    def is_older_than_n_minutes(
        self,
        n,
        unix_timestamp
    ) -> bool:
        seconds = n*60
        current_time = datetime.now(timezone.utc).timestamp()
        utils.log_to_text_file(f"Message duration: {current_time - unix_timestamp}")
        self._logger.info(f"Message duration: {current_time - unix_timestamp}")
        if current_time - unix_timestamp > seconds:
            return True
        return False
        

    async def apublish_message(
        self,
        message,
        channel,
        integration_id: str | None = None,
    ):
        self._logger.info("[apublish_message] ▶ start")
        self._logger.info("[apublish_message]   in  channel=%s integration_id=%s", channel, integration_id)

        byoeb_message: ByoebMessageContext = None
        n = 5

        if channel == "whatsapp":
            self._logger.debug("[apublish_message] → __convert_whatsapp_to_byoeb_message")
            byoeb_message = self.__convert_whatsapp_to_byoeb_message(message)
            if integration_id and byoeb_message and byoeb_message.message_context:
                if byoeb_message.message_context.additional_info is None:
                    byoeb_message.message_context.additional_info = {}
                byoeb_message.message_context.additional_info[constants.INTEGRATION_ID] = integration_id
            mid_conv = getattr(getattr(byoeb_message, "message_context", None), "message_id", None)
            self._logger.debug(
                "[apublish_message] ← converted message_id=%s incoming_ts=%s byoeb_message=%s",
                mid_conv,
                getattr(byoeb_message, "incoming_timestamp", None),
                byoeb_message,
            )

        if byoeb_message is None or byoeb_message is False:
            self._logger.warning("[apublish_message] ↳ invalid byoeb_message → return (None, 'Invalid message')")
            return None, "Invalid message"

        mid = getattr(byoeb_message.message_context, "message_id", None)

        # older-than check
        self._logger.debug("[apublish_message] → is_older_than_n_minutes(n=%s, incoming_ts=%s)", n, byoeb_message.incoming_timestamp)
        if self.is_older_than_n_minutes(n, byoeb_message.incoming_timestamp):
            self._logger.info("[apublish_message] ↳ older than %s minutes → return ('Skipped...', None)", n)
            return f"Skipped. Older than {n} minutes", None

        # duplicate check
        self._logger.debug("[apublish_message] → get_bot_messages_by_ids([%s])", mid)
        res = await self.__message_db_service.get_bot_messages_by_ids([mid])
        self._logger.debug("[apublish_message] ← duplicates count=%s", len(res))
        if len(res) > 0:
            self._logger.info("[apublish_message] ↳ already processed → return ('Already processed', None)")
            return "Already processed", None

        try:
            # queue publish (inject trace context into payload when available)
            self._logger.debug("[apublish_message] → queue.send_message(...)")
            result = await self.__queue_client.send_message(
                byoeb_message.model_dump_json(),
                time_to_live=self._config["message_queue"]["azure"]["time_to_live"]
            )
            self._logger.info("[apublish_message] ← queue result id=%s", getattr(result, "id", None))

            # app insights log (no print needed, but keep one-liner)
            self._message_pub_logger.info(f"Published message_id={mid} phone_number_id={getattr(byoeb_message.user, 'phone_number_id', None)}", extra={AppInsightsLogHandler.DETAILS: {
                "message_id": mid,
                "phone_number_id": getattr(byoeb_message.user, "phone_number_id", None)
            }})

            # db write
            self._logger.debug("[apublish_message] → message_db_service.execute_queries(CREATE)")
            message_db_queries = {
                constants.CREATE: self.__message_db_service.message_create_queries(byoeb_messages=[byoeb_message]),
            }
            await self.__message_db_service.execute_queries(message_db_queries)
            self._logger.info("[apublish_message] ← db write done")

            # success
            self._logger.info(f"Message sent: {result}")
            self._logger.info("[apublish_message] ◀ success Published successfully %s", getattr(result, "id", None))
            return f"Published successfully {getattr(result, 'id', None)}", None
        except Exception as e:
            self._logger.exception("[apublish_message] ✖ exception: %s", e)
            return None, e