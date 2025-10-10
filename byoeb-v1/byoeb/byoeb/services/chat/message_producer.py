import logging
import json
import time
import traceback
import byoeb.utils.utils as utils
from byoeb.services.chat import constants
from datetime import datetime, timezone
from byoeb_core.models.byoeb.message_status import ByoebMessageStatus
from byoeb_core.models.byoeb.message_context import ByoebMessageContext
from byoeb_core.message_queue.base import BaseQueue
from byoeb.chat_app.configuration.dependency_setup import app_insights_logger
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

    def __convert_whatsapp_to_byoeb_message(
        self,
        message
    ) -> ByoebMessageContext:
        import byoeb_integrations.channel.whatsapp.validate_message as wa_validator
        import byoeb_integrations.channel.whatsapp.convert_message as wa_converter
        _, message_type = wa_validator.validate_whatsapp_message(message)
        print("message", message)
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
                    print(f"[FALLBACK] Set message_source_text='{selected_text}' for interactive list reply")
        except Exception as e:
            print(f"[FALLBACK] Error extracting text: {e}")
        print("byoeb_message", byoeb_message)
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
        channel
    ):
        print("[apublish_message] ▶ start")
        print(f"[apublish_message]   in  channel={channel}")

        byoeb_message: ByoebMessageContext = None
        n = 5

        if channel == "whatsapp":
            print("[apublish_message] → __convert_whatsapp_to_byoeb_message")
            byoeb_message = self.__convert_whatsapp_to_byoeb_message(message)
            print("converted byoed_message", byoeb_message)
            print(f"[apublish_message] ← converted "
                f"message_id={getattr(getattr(byoeb_message, 'message_context', None), 'message_id', None)} "
                f"incoming_ts={getattr(byoeb_message, 'incoming_timestamp', None)}")

        if byoeb_message is None or byoeb_message is False:
            print("[apublish_message] ↳ invalid byoeb_message → return (None, 'Invalid message')")
            return None, "Invalid message"

        # older-than check
        print(f"[apublish_message] → is_older_than_n_minutes(n={n}, incoming_ts={byoeb_message.incoming_timestamp})")
        if self.is_older_than_n_minutes(n, byoeb_message.incoming_timestamp):
            print(f"[apublish_message] ↳ older than {n} minutes → return ('Skipped...', None)")
            return f"Skipped. Older than {n} minutes", None

        # duplicate check
        mid = getattr(byoeb_message.message_context, "message_id", None)
        print(f"[apublish_message] → get_bot_messages_by_ids([{mid}])")
        res = await self.__message_db_service.get_bot_messages_by_ids([mid])
        print(f"[apublish_message] ← duplicates count={len(res)}")
        if len(res) > 0:
            print("[apublish_message] ↳ already processed → return ('Already processed', None)")
            return "Already processed", None

        try:
            # queue publish
            print("[apublish_message] → queue.asend_message(...)")
            result = await self.__queue_client.asend_message(
                byoeb_message.model_dump_json(),
                time_to_live=self._config["message_queue"]["azure"]["time_to_live"]
            )
            print(f"[apublish_message] ← queue result id={getattr(result, 'id', None)}")

            # app insights log (no print needed, but keep one-liner)
            print(f"[apublish_message] log app_insights message_id={mid} phone_number_id={getattr(byoeb_message.user, 'phone_number_id', None)}")
            app_insights_logger.add_log(
                event_name="message_published",
                details={
                    "message_id": mid,
                    "phone_number_id": getattr(byoeb_message.user, "phone_number_id", None)
                }
            )

            # db write
            print("[apublish_message] → message_db_service.execute_queries(CREATE)")
            message_db_queries = {
                constants.CREATE: self.__message_db_service.message_create_queries(byoeb_messages=[byoeb_message]),
            }
            await self.__message_db_service.execute_queries(message_db_queries)
            print("[apublish_message] ← db write done")

            # success
            self._logger.info(f"Message sent: {result}")
            print(f"[apublish_message] ◀ success Published successfully {getattr(result, 'id', None)}")
            return f"Published successfully {getattr(result, 'id', None)}", None

        except Exception as e:
            print(f"[apublish_message] ✖ exception: {e}")
            traceback.print_exc()
            return None, e