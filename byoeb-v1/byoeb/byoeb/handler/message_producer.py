import logging
import traceback
import byoeb_integrations.channel.whatsapp.validate_message as wa_validator
from byoeb.services.databases.mongo_db.message_db import MessageMongoDBService
from typing import Any
from byoeb.factory import QueueProducerFactory
from byoeb.services.chat.message_producer import MessageProducerService
from byoeb_core.models.byoeb.response import ByoebResponseModel, ByoebStatusCodes

class QueueProducerHandler:
    def __init__(
        self,
        config,
        queue_producer_factory: QueueProducerFactory,
        message_db_service: MessageMongoDBService
    ):
        self._logger = logging.getLogger(self.__class__.__name__)
        self._config = config
        self._queue_provider = config["app"]["queue_provider"]
        self.queue_producer_factory = queue_producer_factory
        self.message_db_service = message_db_service

    async def __get_or_create_message_producer(
        self,
        message_type
    ) -> MessageProducerService:
        queue_client = await self.queue_producer_factory.get(self._queue_provider, message_type)
        return MessageProducerService(
            self._config,
            queue_client,
            self.message_db_service)

    async def __validate_channel_and_get_message_type(
        self,
        message
    ) -> Any:
        is_whatsapp, message_type = wa_validator.validate_whatsapp_message(message)
        print("message_type", message_type)
        if is_whatsapp:
            return "whatsapp", message_type
        return False, None
            
        
    async def handle(self, message):
        print("[handle] ▶ start")
        print(f"[handle]   in  message={message}")

        print("[handle] → __validate_channel_and_get_message_type")
        channel, message_type = await self.__validate_channel_and_get_message_type(message)
        print(f"[handle] ← __validate...  out channel={channel}, message_type={message_type}")

        if message_type == "status":
            print("[handle] ↳ branch: status → return OK('status update')")
            return ByoebResponseModel(
                status_code=ByoebStatusCodes.OK,
                message="status update"
            )

        if not channel:
            print("[handle] ↳ branch: invalid channel → return BAD_REQUEST('Invalid channel')")
            return ByoebResponseModel(
                status_code=ByoebStatusCodes.BAD_REQUEST,
                message="Invalid channel"
            )

        try:
            print(f"[handle] → __get_or_create_message_producer(message_type={message_type})")
            message_producer_service = await self.__get_or_create_message_producer(message_type)
            print(f"[handle] ← __get_or_create... out producer={type(message_producer_service).__name__}")
        except Exception as e:
            print(f"[handle] ✖ producer init failed: {e}")
            traceback.print_exc()
            return ByoebResponseModel(
                status_code=ByoebStatusCodes.INTERNAL_SERVER_ERROR,
                message=f"Invalid producer type: {str(e)}"
            )

        print("[handle] → apublish_message(message, channel)", message)
        response, err = await message_producer_service.apublish_message(message, channel)
        print(f"[handle] ← apublish_message out response={response}, err={err}")

        if err is not None:
            print("[handle] ↳ branch: publish error → return 500")
            return ByoebResponseModel(
                status_code=ByoebStatusCodes.INTERNAL_SERVER_ERROR,
                message=err
            )

        print("[handle] ◀ return OK(response)")
        return ByoebResponseModel(
            status_code=ByoebStatusCodes.OK,
            message=response
        )
        