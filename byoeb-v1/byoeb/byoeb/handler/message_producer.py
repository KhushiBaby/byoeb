import json
import logging
import byoeb_integrations.channel.whatsapp.validate_message as wa_validator
from byoeb.services.databases.mongo_db.message_db import MessageMongoDBService
from typing import Any
from byoeb.factory import QueueProducerFactory
from byoeb.services.chat.message_producer import MessageProducerService
from byoeb_core.models.byoeb.response import ByoebResponseModel, ByoebStatusCodes
from byoeb.application_logger.azure_app_insights import AppInsightsLogHandler

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
        self._logger.debug("message_type=%s", message_type)
        if is_whatsapp:
            return "whatsapp", message_type
        return False, None
            
        
    async def handle(self, message, integration_id: str | None = None):
        self._logger.info("[handle] ▶ start")
        self._logger.debug("[handle]   in message=%s", message)

        self._logger.debug("[handle] → __validate_channel_and_get_message_type")
        channel, message_type = await self.__validate_channel_and_get_message_type(message)
        self._logger.debug("[handle] ← __validate... out channel=%s, message_type=%s", channel, message_type)

        if message_type is None:
            self._logger.warning("[handle] ↳ branch: unsupported message type")
            return ByoebResponseModel(status_code=ByoebStatusCodes.OK, message="unsupported message type")

        if message_type == "status":
            self._logger.info("[handle] ↳ branch: status → return OK('status update')")
            status = message["entry"][0]["changes"][0]["value"]["statuses"][0]
            AppInsightsLogHandler.getLogger("wa_transmission_status").info(f"Received status {status['status']} for message {status['id']}", extra={AppInsightsLogHandler.DETAILS: {
                "id": status["id"],
                "status": status["status"],
                "timestamp": str(status["timestamp"]),
                "errors": json.dumps(status["errors"] if "errors" in status else [])
            }})
            return ByoebResponseModel(
                status_code=ByoebStatusCodes.OK,
                message={"id": status["id"], "status": status["status"]}
            )

        if not channel:
            self._logger.warning("[handle] ↳ branch: invalid channel → return BAD_REQUEST('Invalid channel')")
            return ByoebResponseModel(
                status_code=ByoebStatusCodes.BAD_REQUEST,
                message="Invalid channel"
            )

        try:
            message_producer_service = await self.__get_or_create_message_producer(message_type)
        except Exception as e:
            self._logger.exception("[handle] ✖ producer init failed: %s", e)
            return ByoebResponseModel(
                status_code=ByoebStatusCodes.INTERNAL_SERVER_ERROR,
                message=f"Invalid producer type: {str(e)}"
            )

        self._logger.info("[handle] → apublish_message(message, channel, integration_id=%s)", integration_id)
        response, err = await message_producer_service.apublish_message(message, channel, integration_id=integration_id)
        self._logger.info("[handle] ← apublish_message out response=%s err=%s", response, err)

        if err is not None:
            self._logger.error("[handle] ↳ branch: publish error → return 500")
            return ByoebResponseModel(
                status_code=ByoebStatusCodes.INTERNAL_SERVER_ERROR,
                message=err
            )

        self._logger.info("[handle] ◀ return OK(response)")
        return ByoebResponseModel(
            status_code=ByoebStatusCodes.OK,
            message=response
        )
