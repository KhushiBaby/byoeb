import logging
from fastapi import HTTPException, Request, status
from byoeb.factory import ChannelRegisterFactory

class ChannelRegisterHandler:
    def __init__(
        self,
        registrer_factory: ChannelRegisterFactory
    ) -> None:
        self.__registrer_factory = registrer_factory
        self.__logger = logging.getLogger(self.__class__.__name__)
        self.__logger.setLevel(logging.DEBUG)

    async def handle(
        self,
        request: Request
    ):
        verify_token = request.query_params.get("hub.verify_token")
        if not verify_token:
            self.__logger.warning("Webhook verification attempt with missing hub.verify_token")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Missing hub.verify_token"
            )
        register = await self.__registrer_factory.get(channel_type="whatsapp", verify_token=verify_token)
        return await register.register(request.query_params._dict)