import asyncio
import logging
from typing import Dict, Optional
from enum import Enum
from byoeb_integrations.channel.whatsapp.register import RegisterWhatsapp
from byoeb_integrations.channel.whatsapp.meta.async_whatsapp_client import AsyncWhatsAppClient

class ChannelType(Enum):
    WHATSAPP = 'whatsapp'

class ChannelRegisterFactory:
    def __init__(self):
        self._logger = logging.getLogger(__name__)

    async def get(
        self,
        channel_type: str,
        verify_token: Optional[str]
    ):
        if channel_type == ChannelType.WHATSAPP.value:
            from byoeb.services.auth.auth_service import get_auth_service
            auth_service = await get_auth_service()
            if await auth_service.verify_integration_token("whatsapp", verify_token):
                return RegisterWhatsapp(verify_token)
            else:
                self._logger.error(f"Invalid verification token for {channel_type}")
                raise ValueError(f"Invalid verification token for {channel_type}")
        else:
            self._logger.error(f"Invalid channel type: {channel_type}")
            raise ValueError(f"Invalid channel type: {channel_type}")
    

class ChannelClientFactory:
    _whatsapp_clients: Dict[str, AsyncWhatsAppClient] = {}
    _lock: asyncio.Lock = asyncio.Lock()

    def __init__(
        self,
        config
    ):
        self._logger = logging.getLogger(__name__)
        self._config = config

    async def __get_whatsapp_client(
        self,
        phone_number_id: str
    ) -> AsyncWhatsAppClient:
        async with self._lock:
            if phone_number_id not in self._whatsapp_clients:
                from byoeb.services.auth.auth_service import get_auth_service
                auth_service = await get_auth_service()
                integration = await auth_service.resolve_integration("whatsapp", phone_number_id)
                if not integration or "bearer_token" not in integration.credentials:
                    raise ValueError(f"WhatsApp integration not configured for {phone_number_id}")
                
                bearer_token = integration.credentials["bearer_token"]
                self._whatsapp_clients[phone_number_id] = AsyncWhatsAppClient(
                    phone_number_id=phone_number_id,
                    bearer_token=bearer_token,
                    reuse_client=self._config["channel"]["whatsapp"]["reuse_client"]
                )
            return self._whatsapp_clients[phone_number_id]

    async def get(
        self,
        channel_type: str,
        phone_number_id: str
    ) -> AsyncWhatsAppClient:
        if channel_type == ChannelType.WHATSAPP.value:
            return await self.__get_whatsapp_client(phone_number_id)
        else:
            self._logger.error(f"Invalid channel type: {channel_type}")
            raise ValueError(f"Invalid channel type: {channel_type}")
    
    async def close(self):
        async with self._lock:
            for client in self._whatsapp_clients.values():
                await client._close()
            self._whatsapp_clients.clear()