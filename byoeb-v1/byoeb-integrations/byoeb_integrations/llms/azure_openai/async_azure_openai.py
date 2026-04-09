import threading
import logging
from typing import Any, Dict
from enum import Enum
from langfuse.openai import AsyncAzureOpenAI
from byoeb_core.llms.base import BaseLLM

logger = logging.getLogger(__name__)

class AzureOpenAIParamsEnum(Enum):
    TEMPERATURE = "temperature"

class AsyncAzureOpenAILLM(BaseLLM):
    __DEFAULT_TEMPERATURE = 0

    def __init__(
        self,
        model: str,
        azure_endpoint: str,
        token_provider: str = None,
        api_key: str = None,
        api_version: str = None,
        **kwargs
    ):
        client = None
        if model is None:
            raise ValueError("model must be provided")
        if api_version is None:
            raise ValueError("api_version must be provided")
        if azure_endpoint is None:
            raise ValueError("azure_endpoint must be provided")
        if token_provider is not None:
            client = AsyncAzureOpenAI(
                azure_endpoint=azure_endpoint,
                azure_ad_token_provider=token_provider,
                api_version=api_version
            )
        elif api_key is not None:
            client = AsyncAzureOpenAI(
                api_key=api_key,
                azure_endpoint=azure_endpoint,
                api_version=api_version
            )
        else:
            raise ValueError("Either token_provider or api_key must be provided")
        
        self.__model = model
        self.__client = client
    
    async def generate_response(
        self,
        prompts: list,
        **kwargs
    ) -> Any:
        logger.debug("Thread ID: %s, Variable Address: %s, client address: %s", 
                     threading.get_ident(), id(prompts), id(self.__client))
        temperature = kwargs.get(
            AzureOpenAIParamsEnum.TEMPERATURE.value,
            self.__DEFAULT_TEMPERATURE
        )
        response = await self.__client.chat.completions.create(
            model = self.__model,
            messages=prompts,
            temperature=temperature,
        )
        return response, response.choices[0].message.content.strip()
    
    def get_response_tokens(
        self,
        response: Any
    ) -> Dict[str, int]:
        raise NotImplementedError

    def get_llm_client(self):
        return self.__client