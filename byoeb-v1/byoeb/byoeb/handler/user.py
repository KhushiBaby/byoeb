import json
import byoeb.services.user.utils as user_utils
from typing import List, Any, Dict
from byoeb.services.user.user import UserService
from byoeb.constants import ErrorMessage
from byoeb_core.models.byoeb.response import ByoebResponseModel, ByoebStatusCodes
from byoeb_core.models.byoeb.user import User 
from byoeb.chat_app.configuration.config import app_config, bot_config
from byoeb.factory import MongoDBFactory
from byoeb_core.databases.mongo_db.base import BaseDocumentCollection
from byoeb_integrations.databases.mongo_db.azure.async_azure_cosmos_mongo_db import AsyncAzureCosmosMongoDB, AsyncAzureCosmosMongoDBCollection

class UsersHandler:
    _user_service = None
    def __init__(
        self,
        db_provider: str,
        mongo_db_facory: MongoDBFactory
    ) -> None:
        self.__mongo_db_facory = mongo_db_facory
        self.__db_provider = db_provider
        self.__user_collection = app_config["databases"]["mongo_db"]["user_collection"]
        self.__mongo_db = None
        self.__user_collection_client = None
        _regular = bot_config["regular"]["user_type"]
        self.__regular_user_types = _regular if isinstance(_regular, list) else [_regular]
        self.__expert_user_types = list(set(bot_config["expert"].values()))
        self.__expert_user_types_set = set(self.__expert_user_types)

    def __is_regular_user_type(self, user_type: str) -> bool:
        return user_type in self.__regular_user_types
    
    def __validate_expert_user_type(
        self,
        expert_user_type: str
    ) -> bool:
        return expert_user_type in self.__expert_user_types_set
    
    def __validate_experts(
        self,
        experts: Dict[str, List[str]]
    ):
        if experts is None:
            return True
        for key in experts.keys():
            if key not in self.__expert_user_types_set:
                return False
        return True

    async def get_collection_client(self) -> BaseDocumentCollection:
        if self.__user_collection_client is not None:
            return self.__user_collection_client
        self.__mongo_db = await self.__mongo_db_facory.get(self.__db_provider)
        if isinstance(self.__mongo_db, AsyncAzureCosmosMongoDB):
            self.__user_collection_client = AsyncAzureCosmosMongoDBCollection(
                collection=self.__mongo_db.get_collection(self.__user_collection)
            )
        return self.__user_collection_client
    
    async def get_or_create_user_service(self) -> UserService:
        if self._user_service is not None:
            return self._user_service
        self._user_service = UserService(
            collection_client=await self.get_collection_client(),
            bot_config=bot_config
        )
        return self._user_service
    
    async def aregister(
        self,
        data: list
    ) -> ByoebResponseModel:
        user_svc = await self.get_or_create_user_service()
        byoeb_users = []
        byoeb_messages = []
        for user in data:
            byoeb_user = User(**user)
            expert_numbers = user_utils.get_experts_numbers(byoeb_user.experts)

            required_fields = [
                ("phone_number_id", ErrorMessage.PHONE_NUMBER_ID_REQUIRED.value),
                ("user_language",   ErrorMessage.USER_LANGUAGE_REQUIRED.value),
                ("user_type",       ErrorMessage.USER_TYPE_REQUIRED.value),
            ]
            missing_msg = next(
                (msg for attr, msg in required_fields if getattr(byoeb_user, attr) is None),
                None
            )
            if missing_msg:
                byoeb_messages.append(user_utils.get_register_message(byoeb_user, missing_msg))
                continue

            is_regular = self.__is_regular_user_type(byoeb_user.user_type)
            is_expert  = self.__validate_expert_user_type(byoeb_user.user_type)
            audience     = byoeb_user.audience or []
            experts_ok   = self.__validate_experts(byoeb_user.experts)

            checks = [
                (
                    not is_regular and not is_expert,
                    ErrorMessage.INVALID_USER_TYPE.value.format(
                        regular_types=self.__regular_user_types,
                        expert_types=self.__expert_user_types
                    )
                ),
                (
                    is_regular and not experts_ok,
                    ErrorMessage.INVALID_EXPERT_TYPE.value.format(
                        expert_types=self.__expert_user_types
                    )
                ),
                (
                    is_regular and len(audience) != 0,
                    ErrorMessage.CANNOT_HAVE_AUDIENCE.value
                ),
                (
                    is_regular and expert_numbers is not None and byoeb_user.phone_number_id in expert_numbers,
                    ErrorMessage.CANNOT_BE_OWN_EXPERT.value
                ),
                (
                    is_expert and len(expert_numbers or []) != 0,
                    ErrorMessage.CANNOT_HAVE_EXPERTS.value
                ),
                (
                    is_expert and byoeb_user.audience is not None and byoeb_user.phone_number_id in audience,
                    ErrorMessage.CANNOT_BE_OWN_AUDIENCE.value
                ),
            ]
            for condition, message in checks:
                if condition:
                    byoeb_messages.append(user_utils.get_register_message(byoeb_user, message))
                    break
            else:
                # No check triggered → accept user
                byoeb_users.append(byoeb_user)

        if len(byoeb_messages) > 0:
            return ByoebResponseModel(
                status_code=ByoebStatusCodes.BAD_REQUEST.value,
                message=byoeb_messages
            )
        results = await user_svc.aregister(byoeb_users)
        return ByoebResponseModel(
            status_code=ByoebStatusCodes.OK.value,
            message=results
        )
    
    async def adelete(
        self,
        phone_number_ids: Any
    ):
        if not isinstance(phone_number_ids, list):
            return ByoebResponseModel(
                status_code=ByoebStatusCodes.BAD_REQUEST.value,
                message=ErrorMessage.PHONE_NUMBER_IDS_LIST_REQUIRED.value
            )
        user_svc = await self.get_or_create_user_service()
        results = await user_svc.adelete(
            phone_number_ids=phone_number_ids
        )
        return ByoebResponseModel(
            status_code=ByoebStatusCodes.OK.value,
            message=results
        )
    
    async def aupdate(
        self,
        data: str
    ):
        user_collection_client = await self.get_collection_client()
        
    async def aget(
        self,
        phone_number_ids: Any
    ):
        if not isinstance(phone_number_ids, list):
            return ByoebResponseModel(
                status_code=ByoebStatusCodes.BAD_REQUEST.value,
                message=ErrorMessage.PHONE_NUMBER_IDS_LIST_REQUIRED.value
            )
        user_svc = await self.get_or_create_user_service()
        results = await user_svc.aget(
            phone_number_ids=phone_number_ids
        )
        return ByoebResponseModel(
            status_code=ByoebStatusCodes.OK.value,
            message=results
        )