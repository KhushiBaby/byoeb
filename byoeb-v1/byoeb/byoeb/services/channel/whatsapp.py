import asyncio
import json
import logging
import uuid
import byoeb.services.chat.constants as constants
import byoeb.services.chat.utils as utils 
from byoeb_integrations.channel.whatsapp.meta.async_whatsapp_client import AsyncWhatsAppClient
import byoeb_integrations.channel.whatsapp.request_payload as wa_req_payload
from byoeb.services.channel.base import BaseChannelService, MessageReaction
from byoeb.factory import ChannelClientFactory
from byoeb_core.models.byoeb.message_context import (
    User,
    ByoebMessageContext,   
    MessageContext,
    ReplyContext,
    MediaContext,
    MessageTypes
)
from byoeb_core.models.whatsapp.response.message_response import WhatsAppResponse
from typing import List, Dict, Any, Tuple, Optional
from datetime import datetime, timezone


_logger = logging.getLogger(__name__)


class WhatsAppService(BaseChannelService):
    __client_type = "whatsapp"
    __passover_integration_id = "_byoeb_integration_id"

    def __init__(
        self,
        channel_client_factory: ChannelClientFactory
    ):
        self.__channel_client_factory = channel_client_factory
    def prepare_reaction_requests(
        self,
        message_reactions: List[MessageReaction]
    ) -> List[Dict[str, Any]]:
        reactions = []
        for message_reaction in message_reactions:
            message_id = message_reaction.message_id
            phone_number_id = message_reaction.phone_number_id
            reaction = message_reaction.reaction
            reaction_request = wa_req_payload.get_whatsapp_reaction_request(
                phone_number_id,
                message_id,
                reaction
            )
            reactions.append(reaction_request)
        return reactions
    
    def prepare_requests(
        self,
        byoeb_message: ByoebMessageContext
    ) -> List[Dict[str, Any]]:
        _log = logging.getLogger(__name__)
        wa_requests = []
        msg_ctx = byoeb_message.message_context
        msg_type = getattr(msg_ctx, "message_type", None) or (msg_ctx.get("message_type") if isinstance(msg_ctx, dict) else None)
        has_tpl = utils.has_template_additional_info(byoeb_message)
        _log.debug(
            "[prepare_requests] message_type=%s has_interactive_button=%s has_interactive_list=%s has_text=%s has_template=%s has_audio=%s",
            msg_type,
            utils.has_interactive_button_additional_info(byoeb_message),
            utils.has_interactive_list_additional_info(byoeb_message),
            utils.has_text(byoeb_message),
            has_tpl,
            utils.has_audio_additional_info(byoeb_message),
        )
        if utils.has_interactive_button_additional_info(byoeb_message):
            _log.debug("[prepare_requests] Preparing interactive button message")
            wa_interactive_button_message = wa_req_payload.get_whatsapp_interactive_button_request_from_byoeb_message(byoeb_message)
            wa_requests.append(wa_interactive_button_message)
        elif utils.has_interactive_list_additional_info(byoeb_message):
            _log.debug("[prepare_requests] Preparing interactive list message")
            wa_interactive_list_message = wa_req_payload.get_whatsapp_interactive_list_request_from_byoeb_message(byoeb_message)
            wa_requests.append(wa_interactive_list_message)
        elif utils.has_text(byoeb_message):
            _log.debug("[prepare_requests] Preparing text message")
            wa_text_message = wa_req_payload.get_whatsapp_text_request_from_byoeb_message(byoeb_message)
            wa_requests.append(wa_text_message)
        else:
            _log.debug("[prepare_requests] No text/button/list; will add template if present")
        if has_tpl:
            _log.info("[prepare_requests] Preparing template message (e.g. DYK)")
            wa_template_message = wa_req_payload.get_whatsapp_template_request_from_byoeb_message(byoeb_message)
            _log.debug("Whatsapp template message %s", json.dumps(wa_template_message))
            wa_requests.append(wa_template_message)
        elif not wa_requests and msg_type in ("template_text", "template"):
            # Fallback: template-only message (e.g. DYK) when additional_info has template keys
            info = getattr(msg_ctx, "additional_info", None) or (msg_ctx.get("additional_info") if isinstance(msg_ctx, dict) else None)
            if info and all(k in info for k in ("template_name", "template_language", "template_parameters")):
                _log.info("[prepare_requests] Preparing template message (fallback for template_text)")
                wa_template_message = wa_req_payload.get_whatsapp_template_request_from_byoeb_message(byoeb_message)
                wa_requests.append(wa_template_message)
        if utils.has_audio_additional_info(byoeb_message):
            wa_audio_message = wa_req_payload.get_whatsapp_audio_request_from_byoeb_message(byoeb_message)
            if wa_audio_message is not None:
                wa_requests.append(wa_audio_message)
        if byoeb_message.message_context and byoeb_message.message_context.additional_info and constants.INTEGRATION_ID in byoeb_message.message_context.additional_info:
            for request in wa_requests:
                request[self.__passover_integration_id] = byoeb_message.message_context.additional_info[constants.INTEGRATION_ID]
        return wa_requests
    
    async def amark_read(
        self,
        messages: List[ByoebMessageContext]
    ):
        task_params = []
        for message in messages:
            if message.message_context and message.message_context.additional_info and (
                (msg_id := message.message_context.message_id) and
                (integration_id := message.message_context.additional_info.get(constants.INTEGRATION_ID))
            ):
                task_params.append((str(integration_id), msg_id))

        if not task_params:
            return

        from byoeb.services.auth.auth_service import get_auth_service
        auth_service = await get_auth_service()
        integrations = await auth_service.fetch_integrations(list({iid for iid, _ in task_params}))
        clients = {str(i.id): await self.__channel_client_factory.get(self.__client_type, i.identifier) for i in integrations}
        missing = [iid for iid, _ in task_params if iid not in clients]
        if missing:
            _logger.warning(
                "amark_read: skipping %d message(s) — integration IDs not found: %s",
                len(missing),
                missing,
            )
        await asyncio.gather(*[clients[iid].amark_as_read(msg_id) for iid, msg_id in task_params if iid in clients])
    
    async def _resolve_clients(self, requests: List[Dict[str, Any]]) -> List[tuple[AsyncWhatsAppClient, Dict[str, Any]]]:
        from byoeb.services.auth.auth_service import get_auth_service
        from byoeb.chat_app.configuration.dependency_setup import users_handler

        user_service = await users_handler.get_or_create_user_service()
        users_raw = await user_service.aget(list({req["to"] for req in requests}))
        recipient_to_tenant = {str(u["phone_number_id"]): u["tenant_id"] for u in users_raw if isinstance(u, dict) and u.get("tenant_id")}
        
        auth_service = await get_auth_service()
        integrations1, integrations2 = await asyncio.gather(
            auth_service.fetch_integrations_by_tenants(self.__client_type, list({uuid.UUID(str(tid)) for tid in set(recipient_to_tenant.values()) if tid is not None})),
            auth_service.fetch_integrations([req[self.__passover_integration_id] for req in requests if self.__passover_integration_id in req])
        )
        all_integrations = [*integrations1, *integrations2]
        integrations_to_client = {str(i.id): await self.__channel_client_factory.get(self.__client_type, i.identifier) for i in all_integrations}

        # Build tenant→client map only for tenants with exactly one integration.
        # Tenants with multiple integrations must use an explicit _byoeb_integration_id; a
        # dict-overwrite fallback would silently pick the wrong integration.
        tenant_integration_ids: dict = {}
        for i in all_integrations:
            tenant_integration_ids.setdefault(i.tenant_id, []).append(str(i.id))
        tenant_to_client = {
            tid: integrations_to_client[int_ids[0]]
            for tid, int_ids in tenant_integration_ids.items()
            if len(int_ids) == 1
        }

        resolved_clients = []
        for request in requests:
            if self.__passover_integration_id in request and request[self.__passover_integration_id] in integrations_to_client:
                client = integrations_to_client[request[self.__passover_integration_id]]
            else:
                tenant_id = recipient_to_tenant.get(request["to"])
                client = tenant_to_client.get(tenant_id) if tenant_id else None
                if client is None and tenant_id and tenant_id in {i.tenant_id for i in all_integrations}:
                    tenant_ids = tenant_integration_ids.get(tenant_id, [])
                    _logger.error(
                        "_resolve_clients: tenant %s has %d WhatsApp integrations but no explicit %s provided for user %s; integration IDs: %s",
                        tenant_id,
                        len(tenant_ids),
                        self.__passover_integration_id,
                        request["to"],
                        tenant_ids,
                    )
                    raise Exception(
                        f"Tenant has multiple WhatsApp integrations; explicit {self.__passover_integration_id} required for user: {request['to']}"
                    )
            if client:
                resolved_clients.append((client, request))
            else:
                _logger.error(
                    "_resolve_clients: no WhatsApp client found for user %s (tenant=%s)",
                    request["to"],
                    recipient_to_tenant.get(request["to"]),
                )
                raise Exception(f"No WhatsApp client found for user: {request['to']}")
        return resolved_clients
    
    async def send_requests(
        self,
        requests: List[Dict[str, Any]]
    ) -> Tuple[List[WhatsAppResponse], List[Optional[str]]]:
        resolved_clients = await self._resolve_clients(requests)
        tasks = [client.asend_batch_messages([request], request["type"]) for client, request in resolved_clients]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        responses: List[WhatsAppResponse] = []
        message_ids: List[Optional[str]] = []
        logger = logging.getLogger(__name__)
        for (_, request), result in zip(resolved_clients, results):
            if isinstance(result, Exception):
                logger.exception(
                    "Failed to send WhatsApp request to '%s' with type '%s'.",
                    request.get("to"),
                    request.get("type"),
                    exc_info=(type(result), result, result.__traceback__),
                )
                message_ids.append(None)
                continue

            responses.extend(result)
            message_ids.extend(
                response.messages[0].id if response.messages and response.messages[0].id else None
                for response in result
            )

        logger.debug("WhatsApp responses %s", responses)
        return responses, message_ids
    
    def create_conv(
        self,
        byoeb_user_message: ByoebMessageContext,
        responses: List[WhatsAppResponse]
    ) -> List[ByoebMessageContext]:
        bot_to_user_messages = []
        for response in responses:
            media_info = None
            message_type = None
            if response.media_message is not None:
                media_info = MediaContext(
                    media_id=response.media_message.id
                )
                message_type = MessageTypes.REGULAR_AUDIO.value
            elif byoeb_user_message.message_context.message_type == MessageTypes.INTERACTIVE_LIST.value:
                message_type = MessageTypes.INTERACTIVE_LIST.value
            elif byoeb_user_message.message_context.message_type == MessageTypes.INTERACTIVE_BUTTON.value:
                message_type = MessageTypes.INTERACTIVE_BUTTON.value
            
            byoeb_message = ByoebMessageContext( 
                channel_type=byoeb_user_message.channel_type,
                message_category=byoeb_user_message.message_category,
                user=User(
                    user_id=byoeb_user_message.user.user_id,
                    user_type=byoeb_user_message.user.user_type,
                    user_language=byoeb_user_message.user.user_language,
                    test_user=byoeb_user_message.user.test_user,
                    phone_number_id=byoeb_user_message.user.phone_number_id,
                ),
                message_context=MessageContext(
                    message_id=response.messages[0].id,
                    message_type=message_type,
                    message_english_text=byoeb_user_message.message_context.message_english_text,
                    message_source_text=byoeb_user_message.message_context.message_source_text,
                    additional_info=byoeb_user_message.message_context.additional_info,
                    media_info=media_info
                ),
                reply_context=ReplyContext(
                    reply_id=byoeb_user_message.reply_context.reply_id,
                    reply_type=byoeb_user_message.reply_context.reply_type,
                    reply_source_text=byoeb_user_message.reply_context.reply_source_text,
                    reply_english_text=byoeb_user_message.reply_context.reply_english_text,
                    media_info=byoeb_user_message.reply_context.media_info,
                    additional_info=byoeb_user_message.reply_context.additional_info
                ),
                incoming_timestamp=byoeb_user_message.incoming_timestamp,
                outgoing_timestamp=str(int(datetime.now(timezone.utc).timestamp()))
            )
            bot_to_user_messages.append(byoeb_message)
        return bot_to_user_messages
    
    def create_consensus_cross_conv(
        self,
        byoeb_user_message: ByoebMessageContext,
        byoeb_expert_message: ByoebMessageContext,
        expert_response: WhatsAppResponse
    ):
        """
        Create cross conversation context for consensus messages
        This is only for background jobs
        """
        message_context = MessageContext(
            message_id=byoeb_user_message.message_context.message_id,
        )
        reply_context = ReplyContext(
            reply_id=byoeb_user_message.reply_context.reply_id,
        )
        cross_conversation_context = {
            constants.USER: User(
                user_id=byoeb_user_message.user.user_id,
                user_type=byoeb_user_message.user.user_type,
                user_language=byoeb_user_message.user.user_language,
                test_user=byoeb_user_message.user.test_user,
                phone_number_id=byoeb_user_message.user.phone_number_id,
            ),
            constants.MESSAGES_CONTEXT: [
                ByoebMessageContext(
                    channel_type=byoeb_user_message.channel_type,
                    message_context=message_context,
                    reply_context=reply_context
                )
            ]
        }
        bot_to_expert_message = ByoebMessageContext(
            channel_type=byoeb_expert_message.channel_type,
            message_category=byoeb_expert_message.message_category,
            user=byoeb_expert_message.user,
            message_context=MessageContext(
                message_id=expert_response.messages[0].id,
                message_type=byoeb_expert_message.message_context.message_type,
                message_english_text=byoeb_expert_message.message_context.message_english_text,
                message_source_text=byoeb_expert_message.message_context.message_source_text,
                additional_info={}
            ),
            cross_conversation_context=cross_conversation_context,
            incoming_timestamp=byoeb_expert_message.incoming_timestamp,
            outgoing_timestamp=str(int(datetime.now(timezone.utc).timestamp()))
        )
        return bot_to_expert_message
    
    def create_cross_conv(
        self,
        byoeb_user_message: ByoebMessageContext,
        byoeb_expert_message: ByoebMessageContext,
        user_responses: List[WhatsAppResponse],
        expert_responses: List[WhatsAppResponse]
    ):
        user_messages_context = []
        for user_response in user_responses:
            message_type = MessageTypes.INTERACTIVE_LIST.value
            if user_response.media_message is not None:
                message_type = MessageTypes.REGULAR_AUDIO.value
            message_context = MessageContext(
                message_id=user_response.messages[0].id,
                message_type=message_type,
                additional_info=byoeb_user_message.message_context.additional_info
            )
            reply_context = ReplyContext(
                reply_id=byoeb_user_message.reply_context.reply_id,
            )
            user_message_context = ByoebMessageContext(
                channel_type=byoeb_user_message.channel_type,
                message_context=message_context,
                reply_context=reply_context
            )
            user_messages_context.append(user_message_context)
        
        cross_conversation_context = {
            constants.USER: User(
                    user_id=byoeb_user_message.user.user_id,
                    user_type=byoeb_user_message.user.user_type,
                    user_language=byoeb_user_message.user.user_language,
                    test_user=byoeb_user_message.user.test_user,
                    phone_number_id=byoeb_user_message.user.phone_number_id,
                ),
            constants.MESSAGES_CONTEXT: user_messages_context
        }
        bot_to_expert_messages = []
        for expert_response in expert_responses:
            byoeb_message = ByoebMessageContext( 
                channel_type=byoeb_expert_message.channel_type,
                message_category=byoeb_expert_message.message_category,
                user=byoeb_expert_message.user,
                message_context=MessageContext(
                    message_id=expert_response.messages[0].id,
                    message_type=byoeb_expert_message.message_context.message_type,
                    message_english_text=byoeb_expert_message.message_context.message_english_text,
                    message_source_text=byoeb_expert_message.message_context.message_source_text,
                    additional_info=byoeb_expert_message.message_context.additional_info
                ),
                cross_conversation_context=cross_conversation_context,
                incoming_timestamp=byoeb_expert_message.incoming_timestamp,
                outgoing_timestamp=str(int(datetime.now(timezone.utc).timestamp()))
            )
            bot_to_expert_messages.append(byoeb_message)
            
        return bot_to_expert_messages
