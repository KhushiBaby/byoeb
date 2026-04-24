import asyncio
import time
import base64
import hashlib
import logging
from byoeb.constants.feature_enums import FeatureFlag
from byoeb.services.chat.utils import clean_message_for_console
import byoeb.services.chat.constants as constants
import re
from byoeb.utils.embedding_cache import CacheResult, EmbeddingCache
import byoeb.utils.utils as utils
import random
from rapidfuzz.fuzz import ratio
from tenacity import retry, stop_after_attempt, wait_exponential, RetryError
from typing import Iterable, List, Dict, Any, Optional
from byoeb.chat_app.configuration.config import bot_config, app_config
import byoeb.chat_app.configuration.config as env_config
from byoeb.models.message_category import MessageCategory
from byoeb_core.models.vector_stores.chunk import Chunk, Chunk_metadata
from byoeb_core.models.byoeb.message_context import (
    ByoebMessageContext,
    MessageContext,
    ReplyContext,
    MessageTypes
)
from byoeb_integrations.embeddings.llama_index.openai import OpenAIEmbed
from byoeb_integrations.vector_stores.azure_vector_search.azure_vector_search import AzureVectorSearchType
from byoeb_core.models.byoeb.user import User
from byoeb.services.chat.message_handlers.base import Handler
from byoeb.chat_app.configuration.dependency_setup import langfuse, llm_client
from byoeb.chat_app.configuration.config import env_ashabot_message_cache_capacity, feature_flags
from byoeb.application_logger.azure_app_insights import AppInsightsLogHandler
from langfuse.media import LangfuseMedia
from langfuse.api import MediaContentType


logger = logging.getLogger(__name__)

embedding_cap = int(env_ashabot_message_cache_capacity or 64)
embedding_fn = (
    OpenAIEmbed(model="text-embedding-3-small", dimensions=768, api_key=env_config.env_openai_api_key).get_embedding_function()
    if env_config.env_openai_api_key and embedding_cap > 0 else None
)

class ByoebUserGenerateResponse(Handler):
    AUDIO_MODALITY = "audio"
    TEXT_MODALITY = "text"
    EXPERT_PENDING_EMOJI = app_config["channel"]["reaction"]["expert"]["pending"]
    USER_PENDING_EMOJI = app_config["channel"]["reaction"]["user"]["pending"]
    _expert_user_types = bot_config["expert"]
    _regular_user_types = bot_config["regular"]["user_type"]
    _asha_work_related = "asha_work_related"
    _small_talk = "small_talk"
    _incomprehensible = "incomprehensible"
    embedding_cache = EmbeddingCache("message-consumer", dim=768, capacity=embedding_cap)

    def __init__(self, successor=None):
        super().__init__(successor)

    def get_retrieval_type(self, chunk: Chunk) -> str | None:
        return chunk.metadata.additional_metadata.get("retrieval_type") if chunk.metadata and chunk.metadata.additional_metadata else None

    def annotate_retrieval_type(self, chunk: Chunk, retrieval_type: str):
        if chunk.metadata is None: chunk.metadata = Chunk_metadata()
        if chunk.metadata.additional_metadata is None: chunk.metadata.additional_metadata = {}
        chunk.metadata.additional_metadata["retrieval_type"] = retrieval_type

    async def __aretrieve_chunks(
        self,
        text,
        k,
        search_type
    ) -> List[Chunk]:
        """
        Retrieve top k chunks from the vector store based on the input text.
        
        Args:
            text (str): The input text to search for relevant chunks.
            k (int): The number of top chunks to retrieve.
            search_type (str): The type of search to perform (default is HYBRID).
        
        Returns:
            List[Chunk]: A list of retrieved chunks containing relevant information.
        
        This method uses the AzureVectorSearchType.HYBRID search type by default,
        which combines both dense and sparse search methods to find relevant chunks.
        The retrieved chunks include fields such as id, text, metadata, and related questions.
        """
        from byoeb.chat_app.configuration.dependency_setup import vector_store
        start_time = time.perf_counter()
        with langfuse.start_as_current_observation(as_type="retriever", name="search-documents", input=text, metadata={"k": k, "search_type": search_type}) as span:
            retrieved_chunks = await vector_store.retrieve_top_k_chunks(
                text,
                k,
                search_type=search_type,
                select=["id", "text", "metadata"],
                vector_field="text_vector_3072"
            )
            span.update(output=[c.model_dump(include={"chunk_id", "text", "metadata", "similarity"}) for c in retrieved_chunks])
        end_time = time.perf_counter()
        for chunk in retrieved_chunks:
            self.annotate_retrieval_type(chunk, search_type)
        utils.log_to_text_file(f"Retrieved chunks in {end_time - start_time} seconds")
        return retrieved_chunks
    
    async def _retrieve_top_k_chunks_for_related_questions(
        self,
        text,
        k
    ) -> List[Chunk]:
        """
        Retrieve top k chunks for related questions based on the input text.
        Uses the AzureVectorSearchType.DENSE search type to find relevant chunks.
        
        Args:
            text (str): The input text to search for related questions.
            k (int): The number of top chunks to retrieve.
        
        Returns:
            List[Chunk]: A list of retrieved chunks containing related questions.
        """
        from byoeb.chat_app.configuration.dependency_setup import vector_store
        start_time = time.perf_counter()
        with langfuse.start_as_current_observation(as_type="span", name="related-questions", input=text, metadata={"k": k}) as span:
            retrieved_chunks = await vector_store.retrieve_top_k_chunks(
                text,
                k,
                search_type=AzureVectorSearchType.DENSE.value,
                select=["id", "related_questions"],
                vector_field="text_vector_3072"
            )
            span.update(output=[c.model_dump(include={"chunk_id", "related_questions", "similarity"}) for c in retrieved_chunks])
        end_time = time.perf_counter()
        utils.log_to_text_file(f"Retrieved chunks for related questions in {end_time - start_time} seconds")
        return retrieved_chunks
    
    def _get_system_prompt(self, user_language: str) -> str:
        task_description = bot_config["llm_response"]["answer_prompts"]["system_prompt"]["task_description"]
        response_generate = bot_config["llm_response"]["answer_prompts"]["system_prompt"]["response_generate"]
        response_translate = bot_config["llm_response"]["answer_prompts"]["system_prompt"]["response_translate"][user_language]
        output = bot_config["llm_response"]["answer_prompts"]["system_prompt"]["output"]
        system_prompt = task_description + "\n" + response_generate + "\n" + response_translate + "\n" + output
        return system_prompt
      
    def __augment(
        self,
        system_prompt,
        user_prompt
    ):
        augmented_prompts = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ]
        return augmented_prompts
    
    def __get_expert_additional_info(
        self,
        texts: List[str],
        emoji = None,
        status = None
    ):
        additional_info = {
            constants.EMOJI: emoji,
            constants.VERIFICATION_STATUS: status,
            "button_titles": bot_config["template_messages"]["expert"]["verification"]["button_titles"],
            "template_name": bot_config["channel_templates"]["expert"]["verification"],
            "template_language": "en",  
            "template_parameters": texts
        }
        return additional_info
    
    def __get_expert_number_and_type(
        self,
        experts: Dict[str, List[Any]],
        query_type = "medical"
    ):
        expert_type = self._expert_user_types.get(query_type)
        if experts is None:
            return None, None
        if expert_type not in experts:
            return None, None
        return experts[expert_type][0], expert_type
    
    def __create_read_reciept_message(
        self,
        message: ByoebMessageContext,
    ) -> ByoebMessageContext:
        read_reciept_message = ByoebMessageContext(
            channel_type=message.channel_type,
            message_category=MessageCategory.READ_RECEIPT.value,
            message_context=MessageContext(
                message_id=message.message_context.message_id,
            )
        )
        return read_reciept_message
    
    def __get_idk_status(
        self,
        message: ByoebMessageContext,
        query_type: str
    ):
        source_text = message.message_context.message_source_text
        logger.debug("IDK Message source text: %s", source_text)
        logger.debug("IDK Query type: %s", query_type)
        template_idk = bot_config["template_messages"]["user"]["audio"]["idk"][query_type]
        if query_type != self._incomprehensible and query_type != self._asha_work_related:
            return {}
        if message.reply_context.message_category == MessageCategory.AUDIO_IDK.value:
            user_lang = message.user.user_language
            options = template_idk["interactive"]["options"][user_lang]
            if source_text == options[0]:
                return {
                    constants.STATUS: constants.RESOLVED
                }
            if source_text == options[1]:
                return {
                    constants.STATUS: constants.WAITING
                }
        
        return {}
        
    def __get_idk_response(
        self,
        message: ByoebMessageContext,
        response_text: str,
        query_type: str,
    ):
        modality = None
        message_type = message.message_context.message_type
        query = message.message_context.message_source_text
        user_language = message.user.user_language
        if (message_type == MessageTypes.REGULAR_AUDIO.value
           or message.reply_context.message_category == MessageCategory.AUDIO_IDK.value
        ):
            modality = self.AUDIO_MODALITY
        elif (message_type == MessageTypes.REGULAR_TEXT.value
            or message_type == MessageTypes.INTERACTIVE_LIST.value
        ):
            modality = self.TEXT_MODALITY
        logger.debug("Modality: %s", modality)
        logger.debug("Query: %s", query)
        template_idk = bot_config["template_messages"]["user"][modality]["idk"][query_type]
        if response_text == constants.IDK and modality == self.AUDIO_MODALITY:
            status = message.reply_context.additional_info.get(constants.STATUS)
            if status == constants.WAITING:
                return template_idk["waiting"][user_language], None, True
            if status == constants.RESOLVED:
                return template_idk["resolved"][user_language], None, True
            options = template_idk["interactive"]["options"][user_language]
            if query == options[0]:
                return template_idk["ask_again"][user_language], None, True
            if query == options[1]:
                return template_idk["send"][user_language], None, True
            return template_idk["pending"][user_language], None, True
        if query_type == self._incomprehensible or query_type == self._asha_work_related:
            if modality == self.AUDIO_MODALITY:
                options = template_idk["interactive"]["options"][user_language]
                text = template_idk["interactive"]["text"][user_language].replace(
                    "<query>",
                    query
                )
                return text, options, False
            if modality == self.TEXT_MODALITY:
                text = template_idk[user_language]
                return text, None, True
        elif query_type == self._small_talk:
            text = template_idk[user_language].replace(
                "<query>",
                query
            )
            return text, None, True
        return None, None, False
    
    def __create_reply_context(
        self,
        message: ByoebMessageContext
    ):
        if message.reply_context.message_category == MessageCategory.AUDIO_IDK.value:
            user_language = message.user.user_language
            query = message.message_context.message_source_text
            query_type = message.reply_context.additional_info.get(constants.QUERY_TYPE)
            template_idk = bot_config["template_messages"]["user"]["audio"]["idk"][query_type]
            options = template_idk["interactive"]["options"][user_language]
            reply_id = message.reply_context.reply_id
            status = message.reply_context.additional_info.get(constants.STATUS, None)
            message.reply_context.additional_info[constants.TRACK_MESSAGE_ID] = message.message_context.message_id
            if status == constants.PENDING and query == options[0]:
                message.reply_context.additional_info[constants.STATUS] = constants.RESOLVED
            elif status == constants.PENDING and query == options[1]:
                message.reply_context.additional_info[constants.STATUS] = constants.WAITING
            else:
                reply_id = message.reply_context.additional_info.get(constants.BOT_AUDIO_IDK_MESSAGE_ID)
                message.reply_context.additional_info[constants.STATUS] = None
            return ReplyContext(
                message_category=MessageCategory.AUDIO_IDK.value,
                reply_id=reply_id,
                reply_type=message.reply_context.reply_type,
                reply_english_text=message.reply_context.reply_english_text,
                reply_source_text=message.reply_context.reply_source_text,
                additional_info=message.reply_context.additional_info,
                media_info=message.reply_context.media_info
            )
        return ReplyContext(
            reply_id=message.message_context.message_id,
            reply_type=message.message_context.message_type,
            reply_english_text=message.message_context.message_english_text,
            reply_source_text=message.message_context.message_source_text,
            media_info=message.message_context.media_info,
            additional_info=message.message_context.additional_info
        )

    async def __create_source_audio(
        self,
        message_source_text: str,
        user: User
    ):
        from byoeb.chat_app.configuration.dependency_setup import speech_translator
        with langfuse.start_as_current_observation(as_type="span", name="text-to-speech", input=message_source_text) as span:
            translated_audio_message = await speech_translator.atext_to_speech(
                input_text=message_source_text,
                source_language=user.user_language,
                test_user=user.test_user
            )
            span.update(output=LangfuseMedia(content_bytes=translated_audio_message, content_type=MediaContentType.AUDIO_OGG))
        return {
            constants.DATA: base64.b64encode(translated_audio_message).decode("utf-8"),
            constants.MIME_TYPE: "audio/ogg",
        }
    
    async def __create_source_text(
        self,
        message: ByoebMessageContext,
        response_text: str,
        query_type: str,
    ):
        from byoeb.chat_app.configuration.dependency_setup import text_translator
        if utils.is_idk(response_text) or query_type == self._incomprehensible:
            return self.__get_idk_response(
                message=message,
                response_text=response_text,
                query_type=query_type,
            )

        with langfuse.start_as_current_observation(as_type="span", name="translation", input=response_text) as span:
            source_text = await text_translator.atranslate_text(
                input_text=response_text,
                source_language="en",
                target_language=message.user.user_language
            )
            span.update(output=source_text)
        return source_text, None, True
    
    async def __create_user_message(
        self,
        message: ByoebMessageContext,
        query_type: str,
        response_en: str,
        response_source: str = None,
        related_questions: List[str] = None,
        emoji = None,
        status = None,
        cache_details: CacheResult = (None, None, None),  # used for audio cache
        cache_hit: bool = False,
        default_message_category: Optional[MessageCategory] = None
    ) -> ByoebMessageContext:
        start_time = time.perf_counter()
        user_language = message.user.user_language
        
        # Use canned responses with user.language - no need to detect script/language
        # If response_source is provided, use it directly (it's from canned templates)
        # If response_source is None or empty, translate from response_en to user's language
        if utils.is_idk(response_en):
            message_source_text, options, send_related_questions = self.__get_idk_response(
                message=message,
                response_text=response_en,
                query_type=query_type,
            )
        elif response_source is None or (isinstance(response_source, str) and not response_source.strip()):
            # If no response_source, translate from response_en to user's language
            logger.debug("[__create_user_message] response_source is None or empty, translating from response_en to %s", user_language)
            message_source_text, options, send_related_questions = await self.__create_source_text(
                message=message,
                response_text=response_en,
                query_type=query_type,
            )
        else:
            # Use provided response_source - it's from canned templates in the correct language
            message_source_text = response_source
            options = None
            send_related_questions = True
            logger.debug("[__create_user_message] Using provided response_source from canned templates for language %s: '%s...'", user_language, (message_source_text or "")[:100])
        logger.debug("Options: %s", options)
        end_time = time.perf_counter()
        utils.log_to_text_file(f"Translated response message in {end_time - start_time} seconds")
        langfuse.update_current_trace(output=message_source_text, metadata={"related_questions": related_questions})

        cache_info = {"cache_score": cache_details[0]} if cache_details[0] is not None else {}
        if cache_hit:
            cache_info["cache_hit"] = cache_hit
        if cache_details[1] is not None:
            cache_id = cache_details[1]
            cache = cache_details[2]
        else:
            cache_id, cache = None, None

        media_info = cache.get("media_info", {}).get(user_language) if cache is not None else None
        if media_info is None:
            start_time = time.perf_counter()
            media_info = await self.__create_source_audio(
                message_source_text=message_source_text,
                user=message.user
            )
            end_time = time.perf_counter()
            AppInsightsLogHandler.getLogger("text_to_audio").info(f"Created audio response message in {end_time - start_time} seconds", extra={AppInsightsLogHandler.DETAILS: {
                "message_id": message.message_context.message_id,
                "time_taken": end_time - start_time
            }})
            if cache_id is not None:
                assert cache is not None
                if "media_info" not in cache:
                    cache["media_info"] = {user_language: media_info}
                else:
                    cache[user_language] = media_info
                try:
                    self.embedding_cache.update(cache_id, cache)
                except Exception as e:
                    logger.warning("Embedding cache update failed: %s. Continuing without cache update.", e)

        utils.log_to_text_file(f"Created audio response message in {end_time - start_time} seconds")
        description = bot_config["template_messages"]["user"]["follow_up_questions_description"][user_language]
        message_type = None
        message_category = (default_message_category or MessageCategory.BOT_TO_USER_RESPONSE).value
        logger.debug("[__create_user_message] Determining message_type. Incoming message_type: '%s'", message.message_context.message_type)
        if (message.message_context.message_type == MessageTypes.REGULAR_AUDIO.value):
            message_type = MessageTypes.REGULAR_AUDIO.value
            logger.debug("[__create_user_message] Set message_type to REGULAR_AUDIO (incoming was audio)")
        elif (message.message_context.message_type == MessageTypes.REGULAR_TEXT.value
              or message.message_context.message_type == MessageTypes.INTERACTIVE_LIST.value
              or message.message_context.message_type == MessageTypes.INTERACTIVE_BUTTON.value):
            message_type = MessageTypes.INTERACTIVE_LIST.value
            logger.debug("[__create_user_message] Set message_type to INTERACTIVE_LIST (incoming was text/interactive)")
        button_reply_additional_info = {}
        interactive_list_additional_info = {}
        text_additional_info = {}
        idk_status = self.__get_idk_status(message, query_type)
        if utils.is_idk(response_en) and message.message_context.message_type == MessageTypes.REGULAR_AUDIO.value:
            message_type = MessageTypes.INTERACTIVE_BUTTON.value
            message_category = MessageCategory.AUDIO_IDK.value
            button_reply_additional_info = {
                constants.BUTTON_TITLES: options,
                constants.ROW_TEXTS: related_questions,
                constants.QUERY_TYPE: query_type,
            }
            idk_status = {
                constants.STATUS: constants.PENDING
            }
        elif (utils.is_idk(response_en)
              and (
                    message.message_context.message_type == MessageTypes.REGULAR_TEXT.value
                    or message.message_context.message_type == MessageTypes.INTERACTIVE_LIST.value
              )
        ):
            if query_type == self._asha_work_related:
                idk_status = {
                    constants.STATUS: constants.WAITING
                }
            message_type = MessageTypes.INTERACTIVE_LIST.value
            message_category = MessageCategory.TEXT_IDK.value
            interactive_list_additional_info = {
                constants.DESCRIPTION: description,
                constants.ROW_TEXTS: related_questions,
                constants.QUERY_TYPE: query_type
            }
            
        elif (message.message_context.message_type == MessageTypes.INTERACTIVE_BUTTON.value
            and not send_related_questions
        ):
            message_type = MessageTypes.REGULAR_TEXT.value
            message_category = MessageCategory.BOT_TO_USER_RESPONSE.value
            text_additional_info = {
                constants.ROW_TEXTS: related_questions,
                constants.QUERY_TYPE: query_type
            }
        else:
            # Default case: for normal text messages, use INTERACTIVE_LIST to show related questions
            if message_type is None:
                # If message_type wasn't set above, default to INTERACTIVE_LIST for text messages
                # This ensures text responses are sent as text with interactive list (related questions)
                if message.message_context.message_type != MessageTypes.REGULAR_AUDIO.value:
                    message_type = MessageTypes.INTERACTIVE_LIST.value
                    logger.debug("[__create_user_message] Set message_type to INTERACTIVE_LIST (default for non-audio)")
                else:
                    # If it was an audio message, keep it as audio
                    message_type = MessageTypes.REGULAR_AUDIO.value
                    logger.debug("[__create_user_message] Set message_type to REGULAR_AUDIO (default for audio)")
            interactive_list_additional_info = {
                constants.DESCRIPTION: description,
                constants.ROW_TEXTS: related_questions,
                constants.QUERY_TYPE: query_type
            }
        
        logger.debug("[__create_user_message] Final message_type: '%s', message_category: '%s'; related_questions count: %s", message_type, message_category, len(related_questions) if related_questions else 0)
        reply_context= self.__create_reply_context(message)
        user_message = ByoebMessageContext(
            channel_type=message.channel_type,
            message_category=message_category,
            user=User(
                user_id=message.user.user_id,
                user_language=user_language,
                user_type=self._regular_user_types[0],
                phone_number_id=message.user.phone_number_id,
                last_conversations=message.user.last_conversations
            ),
            message_context=MessageContext(
                message_type=message_type,
                message_source_text=message_source_text,  # This should be in user's language (Hindi, etc.)
                message_english_text=response_en,  # This is the English translation for internal use
                additional_info={
                    **media_info,
                    **button_reply_additional_info,
                    **interactive_list_additional_info,
                    **text_additional_info,
                    **idk_status,
                    **cache_info
                }
            ),
            reply_context=reply_context,
            incoming_timestamp=message.incoming_timestamp,
        )
        logger.debug("[__create_user_message] Final message - category: %s; source_text: '%s...'; english_text: '%s...'", user_message.message_category, (user_message.message_context.message_source_text or "")[:100], (user_message.message_context.message_english_text or "")[:100])
        return user_message
    
    def __create_expert_verification_message(
        self,
        message: ByoebMessageContext,
        response_text: str,
        query_type = "medical",
        emoji = None,
        status = None,
    ) -> ByoebMessageContext:
        
        expert_phone_number_id , expert_type= self.__get_expert_number_and_type(message.user.experts, query_type)
        if expert_phone_number_id is None:
            return None
        expert_user_id = hashlib.md5(expert_phone_number_id.encode()).hexdigest()
        verification_question_template = bot_config["template_messages"]["expert"]["verification"]["Question"]
        verification_bot_answer_template = bot_config["template_messages"]["expert"]["verification"]["Bot_Answer"]
        verification_question = verification_question_template.replace(
            "<QUESTION>",
            message.message_context.message_english_text
        )
        verification_bot_answer = verification_bot_answer_template.replace(
            "<ANSWER>",
            response_text
        )
        verification_footer_message = bot_config["template_messages"]["expert"]["verification"]["footer"]
        additional_info = self.__get_expert_additional_info(
            [verification_question, verification_bot_answer],
            emoji,
            status
        )
        expert_message = verification_question + "\n" + verification_bot_answer + "\n" + verification_footer_message
        new_expert_verification_message = ByoebMessageContext(
            channel_type=message.channel_type,
            message_category=MessageCategory.BOT_TO_EXPERT_VERIFICATION.value,
            user=User(
                user_id=expert_user_id,
                user_type=expert_type,
                user_language='en',
                phone_number_id=expert_phone_number_id
            ),
            message_context=MessageContext(
                message_type=MessageTypes.INTERACTIVE_BUTTON.value,
                message_source_text=expert_message,
                message_english_text=expert_message,
                additional_info=additional_info
            ),
            incoming_timestamp=message.incoming_timestamp,
        )
        return new_expert_verification_message

    def filter_retrieved_chunks(self, retrieved_chunks: Iterable[Chunk], thresholds: dict[str, float]) -> Iterable[Chunk]:
        return [
            chunk for chunk in retrieved_chunks
            if chunk.similarity >= thresholds.get(self.get_retrieval_type(chunk) or "", 0) and
            chunk.text and len(re.sub(r'\W+', '', chunk.text)) > 0
        ]

    def _chunks_to_kb_topics(self, chunks: Iterable[Chunk]) -> str:
        return "\n".join("\n".join([
            f"<chunk_{i}>",
            f"<score>{chunk.similarity:.2f}</score>",
            f"<text>{chunk.text}</text>",
            f"<search_type>{self.get_retrieval_type(chunk) or 'unknown'}</search_type>",
            f"</chunk_{i}>"
        ]) for i, chunk in enumerate(chunks))

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, max=10))
    async def agenerate_answer(
        self,
        user_language: str,
        query: str,
        query_type: str,
        vector_search_queries: list[tuple[str | None, str, int]] | None = None
    ) -> tuple[str, str, dict[str, int], list[Chunk]]:
        def parse_response_xml(xml_string: str) -> tuple[str, str]:
            # "If ANY of the following conditions are true, respond with <response_idk>I do not know the answer to your question.</response_idk> in these exact words"
            if (match := re.search(r"<response_idk\s*>(.*?)</response_idk\s*>", xml_string, re.DOTALL | re.IGNORECASE)) is not None:
                response = match.group(1).strip()
                return response, response

            patterns = {
                "response_en": r"<response_en\s*>(.*?)</response_en\s*>",
                "response_src": r"<response_src\s*>(.*?)</response_src\s*>",
            }
            extracted_data = {}
            for key, pattern in patterns.items():
                match = re.search(pattern, xml_string, re.DOTALL | re.IGNORECASE)  # Supports multiline and case-insensitive matches
                if match is None or not match.group(1).strip(): raise ValueError("Parsing failed, did not match pattern '%s' (%s) in output" % (pattern, key))
                extracted_data[key] = match.group(1).strip()  # Strip removes extra spaces and newlines

            return extracted_data["response_en"], extracted_data["response_src"]

        vector_search_queries = vector_search_queries or [(query, AzureVectorSearchType.HYBRID.value, 3)]
        
        retrieved_chunks: dict[str, Chunk] = {}
        with langfuse.start_as_current_observation(as_type="span", name="generate-answer", input=query, metadata={"query_type": query_type}) as span:
            for chunks in await asyncio.gather(*(self.__aretrieve_chunks(q or query, k=k, search_type=t) for q, t, k in vector_search_queries)):
                for chunk in chunks:
                    retrieved_chunks[chunk.chunk_id] = chunk

            # Sort chunks deterministically by chunk_id for consistent ordering (deterministic RAG)
            sorted_chunks = sorted(retrieved_chunks.values(), key=lambda c: c.chunk_id)
            retrieved_chunks_list = list(self.filter_retrieved_chunks(sorted_chunks, thresholds=bot_config["retrieval"]["similarity_thresholds"]))
            if not retrieved_chunks_list:
                span.update(output={"response_en": constants.IDK, "response_source": constants.IDK, "is_idk": True})
                return constants.IDK, constants.IDK, {}, list(retrieved_chunks.values())

            update_kb_list = self._chunks_to_kb_topics(chunk for chunk in retrieved_chunks_list if chunk.metadata and "KB Updated" in chunk.metadata.source)
            raw_kb_list = self._chunks_to_kb_topics(chunk for chunk in retrieved_chunks_list if not chunk.metadata or "KB Updated" not in chunk.metadata.source)

            system_prompt = self._get_system_prompt(user_language)
            template_user_prompt = bot_config["llm_response"]["answer_prompts"]["user_prompt"]
            # Replace placeholders with actual values

            user_prompt = template_user_prompt.replace("<QUERY_TYPE>", query_type).replace("<QUERY_EN_ADDCONTEXT>", query).replace("<RAW_KB>", raw_kb_list).replace("<NEW_KB>", update_kb_list)
            augmented_prompts = self.__augment(system_prompt, user_prompt)

            logger.debug("[agenerate_answer] Generating answer for user_language: %s", user_language)
            logger.debug("[agenerate_answer] System prompt language section: %s...", bot_config['llm_response']['answer_prompts']['system_prompt']['response_translate'].get(user_language, 'NOT FOUND')[:200])

            start_time = time.perf_counter()
            llm_response, response_text = await llm_client.generate_response(augmented_prompts)
            response_en, response_source = parse_response_xml(response_text)
            span.update(output={"response_en": response_en, "response_source": response_source, "is_idk": utils.is_idk(response_en)})

        tokens = llm_client.get_response_tokens(llm_response)
        end_time = time.perf_counter()
        utils.log_to_text_file(f"Generated answer tokens and response in {end_time - start_time} seconds: {str(tokens)} {response_text}")
        logger.debug("[agenerate_answer] Generated answer_en: %s...", response_en[:100] if response_en else 'None')
        logger.debug("[agenerate_answer] Generated answer_source (for language %s): %s...", user_language, response_source[:100] if response_source else 'None')
        logger.debug("[agenerate_answer] Query type: %s", query_type)
        return response_en, response_source, tokens, list(retrieved_chunks.values())

    async def needs_clarification(self, query: str, query_type: str, user_language: str, retrieved_chunks: list[Chunk]) -> Optional[tuple[str, str, dict[str, int]]]:
        kb_topics = self._chunks_to_kb_topics(retrieved_chunks)
        task_description = bot_config["llm_response"]["clarification_prompts"]["system_prompt"]
        response_translate = bot_config["llm_response"]["clarification_prompts"]["response_translate"][user_language]
        output_format = bot_config["llm_response"]["clarification_prompts"]["output"]

        system_prompt = task_description + "\n\n" + response_translate + "\n\n" + output_format
        user_prompt = bot_config["llm_response"]["clarification_prompts"]["user_prompt"] \
            .replace("<QUERY>", query) \
            .replace("<QUERY_TYPE>", query_type) \
            .replace("<KB_TOPICS>", kb_topics)

        with langfuse.start_as_current_observation(as_type="span", name="disambiguation", input=query, metadata={
            "query_type": query_type,
            "kb_topics": kb_topics
        }):
            llm_response, response = await llm_client.generate_response(self.__augment(system_prompt, user_prompt))
        response = response.strip()
        tokens = llm_client.get_response_tokens(llm_response)
        if not response:
            return None

        clarification_en_match = re.search(r"<clarification_en\s*>(.*?)</clarification_en\s*>", response, re.DOTALL | re.IGNORECASE)
        clarification_src_match = re.search(r"<clarification_src\s*>(.*?)</clarification_src\s*>", response, re.DOTALL | re.IGNORECASE)

        clarification_en = clarification_en_match.group(1).strip() if clarification_en_match else None
        clarification_src = clarification_src_match.group(1).strip() if clarification_src_match else None
        if not clarification_en or not clarification_src:
            return None

        return clarification_en, clarification_src, tokens

    async def agenerate_expansion_queries(self, original_query: str, retrieved_chunks: list[Chunk]) -> list[str]:
        def parse_expansion_xml(xml_string: str) -> tuple[bool, List[str]]:
            # Check if reformulation is not possible
            cannot_reformulate = re.search(r"<cannot_reformulate\s*>(.*?)</cannot_reformulate\s*>", xml_string, re.DOTALL | re.IGNORECASE)
            if cannot_reformulate:
                reason = cannot_reformulate.group(1).strip()
                return False, [reason]

            # Extract reformulated queries
            pattern = r"<reformulated_query\s*>(.*?)</reformulated_query\s*>"
            matches = re.findall(pattern, xml_string, re.DOTALL | re.IGNORECASE)
            queries = [match.strip() for match in matches if match.strip()]
            return True, queries

        # Prepare retrieved chunks context (limit to avoid token bloat)
        chunks_text = "\n\n---\n\n".join(f"Chunk {i+1}: {chunk.text}" for i, chunk in enumerate(retrieved_chunks))

        system_prompt = bot_config["llm_response"]["expansion_prompts"]["system_prompt"]
        template_user_prompt = bot_config["llm_response"]["expansion_prompts"]["user_prompt"]

        user_prompt = template_user_prompt.replace("<QUERY>", original_query).replace("<RETRIEVED_CHUNKS>", chunks_text)
        augmented_prompts = self.__augment(system_prompt, user_prompt)

        start_time = time.perf_counter()
        with langfuse.start_as_current_observation(as_type="span", name="query-expansion", input=original_query, metadata={
            "retrieved_chunks": [c.text for c in retrieved_chunks]
        }) as span:
            llm_response, response_text = await llm_client.generate_response(augmented_prompts)
            tokens = llm_client.get_response_tokens(llm_response)
            end_time = time.perf_counter()

            utils.log_to_text_file(f"Generated expansion queries in {end_time - start_time} seconds: {str(tokens)}")

            can_reformulate, result = parse_expansion_xml(response_text)
            span.update(output={"can_reformulate": can_reformulate, "result": result})

        logger.debug("Original query: %s", original_query)
        if not can_reformulate:
            logger.debug("Query expansion skipped: %s", original_query)
            for reason in result or ["Insufficient context in original query"]:
                logger.debug("Query expansion skip reason: %s", reason)
            return []

        return result
    @retry(
        stop=stop_after_attempt(3),  # Retry up to 3 times
        wait=wait_exponential(multiplier=1, max=10),  # Exponential backoff with a max wait time of 10 seconds
    )
    async def agenerate_follow_up_questions(
        self,
        retrieved_chunks: List[Chunk],
    ):
        chunks_list = [chunk.text for chunk in retrieved_chunks]
        system_prompt = bot_config["llm_response"]["follow_up_prompts"]["system_prompt"]
        template_user_prompt = bot_config["llm_response"]["follow_up_prompts"]["user_prompt"]
        chunks = ", ".join(chunks_list)
        user_prompt = template_user_prompt.replace("<CHUNKS>", chunks)
        augmented_prompts = self.__augment(system_prompt, user_prompt)
        llm_response, response_text = await llm_client.generate_response(augmented_prompts)
        tokens = llm_client.get_response_tokens(llm_response)
        utils.log_to_text_file(f"Generated answer tokens: {str(tokens)}")
        next_questions = re.findall(r"<q_\d+>(.*?)</q_\d+>", response_text)
        if next_questions is None or len(next_questions) != 3:
            raise ValueError("Parsing failed, next_questions.")
        return next_questions
    
    def get_related_questions(
        self,
        user_lang_code: str,
        retrieved_chunks: List[Chunk],
        message_src
    ):
        all_questions = set()
        # Collect all related questions from all chunks
        for retrieved_chunk in retrieved_chunks:
            related_questions = retrieved_chunk.related_questions.get(user_lang_code)
            if not related_questions:
                continue

            for related_question in related_questions:
                if ratio(related_question, message_src) > 70:
                    continue
                all_questions.add(related_question)

        # Filter and shuffle
        valid_questions = [q for q in all_questions if len(q) < 70]
        random.shuffle(valid_questions)

        return valid_questions[:3]

    async def handle_message_generate_workflow(self, messages: list[ByoebMessageContext]) -> list[ByoebMessageContext]:
        byoeb_messages = []
        message: ByoebMessageContext = messages[0].model_copy(deep=True)
        read_reciept_message = self.__create_read_reciept_message(message)

        if message.reply_context.message_category == MessageCategory.AUDIO_IDK.value:
            related_questions = message.reply_context.additional_info.get(constants.RELATED_QUESTIONS)
            byoeb_user_message = await self.__create_user_message(
                message=message,
                response_en=constants.IDK,
                response_source=None,
                query_type=message.reply_context.additional_info.get(constants.QUERY_TYPE),
                emoji=self.USER_PENDING_EMOJI,
                status=constants.PENDING,
                related_questions=related_questions
            )
        elif (utils.is_onboard(message.message_context.message_source_text, message.user.user_language) or
              utils.is_onboard(message.message_context.message_english_text or "", message.user.user_language)):
            logger.info("Is onboard message - returning already registered response (user_language=%s)", message.user.user_language)
            logger.debug("  message_source_text: '%s'; message_english_text: '%s'", message.message_context.message_source_text, message.message_context.message_english_text)
            # Import constants for onboarding messages
            from byoeb.constants.onboarding_text import ALREADY_REGISTERED_DICT, THANK_YOU_DICT, RELATED_QUESTIONS
            from byoeb.constants.user_enums import UserType, LanguageCode
            
            user_language = message.user.user_language
            user_type = message.user.user_type or UserType.ASHA.value
            
            logger.debug("  user_language: %s, user_type: %s; ALREADY_REGISTERED_DICT keys: %s; THANK_YOU_DICT[%s] keys: %s", user_language, user_type, list(ALREADY_REGISTERED_DICT.keys()), user_type, list(THANK_YOU_DICT.get(user_type, {}).keys()))
            
            # Get the "already registered" message in user's language
            already_registered_msg = ALREADY_REGISTERED_DICT.get(user_language, ALREADY_REGISTERED_DICT[LanguageCode.ENGLISH.value])
            logger.debug("  Retrieved already_registered_msg: '%s...'", (already_registered_msg or "")[:50])
            
            # Get the thank you message from THANK_YOU_DICT
            # Fallback: OTHERS -> ASHA -> English
            if user_type in THANK_YOU_DICT:
                thank_you_msg = THANK_YOU_DICT[user_type].get(user_language, THANK_YOU_DICT[user_type].get(LanguageCode.ENGLISH.value, ""))
            else:
                # For user types not in dict (like OTHERS), use ASHA messages
                logger.debug("  User type '%s' not in THANK_YOU_DICT, falling back to ASHA messages", user_type)
                thank_you_msg = THANK_YOU_DICT[UserType.ASHA.value].get(user_language, THANK_YOU_DICT[UserType.ASHA.value].get(LanguageCode.ENGLISH.value, ""))
            logger.debug("  Retrieved thank_you_msg: '%s...'", (thank_you_msg or "")[:50])
            
            # Combine messages
            response_text = f"{already_registered_msg} {thank_you_msg}"
            
            logger.debug("  Constructed response_text in %s: '%s...'", user_language, (response_text or "")[:100])
            
            # Use static RELATED_QUESTIONS instead of dynamic fetching
            related_questions = RELATED_QUESTIONS["questions"].get(user_language, RELATED_QUESTIONS["questions"][LanguageCode.ENGLISH.value])
            logger.debug("  Using static related_questions: %s", related_questions)
            
            # Translate response to English for response_en (needed for internal processing)
            # If translation fails, use a simple fallback
            try:
                from byoeb.chat_app.configuration.dependency_setup import text_translator
                response_en = await text_translator.atranslate_text(
                    input_text=response_text,
                    source_language=user_language,
                    target_language="en"
                )
                logger.debug("  Translated response_en: '%s...'", response_en[:100] if response_en else 'None')
            except Exception as translation_error:
                logger.warning("  Translation failed: %s", translation_error)
                # Fallback: Use English version of already_registered message
                response_en = ALREADY_REGISTERED_DICT.get(LanguageCode.ENGLISH.value, "You are already registered with the system.")
                logger.debug("  Using fallback response_en: '%s'", response_en)
            
            query_type = "asha_work_related"
            print(f"  Calling __create_user_message with response_source='{response_text[:50]}...'")
            byoeb_user_message = await self.__create_user_message(
                message=message,
                response_en=response_en,
                response_source=response_text,  # Pass the Hindi text directly - this should be used as message_source_text
                query_type=query_type,
                related_questions=related_questions
            )
            logger.debug("  Created user message with source_text: '%s...'", (byoeb_user_message.message_context.message_source_text or "")[:100])
        else:
            # Normal message flow - not AUDIO_IDK and not onboarding
            logger.info("[generate] Processing normal message (user_language=%s)", message.user.user_language)
            logger.debug("  message_source_text: '%s...'; message_english_text: '%s...'", (message.message_context.message_source_text or "")[:100], (message.message_context.message_english_text or "")[:100])
            
            message_english = message.message_context.message_english_text
            if not message_english:
                logger.warning("[generate] message_english_text is None or empty, using message_source_text as fallback")
                message_english = message.message_context.message_source_text
            
            user_language = message.user.user_language
            query_type = message.message_context.additional_info.get(constants.QUERY_TYPE)
            if query_type is None:
                raise ValueError("query_type must not be None")
            
            default_message_category = None
            cache_hit = False

            if embedding_fn and (FeatureFlag.CACHE_MESSAGES in feature_flags or message.user.test_user):
                with langfuse.start_as_current_observation(as_type="span", name="cache", input=message_english) as span:
                    embedding = await embedding_fn.aget_text_embedding(message_english)
                    cache_result = self.embedding_cache.query(embedding, 0.9)
                    hit = cache_result[2] is not None and bool(cache_result[2])
                    span.update(output={"hit": hit, "score": float(cache_result[0]) if hit and cache_result[0] is not None else None})
            else:
                embedding = None
                cache_result = None, None, None

            cache_val = cache_result[2] if cache_result else None
            if cache_val and "answer" in cache_val and user_language in cache_val["answer"]:
                response_en, response_source, related_questions, tokens = cache_val["answer"][user_language]
                cache_hit = True
            else:
                start_time = time.perf_counter()
                skip_cache = True
                retrieved_chunks_related_questions = asyncio.create_task(self._retrieve_top_k_chunks_for_related_questions(message_english, k=10))
                response_en, response_source, tokens, retrieved_chunks = await self.agenerate_answer(user_language, message_english, query_type)
                if not utils.is_idk(response_en):  # got answer on first try :)
                    skip_cache = False
                else:
                    query_expansion_search_ops = [(None, AzureVectorSearchType.HYBRID.value, 3), (None, AzureVectorSearchType.DENSE.value, 3)]
                    query_expansions_queries = await self.agenerate_expansion_queries(message_english, retrieved_chunks)
                    for q in query_expansions_queries:
                        try:
                            response_en2, response_source2, tokens2, retrieved_chunks2 = await self.agenerate_answer(user_language, q, self._asha_work_related, query_expansion_search_ops)
                        except Exception as e:
                            utils.log_to_text_file(f"Query expansion failed for query '{q}': {e}")
                            continue
                        retrieved_chunks = list({c.chunk_id: c for c in retrieved_chunks + retrieved_chunks2}.values())
                        if not utils.is_idk(response_en2):
                            skip_cache = False
                            response_en, response_source, tokens = response_en2, response_source2, tokens2
                            break

                if utils.is_idk(response_en) and (FeatureFlag.QUERY_DISAMBIGUATION in feature_flags or message.user.test_user):
                    logger.debug("Query expansion was unsuccessful, assessing whether clarification is required...")
                    clarification = await self.needs_clarification(message_english, query_type, user_language, retrieved_chunks)
                    if clarification:
                        default_message_category = MessageCategory.AUDIO_DISAMBIGUATION if message.message_context.message_type == MessageTypes.REGULAR_AUDIO else MessageCategory.TEXT_DISAMBIGUATION
                        response_en, response_source, tokens = clarification

                related_questions = self.get_related_questions(message.user.user_language, await retrieved_chunks_related_questions, message.message_context.message_source_text)
                if not skip_cache and embedding:
                    cache_val = cache_val or {}
                    cache_val["answer"] = cache_val.get("answer", {})
                    cache_val["answer"][user_language] = response_en, response_source, related_questions, tokens
                    miss_thresh = cache_result[0] if cache_result is not None else None
                    try:
                        cache_result = self.embedding_cache.store(embedding, cache_val)
                        cache_result = miss_thresh, *cache_result[1:]
                    except Exception as e:
                        logger.warning("Embedding cache store failed: %s. Continuing without cache.", e)
                        cache_result = None, None, None
                else:
                    cache_result = None, None, None

                end_time = time.perf_counter()
                AppInsightsLogHandler.getLogger("generate_answer_and_related_questions").info(f"Generated related questions for {message.message_context.message_id} in {end_time - start_time}s", extra={AppInsightsLogHandler.DETAILS: {
                    "message_id": message.message_context.message_id,
                    "time_taken": end_time - start_time,
                    **tokens
                }})
            
            byoeb_user_message = await self.__create_user_message(
                message=message,
                response_en=response_en,
                response_source=response_source,
                query_type=query_type,
                emoji=self.USER_PENDING_EMOJI,
                status=constants.PENDING,
                related_questions=related_questions,
                cache_details=cache_result,
                cache_hit=cache_hit,
                default_message_category=default_message_category
            )
        logger.info("Created user message")
        byoeb_expert_message = None
        # byoeb_expert_message = self.__create_expert_verification_message(
        #     message,
        #     answer,
        #     query_type.lower(),
        #     self.EXPERT_PENDING_EMOJI,
        #     constants.PENDING
        # )
        # print("Created expert message")

        # Aggregate all messages
        logger.debug("[GENERATE] byoeb_user_message: %s", clean_message_for_console(byoeb_user_message))
        logger.debug("[GENERATE] byoeb_expert_message: %s", clean_message_for_console(byoeb_expert_message) if byoeb_expert_message else byoeb_expert_message)
        logger.debug("[GENERATE] read_reciept_message: %s", clean_message_for_console(read_reciept_message))
        
        if byoeb_user_message is not None:
            byoeb_messages.append(byoeb_user_message)
            logger.debug("[GENERATE] Added user message to list")
        if byoeb_expert_message is not None:
            byoeb_messages.append(byoeb_expert_message)
            logger.debug("[GENERATE] Added expert message to list")
        if read_reciept_message is not None:
            byoeb_messages.append(read_reciept_message)
            logger.debug("[GENERATE] Added read receipt message to list")
            
        logger.info("[GENERATE] Final byoeb_messages count: %s", len(byoeb_messages))
        return byoeb_messages
    
    async def handle(
        self,
        messages: List[ByoebMessageContext]
    ) -> Dict[str, Any]:
        if messages is None or len(messages) == 0:
            return {}
        new_messages = []
        try:
            start_time = time.perf_counter()
            new_messages = await self.handle_message_generate_workflow(messages)
            end_time = time.perf_counter()
            logger.info("[GENERATE] Generated %s messages", len(new_messages))
            utils.log_to_text_file(f"E2E Generated answer and related questions in {end_time - start_time} seconds")
        except RetryError as e:
            utils.log_to_text_file(f"RetryError in generating response: {e}")
            logger.error("RetryError in generating response: %s", e, exc_info=True)
            raise e
        except Exception as e:
            utils.log_to_text_file(f"Error in generating response: {e}")
            logger.error("Error in generating response: %s", e, exc_info=True)
            raise e
        if self._successor:
            logger.info("[GENERATE] Passing %s messages to successor", len(new_messages))
            return await self._successor.handle(
                new_messages
            )
