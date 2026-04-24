import logging
import re
from byoeb.constants.user_enums import LanguageCode
import byoeb.utils.utils as utils
import byoeb.services.chat.constants as constants
from tenacity import retry, stop_after_attempt, wait_exponential
from byoeb.chat_app.configuration.config import bot_config
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional

from byoeb.chat_app.configuration.dependency_setup import langfuse
from byoeb_core.models.byoeb.message_context import ByoebMessageContext, MessageTypes
from byoeb.services.chat.message_handlers.base import Handler
from byoeb.models.message_category import MessageCategory
from byoeb.application_logger.azure_app_insights import AppInsightsLogHandler

from byoeb_core.models.whatsapp.requests.media_request import MediaData
from langfuse.media import LangfuseMedia
from langfuse.api import MediaContentType

logger = logging.getLogger(__name__)


class ByoebUserProcess(Handler):

    QUERY_EN = "query_en"
    QUERY_EN_ADDCONTEXT = "query_en_addcontext"

    def __init__(self, successor=None):
        super().__init__(successor)

    def build_augmented_prompts(self, system_prompt, user_prompt, conversation_history):
        return [
            {"role": "system", "content": system_prompt},
            *conversation_history,
            {"role": "user", "content": user_prompt}
        ]

    def _get_system_prompt(self, user_language: str) -> str:
        cfg = bot_config["llm_response"]["translation_and_rewrite_prompts"]
        return "\n".join((
            cfg["task_description"],
            cfg["query_translate"][user_language],
            cfg["query_rewrite"],
            cfg["query_classify"],
            cfg["output"]
        ))
    
    def _create_conversation_history(self, last_conversations: List[Dict[str, Any]]) -> list[dict[str, str]]:
        conversation_history = []
        curr_time = datetime.now(timezone.utc)
        for conversation in last_conversations:
            conversation_time = conversation.get(constants.TIMESTAMP, None)
            if conversation_time is None or not isinstance(conversation_time, datetime):
                continue
            if conversation_time.tzinfo is None:
                conversation_time = conversation_time.replace(tzinfo=timezone.utc)
            if (curr_time - conversation_time) > timedelta(minutes=30):
                continue
            
            question = conversation.get(constants.QUESTION, None)
            answer = conversation.get(constants.ANSWER, None)
            if question is None or answer is None:
                continue
            conversation_history.append({"role": "user", "content": question})
            conversation_history.append({"role": "assistant", "content": answer})
        return conversation_history
    
    @retry(
        stop=stop_after_attempt(3),  # Retry up to 3 times
        wait=wait_exponential(multiplier=1, max=10),  # Exponential backoff with a max wait time of 10 seconds
    )
    async def llm_translation_and_query_rewritting(
        self,
        messages: ByoebMessageContext
    ):
        def parse_xml_with_regex(xml_string: str) -> tuple[str, str, str]:
            # Patterns for extracting the required tags, ignoring their position in XML
            patterns = {
                self.QUERY_EN: r"<query_en\s*>(.*?)</query_en\s*>",
                self.QUERY_EN_ADDCONTEXT: r"<query_en_addcontext\s*>(.*?)</query_en_addcontext\s*>",
                constants.QUERY_TYPE: r"<query_type\s*>(.*?)</query_type\s*>",
            }

            extracted_data = {}
            for key, pattern in patterns.items():
                match = re.search(pattern, xml_string, re.DOTALL | re.IGNORECASE)  # Supports multiline and case-insensitive matches
                extracted_data[key] = match.group(1).strip() if match else None  # Strip removes extra spaces and newlines

            return extracted_data[self.QUERY_EN], extracted_data[self.QUERY_EN_ADDCONTEXT], extracted_data[constants.QUERY_TYPE]
        # dependency injection
        from byoeb.chat_app.configuration.dependency_setup import langfuse, llm_translate_and_rewrite_client
        user_prompt = messages.message_context.message_source_text
        conversation_history = self._create_conversation_history(messages.user.last_conversations)
        system_prompt = self._get_system_prompt(messages.user.user_language)
        augmented_prompts = self.build_augmented_prompts(system_prompt, user_prompt, conversation_history)
        start_time = datetime.now(timezone.utc)
        with langfuse.start_as_current_observation(as_type="span", name="translate-and-rewrite", input=user_prompt, metadata={
            "conversation_history": conversation_history
        }) as span:
            llm_response, response_text = await llm_translate_and_rewrite_client.generate_response(augmented_prompts)
            query_en, query_en_addcontext, query_type = parse_xml_with_regex(response_text)
            span.update(output={"query_en": query_en, "query_en_addcontext": query_en_addcontext, "query_type": query_type})
        tokens = llm_translate_and_rewrite_client.get_response_tokens(llm_response)
        if query_en is None or query_en_addcontext is None or query_type is None:
            raise Exception("LLM response is not in expected format")
        end_time = datetime.now(timezone.utc)
        duration_seconds = (end_time - start_time).total_seconds()
        utils.log_to_text_file(f"Query rewritting and transcribe in {duration_seconds} seconds: {str(tokens)} {response_text}")
        return query_en, query_en_addcontext, query_type, tokens, conversation_history

    async def annotate_audio_transcription(self, message: ByoebMessageContext, audio_message: Optional[MediaData] = None):
        with langfuse.start_as_current_observation(as_type="span", name="transcription") as span:
            # dependency injection
            from byoeb.chat_app.configuration.dependency_setup import channel_client_factory, speech_translator
            from byoeb_core.convertor.audio_convertor import ogg_opus_to_wav_bytes

            start_time = datetime.now(timezone.utc)
            if audio_message is None:
                media_info = getattr(message.message_context, "media_info", None)
                if media_info is None:
                    raise ValueError("media_info missing for audio message; cannot run speech-to-text")
                media_id = media_info.media_id
                with langfuse.start_as_current_observation(as_type="span", name="audio-download"):
                    channel_client = await channel_client_factory.get(message.channel_type)
                    _, audio_message, err = await channel_client.adownload_media(media_id)
                if err or audio_message is None:
                    raise RuntimeError("failed to download audio for speech-to-text")

            span.update(input=LangfuseMedia(content_bytes=audio_message.data, content_type=MediaContentType.AUDIO_OPUS))
            with langfuse.start_as_current_observation(as_type="span", name="audio-convert"):
                audio_message_wav = ogg_opus_to_wav_bytes(audio_message.data)
            with langfuse.start_as_current_observation(as_type="span", name="speech-to-text"):
                audio_to_text = await speech_translator.aspeech_to_text(audio_message_wav, message.user.user_language, test_user=message.user.test_user)
            span.update(output=audio_to_text)

        message.message_context.message_source_text = audio_to_text
        end_time = datetime.now(timezone.utc)
        duration_seconds = (end_time - start_time).total_seconds()
        AppInsightsLogHandler.getLogger("audio_to_text").info(f"Time taken for audio to text transcribe: {duration_seconds} seconds", extra={AppInsightsLogHandler.DETAILS: {
            "message_id": message.message_context.message_id,
            "time_taken": duration_seconds
        }})
        utils.log_to_text_file(f"Time taken for audio to text transcribe: {duration_seconds} seconds")
        if message.message_context.media_info:
            message.message_context.media_info.media_type = audio_message.mime_type

    async def handle_process_message_workflow(
        self,
        messages: List[ByoebMessageContext]
    ) -> ByoebMessageContext:
        message = messages[0].model_copy(deep=True)

        query_type = None
        query_en = None
        query_en_addcontext = None
        conv_history = []
        if message.message_context.message_type == MessageTypes.REGULAR_AUDIO.value:
            await self.annotate_audio_transcription(message)

        # Check if this is an onboarding message BEFORE processing
        is_onboarding_message = utils.is_onboard(message.message_context.message_source_text, message.user.user_language)

        # Skip LLM translation/rewriting for onboarding messages to prevent them from being sent to vector store/LLM
        # Also skip for AUDIO_IDK messages (they don't need translation/rewriting)
        if message.reply_context.message_category == MessageCategory.AUDIO_IDK.value:
            pass
        elif is_onboarding_message:
            logger.info("[process] Detected onboarding message: '%s...'", (message.message_context.message_source_text or "")[:50])
            source_text = message.message_context.message_source_text
            query_en = source_text
            query_en_addcontext = source_text
            query_type = "small_talk"
        else:
            start_time = datetime.now(timezone.utc)
            logger.info("[process] Processing normal message (not onboarding): '%s...'", (message.message_context.message_source_text or "")[:50])
            query_en, query_en_addcontext, query_type, tokens, conv_history = await self.llm_translation_and_query_rewritting(message)
            end_time = datetime.now(timezone.utc)
            duration_seconds = (end_time - start_time).total_seconds()
            AppInsightsLogHandler.getLogger("query_rewriting").info(f"Rewrote queries for {message.message_context.message_id} in {duration_seconds} using {tokens.get('completion_tokens')} completion and {tokens.get('prompt_tokens')} prompt tokens", extra={AppInsightsLogHandler.DETAILS: {
                "message_id": message.message_context.message_id,
                "time_taken": duration_seconds,
                "completion_tokens": tokens.get("completion_tokens"),
                "prompt_tokens": tokens.get("prompt_tokens")
            }})
            

        # Set message_english_text - use query_en_addcontext if available, otherwise fallback to source text
        if query_en_addcontext is not None:
            message.message_context.message_english_text = query_en_addcontext
        else:
            message.message_context.message_english_text = message.message_context.message_source_text

        chunks = [conv_history[i:i + 2] for i in range(0, len(conv_history), 2)]
        conv_history_legacy = [f"query{i}: {chunk[0]['content']} answer{i}: {chunk[1]['content']}" for i, chunk in enumerate(chunks, start=1)]
        message.message_context.additional_info = {
            constants.QUERY_TYPE: query_type,
            constants.QUERY_EN: query_en,
            constants.CONV_HISTORY: conv_history_legacy
        }
        return message

    async def handle(
        self,
        messages: List[ByoebMessageContext]
    ) -> Dict[str, Any]:
        with langfuse.start_as_current_observation(name="ASHABot", trace_context={"trace_id": langfuse.create_trace_id()}) as span:
            message = await self.handle_process_message_workflow(messages)
            assert message.message_context is not None
            assert message.user is not None
            span.update(input=message.message_context.message_source_text, metadata={"message_id": message.message_context.message_id})

            tags = []
            try:
                tags.append(LanguageCode(message.user.user_language).name.lower())
            except ValueError:
                pass
            if message.user.user_type: tags.append(message.user.user_type)
            if message.message_context.message_type: tags.append(message.message_context.message_type)
            if message.message_context.additional_info:
                if constants.QUERY_TYPE in message.message_context.additional_info: tags.append(message.message_context.additional_info[constants.QUERY_TYPE])
            span.update_trace(user_id=message.user.user_id, tags=tags)

            if self._successor:
                return await self._successor.handle([message])
