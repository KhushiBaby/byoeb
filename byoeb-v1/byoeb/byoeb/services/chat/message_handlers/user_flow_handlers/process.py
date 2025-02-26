import re
import threading
import byoeb.utils.utils as utils
import byoeb.services.chat.constants as constants
from tenacity import retry, stop_after_attempt, wait_exponential, RetryError
from byoeb.chat_app.configuration.config import bot_config
from datetime import datetime, timezone
from typing import Dict, Any, List
from byoeb_core.models.byoeb.message_context import ByoebMessageContext, MessageTypes
from byoeb.services.chat.message_handlers.base import Handler
from byoeb.models.message_category import MessageCategory

class ByoebUserProcess(Handler):

    QUERY_EN = "query_en"
    QUERY_EN_ADDCONTEXT = "query_en_addcontext"

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
    
    def _create_conversation_history(self, last_conversations: List[Dict[str, Any]]) -> str:
        conversation_history = []
        curr_time = datetime.now(timezone.utc).timestamp()
        i = 1
        for conversation in last_conversations:
            timestamp = int(conversation.get(constants.TIMESTAMP, 0))
            if curr_time - timestamp > 1000:
                continue  # Skip conversations older than 30 min
            
            question = conversation.get(constants.QUESTION, None)
            answer = conversation.get(constants.ANSWER, None)
            if question is None or answer is None:
                continue
            conversation_history.append(f"query{i}: {question} answer{i}: {answer}")
            i+=1
        return conversation_history
    
    @retry(
        stop=stop_after_attempt(3),  # Retry up to 3 times
        wait=wait_exponential(multiplier=1, max=10),  # Exponential backoff with a max wait time of 10 seconds
    )
    async def llm_translation_and_query_rewritting(
        self,
        messages: ByoebMessageContext
    ):
        def parse_xml_with_regex(xml_string: str):
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
        from byoeb.chat_app.configuration.dependency_setup import llm_translate_and_rewrite_client
        source_text = messages.message_context.message_source_text
        conversation_history = self._create_conversation_history(messages.user.last_conversations)
        conversation_history_str = ", ".join(conversation_history)
        system_prompt = bot_config["llm_response"]["translation_and_rewrite_prompts"]["system_prompt"]
        template_user_prompt = bot_config["llm_response"]["translation_and_rewrite_prompts"]["user_prompt"]
        user_prompt = template_user_prompt.replace("<QUERY>", source_text).replace("<CONVERSATION_HISTORY>", conversation_history_str)
        augmented_prompts = self.__augment(system_prompt, user_prompt)
        start_time = datetime.now(timezone.utc).timestamp()
        llm_response, response_text = await llm_translate_and_rewrite_client.agenerate_response(augmented_prompts)
        tokens = llm_translate_and_rewrite_client.get_response_tokens(llm_response)
        query_en, query_en_addcontext, query_type  = parse_xml_with_regex(response_text)
        if query_en is None or query_en_addcontext is None or query_type is None:
            raise Exception("LLM response is not in expected format")
        end_time = datetime.now(timezone.utc).timestamp()
        utils.log_to_text_file(f"Query rewritting and transcribe in {end_time - start_time} seconds: {str(tokens)} {response_text}")
        return query_en, query_en_addcontext, query_type

    async def __handle_process_message_workflow(
        self,
        messages: List[ByoebMessageContext]
    ) -> ByoebMessageContext:
        # dependency injection
        from byoeb.chat_app.configuration.dependency_setup import channel_client_factory
        from byoeb.chat_app.configuration.dependency_setup import speech_translator_whisper
        from byoeb_core.convertor.audio_convertor import ogg_opus_to_wav_bytes

        message = messages[0].model_copy(deep=True)
        channel_type = message.channel_type
        source_language = message.user.user_language
        query_type = None
        query_en = None
        query_en_addcontext = None

        if message.message_context.message_type == MessageTypes.REGULAR_AUDIO.value:
            start_time = datetime.now(timezone.utc).timestamp()
            media_id = message.message_context.media_info.media_id
            channel_client = await channel_client_factory.get(channel_type)
            _, audio_message, err = await channel_client.adownload_media(media_id)
            audio_message_wav = ogg_opus_to_wav_bytes(audio_message.data)
            audio_to_text = await speech_translator_whisper.aspeech_to_text(audio_message_wav, source_language)
            message.message_context.message_source_text = audio_to_text
            end_time = datetime.now(timezone.utc).timestamp()
            utils.log_to_text_file(f"Time taken for audio to text transcribe: {end_time - start_time} seconds")
            query_en, query_en_addcontext, query_type = await self.llm_translation_and_query_rewritting(message)
            # print("audio_to_text", audio_to_text)
            # translated_en_text = await text_translator.atranslate_text(
            #     input_text=audio_to_text,
            #     source_language=source_language,
            #     target_language="en"
            # )
            message.message_context.media_info.media_type = audio_message.mime_type
        
        else:
            # source_text = message.message_context.message_source_text
            if message.reply_context.message_category != MessageCategory.AUDIO_IDK.value:
                query_en, query_en_addcontext, query_type = await self.llm_translation_and_query_rewritting(message)
                # translated_en_text = await text_translator.atranslate_text(
                #     input_text=source_text,
                #     source_language=source_language,
                #     target_language="en"
                # )
            
        message.message_context.message_english_text = query_en_addcontext
        message.message_context.additional_info = {
            constants.QUERY_TYPE: query_type,
            self.QUERY_EN: query_en,
            constants.CONV_HISTORY: self._create_conversation_history(message.user.last_conversations)
        }
        return message

    async def handle(
        self,
        messages: List[ByoebMessageContext]
    ) -> Dict[str, Any]:
        message = None
        try:
            message = await self.__handle_process_message_workflow(messages)
        except Exception as e:
            raise e
        
        if self._successor:
            return await self._successor.handle([message])