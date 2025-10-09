import asyncio
import hashlib
import byoeb.services.chat.constants as constants
import re
import byoeb.utils.utils as utils
import random
from rapidfuzz.fuzz import ratio
from datetime import datetime, timezone
from tenacity import retry, stop_after_attempt, wait_exponential, RetryError
from typing import List, Dict, Any
from byoeb.chat_app.configuration.config import bot_config, app_config
from byoeb.models.message_category import MessageCategory
from byoeb_core.models.vector_stores.chunk import Chunk
from byoeb_core.models.vector_stores.azure.azure_search import AzureSearchNode
from byoeb_core.models.byoeb.message_context import (
    ByoebMessageContext,
    MessageContext,
    ReplyContext,
    MessageTypes
)
from byoeb_integrations.vector_stores.azure_vector_search.azure_vector_search import AzureVectorSearchType
from byoeb_core.models.byoeb.user import User
from byoeb.services.chat.message_handlers.base import Handler
from byoeb.chat_app.configuration.dependency_setup import llm_client, app_insights_logger

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

    async def __aretrieve_chunks(
        self,
        text,
        k,
        search_type=AzureVectorSearchType.HYBRID.value
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
        start_time = datetime.now(timezone.utc).timestamp()
        retrieved_chunks = await vector_store.aretrieve_top_k_chunks(
            text,
            k,
            search_type=search_type,
            select=["id", "text", "metadata", "related_questions"],
            vector_field="text_vector_3072"
        )
        end_time = datetime.now(timezone.utc).timestamp()
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
        start_time = datetime.now(timezone.utc).timestamp()
        retrieved_chunks = await vector_store.aretrieve_top_k_chunks(
            text,
            k,
            search_type=AzureVectorSearchType.DENSE.value,
            select=["id", "metadata", "related_questions"],
            vector_field="text_vector_3072"
        )
        end_time = datetime.now(timezone.utc).timestamp()
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
        print("IDK Message source text: ", source_text)
        print("IDK Query type: ", query_type)
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
        print("Modality: ", modality)
        print("Query:", query)
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
        user_language: str
    ):
        from byoeb.chat_app.configuration.dependency_setup import speech_translator
        translated_audio_message = await speech_translator.atext_to_speech(
            input_text=message_source_text,
            source_language=user_language,
        )
        return {
            constants.DATA: translated_audio_message,
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
        
        source_text = await text_translator.atranslate_text(
            input_text=response_text,
            source_language="en",
            target_language=message.user.user_language
        )
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
    ) -> ByoebMessageContext:
        start_time = datetime.now(timezone.utc).timestamp()
        if response_source is None:
            message_source_text, options, send_related_questions = await self.__create_source_text(
                message=message,
                response_text=response_en,
                query_type=query_type,
            )
        elif utils.is_idk(response_en):
            message_source_text, options, send_related_questions = self.__get_idk_response(
                message=message,
                response_text=response_en,
                query_type=query_type,
            )
        else:
            message_source_text = response_source
            options = None
            send_related_questions = True
        print("Options: ", options)
        end_time = datetime.now(timezone.utc).timestamp()
        utils.log_to_text_file(f"Translated response message in {end_time - start_time} seconds")
        start_time = datetime.now(timezone.utc).timestamp()
        user_language = message.user.user_language
        media_info = await self.__create_source_audio(
            message_source_text=message_source_text,
            user_language=user_language
        )
        end_time = datetime.now(timezone.utc).timestamp()
        app_insights_logger.add_log(
            event_name="text_to_audio",
            details={
                "message_id": message.message_context.message_id,
                "time_taken": end_time - start_time
            }
        )
        utils.log_to_text_file(f"Created audio response message in {end_time - start_time} seconds")
        description = bot_config["template_messages"]["user"]["follow_up_questions_description"][user_language]
        message_type = None
        message_category=MessageCategory.BOT_TO_USER_RESPONSE.value
        if (message.message_context.message_type == MessageTypes.REGULAR_AUDIO.value):
            message_type = MessageTypes.REGULAR_AUDIO.value
        elif (message.message_context.message_type == MessageTypes.REGULAR_TEXT.value
              or message.message_context.message_type == MessageTypes.INTERACTIVE_LIST.value
              or message.message_context.message_type == MessageTypes.INTERACTIVE_BUTTON.value):
            message_type = MessageTypes.INTERACTIVE_LIST.value
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
            interactive_list_additional_info = {
                constants.DESCRIPTION: description,
                constants.ROW_TEXTS: related_questions,
                constants.QUERY_TYPE: query_type
            }
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
                message_source_text=message_source_text,
                message_english_text=response_en,
                additional_info={
                    **media_info,
                    **button_reply_additional_info,
                    **interactive_list_additional_info,
                    **text_additional_info,
                    **idk_status
                }
            ),
            reply_context=reply_context,
            incoming_timestamp=message.incoming_timestamp,
        )
        print("Message category: ", user_message.message_category)
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
    
    @retry(
        stop=stop_after_attempt(3),  # Retry up to 3 times
        wait=wait_exponential(multiplier=1, max=10),  # Exponential backoff with a max wait time of 10 seconds
    )
    async def agenerate_answer(
        self,
        user_language: str,
        query,
        query_type,
        retrieved_chunks: List[Chunk],
    ):
        def parse_response_xml(xml_string: str):
            # Patterns for extracting response_en and response_hi
            patterns = {
                "response_en": r"<response_en\s*>(.*?)</response_en\s*>",
                "response_src": r"<response_src\s*>(.*?)</response_src\s*>",
            }

            extracted_data = {}
            for key, pattern in patterns.items():
                match = re.search(pattern, xml_string, re.DOTALL | re.IGNORECASE)  # Supports multiline and case-insensitive matches
                extracted_data[key] = match.group(1).strip() if match else None  # Strip removes extra spaces and newlines

            return extracted_data["response_en"], extracted_data["response_src"]
        
        update_kb = [chunk.text for chunk in retrieved_chunks if "KB Updated" in chunk.metadata.source]
        raw_kb = [chunk.text for chunk in retrieved_chunks if "KB Updated" not in chunk.metadata.source]
        update_kb_list = ", ".join(update_kb)
        raw_kb_list = ", ".join(raw_kb)

        system_prompt = self._get_system_prompt(user_language)
        template_user_prompt = bot_config["llm_response"]["answer_prompts"]["user_prompt"]
        # Replace placeholders with actual values
        
        user_prompt = template_user_prompt.replace("<QUERY_TYPE>", query_type).replace("<QUERY_EN_ADDCONTEXT>", query).replace("<RAW_KB>", raw_kb_list).replace("<NEW_KB>", update_kb_list)
        augmented_prompts = self.__augment(system_prompt, user_prompt)
        start_time = datetime.now(timezone.utc).timestamp()
        llm_response, response_text = await llm_client.agenerate_response(augmented_prompts)
        tokens = llm_client.get_response_tokens(llm_response)
        response_en, response_source = parse_response_xml(response_text)
        end_time = datetime.now(timezone.utc).timestamp()
        utils.log_to_text_file(f"Generated answer tokens and response in {end_time - start_time} seconds: {str(tokens)} {response_text}")
        print("Generated answer: ", response_en)
        print("Query type: ", query_type)
        if response_en is None or query_type is None:
            raise ValueError("Parsing failed, response or query_type is None.")
        return response_en, response_source, tokens
    
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
        llm_response, response_text = await llm_client.agenerate_response(augmented_prompts)
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
    
    async def handle_message_generate_workflow(
        self,
        messages: ByoebMessageContext
    ) -> List[ByoebMessageContext]:
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
        elif utils.is_onboard(message.message_context.message_source_text, message.user.user_language):
            print("Is onboard message")
            query_type = "asha_work_related"
            query = "antara injection, tobacco, chaaya goli"
            retrieved_chunks_related_questions = await self._retrieve_top_k_chunks_for_related_questions(query, k=10)
            related_questions = self.get_related_questions(message.user.user_language, retrieved_chunks_related_questions, message.message_context.message_source_text)
            byoeb_user_message = await self.__create_user_message(
                message=message,
                response_en="You are already connected to Ashabot.",
                response_source=None,
                query_type=query_type,
                related_questions=related_questions
            )
        else:
            message_english = message.message_context.message_english_text
            user_language = message.user.user_language
            query_type = message.message_context.additional_info.get(constants.QUERY_TYPE)
            start_time = datetime.now(timezone.utc).timestamp()
            retrieved_chunks_task = self.__aretrieve_chunks(message_english, k=3)
            retrieved_chunks_backup_task = self.__aretrieve_chunks(
                message_english,
                k=5,
                search_type=AzureVectorSearchType.DENSE.value
            )
            retrieved_chunks_related_questions_task = self._retrieve_top_k_chunks_for_related_questions(message_english, k=10)
            retrieved_chunks, retrieved_chunks_backup, retrieved_chunks_related_questions = await asyncio.gather(
                retrieved_chunks_task,
                retrieved_chunks_backup_task,
                retrieved_chunks_related_questions_task
            )
            end_time = datetime.now(timezone.utc).timestamp()
            app_insights_logger.add_log(
                event_name="retrieve_chunks",
                details={
                    "message_id": message.message_context.message_id,
                    "time_taken": end_time - start_time
                }
            )
            start_time = datetime.now(timezone.utc).timestamp()
            response_task = self.agenerate_answer(user_language,message_english,query_type,retrieved_chunks)
            response_backup_task = self.agenerate_answer(user_language,message_english,query_type,retrieved_chunks_backup)
            response_result, response_backup_result = await asyncio.gather(
                response_task,
                response_backup_task
            )
            response_en, response_source, tokens = response_result
            response_en_backup, response_source_backup, tokens_backup = response_backup_result
            
            if utils.is_idk(response_en):
                response_en = response_en_backup
                response_source = response_source_backup
            # response_en, response_source, tokens = await self.agenerate_answer(user_language, message_english, query_type, retrieved_chunks)
            
            if message.user.user_language == "en":
                response_source = response_en
            related_questions = self.get_related_questions(message.user.user_language, retrieved_chunks_related_questions, message.message_context.message_source_text)
            end_time = datetime.now(timezone.utc).timestamp()
            app_insights_logger.add_log(
                event_name="generate_answer_and_related_questions",
                details={
                    "message_id": message.message_context.message_id,
                    "time_taken": end_time - start_time,
                    "completion_tokens": tokens.get("completion_tokens"),
                    "backup_completion_tokens": tokens_backup.get("completion_tokens"),
                    "prompt_tokens": tokens.get("prompt_tokens"),
                    "backup_prompt_tokens": tokens_backup.get("prompt_tokens")
                }
            )
            byoeb_user_message = await self.__create_user_message(
                message=message,
                response_en=response_en,
                response_source=response_source,
                query_type=query_type,
                emoji=self.USER_PENDING_EMOJI,
                status=constants.PENDING,
                related_questions=related_questions
            )
        print("Created user message")
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
        print(f"[GENERATE] byoeb_user_message: {byoeb_user_message}")
        print(f"[GENERATE] byoeb_expert_message: {byoeb_expert_message}")
        print(f"[GENERATE] read_reciept_message: {read_reciept_message}")
        
        if byoeb_user_message is not None:
            byoeb_messages.append(byoeb_user_message)
            print(f"[GENERATE] Added user message to list")
        if byoeb_expert_message is not None:
            byoeb_messages.append(byoeb_expert_message)
            print(f"[GENERATE] Added expert message to list")
        if read_reciept_message is not None:
            byoeb_messages.append(read_reciept_message)
            print(f"[GENERATE] Added read receipt message to list")
            
        print(f"[GENERATE] Final byoeb_messages count: {len(byoeb_messages)}")
        return byoeb_messages
    
    async def handle(
        self,
        messages: List[ByoebMessageContext]
    ) -> Dict[str, Any]:
        if messages is None or len(messages) == 0:
            return {}
        new_messages = []
        try:
            start_time = datetime.now(timezone.utc).timestamp()
            new_messages = await self.handle_message_generate_workflow(messages)
            end_time = datetime.now(timezone.utc).timestamp()
            print(f"[GENERATE] Generated {len(new_messages)} messages")
            utils.log_to_text_file(f"E2E Generated answer and related questions in {end_time - start_time} seconds")
        except RetryError as e:
            utils.log_to_text_file(f"RetryError in generating response: {e}")
            print("RetryError in generating response: ", e)
            raise e
        except Exception as e:
            utils.log_to_text_file(f"Error in generating response: {e}")
            print("Error in generating response: ", e)
            raise e
        if self._successor:
            print(f"[GENERATE] Passing {len(new_messages)} messages to successor")
            return await self._successor.handle(
                new_messages
            )
