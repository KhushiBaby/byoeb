import os
import shutil
import sys
__import__("pysqlite3")
sys.modules["sqlite3"] = sys.modules.pop("pysqlite3")

import chromadb
import json

from database import UserConvDB, BotConvDB, AppLogger
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain.document_loaders import DirectoryLoader
from chromadb.utils import embedding_functions
import shutil
from typing import Any
from chromadb.config import Settings
from utils import get_llm_response
from datetime import datetime


class KnowledgeBase:
    def __init__(self, config: dict[str, Any]):
        self.config = config
        self.persist_directory = os.path.join(
            os.path.join(os.environ["APP_PATH"], os.environ["DATA_PATH"]), "vectordb"
        )
        self.embedding_fn = embedding_functions.OpenAIEmbeddingFunction(
            api_key=os.environ['OPENAI_API_KEY'].strip(),
            model_name="text-embedding-3-large"
        )
        
        self.llm_prompts = json.load(open(os.path.join(os.environ["APP_PATH"], os.environ["DATA_PATH"], "llm_prompt.json")))

        self.client = chromadb.PersistentClient(
            path=self.persist_directory, settings=Settings(anonymized_telemetry=False)
        )

    def answer_query(
        self,
        user_conv_db: UserConvDB,
        bot_conv_db: BotConvDB,
        msg_id: str,
        app_logger: AppLogger,
        max_retries: int = 5,
    ):
        for i in range(max_retries):
            try:
                gpt_output, citations, query_type = self.answer_query_helper(user_conv_db, bot_conv_db, msg_id, app_logger, 3)
                if gpt_output.strip().startswith("I do not know the answer to your question"):
                    print("Retrying with top 7")
                    gpt_output, citations, query_type = self.answer_query_helper(user_conv_db, bot_conv_db, msg_id, app_logger, 7)
                return gpt_output, citations, query_type
            except Exception as e:
                print(f"Error in answer_query: {e}")
                continue
        return "I do not know the answer to your question", "llm-failure-fallback", "Clinical"

    def answer_query_helper(
        self,
        user_conv_db: UserConvDB,
        bot_conv_db: BotConvDB,
        msg_id: str,
        app_logger: AppLogger,
        top_k: int = 3,
    ):
        """answer the user's query using the knowledge base and chat history
        Args:
            query (str): the query
            llm (OpenAI): any foundational model
            database (Any): the database
            db_id (str): the database id of the row with the query
        Returns:
            tuple[str, str]: the response and the citations
        """

        if self.config["API_ACTIVATED"] is False:
            gpt_output = "API not activated"
            citations = "NA-API"
            query_type = "small-talk"
            return (gpt_output, citations, query_type)
        
        self.collection = self.client.get_collection(
            name=self.config["PROJECT_NAME"], embedding_function=self.embedding_fn
        )
        
        collection_count = self.collection.count()
        print('collection ids count: ', collection_count)

        db_row = user_conv_db.get_from_message_id(msg_id)
        query = db_row["message_english"]

        query_source = db_row["message_source_lang"]
        if not query.endswith("?"):
            query += "?"

        relevant_chunks = self.collection.query(
            query_texts=[query],
            n_results=top_k,
        )
        citations: str = "\n".join(
            [metadata["source"] for metadata in relevant_chunks["metadatas"][0]]
        )

        relevant_chunks_string = ""
        relevant_update_chunks_string = ""
        chunks = []

        chunk1 = 0
        chunk2 = 0
        for chunk, chunk_text in enumerate(relevant_chunks["documents"][0]):
            if relevant_chunks["metadatas"][0][chunk]["source"].strip() == "KB Updated":
                relevant_update_chunks_string += (
                    f"Chunk #{chunk2 + 1}\n{chunk_text}\n\n"
                )
                chunk2 += 1
                chunks.append((chunk_text, relevant_chunks["metadatas"][0][chunk]["source"].strip()))
            else:
                relevant_chunks_string += f"Chunk #{chunk1 + 1}\n{chunk_text}\n\n"
                chunk1 += 1
                chunks.append((chunk_text, relevant_chunks["metadatas"][0][chunk]["source"].strip()))

        app_logger.add_log(
            event_name="get_citations",
            details={"query": query, "chunks": chunks, "transaction_id": db_row["message_id"]},
        )


        # take all non empty conversations 
        all_conversations = user_conv_db.get_all_user_conv(db_row["user_id"])
        conversation_string = ""
        all_conversations = [conv for conv in all_conversations if conv["message_type"] != "feedback_response"]
        
        last_two_conversations = all_conversations[-3:-1]
        for conv in last_two_conversations:
            response = bot_conv_db.find_with_transaction_id(conv["message_id"], "query_response")
            if response:
                conversation_string += f"User: {conv['message_english']}\nBot: {response['message_english']}\n\n"
            else:
                conversation_string += f"User: {conv['message_english']}\n\n"

        system_prompt = self.llm_prompts["answer_query"]
        query_prompt = f"""
            Today's date is {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}\n\
            
            The following knowledge base chunks have been provided to you as reference:\n\n\
            Raw documents are as follows:\n\
            {relevant_chunks_string}\n\n\
            New documents are as follows:\n\
            {relevant_update_chunks_string}\n\n\
            The most recent conversations are here:\n\
            {conversation_string}\n\
            Use previous conversation as added context for the query.\n\
            You are asked the following query:\n\
            Original query (in Hindi/Hinglish): {query_source}\n\
            Transalted query in English: {query}\n\
            Please return the answer in english only.\n\

        """

        schema = {
            "name": "response_schema",
            "schema": {
                "type": "object",
                "properties": {
                    "response": {"type": "string"},
                    "query_type": {"type": "string"}
                },
                "required": ["response", "query_type"]
            }
        }

        prompt = [{"role": "system", "content": system_prompt}]
        prompt.append({"role": "user", "content": query_prompt})
        app_logger.add_log(
            event_name="answer_query_request_gpt4",
            details={
                "system_prompt": system_prompt,
                "query_prompt": query_prompt,
                "transaction_id": db_row["message_id"],
            },
        )
        gpt_output = get_llm_response(prompt, schema=schema)
        app_logger.add_log(
            event_name="answer_query_response_gpt4",
            details={
                "system_prompt": system_prompt,
                "query_prompt": query_prompt,
                "gpt_output": gpt_output,
                "transaction_id": db_row["message_id"],
            },
        )

        json_output = json.loads(gpt_output.strip())
        bot_response = json_output["response"]
        query_type = json_output["query_type"]

        # print('bot response: ', bot_response, 'query type: ', query_type)

        if len(bot_response) < 700:
            return (bot_response, citations, query_type)
        else:
            system_prompt = f"""Please summarise the given answer in 700 characters or less. Only return the summarized answer and nothing else.\n"""
            
            query_prompt = f"""You are given the following response: {bot_response}"""
            prompt = [{"role": "system", "content": system_prompt}]
            prompt.append({"role": "user", "content": query_prompt})
            app_logger.add_log(
                event_name="answer_summary_request_gpt4",
                details={
                    "system_prompt": system_prompt,
                    "query_prompt": query_prompt,
                    "transaction_id": db_row["message_id"],
                },
            )

            gpt_output = get_llm_response(prompt)

            app_logger.add_log(
                event_name="answer_summary_response",
                details={
                    "system_prompt": system_prompt,
                    "query_prompt": query_prompt,
                    "gpt_output": gpt_output,
                    "transaction_id": db_row["message_id"],
                },
            )
            return (gpt_output, citations, query_type)
        
    def answer_query_text(
        self,
        query_src: str,
        query: str,
        app_logger: AppLogger,
    ) -> tuple[str, str]:
        """answer the user's query using the knowledge base and chat history
        Args:
            query (str): the query
            llm (OpenAI): any foundational model
            database (Any): the database
            db_id (str): the database id of the row with the query
        Returns:
            tuple[str, str]: the response and the citations
        """

        if self.config["API_ACTIVATED"] is False:
            gpt_output = "API not activated"
            citations = "NA-API"
            query_type = "small-talk"
            return (gpt_output, citations, query_type)
        

        self.collection = self.client.get_collection(
            name=self.config["PROJECT_NAME"], embedding_function=self.embedding_fn
        )
        collection_count = self.collection.count()
        print('collection ids count: ', collection_count)

        relevant_chunks = self.collection.query(
            query_texts=query,
            n_results=3,  # take the top 3 most relevant chunks, think of a better way to do this later
        )
        citations: str = "\n".join(
            [metadata["source"] for metadata in relevant_chunks["metadatas"][0]]
        )

        relevant_chunks_string = ""
        relevant_update_chunks_string = ""

        chunk1 = 0
        chunk2 = 0
        for chunk, chunk_text in enumerate(relevant_chunks["documents"][0]):
            if relevant_chunks["metadatas"][0][chunk]["source"].strip() == "KB Updated":
                relevant_update_chunks_string += (
                    f"Chunk #{chunk2 + 1}\n{chunk_text}\n\n"
                )
                chunk2 += 1
            else:
                relevant_chunks_string += f"Chunk #{chunk1 + 1}\n{chunk_text}\n\n"
                chunk1 += 1

        app_logger.add_log(
            event_name="get_citations",
            details={"query": query, "citations": citations},
        )

        # take all non empty conversations 
        # all_conversations = database.get_rows_with_user_id(db_row['user_id'], db_row['user_type'])
        # conversation_string = "\n".join(
        #     [
        #         row["query"] + "\n" + row["response"]
        #         for row in all_conversations
        #         if row["response"]
        #     ][-5:]
        # )

        system_prompt = self.llm_prompts["answer_query"]
        query_prompt = f"""
            The following knowledge base have been provided to you as reference:\n\n\
            Raw documents are as follows:\n\
            {relevant_chunks_string}\n\n\
            New documents are as follows:\n\
            {relevant_update_chunks_string}\n\n\
            You are asked the following query:\n\n\
            Original query (in Hindi): {query_src}\n\n\
            Transalted query in English: {query}\n\n\
            Please return the answer in english only.\n\n\
            \n\n\

        """

        prompt = [{"role": "system", "content": system_prompt}]
        prompt.append({"role": "user", "content": query_prompt})
        gpt_output = get_llm_response(prompt)
        app_logger.add_log(
            event_name="gpt4",
            details={
                "system_prompt": system_prompt,
                "query_prompt": query_prompt,
                "gpt_output": gpt_output,
            },
        )
        print(gpt_output.strip())
        gpt_output = gpt_output.strip()
        
        try:
            # Try to parse the JSON string without escaping
            json_output = json.loads(gpt_output)
        except json.JSONDecodeError:
            # If an error is raised, try to escape the string and parse it again
            try:
                json_output = json.loads(gpt_output.encode('unicode_escape').decode())
            except json.JSONDecodeError:
                print("The string could not be parsed as JSON.")
                return gpt_output, citations, "error", relevant_chunks
        
        bot_response = json_output["response"]
        query_type = json_output["query_type"]

        # print('bot response: ', bot_response, 'query type: ', query_type)

        if len(bot_response) < 700:
            return (bot_response, citations, query_type, relevant_chunks)
        else:
            system_prompt = f"""Please summarise the given answer in 700 characters or less. Only return the summarized answer and nothing else.\n"""
            
            query_prompt = f"""You are given the following response: {bot_response}"""
            prompt = [{"role": "system", "content": system_prompt}]
            prompt.append({"role": "user", "content": query_prompt})

            gpt_output = get_llm_response(prompt)
            app_logger.add_log(
                event_name="gpt4",
                details={
                    "system_prompt": system_prompt,
                    "query_prompt": query_prompt,
                    "gpt_output": gpt_output,
                },
            )
            return (gpt_output, citations, query_type, relevant_chunks)

    def generate_correction(
        self,
        row_query: dict[str, Any],
        row_response: dict[str, Any],
        row_correction: dict[str, Any],
        app_logger: AppLogger,
    ):
        
        if self.config["API_ACTIVATED"] is False:
            gpt_output = "API not activated"
            return gpt_output

        system_prompt = self.llm_prompts["generate_correction"]
        query = row_query["message_english"]
        response = row_response["message_english"]
        correction = row_correction["message"]
        query_prompt = f"""
        A user asked the following query:\n\
                "{query}"\n\
            A chatbot answered the following:\n\
            "{response}"\n\
            An expert corrected the response as follows:\n\
            "{correction}"\n\

        """
        transaction_message_id = row_query["message_id"]
        app_logger.add_log(
            event_name="get_correction",
            details={"system_prompt": system_prompt, "query_prompt": query_prompt, "transaction_message_id": transaction_message_id},
        )

        prompt = [{"role": "system", "content": system_prompt}]
        prompt.append({"role": "user", "content": query_prompt})

        gpt_output = get_llm_response(prompt)

        if len(gpt_output) < 700:
            return gpt_output
        else:
            system_prompt = f"""Please summarise the provided answer in 700 characters or less. Only return the summarized answer and nothing else.\n"""
            query_prompt = f"""You are given the following response: {gpt_output}"""
            prompt = [{"role": "system", "content": system_prompt}]
            prompt.append({"role": "user", "content": query_prompt})

            app_logger.add_log(
                event_name="gpt4",
                details={"system_prompt": system_prompt, "query_prompt": query_prompt},
            )
            gpt_output = get_llm_response(prompt)

            return gpt_output

    def follow_up_questions(
        self,
        query: str,
        response: str,
        user_type: str,
        app_logger: AppLogger,
        max_retries: int = 5,
    ):
        for i in range(max_retries):
            try:
                return self.follow_up_questions_helper(query, response, user_type, app_logger)
            except Exception as e:
                print(f"Error in follow_up_questions: {e}")
                continue
        return None

    def follow_up_questions_helper(
        self,
        query: str,
        response: str,
        user_type: str,
        app_logger: AppLogger,
    ) -> list[str]:
        """look at the chat history and suggest follow up questions

        Args:
            query (str): the query
            response (str): the response from the bot
            llm (OpenAI): an OpenAI model

        Returns:
            list[str]: a list of potential follow up questions
        """

        if self.config["API_ACTIVATED"] is False:
            print("API not activated")
            return ["Q1", "Q2", "Q3"]
        
        schema = {
            "name": "follow_up_questions_schema",
            "schema": {
                "type": "object",
                "properties": {
                    "questions": {
                        "type": "array",
                        "items": {
                            "type": "string",
                            "description": "A follow-up question"
                        },
                        "minItems": 3,
                        "maxItems": 3
                    }
                },
                "required": ["questions"]
            }
        }

        system_prompt = self.llm_prompts["follow_up_questions"]
        query_prompt = f"""
            A user asked the following query:\n\
                    "{query}"\n\
                A chatbot answered the following:\n\
                "{response}"\n\
            """

        prompt = [{"role": "system", "content": system_prompt}]
        prompt.append({"role": "user", "content": query_prompt})

        llm_out = get_llm_response(prompt, schema)
        next_questions = json.loads(llm_out)["questions"]

        app_logger.add_log(
            event_name="gpt4",
            details={
                "system_prompt": system_prompt,
                "query_prompt": query_prompt,
                "gpt_output": llm_out,
            },
        )

        return next_questions

    def update_kb_wa(self):
        client = chromadb.PersistentClient(
            path=self.persist_directory, settings=Settings(anonymized_telemetry=False)
        )

        collection = client.get_collection(
            name=self.config["PROJECT_NAME"], embedding_function=self.embedding_fn
        )

        collection_count = collection.count()
        print("collection ids count: ", collection_count)
        self.documents = DirectoryLoader(
            os.path.join(
                os.path.join(os.environ["APP_PATH"], os.environ["DATA_PATH"]),
                "kb_update_raw",
            ),
            glob=self.config["GLOB_SUFFIX"],
        ).load()
        self.texts = []
        self.sources = []
        for document in self.documents:
            next_text = RecursiveCharacterTextSplitter(chunk_size=1000).split_text(
                document.page_content
            )  # list of chunks
            self.texts.extend(next_text)
            self.sources.extend(
                [
                    document.metadata["source"].split("/")[-1][:-4]
                    for _ in range(len(next_text))
                ]
            )

        # if os.path.exists(self.persist_directory):
        #     shutil.rmtree(self.persist_directory)
        self.texts = [text.replace("\n\n", "\n") for text in self.texts]

        ids = []
        for index in range(len(self.texts)):
            ids.append([str(index + collection_count)])

        print("ids: ", ids)

        metadatas = []
        for source in self.sources:
            metadatas.append({"source": source})

        print("metadatas: ", metadatas)

        print("texts: ", self.texts)
        self.collection.add(
            ids=[str(index + collection_count) for index in range(len(self.texts))],
            metadatas=[{"source": source} for source in self.sources],
            documents=self.texts,
        )

        client = chromadb.PersistentClient(
            path=self.persist_directory, settings=Settings(anonymized_telemetry=False)
        )

        collection = client.get_collection(
            name=self.config["PROJECT_NAME"], embedding_function=self.embedding_fn
        )

        collection_count = collection.count()
        print("collection ids count: ", collection_count)
        return

    def create_embeddings(self):

        # if os.path.exists(self.persist_directory):
        #     shutil.rmtree(self.persist_directory)
            
        self.client = chromadb.PersistentClient(
            path=self.persist_directory, settings=Settings(anonymized_telemetry=False)
        )

        try:
            self.client.delete_collection(
                name=self.config["PROJECT_NAME"],
            )
        except:
            print("Creating new collection.")

        self.collection = self.client.create_collection(
            name=self.config["PROJECT_NAME"],
            embedding_function=self.embedding_fn,
        )
        self.documents = DirectoryLoader(
            os.path.join(
                os.path.join(os.environ["APP_PATH"], os.environ["DATA_PATH"]),
                "raw_documents",
            ),
            glob=self.config["GLOB_SUFFIX"],
        ).load()
        self.texts = []
        self.sources = []
        for document in self.documents:
            if 'kb update' in document.metadata['source'].strip().lower():
                print('Splitting text for kb update')
                next_text = document.page_content.split('##')[1:]
            else:
                print('Splitting text for normal document')
                next_text = RecursiveCharacterTextSplitter(chunk_size=1000).split_text(document.page_content)
            
            self.texts.extend(next_text)
            self.sources.extend(
                [
                    document.metadata["source"].split("/")[-1][:-4]
                    for _ in range(len(next_text))
                ]
            )

        self.texts = [text.replace("\n\n", "\n") for text in self.texts]
        self.collection.add(
            ids=[str(index) for index in range(len(self.texts))],
            metadatas=[{"source": source} for source in self.sources],
            documents=self.texts,
        )

        collection_count = self.collection.count()
        print("collection ids count: ", collection_count)
        return
