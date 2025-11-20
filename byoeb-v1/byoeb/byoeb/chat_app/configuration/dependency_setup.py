import byoeb.chat_app.configuration.config as env_config
from byoeb.chat_app.configuration.config import app_config

import time, json, traceback, uuid, asyncio
import logging

_logger = logging.getLogger("flow")

def _safe_json(obj):
    try:
        return json.dumps(obj, ensure_ascii=False)[:50_000]  # cap size
    except Exception:
        return f"<non-serializable type={type(obj).__name__}>"

def log_async_call(name):
    def decorator(fn):
        if asyncio.iscoroutinefunction(fn):
            async def wrapper(*args, **kwargs):
                rid = str(uuid.uuid4())[:8]  # request trace id
                t0 = time.perf_counter()
                _logger.info(f"[{rid}] ▶ {name} args={_safe_json(args)} kwargs={_safe_json(kwargs)}")
                try:
                    result = await fn(*args, **kwargs)
                    dt = (time.perf_counter() - t0) * 1000
                    _logger.info(f"[{rid}] ◀ {name} ok in {dt:.1f}ms result={_safe_json(getattr(result, '__dict__', result))}")
                    return result
                except Exception as e:
                    dt = (time.perf_counter() - t0) * 1000
                    _logger.exception(f"[{rid}] ✖ {name} failed in {dt:.1f}ms: {e}\n{traceback.format_exc()}")
                    raise
            return wrapper
        else:
            def wrapper(*args, **kwargs):
                rid = str(uuid.uuid4())[:8]
                t0 = time.perf_counter()
                _logger.info(f"[{rid}] ▶ {name} args={_safe_json(args)} kwargs={_safe_json(kwargs)}")
                try:
                    result = fn(*args, **kwargs)
                    dt = (time.perf_counter() - t0) * 1000
                    _logger.info(f"[{rid}] ◀ {name} ok in {dt:.1f}ms result={_safe_json(getattr(result, '__dict__', result))}")
                    return result
                except Exception as e:
                    dt = (time.perf_counter() - t0) * 1000
                    _logger.exception(f"[{rid}] ✖ {name} failed in {dt:.1f}ms: {e}\n{traceback.format_exc()}")
                    raise
            return wrapper
    return decorator


# App logger
if env_config.env_appinsights_connection_string:
    from azure.monitor.opentelemetry import configure_azure_monitor
    print("✅ App Insights connection string set. Enabling Azure logging.")
    configure_azure_monitor(
        logger_name=app_config["app_logger"]["azure"]["logger_name"],
        connection_string=env_config.env_appinsights_connection_string,
        instrumentations=["fastapi", "urllib3"]
    )
else:
    print("⚠️ App Insights connection string not set. Skipping Azure logging.")

import byoeb.utils.utils as byoeb_utils
from byoeb.factory import (
    ChannelRegisterFactory,
    ChannelClientFactory,
    QueueProducerFactory,
    MongoDBFactory
)
from byoeb.handler import (
    ChannelRegisterHandler,
    QueueProducerHandler,
    UsersHandler
)

from byoeb.services.databases.mongo_db import UserMongoDBService, MessageMongoDBService

SINGLETON = "singleton"

# channel
channel_register_factory = ChannelRegisterFactory()
channel_client_factory = ChannelClientFactory(config=app_config)
channel_register_handler = ChannelRegisterHandler(channel_register_factory)

# mongo db
mongo_db_factory = MongoDBFactory(
    config=app_config,
    scope=SINGLETON
)

user_db_service = UserMongoDBService(
    config=app_config,
    mongo_db_factory=mongo_db_factory
)
message_db_service = MessageMongoDBService(
    config=app_config,
    mongo_db_factory=mongo_db_factory,
    user_db_service=user_db_service  # Pass user_db_service for leaderboard functionality
)

# Leaderboard service functions
from byoeb.services.leaderboard import LeaderboardService
from typing import Optional

_leaderboard_service: Optional[LeaderboardService] = None

async def get_leaderboard_service() -> LeaderboardService:
    """Get or create leaderboard service instance."""
    global _leaderboard_service
    if _leaderboard_service is None:
        _leaderboard_service = LeaderboardService(user_db_service, message_db_service)
    return _leaderboard_service

# message queue
queue_producer_factory = QueueProducerFactory(
    config=app_config,
    scope = SINGLETON
)
message_producer_handler = QueueProducerHandler(
    config=app_config,
    queue_producer_factory=queue_producer_factory,
    message_db_service=message_db_service
)

# message consumer
from byoeb.listener.message_consumer import QueueConsumer
message_consumer = QueueConsumer(
    config=app_config,
    account_url=app_config["message_queue"]["azure"]["account_url"],
    queue_name=app_config["message_queue"]["azure"]["queue_bot"],
    consuemr_type=app_config["app"]["queue_provider"],
    user_db_service=user_db_service,
    message_db_service=message_db_service,
    channel_client_factory=channel_client_factory
)

# user handler
users_handler = UsersHandler(
    db_provider=app_config["app"]["db_provider"],
    mongo_db_facory=mongo_db_factory
)

# Text translator
from byoeb_integrations.translators.text.azure.async_azure_text_translator import AsyncAzureTextTranslator
# TODO: factory implementation
if env_config.env_azure_cognitive_key:
    print("✅ Azure Cognitive Services key set. Enabling Azure text translator.")
    text_translator = AsyncAzureTextTranslator(
        key=env_config.env_azure_cognitive_key,
        region=app_config["translators"]["text"]["azure_cognitive"]["region"],
        resource_id=app_config["translators"]["text"]["azure_cognitive"]["resource_id"],
    )
else:
    from azure.identity import get_bearer_token_provider, DefaultAzureCredential
    print("⚠️ Azure Cognitive Services key not set. Defaulting to DefaultAzureCredential for Azure text translator")
    text_translator = AsyncAzureTextTranslator(
    credential=DefaultAzureCredential(),
    region=app_config["translators"]["text"]["azure_cognitive"]["region"],
    resource_id=app_config["translators"]["text"]["azure_cognitive"]["resource_id"],
)

# Speech translator
# TODO: factory implementation
from byoeb_integrations.translators.speech.azure.async_azure_speech_translator import AsyncAzureSpeechTranslator
voice_dict = {
    "male": {
        "en-IN": "en-IN-PrabhatNeural",
        "hi-IN": "hi-IN-MadhurNeural",
        "mr-IN": "mr-IN-ManoharNeural",
        "te-IN": "te-IN-MohanNeural"
    },
    "female": {
        "en-IN": "en-IN-NeerjaNeural",
        "hi-IN": "hi-IN-SwaraNeural",
        "mr-IN": "mr-IN-AarohiNeural",
        "te-IN": "te-IN-ShrutiNeural"
    },
}
if env_config.env_azure_speech_key:
    print("✅ Azure Cognitive Services key set. Enabling Azure speech translator.")
    speech_translator = AsyncAzureSpeechTranslator(
        key=env_config.env_azure_speech_key,
        region=app_config["translators"]["speech"]["azure_cognitive"]["region"],
        resource_id=app_config["translators"]["speech"]["azure_cognitive"]["resource_id"],
    )
else:
    print("⚠️ Azure Cognitive Services key not set. Defaulting to DefaultAzureCredential for Azure speech translator")
    from azure.identity import get_bearer_token_provider, DefaultAzureCredential

    token_provider = get_bearer_token_provider(
        DefaultAzureCredential(), app_config["app"]["azure_cognitive_endpoint"]
    )
    speech_translator = AsyncAzureSpeechTranslator(
        token_provider=token_provider,
        region=app_config["translators"]["speech"]["azure_cognitive"]["region"],
        resource_id=app_config["translators"]["speech"]["azure_cognitive"]["resource_id"],
    )

speech_translator.change_voice_dict(voice_dict)
from byoeb_integrations.translators.speech.azure.async_azure_openai_whisper import AsyncAzureOpenAIWhisper

if env_config.env_azure_openai_whisper_key:
    print("✅ Azure OpenAI Whisper key set. Enabling Azure OpenAI Whisper translator.")
    speech_translator_whisper = AsyncAzureOpenAIWhisper(
    api_key=env_config.env_azure_openai_whisper_key,
    model=app_config["translators"]["speech"]["azure_oai"]["model"],
    azure_endpoint=app_config["translators"]["speech"]["azure_oai"]["endpoint"],
    api_version=app_config["translators"]["speech"]["azure_oai"]["api_version"]
    )
else:
    print("⚠️ Azure OpenAI Whisper key not set. Defaulting to DefaultAzureCredential for Azure OpenAI Whisper translator")
    from azure.identity import get_bearer_token_provider, DefaultAzureCredential

    token_provider = get_bearer_token_provider(
        DefaultAzureCredential(), app_config["app"]["azure_cognitive_endpoint"]
    )
    speech_translator_whisper = AsyncAzureOpenAIWhisper(
    token_provider=token_provider,
    model=app_config["translators"]["speech"]["azure_oai"]["model"],
    azure_endpoint=app_config["translators"]["speech"]["azure_oai"]["endpoint"],
    api_version=app_config["translators"]["speech"]["azure_oai"]["api_version"]
)

# vector store
import os
from byoeb_integrations.embeddings.llama_index.azure_openai import AzureOpenAIEmbed
from byoeb_integrations.vector_stores.azure_vector_search.azure_vector_search import AzureVectorStore
from byoeb_integrations.embeddings.chroma.llama_index_azure_openai import AzureOpenAIEmbeddingFunction
from byoeb_core.vector_stores.base import BaseVectorStore

# Use environment variables for Azure OpenAI endpoint and deployment if set, otherwise fallback to app_config.json
azure_openai_endpoint = env_config.env_azure_openai_endpoint or app_config["embeddings"]["azure"]["endpoint"]
azure_openai_deployment_name = env_config.env_azure_openai_deployment_name or app_config["embeddings"]["azure"]["deployment_name"]

# Azure OpenAI Embed - try API key first, fallback to token provider
if env_config.env_azure_openai_whisper_key or env_config.env_azure_openai_key:
    print("✅ Azure OpenAI Embed key set. Enabling Azure OpenAI Embed.")
    azure_openai_key = env_config.env_azure_openai_key or env_config.env_azure_openai_whisper_key
    azure_openai_embed = AzureOpenAIEmbed(
        model=app_config["embeddings"]["azure"]["model"],
        deployment_name=azure_openai_deployment_name,
        azure_endpoint=azure_openai_endpoint,
        api_key=azure_openai_key,
        api_version=app_config["embeddings"]["azure"]["api_version"]
    )
else:
    from azure.identity import get_bearer_token_provider, DefaultAzureCredential
    print("⚠️ Azure OpenAI Embed key not set. Defaulting to DefaultAzureCredential for Azure OpenAI Embed")
    token_provider = get_bearer_token_provider(
        DefaultAzureCredential(), app_config["app"]["azure_cognitive_endpoint"]
    )
    azure_openai_embed = AzureOpenAIEmbed(
        model=app_config["embeddings"]["azure"]["model"],
        deployment_name=azure_openai_deployment_name,
        azure_endpoint=azure_openai_endpoint,
        token_provider=token_provider,
        api_version=app_config["embeddings"]["azure"]["api_version"]
    )

# Vector Store Type Configuration - use environment variable if set, otherwise fallback to app_config.json
# Default to "azure_vector_search" if not specified (for backward compatibility)
vector_store_type = env_config.env_vector_store_type or "azure_vector_search"

# Initialize vector store based on configuration
vector_store: BaseVectorStore = None

if vector_store_type == "azure_vector_search":
    # Azure Search Service Configuration - use environment variables if set, otherwise fallback to app_config.json
    azure_search_service_name = env_config.env_azure_search_service_name or app_config["vector_store"]["azure_vector_search"]["service_name"]
    azure_search_doc_index_name = env_config.env_azure_search_index_name or app_config["vector_store"]["azure_vector_search"]["doc_index_name"]
    
    if env_config.env_azure_search_api_key:
        from azure.core.credentials import AzureKeyCredential
        print("✅ Azure Search API key set. Enabling Azure vector store.")
        credential = AzureKeyCredential(env_config.env_azure_search_api_key)
    else:
        from azure.identity import DefaultAzureCredential
        credential = DefaultAzureCredential()   
        print("⚠️ Azure Search API key not set. Defaulting to DefaultAzureCredential")
    
    # Azure Vector Store uses LlamaIndex embedding function
    embedding_function = azure_openai_embed.get_embedding_function()
    
    vector_store = AzureVectorStore(
        service_name=azure_search_service_name,
        index_name=azure_search_doc_index_name,
        embedding_function=embedding_function,
        credential=credential
    )
    print(f"✅ Initialized Azure Vector Store: {azure_search_service_name}/{azure_search_doc_index_name}")

elif vector_store_type == "chroma":
    # ChromaDB Vector Store - needs ChromaDB-compatible embedding function
    collection_name = app_config["vector_store"]["chroma"]["doc_index_name"]
    persist_directory = env_config.env_persist_directory
    
    if not persist_directory:
        # Default persist directory if not specified
        git_root_dir = byoeb_utils.get_git_root_path()
        persist_directory = os.path.join(git_root_dir, "../vector_db")
    
    # Ensure persist directory exists
    os.makedirs(persist_directory, exist_ok=True)
    
    # Reuse the existing azure_openai_embed instance to create ChromaDB-compatible wrapper
    # This avoids creating a duplicate AzureOpenAIEmbed instance
    llama_index_embedding = azure_openai_embed.get_embedding_function()
    
    # Use the reusable ChromaDB embedding function wrapper from byoeb_integrations
    chroma_embedding_function = AzureOpenAIEmbeddingFunction(
        embedding_instance=llama_index_embedding
    )
    
    from byoeb_integrations.vector_stores.chroma.base import ChromaDBVectorStore
    vector_store = ChromaDBVectorStore(
        persist_directory=persist_directory,
        collection_name=collection_name,
        embedding_function=chroma_embedding_function
    )
    print(f"✅ Initialized ChromaDB Vector Store: {persist_directory}/{collection_name}")

elif vector_store_type == "llama_index_chroma":
    # LlamaIndex ChromaDB Vector Store - uses LlamaIndex embedding function
    collection_name = app_config["vector_store"]["llama_index_chroma"]["doc_index_name"]
    persist_directory = env_config.env_persist_directory
    
    if not persist_directory:
        # Default persist directory if not specified
        git_root_dir = byoeb_utils.get_git_root_path()
        persist_directory = os.path.join(git_root_dir, "../vector_db")
    
    # Ensure persist directory exists
    os.makedirs(persist_directory, exist_ok=True)
    
    # LlamaIndex ChromaDB Store uses LlamaIndex embed model
    embedding_function = azure_openai_embed.get_embedding_function()
    
    from byoeb_integrations.vector_stores.llama_index.llama_index_chroma_store import LlamaIndexChromaDBStore
    vector_store = LlamaIndexChromaDBStore(
        persist_directory=persist_directory,
        collection_name=collection_name,
        embedding_function=embedding_function
    )
    print(f"✅ Initialized LlamaIndex ChromaDB Vector Store: {persist_directory}/{collection_name}")

else:
    raise ValueError(
        f"Invalid vector_store type: {vector_store_type}. "
        f"Supported types: 'azure_vector_search', 'chroma', 'llama_index_chroma'"
    )

# llm
# from byoeb_integrations.llms.llama_index.llama_index_azure_openai import AsyncLLamaIndexAzureOpenAILLM
# llm_client = AsyncLLamaIndexAzureOpenAILLM(
#     model=app_config["llms"]["azure"]["model"],
#     deployment_name=app_config["llms"]["azure"]["deployment_name"],
#     azure_endpoint=app_config["llms"]["azure"]["endpoint"],
#     token_provider=token_provider,
#     api_version=app_config["llms"]["azure"]["api_version"]
# )
from byoeb_integrations.llms.llama_index.llama_index_openai import AsyncLLamaIndexOpenAILLM
llm_client = AsyncLLamaIndexOpenAILLM(
    model=app_config["llms"]["openai"]["model"],
    api_key=env_config.env_openai_api_key,
    api_version=app_config["llms"]["openai"]["api_version"],
    organization=env_config.env_openai_org_id
)

llm_translate_and_rewrite_client = AsyncLLamaIndexOpenAILLM(
    model=app_config["llms"]["openai"]["model"],
    api_key=env_config.env_openai_api_key,
    api_version=app_config["llms"]["openai"]["api_version"],
    organization=env_config.env_openai_org_id
)


# Process user message Chain of Responsibility
from byoeb.services.chat.message_handlers import (
    ByoebUserProcess,
    ByoebUserGenerateResponse, 
    ByoebUserSendResponse
)
byoeb_user_send_response = ByoebUserSendResponse(
    user_db_service=user_db_service,
    message_db_service=message_db_service
)
byoeb_user_generate_response = ByoebUserGenerateResponse(successor=byoeb_user_send_response)
byoeb_user_process = ByoebUserProcess(successor=byoeb_user_generate_response)

# Process expert message Chain of Responsibility
from byoeb.services.chat.message_handlers import (
    ByoebExpertProcess,
    ByoebExpertGenerateResponse, 
    ByoebExpertSendResponse
)
byoeb_expert_send_response = ByoebExpertSendResponse(
    user_db_service=user_db_service,
    message_db_service=message_db_service
)
byoeb_expert_generate_response = ByoebExpertGenerateResponse(successor=byoeb_expert_send_response)
byoeb_expert_process = ByoebExpertProcess(successor=byoeb_expert_generate_response)

from byoeb_core.media_storage.base import BaseMediaStorage
from byoeb_integrations.media_storage.azure.async_azure_blob_storage import AsyncAzureBlobStorage
from azure.identity import DefaultAzureCredential

container_name = app_config["media_storage"]["azure"]["container_name"]
account_url = app_config["media_storage"]["azure"]["account_url"]

if env_config.env_azure_storage_connection_string:
    media_storage: BaseMediaStorage = AsyncAzureBlobStorage(
        container_name=container_name,
        account_url=None,
        credentials=None,
        connection_string=env_config.env_azure_storage_connection_string
    )
elif account_url:
    media_storage: BaseMediaStorage = AsyncAzureBlobStorage(
        container_name=container_name,
        account_url=account_url,
        credentials=DefaultAzureCredential()
    )
else:
    media_storage = None
    print("⚠️ Azure Blob Storage not configured. Media storage disabled.")

# Scheduler configuration
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.jobstores.mongodb import MongoDBJobStore
from apscheduler.executors.asyncio import AsyncIOExecutor
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR
import pymongo
from byoeb.chat_app.configuration.config import env_mongo_db_connection_string

# MongoDB connection configuration for scheduler job store
MONGODB_URL = env_mongo_db_connection_string
MONGODB_DATABASE = app_config["databases"]["mongo_db"]["database_name"]
MONGODB_COLLECTION = app_config["databases"]["mongo_db"]["jobs_collection"]

# Initialize MongoDB client and job store
mongodb_client = pymongo.MongoClient(MONGODB_URL)
mongodb_jobstore = MongoDBJobStore(
    database=MONGODB_DATABASE,
    collection=MONGODB_COLLECTION,
    client=mongodb_client
)

# Initialize the scheduler with MongoDB job store
scheduler = AsyncIOScheduler(
    jobstores={'default': mongodb_jobstore},
    executors={'default': AsyncIOExecutor()},
    job_defaults={'coalesce': False, 'max_instances': 1}
)

def get_scheduler() -> AsyncIOScheduler:
    """Get the scheduler instance."""
    return scheduler

def start_scheduler():
    """Start the scheduler."""
    if not scheduler.running:
        scheduler.start()
        print("✅ Background job scheduler started")

def stop_scheduler():
    """Stop the scheduler."""
    if scheduler.running:
        scheduler.shutdown()
        print("✅ Background job scheduler stopped")