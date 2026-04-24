import asyncio
import os
import json
import logging
from byoeb.constants.feature_enums import FeatureFlag
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

# Get the directory of the current script
current_dir = os.path.dirname(os.path.abspath(__file__))
app_config_path = os.path.join(current_dir, '..', 'app_config.json')
app_config_path = os.path.normpath(app_config_path)
app_config = None
with open(app_config_path, 'r', encoding="utf-8") as file:
    app_config = json.load(file)

app_tempdir: asyncio.Future[str] = asyncio.Future()

bot_config_path = os.path.join(current_dir, '..', 'bot_config.json')
bot_config_path = os.path.normpath(bot_config_path)
bot_config = None
with open(bot_config_path, 'r', encoding="utf-8") as file:
    bot_config = json.load(file)

environment_path = os.path.join(current_dir, '../../..', 'keys.env')
environment_path = os.path.normpath(environment_path)
if os.path.exists(environment_path):
    # Use override=True to allow .env file values to override system environment variables
    load_dotenv(environment_path, override=True)
else:
    logger.warning("Environment file not found at %s", environment_path)

# Environment variables
env_app = os.getenv("APP_ENV", "LOCAL")  # "PROD" in production

# OpenAI
env_openai_api_key = os.getenv("OPENAI_API_KEY")
env_openai_org_id = os.getenv("OPENAI_ORG_ID")

# Azure cosmos db
env_mongo_db_connection_string = os.getenv("MONGO_DB_CONNECTION_STRING")

# Logger
env_appinsights_connection_string = os.getenv("APPINSIGHTS_CONNECTION_STRING")
env_app_logger_name = os.getenv("APP_LOGGER_NAME")

# Azure Storage
env_azure_storage_connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
env_azure_storage_blob_account_url = os.getenv("AZURE_STORAGE_BLOB_ACCOUNT_URL")
env_azure_storage_queue_account_url = os.getenv("AZURE_STORAGE_QUEUE_ACCOUNT_URL")
env_azure_storage_container_name = os.getenv("AZURE_STORAGE_CONTAINER_NAME")

# Azure Queue Names - REQUIRED (must be set to prevent accidental production access)
# These MUST be set in keys.env to explicitly specify which queues to use
# Queue names and storage URL together determine the environment (local/staging/production)
env_azure_queue_status = os.getenv("AZURE_QUEUE_STATUS")
env_azure_queue_bot = os.getenv("AZURE_QUEUE_BOT")
env_azure_queue_dead_letter = os.getenv("AZURE_QUEUE_DEAD_LETTER")

# Validate that queue names are set (fail fast if not configured)
if not env_azure_queue_status:
    raise ValueError(
        "AZURE_QUEUE_STATUS environment variable must be set in keys.env. "
    )
if not env_azure_queue_bot:
    raise ValueError(
        "AZURE_QUEUE_BOT environment variable must be set in keys.env. "
    )
if not env_azure_queue_dead_letter:
    raise ValueError(
        "AZURE_QUEUE_DEAD_LETTER environment variable must be set in keys.env. "
    )

# Azure Cognitive Services
env_azure_cognitive_key = os.getenv("AZURE_COGNITIVE_KEY")
env_azure_cognitive_region = os.getenv("AZURE_COGNITIVE_REGION")
env_azure_cognitive_text_to_speech_resource = os.getenv("AZURE_COGNITIVE_TEXT_TO_SPEECH_RESOURCE")
env_azure_cognitive_text_to_text_resource = os.getenv("AZURE_COGNITIVE_TEXT_TO_TEXT_RESOURCE")

#Azure Speech Key
env_azure_speech_key= os.getenv("AZURE_SPEECH_KEY")
env_azure_openai_speech_key = os.getenv("AZURE_OPENAI_SPEECH_KEY") or os.getenv("AZURE_OPENAI_WHISPER_KEY")
env_azure_openai_speech_endpoint = os.getenv("AZURE_OPENAI_SPEECH_ENDPOINT")

# Azure Search
env_azure_search_api_key = os.getenv("AZURE_SEARCH_API_KEY")
env_azure_search_service_name = os.getenv("AZURE_SEARCH_SERVICE_NAME")
env_azure_search_index_name = os.getenv("AZURE_SEARCH_INDEX_NAME")

# Azure OpenAI Configuration (optional - for staging/production switching)
env_azure_openai_key = os.getenv("AZURE_OPENAI_KEY") or os.getenv("AZURE_OPENAI_WHISPER_KEY")
env_azure_openai_endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
env_azure_openai_deployment_name = os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME")

# Vector Store Type (optional - will fallback to app_config.json if not set)
# Options: "azure_vector_search", "chroma", "llama_index_chroma"
env_vector_store_type = os.getenv("VECTOR_STORE_TYPE")
# ChromaDB persist directory (optional - for local ChromaDB stores)
env_persist_directory = os.getenv("PERSIST_DIRECTORY")

# Others
env_ashabot_message_cache_capacity = os.getenv("ASHABOT_MESSAGE_CACHE_CAPACITY")

env_ashabot_feature_flags = os.getenv("ASHABOT_FEATURE_FLAGS")
feature_flags: set[FeatureFlag] = set()
for entry in (env_ashabot_feature_flags or "").split(","):
    logger.debug("Feature flag entry: %s", entry)
    entry = entry.strip()
    if not entry:
        continue
    try:
        feature_flags.add(FeatureFlag(entry))
    except ValueError:
        raise RuntimeError("Unexpected feature flag: " + entry)

# Auth
env_auth_token_secret = os.getenv("AUTH_TOKEN_SECRET")
env_auth_token_ttl_seconds = os.getenv("AUTH_TOKEN_TTL_SECONDS")
env_auth_token_algorithm = os.getenv("AUTH_TOKEN_ALGORITHM")
env_auth_token_issuer = os.getenv("AUTH_TOKEN_ISSUER")
env_auth_token_audience = os.getenv("AUTH_TOKEN_AUDIENCE")
env_auth_token_leeway_seconds = os.getenv("AUTH_TOKEN_LEEWAY_SECONDS")

# Public base URL
env_public_base_url = os.getenv("PUBLIC_BASE_URL")
