import asyncio
import os
import json
from byoeb.constants.feature_enums import FeatureFlag
from dotenv import load_dotenv

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
    print(f"Warning: Environment file not found at {environment_path}")

# Environment variables
# Whatsapp
env_whatsapp_token = os.getenv("WHATSAPP_VERIFICATION_TOKEN")
env_whatsapp_auth_token = os.getenv("WHATSAPP_AUTH_TOKEN")
env_whatsapp_phone_number_id = os.getenv("WHATSAPP_PHONE_NUMBER_ID")

# OpenAI
env_openai_api_key = os.getenv("OPENAI_API_KEY")
env_openai_org_id = os.getenv("OPENAI_ORG_ID")

# Azure cosmos db
env_mongo_db_connection_string = os.getenv("MONGO_DB_CONNECTION_STRING")

# Logger
env_appinsights_connection_string = os.getenv("APPINSIGHTS_CONNECTION_STRING")

# Azure Storage
env_azure_storage_connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

# Azure Cognitive Services
env_azure_cognitive_key = os.getenv("AZURE_COGNITIVE_KEY")

#Azure Speech Key
env_azure_speech_key= os.getenv("AZURE_SPEECH_KEY")
env_azure_openai_whisper_key= os.getenv("AZURE_OPENAI_WHISPER_KEY")

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
    print(entry)
    entry = entry.strip()
    if not entry:
        continue
    try:
        feature_flags.add(FeatureFlag(entry))
    except ValueError:
        raise RuntimeError("Unexpected feature flag: " + entry)
