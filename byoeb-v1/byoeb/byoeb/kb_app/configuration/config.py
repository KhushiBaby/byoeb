import os
import json
from dotenv import load_dotenv

# Get the directory of the current script
current_dir = os.path.dirname(os.path.abspath(__file__))
app_config_path = os.path.join(current_dir, '..', 'app_config.json')
app_config_path = os.path.normpath(app_config_path)
app_config = None
with open(app_config_path, 'r') as file:
    app_config = json.load(file)

prompt_config_path = os.path.join(current_dir, '..', 'prompts.json')
prompt_config_path = os.path.normpath(prompt_config_path)
prompt_config = None
with open(prompt_config_path, 'r') as file:
    prompt_config = json.load(file)

environment_path = os.path.join(current_dir, '../../..', 'keys.env')
environment_path = os.path.normpath(environment_path)
# Use override=True to allow .env file values to override system environment variables
load_dotenv(environment_path, override=True)

# OpenAI
env_openai_api_key = os.getenv("OPENAI_API_KEY")
env_openai_org_id = os.getenv("OPENAI_ORG_ID")

# Azure Storage
env_azure_storage_connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

# Azure Search (optional - for API key authentication)
env_azure_search_api_key = os.getenv("AZURE_SEARCH_API_KEY")
# Azure Search Service Configuration (optional - will fallback to app_config.json if not set)
env_azure_search_service_name = os.getenv("AZURE_SEARCH_SERVICE_NAME")
env_azure_search_index_name = os.getenv("AZURE_SEARCH_INDEX_NAME")

env_azure_search_vectorizer_model_uri = os.getenv("AZURE_SEARCH_VECTORIZER_MODEL_URI")
env_azure_search_vectorizer_model_name = os.getenv("AZURE_SEARCH_VECTORIZER_MODEL_NAME")
env_azure_search_vectorizer_model_api_key = os.getenv("AZURE_SEARCH_VECTORIZER_MODEL_API_KEY")

# Azure Cognitive Services (optional - for API key authentication)
env_azure_cognitive_key = os.getenv("AZURE_COGNITIVE_KEY")

# Azure OpenAI API Key (for Azure OpenAI service)
env_azure_openai_key = os.getenv("AZURE_OPENAI_KEY") or os.getenv("AZURE_OPENAI_WHISPER_KEY")
# Azure OpenAI Endpoint and Deployment (optional - for staging/production switching)
env_azure_openai_endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
env_azure_openai_deployment_name = os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME")

# Vector Store Type (optional - will fallback to app_config.json if not set)
# Options: "azure_vector_search", "chroma", "llama_index_chroma"
env_vector_store_type = os.getenv("VECTOR_STORE_TYPE")
# ChromaDB persist directory (optional - for local ChromaDB stores)
env_persist_directory = os.getenv("PERSIST_DIRECTORY")
