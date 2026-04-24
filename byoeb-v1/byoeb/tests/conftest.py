# Fixes crash during 'import chromadb' on some environments - see: https://docs.trychroma.com/docs/overview/troubleshooting#sqlite
# pysqlite3 is optional (e.g. not installed on Windows); use it only when available.
import sys
try:
    __import__("pysqlite3")
    sys.modules["sqlite3"] = sys.modules.pop("pysqlite3")
except ModuleNotFoundError:
    pass  # use default sqlite3

import os

# Set required environment variables for tests to prevent import errors
# These are dummy/test values and should never be used to access production resources
os.environ["AZURE_OPENAI_API_KEY"] = "sk-xxxx"
os.environ["AZURE_OPENAI_SPEECH_ENDPOINT"] = "http://localhost:8000"
os.environ["AZURE_COGNITIVE_REGION"] = "swedencentral"
os.environ["AZURE_COGNITIVE_TEXT_TO_SPEECH_RESOURCE"] = "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/dummy_resource_group/providers/Microsoft.CognitiveServices/accounts/dummy-speech-to-text-account"
os.environ["AZURE_COGNITIVE_TEXT_TO_TEXT_RESOURCE"] = "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/dummy_resource_group/providers/Microsoft.CognitiveServices/accounts/dummy-speech-to-text-account"

# Required environment variables for production resources (Issue #89)
# Set dummy values for tests to prevent accidental production access
os.environ["AZURE_STORAGE_BLOB_ACCOUNT_URL"] = "https://test-storage.blob.core.windows.net"
os.environ["AZURE_STORAGE_QUEUE_ACCOUNT_URL"] = "https://test-storage.queue.core.windows.net"
os.environ["AZURE_STORAGE_CONTAINER_NAME"] = "test-container"
# Required queue names (must be set to prevent accidental production access)
os.environ["AZURE_QUEUE_STATUS"] = "test-statusmessages"
os.environ["AZURE_QUEUE_BOT"] = "test-botmessages"
os.environ["AZURE_QUEUE_DEAD_LETTER"] = "test-deadletterqueue"
os.environ["AZURE_OPENAI_ENDPOINT"] = "https://test-openai.openai.azure.com/"
os.environ["AZURE_OPENAI_DEPLOYMENT_NAME"] = "test-deployment"
os.environ["AZURE_SEARCH_SERVICE_NAME"] = "test-search-service"
os.environ["AZURE_SEARCH_INDEX_NAME"] = "test-index"
os.environ["APP_LOGGER_NAME"] = "test-logger"
# MongoDB connection string must include database name
# Set a test connection string with database name if not already set
if "MONGO_DB_CONNECTION_STRING" not in os.environ:
    os.environ["MONGO_DB_CONNECTION_STRING"] = "mongodb://localhost:27017/test-db"

os.environ["AUTH_TOKEN_SECRET"] = "x" * 36