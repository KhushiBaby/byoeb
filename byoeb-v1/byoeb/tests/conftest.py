# fixes crash during 'import chromadb' - see: https://docs.trychroma.com/docs/overview/troubleshooting#sqlite
import sys
import importlib.util
if importlib.util.find_spec("pysqlite3") is not None:
    __import__("pysqlite3")
    sys.modules["sqlite3"] = sys.modules.pop("pysqlite3")

import os
os.environ["AZURE_OPENAI_API_KEY"] = "sk-xxxx"
os.environ["AZURE_OPENAI_SPEECH_ENDPOINT"] = "http://localhost:8000"
os.environ["AZURE_COGNITIVE_REGION"] = "swedencentral"
os.environ["AZURE_COGNITIVE_TEXT_TO_SPEECH_RESOURCE"] = "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/dummy_resource_group/providers/Microsoft.CognitiveServices/accounts/dummy-speech-to-text-account"
os.environ["AZURE_COGNITIVE_TEXT_TO_TEXT_RESOURCE"] = "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/dummy_resource_group/providers/Microsoft.CognitiveServices/accounts/dummy-speech-to-text-account"