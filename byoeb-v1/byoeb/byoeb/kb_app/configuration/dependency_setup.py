import os
import byoeb.kb_app.configuration.config as env_config
from byoeb.kb_app.configuration.config import app_config
from azure.identity import DefaultAzureCredential, get_bearer_token_provider
from byoeb_integrations.media_storage.azure.async_azure_blob_storage import AsyncAzureBlobStorage
from byoeb_integrations.embeddings.llama_index.azure_openai import AzureOpenAIEmbed
from byoeb_integrations.vector_stores.azure_vector_search.azure_vector_search import AzureVectorStore
from byoeb_integrations.llms.llama_index.llama_index_openai import AsyncLLamaIndexOpenAILLM
from byoeb_integrations.embeddings.chroma.llama_index_azure_openai import AzureOpenAIEmbeddingFunction
from byoeb_core.media_storage.base import BaseMediaStorage
from byoeb_core.vector_stores.base import BaseVectorStore

account_url = app_config["media_storage"]["azure"]["account_url"]
container_name = app_config["media_storage"]["azure"]["container_name"]
model = app_config["embeddings"]["azure"]["model"]
deployment_name = env_config.env_azure_openai_deployment_name or app_config["embeddings"]["azure"]["deployment_name"]
aoai_endpoint = env_config.env_azure_openai_endpoint or app_config["embeddings"]["azure"]["endpoint"]
cognitive_services_endpoint = app_config["app"]["azure_cognitive_endpoint"]
api_version = app_config["embeddings"]["azure"]["api_version"]
default_credential = DefaultAzureCredential()

llm_client = AsyncLLamaIndexOpenAILLM(
    model=app_config["llms"]["openai"]["model"],
    api_key=env_config.env_openai_api_key,
    api_version=app_config["llms"]["openai"]["api_version"],
    organization=env_config.env_openai_org_id
)

# Azure OpenAI Embed - try API key first, fallback to token provider
if env_config.env_azure_openai_key:
    # Use Azure OpenAI specific key if available
    azure_openai_embed = AzureOpenAIEmbed(
        model=model,
        deployment_name=deployment_name,
        azure_endpoint=aoai_endpoint,
        api_key=env_config.env_azure_openai_key,
        api_version=api_version
    )
elif env_config.env_azure_cognitive_key:
    # Fallback to cognitive key if Azure OpenAI key not available
    azure_openai_embed = AzureOpenAIEmbed(
        model=model,
        deployment_name=deployment_name,
        azure_endpoint=aoai_endpoint,
        api_key=env_config.env_azure_cognitive_key,
        api_version=api_version
    )
else:
    # Last resort: use token provider with default credentials
    azure_openai_token_provider = get_bearer_token_provider(default_credential, "https://cognitiveservices.azure.com/.default")
    azure_openai_embed = AzureOpenAIEmbed(
        model=model,
        deployment_name=deployment_name,
        azure_endpoint=aoai_endpoint,
        token_provider=azure_openai_token_provider,
        api_version=api_version
    )

if env_config.env_azure_storage_connection_string:
    amedia_storage: BaseMediaStorage = AsyncAzureBlobStorage(
        container_name=container_name,
        account_url=None,
        credentials=None,
        connection_string=env_config.env_azure_storage_connection_string
    )
    print("✅ Azure Storage API key set. Enabling Azure Blob Storage.")
else:
    amedia_storage: BaseMediaStorage = AsyncAzureBlobStorage(
        container_name=container_name,
        account_url=account_url,
        credentials=DefaultAzureCredential()
    )

# Vector Store Type Configuration - use environment variable if set, otherwise fallback to app_config.json
# Default to "azure_vector_search" if not specified (for backward compatibility)
vector_store_type = env_config.env_vector_store_type or "azure_vector_search"
# Initialize vector store based on configuration
vector_store: BaseVectorStore = None

if vector_store_type == "azure_vector_search":
    from azure.search.documents.indexes.models import AzureOpenAIVectorizerParameters

    # Azure Search Service Configuration - use environment variables if set, otherwise fallback to app_config.json
    azure_search_service_name = env_config.env_azure_search_service_name or app_config["vector_store"]["azure_vector_search"]["service_name"]
    azure_search_doc_index_name = env_config.env_azure_search_index_name or app_config["vector_store"]["azure_vector_search"]["doc_index_name"]
    if env_config.env_azure_search_api_key:
        from azure.core.credentials import AzureKeyCredential
        print("✅ Azure Search API key set. Enabling Azure vector store.")
        credential = AzureKeyCredential(env_config.env_azure_search_api_key)
    else:
        credential = DefaultAzureCredential()   
        print("⚠️ Azure Search API key not set. Defaulting to DefaultAzureCredential")
    
    # Azure Vector Store uses LlamaIndex embedding function
    embedding_function = azure_openai_embed.get_embedding_function()
    
    vector_store = AzureVectorStore(
        service_name=azure_search_service_name,
        index_name=azure_search_doc_index_name,
        embedding_function=embedding_function,
        credential=credential,
        vectorizer_params=AzureOpenAIVectorizerParameters(
            resource_url=env_config.env_azure_search_vectorizer_model_uri,
            deployment_id=env_config.env_azure_search_vectorizer_model_name,
            deployment_name=env_config.env_azure_search_vectorizer_model_name,
            model_name=env_config.env_azure_search_vectorizer_model_name,
            api_key=env_config.env_azure_search_vectorizer_model_api_key
        )
    )
    print(f"✅ Initialized Azure Vector Store: {azure_search_service_name}/{azure_search_doc_index_name}")

elif vector_store_type == "chroma":
    # ChromaDB Vector Store - needs ChromaDB-compatible embedding function
    collection_name = app_config["vector_store"]["chroma"]["doc_index_name"]
    persist_directory = env_config.env_persist_directory
    
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