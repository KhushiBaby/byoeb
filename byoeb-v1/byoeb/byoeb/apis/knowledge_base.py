from io import BytesIO
import logging
import tempfile
from byoeb.kb_app.configuration.dependency_setup import amedia_storage, vector_store
from byoeb.services.knowledge_base.kb_service import upload as kb_upload
from fastapi import APIRouter, File, HTTPException, Query, UploadFile, status
from fastapi.responses import JSONResponse, Response, StreamingResponse

from byoeb_core.models.media_storage.file_data import FileMetadata
from byoeb_core.models.vector_stores.chunk import Chunk
from byoeb_core.vector_stores.base import VectorStoreMetadata
from byoeb_integrations.vector_stores.azure_vector_search.azure_vector_search import AzureVectorSearchType

KB_API_NAME = 'kb_api'

kb_media_apis_router = APIRouter(prefix="/storage", tags=["Media storage"])
kb_vector_apis_router = APIRouter(prefix="/vector", tags=["Vector storage"])
_logger = logging.getLogger(KB_API_NAME)

# valid MIME types and their corresponding labels - used to validate files being uploaded to media store
ALLOWED_MIME_TYPES = {
    "text/csv": "CSV (.csv)",
    "text/markdown": "Markdown (.md)",
    "application/json": "JSON (.json)",
    "text/plain": "TXT (.txt)",
}

@kb_media_apis_router.get("/list")
async def list_files() -> list[FileMetadata]:
    """
    Lists properties of all files in the store.
    """
    return await amedia_storage.aget_all_files_properties()

@kb_media_apis_router.get("/file/info")
async def get_file(file_name: str = Query(description="Path to the file")) -> FileMetadata:
    """
    Lists properties of the given file in the store.
    """
    result = await amedia_storage.aget_file_properties(file_name)
    if result is None: raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="File not found")
    return result

@kb_media_apis_router.get("/file")
async def download_file(file_name: str = Query(description="Path to the file")) -> StreamingResponse:
    """
    Download a given file from the store.
    """
    result = await amedia_storage.adownload_file(file_name)
    if result is None: raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="File not found")
    return StreamingResponse(BytesIO(result.data), media_type="application/octet-stream", headers={
        "Content-Disposition": f'attachment; filename="{result.metadata.file_name if result.metadata else file_name}"'
    })

@kb_media_apis_router.post("/file")
async def upload_file(file: UploadFile = File(description="Upload a .txt file", media_type="text/plain")):
    """
    Upload a given file to the store.
    """
    if file.content_type not in ALLOWED_MIME_TYPES:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid file type, allowed types: %s" % ", ".join(ALLOWED_MIME_TYPES.values()))

    if file.filename is None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Uploaded file must have a filename")

    with tempfile.NamedTemporaryFile(prefix="ashabot-kb-app-") as tmp:
        tmp.write(await file.read())
        tmp.seek(0)
        code, _ = await amedia_storage.aupload_file(file.filename, tmp.name)

    return Response(status_code=code)

@kb_media_apis_router.delete("/file")
async def delete_file(file_name: str = Query(description="Path to the file")) -> FileMetadata:
    """
    Delete a given file from the store.
    """
    result = await amedia_storage.aget_file_properties(file_name)
    if result is None: raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="File not found")
    await amedia_storage.adelete_file(file_name)
    return result

@kb_vector_apis_router.get("/index")
async def index_file(files: set[str] = Query(description="Path to files to load")):
    """
    Index a file from media storage into the vector storage.
    """
    _logger.info("🚀 Starting knowledge base load from blob store")
    existing = await amedia_storage.aget_all_files_properties()
    selected = [file for file in existing if file.file_name in files]
    count = await kb_upload(selected)
    _logger.info(f"✅ Successfully loaded {count} documents into knowledge base")
    return JSONResponse(status_code=status.HTTP_200_OK, content={"message": f"Loaded {count} documents"})

@kb_vector_apis_router.get("/search")
async def search_chunks(
    query: str = Query(description="Query to search the storage for"),
    search_type: AzureVectorSearchType = Query(default=AzureVectorSearchType.DENSE),
    k: int = Query(default=1, description="Number of top chunks to retrieve")
) -> list[Chunk]:
    """
    Query the store for a phrase.
    """
    return await vector_store.aretrieve_top_k_chunks(text=query, k=k, search_type=search_type.value, select=["id", "text", "metadata"], vector_field="text_vector_3072")

@kb_vector_apis_router.get("/metadata")
async def get_metadata() -> VectorStoreMetadata:
    """
    Get metadata properties of the store.
    """
    return await vector_store.get_metadata()
