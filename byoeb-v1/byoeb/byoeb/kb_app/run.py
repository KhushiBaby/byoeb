import logging
import asyncio
import uvicorn
from fastapi import FastAPI
from byoeb.apis.knowledge_base import kb_media_apis_router, kb_vector_apis_router

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Set specific loggers to INFO level
logging.getLogger("kb_api").setLevel(logging.INFO)
logging.getLogger("kb_service").setLevel(logging.INFO)
logging.getLogger("byoeb_core.data_parser.llama_index_text_parser").setLevel(logging.INFO)
logging.getLogger("byoeb_integrations.vector_stores.chroma.base").setLevel(logging.INFO)

# Reduce httpx logging noise (only show warnings/errors)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.WARNING)

# Set event loop debug mode (only if event loop exists)
try:
    loop = asyncio.get_running_loop()
    loop.set_debug(True)
except RuntimeError:
    # No event loop running, create one for debugging
    import sys
    if sys.platform != 'win32':
        asyncio.set_event_loop_policy(asyncio.DefaultEventLoopPolicy())
    loop = asyncio.new_event_loop()
    loop.set_debug(True)
    asyncio.set_event_loop(loop)
def create_app():
    """
    Creates and configures a FastAPI application.

    Returns:
        Flask: A configured FastAPI application instance.
    """

    app = FastAPI()
    app.include_router(kb_media_apis_router)
    app.include_router(kb_vector_apis_router)
    return app

app = create_app()
if __name__ == '__main__':
    uvicorn.run(
        app,
        host="127.0.0.1",
        port=5000
    )