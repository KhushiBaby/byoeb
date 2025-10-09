import logging
import logging.config
import os
import asyncio
import uvicorn
import yaml
from fastapi import FastAPI
from fastmcp import FastMCP
from contextlib import asynccontextmanager
from byoeb.apis.health import health_apis_router, health_mcps_router
from byoeb.apis.channel_register import register_apis_router
from byoeb.apis.chat import chat_apis_router, chat_mcps_router
from byoeb.apis.user import user_apis_router, user_mcps_router
from byoeb.apis.background_jobs import background_apis_router
from byoeb.apis.admin import admin_apis_router

logger = logging.getLogger(__name__)
asyncio.get_event_loop().set_debug(True)
def create_apps():
    """
    Creates and configures a FastAPI application.

    Returns:
        Flask: A configured FastAPI application instance.
    """

    app = FastAPI(lifespan=lifespan)
    app.include_router(background_apis_router)
    app.include_router(health_apis_router)
    app.include_router(register_apis_router)
    app.include_router(chat_apis_router)
    app.include_router(user_apis_router)
    app.include_router(admin_apis_router)

    mcp = FastMCP()
    health_mcps_router(mcp)
    chat_mcps_router(mcp)
    user_mcps_router(mcp)
    mcp_app = mcp.http_app(path="/mcp")
    app.mount("/", mcp_app)
    return app, mcp_app

@asynccontextmanager
async def lifespan(app: FastAPI):
    pid = os.getpid()
    print(f"FastAPI app is running with PID: {pid}")
    from byoeb.chat_app.configuration.dependency_setup import (
        channel_client_factory, 
        message_consumer,
        queue_producer_factory,
        text_translator
    )
    await message_consumer.initialize()
    asyncio.create_task(message_consumer.listen())
    async with mcp_app.lifespan(app):
        yield
    await channel_client_factory.close()
    await message_consumer.close()
    await queue_producer_factory.close()
    await text_translator._close()
    logger.info("FastAPI app is shutting down. Closing all clients")

app, mcp_app = create_apps()

# Issue with multiple workers in FastAPI
# https://github.com/encode/uvicorn/discussions/2450
if __name__ == '__main__':
    module_name = "byoeb.chat_app.run"
    if os.getenv("APP_ENV") == "PROD":
        current_dir = os.path.dirname(os.path.abspath(__file__))
        log_config_path = os.path.join(current_dir, 'logging.yaml')
        log_config_path = os.path.normpath(log_config_path)
        log_config = None
        with open(log_config_path, 'r') as file:
            log_config = yaml.safe_load(file)
        logging.config.dictConfig(log_config)
        logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.CRITICAL)
        uvicorn.run(
            f"{module_name}:app",
            host="0.0.0.0",
            port=8000
        )
    else:
        print(module_name)
        uvicorn.run(
            f"{module_name}:app",
            host="127.0.0.1",
            port=5000
        )
