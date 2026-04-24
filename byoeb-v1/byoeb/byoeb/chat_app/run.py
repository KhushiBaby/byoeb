# fixes crash during 'import chromadb' - see: https://docs.trychroma.com/docs/overview/troubleshooting#sqlite
import sys
import importlib.util
if importlib.util.find_spec("pysqlite3") is not None:
    __import__("pysqlite3")
    sys.modules["sqlite3"] = sys.modules.pop("pysqlite3")

import logging
import logging.config
import os
import tempfile
import asyncio
import uvicorn
import yaml
from fastapi import Depends, FastAPI
from fastmcp import FastMCP
from contextlib import asynccontextmanager
from byoeb.apis.auth import auth_apis_router
from byoeb.services.auth.dependencies import get_public_base_url, require_csrf_token, require_permissions
from byoeb.services.auth.models import AuthPermission
from byoeb.services.auth.oauth_provider import OAuthProvider
from byoeb.services.auth.handlers import AuthMcpErrorMiddleware, register_auth_exception_handlers
from byoeb.apis.health import health_apis_router, health_mcps_router
from byoeb.apis.channel_register import register_apis_router
from byoeb.apis.chat import chat_apis_router, chat_mcps_router
from byoeb.apis.user import user_apis_router, user_mcps_router
from byoeb.apis.background_jobs import background_apis_router, background_apis_router_deprecated
from byoeb.apis.admin import admin_apis_router, admin_public_router
from byoeb.chat_app.configuration.config import env_app

logger = logging.getLogger(__name__)

# Configure logging early - this ensures it works when uvicorn imports the module
def _setup_logging():
    """Setup logging configuration"""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    log_config_path = os.path.join(current_dir, 'logging.yaml')
    log_config_path = os.path.normpath(log_config_path)
    
    # Try to load logging.yaml, fallback to basicConfig if not available
    if os.path.exists(log_config_path):
        try:
            with open(log_config_path, 'r') as file:
                log_config = yaml.safe_load(file)
                logging.config.dictConfig(log_config)
        except Exception as e:
            logger.warning("Failed to load logging.yaml: %s. Using basicConfig.", e)
            logging.basicConfig(
                level=logging.INFO,
                format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S',
            )
    else:
        # Only setup basicConfig if no root handler exists
        if not logging.root.handlers:
            logging.basicConfig(
                level=logging.INFO,
                format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S',
            )
    
    # Ensure specific loggers are set to INFO level for visibility
    logging.getLogger("byoeb.listener.message_consumer").setLevel(logging.INFO)
    logging.getLogger("byoeb.services.chat.message_consumer").setLevel(logging.INFO)
    logging.getLogger("byoeb.services.chat").setLevel(logging.INFO)
    # Reduce httpx logging noise (only show warnings/errors)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("azure.monitor.opentelemetry").setLevel(logging.WARNING)
    logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.CRITICAL)

# Setup logging at module import time
_setup_logging()

try:
    asyncio.get_event_loop().set_debug(True)
except RuntimeError:
    pass

def create_apps(is_prod: bool):
    """
    Creates and configures a FastAPI application.

    Returns:
        Flask: A configured FastAPI application instance.
    """

    kwargs = {}
    if is_prod:
        # Swagger UI (/docs), ReDoc (/redoc), and OpenAPI schema (/openapi.json) are
        # intentionally disabled in production (APP_ENV=PROD) to reduce attack surface.
        # Use a non-production environment or set APP_ENV != PROD to access API docs.
        kwargs["docs_url"] = None
        kwargs["redoc_url"] = None
        kwargs["openapi_url"] = None
    app = FastAPI(lifespan=lifespan, **kwargs)

    register_auth_exception_handlers(app)
    app.include_router(auth_apis_router)
    app.include_router(admin_public_router)
    app.include_router(admin_apis_router, dependencies=[Depends(require_permissions(AuthPermission.ADMIN_ACCESS)), Depends(require_csrf_token)])
    app.include_router(background_apis_router, dependencies=[Depends(require_permissions(AuthPermission.JOBS_RUN)), Depends(require_csrf_token)])
    app.include_router(background_apis_router_deprecated)
    app.include_router(chat_apis_router)
    app.include_router(user_apis_router, dependencies=[Depends(require_permissions(AuthPermission.USERS_MANAGE)), Depends(require_csrf_token)])
    app.include_router(register_apis_router)
    app.include_router(health_apis_router)

    auth = OAuthProvider(base_url=get_public_base_url(), scopes_required=[], scopes_default=[])
    app.router.routes.extend(auth.get_well_known_routes())
    app.router.routes.extend(auth.create_resource_routes("/mcp", [AuthPermission.MCP_ACCESS]))

    mcp = FastMCP(auth=auth, middleware=[AuthMcpErrorMiddleware()])
    health_mcps_router(mcp)
    chat_mcps_router(mcp)
    user_mcps_router(mcp)
    mcp_app = mcp.http_app(path="/mcp", stateless_http=True)
    app.mount("/", mcp_app)
    return app, mcp_app

@asynccontextmanager
async def lifespan(app: FastAPI):
    with tempfile.TemporaryDirectory(prefix="ashabot-") as tempdir:
        pid = os.getpid()
        logger.info("FastAPI app is running with PID: %s", pid)

        from byoeb.chat_app.configuration import config
        config.app_tempdir.set_result(tempdir)

        from byoeb.chat_app.configuration.dependency_setup import (
            channel_client_factory,
            message_consumer,
            queue_producer_factory,
            text_translator
        )
        from byoeb.apis.background_jobs import setup_scheduled_jobs
        from byoeb.chat_app.configuration.dependency_setup import start_scheduler, stop_scheduler

        try:
            await message_consumer.initialize()
            asyncio.create_task(message_consumer.listen())
        except Exception as e:
            logger.error(f"Failed to initialize message consumer: {e}")
            logger.warning("Application will continue without message queue consumer")
            import traceback
            logger.error(traceback.format_exc())

        setup_scheduled_jobs()
        start_scheduler()
        logger.info("Background job scheduler started during application startup")

        async with mcp_app.lifespan(app):
            yield

        stop_scheduler()
        logger.info("Background job scheduler stopped during application shutdown")

        await channel_client_factory.close()
        await message_consumer.close()
        await queue_producer_factory.close()
        await text_translator._close()
        logger.info("FastAPI app is shutting down. Closing all clients")

app, mcp_app = create_apps(env_app == "PROD")

# Issue with multiple workers in FastAPI
# https://github.com/encode/uvicorn/discussions/2450
if __name__ == '__main__':
    uvicorn.run(
        "byoeb.chat_app.run:app",
        host="0.0.0.0",
        port=8000,
        ws="websockets-sansio"
    )
