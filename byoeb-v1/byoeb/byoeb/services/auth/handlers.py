from __future__ import annotations

import logging
from http import HTTPStatus

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastmcp.server.middleware.middleware import CallNext, Middleware, MiddlewareContext
from mcp import McpError
from mcp.types import ErrorData

from byoeb.services.auth.exceptions import AuthError

logger = logging.getLogger(__name__)


def register_auth_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(AuthError)
    async def handle_auth_error(_: Request, exc: AuthError) -> JSONResponse:
        headers = dict(exc.headers) if exc.headers else None
        return JSONResponse(status_code=exc.status_code, content=exc.payload(), headers=headers)


def _map_auth_error_to_mcp(exc: AuthError) -> McpError:
    status_code = int(exc.status_code)
    if status_code == HTTPStatus.BAD_REQUEST:
        code = -32602
    elif status_code == HTTPStatus.NOT_FOUND:
        code = -32001
    else:
        code = -32000
    message = f"{exc.error_code}: {exc.detail}"
    return McpError(ErrorData(code=code, message=message))


class AuthMcpErrorMiddleware(Middleware):
    async def on_message(self, context: MiddlewareContext, call_next: CallNext):
        try:
            return await call_next(context)
        except AuthError as exc:
            logger.info("MCP auth error: %s", exc.error_code)
            raise _map_auth_error_to_mcp(exc) from exc
