import csv
import io
import logging
import os
import logging
from typing import AsyncIterator
from byoeb.models.experiment import QueryInput
from datetime import datetime
from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from byoeb.background_jobs.daily_logs.asha_logs import fetch_daily_logs
from byoeb.services.admin.message_process import process_message, clear_history as clear_user_history
from byoeb.services.auth.dependencies import require_csrf_token, require_permissions
from byoeb.services.auth.models import AuthPermission, AuthUser

REGISTER_API_NAME = 'admin_apis'

admin_apis_router = APIRouter(tags=["Administrative"])
admin_public_router = APIRouter(tags=["Administrative"])

logger = logging.getLogger(__name__)

current_dir = os.path.dirname(os.path.abspath(__file__))
template_dir = os.path.join(current_dir, 'ui_templates')
templates = Jinja2Templates(directory=template_dir)

@admin_public_router.get("/asha_logs", include_in_schema=False)
async def asha_logs(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(request, "index.html")

@admin_public_router.get("/login", include_in_schema=False)
async def login(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(request, "login.html")

@admin_public_router.get("/experiment", include_in_schema=False)
async def experiment(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(request, "experiment.html")

@admin_apis_router.post("/asha_logs")
async def asha_logs_form(
    start: datetime = Form(...),
    end: datetime = Form(...),
    _: AuthUser = Depends(require_permissions(AuthPermission.ADMIN_ACCESS)),
    __: None = Depends(require_csrf_token),
) -> StreamingResponse:
    start_unix = int(start.timestamp())
    end_unix = int(end.timestamp())

    async def stream_csv() -> AsyncIterator[bytes]:
        buffer = io.StringIO()
        writer = None
        chunk_rows = 0

        async for row in fetch_daily_logs(start_unix, end_unix):
            if writer is None:
                writer = csv.DictWriter(buffer, fieldnames=row.keys())
                writer.writeheader()

            writer.writerow(row)
            chunk_rows += 1

            # Flush every 100 rows to avoid holding everything in memory
            # while still reducing per-row HTTP chunk overhead.
            if chunk_rows >= 100:
                chunk = buffer.getvalue()
                buffer.seek(0)
                buffer.truncate(0)
                chunk_rows = 0
                yield chunk.encode("utf-8")

        # Flush remaining rows
        remaining = buffer.getvalue()
        if remaining:
            yield remaining.encode("utf-8")

    return StreamingResponse(stream_csv(), media_type="text/csv", headers={
        "Content-Disposition": f"attachment; filename=asha-logs-{start.isoformat()}-{end.isoformat()}.csv"
    })

@admin_apis_router.post("/experiment")
async def experiment_form(input: QueryInput) -> JSONResponse:
    output = await process_message(input)
    output_json = output.model_dump(mode="json")
    logging.getLogger(__name__).debug("Output JSON: %s", output_json)
    return JSONResponse(content=output_json, status_code=200)

@admin_apis_router.post("/clear_history")
async def clear_history(request: Request) -> JSONResponse:
    data = await request.json()
    phone_number_id = data.get("phone_number_id")
    clear_user_history(phone_number_id)
    return JSONResponse(content={"status": "cleared"}, status_code=200)

@admin_apis_router.post("/purge_request_cache")
async def purge_request_cache() -> int:
    from byoeb.chat_app.configuration.dependency_setup import byoeb_user_generate_response
    return byoeb_user_generate_response.embedding_cache.purge()
