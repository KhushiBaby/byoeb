import csv
import io
import os
from typing import AsyncIterator
from byoeb.models.experiment import QueryInput
from datetime import datetime
from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from byoeb.background_jobs.daily_logs.asha_logs import fetch_daily_logs
from byoeb.services.admin.message_process import process_message, clear_history

REGISTER_API_NAME = 'admin_apis'

admin_apis_router = APIRouter(tags=["Administrative"])

current_dir = os.path.dirname(os.path.abspath(__file__))
jobs_path = os.path.join(current_dir, '..', 'background_jobs')
jobs_path = os.path.normpath(jobs_path)
template_dir = os.path.join(current_dir, 'ui_templates')
templates = Jinja2Templates(directory=template_dir)
file_path = "asha_data.xlsx"

@admin_apis_router.get("/asha_logs")
async def form_get(request: Request) -> HTMLResponse:
    return templates.TemplateResponse("index.html", {"request": request})

@admin_apis_router.get("/experiment")
async def experiment_form_get(request: Request) -> HTMLResponse:
    return templates.TemplateResponse("admin.html", {"request": request})

@admin_apis_router.post("/asha_logs")
async def asha_logs(start: datetime = Form(...), end: datetime = Form(...)) -> StreamingResponse:
    start_unix = str(start.timestamp())
    end_unix = str(end.timestamp())

    async def stream_csv() -> AsyncIterator[bytes]:
        buffer = io.StringIO()
        writer = None

        async for row in fetch_daily_logs(start_unix, end_unix):
            if writer is None:
                writer = csv.DictWriter(buffer, fieldnames=row.keys())
                writer.writeheader()

            writer.writerow(row)

            chunk = buffer.getvalue()
            buffer.seek(0)
            buffer.truncate(0)

            yield chunk.encode("utf-8")

    return StreamingResponse(stream_csv(), media_type="text/csv", headers={
        "Content-Disposition": f"attachment; filename=asha-logs-{start.isoformat()}-{end.isoformat()}.csv"
    })

@admin_apis_router.post("/experiment")
async def query_handler(input: QueryInput) -> JSONResponse:
    output = await process_message(input)
    output_json = output.model_dump(mode="json")
    print("Output JSON:", output_json)
    return JSONResponse(content=output_json, status_code=200)

@admin_apis_router.post("/clear_history")
async def clear(request: Request) -> JSONResponse:
    data = await request.json()
    phone_number_id = data.get("phone_number_id")
    clear_history(phone_number_id)
    return JSONResponse(content={"status": "cleared"}, status_code=200)

@admin_apis_router.post("/purge_request_cache")
async def query_handler() -> int:
    from byoeb.chat_app.configuration.dependency_setup import byoeb_user_generate_response
    return byoeb_user_generate_response.embedding_cache.purge()
