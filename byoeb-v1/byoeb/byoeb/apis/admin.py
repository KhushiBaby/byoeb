import os
from byoeb.models.experiment import QueryInput
from io import BytesIO
from datetime import datetime
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from fastapi import Form, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from fastapi.responses import StreamingResponse
from byoeb.background_jobs.daily_logs.asha_logs import fetch_daily_logs
from byoeb.services.admin.message_process import process_message, clear_history
from byoeb.chat_app.configuration.dependency_setup import media_storage

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
async def form_post(request: Request, start_datetime: str = Form(...), end_datetime: str = Form(...)) -> HTMLResponse:
    start = datetime.strptime(start_datetime, "%Y-%m-%dT%H:%M")
    end = datetime.strptime(end_datetime, "%Y-%m-%dT%H:%M")
    
    start_unix = str(start.timestamp())
    end_unix = str(end.timestamp())
    ashas_df = await fetch_daily_logs(
        start_timestamp=start_unix,
        end_timestamp=end_unix
    )
    
    # Save to excel for download
    ashas_df.to_excel(file_path, index=False)
    blob_file_name = f"logs/{os.path.basename(file_path)}"
    # Check if file exists before deleting using aget_file_properties
    status, _ = await media_storage.aget_file_properties(file_name=blob_file_name)

    # Only delete if file exists (status 200 means file exists)
    if status == 200:
        await media_storage.adelete_file(file_name=blob_file_name)
        print(f"Deleted existing file: {blob_file_name}")
    else:
        print(f"File {blob_file_name} does not exist (status: {status}), skipping delete")
    await media_storage.aupload_file(
        file_path=file_path,
        file_name=blob_file_name
    )
    # await media_storage._close()
    # Render HTML
    df_html = ashas_df.to_html(classes="table table-bordered", index=False)
    return templates.TemplateResponse("index.html", {
        "request": request,
        "table": df_html,
        "show_download": True
    })

@admin_apis_router.get("/download")
async def download_excel():
    _, asha_data = await media_storage.adownload_file(
        file_name=f"logs/{os.path.basename(file_path)}"
    )
    # await media_storage._close()
    stream = BytesIO(asha_data.data)  # or just use Filedata if already BytesIO
    return StreamingResponse(
        stream,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": "attachment; filename=downloaded.xlsx"
        }
    )
    # return FileResponse(
    #     path=file_path,
    #     media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    #     filename="data.xlsx",
    # )

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