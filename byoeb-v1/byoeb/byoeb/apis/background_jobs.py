import asyncio
from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Any, Awaitable, Callable, Dict, List, Optional
from byoeb.application_logger.azure_app_insights import AppInsightsLogHandler

from fastapi import APIRouter, Path, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from apscheduler.triggers.cron import BaseTrigger, CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR

from byoeb.chat_app.configuration.dependency_setup import scheduler

REGISTER_API_NAME = 'background_api'
TIMEZONE = ZoneInfo("Asia/Kolkata")

background_apis_router = APIRouter(tags=["Background Jobs"])
_logger = AppInsightsLogHandler.getLogger(REGISTER_API_NAME)

# ---------------------------------------------------------
# Job Configuration
# ---------------------------------------------------------
from byoeb.background_jobs.consensus.respond_with_consensus import main as respond_with_consensus
from byoeb.background_jobs.consensus.send_query_to_expert import main as send_query_to_expert
from byoeb.background_jobs.message_leaderboard.leaderboard import main as message_leaderboard
from byoeb.background_jobs.did_you_know.send_dyk import run as send_dyk

@dataclass
class JobInfo:
    id: str
    name: str
    trigger: BaseTrigger
    function: Callable[[], Any | Awaitable[Any]]
    enabled: bool


class JobStatus(BaseModel):
    id: str = Field(..., description="Unique identifier for the job", examples=["consensus_responder"])
    name: str = Field(..., description="Human readable job name", examples=["Respond with Consensus"])
    trigger: str = Field(..., description="APScheduler trigger expression", examples=[str(CronTrigger.from_crontab("*/30 * * * *", timezone=TIMEZONE))])
    enabled: bool = Field(..., description="Whether the job is enabled for scheduling")
    next_run: Optional[datetime] = Field(default=None, description="ISO timestamp for the next scheduled run, if any")
    error: Optional[str] = Field(default=None, description="Error message from the last run, if it failed", examples=["Timeout while sending messages", None])


JOB_CONFIGURATIONS = [
    JobInfo(
        id="consensus_responder",
        name="Respond with Consensus",
        trigger=CronTrigger.from_crontab("*/30 * * * *", timezone=TIMEZONE),  # Every 30 minutes
        function=respond_with_consensus,
        enabled=True
    ),
    JobInfo(
        id="expert_query_sender",
        name="Send Query to Expert",
        trigger=CronTrigger.from_crontab("0 8-20 * * *", timezone=TIMEZONE),  # Every hour from 8 AM to 8 PM
        function=send_query_to_expert,
        enabled=True
    ),
    JobInfo(
        id="message_leaderboard",
        name="Message Leaderboard",
        trigger=CronTrigger.from_crontab("0 12 * * FRI", timezone=TIMEZONE),   # 12 PM every Friday
        function=message_leaderboard,
        enabled=True
    ),
    JobInfo(
        id="send_dyk",
        name="Send DYK",
        trigger=IntervalTrigger(weeks=2, start_date=datetime(2025, 11, 5, 11, 0, tzinfo=TIMEZONE)),  # Biweekly 11 AM Wednesday
        function=send_dyk,
        enabled=True
    )
]

# ---------------------------------------------------------
# Job Management Utilities
# ---------------------------------------------------------
job_errors: Dict[str, Optional[str]] = {job.id: None for job in JOB_CONFIGURATIONS}

def job_listener(event):
    """Handle job execution events"""
    if event.exception:
        _logger.error(f"Job {event.job_id} failed: {event.exception}", extra={AppInsightsLogHandler.DETAILS: {
            "context": job_listener.__name__,
            "job_id": event.job_id
        }})
        job_errors[event.job_id] = str(event.exception)
    else:
        _logger.info(f"Job {event.job_id} executed successfully", extra={AppInsightsLogHandler.DETAILS: {
            "context": job_listener.__name__,
            "job_id": event.job_id
        }})
        job_errors[event.job_id] = None

# Add event listeners
scheduler.add_listener(job_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)

async def execute_job_function(job_function):
    """Execute a job function directly"""
    try:
        if asyncio.iscoroutinefunction(job_function):
            await job_function()
        else:
            job_function()
        _logger.info(f"Successfully executed job function {job_function.__name__}")
    except Exception as e:
        _logger.error(f"Failed to execute job function {job_function.__name__}: {str(e)}")
        raise

def setup_scheduled_jobs():
    """Setup all scheduled jobs"""
    for job_config in JOB_CONFIGURATIONS:
        if not job_config.enabled:
            continue

        scheduler.add_job(
            execute_job_function,
            job_config.trigger,
            args=[job_config.function],
            id=job_config.id,
            name=job_config.name,
            replace_existing=True,
            misfire_grace_time=60
        )
        _logger.info(f"Added job: {job_config.name} with schedule: {job_config.trigger}", extra={AppInsightsLogHandler.DETAILS: {
            "context": setup_scheduled_jobs.__name__,
            "job_id": job_config.id
        }})


# ---------------------------------------------------------
# API Endpoints
# ---------------------------------------------------------
@background_apis_router.post("/schedule", deprecated=True)
async def __schedule() -> JSONResponse:
    return JSONResponse(status_code=status.HTTP_200_OK, content={"detail": "This endpoint has been deprecated, use POST /jobs/{job_id} instead."})

@background_apis_router.post("/jobs/{job_id}", summary="Run a background job manually")
async def run_job_manually(job_id: str = Path(..., description="Job ID to trigger")) -> JSONResponse:
    """
    Manually triggers a background job by its job_id.
    """
    job_config = next((j for j in JOB_CONFIGURATIONS if j.id == job_id), None)
    if not job_config:
        return JSONResponse(content=f"Job '{job_id}' not found", status_code=status.HTTP_404_NOT_FOUND)

    await execute_job_function(job_config.function)
    return JSONResponse(content=f"Job '{job_id}' executed successfully", status_code=status.HTTP_200_OK)

@background_apis_router.get("/jobs", summary="List configured jobs and schedules")
async def list_jobs() -> List[JobStatus]:
    """
    Returns a list of all configured jobs with status, schedule, and next run info.
    """
    return [JobStatus(
        id=job.id,
        name=job.name,
        trigger=str(job.trigger),
        enabled=job.enabled,
        next_run=scheduler.get_job(job.id).next_run_time,
        error=job_errors[job.id]
    ) for job in JOB_CONFIGURATIONS]
