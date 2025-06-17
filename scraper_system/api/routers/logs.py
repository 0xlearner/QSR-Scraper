from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sse_starlette.sse import EventSourceResponse
from ..services.log_service import get_log_stream

router = APIRouter()
templates = Jinja2Templates(directory="scraper_system/api/templates")


@router.get("/logs", response_class=HTMLResponse)
async def get_logs_page(request: Request):
    """
    Render the logs viewing page
    """
    return templates.TemplateResponse("logs.html", {"request": request})


@router.get("/logs/stream")
async def stream_logs():
    """
    Stream logs via Server-Sent Events
    """
    return EventSourceResponse(get_log_stream())
