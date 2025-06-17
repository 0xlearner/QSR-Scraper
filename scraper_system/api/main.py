from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from .routers import scraper, logs

app = FastAPI(title="QSR Scraper API")

# Mount templates
templates = Jinja2Templates(directory="scraper_system/api/templates")

# Include routers
app.include_router(scraper.router, prefix="/api/v1", tags=["scraper"])
app.include_router(logs.router, prefix="/api/v1", tags=["logs"])


@app.get("/")
async def root():
    return {"message": "QSR Scraper System API"}
