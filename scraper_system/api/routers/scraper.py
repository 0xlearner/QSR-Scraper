from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
import yaml
import os
from ..services.queue_service import queue, run_scraper
import logging
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

CONFIG_PATH = "configs/config.yaml"

router = APIRouter()
templates = Jinja2Templates(directory="scraper_system/api/templates")


def process_env_vars(item):
    """Recursively process dictionary values to replace ${ENV_VAR} patterns with actual env values."""
    if isinstance(item, dict):
        return {k: process_env_vars(v) for k, v in item.items()}
    elif isinstance(item, list):
        return [process_env_vars(i) for i in item]
    elif isinstance(item, str) and item.startswith("${") and item.endswith("}"):
        # Extract the environment variable name
        env_var = item[2:-1]
        # Get the environment variable value, with optional default
        if ":" in env_var:
            env_var, default = env_var.split(":", 1)
            return os.environ.get(env_var, default)
        return os.environ.get(env_var, f"ENV_{env_var}_NOT_FOUND")
    return item


def load_config(path: str) -> dict:
    """Loads YAML configuration file and processes environment variables."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
            if config is None:
                logging.error(f"Configuration file {path} is empty or invalid.")
                return {}

            # Process the loaded config to replace env var placeholders
            config = process_env_vars(config)
            return config
    except FileNotFoundError:
        logging.error(f"Configuration file not found at {path}")
        return {}
    except yaml.YAMLError as e:
        logging.error(f"Error parsing YAML configuration file {path}: {e}")
        return {}
    except Exception as e:
        logging.error(f"Failed to load config from {path}: {e}")
        return {}


@router.post("/scrape")
async def start_scraper():
    """
    Endpoint to trigger the scraper system using configuration from config.yaml
    """
    # Load config using the same mechanism as main.py
    config = load_config(CONFIG_PATH)
    if not config:
        raise HTTPException(
            status_code=500, detail=f"Failed to load configuration from {CONFIG_PATH}"
        )

    # Log the start of job
    logging.info("Queuing new scraper job with config from %s", CONFIG_PATH)

    # Enqueue the scraping job
    job = queue.enqueue(run_scraper, config)

    return {
        "status": "accepted",
        "job_id": job.id,
        "message": f"Scraping job has been queued using config from {CONFIG_PATH}",
    }


@router.get("/status/{job_id}")
async def get_job_status(job_id: str):
    """
    Get the status of a running job
    """
    job = queue.fetch_job(job_id)
    if job is None:
        return {"status": "not_found"}

    return {
        "status": job.get_status(),
        "result": job.result if job.is_finished else None,
    }


@router.get("/dashboard", response_class=HTMLResponse)
async def get_dashboard(request: Request):
    """
    Render the scraper dashboard with a start button
    """
    return templates.TemplateResponse("dashboard.html", {"request": request})
