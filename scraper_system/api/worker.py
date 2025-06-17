import redis
from rq import Worker
import logging
import sys
import os
from logging.handlers import RotatingFileHandler

# Create logs directory if it doesn't exist
LOG_DIR = "logs"
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

# Set up logging
log_file_path = os.path.join(LOG_DIR, "scraper_worker.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        # Console handler
        logging.StreamHandler(sys.stdout),
        # File handler with rotation (10MB max size, keep 5 backup files)
        RotatingFileHandler(
            log_file_path,
            maxBytes=10 * 1024 * 1024,  # 10 MB
            backupCount=5,
            encoding="utf-8",
        ),
    ],
)

logger = logging.getLogger("scraper_worker")
logger.info(f"Logging to file: {log_file_path}")

redis_conn = redis.Redis(host="localhost", port=6379, db=0)

if __name__ == "__main__":
    try:
        logger.info("Starting scraper worker...")
        worker = Worker(queues=["scraper_jobs"], connection=redis_conn)
        logger.info(f"Listening for jobs on queue: scraper_jobs (timeout: 1800s)")
        worker.work()
    except Exception as e:
        logger.error(f"Worker failed: {str(e)}")
        sys.exit(1)
