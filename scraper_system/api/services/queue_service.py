import redis
from rq import Queue
from typing import Dict, Any
import asyncio
from scraper_system.core.orchestrator import Orchestrator
import logging

logger = logging.getLogger(__name__)

redis_conn = redis.Redis(host="localhost", port=6379, db=0)
queue = Queue(
    "scraper_jobs", connection=redis_conn, default_timeout=-1
)  # 30 minute timeout


def run_scraper(config: Dict[str, Any]):
    """Run the scraper system with the given configuration"""
    logger.info("Starting scraper job...")
    try:
        # Create event loop for async operation
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        # Initialize orchestrator
        orchestrator = Orchestrator(config)

        try:
            # Run the orchestrator
            logger.info("Running orchestrator...")
            loop.run_until_complete(orchestrator.run())

            # Cleanup resources
            logger.info("Cleaning up resources...")
            loop.run_until_complete(orchestrator.cleanup())

            logger.info("Scraper job completed successfully")
            return {"status": "completed", "success": True}

        except Exception as e:
            logger.error(f"Error during scraping: {str(e)}", exc_info=True)
            return {"status": "failed", "error": str(e)}

        finally:
            # Always attempt to cleanup
            try:
                if not loop.is_closed():
                    loop.run_until_complete(orchestrator.cleanup())
            except Exception as e:
                logger.error(f"Error during cleanup: {str(e)}")
            finally:
                loop.close()

    except Exception as e:
        logger.error(f"Critical error in scraper job: {str(e)}", exc_info=True)
        return {"status": "failed", "error": str(e)}
