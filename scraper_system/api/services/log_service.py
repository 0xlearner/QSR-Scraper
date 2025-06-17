import os
from typing import AsyncGenerator
import asyncio
from datetime import datetime

LOG_FILE = "logs/scraper_worker.log"


async def get_log_stream() -> AsyncGenerator[str, None]:
    """
    Asynchronous generator that yields new log entries as they appear
    """
    if not os.path.exists(LOG_FILE):
        yield "Log file not found"
        return

    try:
        # Open file in read mode and seek to end
        with open(LOG_FILE, "r", encoding="utf-8") as f:
            # First, yield existing content
            content = f.read()
            if content:
                yield content

            # Seek to end for monitoring new content
            f.seek(0, 2)

            while True:
                line = f.readline()
                if not line:
                    await asyncio.sleep(1)
                    continue
                yield line
    except Exception as e:
        yield f"Error reading log file: {str(e)}"
