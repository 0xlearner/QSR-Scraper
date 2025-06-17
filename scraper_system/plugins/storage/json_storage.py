import logging
import json
import os
from typing import List, Dict, Any
import aiofiles  # For async file operations
from datetime import datetime
from scraper_system.interfaces.storage_interface import StorageInterface

logger = logging.getLogger(__name__)


# Custom JSON encoder to handle datetime objects
def json_serializer(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, datetime):
        return obj.isoformat()  # Convert datetime to ISO 8601 string format
    raise TypeError(f"Type {type(obj)} not serializable")


class JSONStorage(StorageInterface):
    """Stores data as JSON Lines in a file."""

    async def save(self, data: List[Dict[str, Any]], config: Dict[str, Any]):
        """Saves data to a JSON Lines file asynchronously, handling datetime."""
        output_file = config.get("output_file", "output.jsonl")
        output_dir = os.path.dirname(output_file)

        if output_dir:
            # Use os.makedirs to handle potential nested directories
            os.makedirs(output_dir, exist_ok=True)

        if not data:
            logger.info("No data provided to save.")
            return

        try:
            async with aiofiles.open(output_file, mode="a", encoding="utf-8") as f:
                for item in data:
                    # Use the default parameter in json.dumps to specify our custom serializer
                    json_string = json.dumps(
                        item, ensure_ascii=False, default=json_serializer
                    )
                    await f.write(json_string + "\n")
            logger.info(f"Successfully appended {len(data)} items to {output_file}")
        except TypeError as e:
            # Catch potential new serialization errors
            logger.error(
                f"JSON serialization error saving to {output_file}: {e} - Check item structure.",
                exc_info=True,
            )
        except IOError as e:
            logger.error(f"Error writing to JSON file {output_file}: {e}")
        except Exception as e:
            logger.error(
                f"Unexpected error saving to JSON file {output_file}: {e}",
                exc_info=True,
            )
