import logging
import json
import os
from typing import List, Dict, Any
import aiofiles # For async file operations
from scraper_system.interfaces.storage_interface import StorageInterface

logger = logging.getLogger(__name__)

class JSONStorage(StorageInterface):
    """Stores data as JSON Lines in a file."""

    async def save(self, data: List[Dict[str, Any]], config: Dict[str, Any]):
        """Saves data to a JSON Lines file asynchronously."""
        output_file = config.get("output_file", "output.jsonl") # Default filename
        output_dir = os.path.dirname(output_file)

        if output_dir:
             os.makedirs(output_dir, exist_ok=True) # Ensure directory exists

        if not data:
            logger.info("No data provided to save.")
            return

        try:
            async with aiofiles.open(output_file, mode='a', encoding='utf-8') as f: # Append mode
                for item in data:
                    await f.write(json.dumps(item, ensure_ascii=False) + '\n')
            logger.info(f"Successfully appended {len(data)} items to {output_file}")
        except IOError as e:
            logger.error(f"Error writing to JSON file {output_file}: {e}")
        except Exception as e:
            logger.error(f"Unexpected error saving to JSON file {output_file}: {e}")
