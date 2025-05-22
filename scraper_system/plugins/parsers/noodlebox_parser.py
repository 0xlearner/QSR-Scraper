import json
import logging
from typing import List, Dict, Any, Optional

from scraper_system.interfaces.parser_interface import ParserInterface
from scraper_system.interfaces.fetcher_interface import FetcherInterface

logger = logging.getLogger(__name__)


class NoodleboxParser(ParserInterface):
    """
    Parses Noodlebox locations from their API.
    Focuses only on scraping the raw data, leaving transformation to the transformer.
    """

    def __init__(self, fetcher: Optional[FetcherInterface] = None):
        self.fetcher = fetcher
        logger.info("NoodleboxParser initialized")

    async def parse(
        self, content: str, content_type: Optional[str], config: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Main parsing method that fetches Noodlebox locations and returns raw data
        """
        json_data = {}

        # If we have existing content, try to parse it
        if content:
            try:
                json_data = json.loads(content)
                logger.info("Using provided JSON content")
            except json.JSONDecodeError:
                logger.warning("Provided content is not valid JSON")

        # If we don't have content, or it's not valid JSON, fetch it
        if not json_data and self.fetcher:
            json_data = await self._fetch_noodlebox_data(config)

        if not json_data or 'data' not in json_data:
            logger.error("Failed to get Noodlebox location data")
            return []

        # Process the data - just extract basic info for the transformer
        locations = []
        for location in json_data.get("data", []):
            business_name = location.get("name", "").strip()
            address_list = location.get("address", [])
            
            # Join all address components for full address
            raw_address = " ".join(address_list) if address_list else ""
            
            # Create a simple dictionary with the basic info
            location_data = {
                "business_name": "Noodlebox " + business_name,
                "raw_address": raw_address,
                "drive_thru": False,  # Assuming no drive-thru unless specified
                "source_url": "https://www.noodlebox.com.au/locations",
                "source": "noodlebox"
            }
            
            locations.append(location_data)

        logger.info(f"NoodleboxParser finished, returning {len(locations)} items.")
        return locations

    async def _fetch_noodlebox_data(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Fetch data from Noodlebox API
        """
        url = "https://www.noodlebox.com.au/data/locations"

        # Set headers to mimic a browser visit
        headers = {
            "accept": "application/json, text/javascript, */*; q=0.01",
            "accept-language": "en-US,en;q=0.8",
            "origin": "https://www.noodlebox.com.au",
            "priority": "u=1, i",
            "referer": "https://www.noodlebox.com.au/locations",
            "sec-ch-ua": '"Brave";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Linux"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "sec-gpc": "1",
            "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
            "x-requested-with": "XMLHttpRequest",
        }

        fetcher_config = config.get("fetcher_options", {})
        if not fetcher_config.get("headers"):
            fetcher_config["headers"] = headers

        try:
            # Fetch the content
            content, _, status_code = await self.fetcher.fetch(url, fetcher_config)

            if not content:
                logger.error(f"Failed to fetch Noodlebox locations (Status: {status_code})")
                return {}

            # Parse the JSON content
            if isinstance(content, str):
                json_data = json.loads(content)
                logger.info(f"Successfully fetched Noodlebox location data with {len(json_data.get('data', []))} locations")
                return json_data
            else:
                logger.error("Unexpected response type from fetcher")
                return {}

        except Exception as e:
            logger.error(f"Error fetching Noodlebox location data: {e}", exc_info=True)
            return {}