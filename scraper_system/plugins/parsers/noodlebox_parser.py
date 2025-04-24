import logging
import json
from typing import List, Dict, Any, Optional, Tuple

from scraper_system.interfaces.parser_interface import ParserInterface
from scraper_system.interfaces.fetcher_interface import (
    FetcherInterface,
)

logger = logging.getLogger(__name__)


class NoodleboxParser(ParserInterface):
    """
    Parses Noodle Box locations by fetching data directly from their internal API endpoint
    using the injected FetcherInterface configured for a POST request. API details are
    expected in the site configuration passed to the parse method.
    """

    def __init__(self, fetcher: Optional[FetcherInterface] = None):
        if fetcher is None:
            raise ValueError(f"{self.__class__.__name__} requires a Fetcher instance.")
        self.fetcher = fetcher
        logger.info(f"{self.__class__.__name__} initialized with fetcher.")

    def _prepare_fetcher_config(self, global_config: Dict[str, Any]) -> Dict[str, Any]:
        """Prepares the configuration dictionary for the fetcher call using API settings from global_config."""
        api_settings = global_config.get("api_settings", {})
        api_headers = api_settings.get("headers", {})
        api_payload = api_settings.get("payload", {})

        global_fetcher_options = global_config.get("fetcher_options", {})
        # Merge global headers, then API-specific headers (API headers take precedence)
        merged_headers = {**global_fetcher_options.get("headers", {}), **api_headers}

        return {
            **global_fetcher_options,
            "method": "POST",
            "headers": merged_headers,
            "json_payload": api_payload,
        }

    async def _fetch_and_decode_data(
        self, api_url: str, fetcher_config: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Fetches data using the fetcher and decodes the JSON response."""
        if not api_url:
            logger.error("API URL is missing, cannot fetch data.")
            return None

        response_content, _, status_code = await self.fetcher.fetch(
            api_url, fetcher_config
        )

        if response_content is None:
            logger.error(
                f"Fetcher failed to retrieve data from Noodle Box API (Status: {status_code})"
            )
            return None

        try:
            return json.loads(response_content)
        except json.JSONDecodeError as e:
            logger.error(
                f"Failed to decode JSON response from Noodle Box API (URL: {api_url}, Status: {status_code}) via fetcher: {e}. Content snippet: {response_content[:500]}"
            )
            return None

    def _extract_location_list(
        self, raw_data: Optional[Dict[str, Any]]
    ) -> Optional[List[Dict[str, Any]]]:
        """Extracts and validates the list of locations from the raw API response."""
        if not isinstance(raw_data, dict) or "data" not in raw_data:
            logger.error(
                f"Noodle Box API response is not a dictionary or missing 'data' key. Found type: {type(raw_data)}. Data: {str(raw_data)[:500]}"
            )
            return None

        location_list = raw_data.get("data")

        if not isinstance(location_list, list):
            logger.error(
                f"The 'data' key in Noodle Box API response is not a list. Found type: {type(location_list)}. Data: {str(location_list)[:500]}"
            )
            return None

        logger.info(
            f"Received {len(location_list)} raw location entries from Noodle Box API via fetcher."
        )
        return location_list

    def _parse_latlng(
        self, latlng_str: Optional[str], name: str
    ) -> Tuple[Optional[str], Optional[str]]:
        """Parses the latitude/longitude string."""
        latitude, longitude = None, None
        if not latlng_str or "," not in latlng_str:
            logger.warning(
                f"Skipping lat/lng parsing for '{name}' due to missing or invalid string: {latlng_str}"
            )
            return latitude, longitude

        try:
            lat_str, lon_str = latlng_str.split(",", 1)
            latitude = lat_str.strip()
            longitude = lon_str.strip()
        except ValueError:
            logger.warning(f"Could not split latlng string for '{name}': {latlng_str}")

        return latitude, longitude

    def _parse_address(
        self, address_parts: Optional[List[str]], name: str
    ) -> Optional[str]:
        """Parses the address list into a single string."""
        if not isinstance(address_parts, list) or not address_parts:
            logger.warning(
                f"Cannot parse address for '{name}' due to missing or invalid address list: {address_parts}"
            )
            return None
        return ", ".join(
            part.strip() for part in address_parts if part and part.strip()
        )

    def _parse_single_location(self, item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Parses a single location dictionary from the API response."""
        if not isinstance(item, dict):
            logger.warning(
                f"Skipping non-dictionary item in Noodle Box response data list: {item}"
            )
            return None

        try:
            name = item.get("name")
            if not name:
                logger.warning(f"Skipping Noodle Box item due to missing name: {item}")
                return None
            name = name.strip()  # Clean name early

            address = self._parse_address(item.get("address"), name)
            if not address:
                return None  # Error logged in helper

            latitude, longitude = self._parse_latlng(item.get("latlng"), name)
            # Continue even if lat/lng parsing fails

            return {
                "name": name,
                "address": address,
                "latitude": latitude,
                "longitude": longitude,
                "drive_thru": False,  # Assuming no drive-thru info available
                "source_url": "https://www.noodlebox.com.au/locations",  # Static source page URL
            }
        except Exception as e:
            # Catch errors specific to processing this single item
            logger.error(
                f"Error processing individual Noodle Box item: {item}. Error: {e}",
                exc_info=True,
            )
            return None

    async def parse(
        self,
        content: Optional[str],
        content_type: Optional[str],
        config: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        """
        Fetches and parses location data from the Noodle Box API using the injected fetcher.
        API details (URL, headers, payload) are read from the 'config' dictionary.
        Orchestrates fetching, decoding, and parsing using helper methods.
        """
        results = []
        api_settings = config.get("api_settings")
        if not api_settings:
            logger.error(
                f"{self.__class__.__name__} requires 'api_settings' in the configuration."
            )
            return []

        api_url = api_settings.get("url")
        if not api_url:
            logger.error(
                f"{self.__class__.__name__} requires 'url' within 'api_settings' in the configuration."
            )
            return []

        logger.info(
            f"{self.__class__.__name__} starting parse. Requesting data from {api_url} via fetcher."
        )

        try:
            # Prepare fetcher config using the full site config (which includes api_settings)
            fetcher_config = self._prepare_fetcher_config(config)
            # Fetch data using the specific API URL from config
            raw_data = await self._fetch_and_decode_data(api_url, fetcher_config)
            location_list = self._extract_location_list(raw_data)

            if location_list is None:
                # Error logged in helper methods
                return []  # Return empty list if fetch/decode/extract fails

            # Process each valid location item
            for item in location_list:
                parsed_location = self._parse_single_location(item)
                if parsed_location:
                    results.append(parsed_location)

        except Exception as e:
            # Catch unexpected errors during the orchestration process
            logger.error(
                f"Unexpected error in {self.__class__.__name__}.parse orchestration: {e}",
                exc_info=True,
            )

        logger.info(
            f"{self.__class__.__name__} finished, returning {len(results)} parsed items."
        )
        return results
