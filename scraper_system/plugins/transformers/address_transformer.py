import logging
import hashlib
import asyncio
from typing import List, Dict, Any, Optional
import aiohttp
from pydantic import ValidationError

from scraper_system.interfaces.transformer_interface import TransformerInterface
from scraper_system.models.location import TransformedLocation

logger = logging.getLogger(__name__)


class AddressTransformer(TransformerInterface):
    """
    Transforms raw scraped Guzman y Gomez data using Geoapify reverse geocoding API.
    Implements rate limiting of 5 requests per second.
    """

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.geoapify.com/v1/geocode/reverse"
        self.semaphore = asyncio.Semaphore(5)  # Limit to 5 concurrent requests
        self.last_request_time = 0
        self.request_interval = 0.2  # 200ms between requests (5 per second)

    async def _get_address_details(
        self, lat: str, lon: str
    ) -> Optional[Dict[str, Any]]:
        """
        Fetches and parses address details from Geoapify API with rate limiting.
        """
        if not lat or not lon:
            return None

        params = {"lat": lat, "lon": lon, "format": "json", "apiKey": self.api_key}

        # Rate limiting logic
        async with self.semaphore:
            current_time = asyncio.get_event_loop().time()
            time_since_last_request = current_time - self.last_request_time
            if time_since_last_request < self.request_interval:
                await asyncio.sleep(self.request_interval - time_since_last_request)

            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(self.base_url, params=params) as response:
                        self.last_request_time = asyncio.get_event_loop().time()

                        if response.status == 200:
                            data = await response.json()
                            if data.get("results") and len(data["results"]) > 0:
                                result = data["results"][0]

                                # Construct street address
                                street_parts = []
                                if result.get("housenumber"):
                                    street_parts.append(result["housenumber"])
                                if result.get("street"):
                                    street_parts.append(result["street"])
                                street_address = " ".join(street_parts)

                                # Check for shopping mall
                                shopping_centre_name = None
                                if result.get("category") == "commercial.shopping_mall":
                                    shopping_centre_name = result.get("name")

                                return {
                                    "street_address": street_address or None,
                                    "suburb": result.get("suburb"),
                                    "state": result.get("state_code"),
                                    "postcode": result.get("postcode"),
                                    "shopping_centre_name": shopping_centre_name,
                                }

                        logger.warning(
                            f"Failed to get address details from Geoapify. Status: {response.status}"
                        )
                        return None

            except Exception as e:
                logger.error(f"Error fetching address details from Geoapify: {e}")
                return None

    async def _get_address_by_text(self, address: str) -> Optional[Dict[str, Any]]:
        """
        Fetches and parses address details from Geoapify API using text search.
        """
        if not address:
            return None

        search_url = "https://api.geoapify.com/v1/geocode/search"
        params = {"text": address, "format": "json", "apiKey": self.api_key}

        # Rate limiting logic
        async with self.semaphore:
            current_time = asyncio.get_event_loop().time()
            time_since_last_request = current_time - self.last_request_time
            if time_since_last_request < self.request_interval:
                await asyncio.sleep(self.request_interval - time_since_last_request)

            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(search_url, params=params) as response:
                        self.last_request_time = asyncio.get_event_loop().time()

                        if response.status == 200:
                            data = await response.json()
                            if data.get("results") and len(data["results"]) > 0:
                                result = data["results"][0]

                                # Construct street address
                                street_parts = []
                                if result.get("housenumber"):
                                    street_parts.append(result["housenumber"])
                                if result.get("street"):
                                    street_parts.append(result["street"])
                                street_address = " ".join(street_parts)

                                # Check for shopping mall
                                shopping_centre_name = None
                                if result.get("category") == "commercial.shopping_mall":
                                    shopping_centre_name = result.get("name")

                                return {
                                    "street_address": street_address or None,
                                    "suburb": result.get("suburb"),
                                    "state": result.get("state_code"),
                                    "postcode": result.get("postcode"),
                                    "shopping_centre_name": shopping_centre_name,
                                }

                        logger.warning(
                            f"Failed to get address details from Geoapify text search. Status: {response.status}"
                        )
                        return None

            except Exception as e:
                logger.error(
                    f"Error fetching address details from Geoapify text search: {e}"
                )
                return None

    async def transform(
        self, data: List[Dict[str, Any]], config: Dict[str, Any], site_name: str
    ) -> List[Dict[str, Any]]:
        transformed_data = []
        tasks = []

        # Create tasks for all locations
        for item in data:
            tasks.append(self._transform_single_item(item, site_name))

        # Process all tasks with built-in rate limiting
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Filter out errors and None results
        for result in results:
            if isinstance(result, dict):  # Only add successful transformations
                transformed_data.append(result)
            elif isinstance(result, Exception):
                logger.error(f"Error during transformation: {result}")

        logger.info(f"Transformed {len(transformed_data)} locations successfully")
        return transformed_data

    async def _transform_single_item(
        self, item: Dict[str, Any], site_name: str
    ) -> Optional[Dict[str, Any]]:
        """Helper method to transform a single location item."""
        business_name = item.get("name", "Unknown Name")
        latitude = item.get("latitude")
        longitude = item.get("longitude")
        address = item.get("address")

        # Get address details from Geoapify
        address_details = None
        if latitude and longitude:
            # Use reverse geocoding if coordinates are available
            address_details = await self._get_address_details(latitude, longitude)
        elif address:
            # Use text search if only address is available
            address_details = await self._get_address_by_text(address)
        else:
            logger.warning(
                f"Missing both coordinates and address for location: {business_name}"
            )
            return None

        if not address_details:
            logger.warning(f"Could not get address details for {business_name}")
            return None

        try:
            # Create location object with Geoapify parsed address
            location = TransformedLocation(
                business_name=business_name,
                street_address=address_details["street_address"],
                suburb=address_details["suburb"],
                state=address_details["state"],
                postcode=address_details["postcode"],
                drive_thru=item.get("drive_thru", False),
                shopping_centre_name=address_details["shopping_centre_name"],
                source_url=item.get("source_url"),
                source=site_name,
                business_id=generate_business_id(
                    business_name,
                    f"{address_details['street_address']}, {address_details['suburb']} {address_details['state']} {address_details['postcode']}",
                ),
            )
            return location.model_dump()

        except ValidationError as e:
            logger.error(f"Validation failed for '{business_name}': {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error transforming '{business_name}': {e}")
            return None


def generate_business_id(name: str, address: str) -> str:
    """Generates a simple unique ID based on name and address."""
    data_string = f"{name.lower().strip()}|{address.lower().strip()}"
    return hashlib.sha1(data_string.encode("utf-8")).hexdigest()
