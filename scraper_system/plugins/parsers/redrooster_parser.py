import json
import logging
from typing import List, Dict, Any, Optional

from scraper_system.interfaces.parser_interface import ParserInterface
from scraper_system.interfaces.fetcher_interface import FetcherInterface

logger = logging.getLogger(__name__)


class RedRoosterParser(ParserInterface):
    """
    Parses Red Rooster locations from their API endpoint.
    Fetches store data from https://content-acl.redrooster.com.au/all_stores.json
    and extracts store information including name and address components.
    """

    def __init__(self, fetcher: Optional[FetcherInterface] = None):
        if fetcher is None:
            raise ValueError("RedRoosterParser requires a Fetcher instance.")
        self.fetcher = fetcher
        logger.info("RedRoosterParser initialized with a fetcher.")

    async def parse(
        self, content: str, content_type: Optional[str], config: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Main parsing method that fetches Red Rooster store data from their API.
        
        Args:
            content: Initial content (ignored, we fetch from API directly)
            content_type: Content type (ignored)
            config: Parser configuration
            
        Returns:
            List of store dictionaries with raw data
        """
        logger.info("RedRoosterParser starting parse...")
        
        # API endpoint URL
        api_url = "https://content-acl.redrooster.com.au/all_stores.json"
        
        # Get fetcher configuration
        fetcher_config = config.get("fetcher_options", {})
        
        # Set default headers if not provided
        if not fetcher_config.get("headers"):
            fetcher_config["headers"] = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "keep-alive",
                "Sec-Fetch-Dest": "empty",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Site": "same-origin"
            }
        
        try:
            # Fetch the API data
            logger.info(f"Fetching Red Rooster stores from: {api_url}")
            content, content_type, status_code = await self.fetcher.fetch(api_url, fetcher_config)
            
            if not content:
                logger.error(f"Failed to fetch Red Rooster API data (Status: {status_code})")
                return []
            
            # Parse JSON response
            try:
                api_data = json.loads(content)
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse JSON response: {e}")
                return []
            
            # Extract store data
            stores = self._extract_stores(api_data)
            
            logger.info(f"RedRoosterParser finished, returning {len(stores)} items.")
            return stores
            
        except Exception as e:
            logger.error(f"Error in RedRoosterParser: {e}", exc_info=True)
            return []

    def _extract_stores(self, api_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Extract store information from the API response.
        
        Args:
            api_data: Raw API response data
            
        Returns:
            List of store dictionaries
        """
        stores = []
        
        # The API returns data in a "data" array
        store_data = api_data.get("data", [])
        
        if not isinstance(store_data, list):
            logger.warning("API response 'data' field is not a list")
            return []
        
        for store_item in store_data:
            try:
                store_info = self._parse_store_item(store_item)
                if store_info:
                    stores.append(store_info)
            except Exception as e:
                logger.error(f"Error parsing store item: {e}", exc_info=True)
                continue
        
        return stores

    def _parse_store_item(self, store_item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Parse a single store item from the API response.
        
        Args:
            store_item: Single store data from API
            
        Returns:
            Parsed store dictionary or None if invalid
        """
        try:
            # Extract basic store attributes
            attributes = store_item.get("attributes", {})
            relationships = store_item.get("relationships", {})
            
            # Get store name
            store_name = attributes.get("storeName", "")
            if not store_name:
                logger.warning("Store missing name, skipping")
                return None
            
            # Extract address from relationships -> storeAddress -> data -> attributes -> addressComponents
            address_info = self._extract_address_info(relationships)
            
            # Check if store is enabled for trading (filter out closed stores)
            is_enabled = attributes.get("isEnabledForTrading", False)
            if not is_enabled:
                logger.debug(f"Skipping disabled store: {store_name}")
                return None
            
            # Determine drive-thru availability from collection pickup types
            drive_thru = self._determine_drive_thru(relationships)
            
            # Build the store info dictionary
            store_info = {
                "brand": "Red Rooster",
                "business_name": store_name,
                "street_address": address_info.get("street_name", ""),
                "suburb": address_info.get("suburb", ""),
                "state": address_info.get("state", ""),
                "postcode": address_info.get("postcode", ""),
                "drive_thru": drive_thru,
                "source": "redrooster",
                "source_url": "https://redrooster.com.au/",  # Generic URL since no specific store URLs
                "raw_data": store_item  # Keep original data for debugging
            }
            
            return store_info
            
        except Exception as e:
            logger.error(f"Error parsing store item: {e}", exc_info=True)
            return None

    def _extract_address_info(self, relationships: Dict[str, Any]) -> Dict[str, str]:
        """
        Extract address information from the relationships object.
        
        Args:
            relationships: Store relationships data
            
        Returns:
            Dictionary with address components
        """
        address_info = {
            "street_name": "",
            "suburb": "",
            "state": "",
            "postcode": ""
        }
        
        try:
            # Navigate to address components: relationships -> storeAddress -> data -> attributes -> addressComponents
            store_address = relationships.get("storeAddress", {})
            address_data = store_address.get("data", {})
            address_attributes = address_data.get("attributes", {})
            address_components = address_attributes.get("addressComponents", {})
            
            # Extract each component
            street_name_obj = address_components.get("streetName", {})
            if isinstance(street_name_obj, dict):
                address_info["street_name"] = street_name_obj.get("value", "")
            
            suburb_obj = address_components.get("suburb", {})
            if isinstance(suburb_obj, dict):
                address_info["suburb"] = suburb_obj.get("value", "")
            
            state_obj = address_components.get("state", {})
            if isinstance(state_obj, dict):
                address_info["state"] = state_obj.get("value", "")
            
            postcode_obj = address_components.get("postcode", {})
            if isinstance(postcode_obj, dict):
                address_info["postcode"] = postcode_obj.get("value", "")
                
        except Exception as e:
            logger.error(f"Error extracting address info: {e}", exc_info=True)
        
        return address_info

    def _determine_drive_thru(self, relationships: Dict[str, Any]) -> bool:
        """
        Determine if the store has drive-thru service.
        
        Args:
            relationships: Store relationships data
            
        Returns:
            True if drive-thru is available, False otherwise
        """
        try:
            # Check collection pickup types: relationships -> collection -> data -> attributes -> pickupTypes -> driveThru
            collection = relationships.get("collection", {})
            collection_data = collection.get("data", {})
            collection_attributes = collection_data.get("attributes", {})
            pickup_types = collection_attributes.get("pickupTypes", {})
            
            return pickup_types.get("driveThru", False)
            
        except Exception as e:
            logger.debug(f"Error determining drive-thru status: {e}")
            return False
