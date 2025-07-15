import logging
import asyncio
from typing import List, Dict, Any, Optional
from urllib.parse import urljoin
import re

from selectolax.parser import HTMLParser as SelectolaxHTMLParser
from selectolax.parser import Node

from scraper_system.interfaces.parser_interface import ParserInterface
from scraper_system.interfaces.fetcher_interface import FetcherInterface

logger = logging.getLogger(__name__)

BASE_URL = "https://www.zambrero.com.au"


class ZambreroParser(ParserInterface):
    """
    Custom parser for Zambrero website.
    Scrapes restaurant locations by iterating through all Australian states
    and extracting location data including street address, suburb, state, and postcode.
    """

    def __init__(self, fetcher: Optional[FetcherInterface] = None):
        if fetcher is None:
            raise ValueError("ZambreroParser requires a Fetcher instance.")
        self.fetcher = fetcher
        logger.info("ZambreroParser initialized with a fetcher.")

    async def parse(
        self, content: str, content_type: Optional[str], config: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Parses Zambrero locations by fetching state-specific pages and extracting location data.
        """
        logger.info("ZambreroParser starting location scraping.")

        # Australian states and territories
        states = ["ACT", "NSW", "NT", "QLD", "SA", "TAS", "VIC", "WA"]

        final_results = []
        state_tasks = []

        # Get fetcher configuration
        fetcher_config = config.get(
            "detail_fetcher_options", config.get("fetcher_options", {})
        )

        # Create tasks for each state
        for state in states:
            state_url = f"{BASE_URL}/locations/search?state={state}"
            logger.debug(f"Queueing state fetch for: {state} ({state_url})")
            state_tasks.append(
                self.fetch_and_parse_state(state_url, state, fetcher_config)
            )

        try:
            # Run all state fetching and parsing concurrently
            results_from_states = await asyncio.gather(*state_tasks, return_exceptions=True)

            # Collect valid results
            for result in results_from_states:
                if isinstance(result, Exception):
                    logger.error(f"Error processing state: {result}")
                    continue
                if result:  # Filter out None values (failures)
                    final_results.extend(result)

        except Exception as e:
            logger.error(f"Error processing Zambrero states: {e}", exc_info=True)

        logger.info(f"ZambreroParser finished, returning {len(final_results)} items.")
        return final_results

    async def fetch_and_parse_state(
        self, state_url: str, state: str, fetcher_config: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Helper coroutine to fetch a state page and extract all location data.
        """
        logger.debug(f"Executing state fetch for {state} at {state_url}")

        try:
            state_content, _, status_code = await self.fetcher.fetch(state_url, fetcher_config)

            if not state_content:
                logger.error(f"Failed to fetch state page {state_url} (Status: {status_code})")
                return []

            tree = SelectolaxHTMLParser(state_content)
            locations = []
            seen_locations = set()  # Track unique locations to avoid duplicates

            # Find all location cards - they appear to be in h4 elements followed by address info
            location_headers = tree.css("h4")

            for header in location_headers:
                try:
                    location_data = self.extract_location_from_header(header, state)
                    if location_data:
                        # Create a unique key to identify duplicate locations
                        location_key = (
                            location_data.get('business_name', ''),
                            location_data.get('street_address', ''),
                            location_data.get('suburb', ''),
                            location_data.get('postcode', '')
                        )

                        # Only add if we haven't seen this location before
                        if location_key not in seen_locations:
                            locations.append(location_data)
                            seen_locations.add(location_key)
                        else:
                            logger.debug(f"Skipping duplicate location: {location_data.get('business_name')}")
                except Exception as e:
                    logger.error(f"Error extracting location from header in {state}: {e}")
                    continue

            logger.info(f"Found {len(locations)} unique locations in {state}")
            return locations

        except Exception as e:
            logger.error(f"Error processing state {state}: {e}", exc_info=True)
            return []

    def extract_location_from_header(self, header: Node, state: str) -> Optional[Dict[str, Any]]:
        """
        Extract location data from a location header element and its siblings.
        """
        try:
            # Get the restaurant name from the h4 text
            name_text = header.text(strip=True)
            if not name_text:
                return None

            # Clean up the name - remove status indicators like "- Opening Soon"
            name = re.sub(r'\s*-\s*(Opening Soon|Temporarily.*|Closed).*$', '', name_text, flags=re.IGNORECASE).strip()
            business_name = f"Zambrero {name}"

            # Find the next element that contains the address
            # Look for the address in the next few siblings
            current = header.next
            address_text = ""

            # Search through siblings to find address text
            attempts = 0
            while current and attempts < 10:  # Limit search to avoid infinite loops
                if current.tag and current.text(strip=True):
                    text = current.text(strip=True)
                    # Check if this looks like an address (contains street/road indicators)
                    if self.is_address_text(text):
                        address_text = text
                        break
                current = current.next
                attempts += 1

            if not address_text:
                logger.debug(f"No address found for {name}")
                return None

            # Parse the address to extract components
            parsed_address = self.parse_address(address_text)
            if not parsed_address:
                logger.debug(f"Could not parse address for {name}: {address_text}")
                return None

            # Extract URL if available - look for Order & Store Info link
            source_url = self.find_store_url(header, name)

            # Determine if this is a drive-thru location
            drive_thru = self.check_drive_thru_status(name_text, address_text)

            # Use the restaurant name as the suburb (e.g., "Conder" from "Zambrero Conder")
            suburb = name.strip()

            return {
                "brand": "Zambrero",
                "business_name": business_name,
                "street_address": parsed_address["street_address"],
                "suburb": suburb,
                "state": parsed_address["state"],
                "postcode": parsed_address["postcode"],
                "drive_thru": drive_thru,
                "source_url": source_url,
                "source": "zambrero",
            }

        except Exception as e:
            logger.error(f"Error extracting location data: {e}")
            return None

    def is_address_text(self, text: str) -> bool:
        """
        Check if text looks like an address by looking for common address patterns.
        """
        # Common address indicators
        address_patterns = [
            r'\d+.*(?:Street|St|Road|Rd|Avenue|Ave|Boulevard|Blvd|Highway|Hwy|Drive|Dr|Lane|Ln|Parade|Terrace|Crescent|Cres|Place|Pl|Circuit|Cct)',
            r'Shop\s+\d+',
            r'Level\s+\d+',
            r'Unit\s+\d+',
            r'\d{4}\s+Australia',  # Postcode + Australia
        ]

        for pattern in address_patterns:
            if re.search(pattern, text, re.IGNORECASE):
                return True
        return False

    def parse_address(self, address_text: str) -> Optional[Dict[str, str]]:
        """
        Parse address text to extract street address, state, and postcode.
        Expected format: "Street Address Suburb STATE POSTCODE Australia"
        Note: We don't extract suburb from address since restaurant name contains the suburb.
        """
        try:
            # Clean the address
            address = address_text.strip()

            # Remove "Australia" from the end
            address = re.sub(r'\s+Australia\s*$', '', address, flags=re.IGNORECASE)

            # Extract postcode (4 digits at the end)
            postcode_match = re.search(r'(\d{4})\s*$', address)
            if not postcode_match:
                return None

            postcode = postcode_match.group(1)
            address_without_postcode = address[:postcode_match.start()].strip()

            # Extract state (2-3 letter code before postcode)
            state_match = re.search(r'\b([A-Z]{2,3})\s*$', address_without_postcode)
            if not state_match:
                return None

            state = state_match.group(1)

            # Everything before the state is the street address
            # (we remove the suburb part since we get it from restaurant name)
            address_without_state = address_without_postcode[:state_match.start()].strip()

            # Remove the last word(s) that are likely the suburb to get clean street address
            words = address_without_state.split()
            if len(words) >= 2:
                # Remove last 1-2 words that are likely suburb names
                street_address = ' '.join(words[:-1])
            else:
                street_address = address_without_state

            return {
                "street_address": street_address.strip(),
                "state": state,
                "postcode": postcode
            }

        except Exception as e:
            logger.error(f"Error parsing address '{address_text}': {e}")
            return None

    def find_store_url(self, header: Node, name: str) -> str:
        """
        Find the store URL from nearby elements.
        """
        try:
            # Look for "Order & Store Info" link near the header
            current = header.next
            attempts = 0

            while current and attempts < 15:
                if current.tag == 'a':
                    href = current.attributes.get('href', '')
                    link_text = current.text(strip=True).lower()

                    # Check if this is a store info link
                    if 'store info' in link_text or 'order' in link_text:
                        if href.startswith('/'):
                            return urljoin(BASE_URL, href)
                        elif href.startswith('http'):
                            return href

                # Also check child elements
                links = current.css('a') if hasattr(current, 'css') else []
                for link in links:
                    href = link.attributes.get('href', '')
                    link_text = link.text(strip=True).lower()

                    if 'store info' in link_text or ('/locations/' in href and name.lower().replace(' ', '-') in href):
                        if href.startswith('/'):
                            return urljoin(BASE_URL, href)
                        elif href.startswith('http'):
                            return href

                current = current.next
                attempts += 1

            # Fallback: construct URL from name
            name_slug = name.lower().replace(' ', '-').replace('&', '-').replace('/', '-')
            name_slug = re.sub(r'[^\w\-]', '', name_slug)
            name_slug = re.sub(r'-+', '-', name_slug).strip('-')
            return f"{BASE_URL}/locations/{name_slug}"

        except Exception as e:
            logger.error(f"Error finding store URL for {name}: {e}")
            return f"{BASE_URL}/locations"

    def check_drive_thru_status(self, name_text: str, address_text: str) -> bool:
        """
        Check if the location has drive-thru service.
        """
        # Check for drive-thru indicators in name or address
        drive_thru_keywords = [
            "drive thru", "drive-thru", "drive through", "drivethrough",
            "dt", " dt ", "drive-through"
        ]

        combined_text = f"{name_text} {address_text}".lower()

        for keyword in drive_thru_keywords:
            if keyword in combined_text:
                return True

        return False
