import logging
import json
import re
from typing import List, Dict, Any, Optional, Tuple
from urllib.parse import quote
from selectolax.parser import HTMLParser
from scraper_system.interfaces.parser_interface import ParserInterface
from scraper_system.interfaces.fetcher_interface import FetcherInterface

logger = logging.getLogger(__name__)


class YochiParser(ParserInterface):
    """
    Parser for Yo-Chi locations that scrapes venue addresses from their website
    and enriches them with Google Maps data for detailed location information.
    """

    def __init__(self, fetcher: Optional[FetcherInterface] = None):
        if fetcher is None:
            raise ValueError("YochiParser requires a Fetcher instance.")
        self.fetcher = fetcher
        logger.info("YochiParser initialized with a fetcher.")

    async def parse(
        self, content: str, content_type: Optional[str], config: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Parse Yo-Chi locations by first scraping venue addresses from their website,
        then enriching each address with Google Maps data.
        """
        logger.info("YochiParser starting venue extraction.")

        try:
            # Step 1: Scrape venue addresses and business names from Yo-Chi website
            venues_data = await self._scrape_yochi_addresses()

            if not venues_data:
                logger.warning("No venues found on Yo-Chi website")
                return []

            logger.info(f"Found {len(venues_data)} venue addresses")

            # Step 2: Enrich each venue with Google Maps data
            locations = []
            for venue in venues_data:
                try:
                    address = venue["address"]
                    business_name = venue["business_name"]

                    location_data = await self._fetch_google_maps_data(address)
                    if location_data:
                        # Override the business name with the scraped one
                        location_data["business_name"] = business_name
                        locations.append(location_data)
                except Exception as e:
                    logger.error(f"Error processing venue '{venue}': {e}")
                    continue

            logger.info(f"YochiParser finished, returning {len(locations)} items.")
            return locations

        except Exception as e:
            logger.error(f"Error in YochiParser: {e}")
            import traceback

            traceback.print_exc()
            return []

    async def _scrape_yochi_addresses(self) -> List[Dict[str, str]]:
        """
        Scrape venue addresses and business names from the Yo-Chi website.

        Returns:
            List of dictionaries with 'address' and 'business_name' keys
        """
        try:
            url = "https://yochi.com.au/yochi-venues/"
            logger.info(f"Scraping addresses from: {url}")

            # Configure fetcher for web scraping
            fetcher_config = {
                "headers": {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.5",
                    "Connection": "keep-alive",
                    "Upgrade-Insecure-Requests": "1",
                },
                "timeout": 30,
            }

            # Fetch the page
            content, content_type, status_code = await self.fetcher.fetch(
                url, fetcher_config
            )

            if not content or status_code != 200:
                logger.error(
                    f"Failed to fetch Yo-Chi venues page. Status: {status_code}"
                )
                return []

            # Parse the HTML
            parser = HTMLParser(content)

            # Find all span elements with class "pin-location"
            pin_locations = parser.css("span.pin-location")
            logger.info(f"Found {len(pin_locations)} span.pin-location elements")

            # Also find all business name elements
            business_name_elements = parser.css("h4.title-location")
            logger.info(
                f"Found {len(business_name_elements)} h4.title-location elements"
            )

            # If no pin-location elements found, try alternative selectors
            if len(pin_locations) == 0:
                logger.info(
                    "No pin-location elements found, trying alternative selectors..."
                )
                # Try finding all spans and look for relevant ones
                all_spans = parser.css("span")
                logger.info(f"Total spans found: {len(all_spans)}")

                # Look for spans that might contain address information
                for span in all_spans:
                    if span.attributes and "class" in span.attributes:
                        class_name = span.attributes["class"]
                        if (
                            "pin" in class_name.lower()
                            or "location" in class_name.lower()
                        ):
                            pin_locations.append(span)

            # Extract business names
            business_names = []
            for name_element in business_name_elements:
                if name_element.text():
                    business_name = name_element.text().strip()
                    if business_name:
                        business_names.append(business_name)
                        logger.debug(f"Found business name: {business_name}")

            # Extract addresses
            addresses_data = []
            for i, pin_location in enumerate(pin_locations):
                # Find the anchor tag within the span
                anchor = pin_location.css_first("a")
                if anchor and anchor.text():
                    # Get the text content and clean it up
                    address = anchor.text().strip()
                    if address:
                        # Get corresponding business name if available
                        business_name = "Yo-Chi"  # Default fallback
                        if i < len(business_names):
                            business_name = f"Yo-Chi {business_names[i]}"

                        # Prefix address with "Yo-Chi" for better Google Maps matching
                        clean_address = f"Yo-Chi {address}"

                        addresses_data.append(
                            {"address": clean_address, "business_name": business_name}
                        )
                        logger.debug(f"Found venue: {business_name} - {clean_address}")

            logger.info(f"Total venues extracted: {len(addresses_data)}")
            return addresses_data

        except Exception as e:
            logger.error(f"Error scraping Yo-Chi addresses: {e}")
            return []

    def _clean_address_for_search(self, address: str) -> List[str]:
        """
        Generate multiple cleaned versions of an address for fallback searches.
        Based on the original prototype's comprehensive logic.
        """
        variations = [address]  # Always try the original first

        # Extract business name (e.g., "Yo-Chi") from the beginning of the address
        business_name = ""
        business_match = re.match(r"^([A-Za-z-]+)\s+", address)
        if business_match:
            business_name = business_match.group(1)

        # Special handling for specific problematic patterns
        current_address = address

        # Handle "Restaurant R5" pattern - remove it but keep the rest
        restaurant_pattern = r"^(.*?)\s+Restaurant\s+R\d+\s+(.*)$"
        restaurant_match = re.search(
            restaurant_pattern, current_address, flags=re.IGNORECASE
        )
        if restaurant_match:
            cleaned = f"{restaurant_match.group(1)} {restaurant_match.group(2)}".strip()
            if cleaned not in variations:
                variations.append(cleaned)
                current_address = cleaned

        # Common problematic prefixes that can be removed
        # These patterns are designed to remove everything up to and including the comma
        problematic_patterns = [
            # Remove "Corner of X &, " or "Corner of X and Y, " - more specific pattern
            r"^.*?corner\s+of\s+[^,]+,\s*",
            # Remove "Shop X, " or "Shop X/Y, " - only match up to first comma
            r"^[^,]*shop\s+[^,]*,\s*",
            # Remove "Tenancy X, " or "Tenancy X & Y, "
            r"^[^,]*tenancy\s+[^,]*,\s*",
            # Remove "Unit X, "
            r"^[^,]*unit\s+[^,]*,\s*",
            # Remove "Suite X, "
            r"^[^,]*suite\s+[^,]*,\s*",
            # Remove "Level X, "
            r"^[^,]*level\s+[^,]*,\s*",
            # Remove "Ground Floor, " or "First Floor, "
            r"^[^,]*floor\s*,\s*",
            # Remove "Lot X, "
            r"^[^,]*lot\s+[^,]*,\s*",
            # Remove "Building X, "
            r"^[^,]*building\s+[^,]*,\s*",
        ]

        # Try removing each problematic pattern iteratively
        for pattern in problematic_patterns:
            cleaned = re.sub(pattern, "", current_address, flags=re.IGNORECASE).strip()
            if cleaned and cleaned != current_address and cleaned not in variations:
                # Add the cleaned version with business name preserved
                if business_name and not cleaned.startswith(business_name):
                    business_version = f"{business_name} {cleaned}"
                    if business_version not in variations:
                        variations.append(business_version)

                # Also add the version without business name
                variations.append(cleaned)
                current_address = cleaned  # Use this for the next iteration

        # Smart extraction of important location identifiers
        # Look for shopping centers, plazas, squares that should be preserved
        important_locations = []

        # Extract shopping centers, squares, plazas with their location
        location_patterns = [
            r"([^,]*(?:shopping\s+centre|shopping\s+center|square|plaza|westfield|chadstone)[^,]*,\s*[^,]+\s+[A-Z]{2,3}\s+\d{4})",
            r"([^,]*(?:centre|center|square|plaza)[^,]*,\s*[^,]+\s+[A-Z]{2,3}\s+\d{4})",
        ]

        for pattern in location_patterns:
            matches = re.findall(pattern, address, flags=re.IGNORECASE)
            for match in matches:
                location = match.strip()
                if location and location not in important_locations:
                    important_locations.append(location)

        # Add important location variations with business name
        for location in important_locations:
            if business_name:
                business_location = f"{business_name} {location}"
                if business_location not in variations:
                    variations.append(business_location)
            if location not in variations:
                variations.append(location)

        # Additional fallback: try to extract just the street number and street name + suburb
        # Pattern to match: number + street name + suburb + state + postcode
        street_pattern = r"(\d+\s+[^,]+),\s*([^,]+\s+[A-Z]{2,3}\s+\d{4})"
        match = re.search(street_pattern, current_address)
        if match:
            simple_address = f"{match.group(1)}, {match.group(2)}"
            if simple_address not in variations:
                # Add version with business name
                if business_name:
                    business_version = f"{business_name} {simple_address}"
                    if business_version not in variations:
                        variations.append(business_version)
                variations.append(simple_address)

        # Final fallback: try just the suburb + state + postcode (but only if no important locations found)
        if not important_locations:
            suburb_pattern = r"([^,]+\s+[A-Z]{2,3}\s+\d{4})$"
            match = re.search(suburb_pattern, current_address)
            if match:
                suburb_only = match.group(1).strip()
                if suburb_only not in variations:
                    # Add version with business name
                    if business_name:
                        business_version = f"{business_name} {suburb_only}"
                        if business_version not in variations:
                            variations.append(business_version)
                    variations.append(suburb_only)

        return variations[:8]  # Limit to 8 variations to avoid too many API calls

    async def _get_place_suggestions(
        self, place_query: str
    ) -> Tuple[Optional[str], Optional[float], Optional[float]]:
        """
        Get place ID and coordinates from Google Maps auto-suggest API.

        Returns:
            Tuple of (place_id, latitude, longitude) or (None, None, None) if not found
        """
        try:
            base_url = "https://www.google.com/s"

            # Default coordinates (Melbourne, Australia)
            lat = -37.887846499999995
            lng = 145.082586

            params = {
                "tbm": "map",
                "gs_ri": "maps",
                "suggest": "p",
                "authuser": "0",
                "hl": "en",
                "psi": "MDXtaNnnJvSqxc8PgbO8yQc.1760376925907.1",
                "q": quote(place_query),
                "ech": "9",
                "pb": f"!2i61!4m12!1m3!1d2943.8205169344383!2d{lng}!3d{lat}!2m3!1f0!2f0!3f0!3m2!1i982!2i718!4f13.1!7i20!10b1!12m25!1m5!18b1!30b1!31m1!1b1!34e1!2m4!5m1!6e2!20e3!39b1!10b1!12b1!13b1!16b1!17m1!3e1!20m3!5e2!6b1!14b1!46m1!1b0!96b1!99b1!19m4!2m3!1i360!2i120!4i8!20m57!2m2!1i203!2i100!3m2!2i4!5b1!6m6!1m2!1i86!2i86!1m2!1i408!2i240!7m33!1m3!1e1!2b0!3e3!1m3!1e2!2b1!3e2!1m3!1e2!2b0!3e3!1m3!1e8!2b0!3e3!1m3!1e10!2b0!3e3!1m3!1e10!2b1!3e2!1m3!1e10!2b0!3e4!1m3!1e9!2b1!3e2!2b1!9b0!15m8!1m7!1m2!1m1!1e2!2m2!1i195!2i195!3i20!22m3!1sMDXtaNnnJvSqxc8PgbO8yQc!7e81!17sMDXtaNnnJvSqxc8PgbO8yQc%3A106!23m2!4b1!10b1!24m110!1m31!13m9!2b1!3b1!4b1!6i1!8b1!9b1!14b1!20b1!25b1!18m20!3b1!4b1!5b1!6b1!9b1!12b1!13b1!14b1!17b1!20b1!21b1!22b1!27m1!1b0!28b0!32b1!33m1!1b1!34b1!36e2!10m1!8e3!11m1!3e1!14m1!3b0!17b1!20m2!1e3!1e6!24b1!25b1!26b1!27b1!29b1!30m1!2b1!36b1!37b1!39m3!2m2!2i1!3i1!43b1!52b1!54m1!1b1!55b1!56m1!1b1!61m2!1m1!1e1!65m5!3m4!1m3!1m2!1i224!2i298!72m22!1m8!2b1!5b1!7b1!12m4!1b1!2b1!4m1!1e1!4b1!8m10!1m6!4m1!1e1!4m1!1e3!4m1!1e4!3sother_user_google_review_posts__and__hotel_and_vr_partner_review_posts!6m1!1e1!9b1!89b1!98m3!1b1!2b1!3b1!103b1!113b1!114m3!1b1!2m1!1b1!117b1!122m1!1b1!126b1!127b1!26m4!2m3!1i80!2i92!4i8!34m19!2b1!3b1!4b1!6b1!8m6!1b1!3b1!4b1!5b1!6b1!7b1!9b1!12b1!14b1!20b1!23b1!25b1!26b1!31b1!37m1!1e81!47m0!49m10!3b1!6m2!1b1!2b1!7m2!1e3!2b1!8b1!9b1!10e2!61b1!67m5!7b1!10b1!14b1!15m1!1b0!69i752",
            }

            # Build URL
            query_string = "&".join([f"{k}={v}" for k, v in params.items()])
            url = f"{base_url}?{query_string}"

            # Configure headers for Google Maps API
            headers = {
                "accept": "*/*",
                "accept-language": "en-US,en;q=0.9",
                "downlink": "1.55",
                "priority": "u=1, i",
                "referer": "https://www.google.com/",
                "rtt": "400",
                "sec-ch-ua": '"Google Chrome";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": '"Linux"',
                "sec-fetch-dest": "empty",
                "sec-fetch-mode": "cors",
                "sec-fetch-site": "same-origin",
                "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
                "x-client-data": "CJW2yQEIpLbJAQipncoBCODhygEIlaHLAQiVo8sBCIegzQEIvNXOAQj8284BCLnnzgEIlejOARio5s4BGKnqzgE=",
                "x-maps-diversion-context-bin": "CAE=",
            }

            content, content_type, status_code = await self.fetcher.fetch(
                url, {"headers": headers, "timeout": 60}
            )

            if status_code == 200 and content:
                # Clean the response by removing the )]}'\n prefix
                clean_response = content.replace(")]}'\n", "")

                try:
                    json_data = json.loads(clean_response)

                    # Extract place_id, lat, lng using the paths from the prototype
                    place_id = self._get_nested_value(
                        json_data, [0, 1, 0, 22, 13, 0, 0]
                    )
                    lat_result = self._get_nested_value(json_data, [0, 1, 0, 22, 11, 2])
                    lng_result = self._get_nested_value(json_data, [0, 1, 0, 22, 11, 3])

                    if place_id and lat_result is not None and lng_result is not None:
                        return str(place_id), float(lat_result), float(lng_result)

                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse auto-suggest JSON: {e}")

            return None, None, None

        except Exception as e:
            logger.error(f"Error in auto-suggest API: {e}")
            return None, None, None

    def _get_nested_value(self, data: Dict, indexes: List[int]) -> Optional[Any]:
        """Extract a nested value from a JSON structure using a list of indexes."""
        current = data
        try:
            for index in indexes:
                current = current[index]
            return current
        except (KeyError, IndexError, TypeError):
            return None

    async def _fetch_google_maps_data(self, address: str) -> Optional[Dict[str, Any]]:
        """
        Fetch detailed location data from Google Maps for a given address.

        Args:
            address: The address to look up

        Returns:
            Dictionary with location data or None if not found
        """
        try:
            logger.debug(f"Fetching Google Maps data for: {address}")

            # Get address variations for fallback
            address_variations = self._clean_address_for_search(address)

            place_id = None
            lat = None
            lng = None

            # Try each address variation until we find one that works
            for i, variation in enumerate(address_variations):
                if i > 0:
                    logger.debug(f"Trying fallback address: {variation}")

                place_id, lat, lng = await self._get_place_suggestions(variation)

                if place_id and lat is not None and lng is not None:
                    if i > 0:
                        logger.debug(f"Success with fallback address: {variation}")
                    break

            if not place_id or lat is None or lng is None:
                logger.warning(f"Could not get place suggestions for: {address}")
                return None

            logger.debug(f"Found place ID: {place_id}, coordinates: ({lat}, {lng})")

            # Now fetch detailed place information
            place_details = await self._fetch_place_details(address, place_id, lat, lng)

            if place_details:
                # Convert to the format expected by the transformer
                return {
                    "brand": "Yo-Chi",
                    "business_name": place_details.get("name", f"Yo-Chi {address}"),
                    "street_address": address,  # Keep original scraped address
                    "suburb": place_details.get("suburb"),
                    "state": place_details.get("state"),
                    "postcode": place_details.get("postcode"),
                    "source_url": "https://yochi.com.au/yochi-venues/",
                    "source": "yochi",
                }

            return None

        except Exception as e:
            logger.error(f"Error fetching Google Maps data for '{address}': {e}")
            return None

    async def _fetch_place_details(
        self, place_query: str, place_id: str, lat: float, lng: float
    ) -> Optional[Dict[str, Any]]:
        """
        Fetch detailed place information from Google Maps Places API.

        Args:
            place_query: The original place query
            place_id: Google place ID
            lat: Latitude
            lng: Longitude

        Returns:
            Dictionary with place details or None if not found
        """
        try:
            # Build the Google Maps Places preview URL
            base_url = "https://www.google.com/maps/preview/place"

            params = {
                "authuser": "0",
                "hl": "en",
                "gl": "uk",
                "pb": f"!1m20!1s{place_id}!3m12!1m3!1d3099.0924382042645!2d{lng}!3d{lat}!2m3!1f0!2f0!3f0!3m2!1i982!2i718!4f13.1!4m2!3d{lat}!4d{lng}!5e0!9e0!11s%2Fg%2F11vkhvn81v!12m4!2m3!1i360!2i120!4i8!13m57!2m2!1i203!2i100!3m2!2i4!5b1!6m6!1m2!1i86!2i86!1m2!1i408!2i240!7m33!1m3!1e1!2b0!3e3!1m3!1e2!2b1!3e2!1m3!1e2!2b0!3e3!1m3!1e8!2b0!3e3!1m3!1e10!2b0!3e3!1m3!1e10!2b1!3e2!1m3!1e10!2b0!3e4!1m3!1e9!2b1!3e2!2b1!9b0!15m8!1m7!1m2!1m1!1e2!2m2!1i195!2i195!3i20!14m6!1sBqXraN3BE4H9kdUP8b7ugQ0%3A140!2s1i%3A0%2Ct%3A6986%2Cp%3ABqXraN3BE4H9kdUP8b7ugQ0%3A140!7e81!12e15!17sBqXraN3BE4H9kdUP8b7ugQ0%3A141!18e3!15m111!1m32!13m9!2b1!3b1!4b1!6i1!8b1!9b1!14b1!20b1!25b1!18m21!3b1!4b1!5b1!6b1!9b1!12b1!13b1!14b1!17b1!20b1!21b1!22b1!27m1!1b0!28b0!30b1!32b1!33m1!1b1!34b1!36e2!10m1!8e3!11m1!3e1!14m1!3b0!17b1!20m2!1e3!1e6!24b1!25b1!26b1!27b1!29b1!30m1!2b1!36b1!37b1!39m3!2m2!2i1!3i1!43b1!52b1!54m1!1b1!55b1!56m1!1b1!61m2!1m1!1e1!65m5!3m4!1m3!1m2!1i224!2i298!72m22!1m8!2b1!5b1!7b1!12m4!1b1!2b1!4m1!1e1!4b1!8m10!1m6!4m1!1e1!4m1!1e3!4m1!1e4!3sother_user_google_review_posts__and__hotel_and_vr_partner_review_posts!6m1!1e1!9b1!89b1!98m3!1b1!2b1!3b1!103b1!113b1!114m3!1b1!2m1!1b1!117b1!122m1!1b1!126b1!127b1!21m28!1m6!1m2!1i0!2i0!2m2!1i530!2i718!1m6!1m2!1i932!2i0!2m2!1i982!2i718!1m6!1m2!1i0!2i0!2m2!1i982!2i20!1m6!1m2!1i0!2i698!2m2!1i982!2i718!22m1!1e81!29m0!30m6!3b1!6m1!2b1!7m1!2b1!9b1!34m5!7b1!10b1!14b1!15m1!1b0!37i752!39s{quote(place_query)}",
                "q": quote(place_query),
            }

            # Build URL
            query_string = "&".join([f"{k}={v}" for k, v in params.items()])
            url = f"{base_url}?{query_string}"

            # Configure headers
            headers = {
                "accept": "*/*",
                "accept-language": "en-US,en;q=0.9",
                "downlink": "1.55",
                "priority": "u=1, i",
                "referer": "https://www.google.com/",
                "rtt": "400",
                "sec-ch-ua": '"Google Chrome";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": '"Linux"',
                "sec-fetch-dest": "empty",
                "sec-fetch-mode": "cors",
                "sec-fetch-site": "same-origin",
                "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
                "x-client-data": "CJW2yQEIpLbJAQipncoBCODhygEIlaHLAQiVo8sBCIegzQEIvNXOAQj8284BCLnnzgEIlejOARio5s4BGKnqzgE=",
                "x-maps-diversion-context-bin": "CAE=",
            }

            content, _, status_code = await self.fetcher.fetch(
                url, {"headers": headers, "timeout": 60}
            )

            if status_code == 200 and content:
                # Parse the response
                json_data = self._prepare_places_response(content)
                if json_data:
                    return self._build_place_details(json_data)

            return None

        except Exception as e:
            logger.error(f"Error fetching place details: {e}")
            return None

    def _prepare_places_response(self, input_text: str) -> Optional[List]:
        """Prepare raw input data from Places API by cleaning and parsing it into JSON."""
        try:
            # Remove the JSONP wrapper if present
            prepared = input_text.replace('/*""*/', "")

            # Handle different response formats that Google Places might return
            if prepared.startswith(")]}'\n"):
                prepared = prepared[5:]  # Remove )]}'\n prefix

            json_data = json.loads(prepared)
            return json_data

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON: {e}")
            # Try to find JSON within the response
            start_idx = input_text.find("[")
            end_idx = input_text.rfind("]")
            if start_idx != -1 and end_idx != -1:
                json_part = input_text[start_idx : end_idx + 1]
                try:
                    return json.loads(json_part)
                except json.JSONDecodeError:
                    pass
            return None

    def _build_place_details(self, json_data: List) -> Optional[Dict[str, Any]]:
        """Build place details dictionary from Google Places API response."""
        try:

            def lookup(indexes):
                return self._get_nested_value(json_data, indexes)

            # Extract only the essential address components
            name = lookup([6, 11])

            # Extract address components
            street_address = lookup([6, 183, 1, 1])
            suburb = lookup([6, 183, 1, 3])
            postcode = lookup([6, 183, 1, 4])
            state = lookup([6, 183, 1, 5])

            return {
                "name": str(name) if name else "",
                "street_address": str(street_address) if street_address else None,
                "suburb": str(suburb) if suburb else None,
                "state": str(state) if state else None,
                "postcode": str(postcode) if postcode else None,
            }

        except Exception as e:
            logger.error(f"Error building place details: {e}")
            return None