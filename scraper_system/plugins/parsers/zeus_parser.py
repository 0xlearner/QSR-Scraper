import logging
import asyncio
import re
from typing import List, Dict, Any, Optional

from selectolax.parser import HTMLParser as SelectolaxHTMLParser

from scraper_system.interfaces.parser_interface import ParserInterface
from scraper_system.interfaces.fetcher_interface import FetcherInterface

logger = logging.getLogger(__name__)

BASE_URL = "https://zeusstreetgreek.com.au"

# Manual URL mappings for cases where location name doesn't match URL slug
URL_MAPPINGS = {
    "Ann Street Fortitude Valley": "annstreet",
    "South Yarra": "zeus-street-greek-south-yarra",
    "Maroubra": "zeus-street-greek-maroubra",
    "Queens Plaza": "queens-plaza",
    "Turramurra": "turramurra",
    "Hamilton": "hamilton",
    "Westfield Southland": "westfield-southland",
    "Maitland Green Hills": "maitland-green-hills",
    "Rozelle": "rozelle",
    "Willoughby": "willoughby",
    "Moonee Ponds": "moonee-ponds",
    "Gunghalin": "gunghalin",
    "Westfield Chermside": "westfield-chermside",
    "Karrinyup": "karrinyup",
    "Carindale": "carindale"
}

# Special suburb mappings for locations where the location name doesn't match the actual suburb
SUBURB_MAPPINGS = {
    "Accor Stadium": "Sydney Olympic Park",
    "Macquarie Centre": "North Ryde"
}

# Manual address completion for locations with incomplete addresses
ADDRESS_COMPLETIONS = {
    "Ann Street Fortitude Valley": {
        "street_address": "Ann Street",
        "suburb": "Fortitude Valley",
        "state": "QLD",
        "postcode": "4006"
    }
}


class ZeusParser(ParserInterface):
    """
    Custom parser for Zeus Street Greek website.
    Finds restaurant location links on the order-online page, fetches each location page,
    and extracts the address details.
    """

    def __init__(self, fetcher: Optional[FetcherInterface] = None):
        if fetcher is None:
            raise ValueError("ZeusParser requires a Fetcher instance.")
        self.fetcher = fetcher
        logger.info("ZeusParser initialized with a fetcher.")

    async def parse(
        self, content: str, content_type: Optional[str], config: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Parses the main order-online page, fetches location details, and returns final data.
        """
        if not content:
            logger.warning("No initial content provided to ZeusParser.")
            return []

        logger.info("ZeusParser starting main page parsing.")
        final_results = []
        detail_tasks = []

        try:
            tree = SelectolaxHTMLParser(content)

            # Extract location names from the page
            location_names = self._extract_location_names(tree)
            logger.info(f"Found {len(location_names)} location names on the main page.")

            fetcher_config = config.get(
                "detail_fetcher_options", config.get("fetcher_options", {})
            )

            for location_name in location_names:
                # Use manual mapping if available, otherwise convert to slug
                if location_name in URL_MAPPINGS:
                    location_slug = URL_MAPPINGS[location_name]
                else:
                    location_slug = self._name_to_slug(location_name)

                detail_url = f"{BASE_URL}/store-location/{location_slug}/"

                logger.debug(f"Queueing detail fetch for: {location_name} ({detail_url})")

                # Create a task to fetch and parse the detail page
                detail_tasks.append(
                    self.fetch_and_parse_detail(detail_url, location_name, fetcher_config)
                )

            # Run all detail page fetching and parsing concurrently
            max_concurrent = config.get("parser_options", {}).get("max_concurrent_requests", 10)

            # Process in batches to avoid overwhelming the server
            for i in range(0, len(detail_tasks), max_concurrent):
                batch = detail_tasks[i:i + max_concurrent]
                batch_results = await asyncio.gather(*batch, return_exceptions=True)

                for result in batch_results:
                    if isinstance(result, Exception):
                        logger.error(f"Error in detail fetch: {result}")
                    elif result:  # Filter out None values (failures)
                        final_results.append(result)

        except Exception as e:
            logger.error(f"Error processing main Zeus Street Greek page: {e}", exc_info=True)

        logger.info(f"ZeusParser finished, returning {len(final_results)} items.")
        return final_results

    def _extract_location_names(self, tree: SelectolaxHTMLParser) -> List[str]:
        """
        Extract location names from the order-online page.
        """
        location_names = []

        # Look for location headings - they appear to be in h3 tags
        location_headings = tree.css("h3")

        for heading in location_headings:
            name = heading.text(strip=True)

            # Filter out non-location headings
            if name and not self._is_excluded_heading(name):
                location_names.append(name)

        # Remove duplicates while preserving order
        seen = set()
        unique_names = []
        for name in location_names:
            if name not in seen:
                seen.add(name)
                unique_names.append(name)

        return unique_names

    def _is_excluded_heading(self, heading: str) -> bool:
        """
        Check if a heading should be excluded from location names.
        """
        excluded_patterns = [
            r"@zeusstreetgreek",
            r"zeus street greek",
            r"our food",
            r"about us",
            r"ordering",
            r"contact us",
            r"work for zeus",
            r"win a.*voucher",
            r"rewards",
            r"find your",
            r"zsg",
        ]

        heading_lower = heading.lower()
        return any(re.search(pattern, heading_lower) for pattern in excluded_patterns)

    def _name_to_slug(self, name: str) -> str:
        """
        Convert location name to URL slug format.
        """
        # Convert to lowercase
        slug = name.lower()

        # Replace spaces and special characters with hyphens
        slug = re.sub(r'[^\w\s-]', '', slug)  # Remove special chars except spaces and hyphens
        slug = re.sub(r'[\s_]+', '-', slug)   # Replace spaces and underscores with hyphens
        slug = re.sub(r'-+', '-', slug)       # Replace multiple hyphens with single hyphen
        slug = slug.strip('-')                # Remove leading/trailing hyphens

        return slug

    async def fetch_and_parse_detail(
        self, url: str, location_name: str, fetcher_config: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """
        Helper coroutine to fetch a detail page, parse it for address information,
        and return the combined data.
        """
        logger.debug(f"Executing detail fetch for {location_name} at {url}")
        detail_content, _, status_code = await self.fetcher.fetch(url, fetcher_config)

        if not detail_content:
            logger.error(f"Failed to fetch detail page {url} (Status: {status_code})")
            return None

        try:
            tree = SelectolaxHTMLParser(detail_content)

            # Check if we have manual address completion for this location
            if location_name in ADDRESS_COMPLETIONS:
                completion = ADDRESS_COMPLETIONS[location_name]
                result = {
                    "brand": "Zeus Street Greek",
                    "business_name": f"Zeus Street Greek {location_name}",
                    "address": f"{completion['street_address']}, {completion['suburb']} {completion['state']} {completion['postcode']}",
                    "street_address": completion["street_address"],
                    "suburb": completion["suburb"],
                    "state": completion["state"],
                    "postcode": completion["postcode"],
                    "source_url": url,
                    "source": "zeus",
                }
                logger.debug(f"Used manual completion for {location_name}")
                return result

            # Extract address - look for the address text
            address = self._extract_address(tree)

            if not address:
                logger.warning(f"No address found for {location_name} at {url}")
                return None

            # Parse address components
            address_components = self._parse_address_components(address)

            # Use location name as suburb if no suburb found in address, or use special mapping
            suburb = address_components.get("suburb", "")
            if not suburb:
                # No suburb found in address parsing, use location name
                suburb = SUBURB_MAPPINGS.get(location_name, location_name)
            elif location_name in SUBURB_MAPPINGS:
                # Override with special mapping if exists
                suburb = SUBURB_MAPPINGS[location_name]
            else:
                # Validate extracted suburb - if it doesn't look like a real suburb, use location name
                invalid_suburb_indicators = ['Square', 'Centre', 'Center', 'Mall', 'Plaza', 'Park', 'Gardens', 'Village', 'Place']
                if suburb in invalid_suburb_indicators:
                    suburb = SUBURB_MAPPINGS.get(location_name, location_name)

            # Return the basic scraped data
            result = {
                "brand": "Zeus Street Greek",
                "business_name": f"Zeus Street Greek {location_name}",
                "address": address,  # Raw address for transformation
                "street_address": address_components.get("street_address", ""),
                "suburb": suburb,
                "state": address_components.get("state", ""),
                "postcode": address_components.get("postcode", ""),
                "source_url": url,
                "source": "zeus",
            }

            logger.debug(f"Successfully parsed {location_name}: {address}")
            return result

        except Exception as e:
            logger.error(
                f"Error parsing detail page {url} for {location_name}: {e}", exc_info=True
            )
            return None

    def _extract_address(self, tree: SelectolaxHTMLParser) -> Optional[str]:
        """
        Extract the address from the store location page.
        """
        page_text = tree.text()

        # More specific pattern that matches the exact format used by Zeus Street Greek
        # They use format: "street address, suburb STATE postcode"
        address_patterns = [
            # Pattern 1: Full address with comma separation
            r'(\d+(?:[-/]\d+)?[^,\n]*,\s*[^,\n]+\s+(?:NSW|VIC|QLD|SA|WA|TAS|NT|ACT)\s+\d{4})',
            # Pattern 2: Shop/unit number format
            r'((?:Shop|Unit|Level|S)\s*\d+[A-Za-z]?[^,\n]*,\s*[^,\n]+\s+(?:NSW|VIC|QLD|SA|WA|TAS|NT|ACT)\s+\d{4})',
            # Pattern 3: More flexible street pattern
            r'(\d+[^,\n]*(?:Street|St|Road|Rd|Avenue|Ave|Drive|Dr|Highway|Hwy|Place|Pl|Court|Ct|Lane|Ln|Parade|Pde|Boulevard|Blvd|Crescent|Cres|Close|Cl|Terrace|Tce|Circle|Cir|Way|Mall|Plaza)[^,\n]*,\s*[^,\n]+\s+(?:NSW|VIC|QLD|SA|WA|TAS|NT|ACT)\s+\d{4})'
        ]

        for pattern in address_patterns:
            match = re.search(pattern, page_text, re.IGNORECASE)
            if match:
                address = match.group(1).strip()
                # Clean up the address
                address = re.sub(r'\s+', ' ', address)  # Normalize whitespace
                return address

        # Final fallback: look for any line that contains state and postcode
        lines = page_text.split('\n')
        for line in lines:
            line = line.strip()
            if re.search(r'(?:NSW|VIC|QLD|SA|WA|TAS|NT|ACT)\s+\d{4}', line) and len(line) < 200:
                # Filter out very long lines that are unlikely to be addresses
                return line

        return None

    def _parse_address_components(self, address: str) -> Dict[str, str]:
        """
        Parse address string to extract components.
        """
        components = {
            "street_address": "",
            "suburb": "",
            "state": "",
            "postcode": "",
        }

        if not address:
            return components

        # Extract state and postcode
        state_postcode_match = self._extract_state_postcode(address)
        if not state_postcode_match:
            return components

        components["state"] = state_postcode_match["state"]
        components["postcode"] = state_postcode_match["postcode"]

        # Get address without state/postcode
        address_without_state_postcode = state_postcode_match["remaining_address"]

        # Parse the remaining address
        if "," in address_without_state_postcode:
            self._parse_comma_separated_address(address_without_state_postcode, components)
        else:
            self._parse_non_comma_address(address_without_state_postcode, components)

        return components

    def _extract_state_postcode(self, address: str) -> Optional[Dict[str, str]]:
        """Extract state and postcode from address."""
        state_postcode_match = re.search(r'([A-Z]{2,3})\s+(\d{4})$', address)
        if not state_postcode_match:
            return None

        return {
            "state": state_postcode_match.group(1),
            "postcode": state_postcode_match.group(2),
            "remaining_address": address[:state_postcode_match.start()].strip()
        }

    def _get_street_indicators(self) -> List[str]:
        """Get list of street type indicators."""
        return ['St', 'Street', 'Rd', 'Road', 'Ave', 'Avenue', 'Dr', 'Drive',
                'Hwy', 'Highway', 'Pl', 'Place', 'Ct', 'Court', 'Ln', 'Lane',
                'Pde', 'Parade', 'Blvd', 'Boulevard', 'Cres', 'Crescent',
                'Cl', 'Close', 'Tce', 'Terrace', 'Cir', 'Circle', 'Way',
                'Mall', 'Plaza']

    def _find_street_indicator_index(self, words: List[str]) -> int:
        """Find the last occurrence of a street indicator in words."""
        street_indicators = self._get_street_indicators()
        street_end_idx = -1
        for i, word in enumerate(words):
            if word in street_indicators or word.rstrip(',') in street_indicators:
                street_end_idx = i
        return street_end_idx

    def _parse_comma_separated_address(self, address: str, components: Dict[str, str]) -> None:
        """Parse comma-separated address format."""
        parts = [p.strip() for p in address.split(",")]

        if len(parts) >= 2:
            last_part = parts[-1]
            words = last_part.split()
            street_end_idx = self._find_street_indicator_index(words)

            if street_end_idx != -1 and street_end_idx < len(words) - 1:
                # Everything after the street indicator is suburb
                components["suburb"] = " ".join(words[street_end_idx + 1:])
                # Street address includes everything before + street part
                street_part = " ".join(words[:street_end_idx + 1])
                components["street_address"] = ", ".join(parts[:-1] + [street_part]).strip()
            else:
                # No street indicator found, use full address as street and rely on fallback
                components["street_address"] = address
        else:
            # Only one part, treat as street address
            components["street_address"] = parts[0].strip()

    def _parse_non_comma_address(self, address: str, components: Dict[str, str]) -> None:
        """Parse non-comma-separated address format."""
        words = address.split()

        if len(words) >= 2:
            street_end_idx = self._find_street_indicator_index(words)

            if street_end_idx != -1 and street_end_idx < len(words) - 1:
                # Street address is everything up to and including the street indicator
                components["street_address"] = " ".join(words[:street_end_idx + 1])
                # Suburb is everything after the street indicator
                components["suburb"] = " ".join(words[street_end_idx + 1:])
            else:
                # Fallback: assume last word(s) are suburb
                self._parse_fallback_suburb(words, components, address)
        else:
            # Only one word, treat as street address
            components["street_address"] = address

    def _parse_fallback_suburb(self, words: List[str], components: Dict[str, str], full_address: str) -> None:
        """Parse suburb using fallback logic for capitalized words."""
        if len(words) >= 3:
            # Assume last 1-2 words are suburb if they're capitalized
            if words[-1][0].isupper():
                if (len(words) >= 4 and
                    words[-2][0].isupper() and
                    len(words[-2]) > 2):
                    # Two-word suburb
                    components["suburb"] = " ".join(words[-2:])
                    components["street_address"] = " ".join(words[:-2])
                else:
                    # One-word suburb
                    components["suburb"] = words[-1]
                    components["street_address"] = " ".join(words[:-1])
            else:
                # Can't determine, use full address as street address
                components["street_address"] = full_address
        else:
            # Too few words, use as street address
            components["street_address"] = full_address
