import logging
import hashlib
import re
from typing import List, Dict, Any, Optional

from scraper_system.interfaces.transformer_interface import TransformerInterface
from scraper_system.models.location import TransformedLocation

logger = logging.getLogger(__name__)


class ZeusTransformer(TransformerInterface):
    """
    Transformer for Zeus Street Greek data.
    Responsible for parsing address components and transforming raw data into final format.
    """

    async def transform(
        self, data: List[Dict[str, Any]], config: Dict[str, Any], site_name: str
    ) -> List[Dict[str, Any]]:
        """
        Transforms raw scraped data from Zeus parser.

        Args:
            data: List of raw scraped items with basic data
            config: Transformation configuration
            site_name: Source name (should be 'zeus')

        Returns:
            List of transformed location dictionaries
        """
        if not data:
            logger.warning("No data to transform for Zeus Street Greek")
            return []

        transformed_items = []

        for item in data:
            try:
                # Extract raw data
                brand = item.get("brand", "Zeus Street Greek")
                name = item.get("business_name", "")
                address = item.get("address", "")  # Raw address string
                street_address = item.get("street_address", "")
                suburb = item.get("suburb", "")  # This should already be set correctly by parser
                state = item.get("state", "")
                postcode = item.get("postcode", "")
                source_url = item.get("source_url", "")

                # Parse address components if we have a raw address but missing components
                if address and not all([street_address, state, postcode]):
                    address_components = self._parse_address_components(address)
                    street_address = street_address or address_components.get("street_address", "")
                    # Only use parsed suburb if we don't already have one from parser
                    if not suburb:
                        suburb = address_components.get("suburb", "")
                    state = state or address_components.get("state", "")
                    postcode = postcode or address_components.get("postcode", "")

                # Clean and validate components
                street_address = self._clean_street_address(street_address)
                suburb = self._clean_suburb(suburb)
                state = self._clean_state(state)
                postcode = self._clean_postcode(postcode)

                # Skip if we don't have essential address components
                if not street_address or not state or not suburb:
                    logger.warning(f"Skipping {name} - missing essential address components")
                    continue

                # Generate business ID
                business_id = self.generate_business_id(
                    name,
                    f"{street_address}, {suburb} {state} {postcode}",
                )

                # Create the transformed location
                location = TransformedLocation(
                    brand=brand,
                    business_name=name,
                    street_address=street_address,
                    suburb=suburb,
                    state=state,
                    postcode=postcode,
                    drive_thru=False,  # Zeus Street Greek doesn't typically have drive-thru
                    shopping_centre_name=self._extract_shopping_centre(street_address),
                    source_url=source_url,
                    source="zeus",
                    business_id=business_id,
                )

                transformed_items.append(location.model_dump())

            except Exception as e:
                logger.error(f"Error transforming Zeus Street Greek item: {e}", exc_info=True)

        logger.info(f"Transformed {len(transformed_items)} Zeus Street Greek items")
        return transformed_items

    def _parse_address_components(self, address: str) -> Dict[str, str]:
        """Parse address string to extract components."""
        components = {
            "street_address": "",
            "suburb": "",
            "state": "",
            "postcode": "",
        }

        if not address:
            return components

        # Clean the address
        address = address.strip()

        # Extract state and postcode
        state_postcode_data = self._extract_state_postcode(address)
        if not state_postcode_data:
            return components

        components["state"] = state_postcode_data["state"]
        components["postcode"] = state_postcode_data["postcode"]

        # Parse the remaining address
        address_without_state_postcode = state_postcode_data["remaining_address"]

        if "," in address_without_state_postcode:
            self._parse_comma_separated_format(address_without_state_postcode, components)
        else:
            self._parse_space_separated_format(address_without_state_postcode, components)

        return components

    def _extract_state_postcode(self, address: str) -> Optional[Dict[str, str]]:
        """Extract state and postcode from address."""
        state_postcode_match = re.search(r'\b([A-Z]{2,3})\s+(\d{4})\b', address)
        if not state_postcode_match:
            return None

        return {
            "state": state_postcode_match.group(1),
            "postcode": state_postcode_match.group(2),
            "remaining_address": address.replace(state_postcode_match.group(0), "").strip()
        }

    def _parse_comma_separated_format(self, address: str, components: Dict[str, str]) -> None:
        """Parse comma-separated address format."""
        parts = [p.strip() for p in address.split(",")]

        if len(parts) >= 2:
            # Street address is everything except the last part, suburb is the last part
            components["street_address"] = ", ".join(parts[:-1]).strip()
            components["suburb"] = parts[-1].strip()
        elif len(parts) == 1:
            # Only one part - could be street or suburb, need to guess
            part = parts[0].strip()
            if re.search(r'\d+', part):  # Contains numbers, likely street address
                components["street_address"] = part
            else:
                components["suburb"] = part

    def _parse_space_separated_format(self, address: str, components: Dict[str, str]) -> None:
        """Parse space-separated address format."""
        words = address.split()

        if len(words) >= 3:
            self._parse_multi_word_address(words, components)
        elif len(words) == 2:
            # Two words - first is likely street, second is suburb
            components["street_address"] = words[0]
            components["suburb"] = words[1]
        elif len(words) == 1:
            # Single word - guess based on content
            word = words[0]
            if re.search(r'\d+', word):
                components["street_address"] = word
            else:
                components["suburb"] = word

    def _parse_multi_word_address(self, words: List[str], components: Dict[str, str]) -> None:
        """Parse address with 3 or more words."""
        street_indicators = ['st', 'street', 'rd', 'road', 'ave', 'avenue', 'dr', 'drive',
                           'hwy', 'highway', 'pl', 'place', 'ct', 'court', 'ln', 'lane',
                           'pde', 'parade', 'blvd', 'boulevard', 'cres', 'crescent',
                           'cl', 'close', 'tce', 'terrace', 'cir', 'circle', 'way']

        street_end_idx = self._find_street_indicator(words, street_indicators)

        if street_end_idx != -1:
            # Found street indicator
            components["street_address"] = " ".join(words[:street_end_idx + 1])
            if street_end_idx + 1 < len(words):
                components["suburb"] = " ".join(words[street_end_idx + 1:])
        else:
            # No clear street indicator, assume last word is suburb
            components["street_address"] = " ".join(words[:-1])
            components["suburb"] = words[-1]

    def _find_street_indicator(self, words: List[str], street_indicators: List[str]) -> int:
        """Find the index of the first street indicator in words."""
        for i, word in enumerate(words):
            if word.lower().rstrip('.,') in street_indicators:
                return i
        return -1

    def _clean_street_address(self, street_address: str) -> str:
        """Clean and normalize street address."""
        if not street_address:
            return ""

        # Remove extra whitespace
        street_address = re.sub(r'\s+', ' ', street_address.strip())

        # Remove any trailing commas
        street_address = street_address.rstrip(',')

        return street_address

    def _clean_suburb(self, suburb: str) -> str:
        """Clean and normalize suburb."""
        if not suburb:
            return ""

        # Remove extra whitespace
        suburb = re.sub(r'\s+', ' ', suburb.strip())

        # Title case the suburb
        suburb = suburb.title()

        return suburb

    def _clean_state(self, state: str) -> str:
        """Clean and normalize state."""
        if not state:
            return ""

        # Convert to uppercase and remove whitespace
        state = state.upper().strip()

        # Validate Australian states
        valid_states = ["NSW", "VIC", "QLD", "SA", "WA", "TAS", "NT", "ACT"]
        if state in valid_states:
            return state

        # Handle common variations
        state_mappings = {
            "NEW SOUTH WALES": "NSW",
            "VICTORIA": "VIC",
            "QUEENSLAND": "QLD",
            "SOUTH AUSTRALIA": "SA",
            "WESTERN AUSTRALIA": "WA",
            "TASMANIA": "TAS",
            "NORTHERN TERRITORY": "NT",
            "AUSTRALIAN CAPITAL TERRITORY": "ACT"
        }

        return state_mappings.get(state, state)

    def _clean_postcode(self, postcode: str) -> str:
        """Clean and validate postcode."""
        if not postcode:
            return ""

        # Remove non-digits
        postcode = re.sub(r'\D', '', postcode)

        # Australian postcodes are 4 digits
        if len(postcode) == 4 and postcode.isdigit():
            return postcode

        return ""

    def _extract_shopping_centre(self, street_address: str) -> Optional[str]:
        """Extract shopping centre name from street address if present."""
        if not street_address:
            return None

        shopping_centre_keywords = [
            "Shopping Centre",
            "Shopping Center",
            "Plaza",
            "Mall",
            "Centre",
            "Center",
            "Westfield",
            "Complex"
        ]

        for keyword in shopping_centre_keywords:
            # Look for the keyword in the address
            if keyword.lower() in street_address.lower():
                # Try to extract the shopping centre name
                # Look for patterns like "Shop X, Shopping Centre Name" or "Shopping Centre Name"
                pattern = rf'([^,]*{re.escape(keyword)}[^,]*)'
                match = re.search(pattern, street_address, re.IGNORECASE)
                if match:
                    centre_name = match.group(1).strip()
                    # Clean up the centre name
                    centre_name = re.sub(r'^Shop\s+\d+[A-Za-z]?,?\s*', '', centre_name, flags=re.IGNORECASE)
                    centre_name = re.sub(r'^Unit\s+\d+[A-Za-z]?,?\s*', '', centre_name, flags=re.IGNORECASE)
                    centre_name = re.sub(r'^Level\s+\d+,?\s*', '', centre_name, flags=re.IGNORECASE)
                    return centre_name.strip()

        return None

    def generate_business_id(self, name: str, address: str) -> str:
        """Generates a unique ID based on name and address."""
        data_string = f"{name.lower().strip()}|{address.lower().strip()}"
        return hashlib.sha1(data_string.encode("utf-8")).hexdigest()
