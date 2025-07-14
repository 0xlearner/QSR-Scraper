import logging
import hashlib
import re
from typing import List, Dict, Any, Optional

from scraper_system.interfaces.transformer_interface import TransformerInterface
from scraper_system.models.location import TransformedLocation

logger = logging.getLogger(__name__)


class NandosTransformer(TransformerInterface):
    """
    Transformer for Nandos data.
    Responsible for converting JSON-LD structured data into standardized format.
    """

    async def transform(
        self, data: List[Dict[str, Any]], config: Dict[str, Any], site_name: str
    ) -> List[Dict[str, Any]]:
        """
        Transforms raw JSON-LD data from Nandos parser.

        Args:
            data: List of raw JSON-LD items from restaurant pages
            config: Transformation configuration
            site_name: Source name (should be 'nandos')

        Returns:
            List of transformed location dictionaries
        """
        if not data:
            logger.warning("No data to transform for Nandos")
            return []

        transformed_items = []

        for item in data:
            try:
                # Extract basic information
                brand = item.get("brand", "Nandos")
                name = "Nandos " + item.get("name", "").title()
                source_url = item.get("source_url", "")

                # Extract address from JSON-LD structure
                address_data = item.get("address", {})
                if not isinstance(address_data, dict):
                    logger.warning(f"Address data is not a dict for {name}")
                    continue

                street_address = address_data.get("streetAddress", "")
                suburb = address_data.get("addressLocality", "").upper()
                state = address_data.get("addressRegion", "")
                postcode = address_data.get("postalCode", "")

                # Clean and process address components
                street_address = self._clean_street_address(street_address)
                suburb = self._clean_suburb(suburb)
                state = self._clean_state(state)
                postcode = self._clean_postcode(postcode)

                # Extract shopping centre name from street address if present
                shopping_centre_name = self._extract_shopping_centre_name(street_address)
                street_address = street_address.strip()

                # Note: Coordinates, phone, and opening hours not required per user request

                # Determine drive-thru status (Nandos typically doesn't have drive-thru, but check just in case)
                drive_thru = self._determine_drive_thru_status(item, name)

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
                    drive_thru=drive_thru,
                    shopping_centre_name=shopping_centre_name,
                    source_url=source_url,
                    source="nandos",
                    business_id=business_id,
                )

                # Only include the base model data (no additional fields needed)
                transformed_items.append(location.model_dump())

            except Exception as e:
                logger.error(f"Error transforming Nandos item: {e}", exc_info=True)
                continue

        logger.info(f"Transformed {len(transformed_items)} Nandos items")
        return transformed_items

    def _clean_street_address(self, address: str) -> str:
        """Clean and normalize street address."""
        if not address:
            return ""

        # Remove extra whitespace and normalize
        address = re.sub(r'\s+', ' ', address.strip())

        return address

    def _clean_suburb(self, suburb: str) -> str:
        """Clean and normalize suburb name."""
        if not suburb:
            return ""

        # Remove extra whitespace and normalize
        suburb = re.sub(r'\s+', ' ', suburb.strip())

        return suburb

    def _clean_state(self, state: str) -> str:
        """Clean and normalize state code."""
        if not state:
            return ""

        # Convert to uppercase and normalize common state codes
        state = state.strip().upper()

        # Handle common variations
        state_mapping = {
            "NEW SOUTH WALES": "NSW",
            "VICTORIA": "VIC",
            "QUEENSLAND": "QLD",
            "SOUTH AUSTRALIA": "SA",
            "WESTERN AUSTRALIA": "WA",
            "TASMANIA": "TAS",
            "NORTHERN TERRITORY": "NT",
            "AUSTRALIAN CAPITAL TERRITORY": "ACT",
        }

        return state_mapping.get(state, state)

    def _clean_postcode(self, postcode: str) -> str:
        """Clean and validate postcode."""
        if not postcode:
            return ""

        # Extract 4-digit postcode
        postcode = re.sub(r'\D', '', postcode.strip())

        if len(postcode) == 4 and postcode.isdigit():
            return postcode

        return ""

    def _extract_shopping_centre_name(self, address: str) -> Optional[str]:
        """Extract shopping centre name from address."""
        if not address:
            return None

        shopping_centre_patterns = [
            r'(.+?(?:Shopping Centre|Shopping Center|Centre|Mall|Plaza))',
            r'(.+?(?:Westfield|Centro|DFO))',
        ]

        for pattern in shopping_centre_patterns:
            match = re.search(pattern, address, re.IGNORECASE)
            if match:
                centre_name = match.group(1).strip()
                # Clean up common prefixes like "Shop 123, "
                centre_name = re.sub(r'^(?:Shop\s*\d+[A-Za-z]*,?\s*|Level\s*\d+,?\s*|Unit\s*\d+[A-Za-z]*,?\s*)', '', centre_name, flags=re.IGNORECASE)
                return centre_name.strip()

        return None

    def _determine_drive_thru_status(self, item: Dict[str, Any], name: str) -> bool:
        """Determine if location has drive-thru (unlikely for Nandos but check anyway)."""
        # Check various places where drive-thru might be mentioned
        text_to_check = [
            item.get("name", ""),
            item.get("address", {}).get("streetAddress", ""),
            str(item)  # Check entire JSON for any mention
        ]

        drive_thru_keywords = ["drive thru", "drive-thru", "drive through", "drivethrough"]

        for text in text_to_check:
            if isinstance(text, str):
                text_lower = text.lower()
                if any(keyword in text_lower for keyword in drive_thru_keywords):
                    return True

        return False

    def generate_business_id(self, name: str, address: str) -> str:
        """Generates a unique ID based on name and address."""
        data_string = f"{name.lower().strip()}|{address.lower().strip()}"
        return hashlib.sha1(data_string.encode("utf-8")).hexdigest()
