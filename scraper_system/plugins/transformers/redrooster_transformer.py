import logging
import hashlib
import re
from typing import List, Dict, Any, Optional

from scraper_system.interfaces.transformer_interface import TransformerInterface
from scraper_system.models.location import TransformedLocation

logger = logging.getLogger(__name__)


class RedRoosterTransformer(TransformerInterface):
    """
    Transformer for Red Rooster data.
    Responsible for converting parsed API data into standardized format.
    """

    async def transform(
        self, data: List[Dict[str, Any]], config: Dict[str, Any], site_name: str
    ) -> List[Dict[str, Any]]:
        """
        Transforms raw Red Rooster data from parser.

        Args:
            data: List of raw store items from Red Rooster parser
            config: Transformation configuration
            site_name: Source name (should be 'redrooster')

        Returns:
            List of transformed location dictionaries
        """
        if not data:
            logger.warning("No data to transform for Red Rooster")
            return []

        transformed_items = []

        for item in data:
            try:
                # Extract and clean data
                brand = item.get("brand", "Red Rooster")
                business_name = item.get("business_name", "")
                street_address = self._clean_street_address(item.get("street_address", ""))
                suburb = self._clean_suburb(item.get("suburb", ""))
                state = self._clean_state(item.get("state", ""))
                postcode = self._clean_postcode(item.get("postcode", ""))
                drive_thru = item.get("drive_thru", False)
                source_url = item.get("source_url", "")

                # Skip if essential data is missing
                if not business_name or not suburb:
                    logger.warning(f"Skipping item with missing essential data: {item}")
                    continue

                # Determine shopping centre name from street address
                shopping_centre_name = self._extract_shopping_centre_name(street_address)

                # Generate business ID
                business_id = self.generate_business_id(
                    business_name,
                    f"{street_address}, {suburb} {state} {postcode}",
                )

                # Create the transformed location
                location = TransformedLocation(
                    brand=brand,
                    business_name=business_name,
                    street_address=street_address,
                    suburb=suburb,
                    state=state,
                    postcode=postcode,
                    drive_thru=drive_thru,
                    shopping_centre_name=shopping_centre_name,
                    source_url=source_url,
                    source="redrooster",
                    business_id=business_id,
                )

                # Only include the base model data
                transformed_items.append(location.model_dump())

            except Exception as e:
                logger.error(f"Error transforming Red Rooster item: {e}", exc_info=True)
                continue

        logger.info(f"Transformed {len(transformed_items)} Red Rooster items")
        return transformed_items

    def _clean_street_address(self, street_address: str) -> str:
        """Clean and normalize street address."""
        if not street_address:
            return ""

        # Remove extra whitespace and normalize
        address = re.sub(r'\s+', ' ', street_address.strip())
        
        # Remove leading/trailing commas
        address = address.strip(',').strip()
        
        return address

    def _clean_suburb(self, suburb: str) -> str:
        """Clean and normalize suburb name."""
        if not suburb:
            return ""

        # Convert to title case and clean
        suburb = suburb.strip().title()
        
        # Handle special cases
        suburb = re.sub(r'\bMc([a-z])', r'Mc\1', suburb)  # McDonald -> McDonald
        suburb = re.sub(r'\bSt\b', 'St', suburb)  # St -> St (keep as is)
        
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

    def _extract_shopping_centre_name(self, street_address: str) -> Optional[str]:
        """
        Extract shopping centre name from street address.
        
        Args:
            street_address: Street address string
            
        Returns:
            Shopping centre name or None if not found
        """
        if not street_address:
            return None

        # Common shopping centre patterns
        shopping_centre_patterns = [
            r'(.+?)\s*Shopping\s*Centre',
            r'(.+?)\s*Shopping\s*Center',
            r'(.+?)\s*Plaza',
            r'(.+?)\s*Mall',
            r'(.+?)\s*Square',
            r'(.+?)\s*Centre(?!\s+Shopping)',  # Centre but not followed by Shopping
            r'Westfield\s+(.+?)(?:\s|$)',
            r'(.+?)\s*Food\s*Court',
            r'(.+?)\s*Marketplace',
            r'(.+?)\s*Village',
            r'(.+?)\s*Arcade',
            r'(.+?)\s*Emporium',
        ]

        for pattern in shopping_centre_patterns:
            match = re.search(pattern, street_address, re.IGNORECASE)
            if match:
                centre_name = match.group(1).strip()
                
                # Clean up the centre name
                centre_name = self._clean_shopping_centre_name(centre_name)
                
                # Skip if it's too short or looks like a street name
                if len(centre_name) < 3 or self._looks_like_street_name(centre_name):
                    continue
                    
                return centre_name

        return None

    def _clean_shopping_centre_name(self, name: str) -> str:
        """Clean shopping centre name by removing unwanted prefixes/suffixes."""
        if not name:
            return ""

        # Remove common prefixes that shouldn't be part of the name
        prefixes_to_remove = [
            r'^Shop\s+\d+[A-Za-z]?\s*,?\s*',
            r'^Unit\s+\d+[A-Za-z]?\s*,?\s*',
            r'^Level\s+\d+\s*,?\s*',
            r'^Floor\s+\d+\s*,?\s*',
            r'^Tenancy\s+\d+[A-Za-z]?\s*,?\s*',
            r'^Kiosk\s+\d+[A-Za-z]?\s*,?\s*',
            r'^\d+[A-Za-z]?\s*,?\s*',  # Just numbers
        ]

        cleaned = name
        for prefix_pattern in prefixes_to_remove:
            cleaned = re.sub(prefix_pattern, '', cleaned, flags=re.IGNORECASE)

        # Remove trailing commas and extra whitespace
        cleaned = cleaned.strip().rstrip(',').strip()

        return cleaned

    def _looks_like_street_name(self, name: str) -> bool:
        """Check if the name looks like a street name rather than a shopping centre."""
        street_indicators = [
            r'\b(Street|St|Road|Rd|Avenue|Ave|Drive|Dr|Lane|Ln|Boulevard|Blvd|Highway|Hwy|Parade|Pde|Crescent|Cres|Close|Cl|Place|Pl|Way|Circuit|Cct)\b'
        ]
        
        for pattern in street_indicators:
            if re.search(pattern, name, re.IGNORECASE):
                return True
                
        return False

    def generate_business_id(self, name: str, address: str) -> str:
        """Generates a simple unique ID based on name and address."""
        data_string = f"{name.lower().strip()}|{address.lower().strip()}"
        return hashlib.sha1(data_string.encode("utf-8")).hexdigest()
