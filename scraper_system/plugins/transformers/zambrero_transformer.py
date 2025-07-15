import logging
import hashlib
import re
from typing import List, Dict, Any, Optional

from scraper_system.interfaces.transformer_interface import TransformerInterface
from scraper_system.models.location import TransformedLocation

logger = logging.getLogger(__name__)


class ZambreroTransformer(TransformerInterface):
    """
    Transformer for Zambrero data.
    Responsible for converting raw scraped data into standardized format.
    """

    async def transform(
        self, data: List[Dict[str, Any]], config: Dict[str, Any], site_name: str
    ) -> List[Dict[str, Any]]:
        """
        Transforms raw Zambrero location data from parser.

        Args:
            data: List of raw location items from Zambrero parser
            config: Transformation configuration
            site_name: Source name (should be 'zambrero')

        Returns:
            List of transformed location dictionaries
        """
        if not data:
            logger.warning("No data to transform for Zambrero")
            return []

        transformed_items = []

        for item in data:
            try:
                # Extract basic information
                brand = item.get("brand", "Zambrero")
                business_name = item.get("business_name", "")
                source_url = item.get("source_url", "")

                # Extract address components
                street_address = item.get("street_address", "")
                suburb = item.get("suburb", "")
                state = item.get("state", "")
                postcode = item.get("postcode", "")

                # Clean and process address components
                street_address = self._clean_street_address(street_address)
                suburb = self._clean_suburb(suburb)
                state = self._clean_state(state)
                postcode = self._clean_postcode(postcode)

                # Skip if essential data is missing
                if not street_address or not suburb or not state or not postcode:
                    logger.warning(f"Skipping item with missing essential data: {business_name}")
                    continue

                # Extract shopping centre name from street address if present
                shopping_centre_name = self._extract_shopping_centre_name(street_address)

                # Determine drive-thru status
                drive_thru = item.get("drive_thru", False)

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
                    source="zambrero",
                    business_id=business_id,
                )

                # Only include the base model data
                transformed_items.append(location.model_dump())

            except Exception as e:
                logger.error(f"Error transforming Zambrero item: {e}", exc_info=True)
                continue

        logger.info(f"Transformed {len(transformed_items)} Zambrero items")
        return transformed_items

    def _clean_street_address(self, address: str) -> str:
        """Clean and normalize street address."""
        if not address:
            return ""

        # Remove extra whitespace and normalize
        address = re.sub(r'\s+', ' ', address.strip())

        # Normalize common abbreviations
        abbreviations = {
            r'\bStreet\b': 'St',
            r'\bRoad\b': 'Rd',
            r'\bAvenue\b': 'Ave',
            r'\bBoulevard\b': 'Blvd',
            r'\bDrive\b': 'Dr',
            r'\bLane\b': 'Ln',
            r'\bCrescent\b': 'Cres',
            r'\bPlace\b': 'Pl',
            r'\bTerrace\b': 'Tce',
            r'\bCircuit\b': 'Cct',
            r'\bHighway\b': 'Hwy',
        }

        for full_form, abbrev in abbreviations.items():
            address = re.sub(full_form, abbrev, address, flags=re.IGNORECASE)

        return address.strip()

    def _clean_suburb(self, suburb: str) -> str:
        """Clean and normalize suburb name from restaurant name."""
        if not suburb:
            return ""

        # Remove extra whitespace and normalize
        suburb = re.sub(r'\s+', ' ', suburb.strip())

        # Handle common location name patterns that aren't actual suburbs
        # Remove common descriptors that appear in restaurant names
        patterns_to_clean = [
            r'\s*-\s*(Opening Soon|Temporarily.*|Closed).*$',  # Status indicators
            r'\s+(DT|Drive.?Thru?).*$',  # Drive-thru indicators
            r'\s+(Northbound|Southbound|East|West|North|South)\s*$',  # Directional indicators
        ]

        for pattern in patterns_to_clean:
            suburb = re.sub(pattern, '', suburb, flags=re.IGNORECASE)

        # Convert to title case for consistency
        suburb = suburb.title()

        # Handle special cases for abbreviations and proper nouns
        special_cases = {
            'Cbd': 'CBD',
            'Nsw': 'NSW',
            'Qld': 'QLD',
            'Sa': 'SA',
            'Wa': 'WA',
            'Vic': 'VIC',
            'Act': 'ACT',
            'Nt': 'NT',
            'Tas': 'TAS',
            'Anu': 'ANU',  # Australian National University
            'Dfo': 'DFO',  # Direct Factory Outlets
            'Mt ': 'Mount ',  # Mount abbreviation
        }

        for old, new in special_cases.items():
            suburb = re.sub(r'\b' + old + r'\b', new, suburb)

        return suburb.strip()

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
            r'(.+?(?:Westfield|Centro|DFO|Stockland))',
            r'(.+?(?:Village|Square|Arcade|Central))',
        ]

        for pattern in shopping_centre_patterns:
            match = re.search(pattern, address, re.IGNORECASE)
            if match:
                centre_name = match.group(1).strip()
                # Clean up common prefixes like "Shop 123, "
                centre_name = re.sub(
                    r'^(?:Shop\s*\d+[A-Za-z]*,?\s*|Level\s*\d+,?\s*|Unit\s*\d+[A-Za-z]*,?\s*|T\d+,?\s*)',
                    '',
                    centre_name,
                    flags=re.IGNORECASE
                )
                centre_name = centre_name.strip().strip(',').strip()

                # Only return if it's substantial enough
                if len(centre_name) > 3:
                    return centre_name

        return None

    def generate_business_id(self, name: str, address: str) -> str:
        """Generates a unique ID based on name and address."""
        data_string = f"{name.lower().strip()}|{address.lower().strip()}"
        return hashlib.sha1(data_string.encode("utf-8")).hexdigest()
