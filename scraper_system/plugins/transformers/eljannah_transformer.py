import logging
import hashlib
from typing import List, Dict, Any

from scraper_system.interfaces.transformer_interface import TransformerInterface
from scraper_system.models.location import TransformedLocation

logger = logging.getLogger(__name__)


class EljannahTransformer(TransformerInterface):
    """
    Transformer for El Jannah data.
    Responsible for parsing address components and transforming raw data into final format.
    """

    async def transform(
        self, data: List[Dict[str, Any]], config: Dict[str, Any], site_name: str
    ) -> List[Dict[str, Any]]:
        """
        Transforms raw scraped data from ElJannah parser.

        Args:
            data: List of raw scraped items with basic data
            config: Transformation configuration
            site_name: Source name (should be 'eljannah')

        Returns:
            List of transformed location dictionaries
        """
        if not data:
            logger.warning("No data to transform for El Jannah")
            return []

        transformed_items = []

        for item in data:
            try:
                # Extract raw data
                brand = item.get("brand", "")
                name = item.get("business_name", "")
                street_address = item.get("street_address", "")
                drive_thru = item.get("drive_thru", False)
                source_url = item.get("source_url", "")

                # Parse address to extract components
                address_components = self._parse_address_components(street_address)

                # Get state from URL if available, otherwise use from raw data
                state = None
                if source_url:
                    url_parts = source_url.split("/")
                    if len(url_parts) >= 4:
                        state = url_parts[-3].upper()

                # If state wasn't found in URL, use what was passed
                if not state:
                    state = item.get("state", "")

                # Use suburb from raw data or extracted components
                suburb = item.get("suburb", "").upper()
                postcode = item.get("postcode", "")
                shopping_centre_name = address_components.get("shopping_centre_name")

                # Use the extracted street address from components
                parsed_street_address = address_components.get(
                    "street_address", street_address
                )

                # Generate business ID
                business_id = self.generate_business_id(
                    name,
                    f"{parsed_street_address}, {suburb} {state} {postcode}",
                )

                # Create the transformed location
                location = TransformedLocation(
                    brand=brand,
                    business_name=name,
                    street_address=parsed_street_address,
                    suburb=suburb,
                    state=state,
                    postcode=postcode,
                    drive_thru=drive_thru,
                    shopping_centre_name=shopping_centre_name,
                    source_url=source_url,
                    source="eljannah",
                    business_id=business_id,
                )

                transformed_items.append(location.model_dump())

            except Exception as e:
                logger.error(f"Error transforming El Jannah item: {e}", exc_info=True)

        logger.info(f"Transformed {len(transformed_items)} El Jannah items")
        return transformed_items

    def _parse_address_components(self, street_address: str) -> Dict[str, str]:
        """Parse street address to extract shopping center name and actual street address."""
        components = {"street_address": street_address, "shopping_centre_name": None}

        # Check for shopping center patterns
        shopping_centre_keywords = [
            "Centre",
            "Plaza",
            "Mall",
            "Square",
            "Arcade",
            "Food Court",
        ]

        # Split by comma to separate potential shopping center from street address
        parts = [p.strip() for p in street_address.split(",")]

        if len(parts) > 1:
            # Check if first part contains shopping center keywords
            if any(keyword in parts[0] for keyword in shopping_centre_keywords):
                components["shopping_centre_name"] = parts[0]
                components["street_address"] = ", ".join(parts[1:])
            # Check for pattern like "Pacific Centre Tuggerah, 144-148 Pacific Highway"
            elif any(
                keyword in parts[0].split() for keyword in shopping_centre_keywords
            ):
                components["shopping_centre_name"] = parts[0]
                components["street_address"] = ", ".join(parts[1:])

        return components

    def generate_business_id(self, name: str, address: str) -> str:
        """Generates a simple unique ID based on name and address."""
        data_string = f"{name.lower().strip()}|{address.lower().strip()}"
        return hashlib.sha1(data_string.encode("utf-8")).hexdigest()
