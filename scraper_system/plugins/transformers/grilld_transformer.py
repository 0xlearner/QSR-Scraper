import logging
import hashlib
import re
from typing import List, Dict, Any, Optional

from scraper_system.interfaces.transformer_interface import TransformerInterface
from scraper_system.models.location import TransformedLocation

logger = logging.getLogger(__name__)


class GrilldTransformer(TransformerInterface):
    """
    Transformer for Grill'd data.
    Responsible for parsing address components and transforming raw data into final format.
    """

    async def transform(
        self, data: List[Dict[str, Any]], config: Dict[str, Any], site_name: str
    ) -> List[Dict[str, Any]]:
        """
        Transforms raw scraped data from Grill'd parser.

        Args:
            data: List of raw scraped items with basic data
            config: Transformation configuration
            site_name: Source name (should be 'grilld')

        Returns:
            List of transformed location dictionaries
        """
        if not data:
            logger.warning("No data to transform for Grill'd")
            return []

        transformed_items = []

        for item in data:
            try:
                # Extract raw data
                brand = item.get("brand", "")
                name = item.get("business_name", "")
                address = item.get("address", "")  # Raw address string
                street_address = item.get("street_address", "")
                drive_thru = item.get("drive_thru", False)
                source_url = item.get("source_url", "")

                # Parse address components if we have a raw address
                if address and not street_address:
                    address_components = self._parse_address_components(address)
                    street_address = address_components.get("street_address", "")
                    suburb = address_components.get("suburb", "")
                    state = address_components.get("state", "")
                    postcode = address_components.get("postcode", "")
                    shopping_centre_name = address_components.get(
                        "shopping_centre_name"
                    )
                else:
                    # Use provided components if no raw address parsing needed
                    suburb = item.get("suburb", "")
                    state = item.get("state", "")
                    postcode = item.get("postcode", "")
                    shopping_centre_name = item.get("shopping_centre_name")

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
                    source="grilld",
                    business_id=business_id,
                )

                transformed_items.append(location.model_dump())

            except Exception as e:
                logger.error(f"Error transforming Grill'd item: {e}", exc_info=True)

        logger.info(f"Transformed {len(transformed_items)} Grill'd items")
        return transformed_items

    def _extract_state_postcode(self, parts: List[str]) -> Dict[str, str]:
        """Extract state and postcode from address parts."""
        result = {"state": None, "postcode": None}

        if not parts:
            return result

        last_part = parts[-1].strip()
        state_part = last_part.split()

        if state_part:
            result["state"] = state_part[0]  # Take first word as state

            # Extract postcode if present (4 digits after state)
            if (
                len(state_part) > 1
                and state_part[1].isdigit()
                and len(state_part[1]) == 4
            ):
                result["postcode"] = state_part[1]

        return result

    def _extract_suburb(self, parts: List[str]) -> Optional[str]:
        """Extract suburb from address parts."""
        if len(parts) < 2:
            return None

        # Look backward from the end (excluding last part which is state)
        for i in range(len(parts) - 1, -1, -1):
            if i == len(parts) - 1:  # Skip the state part
                continue

            part = parts[i].strip()
            # Look for UPPERCASE suburb or suburb before state
            if part.isupper() or (i == len(parts) - 2):
                return part

        return None

    def _extract_shopping_centre(self, parts: List[str]) -> Optional[Dict[str, Any]]:
        """Extract shopping centre name from address parts."""
        shopping_centre_keywords = [
            "Shopping Centre",
            "Plaza",
            "Mall",
            "Centre",
        ]

        for i, part in enumerate(parts):
            if any(
                keyword.lower() in part.lower() for keyword in shopping_centre_keywords
            ):
                result = part.strip()
                # If next part continues the shopping center name, include it
                if i + 1 < len(parts) and (
                    "level" in parts[i + 1].lower() or "shop" in parts[i + 1].lower()
                ):
                    result += f", {parts[i+1].strip()}"
                return {"name": result, "index": i}

        return None

    def _extract_street_address(
        self,
        parts: List[str],
        suburb: Optional[str],
        shopping_centre: Optional[Dict[str, Any]],
    ) -> Optional[str]:
        """Extract street address from address parts."""
        street_parts = []

        for i, part in enumerate(parts):
            # Stop when we reach suburb or state
            if (suburb and part.strip() == suburb) or (i == len(parts) - 1):
                break

            # Skip shopping center parts
            if shopping_centre and (
                part.strip() == shopping_centre["name"]
                or (
                    i + 1 < len(parts)
                    and f"{part.strip()}, {parts[i+1].strip()}"
                    == shopping_centre["name"]
                )
            ):
                continue

            street_parts.append(part.strip())

        return ", ".join(street_parts) if street_parts else None

    def _process_street_in_shopping_centre(
        self, shopping_centre_name: str
    ) -> Dict[str, str]:
        """Extract street address from shopping center name if present."""
        result = {"street_address": None, "shopping_centre_name": shopping_centre_name}

        # Look for patterns like "123 Street Name" in the shopping center name
        street_address_match = re.search(
            r"(\d+\s+[A-Za-z\s]+(?:Street|St|Road|Rd|Avenue|Ave|Highway|Hwy|Drive|Dr))",
            shopping_centre_name,
        )

        if street_address_match:
            result["street_address"] = street_address_match.group(1)

            # Remove the street address from the shopping center name
            updated_name = shopping_centre_name.replace(
                result["street_address"], ""
            ).strip()

            # Clean up
            updated_name = re.sub(r",\s*$", "", updated_name)  # Remove trailing commas
            updated_name = re.sub(r"\s+", " ", updated_name).strip()  # Normalize spaces

            result["shopping_centre_name"] = updated_name

        return result

    def _parse_address_components(self, address: str) -> Dict[str, Any]:
        """Parse address string to extract components."""
        components = {
            "street_address": None,
            "suburb": None,
            "state": None,
            "postcode": None,
            "shopping_centre_name": None,
        }

        if not address:
            return components

        # Split by commas
        parts = [p.strip() for p in address.split(",")]

        # Extract components
        state_postcode = self._extract_state_postcode(parts)
        components["state"] = state_postcode["state"]
        components["postcode"] = state_postcode["postcode"]

        components["suburb"] = self._extract_suburb(parts)

        shopping_centre = self._extract_shopping_centre(parts)
        if shopping_centre:
            components["shopping_centre_name"] = shopping_centre["name"]

        components["street_address"] = self._extract_street_address(
            parts, components["suburb"], shopping_centre
        )

        # Handle special case: street address in shopping center name
        if components["shopping_centre_name"] and not components["street_address"]:
            result = self._process_street_in_shopping_centre(
                components["shopping_centre_name"]
            )
            components["street_address"] = result["street_address"]
            components["shopping_centre_name"] = result["shopping_centre_name"]

        return components

    def generate_business_id(self, name: str, address: str) -> str:
        """Generates a simple unique ID based on name and address."""
        data_string = f"{name.lower().strip()}|{address.lower().strip()}"
        return hashlib.sha1(data_string.encode("utf-8")).hexdigest()
