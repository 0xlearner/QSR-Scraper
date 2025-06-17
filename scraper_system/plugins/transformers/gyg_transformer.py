import logging
import hashlib
import re
import pandas as pd
from typing import List, Dict, Any, Optional

from scraper_system.interfaces.transformer_interface import TransformerInterface
from scraper_system.models.location import TransformedLocation

logger = logging.getLogger(__name__)


class GygTransformer(TransformerInterface):
    """
    Transformer for Guzman y Gomez (GYG) data.
    Responsible for parsing address components and transforming raw data into final format.
    """

    def _get_shopping_centre_patterns(self) -> List[str]:
        """Return patterns to identify shopping centers"""
        return [
            r"([\w\s]+Shopping Centre)",
            r"([\w\s]+Plaza)",
            r"([\w\s]+Square)",
            r"([\w\s]+Centre(?!\s+Shopping))",  # Centre but not followed by Shopping
            r"([\w\s]+Mall)",
            r"(Westfield[\s\w]+)",
            r"([\w\s]+Village)",
            r"([\w\s]+Emporium)",
            r"([\w\s]+Campus)",
            r"(Castle Towers)",
            r"(The Concourse)",
            r"(Manly Wharf)",
            r"(Harbour Town)",
            r"(The Oracle)",
            r"([\w\s]+Food Court)",
            r"(Paradise Centre)",
            r"([\w\s]+Waterside)",
        ]

    def _get_non_shopping_centers(self) -> List[str]:
        """Return non-shopping center locations"""
        return [
            "Martin Place",
            "Post Office Square",
            "formerly the MLC Centre",
            "Central",
            "Swanston St",
            "Hay St",
            "St James",
            "King William St",
            "Lonsdale St",
            "Alma",
            "Brunswick St",
            "Elizabeth St",
        ]

    def _get_known_centers(self) -> Dict[str, str]:
        """Return mapping of known shopping centers"""
        return {
            "Guzman Y Gomez Broadway": "Broadway Shopping Centre",
            "Guzman Y Gomez Castle Towers": "Castle Towers Shopping Centre",
            "Guzman Y Gomez Charlestown Square": "Charlestown Square Shopping Centre",
            "Guzman Y Gomez Kotara": "Kotara Home Centre",
            "Guzman Y Gomez Lake Haven": "Lake Haven Shopping Centre",
            "Guzman Y Gomez Frenchs Forest": "Forrestway Shopping Centre",
            "Guzman Y Gomez Kings Cross": "Kings Cross Centre",
            "Guzman Y Gomez Fountain Plaza": "Fountain Plaza",
            "Guzman Y Gomez MacArthur Square": "Macarthur Square Shopping Centre",
            "Guzman Y Gomez Westfield Parramatta": "Westfield Parramatta",
            "Guzman Y Gomez Westfield Sydney": "Westfield Sydney",
            "Guzman Y Gomez Wollongong Central": "Wollongong Central Shopping Centre",
            "Guzman Y Gomez World Square": "World Square Shopping Centre",
            "Guzman Y Gomez Post Office Square": "",
            "Guzman Y Gomez Fortitude Valley Emporium": "Fortitude Valley Emporium",
            "Guzman Y Gomez Fortitude Valley Metro": "Valley Metro",
            "Guzman Y Gomez Griffith University": "Griffith University",
            "Guzman Y Gomez Collins Place": "Collins Place",
            "Guzman Y Gomez Canberra Centre": "Canberra Centre Shopping Centre",
            "Guzman Y Gomez Westfield Belconnen": "Westfield Belconnen",
            "Guzman Y Gomez Rhodes": "Rhodes Waterside Shopping Centre",
            "Guzman Y Gomez Shellharbour": "Stockland Shellharbour Shopping Centre",
            "Guzman Y Gomez Wetherill Park": "Greenway Plaza",
            "Guzman Y Gomez Browns Plains": "Grand Plaza",
            "Guzman Y Gomez Buranda": "Buranda Village",
            "Guzman Y Gomez Gladstone": "Gladstone Centre",
            "Guzman Y Gomez Logan Hyperdome": "Logan Hyperdome Shopping Centre",
            "Guzman Y Gomez Surfers Paradise": "Paradise Centre",
            "Guzman Y Gomez Martin Place": "",
            "Guzman Y Gomez Robina Town Centre": "Robina Centre Shopping Centre",
            "Guzman Y Gomez Victoria Point": "Victoria Point Shopping Centre",
            "Guzman Y Gomez Cranbourne": "Cranbourne Homemaker Centre",
            "Guzman Y Gomez Warnbro": "Warnbro Centre",
        }

    def _extract_shopping_centre_name(self, address: str, patterns: List[str]) -> str:
        """Extract shopping centre name from address using patterns"""
        shopping_centre_name = ""
        for pattern in patterns:
            matches = re.findall(pattern, address)
            for match in matches:
                if match and len(match) > len(shopping_centre_name):
                    # Clean up the match - remove street numbers, street names, etc.
                    cleaned_match = re.sub(
                        r"^\d+[\-\/]?\d*\s+", "", match
                    )  # Remove leading numbers
                    cleaned_match = re.sub(
                        r"^\d+[A-Za-z]?\s+", "", cleaned_match
                    )  # Remove numbers with letter suffix

                    # Remove street names with common suffixes
                    street_suffixes = [
                        "St",
                        "Rd",
                        "Ave",
                        "Dr",
                        "Ln",
                        "Blvd",
                        "Hwy",
                        "Pde",
                        "Cres",
                        "Cl",
                        "Pl",
                        "Way",
                        "Drive",
                        "Street",
                        "Road",
                        "Avenue",
                    ]
                    for suffix in street_suffixes:
                        cleaned_match = re.sub(
                            rf"^\w+\s+{suffix}\s+", "", cleaned_match
                        )

                    shopping_centre_name = cleaned_match.strip()
        return shopping_centre_name

    async def transform(
        self, data: List[Dict[str, Any]], config: Dict[str, Any], site_name: str
    ) -> List[Dict[str, Any]]:
        """
        Transforms raw scraped data from GYG parser.

        Args:
            data: List of raw scraped items with basic data
            config: Transformation configuration
            site_name: Source name (should be 'gyg')

        Returns:
            List of transformed location dictionaries
        """
        if not data:
            logger.warning("No data to transform for GYG")
            return []

        # Process using pandas for complex address parsing
        if any("raw_address" in item for item in data):
            processed_items = self._process_locations(data)
        else:
            # Fall back to simple item-by-item processing if no raw addresses
            processed_items = []
            for item in data:
                try:
                    # Extract raw data with defaults for missing fields
                    brand = item.get("brand", "Guzman Y Gomez")
                    name = item.get("business_name", "")
                    raw_address = item.get("raw_address", "")
                    street_address = item.get("street_address", "")
                    drive_thru = item.get("drive_thru", False)
                    source_url = item.get("source_url", "")

                    # Parse address components if we have a raw address
                    address_components = {}
                    if raw_address:
                        address_components = self._parse_address_components(raw_address)

                    # Use provided components or parsed ones
                    final_street_address = street_address or address_components.get(
                        "street_address", ""
                    )
                    suburb = item.get("suburb", "") or address_components.get(
                        "suburb", ""
                    )
                    state = item.get("state", "") or address_components.get("state", "")
                    postcode = item.get("postcode", "") or address_components.get(
                        "postcode", ""
                    )
                    shopping_centre_name = item.get(
                        "shopping_centre_name"
                    ) or address_components.get("shopping_centre_name", "")

                    # Add to processed items
                    processed_items.append(
                        {
                            "brand": brand,
                            "business_name": name,
                            "street_address": final_street_address,
                            "suburb": suburb,
                            "state": state,
                            "postcode": postcode,
                            "drive_thru": drive_thru,
                            "shopping_centre_name": shopping_centre_name,
                            "source_url": source_url,
                            "source": "gyg",
                        }
                    )

                except Exception as e:
                    logger.error(f"Error transforming GYG item: {e}", exc_info=True)

        # Final transformation with business ID generation
        transformed_items = []
        for item in processed_items:
            try:
                # Generate business ID
                business_id = self.generate_business_id(
                    item.get("business_name", ""),
                    f"{item.get('street_address', '')}, {item.get('suburb', '')} {item.get('state', '')} {item.get('postcode', '')}",
                )

                # Create the transformed location with default values for missing fields
                location = TransformedLocation(
                    brand=item.get(
                        "brand", "Guzman Y Gomez"
                    ),  # Default brand if missing
                    business_name=item.get("business_name", ""),
                    street_address=item.get("street_address", ""),
                    suburb=item.get("suburb", ""),
                    state=item.get("state", ""),
                    postcode=item.get("postcode", ""),
                    drive_thru=item.get("drive_thru", False),
                    shopping_centre_name=item.get("shopping_centre_name", ""),
                    source_url=item.get("source_url", ""),
                    source=item.get("source", "gyg"),
                    business_id=business_id,
                )

                transformed_items.append(location.model_dump())

            except Exception as e:
                logger.error(
                    f"Error creating TransformedLocation for GYG item: {e}",
                    exc_info=True,
                )

        logger.info(f"Transformed {len(transformed_items)} GYG items")
        return transformed_items

    def _initialize_dataframe(self, items: List[Dict[str, Any]]) -> pd.DataFrame:
        """Initialize dataframe with required columns."""
        # Convert to dataframe for easier processing
        df = pd.DataFrame(items)

        # Create new columns for the parsed data if they don't exist
        required_columns = [
            "suburb",
            "state",
            "postcode",
            "shopping_centre_name",
            "street_address",
        ]
        for column in required_columns:
            if column not in df.columns:
                df[column] = ""

        return df

    def _extract_state_postcode_suburb(
        self, address: str, index: int, df: pd.DataFrame
    ) -> None:
        """Extract state, postcode, and suburb from an address."""
        # Fix duplicate state codes (like "VIC VIC")
        address = re.sub(r"([A-Z]{2,3})\s+\1", r"\1", address)

        # Extract state and postcode using regex
        state_postcode_match = re.search(r"([A-Z]{2,3})\s+(\d{4})", address)
        if state_postcode_match:
            df.at[index, "state"] = state_postcode_match.group(1)
            df.at[index, "postcode"] = state_postcode_match.group(2)

            # Extract suburb (usually before state and postcode)
            pre_state_part = address.split(state_postcode_match.group(0))[0].strip()
            suburb_match = re.search(r",\s*([^,]+)$", pre_state_part)
            if suburb_match:
                suburb = suburb_match.group(1).strip()
                # Remove Shop or Tenancy info if present in suburb
                suburb = re.sub(
                    r"(Shop|Tenancy|Floor|Level|Unit).*$", "", suburb
                ).strip()
                df.at[index, "suburb"] = suburb.upper()

    def _process_shopping_centre(
        self, address: str, location_name: str, index: int, df: pd.DataFrame
    ) -> None:
        """Process shopping centre information for a location."""
        known_centers = self._get_known_centers()

        # Check if there's a known shopping center for this location
        if location_name in known_centers:
            df.at[index, "shopping_centre_name"] = known_centers[location_name]
            return

        # Check for shopping center in address
        shopping_centre_patterns = self._get_shopping_centre_patterns()
        non_shopping_centers = self._get_non_shopping_centers()
        shopping_centre_name = self._extract_shopping_centre_name(
            address, shopping_centre_patterns
        )

        # Check if the extracted name should be excluded
        if any(
            non_center in shopping_centre_name for non_center in non_shopping_centers
        ):
            shopping_centre_name = ""

        # Special case for addresses with "Stockland"
        if "Stockland" in address and not shopping_centre_name:
            stockland_match = re.search(
                r"Stockland\s+([\w\s]+)(?:Shopping\s+Centre)?", address
            )
            if stockland_match:
                shopping_centre_name = f"Stockland {stockland_match.group(1)}".strip()
                if "Shopping Centre" not in shopping_centre_name:
                    shopping_centre_name += " Shopping Centre"

        df.at[index, "shopping_centre_name"] = shopping_centre_name

    def _extract_street_address(
        self, address: str, index: int, df: pd.DataFrame, shopping_centre_name: str
    ) -> None:
        """Extract street address from the full address."""
        if df.at[index, "street_address"]:
            return

        # Try to extract from raw address
        parts = [p.strip() for p in address.split(",")]
        street_parts = []
        for i, part in enumerate(parts):
            # Stop when we reach suburb or state
            if (df.at[index, "suburb"] and part.strip() == df.at[index, "suburb"]) or (
                df.at[index, "state"] and part.strip().startswith(df.at[index, "state"])
            ):
                break
            # Skip shopping center parts
            if shopping_centre_name and part.strip() in shopping_centre_name:
                continue
            street_parts.append(part.strip())

        if street_parts:
            df.at[index, "street_address"] = ", ".join(street_parts)

    def _clean_shopping_centre_names(self, df: pd.DataFrame) -> None:
        """Clean shopping centre names in the dataframe."""
        for index, row in df.iterrows():
            center_name = row["shopping_centre_name"]
            if center_name:
                # Remove any numbers at the beginning
                center_name = re.sub(r"^\d+\s+", "", center_name)
                # Remove anything that looks like a street address
                center_name = re.sub(
                    r"^\d+[A-Za-z]?\s+[A-Za-z]+\s+(St|Rd|Ave|Dr|Blvd|Hwy)\s+",
                    "",
                    center_name,
                )
                df.at[index, "shopping_centre_name"] = center_name

    def _fill_missing_suburbs(self, df: pd.DataFrame) -> None:
        """Fill in missing suburbs using business name."""
        for index, row in df.iterrows():
            if not row["suburb"]:
                df.at[index, "suburb"] = (
                    row["business_name"].replace("Guzman Y Gomez ", "").upper()
                )

    def _convert_to_list_of_dicts(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Convert dataframe to list of dictionaries with selected fields."""
        processed_items = []
        for _, row in df.iterrows():
            processed = {
                k: v
                for k, v in row.items()
                if k
                in [
                    "business_name",
                    "street_address",
                    "suburb",
                    "state",
                    "postcode",
                    "drive_thru",
                    "shopping_centre_name",
                    "source_url",
                    "source",
                ]
            }
            processed_items.append(processed)
        return processed_items

    def _process_locations(self, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Process raw locations to extract detailed address components
        """
        if not items:
            return []

        # Initialize the dataframe
        df = self._initialize_dataframe(items)

        # Process each address
        for index, row in df.iterrows():
            if "raw_address" in row and row["raw_address"]:
                address = row["raw_address"]
                location_name = row["business_name"]

                # Extract state, postcode and suburb
                self._extract_state_postcode_suburb(address, index, df)

                # Process shopping centre
                self._process_shopping_centre(address, location_name, index, df)

                # Extract street address
                shopping_centre_name = df.at[index, "shopping_centre_name"]
                self._extract_street_address(address, index, df, shopping_centre_name)

        # Additional processing
        self._fill_missing_suburbs(df)
        self._clean_shopping_centre_names(df)

        # Convert to final format
        return self._convert_to_list_of_dicts(df)

    def _extract_state_postcode(self, parts: List[str]) -> Dict[str, Optional[str]]:
        """Extract state and postcode from address parts."""
        result = {"state": None, "postcode": None, "suburb": None}

        if not parts:
            return result

        # Common Australian state abbreviations
        states = ["NSW", "VIC", "QLD", "SA", "WA", "TAS", "NT", "ACT"]

        last_part = parts[-1].strip()
        state_postcode_match = re.search(r"([A-Z]{2,3})\s+(\d{4})", last_part)

        if state_postcode_match:
            state_candidate = state_postcode_match.group(1)
            if state_candidate in states:
                result["state"] = state_candidate
                result["postcode"] = state_postcode_match.group(2)

                # If we found state and postcode, the suburb is likely the part before
                if len(parts) > 1:
                    suburb_candidate = parts[-2].strip()
                    # Suburbs are often in ALL CAPS
                    if suburb_candidate.isupper() or suburb_candidate.istitle():
                        result["suburb"] = suburb_candidate

        return result

    def _find_shopping_centre(self, parts: List[str]) -> Optional[Dict[str, Any]]:
        """Find shopping centre name in the address parts."""
        # Look for shopping center keywords
        shopping_centre_keywords = [
            "Shopping Centre",
            "Plaza",
            "Mall",
            "Square",
            "Arcade",
            "Food Court",
            "Centre",
            "Center",
            "Westfield",
        ]

        for i, part in enumerate(parts):
            if any(
                keyword.lower() in part.lower() for keyword in shopping_centre_keywords
            ):
                name = part
                # Check if next part is related (e.g., "Level 1")
                if i + 1 < len(parts) and re.search(
                    r"(level|shop|unit)", parts[i + 1].lower()
                ):
                    name += f", {parts[i+1]}"
                return {"name": name, "index": i}

        return None

    def _extract_street_parts(
        self,
        parts: List[str],
        components: Dict[str, Any],
        shopping_centre: Optional[Dict[str, Any]],
    ) -> List[str]:
        """Extract street address parts from the address."""
        street_parts = []

        for i, part in enumerate(parts):
            # Stop if we reach the suburb, state or shopping center
            if (
                part == components.get("suburb")
                or part == components.get("state")
                or (shopping_centre and part == shopping_centre["name"])
            ):
                break

            # Skip if this part is already included in shopping center name
            if shopping_centre and shopping_centre["name"].startswith(part):
                continue

            street_parts.append(part)

        return street_parts

    def _extract_street_from_shopping_centre(
        self, shopping_centre_name: str
    ) -> Dict[str, Optional[str]]:
        """Try to extract street address from shopping centre name."""
        result = {"street_address": None, "shopping_centre_name": shopping_centre_name}

        street_match = re.search(
            r"(\d+[A-Za-z\s\-]+(?:Street|St|Road|Rd|Avenue|Ave|Highway|Hwy|Drive|Dr))",
            shopping_centre_name,
        )

        if street_match:
            result["street_address"] = street_match.group(1)
            # Remove the street address from shopping center name
            result["shopping_centre_name"] = shopping_centre_name.replace(
                result["street_address"], ""
            ).strip()
            # Clean up
            result["shopping_centre_name"] = re.sub(
                r"^,\s*|\s*,\s*$", "", result["shopping_centre_name"]
            )

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

        # Extract state, postcode and possibly suburb
        state_result = self._extract_state_postcode(parts)
        components["state"] = state_result["state"]
        components["postcode"] = state_result["postcode"]
        components["suburb"] = state_result["suburb"]

        # Find shopping centre
        shopping_centre = self._find_shopping_centre(parts)
        if shopping_centre:
            components["shopping_centre_name"] = shopping_centre["name"]

        # Extract street address
        street_parts = self._extract_street_parts(parts, components, shopping_centre)
        if street_parts:
            components["street_address"] = ", ".join(street_parts)

        # If no street address found but we have a shopping center, see if it contains a street address
        if not components["street_address"] and components["shopping_centre_name"]:
            street_result = self._extract_street_from_shopping_centre(
                components["shopping_centre_name"]
            )
            components["street_address"] = street_result["street_address"]
            components["shopping_centre_name"] = street_result["shopping_centre_name"]

        return components

    def generate_business_id(self, name: str, address: str) -> str:
        """Generates a simple unique ID based on name and address."""
        data_string = f"{name.lower().strip()}|{address.lower().strip()}"
        return hashlib.sha1(data_string.encode("utf-8")).hexdigest()
