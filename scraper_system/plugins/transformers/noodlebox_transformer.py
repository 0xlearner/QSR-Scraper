import logging
import hashlib
import re
from typing import List, Dict, Any, Optional

from scraper_system.interfaces.transformer_interface import TransformerInterface
from scraper_system.models.location import TransformedLocation

logger = logging.getLogger(__name__)

class NoodleboxTransformer(TransformerInterface):
    """
    Transformer for Noodlebox data.
    Responsible for transforming raw data into final format.
    """
    # Direct, explicit mapping for every business name directly to correct suburb
    BUSINESS_NAME_TO_SUBURB = {
        "Malvern": "MALVERN",
        "Altona": "ALTONA NORTH",
        "Ballarat": "BALLARAT NORTH",
        "Bendigo": "BENDIGO",
        "Chirnside Park": "CHIRNSIDE PARK",
        "Coburg": "COBURG",
        "Corio": "CORIO",
        "Craigieburn": "CRAIGIEBURN",
        "Cranbourne": "CRANBOURNE",
        "Dandenong": "DANDENONG",
        "Docklands": "DOCKLANDS",
        "East Melbourne": "EAST MELBOURNE",
        "Epping": "EPPING",
        "Essendon": "ESSENDON",
        "Flemington": "FLEMINGTON",
        "Footscray": "FOOTSCRAY",
        "Forest Hill": "FOREST HILL",
        "Fountain Gate": "NARRE WARREN",
        "Glen Waverley": "GLEN WAVERLEY",
        "Caroline Springs": "CAROLINE SPRINGS",
        "Moonee Ponds": "MOONEE PONDS",
        "Mornington": "MORNINGTON",
        "Mount Barker": "MOUNT BARKER",
        "Golden Grove": "GOLDEN GROVE",
        "Munno Para": "MUNNO PARA",
        "Albany Creek": "ALBANY CREEK",
        "Bracken Ridge": "BRACKEN RIDGE",
        "Pacific Pines": "PACIFIC PINES",
        "Mount Gravatt": "MOUNT GRAVATT",
        "Mount Gravatt East": "MOUNT GRAVATT EAST",
        "Mooloolaba": "MOOLOOLABA",
        "Nundah": "NUNDAH",
        "Beenleigh": "BEENLEIGH",
        "Pimpama": "PIMPAMA",
        "Newmarket": "NEWMARKET",
        "Stones Corner": "STONES CORNER",
        "Park Ridge": "PARK RIDGE",
        "Cannon Hill": "CANNON HILL",
        "Logan Village": "LOGAN VILLAGE",
        "Mango Hill": "MANGO HILL",
        "Waterford West": "WATERFORD WEST",
        "Shailer Park": "SHAILER PARK",
        "Everton Park": "EVERTON PARK",
        "Melton West": "MELTON WEST",
        "Morphett Vale": "MORPHETT VALE",
        "Woodcroft": "MORPHETT VALE",
        "Carindale": "MOUNT GRAVATT EAST",
        "Adelaide": "ADELAIDE",
        "Brisbane": "BRISBANE",
        "Canberra": "CANBERRA",
        "Melbourne": "MELBOURNE",
        "Sydney": "SYDNEY",
        "Perth": "PERTH",
        "Darwin": "DARWIN",
        "Hobart": "HOBART",
        "Kirwan Townsville": "KIRWAN",
        "Fairfield Waters Townsville": "IDALIA",
    }

    # Location-specific state mapping based on known suburbs
    SUBURB_TO_STATE = {
        "MALVERN": "VIC",
        "ALTONA NORTH": "VIC",
        "BALLARAT NORTH": "VIC",
        "BENDIGO": "VIC",
        "CHIRNSIDE PARK": "VIC",
        "COBURG": "VIC", 
        "CORIO": "VIC",
        "CRAIGIEBURN": "VIC",
        "CRANBOURNE": "VIC",
        "DANDENONG": "VIC",
        "DOCKLANDS": "VIC",
        "EAST MELBOURNE": "VIC",
        "EPPING": "VIC",
        "ESSENDON": "VIC",
        "FLEMINGTON": "VIC",
        "FOOTSCRAY": "VIC",
        "FOREST HILL": "VIC",
        "NARRE WARREN": "VIC",
        "GLEN WAVERLEY": "VIC",
        "CAROLINE SPRINGS": "VIC",
        "MOONEE PONDS": "VIC",
        "MORNINGTON": "VIC",
        "MOUNT BARKER": "SA",
        "GOLDEN GROVE": "SA",
        "MUNNO PARA": "SA",
        "ALBANY CREEK": "QLD",
        "BRACKEN RIDGE": "QLD",
        "PACIFIC PINES": "QLD",
        "MOUNT GRAVATT": "QLD",
        "MOUNT GRAVATT EAST": "QLD",
        "MOOLOOLABA": "QLD",
        "NUNDAH": "QLD",
        "BEENLEIGH": "QLD",
        "PIMPAMA": "QLD",
        "NEWMARKET": "QLD",
        "STONES CORNER": "QLD",
        "PARK RIDGE": "QLD",
        "CANNON HILL": "QLD",
        "LOGAN VILLAGE": "QLD",
        "MANGO HILL": "QLD",
        "WATERFORD WEST": "QLD",
        "SHAILER PARK": "QLD",
        "EVERTON PARK": "QLD",
        "MELTON": "VIC",
        "MELTON WEST": "VIC",
        "MORPHETT VALE": "SA",
        "ADELAIDE": "SA",
        "BRISBANE": "QLD",
        "CANBERRA": "ACT",
        "MELBOURNE": "VIC",
        "SYDNEY": "NSW",
        "PERTH": "WA",
        "DARWIN": "NT",
        "HOBART": "TAS",
        "KIRWAN": "QLD",
        "IDALIA": "QLD",
    }

    async def transform(self, data: List[Dict[str, Any]], config: Dict[str, Any], site_name: str) -> List[Dict[str, Any]]:
        """
        Transforms raw scraped data from Noodlebox parser.
        """
        if not data:
            logger.warning("No data to transform for Noodlebox")
            return []

        transformed_items = []

        for item in data:
            try:
                # Extract raw data
                business_name = item.get("business_name", "")
                raw_address = item.get("raw_address", "")
                drive_thru = item.get("drive_thru", False)
                source_url = item.get("source_url", "")

                # Clean the business name if it has "Noodlebox " prefix
                cleaned_name = business_name
                if business_name.startswith("Noodlebox "):
                    cleaned_name = business_name[10:]

                # Determine suburb using direct mapping
                if "Temporarily Closed" in raw_address:
                    suburb = "TEMPORARILY CLOSED"
                    street_address = "Temporarily Closed"
                elif "Coming Soon" in raw_address:
                    suburb = "COMING SOON"
                    street_address = "Coming Soon"
                elif cleaned_name in self.BUSINESS_NAME_TO_SUBURB:
                    suburb = self.BUSINESS_NAME_TO_SUBURB[cleaned_name]
                    street_address = raw_address
                else:
                    # Default to business name if no mapping exists
                    suburb = cleaned_name.upper()
                    street_address = raw_address

                # Determine state from suburb
                state = None
                if suburb in self.SUBURB_TO_STATE:
                    state = self.SUBURB_TO_STATE[suburb]
                else:
                    # Try to extract state from address
                    state_match = re.search(r'\b(NSW|VIC|QLD|SA|WA|TAS|NT|ACT)\b', raw_address)
                    if state_match:
                        state = state_match.group(1)
                    else:
                        # Check for full state names
                        state_names = {
                            "VICTORIA": "VIC",
                            "QUEENSLAND": "QLD",
                            "NEW SOUTH WALES": "NSW",
                            "SOUTH AUSTRALIA": "SA",
                            "WESTERN AUSTRALIA": "WA",
                            "TASMANIA": "TAS",
                            "NORTHERN TERRITORY": "NT"
                        }
                        for full_name, abbr in state_names.items():
                            if full_name in raw_address.upper():
                                state = abbr
                                break

                # Extract postcode
                postcode = None
                postcode_match = re.search(r'\b(\d{4})\b', raw_address)
                if postcode_match:
                    postcode = postcode_match.group(1)

                # Extract shopping centre name
                shopping_centre_name = self._extract_shopping_centre(raw_address)

                # Generate business ID
                business_id = self.generate_business_id(
                    business_name,
                    f"{street_address}, {suburb} {state or ''} {postcode or ''}",
                )

                # Create the transformed location
                location = TransformedLocation(
                    business_name=business_name,
                    street_address=street_address,
                    suburb=suburb,
                    state=state,
                    postcode=postcode,
                    drive_thru=drive_thru,
                    shopping_centre_name=shopping_centre_name,
                    source_url=source_url,
                    source="noodlebox",
                    business_id=business_id,
                )

                transformed_items.append(location.model_dump())

            except Exception as e:
                logger.error(f"Error transforming Noodlebox item: {e}", exc_info=True)

        logger.info(f"Transformed {len(transformed_items)} Noodlebox items")
        return transformed_items

    def _extract_shopping_centre(self, address: str) -> Optional[str]:
        """Extract shopping centre name from address."""
        if not address:
            return None

        # Shopping centre patterns
        patterns = [
            r"((?:[\w\s]+)(?:Shopping Centre|Plaza|Mall|Centre|Square|Marketplace))",
            r"(Westfield\s+[\w\s]+)",
            r"(Stockland\s+[\w\s]+)"
        ]

        for pattern in patterns:
            match = re.search(pattern, address)
            if match:
                shopping_centre_name = match.group(1)
                if shopping_centre_name:
                    return self._clean_shopping_centre_name(shopping_centre_name)

        return None

    def _clean_shopping_centre_name(self, shopping_centre_name: str) -> str:
        """
        Clean shopping centre name by removing shop numbers, tenancy references, etc.
        """
        if not shopping_centre_name:
            return ""

        # Remove shop/unit/tenancy numbers at the beginning
        patterns = [
            r"^Shop\s+[A-Za-z0-9]+\s+",  # Shop A13
            r"^Unit\s+[A-Za-z0-9]+\s+",  # Unit 5
            r"^Tenancy\s+[A-Za-z0-9]+\s+",  # Tenancy U
            r"^T\d+[A-Za-z]?\s+&?\s*T?\d*[A-Za-z]?\s+",  # T44 & T50b
            r"^[A-Za-z]\d+[A-Za-z]?\s+",  # A13
            r"^P\d+[A-Za-z]?\s+",  # P5
            r"^Level\s+\d+[A-Za-z]?\s+",  # Level 1
            r"^L\d+[A-Za-z]?\s+",  # L1
            r"^Building\s+\d+[A-Za-z]?\s+Tenancy\s+\d+[A-Za-z]?\s+",  # Building 4 Tenancy 4
            r"^Building\s+\d+[A-Za-z]?\s+",  # Building 4
            r"[Cc]nr\s+[A-Za-z\s]+\s+",  # Cnr Greek
            r"[Cc]orner\s+[A-Za-z\s]+\s+",  # Corner Augusta Park
            r"\d+[/-]\d+\s+",  # 523/532
        ]

        result = shopping_centre_name
        for pattern in patterns:
            result = re.sub(pattern, "", result, flags=re.IGNORECASE)

        # Remove specific phrases that aren't part of the shopping center name
        phrases_to_remove = [
            r"Tenancy\s+[A-Za-z0-9]+\s+",  # Tenancy U
            r"Shop\s+[A-Za-z0-9]+\s+",  # Shop 5
            r"Unit\s+[A-Za-z0-9]+\s+",  # Unit 3
        ]

        for phrase in phrases_to_remove:
            result = re.sub(phrase, "", result, flags=re.IGNORECASE)

        # Clean up address components
        address_pattern = r'\d+[/-]?\d*\s+[A-Za-z]+\s+(?:Street|St|Road|Rd|Avenue|Ave|Highway|Hwy|Drive|Dr|Boulevard|Blvd)'
        result = re.sub(address_pattern, '', result, flags=re.IGNORECASE)

        # Remove suburb names and state/postcode at the end
        result = re.sub(r'\s+[A-Z]{2,3}\s+\d{4}$', '', result)

        # Clean up any remaining commas, extra spaces, etc.
        result = result.strip().rstrip(",").strip()
        result = re.sub(r"\s+", " ", result)  # Normalize whitespace

        return result

    def generate_business_id(self, name: str, address: str) -> str:
        """Generates a simple unique ID based on name and address."""
        data_string = f"{name.lower().strip()}|{address.lower().strip()}"
        return hashlib.sha1(data_string.encode("utf-8")).hexdigest()