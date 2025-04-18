import logging
import re
from typing import List, Dict, Any, Optional, Tuple, Match
from datetime import datetime
import hashlib

from pydantic import BaseModel, Field, ValidationError

from scraper_system.interfaces.transformer_interface import TransformerInterface

logger = logging.getLogger(__name__)

# --- Pydantic Model for Final Output ---
class TransformedLocation(BaseModel):
    business_name: str
    street_address: Optional[str] = None
    suburb: Optional[str] = None
    state: Optional[str] = None
    postcode: Optional[str] = None
    drive_thru: bool = False
    shopping_centre_name: Optional[str] = None
    source_url: Optional[str] = None # Keep the original URL
    source: str # Website identifier (e.g., 'grilld')
    scraped_date: datetime = Field(default_factory=datetime.utcnow)
    business_id: Optional[str] = None # Unique ID for the location

    # Add a pre-validator or root_validator if needed for complex logic/defaults

# --- Address Parsing Logic ---

# Regex v4: Focus on finding STATE[POSTCODE]$ and working backwards
# Group 1: Leading Part (greedy, capture everything before Suburb)
# Group 2: Suburb (non-greedy, must be preceded by space/comma, must end before final comma+State)
# Group 3: State (2 or 3 letters)
# Group 4: Optional Postcode (4 digits)
ADDRESS_REGEX_V4 = re.compile(
    r"^(.*?)[,\s]+([A-Z\s.'-]+?)\s*,\s*([A-Z]{2,3})\s*(?:(\d{4}))?\s*$",
    re.IGNORECASE | re.DOTALL
)
# Explanation:
# ^(.*?)              # Group 1: Non-greedy capture from start (will become greedy later if needed)
# [,\s]+              # Separator: One or more commas or spaces before suburb
# ([A-Z\s.'-]+?)      # Group 2: Suburb - letters, spaces, ., ', - (non-greedy)
# \s*,\s*             # Separator: Optional space, comma, optional space before state
# ([A-Z]{2,3})         # Group 3: State - 2 or 3 letters (handles NT, ACT)
# \s*                 # Optional space after state
# (?:(\d{4}))?        # Optional non-capturing group with capturing Group 4: Postcode (4 digits)
# \s*$                # Optional trailing space and end of string

# Let's try a slightly greedier version for the leading part if V4 fails often
ADDRESS_REGEX_V4_GREEDY_LEAD = re.compile(
    r"^(.*)[,\s]+([A-Z\s.'-]+?)\s*,\s*([A-Z]{2,3})\s*(?:(\d{4}))?\s*$",
    re.IGNORECASE | re.DOTALL
)
# ^(.*)               # Group 1: Greedy capture from start

# Keywords etc remain the same
SHOPPING_CENTRE_KEYWORDS = ['shopping centre', ' shop ', ' sc ', ' mall ', ' square ', ' village ', ' plaza', ' centre', ' boardwalk', ' quay', ' gate ']
STREET_INDICATORS = [' st', ' rd', ' ave', ' hwy', ' dr', ' tce', ' pde', ' blvd', ' way', ' pl', ' cres']
CORNER_INDICATOR = 'cnr '

# --- Helper Functions for Parsing ---

def _try_address_regexes(address_string: str) -> Optional[Match[str]]:
    """Tries the primary and fallback regexes, returning the first match."""
    match = ADDRESS_REGEX_V4.match(address_string)
    if not match:
        match = ADDRESS_REGEX_V4_GREEDY_LEAD.match(address_string)
    return match

def _extract_base_parts(match: Match[str]) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    """Extracts the main groups from a successful regex match."""
    leading_part, suburb, state, postcode = match.groups()
    leading_part = leading_part.strip().rstrip(',') if leading_part else None
    suburb = suburb.strip().upper() if suburb else None
    state = state.strip().upper() if state else None
    postcode = postcode.strip() if postcode else None
    return leading_part, suburb, state, postcode

def _split_leading_part(leading_part: str) -> Tuple[Optional[str], Optional[str]]:
    """Splits the leading part heuristically into street and shopping centre."""
    potential_street = None
    potential_shopping = None
    leading_lower = leading_part.lower()
    is_shopping_centre_kw = any(keyword in leading_lower for keyword in SHOPPING_CENTRE_KEYWORDS)
    is_corner = leading_lower.startswith(CORNER_INDICATOR)

    parts = leading_part.split(',')
    if len(parts) > 1:
        maybe_street = parts[-1].strip()
        maybe_shopping = ", ".join(parts[:-1]).strip()
        street_like = any(ind in maybe_street.lower() for ind in STREET_INDICATORS) or \
                      any(char.isdigit() for char in maybe_street) or \
                      is_corner # Check last part

        if street_like:
            potential_street = maybe_street
            potential_shopping = maybe_shopping
        else:
            # If last part isn't street-like, assign based on keywords in full part
            if is_shopping_centre_kw or is_corner:
                 potential_shopping = leading_part
            else:
                 potential_street = leading_part
    else: # Only one part
         if is_shopping_centre_kw or is_corner:
             potential_shopping = leading_part
         else:
             potential_street = leading_part

    return potential_street, potential_shopping

def _remove_trailing_state_postcode(text: Optional[str], state: Optional[str], postcode: Optional[str]) -> Optional[str]:
    """Removes trailing state/postcode from a string if they match."""
    if not text or not state: # Nothing to clean or no state to match against
        return text

    state_part = re.escape(state)
    pattern_core = rf"\s*{state_part}"
    if postcode:
        postcode_part = re.escape(postcode)
        pattern_core += rf"(?:\s*{postcode_part})?"
    state_postcode_pattern = pattern_core + r"\s*$"

    cleaned_text = re.sub(state_postcode_pattern, '', text, flags=re.IGNORECASE).strip().rstrip(',')
    return cleaned_text if cleaned_text else None # Return None if string becomes empty

def _finalize_parsed_dict(parsed: Dict[str, Optional[str]]) -> Dict[str, Optional[str]]:
    """Cleans up final dictionary values (stripping, None conversion)."""
    final_parsed = {}
    for key, value in parsed.items():
        if isinstance(value, str):
            cleaned_value = value.strip().rstrip(',').strip()
            final_parsed[key] = cleaned_value if cleaned_value else None
        else:
            final_parsed[key] = value # Keep None as None
    return final_parsed

def parse_australian_address(address_string: str) -> Dict[str, Optional[str]]:
    """
    Parses a raw Australian address string into components (Refactored for complexity).
    """
    parsed: Dict[str, Optional[str]] = {
        "street_address": None, "suburb": None, "state": None,
        "postcode": None, "shopping_centre_name": None
    }
    original_address = address_string
    address_string = address_string.strip()

    match = _try_address_regexes(address_string)

    if match:
        leading_part, suburb, state, postcode = _extract_base_parts(match)
        parsed["suburb"] = suburb
        parsed["state"] = state
        parsed["postcode"] = postcode

        if leading_part:
            street, shop = _split_leading_part(leading_part)
            # Assign results from split first
            parsed["street_address"] = street
            parsed["shopping_centre_name"] = shop

            # Clean trailing state/postcode AFTER assigning potential values
            parsed["street_address"] = _remove_trailing_state_postcode(parsed["street_address"], state, postcode)
            parsed["shopping_centre_name"] = _remove_trailing_state_postcode(parsed["shopping_centre_name"], state, postcode)

    else:
        logger.warning(f"Could not parse address string using regex (V4/V4_GREEDY_LEAD): {original_address}")
        # Fallback: Store original in street_address
        parsed["street_address"] = original_address

    # Apply final cleanup (stripping, empty->None) to all fields
    return _finalize_parsed_dict(parsed)


def generate_business_id(name: str, address: str) -> str:
    """Generates a simple unique ID based on name and address."""
    data_string = f"{name.lower().strip()}|{address.lower().strip()}"
    return hashlib.sha1(data_string.encode('utf-8')).hexdigest()


# --- Transformer Implementation ---

class AddressParserTransformer(TransformerInterface):
    """
    Transforms raw scraped data by parsing the address field
    and structuring the output using the TransformedLocation model. (Uses refactored parser V4)
    """
    async def transform(self, data: List[Dict[str, Any]], config: Dict[str, Any], site_name: str) -> List[Dict[str, Any]]:
        transformed_data = []
        logger.info(f"Starting transformation for {len(data)} items from site '{site_name}' using refactored parser V4")

        for item in data:
            raw_address = item.get("address")
            business_name = item.get("name", "Unknown Name")

            if not raw_address:
                logger.warning(f"Item missing 'address' field, skipping: {business_name}")
                continue

            # Call the main refactored parsing function
            parsed_address_parts = parse_australian_address(raw_address)

            try:
                location = TransformedLocation(
                    business_name=business_name,
                    street_address=parsed_address_parts["street_address"],
                    suburb=parsed_address_parts["suburb"],
                    state=parsed_address_parts["state"],
                    postcode=parsed_address_parts["postcode"],
                    drive_thru=item.get("drive_thru", False),
                    shopping_centre_name=parsed_address_parts["shopping_centre_name"],
                    source_url=item.get("source_url"),
                    source=site_name,
                    business_id=generate_business_id(business_name, raw_address)
                )
                transformed_data.append(location.dict())

            except ValidationError as e:
                logger.error(f"Pydantic validation failed for item '{business_name}' (Address: {raw_address}): {e}", exc_info=False)
            except Exception as e:
                 logger.error(f"Unexpected error transforming item '{business_name}' (Address: {raw_address}): {e}", exc_info=True)

        # Reporting logic remains the same
        successful_parses = sum(1 for item in transformed_data if item.get("suburb") and item.get("state"))
        # Recalculate failed parses based on input data length if transformation itself can fail
        valid_input_count = len([item for item in data if item.get("address")]) # Count items that had an address to begin with
        failed_parses = valid_input_count - successful_parses
        logger.info(f"Finished transformation. Produced {len(transformed_data)} items. Successful address parses (Suburb/State found): {successful_parses}, Failed/Fallback on valid inputs: {failed_parses}")
        return transformed_data
