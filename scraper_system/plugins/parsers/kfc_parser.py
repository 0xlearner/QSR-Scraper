import asyncio
import json
import logging
from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import urlparse, urlunparse, urlencode

import numpy as np

from scraper_system.interfaces.fetcher_interface import FetcherInterface
from scraper_system.interfaces.parser_interface import ParserInterface

logger = logging.getLogger(__name__)


@dataclass
class Place:
    """Internal representation of a parsed place before final conversion."""

    address: str
    name: str
    drive_thru: bool
    source_url: str


AUSTRALIA_BOUNDS = {
    "north": -10.0,
    "south": -44.0,
    "east": 154.0,
    "west": 113.0,
}


def generate_search_grid(rows: int = 5, cols: int = 5) -> List[Tuple[float, float]]:
    """Generate a grid of lat/lng coordinates covering Australia."""
    # Create evenly spaced points for latitude and longitude
    lats = np.linspace(AUSTRALIA_BOUNDS["south"], AUSTRALIA_BOUNDS["north"], rows)
    lngs = np.linspace(AUSTRALIA_BOUNDS["west"], AUSTRALIA_BOUNDS["east"], cols)

    # Create grid points
    grid_points = []
    for lat in lats:
        for lng in lngs:
            grid_points.append((round(lat, 6), round(lng, 6)))

    return grid_points


def build_search_urls(
    grid_points: List[Tuple[float, float]],
    search_query: str,
    radius_km: int = 50,
) -> List[str]:
    """Build Google Maps search URLs for each grid point."""
    base_url = "https://www.google.com/search"
    urls = []

    # Make sure to correctly place {radius}, {lng}, {lat}
    pb_template = (
        "!4m12!1m3!1d{radius}!2d{lng}!3d{lat}!2m3!1f0!2f0!3f0!3m2!1i1366!2i605!4f13.1"
        "!7i200!8i20!10b1!12m24!1m5!18b1!30b1!31m1!1b1!34e1!2m3!5m1!6e2!20e3!4b0!10b1"
        "!12b1!13b1!16b1!17m1!3e1!20m3!5e2!6b1!14b1!46m1!1b0!96b1!19m4!2m3!1i360!2i120"
        "!4i8!20m65!2m2!1i203!2i100!3m2!2i4!5b1!6m6!1m2!1i86!2i86!1m2!1i408!2i240!7m33"
        "!1m3!1e1!2b0!3e3!1m3!1e2!2b1!3e2!1m3!1e2!2b0!3e3!1m3!1e8!2b0!3e3!1m3!1e10!2b0"
        "!3e3!1m3!1e10!2b1!3e2!1m3!1e10!2b0!3e4!1m3!1e9!2b1!3e2!2b1!9b0!15m16!1m7!1m2"
        "!1m1!1e2!2m2!1i195!2i195!3i20!1m7!1m2!1m1!1e2!2m2!1i195!2i195!3i20!22m5"
        "!1s7aUIaJWbEZSb4-EP4_zYmAM%3A167!2s1i%3A0%2Ct%3A246204%2Cp%3A7aUIaJWbEZSb4-EP4_zYmAM%3A167!7e81!12e22!17s7aUIaJWbEZSb4-EP4_zYmAM%3A168"  # NOTE: The part starting !22m5 might be dynamic/session specific
        "!24m113!1m32!13m9!2b1!3b1!4b1!6i1!8b1!9b1!14b1!20b1!25b1!18m21!3b1!4b1!5b1!6b1"
        "!9b1!12b1!13b1!14b1!17b1!20b1!21b1!22b1!25b1!27m1!1b0!28b0!32b1!33m1!1b1!34b0!36e1"
        "!10m1!8e3!11m1!3e1!14m1!3b0!17b1!20m2!1e3!1e6!24b1!25b1!26b1!27b1!29b1!30m1!2b1!36b1"
        "!37b1!39m3!2m2!2i1!3i1!43b1!52b1!54m1!1b1!55b1!56m1!1b1!61m2!1m1!1e1!65m5!3m4!1m3!1m2!1i224"
        "!2i298!71b1!72m22!1m8!2b1!5b1!7b1!12m4!1b1!2b1!4m1!1e1!4b1!8m10!1m6!4m1!1e1!4m1!1e3!4m1!1e4"
        "!3sother_user_google_review_posts__and__hotel_and_vr_partner_review_posts"
        "!6m1!1e1!9b1!89b1!98m3!1b1!2b1!3b1!103b1!113b1!114m3!1b1!2m1!1b1!117b1!122m1!1b1!125b0!126b1"
        "!127b1!26m4!2m3!1i80!2i92!4i8!30m28!1m6!1m2!1i0!2i0!2m2!1i530!2i605!1m6!1m2!1i1316!2i0!2m2!1i1366!2i605"
        "!1m6!1m2!1i0!2i0!2m2!1i1366!2i20!1m6!1m2!1i0!2i585!2m2!1i1366!2i605!34m19!2b1!3b1!4b1!6b1!8m6!1b1!3b1!4b1"
        "!5b1!6b1!7b1!9b1!12b1!14b1!20b1!23b1!25b1!26b1!31b1!37m1!1e81!42b1!46m1!1e4!47m0!49m10!3b1!6m2!1b1!2b1!7m2"
        "!1e3!2b1!8b1!9b1!10e2!50m16!1m11!2m7!1u3!4sOpen+now!5e1"
        "!9s0ahUKEwi81_S43-2MAxVY8DgGHabKFkgQ_KkBCAYoAg!10m2!3m1!1e1!3m1!1u3!4BIAE!2e2!3m2!1b1!3b1!59BQ2dBd0Fn!67m3!7b1!10b1!14b1!69i730"
        # Note: The very long string above contains latitude, longitude, and radius markers like !1d{radius}!2d{lng}!3d{lat}
        # It also contains potentially dynamic session IDs or state markers like !1s7aUI... or !9s0ahUKEwi...
        # Using a static version might work initially but could break over time.
    )

    # Static psi value from the example URL. This might expire or change!
    # A more robust solution might involve capturing this from an initial request or omitting it if not essential.
    psi_value = (
        "7aUIaJWbEZSb4-EP4_zYmAM.1745397231200.1"  # Example value, might need updating
    )

    for lat, lng in grid_points:
        # Format the pb parameter with the current coordinates and radius
        pb_value = pb_template.format(
            radius=radius_km * 1000,  # Convert km to meters
            lng=lng,
            lat=lat,
        )

        # Base parameters dictionary - include ALL parameters here
        params: Dict[str, Any] = {
            "tbm": "map",
            "authuser": "0",
            "hl": "en",
            "gl": "au",
            "q": search_query,  # Included once here
            "nfpr": "1",  # Included once here
            "pb": pb_value,  # The full, formatted pb value
            # Add the missing parameters from the example
            "tch": "1",
            "ech": "2",
            "psi": psi_value,  # Use the static value for now
        }

        # Use urlencode to properly join parameters and handle encoding
        query_string = urlencode(params)
        urls.append(f"{base_url}?{query_string}")

    return urls


def deduplicate_places(places: List[Place]) -> List[Place]:
    """Remove duplicate places based on name and address."""
    seen: Set[tuple] = set()
    unique_places: List[Place] = []
    for place in places:
        place_key = (place.name.strip().lower(), place.address.strip().lower())
        if place_key not in seen:
            seen.add(place_key)
            unique_places.append(place)
    return unique_places


def clean_url(url: str) -> str:
    """Remove query parameters and fragments from URL."""
    if not url:
        return ""
    try:
        parsed = urlparse(url)
        # Keep scheme and netloc, clear others
        clean = urlunparse(
            (parsed.scheme, parsed.netloc, parsed.path.rstrip("/"), "", "", "")
        )
        return clean
    except Exception as e:
        logger.error(f"Error cleaning URL {url}: {e}")
        return url  # Return original on error


def get_nested_value(data: List | Dict, indexes: List[int]) -> Optional[Any]:
    """Safely extract a nested value from a JSON structure (list/dict) using indexes."""
    current = data
    try:
        for index in indexes:
            if (
                isinstance(current, list)
                and isinstance(index, int)
                and 0 <= index < len(current)
            ):
                current = current[index]
            elif (
                isinstance(current, dict) and index in current
            ):  # Allow string keys if needed, though example uses ints
                current = current[index]
            else:
                return None  # Index out of bounds or wrong type
        return current
    except (KeyError, IndexError, TypeError):
        return None


def prepare(input_text: str) -> Optional[List[Dict]]:
    """Prepare raw input data by cleaning and parsing it into JSON."""
    # Check if the input is None or empty before proceeding
    if not input_text:
        logger.warning("prepare received None or empty input_text.")
        return None
    try:
        prepared = input_text.replace('/*""*/', "")
        json_data = json.loads(prepared)

        if "d" in json_data and isinstance(json_data["d"], str):
            cleaned_d = json_data["d"].lstrip(")]}'\n")
            # Ensure cleaned_d is not empty before trying to load it
            if not cleaned_d:
                logger.error(
                    "Cleaned 'd' field is empty after stripping prefix. Cannot parse."
                )
                return None
            d_json = json.loads(cleaned_d)

            modified_json = json_data.copy()
            modified_json["d"] = d_json

            try:
                # Get the list of places from index 64 which contains place data
                # Adding checks for existence and type
                if isinstance(modified_json["d"][64], list):
                    first_key_value = modified_json["d"][64]
                    # Each place is represented by a list where index 1 contains the details
                    return [
                        item[1]
                        for item in first_key_value
                        if isinstance(item, list) and len(item) > 1
                    ]
                else:
                    logger.warning(
                        f"No valid data found at index 64 in 'd' field or wrong type. Found: {type(modified_json['d'][64]) if 64 in modified_json['d'] else 'missing'}"
                    )
                    return []  # Return empty list if structure is unexpected

            except IndexError:
                logger.error(
                    f"Index error accessing place data within 'd' field. Content snippet: {input_text[:500]}..."
                )
                return None  # Or return empty list [] if preferred
            except TypeError:
                logger.error(
                    f"Type error accessing place data within 'd' field. Content snippet: {input_text[:500]}..."
                )
                return None  # Or return empty list []

        else:
            logger.warning(
                f"Expected 'd' key with string value in json_data, not found or wrong type. Data: {str(json_data)[:500]}"
            )
            return None  # Or return empty list []

    except json.JSONDecodeError as e:
        logger.error(
            f"JSON decoding failed: {e}. Content snippet: {input_text[:500]}..."
        )
        return None
    except Exception as e:  # Catch other potential errors during preparation
        logger.error(
            f"Unexpected error during prepare: {e}. Content snippet: {input_text[:500]}...",
            exc_info=True,
        )
        return None


def build_address(place_detail_list: List) -> str:
    """Extract the formatted address string (index 18) from place detail list."""
    # Index 18 often contains the full formatted address string.
    address = get_nested_value(place_detail_list, [18])
    return str(address).strip() if address else ""


def is_australian_kfc_website(url: str) -> bool:
    """Check if the website URL points to the Australian KFC site."""
    return bool(url) and "kfc.com.au" in urlparse(url).netloc.lower()


def build_results(raw_place_details_list: List[List]) -> List[Place]:
    """Build a list of Place objects from the extracted raw place detail lists."""
    results = []
    if not isinstance(raw_place_details_list, list):
        logger.error("build_results expected a list, got something else.")
        return []

    for place_detail in raw_place_details_list:
        if not isinstance(place_detail, list):
            logger.debug(
                f"Skipping non-list item in place details: {type(place_detail)}"
            )
            continue
        try:
            # --- Extract fields based on observed indices (HIGHLY VOLATILE) ---
            # Website URL (often at index 7 -> 0)
            website = get_nested_value(place_detail, [7, 0])
            website_str = str(website).strip() if website else ""

            # Filter: Only include places linking to kfc.com.au
            if not is_australian_kfc_website(website_str):
                # logger.debug(f"Skipping place with non-AU website: {website_str}")
                continue

            # Name (often at index 11)
            name = get_nested_value(place_detail, [11])
            name_str = str(name).strip() if name else "Unknown KFC"

            # Address (often at index 18)
            address_str = build_address(place_detail)
            if not address_str:  # Skip if address couldn't be found
                logger.warning(f"Skipping place '{name_str}' due to missing address.")
                continue

            # Drive-Thru (often at index 142 -> 1 -> 0 -> 6 -> 0 -> 1 -> 4, value is "Drive-through")
            # This path is extremely deep and likely to break. Needs careful checking.
            is_drive_thru = False  # Default to False
            drive_thru_indicator = get_nested_value(
                place_detail, [142, 1, 0, 6, 0, 1, 4]
            )
            if (
                isinstance(drive_thru_indicator, str)
                and "drive" in drive_thru_indicator.lower()
            ):
                is_drive_thru = True

            # Fallback: Check other common attribute indices if the deep path fails or didn't find it
            if not is_drive_thru:
                # Check index 4 (attributes list) - often contains 'Drive-through' directly
                attributes_list = get_nested_value(place_detail, [4])
                if isinstance(attributes_list, list):
                    for attr in attributes_list:
                        if isinstance(attr, str) and "drive" in attr.lower():
                            is_drive_thru = True
                            break
            # Add more fallbacks if needed (e.g., checking index 13)

            # --- Create Place object ---
            place = Place(
                address=address_str,
                name=name_str,
                drive_thru=is_drive_thru,
                source_url=clean_url(website_str),
            )
            results.append(place)

        except Exception as e:
            # Log error for a specific place but continue with others
            place_name_debug = get_nested_value(place_detail, [11]) or "Unknown"
            logger.error(
                f"Error processing place details for '{place_name_debug}': {e}",
                exc_info=True,
            )
            continue

    return results


class KfcParser(ParserInterface):
    """
    Parses KFC locations by querying Google Maps via a grid search across Australia.

    This parser generates search URLs based on a configurable grid, fetches
    the results using the provided fetcher, parses the complex JSON response
    from Google, extracts KFC locations linking to kfc.com.au, and returns
    deduplicated results. Headers for Google Maps requests are read from config.

    Note: Relies heavily on the internal structure of Google Maps search results,
    which is undocumented and subject to change without notice. This parser
    may require frequent maintenance.
    """

    def __init__(self, fetcher: FetcherInterface):
        if fetcher is None:
            raise ValueError("KfcParser requires a Fetcher instance.")
        self.fetcher = fetcher
        logger.info("KfcParser initialized with a fetcher.")

    async def parse(
        self,
        content: Optional[str],
        content_type: Optional[str],
        config: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        """
        Main parsing method. Ignores initial content and drives its own fetching based on config.
        Reads Google Maps headers from config['api_settings']['headers'].
        """
        logger.info("KfcParser starting parse...")

        # --- Configuration ---
        parser_opts = config.get("parser_options", {})
        grid_rows = parser_opts.get("grid_rows", 15)
        grid_cols = parser_opts.get("grid_cols", 15)
        search_radius_km = parser_opts.get("search_radius_km", 50)
        search_query = parser_opts.get("search_query", "KFC")

        # API Settings (Headers for Google Maps)
        api_settings = config.get("api_settings", {})
        google_maps_headers = api_settings.get("headers", {})
        if not google_maps_headers:
            logger.warning(
                "Google Maps headers not found in config['api_settings']['headers']. Requests might fail."
            )

        # Fetcher options from main config
        fetcher_options = config.get("fetcher_options", {})

        # Prepare base fetcher config, merging general headers and specific Google Maps headers
        # Google Maps headers take precedence if keys conflict
        merged_headers = {**fetcher_options.get("headers", {}), **google_maps_headers}
        fetcher_config = {
            **fetcher_options,  # Include timeout, scraperapi settings etc.
            "headers": merged_headers,
        }

        # --- Generate Search Strategy ---
        grid_points = generate_search_grid(rows=grid_rows, cols=grid_cols)
        logger.info(
            f"Generated {len(grid_points)} grid points ({grid_rows}x{grid_cols})."
        )
        search_urls = build_search_urls(
            grid_points, search_query=search_query, radius_km=search_radius_km
        )
        logger.info(f"Built {len(search_urls)} search URLs for query '{search_query}'.")

        # --- Fetch and Parse Concurrently ---
        all_places: List[Place] = []
        tasks = [
            self._fetch_and_parse_search_url(url, fetcher_config) for url in search_urls
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # --- Process Results ---
        processed_count = 0
        success_count = 0
        for i, result in enumerate(results):
            processed_count += 1
            if isinstance(result, list):  # Successfully parsed list of Place objects
                all_places.extend(result)
                success_count += 1
            elif isinstance(result, Exception):
                # Error logged within the helper, maybe log URL index here
                logger.debug(
                    f"Task {i} for URL {search_urls[i][:80]}... failed with exception: {result}"
                )
            # None results indicate fetch/parse failure logged in helper

        logger.info(
            f"Processed {processed_count}/{len(search_urls)} search URLs. Successful fetches/parses: {success_count}."
        )

        # --- Deduplicate and Finalize ---
        unique_places = deduplicate_places(all_places)
        logger.info(
            f"Found {len(all_places)} raw matching places, {len(unique_places)} unique."
        )

        # Convert Place objects to dictionaries for the system
        final_results = [asdict(place) for place in unique_places]
        logger.info(f"KfcParser finished, returning {len(final_results)} items.")
        return final_results

    async def _fetch_and_parse_search_url(
        self, url: str, fetcher_config: Dict[str, Any]
    ) -> Optional[List[Place]]:
        """Fetches a single Google Maps search URL and parses the results."""
        url_snippet = url[:100] + "..." if len(url) > 100 else url
        logger.debug(f"Fetching search URL: {url_snippet}")

        content, content_type, status_code = await self.fetcher.fetch(
            url, fetcher_config
        )

        if content is None:
            logger.warning(
                f"Failed to fetch content for search URL: {url_snippet} (Status: {status_code})"
            )
            return None

        logger.info(f"Fetched URL {url_snippet} with status code: {status_code}")

        # Check if the content seems like valid JSON or needs cleaning
        if (
            content_type
            and "json" not in content_type.lower()
            and not content.strip().startswith(("{", "["))
        ):
            logger.debug(
                f"Content type for {url_snippet} is '{content_type}'. Attempting cleaning."
            )
            # Let prepare handle common wrappers like /*""*/ or )]}'\n
            pass

        try:
            # Prepare extracts the core place data list
            raw_place_details = prepare(content)
            if raw_place_details is None:
                # Error logged in prepare, just return None
                logger.warning(
                    f"Prepare function returned None for {url_snippet} (Status: {status_code}). Check logs."
                )
                return None

            # Build turns the raw data into Place objects
            kfc_places = build_results(raw_place_details)
            logger.debug(
                f"Parsed {len(kfc_places)} Australian KFC places from {url_snippet} (Status: {status_code})"
            )
            return kfc_places
        except Exception as e:
            # Catch any unexpected errors during prepare/build
            logger.error(
                f"Unexpected error processing result from {url_snippet} (Status: {status_code}): {e}",
                exc_info=True,
            )
            return None
