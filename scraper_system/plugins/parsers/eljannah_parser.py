import logging
import asyncio
import json
from typing import List, Dict, Any, Optional


from selectolax.parser import HTMLParser as SelectolaxHTMLParser, Node

from scraper_system.interfaces.parser_interface import ParserInterface
from scraper_system.interfaces.fetcher_interface import (
    FetcherInterface,
)  # Required for fetching details

logger = logging.getLogger(__name__)

BASE_URL = "https://eljannah.com.au"


class EljannahParser(ParserInterface):
    """
    Parses El Jannah website.
    1. Finds location links on the main locations page.
    2. Fetches each location's detail page using the provided fetcher.
    3. Extracts name, address, coordinates (from JavaScript), and drive-thru status from the detail page.
    """

    def __init__(self, fetcher: Optional[FetcherInterface] = None):
        if fetcher is None:
            raise ValueError(
                "EljannahParser requires a Fetcher instance (expecting PlaywrightFetcher)."
            )
        self.fetcher = fetcher
        logger.info("EljannahParser initialized with a fetcher.")

    async def parse(
        self, content: str, content_type: Optional[str], config: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Parses the main /locations page, finds detail links, and fetches/parses details.
        """
        if not content:
            logger.warning("No initial content provided to EljannahParser.")
            return []

        logger.info("EljannahParser starting main page parsing to find location links.")
        detail_tasks = []
        final_results = []

        try:
            tree = SelectolaxHTMLParser(content)
            location_links = tree.css(
                "div.location-item__buttons a.btn.btn-primary.btn-empty:first-child"
            )
            logger.info(
                f"Found {len(location_links)} potential location links on the main page."
            )

            fetcher_config = config.get(
                "detail_fetcher_options", config.get("fetcher_options", {})
            )

            for link_node in location_links:
                detail_url = link_node.attributes.get("href")
                if not detail_url or not detail_url.startswith(
                    BASE_URL + "/locations/"
                ):
                    logger.debug(
                        f"Skipping link, doesn't seem like a valid location detail page: {detail_url}"
                    )
                    continue

                logger.debug(f"Queueing detail fetch for: {detail_url}")
                detail_tasks.append(
                    self.fetch_and_parse_detail(detail_url, fetcher_config)
                )

            results_from_details = await asyncio.gather(*detail_tasks)

            for result in results_from_details:
                if result:
                    final_results.append(result)

        except Exception as e:
            logger.error(
                f"Error processing main El Jannah locations page: {e}", exc_info=True
            )

        logger.info(f"EljannahParser finished, returning {len(final_results)} items.")
        return final_results

    def _extract_ld_json_data(
        self, tree: SelectolaxHTMLParser
    ) -> Optional[Dict[str, Any]]:
        """Extract and parse the LD+JSON data from the page."""
        try:
            # Look for the LD+JSON script tag
            ld_json_script = tree.css_first(
                'script[type="application/ld+json"].rank-math-schema-pro'
            )
            if not ld_json_script:
                logger.warning("No LD+JSON script found on the page")
                return None

            # Parse the JSON content
            json_content = ld_json_script.text()
            if not json_content:
                logger.warning("Empty LD+JSON content")
                return None

            json_data = json.loads(json_content)

            # Find the Restaurant object in the @graph array
            restaurant_data = None
            if "@graph" in json_data and isinstance(json_data["@graph"], list):
                for item in json_data["@graph"]:
                    if item.get("@type") == "Restaurant":
                        restaurant_data = item
                        break

            return restaurant_data
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse LD+JSON data: {e}")
            return None
        except Exception as e:
            logger.error(f"Error extracting LD+JSON data: {e}")
            return None

    async def fetch_and_parse_detail(
        self, url: str, fetcher_config: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Fetches and parses a single location detail page."""
        try:
            html_content, _, status_code = await self.fetcher.fetch(url, fetcher_config)

            if not html_content:
                logger.error(
                    f"Failed to fetch detail page {url} (Status: {status_code})"
                )
                return None

            try:
                tree = SelectolaxHTMLParser(html_content)

                # Extract data from LD+JSON
                ld_json_data = self._extract_ld_json_data(tree)

                if ld_json_data:
                    # Extract name from LD+JSON
                    name = ld_json_data.get("name", "")
                    if name:
                        # Clean up name (remove "| The Best Charcoal & Fried Chicken" part)
                        if "|" in name:
                            name = name.split("|")[0].strip()
                    else:
                        # Fallback to HTML extraction if LD+JSON doesn't have name
                        name_node = tree.css_first(
                            "h1.elementor-heading-title.elementor-size-default"
                        )
                        name = (
                            name_node.text(strip=True)
                            if name_node
                            else "Unknown El Jannah Location"
                        )

                    # Extract address components from LD+JSON
                    address_data = ld_json_data.get("address", {})
                    street_address = address_data.get("streetAddress", "")
                    suburb = address_data.get("addressRegion", "")
                    state = url.split("/")[-3].upper()
                    postcode = address_data.get("postalCode", "")

                    # Extract drive-thru status
                    has_drive_thru: bool = False  # Default to False
                    attributes_list: Optional[Node] = tree.css_first(
                        "ul.yext-attributes"
                    )
                    if attributes_list:
                        list_items: List[Node] = attributes_list.css("li")
                        for item in list_items:
                            item_text = item.text(strip=True)
                            if item_text.startswith("Has Drive Through:"):
                                value = item_text.split(":", 1)[-1].strip()
                                if value.lower() == "yes":
                                    has_drive_thru = True
                                    logger.debug(f"Drive-thru found for {url}")
                                break

                    # Return basic data for transformation
                    return {
                        "business_name": name,
                        "street_address": street_address,
                        "suburb": suburb,
                        "state": state,
                        "postcode": postcode,
                        "drive_thru": has_drive_thru,
                        "source_url": url,
                        "source": "eljannah",
                    }

            except Exception as e:
                logger.error(f"Error parsing detail page {url}: {e}", exc_info=True)
                return None

        except Exception as e:
            logger.error(f"Error fetching detail page {url}: {e}", exc_info=True)
            return None