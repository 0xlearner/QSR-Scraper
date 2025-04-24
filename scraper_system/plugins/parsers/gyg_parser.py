import logging
from typing import List, Dict, Any, Optional

from selectolax.parser import HTMLParser as SelectolaxHTMLParser

from scraper_system.interfaces.parser_interface import ParserInterface
from scraper_system.interfaces.fetcher_interface import (
    FetcherInterface,
)

logger = logging.getLogger(__name__)


class GYGParser(ParserInterface):
    """
    Parses the Guzman y Gomez locations page (which lists all locations directly)
    to extract location details from data attributes.
    """

    def __init__(self, fetcher: Optional[FetcherInterface] = None):
        if fetcher is None:
            # This should not happen if Orchestrator injects correctly, but good practice
            raise ValueError("GYGParser requires a Fetcher instance.")
        self.fetcher = fetcher
        logger.info("GYGParser initialized with a fetcher.")

    async def parse(
        self, content: str, content_type: Optional[str], config: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Parses the GYG locations page HTML content.
        """
        if not content:
            logger.warning("No content provided to GYGParser.")
            return []

        logger.info("GYGParser starting parsing.")
        results = []

        try:
            tree = SelectolaxHTMLParser(content)
            # Selector targets the individual location divs within the accordion structure
            location_nodes = tree.css("#accordion-locations div.location")
            logger.info(f"Found {len(location_nodes)} location nodes.")

            for node in location_nodes:
                try:
                    # Extract data directly from attributes
                    address = node.attributes.get("data-address")
                    name = node.attributes.get("data-name")
                    source_url = node.attributes.get(
                        "data-url"
                    )  # The location's specific page URL
                    longitude = node.attributes.get("data-longitude")
                    latitude = node.attributes.get("data-latitude")
                    # Extract categories to check for drive-thru
                    categories_class = node.attributes.get("class", "").lower()
                    has_drive_thru = (
                        "category-drive-thru" in categories_class
                        or "drive thru" in categories_class
                    )  # Adjust if class name differs

                    if not address:
                        logger.warning(
                            f"Skipping location node, missing 'data-address'. Name: {name or 'N/A'}"
                        )
                        continue
                    if not name:
                        logger.warning(
                            f"Location node missing 'data-name'. Address: {address}"
                        )
                        # Decide whether to skip or use a placeholder
                        # continue
                        name = "Unknown GYG Location"  # Example placeholder

                    location_data = {
                        "name": name,
                        "address": address,
                        "source_url": source_url,
                        "latitude": latitude,
                        "longitude": longitude,
                        "drive_thru": has_drive_thru,
                    }
                    results.append(location_data)
                    # logger.debug(f"Extracted GYG Location: {name} - Drive Thru: {has_drive_thru}") # Optional Debug

                except Exception as e:
                    # Log error for a specific node but continue with others
                    node_id = node.attributes.get("id", "N/A")
                    logger.error(
                        f"Error processing individual GYG location node (id: {node_id}): {e}",
                        exc_info=True,
                    )

        except Exception as e:
            logger.error(
                f"Error parsing Guzman y Gomez locations page: {e}", exc_info=True
            )
            return []  # Return empty list on major parsing failure

        logger.info(f"GYGParser finished, returning {len(results)} raw items.")
        return results
