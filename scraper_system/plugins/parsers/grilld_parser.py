import logging
import asyncio
from typing import List, Dict, Any, Optional
from urllib.parse import urljoin # For creating absolute URLs

from selectolax.parser import HTMLParser as SelectolaxHTMLParser

from scraper_system.interfaces.parser_interface import ParserInterface
from scraper_system.interfaces.fetcher_interface import FetcherInterface # Need this type hint

logger = logging.getLogger(__name__)

BASE_URL = "https://grilld.com.au"

class GrilldParser(ParserInterface):
    """
    Custom parser for Grill'd website.
    Finds restaurant location links on the main page, fetches each location page,
    and extracts the address.
    """

    def __init__(self, fetcher: Optional[FetcherInterface] = None):
        if fetcher is None:
            # This should not happen if Orchestrator injects correctly, but good practice
            raise ValueError("GrilldParser requires a Fetcher instance.")
        self.fetcher = fetcher
        logger.info("GrilldParser initialized with a fetcher.")

    async def parse_detail_page(self, content: str, url: str) -> Optional[str]:
        """Parses the detail page to extract the address."""
        if not content:
            return None
        try:
            tree = SelectolaxHTMLParser(content)
            # Selector for the address link based on the example HTML
            address_node = tree.css_first('a.details-text-link[href*="maps.google.com"]')
            if address_node:
                address = address_node.text(strip=True)
                logger.debug(f"Extracted address '{address}' from {url}")
                return address
            else:
                logger.warning(f"Address link not found on detail page: {url}")
                return None
        except Exception as e:
            logger.error(f"Error parsing detail page {url}: {e}", exc_info=True)
            return None

    async def parse(self, content: str, content_type: Optional[str], config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Parses the main restaurant list page, fetches details, and returns final data.
        """
        if not content:
            logger.warning("No initial content provided to GrilldParser.")
            return []

        logger.info("GrilldParser starting main page parsing.")
        final_results = []
        detail_tasks = []

        try:
            tree = SelectolaxHTMLParser(content)
            # Selector for the location links
            location_links = tree.css("div.c-body-rich-text a.simple-text-link")
            logger.info(f"Found {len(location_links)} potential location links on the main page.")

            fetcher_config = config.get("detail_fetcher_options", config.get("fetcher_options", {})) # Use specific or fallback config

            for link_node in location_links:
                relative_url = link_node.attributes.get('href')
                name = link_node.text(strip=True)

                if not relative_url or not relative_url.startswith('/restaurants/'):
                    logger.debug(f"Skipping link, doesn't seem like a valid location: {relative_url}")
                    continue

                # Construct absolute URL
                detail_url = urljoin(BASE_URL, relative_url)
                logger.debug(f"Queueing detail fetch for: {name} ({detail_url})")

                # Create a task to fetch and parse the detail page
                # We pass necessary info (name, url) along with the fetch task
                detail_tasks.append(self.fetch_and_parse_detail(detail_url, name, fetcher_config))

            # Run all detail page fetching and parsing concurrently
            results_from_details = await asyncio.gather(*detail_tasks)

            # Collect valid results
            for result in results_from_details:
                if result: # Filter out None values (failures)
                    final_results.append(result)

        except Exception as e:
            logger.error(f"Error processing main Grill'd page: {e}", exc_info=True)

        logger.info(f"GrilldParser finished, returning {len(final_results)} items.")
        return final_results

    async def fetch_and_parse_detail(self, url: str, name: str, fetcher_config: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Helper coroutine to fetch a detail page, parse it, and return the combined data."""
        logger.debug(f"Executing detail fetch for {url}")
        detail_content, _ = await self.fetcher.fetch(url, fetcher_config) # Use injected fetcher

        if detail_content:
            address = await self.parse_detail_page(detail_content, url) # This is now simpler
            if address:
                return {
                    "name": name,
                    "address": address,
                    "source_url": url
                }
            else:
                 logger.warning(f"Could not extract address for {name} from {url}")
        else:
            logger.warning(f"Failed to fetch detail page: {url}")

        return None # Indicate failure for this item
