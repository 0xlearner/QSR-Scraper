import logging
import asyncio
import json
import xml.etree.ElementTree as ET
from typing import List, Dict, Any, Optional

from selectolax.parser import HTMLParser as SelectolaxHTMLParser

from scraper_system.interfaces.parser_interface import ParserInterface
from scraper_system.interfaces.fetcher_interface import FetcherInterface

logger = logging.getLogger(__name__)

BASE_URL = "https://www.nandos.com.au"
SITEMAP_URL = "https://www.nandos.com.au/sitemap.xml"


class NandosParser(ParserInterface):
    """
    Parses Nandos Australia restaurant locations using sitemap.xml discovery:
    1. Fetch sitemap.xml to get all restaurant URLs
    2. Filter URLs to only restaurant pages
    3. Fetch each restaurant page and extract JSON-LD data
    """

    def __init__(self, fetcher: Optional[FetcherInterface] = None):
        if fetcher is None:
            raise ValueError("NandosParser requires a Fetcher instance.")
        self.fetcher = fetcher
        logger.info("NandosParser initialized with a fetcher.")

    async def parse(
        self, content: str, content_type: Optional[str], config: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Main parsing method that uses sitemap.xml to discover restaurant URLs.
        """
        logger.info("NandosParser starting sitemap-based parsing.")
        final_results = []

        try:
            # Get fetcher configuration
            fetcher_config = config.get(
                "detail_fetcher_options", config.get("fetcher_options", {})
            )

            # Get parser options
            parser_opts = config.get("parser_options", {})
            max_concurrent = parser_opts.get("max_concurrent_requests", 10)

            # Phase 1: Fetch sitemap.xml and extract restaurant URLs
            restaurant_urls = await self._fetch_restaurant_urls_from_sitemap(fetcher_config)
            logger.info(f"Found {len(restaurant_urls)} restaurant URLs from sitemap.")

            if not restaurant_urls:
                logger.warning("No restaurant URLs found in sitemap.")
                return []

            # Phase 2: Fetch all restaurant detail pages and extract JSON-LD data with rate limiting
            restaurant_data = await self._fetch_restaurant_details_with_rate_limit(
                restaurant_urls, fetcher_config, max_concurrent
            )

            # Filter out None results and collect valid data
            final_results = [data for data in restaurant_data if data is not None]

        except Exception as e:
            logger.error(f"Error processing Nandos sitemap: {e}", exc_info=True)

        logger.info(f"NandosParser finished, returning {len(final_results)} items.")
        return final_results

    async def _fetch_restaurant_urls_from_sitemap(
        self, fetcher_config: Dict[str, Any]
    ) -> List[str]:
        """
        Fetch sitemap.xml and extract restaurant URLs.
        """
        restaurant_urls = []

        try:
            logger.debug(f"Fetching sitemap: {SITEMAP_URL}")
            content, _, status_code = await self.fetcher.fetch(SITEMAP_URL, fetcher_config)

            if not content:
                logger.error(f"Failed to fetch sitemap {SITEMAP_URL} (Status: {status_code})")
                return restaurant_urls

            # Parse XML sitemap
            restaurant_urls = self._parse_sitemap_xml(content)
            logger.info(f"Extracted {len(restaurant_urls)} restaurant URLs from sitemap")

        except Exception as e:
            logger.error(f"Error fetching/parsing sitemap: {e}", exc_info=True)

        return restaurant_urls

    def _parse_sitemap_xml(self, xml_content: str) -> List[str]:
        """
        Parse sitemap XML and extract restaurant URLs.
        """
        restaurant_urls = []

        try:
            # Parse XML
            root = ET.fromstring(xml_content)

            # Handle XML namespaces
            namespaces = {
                'sitemap': 'http://www.sitemaps.org/schemas/sitemap/0.9'
            }

            # Find all URL elements
            urls = root.findall('.//sitemap:url/sitemap:loc', namespaces)

            # If namespace approach doesn't work, try without namespace
            if not urls:
                urls = root.findall('.//loc')

            for url_elem in urls:
                url = url_elem.text
                if url and self._is_restaurant_url(url):
                    restaurant_urls.append(url)
                    logger.debug(f"Found restaurant URL: {url}")

        except ET.ParseError as e:
            logger.error(f"Failed to parse sitemap XML: {e}")
        except Exception as e:
            logger.error(f"Error parsing sitemap: {e}")

        return restaurant_urls

    def _is_restaurant_url(self, url: str) -> bool:
        """
        Check if URL is a restaurant page URL.
        Restaurant URLs have format: https://www.nandos.com.au/restaurants/state/restaurant-name
        """
        if not url:
            return False

        # Must contain /restaurants/
        if "/restaurants/" not in url:
            return False

        # Remove base URL to get path
        path = url.replace(BASE_URL, "")

        # Split path into parts
        parts = [p for p in path.split("/") if p]

        # Should have at least 3 parts: restaurants, state, restaurant-name
        # Format: ['restaurants', 'state', 'restaurant-name']
        if len(parts) >= 3 and parts[0] == "restaurants":
            # Exclude state-only URLs (only 2 parts)
            return len(parts) > 2

        return False

    async def _fetch_restaurant_details_with_rate_limit(
        self, restaurant_urls: List[str], fetcher_config: Dict[str, Any], max_concurrent: int = 10
    ) -> List[Optional[Dict[str, Any]]]:
        """
        Fetch all restaurant detail pages with rate limiting and batch processing.
        """
        final_results = []
        semaphore = asyncio.Semaphore(max_concurrent)

        logger.info(f"Processing {len(restaurant_urls)} restaurants with max {max_concurrent} concurrent requests")

        async def fetch_with_semaphore(url: str) -> Optional[Dict[str, Any]]:
            async with semaphore:
                try:
                    result = await self._fetch_and_parse_restaurant_page(url, fetcher_config)
                    # Add small delay between requests to be respectful
                    await asyncio.sleep(0.1)
                    return result
                except Exception as e:
                    logger.error(f"Error fetching restaurant page {url}: {e}")
                    return None

        # Process URLs in batches to avoid overwhelming the server
        batch_size = 50
        for i in range(0, len(restaurant_urls), batch_size):
            batch_urls = restaurant_urls[i:i + batch_size]
            logger.info(f"Processing batch {i//batch_size + 1}/{(len(restaurant_urls) + batch_size - 1)//batch_size} ({len(batch_urls)} restaurants)")

            # Create tasks for this batch
            batch_tasks = [fetch_with_semaphore(url) for url in batch_urls]

            # Execute batch concurrently
            batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)

            # Handle results
            for j, result in enumerate(batch_results):
                if isinstance(result, Exception):
                    logger.error(f"Exception in batch processing: {result}")
                    final_results.append(None)
                else:
                    final_results.append(result)

            # Brief pause between batches
            if i + batch_size < len(restaurant_urls):
                logger.debug("Pausing briefly between batches...")
                await asyncio.sleep(1.0)

        return final_results

    async def _fetch_restaurant_details(
        self, restaurant_urls: List[str], fetcher_config: Dict[str, Any]
    ) -> List[Optional[Dict[str, Any]]]:
        """
        Legacy method - use _fetch_restaurant_details_with_rate_limit instead.
        """
        return await self._fetch_restaurant_details_with_rate_limit(restaurant_urls, fetcher_config)

    async def _fetch_and_parse_restaurant_page(
        self, restaurant_url: str, fetcher_config: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """
        Fetch a single restaurant page and extract the JSON-LD data.
        """
        try:
            logger.debug(f"Fetching restaurant page: {restaurant_url}")
            content, _, status_code = await self.fetcher.fetch(restaurant_url, fetcher_config)

            if not content:
                logger.error(f"Failed to fetch restaurant page {restaurant_url} (Status: {status_code})")
                return None

            # Extract JSON-LD data
            restaurant_data = self._extract_json_ld_data(content, restaurant_url)

            if restaurant_data:
                # Add source information
                restaurant_data["source_url"] = restaurant_url
                restaurant_data["source"] = "nandos"
                restaurant_data["brand"] = "Nandos"

                logger.debug(f"Successfully extracted data for: {restaurant_data.get('name', 'Unknown')}")
                return restaurant_data
            else:
                logger.warning(f"No JSON-LD data found for {restaurant_url}")
                return None

        except Exception as e:
            logger.error(f"Error fetching/parsing restaurant page {restaurant_url}: {e}", exc_info=True)
            return None

    def _extract_json_ld_data(self, content: str, url: str) -> Optional[Dict[str, Any]]:
        """
        Extract restaurant data from JSON-LD script tag.
        Specifically looks for the Restaurant schema type.
        """
        try:
            tree = SelectolaxHTMLParser(content)

            # Find all script tags with type="application/ld+json"
            script_tags = tree.css('script[type="application/ld+json"]')

            for script_tag in script_tags:
                script_content = script_tag.text()
                if not script_content:
                    continue

                try:
                    # Parse JSON content
                    json_data = json.loads(script_content)

                    # Check if this is a Restaurant schema
                    if (isinstance(json_data, dict) and
                        json_data.get("@type") == "Restaurant"):

                        logger.debug(f"Found Restaurant JSON-LD data for {url}")
                        return json_data

                except json.JSONDecodeError as e:
                    logger.debug(f"Failed to parse JSON-LD content: {e}")
                    continue

            logger.warning(f"No Restaurant JSON-LD data found in {url}")
            return None

        except Exception as e:
            logger.error(f"Error extracting JSON-LD data from {url}: {e}", exc_info=True)
            return None
