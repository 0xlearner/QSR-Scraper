import logging
from dataclasses import dataclass
from datetime import datetime
from typing import List, Dict, Any, Optional

from selectolax.parser import HTMLParser

from scraper_system.interfaces.parser_interface import ParserInterface
from scraper_system.interfaces.fetcher_interface import FetcherInterface

logger = logging.getLogger(__name__)


@dataclass
class GyGLocation:
    business_name: str
    address: str
    source_url: str
    source: str
    drive_thru: bool
    scraped_date: datetime


class GygParser(ParserInterface):
    """
    Parses Guzman y Gomez locations from their website.
    Focuses only on scraping the raw data, leaving transformation to the transformer.
    """

    def __init__(self, fetcher: Optional[FetcherInterface] = None):
        self.fetcher = fetcher
        logger.info("GygParser initialized")

    async def parse(
        self, content: str, content_type: Optional[str], config: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Main parsing method that scrapes GYG locations and processes them
        """
        if self.fetcher is None:
            logger.warning("No fetcher provided to GygParser, using initial content")
            if not content:
                logger.error("No content provided to GygParser")
                return []
            html_content = content
        else:
            # URL of the locations page
            url = "https://www.guzmanygomez.com.au/locations/"

            # Set headers to mimic a browser visit
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            }

            fetcher_config = config.get("fetcher_options", {})
            if not fetcher_config.get("headers"):
                fetcher_config["headers"] = headers

            # Fetch the content
            html_content, _, status_code = await self.fetcher.fetch(url, fetcher_config)

            if not html_content:
                logger.error(f"Failed to fetch GYG locations (Status: {status_code})")
                return []

        # Scrape the locations
        locations = self._scrape_locations(html_content)

        # Convert to simple dictionaries for the transformer
        result = []
        for loc in locations:
            result.append(
                {
                    "brand": "Guzman Y Gomez",  # Explicitly add the brand
                    "business_name": loc.business_name,
                    "raw_address": loc.address,
                    "drive_thru": loc.drive_thru,
                    "source_url": loc.source_url,
                    "source": loc.source,
                }
            )

        logger.info(f"GygParser finished, returning {len(result)} items.")
        return result

    def _scrape_locations(self, html_content: str) -> List[GyGLocation]:
        """
        Scrapes location data from HTML content
        """
        locations = []

        try:
            # Parse the HTML content with selectolax
            html = HTMLParser(html_content)

            # Find all location divs
            location_divs = html.css("div.location")

            for div in location_divs:
                # Extract the data attributes
                address = div.attributes.get("data-address")
                name = div.attributes.get("data-name")
                url = div.attributes.get("data-url")
                # Extract categories to check for drive-thru
                categories_class = div.attributes.get("class", "").lower()
                has_drive_thru = (
                    "category-drive-thru" in categories_class
                    or "drive thru" in categories_class
                )

                if address:  # Only add if address exists
                    locations.append(
                        GyGLocation(
                            address=address,
                            source_url=url,
                            source="gyg",
                            business_name="Guzman Y Gomez " + name,
                            drive_thru=has_drive_thru,
                            scraped_date=datetime.now(),
                        )
                    )

            logger.info(f"Found {len(locations)} GYG locations")
            return locations

        except Exception as e:
            logger.error(f"Error scraping GYG locations: {e}", exc_info=True)
            return []
