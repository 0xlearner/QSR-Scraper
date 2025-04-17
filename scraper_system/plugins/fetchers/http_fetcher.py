import logging
from typing import Optional, Tuple, Dict, Any
import httpx
from scraper_system.interfaces.fetcher_interface import FetcherInterface

logger = logging.getLogger(__name__)

class AsyncHTTPXFetcher(FetcherInterface):
    """Fetches web content asynchronously using httpx."""

    async def fetch(self, url: str, config: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
        """Fetches content using httpx."""
        headers = config.get("headers", {})
        timeout = config.get("timeout", 30) # Default timeout 30 seconds

        try:
            async with httpx.AsyncClient(headers=headers, timeout=timeout, follow_redirects=True) as client:
                logger.debug(f"Fetching URL: {url}")
                response = await client.get(url)
                response.raise_for_status() # Raise an exception for bad status codes (4xx or 5xx)

                content_type = response.headers.get("content-type", "").lower()
                content = response.text # Let httpx handle decoding based on headers/charset

                logger.debug(f"Fetched {url} successfully. Status: {response.status_code}, Content-Type: {content_type}")
                return content, content_type

        except httpx.RequestError as e:
            logger.error(f"HTTP Request error fetching {url}: {e.__class__.__name__} - {e}")
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP Status error fetching {url}: Status {e.response.status_code}")
        except Exception as e:
            logger.error(f"Unexpected error fetching {url}: {e.__class__.__name__} - {e}")

        return None, None
