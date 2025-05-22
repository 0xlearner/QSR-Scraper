import logging
import asyncio
from typing import Optional, Tuple, Dict, Any
import httpx
from scraper_system.interfaces.fetcher_interface import FetcherInterface

logger = logging.getLogger(__name__)


class AsyncHTTPXFetcher(FetcherInterface):
    """
    Fetches web content asynchronously using httpx, with optional ScraperAPI proxy support.
    """

    SCRAPERAPI_ENDPOINT = "https://api.scraperapi.com"

    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize the fetcher with optional default configuration.

        Args:
            config: Optional default configuration that will be merged with
                   request-specific config in fetch() calls.
        """
        self.default_config = config or {}

    async def fetch(
        self, url: str, config: Dict[str, Any] = None
    ) -> Tuple[Optional[str], Optional[str], Optional[int]]:
        """
        Fetches content using httpx, optionally via ScraperAPI.

        Config options:
        - headers (dict): Headers to send with the request. Will be forwarded by ScraperAPI if used.
        - timeout (int): Request timeout in seconds (default: 30). Applies to the connection to ScraperAPI or the direct site.
        - use_scraperapi (bool): Set to True to use ScraperAPI (default: False).
        - scraperapi_key (str): Your ScraperAPI API key (required if use_scraperapi is True).
        - scraperapi_options (dict): Additional parameters for the ScraperAPI request
                                      (e.g., {'country_code': 'us', 'render': 'true'}).
        - max_retries (int): Maximum number of retry attempts for transient errors (default: 3).
        - retry_delay (float): Base delay between retries in seconds (default: 1.0).
        - retry_backoff (float): Multiplier for exponential backoff between retries (default: 2.0).
        """
        # Merge default config with request-specific config
        merged_config = {**self.default_config}
        if config:
            for key, value in config.items():
                if (
                    isinstance(value, dict)
                    and key in merged_config
                    and isinstance(merged_config[key], dict)
                ):
                    merged_config[key] = {**merged_config[key], **value}
                else:
                    merged_config[key] = value

        headers = merged_config.get("headers", {})
        timeout = merged_config.get("timeout", 30)
        use_scraperapi = merged_config.get("use_scraperapi", False)
        scraperapi_key = merged_config.get("scraperapi_key")
        scraperapi_options = merged_config.get("scraperapi_options", {})
        
        # Retry configuration
        max_retries = merged_config.get("max_retries", 3)
        retry_delay = merged_config.get("retry_delay", 1.0)
        retry_backoff = merged_config.get("retry_backoff", 2.0)

        request_url = url
        params = None
        log_target = url  # For logging purposes

        if use_scraperapi:
            if not scraperapi_key:
                logger.error(
                    "ScraperAPI is enabled ('use_scraperapi': True) but 'scraperapi_key' is missing in config."
                )
                return None, None, None

            request_url = self.SCRAPERAPI_ENDPOINT
            # Base ScraperAPI parameters
            params = {
                "api_key": scraperapi_key,
                "url": url,  # Pass the original target URL to ScraperAPI
            }
            # Add any extra options provided
            params.update(scraperapi_options)

            log_target = f"{url} (via ScraperAPI)"
            logger.debug(
                f"Routing fetch for {url} through ScraperAPI with params: { {k: v for k, v in params.items() if k != 'api_key'} }"
            )  # Don't log api key

        else:
            logger.debug(f"Fetching URL directly: {url}")

        retry_count = 0
        while retry_count <= max_retries:
            try:
                async with httpx.AsyncClient(
                    headers=headers, timeout=timeout, follow_redirects=True
                ) as client:
                    response = await client.get(request_url, params=params)
                    response.raise_for_status()

                    content_type = response.headers.get("content-type", "").lower()
                    content = response.text
                    status_code = response.status_code

                    logger.debug(
                        f"Fetched {log_target} successfully. Status: {status_code}, Content-Type: {content_type}"
                    )
                    return content, content_type, status_code

            except (httpx.RequestError, httpx.ReadTimeout, httpx.ConnectTimeout, httpx.ReadError, httpx.ConnectError, httpx.RemoteProtocolError) as e:
                retry_count += 1
                if retry_count <= max_retries:
                    # Calculate exponential backoff delay
                    delay = retry_delay * (retry_backoff ** (retry_count - 1))
                    logger.warning(
                        f"Transient error fetching {log_target}: {e.__class__.__name__} - {e}. "
                        f"Retrying ({retry_count}/{max_retries}) after {delay:.2f}s delay..."
                    )
                    await asyncio.sleep(delay)
                else:
                    logger.error(
                        f"HTTP Request error fetching {log_target}: {e.__class__.__name__} - {e}. "
                        f"Max retries ({max_retries}) exceeded."
                    )
            except httpx.HTTPStatusError as e:
                # Don't retry HTTP status errors (4xx, 5xx) as they're less likely to be transient
                logger.error(
                    f"HTTP Status error fetching {log_target}: Status {e.response.status_code}"
                )
                break
            except Exception as e:
                # Don't retry unexpected errors
                logger.error(
                    f"Unexpected error fetching {log_target}: {e.__class__.__name__} - {e}"
                )
                break

        return None, None, None
