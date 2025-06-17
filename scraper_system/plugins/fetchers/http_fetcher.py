import logging
import asyncio
import urllib.parse
from typing import Optional, Tuple, Dict, Any
import httpx
from scraper_system.interfaces.fetcher_interface import FetcherInterface

logger = logging.getLogger(__name__)


class AsyncHTTPXFetcher(FetcherInterface):
    """
    Fetches web content asynchronously using httpx, with IPRoyal proxy support.
    """

    IPROYAL_PROXY_HOST = "geo.iproyal.com"
    IPROYAL_PROXY_PORT = "12321"

    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize the fetcher with optional default configuration.

        Args:
            config: Optional default configuration that will be merged with
                   request-specific config in fetch() calls.
        """
        self.default_config = config or {}

    def _build_proxy_url(self, username: str, password: str) -> str:
        """
        Builds the proxy URL with encoded credentials.

        Args:
            username: IPRoyal username
            password: IPRoyal password (including any country codes)
        """
        encoded_username = urllib.parse.quote(username)
        encoded_password = urllib.parse.quote(password)
        return f"http://{encoded_username}:{encoded_password}@{self.IPROYAL_PROXY_HOST}:{self.IPROYAL_PROXY_PORT}"

    async def fetch(
        self, url: str, config: Dict[str, Any] = None
    ) -> Tuple[Optional[str], Optional[str], Optional[int]]:
        """
        Fetches content using httpx via IPRoyal proxy.

        Config options:
        - headers (dict): Headers to send with the request
        - timeout (int): Request timeout in seconds (default: 30)
        - use_proxy (bool): Set to True to use IPRoyal proxy (default: False)
        - proxy_username (str): Your IPRoyal username (required if use_proxy is True)
        - proxy_password (str): Your IPRoyal password with optional country code (required if use_proxy is True)
        - max_retries (int): Maximum number of retry attempts for transient errors (default: 3)
        - retry_delay (float): Base delay between retries in seconds (default: 1.0)
        - retry_backoff (float): Multiplier for exponential backoff between retries (default: 2.0)
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
        use_proxy = merged_config.get("use_proxy", False)
        proxy_username = merged_config.get("proxy_username")
        proxy_password = merged_config.get("proxy_password")

        # Retry configuration
        max_retries = merged_config.get("max_retries", 3)
        retry_delay = merged_config.get("retry_delay", 1.0)
        retry_backoff = merged_config.get("retry_backoff", 2.0)

        log_target = url
        proxy_url = None

        if use_proxy:
            if not proxy_username or not proxy_password:
                logger.error(
                    "IPRoyal proxy is enabled but 'proxy_username' or 'proxy_password' is missing in config."
                )
                return None, None, None

            proxy_url = self._build_proxy_url(proxy_username, proxy_password)
            log_target = f"{url} (via IPRoyal proxy)"
            logger.debug(f"Routing fetch for {url} through IPRoyal proxy")
        else:
            logger.debug(f"Fetching URL directly: {url}")

        retry_count = 0
        while retry_count <= max_retries:
            try:
                client_kwargs = {
                    "headers": headers,
                    "timeout": timeout,
                    "follow_redirects": True,
                }

                if proxy_url:
                    client_kwargs["proxy"] = proxy_url

                async with httpx.AsyncClient(**client_kwargs) as client:
                    response = await client.get(url)
                    response.raise_for_status()

                    content_type = response.headers.get("content-type", "").lower()
                    content = response.text
                    status_code = response.status_code

                    logger.debug(
                        f"Fetched {log_target} successfully. Status: {status_code}, Content-Type: {content_type}"
                    )
                    return content, content_type, status_code

            except (
                httpx.RequestError,
                httpx.ReadTimeout,
                httpx.ConnectTimeout,
                httpx.ReadError,
                httpx.ConnectError,
                httpx.RemoteProtocolError,
            ) as e:
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
