import logging
import asyncio
from typing import Optional, Tuple, Dict, Any
import zendriver as zd
from scraper_system.interfaces.fetcher_interface import FetcherInterface

logger = logging.getLogger(__name__)


class ZendriverFetcher(FetcherInterface):
    """
    Fetches web content using zendriver for JavaScript-heavy sites.
    This fetcher can handle dynamic content, NUXT/Vue applications, and complex SPAs.
    """

    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize the zendriver fetcher with optional default configuration.

        Args:
            config: Optional default configuration that will be merged with
                   request-specific config in fetch() calls.
        """
        self.default_config = config or {}
        self._browser = None

    async def _get_browser(self, config: Dict[str, Any]):
        """Get or create a browser instance"""
        if self._browser is None:
            browser_config = {
                'headless': config.get('headless', True),
                'user_agent': config.get('user_agent'),
                'sandbox': config.get('sandbox', True),
                'expert': config.get('expert', False)
            }
            
            # Remove None values
            browser_config = {k: v for k, v in browser_config.items() if v is not None}
            
            logger.debug(f"Starting zendriver browser with config: {browser_config}")
            self._browser = await zd.start(**browser_config)
            
        return self._browser

    async def _close_browser(self):
        """Close the browser instance"""
        if self._browser:
            try:
                await self._browser.stop()
            except Exception as e:
                logger.warning(f"Error closing browser: {e}")
            finally:
                self._browser = None

    async def fetch(
        self, url: str, config: Dict[str, Any] = None
    ) -> Tuple[Optional[str], Optional[str], Optional[int]]:
        """
        Fetches content using zendriver for JavaScript execution.

        Config options:
        - headless (bool): Run browser in headless mode (default: True)
        - user_agent (str): Custom user agent string
        - sandbox (bool): Enable browser sandbox (default: True)
        - expert (bool): Enable expert mode with debug features (default: False)
        - timeout (int): Page load timeout in seconds (default: 30)
        - wait_for_load (float): Additional wait time after page load (default: 3.0)
        - wait_for_selector (str): CSS selector to wait for before considering page loaded
        - execute_js (str): JavaScript code to execute after page load
        - extract_data (str): JavaScript code to extract specific data (returns this instead of HTML)
        - max_retries (int): Maximum number of retry attempts (default: 2)
        - retry_delay (float): Delay between retries in seconds (default: 2.0)
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

        timeout = merged_config.get("timeout", 30)
        wait_for_load = merged_config.get("wait_for_load", 3.0)
        wait_for_selector = merged_config.get("wait_for_selector")
        execute_js = merged_config.get("execute_js")
        extract_data = merged_config.get("extract_data")
        max_retries = merged_config.get("max_retries", 2)
        retry_delay = merged_config.get("retry_delay", 2.0)

        retry_count = 0
        while retry_count <= max_retries:
            try:
                browser = await self._get_browser(merged_config)
                
                logger.debug(f"Navigating to {url} with zendriver...")
                
                # Navigate to the page
                page = await browser.get(url)
                
                # Wait for initial load
                if wait_for_load > 0:
                    logger.debug(f"Waiting {wait_for_load}s for page to load...")
                    await asyncio.sleep(wait_for_load)
                
                # Wait for specific selector if provided
                if wait_for_selector:
                    logger.debug(f"Waiting for selector: {wait_for_selector}")
                    try:
                        await page.select(wait_for_selector, timeout=timeout)
                    except Exception as e:
                        logger.warning(f"Selector {wait_for_selector} not found: {e}")
                
                # Execute custom JavaScript if provided
                if execute_js:
                    logger.debug("Executing custom JavaScript...")
                    try:
                        await page.evaluate(execute_js)
                    except Exception as e:
                        logger.warning(f"Error executing JavaScript: {e}")
                
                # Extract specific data or get page content
                if extract_data:
                    logger.debug("Extracting data with custom JavaScript...")
                    try:
                        content = await page.evaluate(extract_data)
                        # Convert to string if it's not already
                        if content is not None and not isinstance(content, str):
                            import json
                            content = json.dumps(content, ensure_ascii=False)
                    except Exception as e:
                        logger.error(f"Error extracting data: {e}")
                        content = None
                else:
                    # Get the page HTML content
                    content = await page.evaluate("document.documentElement.outerHTML")
                
                if content is None:
                    logger.error(f"No content extracted from {url}")
                    return None, None, None
                
                # Get page title for content type detection
                try:
                    title = await page.evaluate("document.title")
                    content_type = "text/html; charset=utf-8"
                    if extract_data:
                        content_type = "application/json; charset=utf-8"
                except Exception:
                    content_type = "text/html; charset=utf-8"
                
                logger.debug(f"Successfully fetched {url} with zendriver. Content length: {len(content)}")
                
                # Return success - we don't have a real status code from zendriver
                # but we can assume 200 if we got content
                return content, content_type, 200
                
            except Exception as e:
                retry_count += 1
                if retry_count <= max_retries:
                    logger.warning(
                        f"Error fetching {url} with zendriver: {e}. "
                        f"Retrying ({retry_count}/{max_retries}) after {retry_delay}s delay..."
                    )
                    # Close browser on error to start fresh
                    await self._close_browser()
                    await asyncio.sleep(retry_delay)
                else:
                    logger.error(
                        f"Failed to fetch {url} with zendriver after {max_retries} retries: {e}"
                    )
                    await self._close_browser()
                    break

        return None, None, None

    async def __aenter__(self):
        """Async context manager entry"""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit - cleanup browser"""
        await self._close_browser()

    def __del__(self):
        """Cleanup on deletion"""
        if self._browser:
            # Can't await in __del__, so just log a warning
            logger.warning("ZendriverFetcher deleted with active browser - use async context manager for proper cleanup")
