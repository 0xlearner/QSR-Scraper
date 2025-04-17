import abc
from typing import Optional, Tuple, Dict, Any

class FetcherInterface(abc.ABC):
    """Interface for fetching web content."""

    @abc.abstractmethod
    async def fetch(self, url: str, config: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
        """
        Fetches content from a given URL.

        Args:
            url: The URL to fetch.
            config: Plugin-specific configuration (e.g., headers, timeouts).

        Returns:
            A tuple containing:
            - The fetched content as a string (or None on failure).
            - The detected content type (e.g., 'text/html', 'application/json') or None.
        """
        pass
