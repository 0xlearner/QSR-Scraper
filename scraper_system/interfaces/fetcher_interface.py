import abc
from typing import Optional, Tuple, Dict, Any


class FetcherInterface(abc.ABC):
    """Interface for fetching web content."""

    @abc.abstractmethod
    async def fetch(
        self, url: str, config: Dict[str, Any]
    ) -> Tuple[Optional[str], Optional[str], Optional[int]]:
        """
        Fetches content from a URL.
        Returns: Tuple of (content, content_type, status_code)
        """
        pass
