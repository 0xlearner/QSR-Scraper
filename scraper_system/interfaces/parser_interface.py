import abc
from typing import List, Dict, Any, Optional

class ParserInterface(abc.ABC):
    """Interface for parsing web content."""

    @abc.abstractmethod
    async def parse(self, content: str, content_type: Optional[str], config: Dict[str, Any]) -> List[Dict[str, Any]]: # Changed to async def
        """
        Parses the raw content to extract structured data.
        For complex parsers, this might involve fetching additional pages.

        Args:
            content: The raw content (e.g., HTML, JSON string) from the initial fetch.
            content_type: The content type hint (optional).
            config: Plugin-specific configuration (passed from site config, useful even for custom parsers for flags etc.).

        Returns:
            A list of dictionaries, where each dictionary represents a final scraped item.
            Returns an empty list if no items are found or on critical parsing errors.
        """
        pass
