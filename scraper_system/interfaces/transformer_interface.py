import abc
from typing import List, Dict, Any

class TransformerInterface(abc.ABC):
    """Interface for transforming scraped data before storage."""

    @abc.abstractmethod
    async def transform(self, data: List[Dict[str, Any]], config: Dict[str, Any], site_name: str) -> List[Dict[str, Any]]:
        """
        Transforms raw scraped data into the final desired structure.

        Args:
            data: A list of dictionaries representing the raw scraped items from the parser.
            config: Plugin-specific configuration (e.g., parsing rules, default values).
            site_name: The name of the website being scraped (useful for 'source').

        Returns:
            A list of dictionaries, where each dictionary represents a transformed item,
            ready for storage. Returns an empty list if no items remain after transformation
            or on critical errors.
        """
        pass
