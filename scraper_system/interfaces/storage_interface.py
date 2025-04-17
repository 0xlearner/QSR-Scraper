import abc
from typing import List, Dict, Any

class StorageInterface(abc.ABC):
    """Interface for storing scraped data."""

    @abc.abstractmethod
    async def save(self, data: List[Dict[str, Any]], config: Dict[str, Any]):
        """
        Saves the extracted data.

        Args:
            data: A list of dictionaries representing the scraped items.
            config: Plugin-specific configuration (e.g., filename, connection string).
        """
        pass
