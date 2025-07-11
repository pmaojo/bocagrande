from abc import ABC, abstractmethod
from typing import Optional

class VectorStorePort(ABC):
    @abstractmethod
    def add_item(self, text: str, metadata: dict, collection_name: Optional[str] = None) -> str:
        """
        Adds an item (text and metadata) to the specified vector store collection.
        Returns a unique identifier for the stored item (vector_id).
        """
        pass

    @abstractmethod
    def delete_item(self, vector_id: str, collection_name: Optional[str] = None) -> None:
        """
        Deletes an item from the specified vector store collection using its vector_id.
        """
        pass
