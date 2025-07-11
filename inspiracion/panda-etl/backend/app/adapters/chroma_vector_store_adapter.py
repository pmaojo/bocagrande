from typing import Optional, List

from app.interfaces.vector_store_port import VectorStorePort
from app.vectorstore.chroma import ChromaDB
from app.logger import Logger

class ChromaVectorStoreAdapter(VectorStorePort):
    """Adapter to interact with ChromaDB implementations."""

    def __init__(self, chroma_manager: ChromaDB, logger: Optional[Logger] = None):
        """Create adapter.

        Parameters
        ----------
        chroma_manager:
            Instance of :class:`ChromaDB` already configured.
        logger:
            Optional logger instance; a default ``Logger`` will be created if not
            supplied.
        """
        self.chroma_manager = chroma_manager
        self._logger = logger or Logger()

    def add_item(self, text: str, metadata: dict, collection_name: Optional[str] = None) -> str:
        """
        Adds a single item (text and metadata) to the vector store.
        Returns a unique identifier for the stored item (vector_id).

        Note: The `collection_name` parameter is currently ignored, as the underlying
        ChromaDB manager instance is expected to be pre-configured for a specific collection.
        If a different collection is specified, a warning is logged.
        """
        if collection_name and collection_name != self.chroma_manager._docs_collection.name:
            self._logger.warning(
                f"ChromaVectorStoreAdapter: `collection_name` ('{collection_name}') provided "
                f"differs from manager's configured collection ('{self.chroma_manager._docs_collection.name}'). "
                "Ignoring provided `collection_name` and using manager's default."
            )

        # ChromaDB's add_docs expects iterables.
        # It also generates an ID if not provided, which we want as the return vector_id.
        # We need to capture this generated ID.
        # For simplicity, we let ChromaDB generate the ID and we'll return the first one (as we add one doc).

        # The add_docs method in ChromaDB generates UUIDs if ids are not provided.
        # It returns the list of IDs used.
        ids: List[str] = self.chroma_manager.add_docs(
            docs=[text],
            metadatas=[metadata]
            # ids parameter ommitted to let ChromaDB generate it
        )
        if not ids:
            raise ValueError("Failed to add item to ChromaDB or no ID was returned.")
        return ids[0] # Return the generated ID for the single document

    def delete_item(self, vector_id: str, collection_name: Optional[str] = None) -> None:
        """
        Deletes an item from the vector store collection using its vector_id.

        Note: The `collection_name` parameter is currently ignored. See add_item note.
        """
        if collection_name and collection_name != self.chroma_manager._docs_collection.name:
            self._logger.warning(
                f"ChromaVectorStoreAdapter: `collection_name` ('{collection_name}') provided "
                f"differs from manager's configured collection ('{self.chroma_manager._docs_collection.name}'). "
                "Ignoring provided `collection_name` and using manager's default."
            )

        # ChromaDB's delete_docs expects a list of ids.
        self.chroma_manager.delete_docs(ids=[vector_id])
