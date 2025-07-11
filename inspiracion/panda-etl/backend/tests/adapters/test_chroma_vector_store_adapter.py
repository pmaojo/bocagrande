from unittest.mock import MagicMock

from app.adapters.chroma_vector_store_adapter import ChromaVectorStoreAdapter
from app.logger import Logger


def test_logger_warning_on_collection_mismatch():
    manager = MagicMock()
    manager._docs_collection.name = "default"
    manager.add_docs.return_value = ["id1"]

    logger = MagicMock(spec=Logger)

    adapter = ChromaVectorStoreAdapter(chroma_manager=manager, logger=logger)
    adapter.add_item("text", {}, collection_name="other")

    logger.warning.assert_called_once()


def test_logger_warning_method_saves_log_entry():
    manager = MagicMock()
    manager._docs_collection.name = "default"
    manager.add_docs.return_value = ["id1"]

    logger = Logger(save_logs=False)

    adapter = ChromaVectorStoreAdapter(chroma_manager=manager, logger=logger)
    adapter.add_item("text", {}, collection_name="other")

    assert logger.logs
    entry = logger.logs[0]
    assert entry["level"] == "WARNING"
    assert "collection_name" in entry["msg"]
