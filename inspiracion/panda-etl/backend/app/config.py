import os
from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    # API Keys
    groq_api_key: Optional[str] = None
    pandaetl_api_key: Optional[str] = None

    # Database
    sqlalchemy_database_url: str = "sqlite:///./instance/app.db"
    chromadb_url: str = os.path.join(
        os.path.dirname(__file__), "..", "instance", "chromadb"
    )
    upload_dir: str = os.path.join(os.path.dirname(__file__), "..", "uploads")
    process_dir: str = os.path.join(os.path.dirname(__file__), "..", "processed")
    api_server_url: str = "https://api.domer.ai"
    pandaetl_server_url: str = "https://api.panda-etl.ai/"
    log_file_path: str = os.path.join(os.path.dirname(__file__), "..", "pandaetl.log")
    max_retries: int = 3
    max_relevant_docs: int = 10
    max_file_size: int = 20 * 1024 * 1024
    chroma_batch_size: int = 5

    # OpenAI embeddings config
    use_openai_embeddings: bool = False
    openai_api_key: str = ""
    openai_embedding_model: str = "text-embedding-ada-002"

    # Google OAuth
    google_client_id: str = ""

    # Allowed content types for file uploads
    allowed_content_types: tuple[str, ...] = (
        "application/pdf",
        "text/plain",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    )

    # Extraction References for chat
    chat_extraction_doc_threshold: float = 0.5
    chat_extraction_max_docs: int = 50

    # PandaETL api key
    pandaetl_api_key: Optional[str] = None

    # Pipeline definitions directory
    pipelines_dir: str = os.environ.get(
        "PIPELINES_DIR",
        os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "pipelines_data")
    ) # Changed "pipelines" to "pipelines_data" to avoid potential conflict if "pipelines" is a module name
      # Used abspath to ensure __file__ is absolute before dirname operations.

    class Config:
        frozen = True


settings = Settings()
