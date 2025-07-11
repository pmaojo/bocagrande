"""Dependency providers for FastAPI endpoints."""

from typing import Generator
from functools import lru_cache

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.orm import Session

from app.database.session import SessionLocal
from app.logger import Logger

from app.config import settings as app_settings, Settings

# Repositories
from app.repositories.project_repository import ProjectRepositoryImpl
from app.repositories.asset_content_repository import AssetContentRepositoryImpl
from app.repositories.process_repository import ProcessRepositoryImpl
from app.repositories import user_repository
from app.interfaces.repositories import IProjectRepository
from app.interfaces.asset_content_repository import IAssetContentRepository
from app.interfaces.process_repository import IProcessRepository
from app.services.app_setting_service import AppSettingService

# Adapters
from app.adapters.local_file_storage_adapter import LocalFileStorageAdapter
from app.interfaces.file_storage_port import FileStoragePort
from app.vectorstore.chroma import ChromaDB
from app.adapters.chroma_vector_store_adapter import ChromaVectorStoreAdapter
from app.interfaces.vector_store_port import VectorStorePort
from app.adapters.celery_task_queue_adapter import CeleryTaskQueueAdapter
from app.interfaces.task_queue_port import TaskQueuePort
from app.adapters.groq_llm_adapter import GroqLLMAdapter
from app.interfaces.llm_service_port import LLMServicePort
from app.adapters.file_system_pipeline_definition_store_adapter import (
    FileSystemPipelineDefinitionStoreAdapter,
)
from app.interfaces.pipeline_definition_store_port import PipelineDefinitionStorePort
from app.adapters.process_scheduler_adapter import ProcessSchedulerAdapter
from app.interfaces.process_scheduler_port import ProcessSchedulerPort

# Processing components
from app.processing.pipeline_repository import PipelineRepository
from app.processing.pipeline_runner import PipelineRunner
from app.processing.pipeline_manager import PipelineManager

# Services
from app.services.project_service import ProjectService

logger = Logger()
security = HTTPBearer()


@lru_cache()
def get_settings() -> Settings:
    return app_settings


def get_logger() -> Logger:
    return logger


def get_db() -> Generator[Session, None, None]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@lru_cache()
def get_app_setting_service() -> AppSettingService:
    return AppSettingService()


# Repository providers
@lru_cache()
def get_project_repository() -> IProjectRepository:
    return ProjectRepositoryImpl()


@lru_cache()
def get_asset_content_repository() -> IAssetContentRepository:
    return AssetContentRepositoryImpl()


@lru_cache()
def get_process_repository() -> IProcessRepository:
    return ProcessRepositoryImpl()


# Adapter providers
@lru_cache()
def get_file_storage_adapter(
    settings: Settings = Depends(get_settings),
) -> FileStoragePort:
    return LocalFileStorageAdapter(upload_dir_base=settings.upload_dir)


@lru_cache()
def get_chroma_db_manager(settings: Settings = Depends(get_settings)) -> ChromaDB:
    return ChromaDB(
        persist_path=settings.chromadb_url,
        collection_name="panda_etl_default_collection",
        settings=settings,
    )


@lru_cache()
def get_vector_store_adapter(
    manager: ChromaDB = Depends(get_chroma_db_manager),
    log: Logger = Depends(get_logger),
) -> VectorStorePort:
    return ChromaVectorStoreAdapter(chroma_manager=manager, logger=log)


@lru_cache()
def get_task_queue_adapter() -> TaskQueuePort:
    return CeleryTaskQueueAdapter()


@lru_cache()
def get_groq_llm_adapter(
    settings: Settings = Depends(get_settings),
    db: Session = Depends(get_db),
    service: AppSettingService = Depends(get_app_setting_service),
) -> LLMServicePort:
    api_key = settings.groq_api_key or service.get_value(db, "groq_api_key")
    base_url = service.get_value(db, "groq_api_base_url")
    return GroqLLMAdapter(api_key=api_key, api_base_url=base_url)


@lru_cache()
def get_pipeline_definition_store_adapter(
    settings: Settings = Depends(get_settings),
) -> PipelineDefinitionStorePort:
    return FileSystemPipelineDefinitionStoreAdapter(
        base_pipelines_dir=settings.pipelines_dir
    )


@lru_cache()
def get_process_scheduler_adapter(
    log: Logger = Depends(get_logger),
) -> ProcessSchedulerPort:
    return ProcessSchedulerAdapter(logger=log)


# Pipeline components
@lru_cache()
def get_pipeline_repository(
    store: PipelineDefinitionStorePort = Depends(get_pipeline_definition_store_adapter),
    log: Logger = Depends(get_logger),
) -> PipelineRepository:
    return PipelineRepository(store=store, logger=log)


@lru_cache()
def get_pipeline_runner(
    repo: PipelineRepository = Depends(get_pipeline_repository),
    log: Logger = Depends(get_logger),
) -> PipelineRunner:
    return PipelineRunner(repository=repo, logger=log)


@lru_cache()
def get_pipeline_manager(
    db: Session = Depends(get_db),
    proc_repo: IProcessRepository = Depends(get_process_repository),
    repository: PipelineRepository = Depends(get_pipeline_repository),
    runner: PipelineRunner = Depends(get_pipeline_runner),
    sched_port: ProcessSchedulerPort = Depends(get_process_scheduler_adapter),
    log: Logger = Depends(get_logger),
) -> PipelineManager:
    manager = PipelineManager(
        repository=repository,
        runner=runner,
        process_repo=proc_repo,
        db=db,
        scheduler_port=sched_port,
        logger=log,
    )
    if isinstance(sched_port, ProcessSchedulerAdapter):
        sched_port.set_executor_callback(manager._execute_pipeline_by_id)
        sched_port.start()
    return manager


# Project service


def get_project_service(
    db: Session = Depends(get_db),
    project_repo: IProjectRepository = Depends(get_project_repository),
    asset_content_repo: IAssetContentRepository = Depends(get_asset_content_repository),
    process_repo: IProcessRepository = Depends(get_process_repository),
    file_storage: FileStoragePort = Depends(get_file_storage_adapter),
    vector_store: VectorStorePort = Depends(get_vector_store_adapter),
    task_queue: TaskQueuePort = Depends(get_task_queue_adapter),
) -> ProjectService:
    return ProjectService(
        db=db,
        repo=project_repo,
        asset_content_repo=asset_content_repo,
        process_repo=process_repo,
        file_storage=file_storage,
        vector_store=vector_store,
        task_queue=task_queue,
    )


def get_api_key(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db),
) -> str:
    token = credentials.credentials
    api_key = user_repository.get_user_by_api_key(db, token)
    if not api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid API key"
        )
    return token
