import os
import uuid  # Imported for backward compatibility in tests  # noqa: F401
from typing import List, Optional, Tuple, BinaryIO # Added BinaryIO
import httpx
from fastapi import UploadFile, HTTPException
from sqlalchemy.orm import Session
from app.interfaces.repositories import IProjectRepository
from app.interfaces.asset_content_repository import IAssetContentRepository
from app.interfaces.process_repository import IProcessRepository
from app.interfaces.file_storage_port import FileStoragePort
from app.interfaces.vector_store_port import VectorStorePort
from app.interfaces.task_queue_port import TaskQueuePort # Added
from app.schemas.project import ProjectCreate, ProjectUpdate, ProjectListItem
from app.schemas.asset import AssetCreate, Asset as AssetSchema
from app.models.project import Project
from app.models.asset import Asset
# from app.core.celery_app import process_file_task # Replaced by TaskQueuePort
# from app.vector_store.chromadb_manager import ChromaDBManager # Replaced by VectorStorePort
from app.config import settings # Import central settings
from app.logger import get_logger

logger = get_logger(__name__)


class ProjectService:
    def __init__(self,
                 db: Session,
                 repo: IProjectRepository,
                 asset_content_repo: IAssetContentRepository,
                 process_repo: IProcessRepository,
                 file_storage: FileStoragePort,
                 vector_store: VectorStorePort,
                 task_queue: TaskQueuePort): # Added
        self.db = db
        self.repo = repo
        self.asset_content_repo = asset_content_repo
        self.process_repo = process_repo
        self.file_storage = file_storage
        self.vector_store = vector_store
        self.task_queue = task_queue # Added

    def create_project(self, project_data: ProjectCreate) -> Project:
        # Basic validation example
        existing_project = self.repo.get_project_by_name(self.db, project_name=project_data.name)
        if existing_project:
            raise HTTPException(status_code=400, detail="Project with this name already exists")
        return self.repo.create_project(self.db, project=project_data)

    def get_projects(self, page: int, page_size: int) -> Tuple[List[ProjectListItem], int]:
        return self.repo.get_projects(self.db, page=page, page_size=page_size)

    def get_project(self, project_id: int) -> Optional[Project]:
        project = self.repo.get_project(self.db, project_id=project_id)
        if not project:
            raise HTTPException(status_code=404, detail="Project not found")
        return project

    def update_project(self, project_id: int, project_data: ProjectUpdate) -> Optional[Project]:
        # Add validation if project_data.name is being changed
        if project_data.name:
            existing_project = self.repo.get_project_by_name(self.db, project_name=project_data.name)
            if existing_project and existing_project.id != project_id:
                raise HTTPException(status_code=400, detail="Another project with this name already exists")

        updated_project = self.repo.update_project(self.db, project_id=project_id, project=project_data)
        if not updated_project:
            raise HTTPException(status_code=404, detail="Project not found for updating")
        return updated_project

    def delete_project(self, project_id: int) -> Optional[Project]:
        project = self.repo.get_project(self.db, project_id=project_id)
        if not project:
            raise HTTPException(status_code=404, detail="Project not found")

        # Soft delete associated assets and their vector store entries
        assets = self.repo.get_assets_without_content(self.db, project_id=project_id) # Get all assets for the project
        for asset in assets:
            if asset.vector_id:
                try:
                    # Assuming default collection behavior or collection name is derivable if needed
                    self.vector_store.delete_item(asset.vector_id)
                except Exception as e:
                    # Log this error, but continue deletion
                    logger.error(f"Error deleting vector for asset {asset.id}: {e}")
            self.repo.delete_asset(self.db, asset_id=asset.id) # Soft delete asset

        # Soft delete processes and steps
        self.repo.delete_processes_and_steps(self.db, project_id=project_id)

        deleted_project = self.repo.delete_project(self.db, project_id=project_id)
        if not deleted_project:
            # This case should ideally not happen if the above check passed
            raise HTTPException(status_code=404, detail="Project not found during deletion")
        return deleted_project

    def upload_file_asset(self, project_id: int, file: UploadFile) -> Asset:
        self.get_project(project_id)  # Ensures project exists

        # File validation
        if file.content_type not in settings.allowed_content_types:
            raise HTTPException(status_code=400, detail=f"Invalid file type. Allowed types: {settings.allowed_content_types}")

        # Determine the file size without relying on UploadFile.size which may be None
        file.file.seek(0, os.SEEK_END)
        actual_size = file.file.tell()
        file.file.seek(0)
        if actual_size > settings.max_file_size:
            raise HTTPException(
                status_code=400,
                detail=f"File size exceeds limit of {settings.max_file_size / (1024 * 1024)}MB",
            )

        project_upload_dir = self.file_storage.create_project_directory(str(project_id))
        # unique_file_path will be like /base_upload_dir/project_id/uuid.ext
        unique_file_path = self.file_storage.generate_unique_file_path(project_upload_dir, file.filename)

        # Save uploaded file using FileStoragePort
        try:
            # Ensure file.file is treated as BinaryIO
            source_file_data: BinaryIO = file.file # type: ignore
            self.file_storage.save_file(unique_file_path, source_file_data)
        except Exception as e:
            # Log error appropriately
            raise HTTPException(status_code=500, detail=f"Could not save uploaded file: {e}")
        finally:
            file.file.close() # Ensure the underlying file stream is closed

        file_size = self.file_storage.get_file_size(unique_file_path)

        # Create asset in DB
        asset_data = AssetCreate(
            filename=file.filename,
            path=unique_file_path,
            asset_type="file",
            project_id=project_id,
            content_type=file.content_type,
            size=file_size
        )
        # Note: The AssetCreate schema only defines 'filename'.
        # Other fields like path, asset_type, project_id, content_type, size
        # are passed to repo.create_asset but are not part of AssetCreate model validation.
        # This is acceptable if repo.create_asset handles these additional parameters.
        db_asset = self.repo.create_asset(self.db, asset=asset_data, project_id=project_id)

        self.task_queue.submit_process_file_task(db_asset.id, unique_file_path)
        return db_asset

    async def add_url_asset(self, project_id: int, url: str, asset_name: Optional[str] = None) -> Asset:
        self.get_project(project_id)  # Ensures project exists

        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(url)
                response.raise_for_status() # Raise an exception for HTTP errors
                html_content = response.text
        except httpx.RequestError as e:
            raise HTTPException(status_code=400, detail=f"Error fetching URL: {e}")

        url_assets_dir = self.file_storage.create_project_sub_directory(str(project_id), "url_assets")
        original_filename_for_path = f"{asset_name or url.split('/')[-1] or os.urandom(4).hex()}.html"
        unique_file_path = self.file_storage.generate_unique_file_path(url_assets_dir, original_filename_for_path)

        try:
            self.file_storage.save_text_file(unique_file_path, html_content)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Could not save URL content: {e}")

        file_size = self.file_storage.get_file_size(unique_file_path)

        asset_data = AssetCreate(
            filename=asset_name or url,  # Changed from name to filename
            path=unique_file_path,
            asset_type="url",
            # project_id is not part of AssetCreate schema, passed to repo directly
            project_id=project_id, # This line is for the repo method, not AssetCreate Pydantic model
            content_type="text/html",
            size=file_size
        )
        # The project_id is passed to repo.create_asset, not part of AssetCreate schema itself
        db_asset = self.repo.create_asset(self.db, asset=asset_data, project_id=project_id)
        self.task_queue.submit_process_file_task(db_asset.id, unique_file_path)
        return db_asset

    def delete_asset(self, asset_id: int) -> Optional[Asset]:
        asset = self.repo.get_asset(self.db, asset_id=asset_id)
        if not asset:
            raise HTTPException(status_code=404, detail="Asset not found")

        if asset.vector_id: # type: ignore
            try:
                self.vector_store.delete_item(asset.vector_id) # Corrected attribute
            except Exception as e:
                logger.error(f"Error deleting vector for asset {asset.id}: {e}")

        deleted_asset = self.repo.delete_asset(self.db, asset_id=asset_id)
        if not deleted_asset:
             raise HTTPException(status_code=404, detail="Asset not found during deletion")
        return deleted_asset

    def get_project_assets(self, project_id: int, page: int, page_size: int, order_by: Optional[str] = None) -> Tuple[List[AssetSchema], int]:
        self.get_project(project_id)
        asset_models, total_count = self.repo.get_assets(
            self.db,
            project_id=project_id,
            page=page,
            page_size=page_size,
            order_by=order_by or "created_at_desc"
        )
        asset_schemas = [AssetSchema.from_orm(asset) for asset in asset_models]
        return asset_schemas, total_count

    def get_asset(self, asset_id: int) -> Optional[AssetSchema]:
        asset = self.repo.get_asset(self.db, asset_id=asset_id)
        if not asset:
            raise HTTPException(status_code=404, detail="Asset not found")
        return AssetSchema.from_orm(asset)

    def get_asset_model(self, asset_id: int) -> Optional[Asset]:
        asset = self.repo.get_asset(self.db, asset_id=asset_id)
        if not asset:
            raise HTTPException(status_code=404, detail="Asset not found")
        return asset

    def get_asset_content_from_db(self, asset_id: int) -> Optional[dict]:
        asset = self.repo.get_asset(self.db, asset_id=asset_id)
        if not asset:
            raise HTTPException(status_code=404, detail="Asset not found")
        # asset.content is the AssetContent related object.
        # The actual JSON content is on asset.content.content (AssetContent.content)
        if not asset.content or asset.content.content is None:
            raise HTTPException(status_code=404, detail="Asset content not found or not processed yet")
        return asset.content.content # Corrected to return the JSON field

    def get_asset_file_path(self, asset_id: int) -> Optional[str]:
        asset = self.repo.get_asset(self.db, asset_id=asset_id)
        if not asset:
            raise HTTPException(status_code=404, detail="Asset not found")
        if not asset.path or not os.path.exists(asset.path):
            raise HTTPException(status_code=404, detail="Asset file not found on server")
        return asset.path

    def process_asset_content(self, asset_id: int, file_path: str):
        logger.info(f"Processing asset {asset_id} from path {file_path}")
        pass

def is_allowed_file(filename: str, content_type: str):
    if content_type not in settings.allowed_content_types:
        return False
    return True
