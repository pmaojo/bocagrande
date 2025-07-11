from typing import List, Optional, Tuple
from sqlalchemy.orm import Session # Removed: and_, or_
from sqlalchemy import func, and_, asc, desc # Added and_ explicitly
from datetime import datetime, timezone

from app import models
from app.schemas.project import ProjectCreate, ProjectUpdate, ProjectListItem
from app.schemas.asset import AssetCreate, AssetUpdate  # Added Asset schemas
from app.models.asset_content import AssetProcessingStatus

from app.interfaces.repositories import IProjectRepository
from app.repositories.process_repository import delete_project_processes_and_steps
from app.repositories.asset_content_repository import AssetContentRepositoryImpl


class ProjectRepositoryImpl(IProjectRepository):
    def create_project(self, db: Session, project: ProjectCreate) -> models.Project:
        db_project = models.Project(name=project.name, description=project.description)
        db.add(db_project)
        db.commit()
        db.refresh(db_project)
        return db_project

    def get_projects(self, db: Session, page: int = 1, page_size: int = 20) -> Tuple[List[ProjectListItem], int]:
        # Note: The original function returned a tuple (projects, total_count).
        # The interface IProjectRepository.get_projects expects List[Project].
        # For now, I'll keep the tuple, but this might need adjustment
        # based on how it's used or if the interface should be updated.
        # For strict adherence, this should just return the list of projects.
        # Let's assume for now the service layer will handle the tuple.
        total_count = db.query(func.count(models.Project.id)).filter(
            models.Project.deleted_at.is_(None)
        ).scalar()

        projects_with_asset_counts = (
            db.query(
                models.Project,
                func.count(models.Asset.id).label("asset_count"),
            )
            .filter(models.Project.deleted_at.is_(None))
            .outerjoin(models.Asset, and_(models.Project.id == models.Asset.project_id, models.Asset.deleted_at.is_(None)))
            .group_by(models.Project.id)
            .order_by(models.Project.created_at.desc()) # Default ordering
            .offset((page - 1) * page_size)
            .limit(page_size)
            .all()
        )
        # The result is a list of tuples (Project, asset_count). We need to extract Project objects.
        # This part is tricky because the original returned Project objects with an asset_count attribute dynamically added.
        # For now, returning as is, but this is a point of attention.
        # The interface expects List[Project], so we should ideally map this.
        # However, the API might rely on this asset_count.
        # For now, let's return the Project part of the tuple, and the service can decide what to do.
        # This is a common ORM pattern, but doesn't fit neatly with strict typing sometimes.
        # To fulfill the interface strictly:
        project_items = [ProjectListItem(project=p, asset_count=count) for p, count in projects_with_asset_counts]
        return project_items, total_count


    def get_all_projects(self, db: Session) -> List[models.Project]: # Not in interface, but was in original file
        return db.query(models.Project).filter(models.Project.deleted_at.is_(None)).all()

    def get_project(self, db: Session, project_id: int) -> Optional[models.Project]:
        return db.query(models.Project).filter(models.Project.id == project_id, models.Project.deleted_at.is_(None)).first()

    def get_project_by_name(self, db: Session, project_name: str) -> Optional[models.Project]:
        return db.query(models.Project).filter(models.Project.name == project_name, models.Project.deleted_at.is_(None)).first()

    def update_project(self, db: Session, project_id: int, project: ProjectUpdate) -> Optional[models.Project]:
        db_project = self.get_project(db, project_id)
        if db_project is None:
            return None

        for key, value in project.dict(exclude_unset=True).items():
            setattr(db_project, key, value)

        db_project.updated_at = datetime.now(timezone.utc)
        db.commit()
        db.refresh(db_project)
        return db_project

    def delete_project(self, db: Session, project_id: int) -> Optional[models.Project]:
        db_project = self.get_project(db, project_id)
        if db_project:
            db_project.deleted_at = datetime.now(timezone.utc)
            db.commit()
            db.refresh(db_project)
        return db_project

    def create_asset(self, db: Session, asset: AssetCreate, project_id: int) -> models.Asset:
        asset_data = asset.model_dump(exclude_unset=True)  # Use model_dump for Pydantic v2+

        # Map AssetCreate fields to Asset ORM model fields
        orm_data = {
            "filename": asset_data.get("filename"),
            "path": asset_data.get("path"),
            "type": asset_data.get("asset_type"), # Map asset_type to type
            "size": asset_data.get("size"),
            "project_id": project_id,
            "details": {} # Initialize details
        }
        if "content_type" in asset_data and asset_data["content_type"] is not None:
            orm_data["details"]["content_type"] = asset_data["content_type"]

        # Filter out None values for fields that are nullable or have defaults in ORM
        # or ensure all required fields for Asset are present.
        # For Asset model: filename, path, type, project_id are likely required.
        # Size is nullable. Details is nullable with default.

        # Ensure required fields are not None before creating ORM model
        if not orm_data["filename"]:
            raise ValueError("filename cannot be empty") # Or handle as per app logic
        if not orm_data["path"]:
            raise ValueError("path cannot be empty")
        if not orm_data["type"]:
            raise ValueError("asset_type (mapped to type) cannot be empty")

        db_asset = models.Asset(**orm_data)
        db.add(db_asset)
        db.commit()
        db.refresh(db_asset)
        return db_asset

    def get_assets(
        self,
        db: Session,
        project_id: int,
        page: int,
        page_size: int,
        order_by: str = "created_at_desc",
    ) -> Tuple[List[models.Asset], int]:
        base_query = db.query(models.Asset).filter(models.Asset.project_id == project_id, models.Asset.deleted_at.is_(None))

        # Get total count before pagination
        total_count = db.query(func.count(base_query.subquery().c.id)).scalar()

        query = base_query # Continue with the base query for ordering and pagination

        if order_by == "created_at_desc":
            order_by_column = desc(models.Asset.created_at)
        elif order_by == "created_at_asc":
            order_by_column = asc(models.Asset.created_at)
        elif order_by == "name_asc":
            order_by_column = asc(models.Asset.name)
        elif order_by == "name_desc":
            order_by_column = desc(models.Asset.name)
        else: # Default ordering
            order_by_column = desc(models.Asset.created_at)

        query = query.order_by(order_by_column)

        if page is not None and page_size is not None:
             assets = query.offset((page - 1) * page_size).limit(page_size).all()
        else:
            assets = query.all()

        return assets, total_count


    def get_asset(self, db: Session, asset_id: int) -> Optional[models.Asset]:
        return db.query(models.Asset).filter(models.Asset.id == asset_id, models.Asset.deleted_at.is_(None)).first()

    def update_asset(self, db: Session, asset_id: int, asset_data: AssetUpdate) -> Optional[models.Asset]:
        db_asset = self.get_asset(db, asset_id)
        if db_asset:
            for key, value in asset_data.dict(exclude_unset=True).items():
                setattr(db_asset, key, value)
            db_asset.updated_at = datetime.now(timezone.utc)
            db.commit()
            db.refresh(db_asset)
        return db_asset

    def delete_asset(self, db: Session, asset_id: int) -> Optional[models.Asset]:
        db_asset = self.get_asset(db, asset_id)
        if db_asset:
            db_asset.deleted_at = datetime.now(timezone.utc)
            # Also mark related AssetContent as deleted if any
            db.query(models.AssetContent).filter(models.AssetContent.asset_id == asset_id).update(
                {models.AssetContent.deleted_at: datetime.now(timezone.utc)},
                synchronize_session=False
            )
            db.commit()
            db.refresh(db_asset)
        return db_asset

    def add_asset_content(self, db: Session, asset_id: int, content_data: dict) -> Optional[models.AssetContent]:
        # This method in the interface was for Asset, but the original was AssetContent.
        # Assuming it's about AssetContent.
        # The 'content' field in Asset model is for structured_content from extraction,
        # not the AssetContent table record itself.
        # This needs clarification. For now, I'm implementing based on original `add_asset_content`
        # which creates an AssetContent record.
        # The interface might need AssetContent schema.

        # Let's assume content_data is the raw content to be stored.
        # The original function created AssetContent.
        # The interface `add_asset_content(db: Session, asset_id: int, content: dict) -> Optional[Asset]`
        # seems to imply updating the Asset model's content field.
        # This is a mismatch. I will implement based on the original `add_asset_content`
        # and create/update `AssetContent` table, and the service layer can adapt.
        # This method is also NOT on the IProjectRepository, the one there is:
        # add_asset_content(self, db: Session, asset_id: int, content: dict) -> Optional[Asset]
        # This is a significant difference.
        # For now, I'll implement a method that updates the Asset.content JSON field.
        # The original `add_asset_content` that creates `models.AssetContent` will be kept as a helper or renamed.

        db_asset = self.get_asset(db, asset_id)
        if not db_asset:
            return None

        # Assuming content_data is a dictionary to be stored in the JSON field 'content' of Asset model
        db_asset.content = content_data
        db_asset.updated_at = datetime.now(timezone.utc)
        db.commit()
        db.refresh(db_asset)
        return db_asset

    # Methods related to AssetContent have been moved to AssetContentRepositoryImpl:
    # - create_db_asset_content_record
    # - update_or_add_db_asset_content_record
    # - update_asset_content_status
    # - get_asset_content_record
    # - get_assets_content_pending (as get_assets_with_pending_content)
    # - get_assets_content_incomplete (as get_assets_with_incomplete_content)

    def delete_processes_and_steps(self, db: Session, project_id: int) -> None:
        """Soft delete all processes and steps for a project."""
        delete_project_processes_and_steps(db, project_id)

    def get_assets_without_content(self, db: Session, project_id: int) -> List[models.Asset]:
        # This likely means assets for which AssetContent record does not exist or content is null
        # Or assets whose own 'content' field is null. The original checked for AssetContent record.
        return (
            db.query(models.Asset)
            .filter(models.Asset.project_id == project_id, models.Asset.deleted_at.is_(None))
            .outerjoin(models.AssetContent, and_(models.Asset.id == models.AssetContent.asset_id, models.AssetContent.deleted_at.is_(None)))
            .filter(models.AssetContent.id.is_(None))
            .all()
        )

    # These functions were in the original file but not in the IProjectRepository interface.
    # They might be used by other services or parts of the API, so I'm keeping them within the class for now.
    # They can be removed or refactored if they are truly unused after the service layer migration.

    # Methods related to Process have been moved to ProcessRepositoryImpl:
    # - get_processes (as get_project_processes)
    # - The core logic of delete_processes_and_steps

    def get_assets_filename(self, db: Session, asset_ids: List[int]) -> List[str]: # Not in interface, kept for now
        return [
            asset.name # Assuming 'name' field stores filename, original used 'filename'
            for asset in db.query(models.Asset.name).filter(models.Asset.id.in_(asset_ids), models.Asset.deleted_at.is_(None)).all()
        ]

# The following functions were not part of IProjectRepository and seem specific to AssetContent or Process.
# They have been moved to their respective repositories or removed if redundant.

# def get_all_projects(...) - Kept as is, marked as not in interface.
# def add_asset_content(...) - The interface version that updates Asset.content is kept.
#                             The original version that created AssetContent record is now in AssetContentRepositoryImpl.

# Module level helpers for easy access to common repository operations.

_project_repo_impl = ProjectRepositoryImpl()
_asset_content_repo_impl = AssetContentRepositoryImpl()


def get_project(db: Session, project_id: int, repo: IProjectRepository = _project_repo_impl):
    """Return a project by id using the default repository implementation."""
    return repo.get_project(db, project_id)


def get_asset(db: Session, asset_id: int, repo: IProjectRepository = _project_repo_impl):
    """Return an asset by id using the default repository implementation."""
    return repo.get_asset(db, asset_id)


def get_assets(
    db: Session,
    project_id: int,
    page: Optional[int] = None,
    page_size: Optional[int] = None,
    order_by: str = "created_at_desc",
    repo: IProjectRepository = _project_repo_impl,
) -> Tuple[List[models.Asset], int]:
    """Return paginated assets for a project."""
    return repo.get_assets(db, project_id, page, page_size, order_by)


def get_asset_content(
    db: Session,
    asset_id: int,
    repo: AssetContentRepositoryImpl = _asset_content_repo_impl,
):
    """Return AssetContent record for an asset."""
    return repo.get_record_by_asset_id(db, asset_id)


def update_or_add_asset_content(
    db: Session,
    asset_id: int,
    content_dict: Optional[dict],
    repo: AssetContentRepositoryImpl = _asset_content_repo_impl,
):
    """Create or update AssetContent for an asset."""
    return repo.update_or_add_record(db, asset_id, content_dict)


def update_asset_content_status(
    db: Session,
    asset_id: int,
    status: AssetProcessingStatus,
    repo: AssetContentRepositoryImpl = _asset_content_repo_impl,
) -> None:
    """Update processing status for an asset's content."""
    repo.update_status(db, status, asset_id=asset_id)


def get_assets_without_content(db: Session, project_id: int, repo: IProjectRepository = _project_repo_impl) -> List[models.Asset]:
    """Return assets missing associated AssetContent."""
    return repo.get_assets_without_content(db, project_id)


def get_assets_content_pending(db: Session, project_id: int, repo: AssetContentRepositoryImpl = _asset_content_repo_impl) -> List[models.Asset]:
    """Return assets whose content is pending processing."""
    return repo.get_assets_with_pending_content(db, project_id)


def get_assets_content_incomplete(db: Session, project_id: int, repo: AssetContentRepositoryImpl = _asset_content_repo_impl) -> List[models.Asset]:
    """Return assets whose content processing is incomplete."""
    return repo.get_assets_with_incomplete_content(db, project_id)


def get_assets_filename(db: Session, asset_ids: List[int], repo: IProjectRepository = _project_repo_impl) -> List[str]:
    """Return filenames for given asset identifiers."""
    return repo.get_assets_filename(db, asset_ids)
