from abc import ABC, abstractmethod
from typing import List, Optional, Tuple
from sqlalchemy.orm import Session
from app.schemas.project import ProjectCreate, ProjectUpdate, ProjectListItem
from app.schemas.asset import AssetCreate, AssetUpdate
from app.models.project import Project
from app.models.asset import Asset


class IProjectRepository(ABC):
    @abstractmethod
    def create_project(self, db: Session, project: ProjectCreate) -> Project:
        pass

    @abstractmethod
    def get_projects(self, db: Session, page: int, page_size: int) -> Tuple[List[ProjectListItem], int]:
        pass

    @abstractmethod
    def get_project(self, db: Session, project_id: int) -> Optional[Project]:
        pass

    @abstractmethod
    def get_project_by_name(self, db: Session, project_name: str) -> Optional[Project]:
        pass

    @abstractmethod
    def update_project(self, db: Session, project_id: int, project: ProjectUpdate) -> Optional[Project]:
        pass

    @abstractmethod
    def delete_project(self, db: Session, project_id: int) -> Optional[Project]:
        pass

    @abstractmethod
    def create_asset(self, db: Session, asset: AssetCreate, project_id: int) -> Asset:
        pass

    @abstractmethod
    def get_assets(self, db: Session, project_id: int, page: int, page_size: int, order_by: str) -> Tuple[List[Asset], int]:
        pass

    @abstractmethod
    def get_asset(self, db: Session, asset_id: int) -> Optional[Asset]:
        pass

    @abstractmethod
    def update_asset(self, db: Session, asset_id: int, asset: AssetUpdate) -> Optional[Asset]:
        pass

    @abstractmethod
    def delete_asset(self, db: Session, asset_id: int) -> Optional[Asset]:
        pass

    @abstractmethod
    def add_asset_content(self, db: Session, asset_id: int, content: dict) -> Optional[Asset]:
        pass

    @abstractmethod
    def delete_processes_and_steps(self, db: Session, project_id: int) -> None:
        pass

    @abstractmethod
    def get_assets_without_content(self, db: Session, project_id: int) -> List[Asset]:
        pass
