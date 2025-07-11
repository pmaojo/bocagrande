from abc import ABC, abstractmethod
from typing import List, Optional
from sqlalchemy.orm import Session
from app.models.asset import Asset
from app.models.asset_content import AssetContent
from app.schemas.enums.asset_processing_status import AssetProcessingStatus


class IAssetContentRepository(ABC):
    @abstractmethod
    def create_record(self, db: Session, asset_id: int, content_dict: Optional[dict] = None, language: Optional[str] = None) -> AssetContent:
        pass

    @abstractmethod
    def update_or_add_record(self, db: Session, asset_id: int, content_dict: dict) -> AssetContent:
        pass

    @abstractmethod
    def update_status(self, db: Session, status: AssetProcessingStatus, asset_content_id: Optional[int] = None, asset_id: Optional[int] = None) -> None:
        pass

    @abstractmethod
    def get_record_by_asset_id(self, db: Session, asset_id: int) -> Optional[AssetContent]:
        pass

    @abstractmethod
    def get_assets_with_pending_content(self, db: Session, project_id: int) -> List[Asset]:
        pass

    @abstractmethod
    def get_assets_with_incomplete_content(self, db: Session, project_id: int) -> List[Asset]:
        pass
