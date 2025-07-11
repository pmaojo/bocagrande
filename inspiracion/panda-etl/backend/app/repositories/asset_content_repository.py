from typing import List, Optional
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_
from datetime import datetime, timezone

from app import models # Assuming models are accessible like this
from app.schemas.enums.asset_processing_status import AssetProcessingStatus
from app.interfaces.asset_content_repository import IAssetContentRepository


class AssetContentRepositoryImpl(IAssetContentRepository):
    def create_record(self, db: Session, asset_id: int, content_dict: Optional[dict] = None, language: Optional[str] = None) -> models.AssetContent:
        asset_content = models.AssetContent(
            asset_id=asset_id,
            content=content_dict, # This is the JSONB field in AssetContent
            language=language if language else (content_dict.get("lang") if content_dict else None),
            processing=AssetProcessingStatus.PENDING,
        )
        db.add(asset_content)
        db.commit()
        db.refresh(asset_content)
        return asset_content

    def update_or_add_record(self, db: Session, asset_id: int, content_dict: dict) -> models.AssetContent:
        asset_content = (
            db.query(models.AssetContent)
            .filter(models.AssetContent.asset_id == asset_id, models.AssetContent.deleted_at.is_(None))
            .first()
        )

        if asset_content:
            asset_content.content = content_dict
            asset_content.language = content_dict.get("lang", asset_content.language)
            asset_content.processing = AssetProcessingStatus.PENDING # Reset status on update
            asset_content.updated_at = datetime.now(timezone.utc)
        else:
            asset_content = models.AssetContent(
                asset_id=asset_id,
                content=content_dict,
                language=content_dict.get("lang"),
                processing=AssetProcessingStatus.PENDING,
            )
            db.add(asset_content)

        db.commit()
        db.refresh(asset_content)
        return asset_content

    def update_status(
        self,
        db: Session,
        status: AssetProcessingStatus,
        asset_content_id: Optional[int] = None,
        asset_id: Optional[int] = None,
    ) -> None:
        if not asset_content_id and not asset_id:
            # Or raise an error, depending on desired behavior
            return

        query = db.query(models.AssetContent).filter(models.AssetContent.deleted_at.is_(None))

        if asset_content_id:
            query = query.filter(models.AssetContent.id == asset_content_id)
        elif asset_id: # asset_id is provided and asset_content_id is not
            query = query.filter(models.AssetContent.asset_id == asset_id)

        # Ensure at least one record is found before updating
        # first_record = query.first()
        # if not first_record:
            # Optionally raise an error or log if no record found to update
            # return

        query.update({models.AssetContent.processing: status, models.AssetContent.updated_at: datetime.now(timezone.utc)}, synchronize_session=False)
        db.commit()

    def get_record_by_asset_id(self, db: Session, asset_id: int) -> Optional[models.AssetContent]:
        return (
            db.query(models.AssetContent)
            .filter(models.AssetContent.asset_id == asset_id, models.AssetContent.deleted_at.is_(None))
            .first()
        )

    def get_assets_with_pending_content(self, db: Session, project_id: int) -> List[models.Asset]:
        return (
            db.query(models.Asset)
            .join(models.AssetContent, and_(models.Asset.id == models.AssetContent.asset_id, models.AssetContent.deleted_at.is_(None)))
            .filter(
                models.Asset.project_id == project_id,
                models.Asset.deleted_at.is_(None),
                models.AssetContent.processing == AssetProcessingStatus.PENDING,
            )
            .all()
        )

    def get_assets_with_incomplete_content(self, db: Session, project_id: int) -> List[models.Asset]:
        return (
            db.query(models.Asset)
            .join(models.AssetContent, and_(models.Asset.id == models.AssetContent.asset_id, models.AssetContent.deleted_at.is_(None)))
            .filter(
                models.Asset.project_id == project_id,
                models.Asset.deleted_at.is_(None),
                or_(
                    models.AssetContent.processing == AssetProcessingStatus.IN_PROGRESS,
                    models.AssetContent.processing == AssetProcessingStatus.PENDING,
                ),
            )
            .all()
        )
