from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel


class AssetBase(BaseModel):
    filename: str
    created_at: datetime
    updated_at: datetime


class Asset(AssetBase):
    id: int
    project_id: int

    class Config:
        from_attributes = True


class UrlAssetCreate(BaseModel):
    url: List[str]


class AssetCreate(BaseModel):
    filename: str
    path: str
    asset_type: str # e.g., "file", "url"
    content_type: Optional[str] = None
    size: Optional[int] = None
    # project_id is handled at the repository layer or service layer, not part of this specific create schema usually
    # If it were, it would be: project_id: int


class AssetUpdate(BaseModel):
    filename: Optional[str] = None
    # Add other updatable fields here if necessary, e.g.
    # description: Optional[str] = None


class AssetInDBBase(AssetBase):
    id: int
    project_id: int # project_id should be part of the asset in DB

    class Config:
        from_attributes = True


class AssetSummary(BaseModel):
    id: int
    filename: str
    project_id: int # Useful to have project_id in summary
    updated_at: datetime

    class Config:
        from_attributes = True
