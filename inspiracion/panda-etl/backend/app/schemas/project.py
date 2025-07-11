from dataclasses import dataclass
from pydantic import BaseModel
from typing import Optional
from datetime import datetime


class ProjectBase(BaseModel):
    name: str
    description: Optional[str] = None


class ProjectCreate(ProjectBase):
    pass


class ProjectUpdate(ProjectBase):
    name: Optional[str] = None
    description: Optional[str] = None


class Project(ProjectBase):
    id: int
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


ProjectInDBBase = Project # Alias Project to ProjectInDBBase


@dataclass
class ProjectListItem:
    """Lightweight wrapper for project listings with asset counts."""

    project: Project
    asset_count: int

