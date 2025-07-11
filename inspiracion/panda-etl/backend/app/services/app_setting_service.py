"""Service layer for application settings management."""

from __future__ import annotations

from sqlalchemy.orm import Session
from app.repositories.app_setting_repository import AppSettingRepository

class AppSettingService:
    def __init__(self, repo: AppSettingRepository | None = None):
        self.repo = repo or AppSettingRepository()

    def get_value(self, db: Session, key: str) -> str | None:
        return self.repo.get_setting(db, key)

    def set_value(self, db: Session, key: str, value: str) -> None:
        self.repo.set_setting(db, key, value)
