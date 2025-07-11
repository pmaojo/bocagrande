from typing import Optional
from sqlalchemy.orm import Session
from app import models

class AppSettingRepository:
    """Repository for managing application settings."""

    def get_setting(self, db: Session, key: str) -> Optional[str]:
        record = db.query(models.AppSetting).filter(models.AppSetting.key == key).first()
        return record.value if record else None

    def set_setting(self, db: Session, key: str, value: str) -> models.AppSetting:
        record = db.query(models.AppSetting).filter(models.AppSetting.key == key).first()
        if record:
            record.value = value
        else:
            record = models.AppSetting(key=key, value=value)
            db.add(record)
        db.commit()
        return record
