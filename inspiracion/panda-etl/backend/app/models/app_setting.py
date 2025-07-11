from sqlalchemy import Column, Integer, String
from .base import Base

class AppSetting(Base):
    __tablename__ = "app_settings"

    id = Column(Integer, primary_key=True, index=True)
    key = Column(String(255), unique=True, nullable=False)
    value = Column(String(1024), nullable=False)

    def __repr__(self):
        return f"<AppSetting {self.key}={self.value}>"
