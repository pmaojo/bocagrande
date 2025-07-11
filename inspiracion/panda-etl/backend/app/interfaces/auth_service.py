from abc import ABC, abstractmethod
from sqlalchemy.orm import Session
from app.models.user import User

class IAuthService(ABC):
    """Abstract interface for authentication services."""

    @abstractmethod
    def authenticate(self, db: Session, token: str) -> User:
        """Authenticate user using provided token."""
        raise NotImplementedError
