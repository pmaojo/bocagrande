"""Service providing authentication via Google OAuth tokens."""

from __future__ import annotations

from google.oauth2 import id_token
from google.auth.transport import requests as google_requests
from fastapi import HTTPException
from sqlalchemy.orm import Session

from app.config import settings
from app.schemas.user import APIKeyRequest
from app.repositories import user_repository
from app.interfaces.auth_service import IAuthService

class GoogleAuthService(IAuthService):
    """Authenticate users via Google OAuth tokens."""

    def __init__(self, client_id: str | None = None) -> None:
        self.client_id = client_id or settings.google_client_id

    def authenticate(self, db: Session, token: str):
        try:
            info = id_token.verify_oauth2_token(
                token, google_requests.Request(), self.client_id
            )
        except Exception as exc:  # pragma: no cover - external library errors
            raise HTTPException(status_code=400, detail="Invalid Google token") from exc

        email = info.get("email")
        if not email:
            raise HTTPException(status_code=400, detail="Email not provided by Google")

        user = user_repository.get_user(db, email)
        if not user:
            user = user_repository.create_user(db, APIKeyRequest(email=email))
        return user
