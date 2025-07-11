from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from pydantic import BaseModel

from app.api.dependencies import get_db
from app.services.google_auth_service import GoogleAuthService

auth_router = APIRouter()

class GoogleToken(BaseModel):
    token: str

@auth_router.post("/google-login", status_code=200)
def google_login(payload: GoogleToken, db: Session = Depends(get_db)):
    service = GoogleAuthService()
    user = service.authenticate(db, payload.token)
    return {"status": "success", "message": "User authenticated", "data": user}
