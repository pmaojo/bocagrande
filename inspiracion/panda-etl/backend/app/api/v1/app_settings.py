from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from app.api.dependencies import get_db, get_app_setting_service
from app.services.app_setting_service import AppSettingService

router = APIRouter(prefix="/settings", tags=["Settings"])

GROQ_API_KEY = "groq_api_key"
GROQ_API_BASE = "groq_api_base_url"


@router.get("/groq-api-key")
def get_groq_api_key(
    db: Session = Depends(get_db),
    service: AppSettingService = Depends(get_app_setting_service),
):
    value = service.get_value(db, GROQ_API_KEY)
    if value is None:
        raise HTTPException(status_code=404, detail="Groq API key not set")
    return {"api_key": value}


@router.post("/groq-api-key")
def set_groq_api_key(
    api_key: str,
    db: Session = Depends(get_db),
    service: AppSettingService = Depends(get_app_setting_service),
):
    service.set_value(db, GROQ_API_KEY, api_key)
    return {"message": "Groq API key updated"}


@router.get("/groq-api-base")
def get_groq_api_base(
    db: Session = Depends(get_db),
    service: AppSettingService = Depends(get_app_setting_service),
):
    value = service.get_value(db, GROQ_API_BASE)
    if value is None:
        raise HTTPException(status_code=404, detail="Groq API base not set")
    return {"api_base": value}


@router.post("/groq-api-base")
def set_groq_api_base(
    api_base: str,
    db: Session = Depends(get_db),
    service: AppSettingService = Depends(get_app_setting_service),
):
    service.set_value(db, GROQ_API_BASE, api_base)
    return {"message": "Groq API base updated"}
