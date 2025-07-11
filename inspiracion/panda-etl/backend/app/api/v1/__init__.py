from fastapi import APIRouter
from .projects import project_router
from .user import user_router
from .processes import process_router
from .process_steps import process_step_router
from .extract import extract_router
from .chat import chat_router
from .auth import auth_router
from .api import api_router # Import the main api_router for v1
from .schema_extractor import router as schema_extractor_router
from .app_settings import router as app_settings_router

v1_router = APIRouter()
v1_router.include_router(project_router, prefix="/projects")
v1_router.include_router(user_router, prefix="/user")
v1_router.include_router(auth_router, prefix="/auth")
v1_router.include_router(process_router, prefix="/processes")
v1_router.include_router(process_step_router, prefix="/process_steps")
v1_router.include_router(extract_router, prefix="/extract")
v1_router.include_router(chat_router, prefix="/chat")
v1_router.include_router(schema_extractor_router, prefix="/schema")
v1_router.include_router(api_router) # Include the api_router (contains pentaho_router and others)
v1_router.include_router(app_settings_router, prefix="/config")
