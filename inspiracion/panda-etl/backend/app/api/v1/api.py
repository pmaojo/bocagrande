"""
Configuración del router principal de la API v1.

Este módulo registra todos los routers de los endpoints en el router principal.
"""

from fastapi import APIRouter

from app.api.v1.endpoints import pipelines
from app.api.v1 import pentaho_router  # Import Pentaho router
from app.api.v1 import kjb_router  # Import KJB router
from app.api.v1 import ai_transform  # Import AI transformation router
from app.api.v1 import app_settings  # App settings router

api_router = APIRouter()

# Registrar los routers de los endpoints
api_router.include_router(pipelines.router, tags=["pipelines"])
api_router.include_router(pentaho_router.router, tags=["Pentaho Importer"]) # Add Pentaho router
api_router.include_router(kjb_router.router, prefix="/kjb-importer", tags=["KJB Importer"]) # Add KJB router with correct prefix
api_router.include_router(ai_transform.router, prefix="/ai", tags=["AI Transformation"]) # Add AI transformation router
api_router.include_router(app_settings.router, prefix="/config", tags=["Settings"])
