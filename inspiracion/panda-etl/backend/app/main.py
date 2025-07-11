from app import models
from app.processing.process_queue import submit_process
from app.repositories import ProjectRepositoryImpl, ProcessRepositoryImpl, AssetContentRepositoryImpl
from app.repositories import user_repository
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from .database import SessionLocal
from fastapi.middleware.cors import CORSMiddleware
from app.processing.file_preprocessing import process_file
from .config import settings
from .api import router as api_router  # Importar el router principal, no el v1_router directamente
from app.schemas.user import APIKeyRequest
from app.startup import ensure_directory, register_startup_events

# Initialize the FastAPI app
app = FastAPI()

# Ensure directories required at import time exist to avoid initialization errors
ensure_directory(settings.upload_dir)

# Initialize repository instances
project_repository = ProjectRepositoryImpl()
process_repository = ProcessRepositoryImpl()
asset_content_repository = AssetContentRepositoryImpl()

def startup_file_preprocessing():
    try:
        with SessionLocal() as db:
            count = db.query(models.AssetContent).count()

            # Check for db initialization
            if count == 0:
                return

            projects = project_repository.get_all_projects(db)
            for project in projects:
                assets = project_repository.get_assets_without_content(
                    db=db, project_id=project.id
                )
                for asset in assets:
                    asset_content_repository.create_record(db, asset.id, None)

                assets_with_incomplete_content = asset_content_repository.get_assets_with_incomplete_content(
                    db, project_id=project.id
                )

                for asset in assets_with_incomplete_content:
                    asset_content = asset_content_repository.get_record_by_asset_id(db, asset.id)
                    if asset_content:
                        process_file(asset_content.id)

    except Exception as e:
        print(f"Error in startup_file_preprocessing: {e}")


def startup_pending_processes():
    try:
        with SessionLocal() as db:
            count = db.query(models.Process).count()

            if count == 0:
                return

            processes = process_repository.get_all_pending_processes(db)

            for process in processes:
                submit_process(process.id)

    except Exception as e:
        print(f"Error in startup_pending_processes: {e}")


def setup_user():
    try:
        with SessionLocal() as db:

            if settings.pandaetl_api_key:
                user = user_repository.get_users(db, n=1)
                api_key = user_repository.get_user_api_key(db)

                if not user:
                    user = user_repository.create_user(db, APIKeyRequest(email="test@pandai-etl.ai"))

                if not api_key:
                    user_repository.add_user_api_key(db, user.id, settings.pandaetl_api_key)

                print("Successfully set up user from api key")

    except Exception as e:
        print(f"Error in setup user from api key: {e}")


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins (for development)
    allow_credentials=True,
    allow_methods=["*"],  # Allow all methods
    allow_headers=["*"],  # Allow all headers
)

app.mount("/assets", StaticFiles(directory=settings.upload_dir), name="assets")

app.include_router(api_router, prefix="/api")  # Prefix /api for consistency

# Register startup hooks for runtime environment checks
register_startup_events(app, settings.upload_dir)

setup_user()
startup_pending_processes()
startup_file_preprocessing()
