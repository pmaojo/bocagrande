from unittest.mock import MagicMock, patch, ANY # Added ANY
import pytest
from fastapi import HTTPException # Added HTTPException
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session
from fastapi import UploadFile
from io import BytesIO

from app.main import app
from app.database import get_db

# Test client setup
client = TestClient(app)


@pytest.fixture
def mock_file():
    """Fixture to mock an uploaded PDF file"""
    file_content = BytesIO(b"Dummy PDF content")
    return UploadFile(filename="test.pdf", file=file_content)


@pytest.fixture
def mock_non_pdf_file():
    """Fixture to mock an uploaded non-PDF file"""
    file_content = BytesIO(b"Dummy non-PDF content")
    return UploadFile(filename="test.txt", file=file_content)


@pytest.fixture
def mock_db():
    """Fixture to mock the database session"""
    db = MagicMock(spec=Session)
    return db


@pytest.fixture(autouse=True)
def override_get_db(mock_db):
    """Override the get_db dependency with the mock"""

    def _override_get_db():
        return mock_db

    app.dependency_overrides[get_db] = _override_get_db
    yield
    app.dependency_overrides.clear()


@patch("app.repositories.project_repository.ProjectRepositoryImpl.get_project")
@patch("app.repositories.project_repository.ProjectRepositoryImpl.create_asset")
@patch("app.adapters.local_file_storage_adapter.LocalFileStorageAdapter.save_file")
@patch("app.adapters.local_file_storage_adapter.LocalFileStorageAdapter.generate_unique_file_path")
@patch("app.adapters.local_file_storage_adapter.LocalFileStorageAdapter.create_project_directory")
@patch("app.adapters.local_file_storage_adapter.LocalFileStorageAdapter.get_file_size")
@patch("app.adapters.celery_task_queue_adapter.CeleryTaskQueueAdapter.submit_process_file_task")
def test_upload_files_success(
    mock_submit_task,
    mock_get_file_size,
    mock_create_proj_dir,
    mock_gen_unique_path,
    mock_save_file,
    mock_create_asset,
    mock_get_project,
    mock_file: UploadFile, # Type hint for clarity
    client: TestClient, # Use client from conftest
    # mock_db is no longer needed directly as get_db is overridden by conftest
):
    """Test uploading files successfully"""
    # Setup mocks
    mock_get_project.return_value = MagicMock(id=1, name="Test Project") # Simulate a Project model

    mock_project_model_instance = MagicMock(id=1, name="Test Project")
    mock_get_project.return_value = mock_project_model_instance

    mock_create_proj_dir.return_value = "uploads/1"
    mock_gen_unique_path.return_value = "uploads/1/test_uuid.pdf"
    mock_get_file_size.return_value = 12345 # Example file size

    # Simulate the asset created by the repository
    created_asset_model = MagicMock()
    created_asset_model.id = 100
    created_asset_model.name = "test.pdf" # or mock_file.filename
    created_asset_model.filename = "test.pdf" # Ensure filename attribute exists
    created_asset_model.path = "uploads/1/test_uuid.pdf"
    created_asset_model.project_id = 1
    created_asset_model.content_type = "application/pdf"
    created_asset_model.size = 12345
    created_asset_model.created_at = MagicMock() # For AssetSchema.from_orm
    created_asset_model.updated_at = MagicMock() # For AssetSchema.from_orm
    mock_create_asset.return_value = created_asset_model

    response = client.post(
        "/api/v1/projects/1/assets",
        files={"files": (mock_file.filename, mock_file.file, "application/pdf")},
    )

    assert response.status_code == 200 # Original endpoint returned 200
    # The response_model is List[AssetSchema], so it should be a list of asset details
    response_json = response.json()
    assert isinstance(response_json, list)
    assert len(response_json) == 1
    assert response_json[0]["filename"] == "test.pdf"
    assert response_json[0]["id"] == 100

    # Verify calls
    mock_get_project.assert_called_once_with(ANY, project_id=1) # Called by ProjectService, db session is first arg
    mock_create_proj_dir.assert_called_once_with("1")
    mock_gen_unique_path.assert_called_once_with("uploads/1", "test.pdf")
    mock_save_file.assert_called_once() # Arguments can be more specific if needed
    mock_get_file_size.assert_called_once_with("uploads/1/test_uuid.pdf")
    mock_create_asset.assert_called_once() # Arguments can be more specific
    mock_submit_task.assert_called_once_with(created_asset_model.id, created_asset_model.path)


@patch("app.repositories.project_repository.ProjectRepositoryImpl.get_project")
def test_upload_files_project_not_found(mock_get_project, mock_file: UploadFile, client: TestClient):
    """Test uploading files when project is not found"""
    # Service's get_project raises HTTPException(404) if not found.
    # This will be caught by FastAPI's exception handling.
    mock_get_project.side_effect = HTTPException(status_code=404, detail="Project not found")

    response = client.post(
        "/api/v1/projects/1/assets",
        files={"files": (mock_file.filename, mock_file.file, "application/pdf")},
    )

    assert response.status_code == 404
    assert response.json() == {"detail": "Project not found"} # Match service's HTTPException detail


@patch("app.repositories.project_repository.ProjectRepositoryImpl.get_project")
@patch("app.adapters.celery_task_queue_adapter.CeleryTaskQueueAdapter.submit_process_file_task") # Added to prevent AttributeError
def test_upload_files_non_pdf(mock_submit_task, mock_get_project, mock_non_pdf_file: UploadFile, client: TestClient):
    """Test uploading a non-PDF file"""
    mock_get_project.return_value = MagicMock(id=1, name="Test Project") # Simulate a Project model

    response = client.post(
        "/api/v1/projects/1/assets",
        files={"files": (mock_non_pdf_file.filename, mock_non_pdf_file.file, "text/plain")}, # Correct content type
    )

    assert response.status_code == 400
    assert response.json() == {"detail": "The file 'test.txt' is not a valid PDF. Please upload only PDF files."}
