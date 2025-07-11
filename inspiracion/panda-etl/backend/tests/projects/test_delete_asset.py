from unittest.mock import MagicMock, patch, ANY
from fastapi.testclient import TestClient
from datetime import datetime

# client fixture will be provided by conftest.py
# DB session override and table creation also handled by conftest.py

@patch("app.repositories.project_repository.ProjectRepositoryImpl.delete_asset")
@patch("app.adapters.chroma_vector_store_adapter.ChromaVectorStoreAdapter.delete_item")
@patch("app.repositories.project_repository.ProjectRepositoryImpl.get_asset")
def test_delete_asset_success(
    mock_repo_get_asset: MagicMock,
    mock_vector_delete_item: MagicMock,
    mock_repo_delete_asset: MagicMock,
    client: TestClient
):
    """Test successful deletion of an asset"""
    # Mock for ORM model returned by repo.get_asset (used by service.get_asset and service.delete_asset)
    mock_asset_model = MagicMock()
    mock_asset_model.id = 1
    mock_asset_model.project_id = 1
    mock_asset_model.filename = "test_asset.pdf"
    mock_asset_model.created_at = datetime.utcnow()
    mock_asset_model.updated_at = datetime.utcnow()
    mock_asset_model.vector_id = "vector_123" # For vector store deletion
    mock_asset_model.deleted_at = None # Ensure it's not already 'deleted'

    # Configure mocks
    # service.get_asset calls repo.get_asset once for validation by endpoint
    # service.delete_asset calls repo.get_asset again internally
    mock_repo_get_asset.return_value = mock_asset_model
    mock_repo_delete_asset.return_value = mock_asset_model # Simulate successful DB soft delete

    response = client.delete("/api/v1/projects/1/assets/1")

    assert response.status_code == 200
    assert response.json() == {"message": "Asset marked for deletion successfully."}

    # Assertions:
    # service.get_asset (called by endpoint for validation) calls repo.get_asset
    # service.delete_asset (called by endpoint) calls repo.get_asset, then vector_store.delete_item, then repo.delete_asset
    assert mock_repo_get_asset.call_count == 2 # Called by service.get_asset and service.delete_asset
    mock_repo_get_asset.assert_any_call(ANY, asset_id=1)
    mock_vector_delete_item.assert_called_once_with("vector_123")
    mock_repo_delete_asset.assert_called_once_with(ANY, asset_id=1)


@patch("app.repositories.project_repository.ProjectRepositoryImpl.get_asset")
def test_delete_asset_project_not_found(mock_repo_get_asset: MagicMock, client: TestClient):
    """Test deletion when asset doesn't belong to the specified project."""
    # service.get_asset (called by endpoint) calls repo.get_asset
    mock_asset_model = MagicMock()
    mock_asset_model.id = 1
    mock_asset_model.project_id = 2  # Asset belongs to project 2
    mock_asset_model.filename = "test_asset.pdf"
    mock_asset_model.created_at = datetime.utcnow()
    mock_asset_model.updated_at = datetime.utcnow()
    mock_repo_get_asset.return_value = mock_asset_model

    response = client.delete("/api/v1/projects/1/assets/1") # Trying to delete from project 1

    assert response.status_code == 403
    assert response.json() == {"detail": "Asset does not belong to the specified project."}
    mock_repo_get_asset.assert_called_once_with(ANY, asset_id=1) # Called by service.get_asset


@patch("app.repositories.project_repository.ProjectRepositoryImpl.get_asset")
def test_delete_asset_not_found(mock_repo_get_asset: MagicMock, client: TestClient):
    """Test deletion when asset is not found by the repository."""
    # service.get_asset calls repo.get_asset. If repo.get_asset returns None,
    # service.get_asset raises HTTPException(404).
    mock_repo_get_asset.return_value = None

    response = client.delete("/api/v1/projects/1/assets/999")

    assert response.status_code == 404
    assert response.json() == {"detail": "Asset not found"}
    mock_repo_get_asset.assert_called_once_with(ANY, asset_id=999) # Called by service.get_asset


@patch("app.repositories.project_repository.ProjectRepositoryImpl.get_asset")
@patch("app.adapters.chroma_vector_store_adapter.ChromaVectorStoreAdapter.delete_item")
@patch("app.repositories.project_repository.ProjectRepositoryImpl.delete_asset")
def test_delete_asset_service_exception(
    mock_repo_delete_asset: MagicMock,
    mock_vector_delete_item: MagicMock,
    mock_repo_get_asset: MagicMock,
    client: TestClient
):
    """Test deletion when the service layer encounters an unexpected error during delete_asset."""
    # Setup for service.get_asset() call in the endpoint
    mock_asset_model_for_get = MagicMock(
        id=1, project_id=1, vector_id="v123",
        filename="test.pdf", created_at=datetime.utcnow(), updated_at=datetime.utcnow()
    )
    mock_repo_get_asset.return_value = mock_asset_model_for_get

    # Make one of the internal calls within service.delete_asset raise an error
    # For example, the call to repo.delete_asset
    mock_repo_delete_asset.side_effect = Exception("Simulated DB error during repo.delete_asset")

    response = client.delete("/api/v1/projects/1/assets/1")

    assert response.status_code == 500
    assert response.json() == {"detail": "Internal server error deleting asset."}

    assert mock_repo_get_asset.call_count == 2 # Validation + in service.delete_asset
    mock_vector_delete_item.assert_called_once_with("v123")
    mock_repo_delete_asset.assert_called_once_with(ANY, asset_id=1)


@patch("app.repositories.project_repository.ProjectRepositoryImpl.get_asset")
def test_delete_asset_wrong_project_id_in_url(mock_repo_get_asset: MagicMock, client: TestClient):
    """Test deletion when asset belongs to project X but URL targets project Y."""
    # This is the same logic as test_delete_asset_project_not_found
    # The endpoint validates if asset_schema.project_id matches the project_id from URL.
    mock_asset_model = MagicMock()
    mock_asset_model.id = 1
    mock_asset_model.project_id = 2  # Asset actually in project 2
    mock_asset_model.filename = "test.pdf"
    mock_asset_model.created_at = datetime.utcnow()
    mock_asset_model.updated_at = datetime.utcnow()
    mock_repo_get_asset.return_value = mock_asset_model

    response = client.delete("/api/v1/projects/1/assets/1") # API call for project 1

    assert response.status_code == 403
    assert response.json() == {"detail": "Asset does not belong to the specified project."}
    mock_repo_get_asset.assert_called_once_with(ANY, asset_id=1)

# Note: The original test_delete_asset_db_error was testing if db.commit failed.
# The new test_delete_asset_service_exception tests a failure during the service.delete_asset call,
# specifically mocking repo.delete_asset to fail. This is a more focused way to test service error handling.
# The old test's `@patch("app.api.v1.projects.ChromaDB")` was also not ideal as ChromaDB is an implementation detail.
# Patching the VectorStorePort adapter method is preferred.
