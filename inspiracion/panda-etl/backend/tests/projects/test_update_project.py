import pytest
from fastapi.testclient import TestClient

from app.schemas.project import ProjectUpdate
from tests.projects.conftest import create_project_model


@pytest.mark.usefixtures("override_project_service")
class TestUpdateProject:
    def test_update_project_success(self, client: TestClient, project_repo_mock, db_session_for_test):
        updated_project = create_project_model(1, "Updated", "New description")
        project_repo_mock.get_project_by_name.return_value = None
        project_repo_mock.update_project.return_value = updated_project

        response = client.put("/api/v1/projects/1", json={"name": "Updated", "description": "New description"})

        assert response.status_code == 200
        body = response.json()
        assert body["id"] == 1
        assert body["name"] == "Updated"
        args, kwargs = project_repo_mock.update_project.call_args
        assert args[0] is db_session_for_test
        assert kwargs["project_id"] == 1
        assert isinstance(kwargs["project"], ProjectUpdate)

    def test_update_project_not_found(self, client: TestClient, project_repo_mock):
        project_repo_mock.get_project_by_name.return_value = None
        project_repo_mock.update_project.return_value = None

        response = client.put("/api/v1/projects/1", json={"name": "Updated"})

        assert response.status_code == 404
        assert response.json() == {"detail": "Project not found for updating"}

    def test_update_project_name_conflict(self, client: TestClient, project_repo_mock):
        project_repo_mock.get_project_by_name.return_value = create_project_model(2, "Existing")

        response = client.put("/api/v1/projects/1", json={"name": "Existing"})

        assert response.status_code == 400
        assert response.json() == {"detail": "Another project with this name already exists"}
        project_repo_mock.update_project.assert_not_called()

    def test_update_project_unexpected_error(self, client: TestClient, project_repo_mock):
        project_repo_mock.get_project_by_name.return_value = None
        project_repo_mock.update_project.side_effect = Exception("db error")

        response = client.put("/api/v1/projects/1", json={"name": "Updated"})

        assert response.status_code == 500
        assert response.json() == {"detail": "Internal server error updating project."}

    def test_update_project_empty_payload(self, client: TestClient, project_repo_mock):
        project_repo_mock.get_project_by_name.return_value = None
        project_repo_mock.update_project.return_value = None

        response = client.put("/api/v1/projects/1", json={})

        assert response.status_code == 404
        assert response.json() == {"detail": "Project not found for updating"}


