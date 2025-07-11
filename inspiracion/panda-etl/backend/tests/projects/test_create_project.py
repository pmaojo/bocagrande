import pytest
from fastapi.testclient import TestClient

from app.schemas.project import ProjectCreate
from tests.projects.conftest import create_project_model


@pytest.mark.usefixtures("override_project_service")
class TestCreateProject:
    def test_create_project_success(self, client: TestClient, project_repo_mock, db_session_for_test):
        mock_project = create_project_model(1, "New Project")
        project_repo_mock.get_project_by_name.return_value = None
        project_repo_mock.create_project.return_value = mock_project

        response = client.post("/api/v1/projects/", json={"name": "New Project"})

        assert response.status_code == 201
        body = response.json()
        assert body["id"] == 1
        assert body["name"] == "New Project"
        assert project_repo_mock.create_project.called
        args, kwargs = project_repo_mock.create_project.call_args
        assert args[0] is db_session_for_test
        assert isinstance(kwargs["project"], ProjectCreate)

    def test_create_project_duplicate_name(self, client: TestClient, project_repo_mock):
        project_repo_mock.get_project_by_name.return_value = create_project_model(2, "New Project")

        response = client.post("/api/v1/projects/", json={"name": "New Project"})

        assert response.status_code == 400
        assert response.json() == {"detail": "Project with this name already exists"}
        project_repo_mock.create_project.assert_not_called()

    def test_create_project_unexpected_error(self, client: TestClient, project_repo_mock):
        project_repo_mock.get_project_by_name.return_value = None
        project_repo_mock.create_project.side_effect = Exception("db error")

        response = client.post("/api/v1/projects/", json={"name": "Fail"})

        assert response.status_code == 500
        assert response.json() == {"detail": "Internal server error creating project."}

    def test_create_project_validation_error(self, client: TestClient):
        response = client.post("/api/v1/projects/", json={})
        assert response.status_code == 422



