"""Pytest fixtures for project-related tests."""

from __future__ import annotations

import pytest
from unittest.mock import MagicMock
from datetime import datetime

from app.main import app
from app.api.dependencies import get_project_service
from app.services.project_service import ProjectService
from app.interfaces.repositories import IProjectRepository


@pytest.fixture
def project_repo_mock():
    """Mock implementation of IProjectRepository."""
    return MagicMock(spec=IProjectRepository)


@pytest.fixture
def override_project_service(project_repo_mock, db_session_for_test):
    """Provide ProjectService with mocked dependencies via dependency override."""
    service = ProjectService(
        db=db_session_for_test,
        repo=project_repo_mock,
        asset_content_repo=MagicMock(),
        process_repo=MagicMock(),
        file_storage=MagicMock(),
        vector_store=MagicMock(),
        task_queue=MagicMock(),
    )
    app.dependency_overrides[get_project_service] = lambda: service
    yield service
    app.dependency_overrides.pop(get_project_service, None)


def create_project_model(project_id: int, name: str, description: str | None = None):
    """Utility to build a minimal Project ORM-like object."""
    project = MagicMock()
    project.id = project_id
    project.name = name
    project.description = description
    project.created_at = datetime.utcnow()
    project.updated_at = datetime.utcnow()
    project.deleted_at = None
    return project
