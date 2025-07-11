from unittest.mock import MagicMock, patch
from sqlalchemy.orm import Session

from app.repositories import project_repository, process_repository
from app.repositories.project_repository import ProjectRepositoryImpl
from app.repositories.process_repository import ProcessRepositoryImpl
from app.models.process_step import ProcessStepStatus


def test_get_project_delegates():
    db = MagicMock(spec=Session)
    with patch.object(ProjectRepositoryImpl, "get_project", return_value="proj") as mock:
        result = project_repository.get_project(db, 1)
        assert result == "proj"
        mock.assert_called_once_with(db, 1)


def test_get_asset_content_delegates():
    db = MagicMock(spec=Session)
    with patch.object(project_repository.AssetContentRepositoryImpl, "get_record_by_asset_id", return_value="content") as mock:
        result = project_repository.get_asset_content(db, 2)
        assert result == "content"
        mock.assert_called_once_with(db, 2)


def test_get_process_delegates():
    db = MagicMock(spec=Session)
    with patch.object(ProcessRepositoryImpl, "get_process", return_value="proc") as mock:
        result = process_repository.get_process(db, 3)
        assert result == "proc"
        mock.assert_called_once_with(db, 3)


def test_update_process_step_status_delegates():
    db = MagicMock(spec=Session)
    step = MagicMock()
    with patch.object(ProcessRepositoryImpl, "update_process_step_status") as mock:
        process_repository.update_process_step_status(db, step, ProcessStepStatus.COMPLETED)
        mock.assert_called_once_with(db, step, ProcessStepStatus.COMPLETED, output=None, output_references=None)

