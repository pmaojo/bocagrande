import pytest
import logging
from unittest.mock import MagicMock, patch, AsyncMock, PropertyMock  # Added PropertyMock
from io import BytesIO
from sqlalchemy.orm import Session
from fastapi import HTTPException, UploadFile

from app.services.project_service import ProjectService
from app.interfaces.repositories import IProjectRepository
from app.interfaces.asset_content_repository import IAssetContentRepository
from app.interfaces.process_repository import IProcessRepository
from app.interfaces.file_storage_port import FileStoragePort
from app.interfaces.vector_store_port import VectorStorePort
from app.interfaces.task_queue_port import TaskQueuePort
from app.models.project import Project as ProjectModel
from app.models.asset import Asset as AssetModel
from app.schemas.project import (
    ProjectCreate,
    ProjectUpdate,
    ProjectListItem,
)
# from app.core.celery_app import process_file_task


# Pytest Fixtures
@pytest.fixture
def mock_db_session():
    return MagicMock(spec=Session)

@pytest.fixture
def mock_project_repository():
    return MagicMock(spec=IProjectRepository)

@pytest.fixture
def mock_asset_content_repository():
    return MagicMock(spec=IAssetContentRepository)

@pytest.fixture
def mock_process_repository():
    return MagicMock(spec=IProcessRepository)

@pytest.fixture
def mock_file_storage_port():
    return MagicMock(spec=FileStoragePort)

@pytest.fixture
def mock_vector_store_port():
    return MagicMock(spec=VectorStorePort)

@pytest.fixture
def mock_task_queue_port():
    return MagicMock(spec=TaskQueuePort)

@pytest.fixture
def project_service(
    mock_db_session: Session,
    mock_project_repository: IProjectRepository,
    mock_asset_content_repository: IAssetContentRepository,
    mock_process_repository: IProcessRepository,
    mock_file_storage_port: FileStoragePort,
    mock_vector_store_port: VectorStorePort,
    mock_task_queue_port: TaskQueuePort
):
    return ProjectService(
        db=mock_db_session,
        repo=mock_project_repository,
        asset_content_repo=mock_asset_content_repository,
        process_repo=mock_process_repository,
        file_storage=mock_file_storage_port,
        vector_store=mock_vector_store_port,
        task_queue=mock_task_queue_port
    )


class TestProjectService:
    def test_create_project_success(self, project_service: ProjectService, mock_project_repository: IProjectRepository, mock_db_session: Session):
        project_create_data = ProjectCreate(name="Test Project", description="A test project")
        mock_project_repository.get_project_by_name.return_value = None
        expected_created_project_model = ProjectModel(id=1, name=project_create_data.name, description=project_create_data.description)
        mock_project_repository.create_project.return_value = expected_created_project_model
        created_project = project_service.create_project(project_data=project_create_data)
        mock_project_repository.get_project_by_name.assert_called_once_with(mock_db_session, project_name=project_create_data.name)
        mock_project_repository.create_project.assert_called_once_with(mock_db_session, project=project_create_data)
        assert isinstance(created_project, ProjectModel)
        assert created_project.id == expected_created_project_model.id

    def test_create_project_already_exists(self, project_service: ProjectService, mock_project_repository: IProjectRepository, mock_db_session: Session):
        project_create_data = ProjectCreate(name="Existing Project", description="A project")
        existing_project_model = ProjectModel(id=1, name=project_create_data.name)
        mock_project_repository.get_project_by_name.return_value = existing_project_model
        with pytest.raises(HTTPException) as exc_info:
            project_service.create_project(project_data=project_create_data)
        assert exc_info.value.status_code == 400
        mock_project_repository.create_project.assert_not_called()

    def test_get_project_success(self, project_service: ProjectService, mock_project_repository: IProjectRepository, mock_db_session: Session):
        project_id = 1
        expected_project_model = ProjectModel(id=project_id, name="Found Project", description="Description")
        mock_project_repository.get_project.return_value = expected_project_model
        retrieved_project = project_service.get_project(project_id=project_id)
        mock_project_repository.get_project.assert_called_once_with(mock_db_session, project_id=project_id)
        assert retrieved_project == expected_project_model

    def test_get_project_not_found(self, project_service: ProjectService, mock_project_repository: IProjectRepository, mock_db_session: Session):
        project_id = 99
        mock_project_repository.get_project.return_value = None
        with pytest.raises(HTTPException) as exc_info:
            project_service.get_project(project_id=project_id)
        assert exc_info.value.status_code == 404
        mock_project_repository.get_project.assert_called_once_with(mock_db_session, project_id=project_id)

    def test_update_project_success(self, project_service: ProjectService, mock_project_repository: IProjectRepository, mock_db_session: Session):
        project_id = 1
        project_update_data = ProjectUpdate(name="Updated Name", description="Updated Description")
        mock_project_repository.get_project_by_name.return_value = None
        updated_project_model = ProjectModel(id=project_id, name=project_update_data.name, description=project_update_data.description)
        mock_project_repository.update_project.return_value = updated_project_model
        result = project_service.update_project(project_id=project_id, project_data=project_update_data)
        mock_project_repository.update_project.assert_called_once_with(mock_db_session, project_id=project_id, project=project_update_data)
        assert result == updated_project_model

    def test_update_project_new_name_matches_same_project(self, project_service: ProjectService, mock_project_repository: IProjectRepository, mock_db_session: Session):
        project_id = 1
        project_update_data = ProjectUpdate(name="Updated Name", description="Updated Description")
        existing_project_with_new_name = ProjectModel(id=project_id, name=project_update_data.name)
        mock_project_repository.get_project_by_name.return_value = existing_project_with_new_name
        updated_project_model = ProjectModel(id=project_id, name=project_update_data.name, description=project_update_data.description)
        mock_project_repository.update_project.return_value = updated_project_model
        result = project_service.update_project(project_id=project_id, project_data=project_update_data)
        assert result == updated_project_model

    def test_update_project_not_found(self, project_service: ProjectService, mock_project_repository: IProjectRepository, mock_db_session: Session):
        project_id = 99
        project_update_data = ProjectUpdate(name="Updated Name")
        mock_project_repository.get_project_by_name.return_value = None
        mock_project_repository.update_project.return_value = None
        with pytest.raises(HTTPException) as exc_info:
            project_service.update_project(project_id=project_id, project_data=project_update_data)
        assert exc_info.value.status_code == 404

    def test_update_project_name_conflict(self, project_service: ProjectService, mock_project_repository: IProjectRepository, mock_db_session: Session):
        project_id = 1
        project_update_data = ProjectUpdate(name="Conflicting Name")
        conflicting_project_model = ProjectModel(id=2, name=project_update_data.name)
        mock_project_repository.get_project_by_name.return_value = conflicting_project_model
        with pytest.raises(HTTPException) as exc_info:
            project_service.update_project(project_id=project_id, project_data=project_update_data)
        assert exc_info.value.status_code == 400
        mock_project_repository.update_project.assert_not_called()

    def test_delete_project_success(self, project_service: ProjectService, mock_project_repository: IProjectRepository, mock_vector_store_port: VectorStorePort, mock_db_session: Session):
        project_id = 1
        project_to_delete = ProjectModel(id=project_id, name="Test Project")
        asset_to_delete1 = AssetModel(id=10, project_id=project_id, filename="asset1.pdf", type="file", path="/some/path1")
        setattr(asset_to_delete1, 'vector_id', 'vec1')
        asset_to_delete2 = AssetModel(id=11, project_id=project_id, filename="asset2.txt", type="file", path="/some/path2")
        setattr(asset_to_delete2, 'vector_id', None)
        mock_project_repository.get_project.return_value = project_to_delete
        mock_project_repository.get_assets_without_content.return_value = [asset_to_delete1, asset_to_delete2]
        mock_project_repository.delete_project.return_value = project_to_delete
        deleted_project = project_service.delete_project(project_id=project_id)
        mock_vector_store_port.delete_item.assert_called_once_with(asset_to_delete1.vector_id)
        assert deleted_project == project_to_delete

    def test_delete_project_vector_deletion_fails(self, project_service: ProjectService, mock_project_repository: IProjectRepository, mock_vector_store_port: VectorStorePort, mock_db_session: Session, caplog):
        project_id = 1
        project_to_delete = ProjectModel(id=project_id, name="Test Project")
        asset_with_vector = AssetModel(id=10, project_id=project_id, filename="asset1.pdf", type="file", path="/some/path")
        setattr(asset_with_vector, 'vector_id', 'vec1')
        mock_project_repository.get_project.return_value = project_to_delete
        mock_project_repository.get_assets_without_content.return_value = [asset_with_vector]
        mock_vector_store_port.delete_item.side_effect = Exception("Vector deletion failed")
        mock_project_repository.delete_project.return_value = project_to_delete
        with caplog.at_level(logging.ERROR):
            project_service.delete_project(project_id=project_id)
        assert f"Error deleting vector for asset {asset_with_vector.id}: Vector deletion failed" in caplog.text

    def test_delete_project_not_found(self, project_service: ProjectService, mock_project_repository: IProjectRepository, mock_db_session: Session):
        project_id = 99
        mock_project_repository.get_project.return_value = None
        with pytest.raises(HTTPException) as exc_info:
            project_service.delete_project(project_id=project_id)
        assert exc_info.value.status_code == 404

    @patch('app.services.project_service.os')
    @patch('app.services.project_service.settings')
    def test_upload_file_asset_success(
        self,
        mock_settings,
        mock_os,
        project_service: ProjectService,
        mock_project_repository: IProjectRepository,
        mock_db_session: Session
    ):
        project_id = 1
        mock_file = MagicMock(spec=UploadFile)
        mock_file.filename = "test_document.pdf"
        mock_file.content_type = "application/pdf"
        mock_file.file = BytesIO(b"small content")
        mock_file.size = None

        # Configure mock_settings using configure_mock
        mock_settings.configure_mock(
            allowed_content_types=["application/pdf", "text/plain"],
            max_file_size=10 * 1024 * 1024
        )

        mock_os.path.join.side_effect = lambda *args: "/".join(args)
        mock_os.path.splitext.return_value = ("test_document", ".pdf")

        mock_project_repository.get_project.return_value = ProjectModel(id=project_id, name="Test Project")
        created_asset_model = AssetModel(id=1, filename=mock_file.filename, path="test_uploads/1/testuuid.pdf", type="file", project_id=project_id, size=12345)
        setattr(created_asset_model, 'created_at', MagicMock())
        setattr(created_asset_model, 'updated_at', MagicMock())
        mock_project_repository.create_asset.return_value = created_asset_model

        project_service.file_storage.create_project_directory = MagicMock(return_value="test_uploads/1")
        project_service.file_storage.generate_unique_file_path = MagicMock(return_value="test_uploads/1/testuuid.pdf")
        project_service.file_storage.save_file = MagicMock()
        project_service.file_storage.get_file_size = MagicMock(return_value=12345)

        result_asset = project_service.upload_file_asset(project_id=project_id, file=mock_file)

        project_service.task_queue.submit_process_file_task.assert_called_once_with(created_asset_model.id, "test_uploads/1/testuuid.pdf")
        assert result_asset == created_asset_model

    @patch('app.services.project_service.settings')
    def test_upload_file_asset_invalid_type(
        self, mock_settings, project_service: ProjectService, mock_project_repository: IProjectRepository
    ):
        project_id = 1
        mock_file = MagicMock(spec=UploadFile)
        mock_file.filename = "test_image.jpg"
        mock_file.content_type = "image/jpeg"
        mock_file.size = 12345
        mock_settings.allowed_content_types = ["application/pdf"]
        mock_project_repository.get_project.return_value = ProjectModel(id=project_id, name="Test Project")
        with pytest.raises(HTTPException) as exc_info:
            project_service.upload_file_asset(project_id=project_id, file=mock_file)
        assert exc_info.value.status_code == 400

    @patch('app.services.project_service.settings')
    def test_upload_file_asset_too_large(
        self,
        mock_settings,
        project_service: ProjectService,
        mock_project_repository: IProjectRepository,
    ):
        project_id = 1
        data = b"0" * (2 * 1024 * 1024)
        file_obj = BytesIO(data)
        mock_file = MagicMock(spec=UploadFile)
        mock_file.filename = "big.pdf"
        mock_file.content_type = "application/pdf"
        mock_file.file = file_obj
        mock_file.size = None
        mock_settings.configure_mock(
            allowed_content_types=["application/pdf"],
            max_file_size=1 * 1024 * 1024,
        )
        mock_project_repository.get_project.return_value = ProjectModel(id=project_id, name="Test Project")
        with pytest.raises(HTTPException) as exc_info:
            project_service.upload_file_asset(project_id=project_id, file=mock_file)
        assert exc_info.value.status_code == 400

    def test_upload_file_asset_project_not_found(self, project_service: ProjectService, mock_project_repository: IProjectRepository):
        project_id = 99
        mock_file = MagicMock(spec=UploadFile)
        mock_file.filename = "test.pdf"
        mock_file.content_type = "application/pdf"
        mock_file.size = 123
        mock_project_repository.get_project.return_value = None
        with pytest.raises(HTTPException) as exc_info:
             project_service.upload_file_asset(project_id=project_id, file=mock_file)
        assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    @patch('app.services.project_service.os')
    @patch('app.services.project_service.httpx.AsyncClient')
    @patch('app.services.project_service.uuid.uuid4')
    @patch('app.services.project_service.settings')
    @patch('builtins.open', new_callable=MagicMock)
    async def test_add_url_asset_success(
        self, mock_open, mock_settings, mock_uuid4, mock_async_client_constructor, mock_os,
        project_service: ProjectService, mock_project_repository: IProjectRepository, mock_db_session: Session
    ):
        project_id = 1
        test_url = "http://example.com/page.html"
        asset_name_param = "My Page"
        html_content = "<html><body>Test HTML</body></html>"
        mock_settings.PANDA_ETL_UPLOAD_FOLDER = "test_uploads"
        mock_os.path.join.side_effect = lambda *args: "/".join(args)
        mock_uuid_obj = MagicMock()
        mock_uuid_obj.__str__.return_value = "testurluuid"
        mock_uuid4.return_value = mock_uuid_obj
        mock_project_repository.get_project.return_value = ProjectModel(id=project_id, name="Test Project")
        mock_response = MagicMock()
        mock_response.text = html_content
        mock_response.raise_for_status = MagicMock()
        mock_async_client_instance = AsyncMock()
        mock_async_client_instance.get.return_value = mock_response
        mock_async_client_constructor.return_value = mock_async_client_instance

        created_asset_model = AssetModel(id=2, filename=(asset_name_param or test_url.split('/')[-1]), path=f"test_uploads/{project_id}/url_assets/testurluuid.html", type="url", project_id=project_id, size=len(html_content))
        setattr(created_asset_model, 'created_at', MagicMock())
        setattr(created_asset_model, 'updated_at', MagicMock())

        mock_project_repository.create_asset.return_value = created_asset_model
        project_service.file_storage.create_project_sub_directory = MagicMock(return_value=f"test_uploads/{project_id}/url_assets")
        project_service.file_storage.generate_unique_file_path = MagicMock(return_value=f"test_uploads/{project_id}/url_assets/testurluuid.html")
        project_service.file_storage.save_text_file = MagicMock()
        project_service.file_storage.get_file_size = MagicMock(return_value=len(html_content))

        result_asset = await project_service.add_url_asset(project_id=project_id, url=test_url, asset_name=asset_name_param)

        project_service.task_queue.submit_process_file_task.assert_called_once_with(created_asset_model.id, created_asset_model.path)
        assert result_asset == created_asset_model


    @pytest.mark.asyncio
    async def test_add_url_asset_project_not_found(self, project_service: ProjectService, mock_project_repository: IProjectRepository):
        project_id = 99
        test_url = "http://example.com"
        mock_project_repository.get_project.return_value = None
        with pytest.raises(HTTPException) as exc_info:
            await project_service.add_url_asset(project_id=project_id, url=test_url)
        assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    @patch('app.services.project_service.httpx.AsyncClient')
    async def test_add_url_asset_fetch_failure(
        self, mock_async_client_constructor, project_service: ProjectService, mock_project_repository: IProjectRepository
    ):
        project_id = 1
        test_url = "http://invalid-url-or-server-down.com"
        mock_project_repository.get_project.return_value = ProjectModel(id=project_id, name="Test Project")
        mock_async_client_instance = AsyncMock()
        mock_async_client_instance.get.side_effect = Exception("Failed to connect")
        mock_async_client_constructor.return_value = mock_async_client_instance
        with pytest.raises(HTTPException) as exc_info:
            await project_service.add_url_asset(project_id=project_id, url=test_url)
        assert exc_info.value.status_code == 400

    def test_delete_asset_success_with_vector(self, project_service: ProjectService, mock_project_repository: IProjectRepository, mock_vector_store_port: VectorStorePort, mock_db_session: Session):
        asset_id = 1
        asset_to_delete = AssetModel(id=asset_id, filename="test.pdf", type="file", path="/some/path", project_id=1)
        setattr(asset_to_delete, 'vector_id', 'vec123')
        mock_project_repository.get_asset.return_value = asset_to_delete
        mock_project_repository.delete_asset.return_value = asset_to_delete
        deleted_asset = project_service.delete_asset(asset_id=asset_id)
        mock_vector_store_port.delete_item.assert_called_once_with("vec123")
        assert deleted_asset == asset_to_delete

    def test_delete_asset_success_no_vector(self, project_service: ProjectService, mock_project_repository: IProjectRepository, mock_vector_store_port: VectorStorePort, mock_db_session: Session):
        asset_id = 2
        asset_to_delete = AssetModel(id=asset_id, filename="test.txt", type="file", path="/some/path", project_id=1)
        setattr(asset_to_delete, 'vector_id', None)
        mock_project_repository.get_asset.return_value = asset_to_delete
        mock_project_repository.delete_asset.return_value = asset_to_delete
        deleted_asset = project_service.delete_asset(asset_id=asset_id)
        mock_vector_store_port.delete_item.assert_not_called()
        assert deleted_asset == asset_to_delete

    def test_delete_asset_not_found(self, project_service: ProjectService, mock_project_repository: IProjectRepository, mock_db_session: Session):
        asset_id = 99
        mock_project_repository.get_asset.return_value = None
        with pytest.raises(HTTPException) as exc_info:
            project_service.delete_asset(asset_id=asset_id)
        assert exc_info.value.status_code == 404

    def test_delete_asset_vector_deletion_fails(self, project_service: ProjectService, mock_project_repository: IProjectRepository, mock_vector_store_port: VectorStorePort, mock_db_session: Session, caplog):
        asset_id = 1
        asset_to_delete = AssetModel(id=asset_id, filename="test.pdf", type="file", path="/some/path", project_id=1)
        setattr(asset_to_delete, 'vector_id', 'vec123')
        mock_project_repository.get_asset.return_value = asset_to_delete
        mock_vector_store_port.delete_item.side_effect = Exception("Vector DB error")
        mock_project_repository.delete_asset.return_value = asset_to_delete
        with caplog.at_level(logging.ERROR):
            deleted_asset = project_service.delete_asset(asset_id=asset_id)
        assert f"Error deleting vector for asset {asset_id}: Vector DB error" in caplog.text
        assert deleted_asset == asset_to_delete

    def test_get_project_assets_success(self, project_service: ProjectService, mock_project_repository: IProjectRepository, mock_db_session: Session):
        project_id = 1
        page = 1
        page_size = 10
        order_by = "name_asc"
        mock_project_repository.get_project.return_value = ProjectModel(id=project_id, name="Test Project")
        asset_model1 = AssetModel(id=1, project_id=project_id, filename="Asset A", path="/path/a.pdf", type="file", size=100, created_at=MagicMock(), updated_at=MagicMock())
        asset_model2 = AssetModel(id=2, project_id=project_id, filename="Asset B", path="/path/b.txt", type="file", size=200, created_at=MagicMock(), updated_at=MagicMock())
        repo_assets = [asset_model1, asset_model2]
        total_count = 2
        mock_project_repository.get_assets.return_value = (repo_assets, total_count)
        asset_schemas, count = project_service.get_project_assets(project_id, page, page_size, order_by)
        mock_project_repository.get_project.assert_called_once_with(mock_db_session, project_id=project_id)
        assert asset_schemas[0].filename == asset_model1.filename

    def test_get_project_assets_project_not_found(self, project_service: ProjectService, mock_project_repository: IProjectRepository, mock_db_session: Session):
        project_id = 99
        page = 1
        page_size = 10
        mock_project_repository.get_project.return_value = None
        with pytest.raises(HTTPException) as exc_info:
            project_service.get_project_assets(project_id, page, page_size)
        assert exc_info.value.status_code == 404
        mock_project_repository.get_project.assert_called_once_with(mock_db_session, project_id=project_id)

    def test_get_project_assets_no_assets(self, project_service: ProjectService, mock_project_repository: IProjectRepository, mock_db_session: Session):
        project_id = 1
        page = 1
        page_size = 10
        mock_project_repository.get_project.return_value = ProjectModel(id=project_id, name="Test Project")
        mock_project_repository.get_assets.return_value = ([], 0)
        asset_schemas, count = project_service.get_project_assets(project_id, page, page_size)
        mock_project_repository.get_project.assert_called_once_with(mock_db_session, project_id=project_id)
        assert count == 0

    def test_get_asset_schema_success(self, project_service: ProjectService, mock_project_repository: IProjectRepository, mock_db_session: Session):
        asset_id = 1
        asset_model = AssetModel(
            id=asset_id, filename="test.pdf", path="/path/test.pdf", type="file", project_id=1,
            size=12345, created_at=MagicMock(), updated_at=MagicMock()
        )
        mock_project_repository.get_asset.return_value = asset_model
        asset_schema = project_service.get_asset(asset_id=asset_id)
        assert asset_schema.filename == asset_model.filename

    def test_get_asset_schema_not_found(self, project_service: ProjectService, mock_project_repository: IProjectRepository, mock_db_session: Session):
        asset_id = 99
        mock_project_repository.get_asset.return_value = None
        with pytest.raises(HTTPException) as exc_info:
            project_service.get_asset(asset_id=asset_id)
        assert exc_info.value.status_code == 404

    def test_get_asset_model_success(self, project_service: ProjectService, mock_project_repository: IProjectRepository, mock_db_session: Session):
        asset_id = 1
        expected_asset_model = AssetModel(id=asset_id, filename="test.pdf", type="file", path="/path", project_id=1)
        mock_project_repository.get_asset.return_value = expected_asset_model
        asset_model_result = project_service.get_asset_model(asset_id=asset_id)
        assert asset_model_result == expected_asset_model

    def test_get_asset_model_not_found(self, project_service: ProjectService, mock_project_repository: IProjectRepository, mock_db_session: Session):
        asset_id = 99
        mock_project_repository.get_asset.return_value = None
        with pytest.raises(HTTPException) as exc_info:
            project_service.get_asset_model(asset_id=asset_id)
        assert exc_info.value.status_code == 404

    def test_get_projects_success(self, project_service: ProjectService, mock_project_repository: IProjectRepository, mock_db_session: Session):
        page = 1
        page_size = 5
        mock_repo_projects_list = [
            ProjectListItem(ProjectModel(id=1, name="Project A"), 2),
            ProjectListItem(ProjectModel(id=2, name="Project B"), 5),
        ]
        mock_total_project_count = 20
        mock_project_repository.get_projects.return_value = (
            mock_repo_projects_list,
            mock_total_project_count,
        )
        result_projects, result_total_count = project_service.get_projects(page=page, page_size=page_size)
        assert result_projects == mock_repo_projects_list
        assert result_total_count == mock_total_project_count

    def test_get_projects_no_projects(self, project_service: ProjectService, mock_project_repository: IProjectRepository, mock_db_session: Session):
        page = 1
        page_size = 5
        mock_project_repository.get_projects.return_value = ([], 0)
        result_projects, result_total_count = project_service.get_projects(page=page, page_size=page_size)
        assert result_projects == []
        assert result_total_count == 0

    def test_get_asset_content_from_db_success(self, project_service: ProjectService, mock_project_repository: IProjectRepository, mock_db_session: Session):
        asset_id = 1
        expected_content = {"text": "This is extracted text."}

        mock_asset = MagicMock(spec=AssetModel)
        mock_asset.id = asset_id

        mock_asset_content_instance = MagicMock()
        mock_asset_content_instance.configure_mock(content=expected_content)

        type(mock_asset).content = PropertyMock(return_value=mock_asset_content_instance) # Mock the relationship to return our instance

        mock_project_repository.get_asset.return_value = mock_asset

        retrieved_json_content = project_service.get_asset_content_from_db(asset_id=asset_id)
        assert retrieved_json_content == expected_content

    def test_get_asset_content_from_db_asset_not_found(self, project_service: ProjectService, mock_project_repository: IProjectRepository, mock_db_session: Session):
        asset_id = 99
        mock_project_repository.get_asset.return_value = None
        with pytest.raises(HTTPException) as exc_info:
            project_service.get_asset_content_from_db(asset_id=asset_id)
        assert exc_info.value.status_code == 404

    def test_get_asset_content_from_db_no_content(self, project_service: ProjectService, mock_project_repository: IProjectRepository, mock_db_session: Session):
        asset_id = 1
        mock_asset = MagicMock(spec=AssetModel)
        mock_asset.id = asset_id

        mock_asset_content_instance = MagicMock()
        mock_asset_content_instance.content = None # JSON field is None
        type(mock_asset).content = PropertyMock(return_value=mock_asset_content_instance) # Relationship returns the mock

        mock_project_repository.get_asset.return_value = mock_asset
        with pytest.raises(HTTPException) as exc_info:
            project_service.get_asset_content_from_db(asset_id=asset_id)
        assert exc_info.value.status_code == 404

    @patch('app.services.project_service.os.path.exists')
    def test_get_asset_file_path_success(self, mock_os_path_exists, project_service: ProjectService, mock_project_repository: IProjectRepository, mock_db_session: Session):
        asset_id = 1
        expected_path = "/uploads/project1/file.pdf"
        asset_model = AssetModel(id=asset_id, filename="file.pdf", path=expected_path, type="file", project_id=1)
        mock_project_repository.get_asset.return_value = asset_model
        mock_os_path_exists.return_value = True
        file_path = project_service.get_asset_file_path(asset_id=asset_id)
        assert file_path == expected_path

    def test_get_asset_file_path_asset_not_found(self, project_service: ProjectService, mock_project_repository: IProjectRepository, mock_db_session: Session):
        asset_id = 99
        mock_project_repository.get_asset.return_value = None
        with pytest.raises(HTTPException) as exc_info:
            project_service.get_asset_file_path(asset_id=asset_id)
        assert exc_info.value.status_code == 404

    @patch('app.services.project_service.os.path.exists')
    def test_get_asset_file_path_no_path_in_db(self, mock_os_path_exists, project_service: ProjectService, mock_project_repository: IProjectRepository, mock_db_session: Session):
        asset_id = 1
        asset_model = AssetModel(id=asset_id, filename="file.pdf", path=None, type="file", project_id=1)
        mock_project_repository.get_asset.return_value = asset_model
        with pytest.raises(HTTPException) as exc_info:
            project_service.get_asset_file_path(asset_id=asset_id)
        assert exc_info.value.status_code == 404
        mock_os_path_exists.assert_not_called()

    @patch('app.services.project_service.os.path.exists')
    def test_get_asset_file_path_file_not_exists_on_disk(self, mock_os_path_exists, project_service: ProjectService, mock_project_repository: IProjectRepository, mock_db_session: Session):
        asset_id = 1
        db_path = "/uploads/project1/ghost_file.pdf"
        asset_model = AssetModel(id=asset_id, filename="ghost_file.pdf", path=db_path, type="file", project_id=1)
        mock_project_repository.get_asset.return_value = asset_model
        mock_os_path_exists.return_value = False
        with pytest.raises(HTTPException) as exc_info:
            project_service.get_asset_file_path(asset_id=asset_id)
        assert exc_info.value.status_code == 404
        mock_os_path_exists.assert_called_once_with(db_path)

    def test_placeholder(self):
        assert True
