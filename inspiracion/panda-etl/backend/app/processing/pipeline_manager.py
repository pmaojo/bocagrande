from __future__ import annotations
from typing import Dict, Any, List, Optional
import datetime
from sqlalchemy.orm import Session

from app.interfaces.process_scheduler_port import ProcessSchedulerPort
from app.interfaces.process_repository import IProcessRepository
from app.schemas.process import ProcessCreate, ProcessUpdate, ProcessSchema
from app.models.process import ProcessStatus
from app.logger import Logger

from .pipeline_repository import PipelineRepository
from .pipeline_runner import PipelineRunner, PipelineExecutionError


class PipelineError(Exception):
    pass


class PipelineManager:
    """Coordinate repository and runner for pipeline operations."""

    def __init__(
        self,
        repository: PipelineRepository,
        runner: PipelineRunner,
        process_repo: IProcessRepository,
        db: Session,
        scheduler_port: ProcessSchedulerPort,
        logger: Optional[Logger] = None,
    ) -> None:
        self.repository = repository
        self.runner = runner
        self.process_repo = process_repo
        self.db = db
        self.scheduler_port = scheduler_port
        self.logger = logger or Logger()

    # Repository delegates
    def create_pipeline(
        self, name: str, description: str = "", project_id: Optional[int] = None
    ) -> str:
        try:
            return self.repository.create_pipeline(name, description, project_id)
        except Exception as exc:  # pragma: no cover - upstream errors
            raise PipelineError(str(exc))

    def save_pipeline_version(
        self,
        pipeline_id: str,
        nodes: List[Dict[str, Any]],
        edges: List[Dict[str, Any]],
        transform_script: str,
        config: Dict[str, Any],
        version_name: Optional[str] = None,
        description: str = "",
    ) -> str:
        try:
            return self.repository.save_pipeline_version(
                pipeline_id,
                nodes,
                edges,
                transform_script,
                config,
                version_name,
                description,
            )
        except Exception as exc:
            raise PipelineError(str(exc))

    def load_pipeline(self, pipeline_id: str) -> Dict[str, Any]:
        try:
            return self.repository.load_pipeline(pipeline_id)
        except Exception as exc:
            raise PipelineError(str(exc))

    def load_pipeline_version(
        self, pipeline_id: str, version: Optional[str] = None
    ) -> Dict[str, Any]:
        try:
            return self.repository.load_pipeline_version(pipeline_id, version)
        except Exception as exc:
            raise PipelineError(str(exc))

    def list_pipelines(self, project_id: Optional[int] = None) -> List[Dict[str, Any]]:
        try:
            return self.repository.list_pipelines(project_id)
        except Exception as exc:
            raise PipelineError(str(exc))

    def list_pipeline_versions(self, pipeline_id: str) -> List[Dict[str, Any]]:
        try:
            return self.repository.list_pipeline_versions(pipeline_id)
        except Exception as exc:
            raise PipelineError(str(exc))

    def delete_pipeline(self, pipeline_id: str) -> None:
        try:
            self.repository.delete_pipeline(pipeline_id)
        except Exception as exc:
            raise PipelineError(str(exc))

    def delete_pipeline_version(self, pipeline_id: str, version: str) -> None:
        try:
            self.repository.delete_pipeline_version(pipeline_id, version)
        except Exception as exc:
            raise PipelineError(str(exc))

    # Runner delegate
    def execute_pipeline(
        self,
        pipeline_id: str,
        version: Optional[str] = None,
        input_config: Optional[Dict[str, Any]] = None,
        output_config: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        try:
            return self.runner.run(pipeline_id, version, input_config, output_config)
        except PipelineExecutionError as exc:
            raise PipelineError(str(exc))

    # Process and scheduling
    def _execute_pipeline_by_id(self, process_id: int) -> None:
        if not self.db or not self.process_repo:
            self.logger.error(
                f"[PipelineManager]: Database session or process repository not available for process {process_id}"
            )
            return
        process = self.process_repo.get_process(self.db, process_id)
        if not process:
            self.logger.error(f"[PipelineManager]: Process {process_id} not found")
            return
        try:
            self.process_repo.update_process(
                self.db,
                process_id,
                ProcessUpdate(
                    status=ProcessStatus.IN_PROGRESS, started_at=datetime.datetime.now()
                ),
            )
            details = process.details or {}
            result = self.execute_pipeline(
                pipeline_id=details.get("pipeline_id"),
                version=details.get("version"),
                input_config=details.get("input_config"),
                output_config=details.get("output_config"),
            )
            self.process_repo.update_process(
                self.db,
                process_id,
                ProcessUpdate(
                    status=ProcessStatus.COMPLETED,
                    completed_at=datetime.datetime.now(),
                    output=result,
                    message="Pipeline executed successfully",
                ),
            )
        except Exception as exc:  # pragma: no cover - execution errors logged
            self.logger.error(
                f"[PipelineManager]: Error executing process {process_id}: {exc}"
            )
            try:
                self.process_repo.update_process(
                    self.db,
                    process_id,
                    ProcessUpdate(
                        status=ProcessStatus.FAILED,
                        completed_at=datetime.datetime.now(),
                        message=str(exc),
                    ),
                )
            except Exception:
                self.logger.error(
                    f"[PipelineManager]: Error updating process status for {process_id}"
                )

    def schedule_pipeline_execution(
        self,
        pipeline_id: str,
        project_id: int,
        version: Optional[str] = None,
        input_config: Optional[Dict[str, Any]] = None,
        output_config: Optional[Dict[str, Any]] = None,
        name: str = "",
    ) -> int:
        if not self.db or not self.process_repo:
            raise PipelineError("Database session or process repository not available")
        process_data = ProcessCreate(
            name=name or f"Pipeline {pipeline_id} execution",
            type="pipeline_execution",
            project_id=project_id,
            message="Scheduled for execution",
            details={
                "pipeline_id": pipeline_id,
                "version": version,
                "input_config": input_config,
                "output_config": output_config,
            },
        )
        created = self.process_repo.create_process(self.db, process_data)
        self.scheduler_port.add_process_to_queue(created.id)
        return created.id

    def get_process_status(self, process_id: int) -> Dict[str, Any]:
        if not self.db or not self.process_repo:
            raise PipelineError("Database session or process repository not available")
        process = self.process_repo.get_process(self.db, process_id)
        if not process:
            raise PipelineError(f"Process {process_id} not found")
        return ProcessSchema.from_orm(process).dict()

    def list_processes(
        self, project_id: Optional[int] = None, status: Optional[ProcessStatus] = None
    ) -> List[Dict[str, Any]]:
        if not self.db or not self.process_repo:
            raise PipelineError("Database session or process repository not available")
        processes = self.process_repo.list_all_by_project_and_status(
            self.db, project_id, status
        )
        return [ProcessSchema.from_orm(p).dict() for p in processes]
