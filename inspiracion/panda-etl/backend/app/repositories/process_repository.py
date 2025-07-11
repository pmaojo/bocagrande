from typing import List, Tuple, Optional
from sqlalchemy.orm import Session, joinedload, aliased
from sqlalchemy import func
from datetime import datetime, timezone

from app import models # Assuming models are accessible like this
from app.models.process import Process as ProcessModel, ProcessStatus
from app.models.process_step import ProcessStep, ProcessStepStatus # Keep ProcessStep for delete logic
from app.schemas.process import ProcessCreate, ProcessUpdate, ProcessSuggestion
from app.interfaces.process_repository import IProcessRepository


class ProcessRepositoryImpl(IProcessRepository):
    def get_project_processes(self, db: Session, project_id: int) -> List[Tuple[ProcessModel, int]]:
        ProcessStepAlias = aliased(models.ProcessStep) # Corrected to ProcessStep
        completed_steps_count_subquery = (
            db.query(
                ProcessStepAlias.process_id,
                func.count(ProcessStepAlias.id).label("completed_steps_count"),
            )
            .filter(ProcessStepAlias.status == ProcessStepStatus.COMPLETED, ProcessStepAlias.deleted_at.is_(None))
            .group_by(ProcessStepAlias.process_id)
            .subquery()
        )
        processes = (
            db.query(
                ProcessModel,
                func.coalesce(
                    completed_steps_count_subquery.c.completed_steps_count, 0
                ).label("completed_steps_count"),
            )
            .filter(ProcessModel.project_id == project_id, ProcessModel.deleted_at.is_(None))
            .outerjoin(
                completed_steps_count_subquery,
                ProcessModel.id == completed_steps_count_subquery.c.process_id,
            )
            # .options(joinedload(ProcessModel.project), defer(ProcessModel.output))
            .order_by(ProcessModel.id.desc())
            .all()
        )
        return processes

    def delete_project_processes_and_steps(self, db: Session, project_id: int) -> None:
        process_ids_tuples = db.query(ProcessModel.id).filter(ProcessModel.project_id == project_id, ProcessModel.deleted_at.is_(None)).all()
        process_ids = [pid[0] for pid in process_ids_tuples]
        current_timestamp = datetime.now(timezone.utc)

        db.query(ProcessModel).filter(ProcessModel.project_id == project_id, ProcessModel.deleted_at.is_(None)).update(
            {ProcessModel.deleted_at: current_timestamp, ProcessModel.updated_at: current_timestamp},
            synchronize_session=False
        )

        if process_ids:
            db.query(ProcessStep).filter(ProcessStep.process_id.in_(process_ids), ProcessStep.deleted_at.is_(None)).update(
                {ProcessStep.deleted_at: current_timestamp, ProcessStep.updated_at: current_timestamp},
                synchronize_session=False
            )
        db.commit()

    def create_process(self, db: Session, process_data: ProcessCreate) -> ProcessModel:
        # Default status to PENDING. Service layer can override if ProcessCreate is extended
        db_process = ProcessModel(
            **process_data.dict(),
            status=ProcessStatus.PENDING,
            # message is nullable in ProcessBase, but not in model. Default to empty string.
            message=process_data.message if process_data.message is not None else ""
        )
        db.add(db_process)
        db.commit()
        db.refresh(db_process)
        return db_process

    def get_process(self, db: Session, process_id: int) -> Optional[ProcessModel]:
        return db.query(ProcessModel).filter(ProcessModel.id == process_id, ProcessModel.deleted_at.is_(None)).first()

    def update_process(self, db: Session, process_id: int, process_data: ProcessUpdate) -> Optional[ProcessModel]:
        db_process = self.get_process(db, process_id)
        if db_process is None:
            return None

        update_data = process_data.dict(exclude_unset=True)
        for key, value in update_data.items():
            setattr(db_process, key, value)

        db_process.updated_at = datetime.now(timezone.utc)
        db.commit()
        db.refresh(db_process)
        return db_process

    def list_all_by_project_and_status(
        self,
        db: Session,
        project_id: Optional[int] = None,
        status: Optional[ProcessStatus] = None
    ) -> List[ProcessModel]:
        query = db.query(ProcessModel).filter(ProcessModel.deleted_at.is_(None))
        if project_id is not None:
            query = query.filter(ProcessModel.project_id == project_id)
        if status is not None:
            query = query.filter(ProcessModel.status == status)
        return query.order_by(ProcessModel.created_at.desc()).all()

    def get_process_steps(self, db: Session, process_id: int) -> List[ProcessStep]:
        """Return all steps for a process."""
        return (
            db.query(ProcessStep)
            .options(joinedload(ProcessStep.asset))
            .filter(ProcessStep.process_id == process_id, ProcessStep.deleted_at.is_(None))
            .order_by(ProcessStep.id.asc())
            .all()
        )

    def get_process_step(self, db: Session, process_step_id: int) -> Optional[ProcessStep]:
        """Return a single process step by id."""
        return (
            db.query(ProcessStep)
            .options(joinedload(ProcessStep.process), joinedload(ProcessStep.asset))
            .filter(ProcessStep.id == process_step_id, ProcessStep.deleted_at.is_(None))
            .first()
        )

    def update_process_status(self, db: Session, process: ProcessModel, status: ProcessStatus) -> None:
        """Update a process status and persist the change."""
        process.status = status
        process.updated_at = datetime.now(timezone.utc)
        db.commit()

    def update_process_step_status(
        self,
        db: Session,
        process_step: ProcessStep,
        status: ProcessStepStatus,
        output: Optional[dict] = None,
        output_references: Optional[dict] = None,
    ) -> ProcessStep:
        """Update status and optional payload for a process step."""
        process_step.status = status
        if output is not None:
            process_step.output = output
        if output_references is not None:
            process_step.output_references = output_references
        process_step.updated_at = datetime.now(timezone.utc)
        db.commit()
        db.refresh(process_step)
        return process_step

    def get_process_steps_with_asset_content(
        self, db: Session, process_id: int, statuses: List[str]
    ) -> List[ProcessStep]:
        """Return steps with their asset content eager loaded."""
        status_values = [ProcessStepStatus[s] for s in statuses]
        return (
            db.query(ProcessStep)
            .options(joinedload(ProcessStep.asset).joinedload(models.Asset.content))
            .filter(
                ProcessStep.process_id == process_id,
                ProcessStep.status.in_(status_values),
                ProcessStep.deleted_at.is_(None),
            )
            .order_by(ProcessStep.id.asc())
            .all()
        )

    def get_all_pending_processes(self, db: Session) -> List[ProcessModel]:
        """Return all processes currently pending execution."""
        return (
            db.query(ProcessModel)
            .filter(
                ProcessModel.status == ProcessStatus.PENDING,
                ProcessModel.deleted_at.is_(None),
            )
            .all()
        )

    def search_relevant_process(
        self, db: Session, suggestion: ProcessSuggestion
    ) -> List[ProcessModel]:
        """Simple search of processes by name and type within a project."""
        return (
            db.query(ProcessModel)
            .filter(
                ProcessModel.project_id == suggestion.project_id,
                ProcessModel.type == suggestion.output_type,
                ProcessModel.name.ilike(f"%{suggestion.name}%"),
                ProcessModel.deleted_at.is_(None),
            )
            .order_by(ProcessModel.created_at.desc())
            .all()
        )


_process_repo_impl = ProcessRepositoryImpl()


def get_process(db: Session, process_id: int, repo: IProcessRepository = _process_repo_impl):
    """Return a process by id."""
    return repo.get_process(db, process_id)


def get_processes(db: Session, repo: IProcessRepository = _process_repo_impl):
    """Return all processes across projects."""
    return repo.list_all_by_project_and_status(db)


def get_process_steps(db: Session, process_id: int, repo: ProcessRepositoryImpl = _process_repo_impl):
    """Return process steps for a process."""
    return repo.get_process_steps(db, process_id)


def get_process_step(db: Session, process_step_id: int, repo: ProcessRepositoryImpl = _process_repo_impl):
    """Return a single process step."""
    return repo.get_process_step(db, process_step_id)


def get_process_steps_with_asset_content(
    db: Session,
    process_id: int,
    statuses: List[str],
    repo: ProcessRepositoryImpl = _process_repo_impl,
) -> List[ProcessStep]:
    """Return steps with content pre-loaded."""
    return repo.get_process_steps_with_asset_content(db, process_id, statuses)


def update_process_step_status(
    db: Session,
    process_step: ProcessStep,
    status: ProcessStepStatus,
    output: Optional[dict] = None,
    output_references: Optional[dict] = None,
    repo: ProcessRepositoryImpl = _process_repo_impl,
) -> ProcessStep:
    """Update a process step's state using the default repository."""
    return repo.update_process_step_status(
        db,
        process_step,
        status,
        output=output,
        output_references=output_references,
    )


def update_process_status(
    db: Session,
    process: ProcessModel,
    status: ProcessStatus,
    repo: ProcessRepositoryImpl = _process_repo_impl,
) -> None:
    """Update and persist a process status."""
    repo.update_process_status(db, process, status)


def get_all_pending_processes(db: Session, repo: ProcessRepositoryImpl = _process_repo_impl) -> List[ProcessModel]:
    """Return all pending processes."""
    return repo.get_all_pending_processes(db)


def search_relevant_process(db: Session, suggestion: ProcessSuggestion, repo: ProcessRepositoryImpl = _process_repo_impl) -> List[ProcessModel]:
    """Delegate search logic to the repository implementation."""
    return repo.search_relevant_process(db, suggestion)


def delete_project_processes_and_steps(
    db: Session,
    project_id: int,
    repo: ProcessRepositoryImpl = _process_repo_impl,
) -> None:
    """Delete all processes and related steps for a project."""
    repo.delete_project_processes_and_steps(db, project_id)
