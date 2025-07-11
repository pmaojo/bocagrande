from abc import ABC, abstractmethod
from typing import List, Tuple, Optional

from sqlalchemy.orm import Session

from app.models.process import Process as ProcessModel, ProcessStatus
from app.models.process_step import ProcessStep, ProcessStepStatus
from app.schemas.process import ProcessCreate, ProcessUpdate


class IProcessRepository(ABC):
    @abstractmethod
    def get_project_processes(self, db: Session, project_id: int) -> List[Tuple[ProcessModel, int]]:
        pass

    @abstractmethod
    def delete_project_processes_and_steps(self, db: Session, project_id: int) -> None:
        pass

    @abstractmethod
    def create_process(self, db: Session, process: ProcessCreate) -> ProcessModel:
        pass

    @abstractmethod
    def get_process(self, db: Session, process_id: int) -> Optional[ProcessModel]:
        pass

    @abstractmethod
    def update_process(self, db: Session, process_id: int, process_data: ProcessUpdate) -> Optional[ProcessModel]:
        pass

    @abstractmethod
    def list_all_by_project_and_status(self, db: Session, project_id: Optional[int] = None, status: Optional[ProcessStatus] = None) -> List[ProcessModel]:
        pass

    @abstractmethod
    def get_process_steps(self, db: Session, process_id: int) -> List[ProcessStep]:
        """Return all steps for the given process."""
        pass

    @abstractmethod
    def update_process_step_status(
        self,
        db: Session,
        process_step: ProcessStep,
        status: ProcessStepStatus,
        output: Optional[dict] = None,
        output_references: Optional[dict] = None,
    ) -> ProcessStep:
        """Persist status and optional payload for a process step."""
        pass
