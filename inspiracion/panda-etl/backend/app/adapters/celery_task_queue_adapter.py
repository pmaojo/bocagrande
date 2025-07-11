"""Celery adapter for submitting asynchronous tasks."""

from typing import Callable, Optional, Any

from app.interfaces.task_queue_port import TaskQueuePort
from app.core.celery_app import process_file_task

TaskCallable = Callable[[int, str], Any]


class CeleryTaskQueueAdapter(TaskQueuePort):
    """Adapter that delegates file processing to a Celery task."""

    def __init__(self, task: Optional[TaskCallable] = None) -> None:
        """Initialize the adapter.

        Parameters
        ----------
        task:
            Celery task or callable with a ``delay`` method to be invoked when
            submitting work. Defaults to :data:`process_file_task`.
        """
        self._task = task or process_file_task

    def submit_process_file_task(self, asset_id: int, file_path: str) -> None:
        """Submit ``process_file_task`` with provided parameters."""
        if hasattr(self._task, "delay"):
            self._task.delay(asset_id, file_path)
        else:
            self._task(asset_id, file_path)
