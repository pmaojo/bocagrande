
from app.adapters.celery_task_queue_adapter import CeleryTaskQueueAdapter


class DummyTask:
    def __init__(self):
        self.calls = []

    def delay(self, asset_id: int, file_path: str) -> None:
        self.calls.append((asset_id, file_path))


def test_custom_task_is_used():
    task = DummyTask()
    adapter = CeleryTaskQueueAdapter(task=task)

    adapter.submit_process_file_task(5, "dummy/path.pdf")

    assert task.calls == [(5, "dummy/path.pdf")]
