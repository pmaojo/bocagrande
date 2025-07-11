from abc import ABC, abstractmethod

class TaskQueuePort(ABC):
    @abstractmethod
    def submit_process_file_task(self, asset_id: int, file_path: str) -> None:
        """
        Submits a task to process a file associated with an asset.

        Args:
            asset_id: The ID of the asset being processed.
            file_path: The path to the file that needs processing.
        """
        pass
