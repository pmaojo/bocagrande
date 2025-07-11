from abc import ABC, abstractmethod

class ProcessSchedulerPort(ABC):
    @abstractmethod
    def add_process_to_queue(self, process_id: int) -> None:
        """Adds a process ID to the scheduler's queue for execution."""
        pass

    @abstractmethod
    def start(self) -> None:
        """Starts the scheduler's monitoring process if not already running."""
        pass

    @abstractmethod
    def stop(self) -> None:
        """Stops the scheduler's monitoring process."""
        pass
