from app.interfaces.process_scheduler_port import ProcessSchedulerPort
from app.processing.process_scheduler import ProcessScheduler
from typing import Callable, Optional # Added Optional
from app.logger import Logger

# Custom Exception for cleaner error handling if desired
class IllegalStateException(RuntimeError):
    pass

class ProcessSchedulerAdapter(ProcessSchedulerPort):
    def __init__(self,
                 logger: Logger,
                 poll_interval_secs: int = 5):
        """
        Initializes the ProcessSchedulerAdapter. Executor callback is set later.

        Args:
            logger: An instance of the logger.
            poll_interval_secs: How often the scheduler checks the queue (in seconds).
        """
        self._logger = logger
        self._poll_interval_secs = poll_interval_secs
        self._executor_callback: Optional[Callable[[int], None]] = None
        self._scheduler: Optional[ProcessScheduler] = None
        self._is_running: bool = False

    def set_executor_callback(self, callback: Callable[[int], None]) -> None:
        """Sets the executor callback function."""
        if self._is_running:
            raise IllegalStateException("Cannot set executor callback while scheduler is running.")
        self._executor_callback = callback

    def add_process_to_queue(self, process_id: int) -> None:
        """Adds a process ID to the scheduler's queue for execution."""
        if not self._scheduler or not self._is_running:
            # Or log an error and do nothing, depending on desired strictness
            self._logger.error("Scheduler not started or not initialized. Cannot add process to queue.")
            raise IllegalStateException("Scheduler not started. Cannot add process to queue.")
        self._scheduler.add_process_to_queue(process_id)

    def start(self) -> None:
        """
        Starts the scheduler's monitoring process.
        Requires executor_callback to be set.
        """
        if self._is_running:
            self._logger.info("ProcessSchedulerAdapter: Start called but already running.")
            return

        if not self._executor_callback:
            raise IllegalStateException("Executor callback not set. Cannot start scheduler.")

        self._logger.info("ProcessSchedulerAdapter: Initializing and starting ProcessScheduler.")
        self._scheduler = ProcessScheduler(
            secs=self._poll_interval_secs,
            executor=self._executor_callback,
            logger=self._logger
        )
        # The ProcessScheduler's add_process_to_queue calls its own start_scheduler.
        # If we want an explicit start here independent of adding a task,
        # we can call it. ProcessScheduler.start_scheduler() is idempotent.
        self._scheduler.start_scheduler()
        self._is_running = True
        self._logger.info("ProcessSchedulerAdapter: Started.")

    def stop(self) -> None:
        """Stops the scheduler's monitoring process."""
        if self._scheduler and self._is_running:
            self._logger.info("ProcessSchedulerAdapter: Stopping ProcessScheduler.")
            self._scheduler.stop_scheduler()
            self._is_running = False
            self._scheduler = None # Optional: release the scheduler instance
            self._logger.info("ProcessSchedulerAdapter: Stopped.")
        else:
            self._logger.info("ProcessSchedulerAdapter: Stop called but not running or not initialized.")
