from enum import Enum

class AssetProcessingStatus(str, Enum):
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    NOT_APPLICABLE = "NOT_APPLICABLE" # For assets that don't need processing
    QUEUED = "QUEUED" # Added based on potential need from process_queue
    # Add any other relevant statuses
