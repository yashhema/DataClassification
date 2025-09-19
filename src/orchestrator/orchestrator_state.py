# src/orchestrator/orchestrator_state.py
"""
Defines shared state models and enums for the Orchestrator to
prevent circular import issues.
"""

from enum import Enum
from core.db_models.job_schema import JobStatus
class JobState(str, Enum):
    """Valid job states for the in-memory state machine"""
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    PAUSING = "PAUSING"
    PAUSED = "PAUSED"
    CANCELLING = "CANCELLING"
    CANCELLED = "CANCELLED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    TERMINATED = "TERMINATED"
    
# NEW: Explicit mapping from database enum to in-memory enum
DB_STATUS_TO_MEMORY_STATE = {
    JobStatus.QUEUED: JobState.QUEUED,
    JobStatus.RUNNING: JobState.RUNNING,
    JobStatus.PAUSING: JobState.PAUSING,
    JobStatus.PAUSED: JobState.PAUSED,
    JobStatus.CANCELLING: JobState.CANCELLING,
    JobStatus.CANCELLED: JobState.CANCELLED,
    JobStatus.COMPLETED: JobState.COMPLETED,
    JobStatus.COMPLETED_WITH_FAILURES: JobState.COMPLETED_WITH_FAILURES,
    JobStatus.FAILED: JobState.FAILED,
}

# NEW: Safe conversion function
def map_db_status_to_memory_state(db_status: JobStatus) -> JobState:
    """Safely converts a JobStatus from the DB to an in-memory JobState."""
    if db_status not in DB_STATUS_TO_MEMORY_STATE:
        raise ValueError(f"Unknown or unmappable DB JobStatus: {db_status}")
    return DB_STATUS_TO_MEMORY_STATE[db_status]