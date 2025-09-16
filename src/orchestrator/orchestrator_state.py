# src/orchestrator/orchestrator_state.py
"""
Defines shared state models and enums for the Orchestrator to
prevent circular import issues.
"""

from enum import Enum

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
