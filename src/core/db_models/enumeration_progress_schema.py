# src/core/db_models/enumeration_progress_schema.py
"""
Defines the database schema for tracking the progress of fault-tolerant,
multi-boundary enumeration tasks.
"""

import enum
from datetime import datetime
from sqlalchemy import (
    String, LargeBinary, DateTime, Index, Enum as SQLAlchemyEnum,
    UniqueConstraint, func, ForeignKey
)
from sqlalchemy.orm import Mapped, mapped_column
from .base import Base

class EnumerationProgressStatus(str, enum.Enum):
    """Defines the state of a boundary within an enumeration task."""
    PROCESSING_COMPLETE = "PROCESSING_COMPLETE" # Worker has finished enumerating and saving objects.
    PIPELINE_COMPLETE = "PIPELINE_COMPLETE"     # Pipeliner has created all downstream tasks.

class EnumerationProgress(Base):
    __tablename__ = 'enumeration_progress'
    __table_args__ = (
        # Ensures that for any given task, a boundary can only be recorded once.
        UniqueConstraint('task_id', 'boundary_id', name='uq_task_boundary'),
        Index('ix_enumeration_progress_status', 'status'),
    )
    __doc__ = """
    Acts as a transactional checkpoint log for enumeration tasks that process
    multiple boundaries (e.g., directories or databases). A record in this table
    is the definitive source of truth that a specific boundary has been fully
    and successfully processed by a worker.
    """

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    # Foreign key to the task that is performing the enumeration.
    task_id: Mapped[bytes] = mapped_column(
        LargeBinary(32),
        ForeignKey('tasks.id', ondelete="CASCADE"),
        nullable=False,
        index=True
    )

    # The unique hash of the boundary (e.g., a hash of a directory path or database name).
    boundary_id: Mapped[bytes] = mapped_column(
        LargeBinary(32),
        nullable=False,
        index=True
    )
    
    # A human-readable version of the boundary for easier debugging.
    boundary_path: Mapped[str] = mapped_column(String(4000), nullable=False)

    # The status of the boundary's processing.
    status: Mapped[EnumerationProgressStatus] = mapped_column(
        SQLAlchemyEnum(EnumerationProgressStatus),
        nullable=False,
        default=EnumerationProgressStatus.PROCESSING_COMPLETE
    )

    created_timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now()
    )
    
    last_updated: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now()
    )