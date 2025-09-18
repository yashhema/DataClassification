# src/core/db_models/processing_status_schema.py
"""
Defines the database schema for storing the processing status of discovered objects.
"""

from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import (
    String, Integer, DateTime, Text, Index
)
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base

class ObjectProcessingStatus(Base):
    __tablename__ = 'object_processing_statuses'
    __table_args__ = (
        Index('ix_object_processing_statuses_job_id', 'job_id', unique=False),
        {'extend_existing': True} # For robustness with validation scripts
    )
    __doc__ = """
    Stores the last known processing status for an object that did not complete 
    classification successfully (e.g., file was too large, corrupt, or unsupported).
    This provides an audit trail for why certain objects have no findings.
    """
    
    # The unique, string-based object_id (e.g., 'ds_smb:hash:path/file.txt')
    # is used as the primary key. This avoids needing a separate lookup query.
    object_id_str: Mapped[str] = mapped_column(String(255), primary_key=True, comment="The unique string identifier for the object.")
    
    job_id: Mapped[int] = mapped_column(Integer, nullable=False, comment="The ID of the job that was running when this status was recorded.")
    
    status: Mapped[str] = mapped_column(String(100), nullable=False, comment="The processing status, e.g., 'FileTooLarge', 'Corrupted'.")
    
    details: Mapped[Optional[str]] = mapped_column(Text, nullable=True, comment="Details about the status, like an error message from the content extractor.")
    
    scan_timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), 
        nullable=False, 
        default=lambda: datetime.now(timezone.utc),
        comment="The timestamp when this status was recorded."
    )