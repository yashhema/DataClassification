# src/core/db_models/job_schema.py
"""
Defines the database schema for Master Jobs, Scan Templates, Policy Templates, and Jobs
using the SQLAlchemy ORM.
"""

import enum
from datetime import datetime
from typing import List, Optional, Dict, Any

from sqlalchemy import (
    String, Integer, ForeignKey, DateTime, JSON, Boolean, func, Index, Enum as SQLAlchemyEnum, and_
)
from sqlalchemy.orm import Mapped, mapped_column, relationship, foreign

from .base import Base

class JobType(str, enum.Enum):
    """Defines the overall goal or workflow of a job."""
    SCANNING = "SCANNING"
    POLICY = "POLICY"

class JobStatus(str, enum.Enum):
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    PAUSING = "PAUSING"
    PAUSED = "PAUSED"
    CANCELLING = "CANCELLING"
    CANCELLED = "CANCELLED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"

class TaskStatus(str, enum.Enum):
    PENDING = "PENDING"
    ASSIGNED = "ASSIGNED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"

# =================================================================
# NEW: MasterJob Model
# =================================================================
class MasterJob(Base):
    __tablename__ = 'MasterJobs'
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    master_job_id: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(String(1024))
    configuration: Mapped[Dict[str, Any]] = mapped_column(JSON, nullable=False)

# =================================================================
# Template Models (Unchanged)
# =================================================================
class PolicyTemplate(Base):
    __tablename__ = 'policy_templates'
    # ... (content of this class remains the same)
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    template_id: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(String(1024))
    is_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    configuration: Mapped[Dict[str, Any]] = mapped_column(JSON, nullable=False)
    
    jobs: Mapped[List["Job"]] = relationship(
        "Job",
        primaryjoin=f"and_(PolicyTemplate.id == foreign(Job.template_table_id), Job.template_type == '{JobType.POLICY.value}')",
        back_populates="policy_template",
        overlaps="scan_template,jobs"
    )

class ScanTemplate(Base):
    __tablename__ = 'scan_templates'
    # ... (content of this class remains the same)
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    template_id: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(String(1024))
    job_type: Mapped[JobType] = mapped_column(SQLAlchemyEnum(JobType), nullable=False, default=JobType.SCANNING)
    priority: Mapped[int] = mapped_column(Integer, nullable=False, default=100)
    classifier_template_id: Mapped[str] = mapped_column(String(255), nullable=False)
    is_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    configuration: Mapped[Dict[str, Any]] = mapped_column(JSON, nullable=False)

    jobs: Mapped[List["Job"]] = relationship(
        "Job",
        primaryjoin=f"and_(ScanTemplate.id == foreign(Job.template_table_id), Job.template_type == '{JobType.SCANNING.value}')",
        back_populates="scan_template",
        overlaps="policy_template,jobs"
    )

# =================================================================
# UPDATED: Job Model
# =================================================================
class Job(Base):
    __tablename__ = 'jobs'
    __table_args__ = (
        Index('IX_Jobs_Status', 'status'),
        Index(
            'IX_Jobs_Lease_For_Recovery', 
            'NodeGroup', 'status', 'OrchestratorLeaseExpiry',
            postgresql_where=lambda: "status = 'RUNNING'"  # Example for PostgreSQL, SQL Server uses WHERE clause in CREATE INDEX
        ),
    )
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    execution_id: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    master_job_id: Mapped[Optional[str]] = mapped_column(String(255), comment="The ID of the master job that created this child job.")
    master_pending_commands: Mapped[Optional[str]] = mapped_column(String(255), comment="Command issued by the master cli.")
    # Audit trail back to the original template
    template_table_id: Mapped[int] = mapped_column(Integer, nullable=False)
    template_type: Mapped[JobType] = mapped_column(SQLAlchemyEnum(JobType), nullable=False)

    # NEW: Self-contained, filtered configuration for this specific job
    configuration: Mapped[Dict[str, Any]] = mapped_column(JSON, nullable=False, comment="A self-contained copy of the filtered scan configuration.")
    
    # NEW: Columns for leasing, recovery, and regional assignment
    NodeGroup: Mapped[Optional[str]] = mapped_column(String(255), comment="The datacenter/region this job is assigned to.")
    OrchestratorID: Mapped[Optional[str]] = mapped_column(String(255), comment="The unique ID of the orchestrator instance that owns this job.")
    OrchestratorLeaseExpiry: Mapped[Optional[datetime]] = mapped_column(DateTime, comment="The timestamp when the current lease expires.")
    LeaseWarningCount: Mapped[int] = mapped_column(Integer, nullable=False, default=0, comment="A counter for the failover grace period.")
    LastLeaseWarningTimestamp: Mapped[Optional[datetime]] = mapped_column(DateTime, comment="Timestamp of the last lease expiry warning.")
    Version: Mapped[int] = mapped_column(Integer, nullable=False, default=1, comment="Version number for optimistic locking.")
    
    # Core job properties
    Priority: Mapped[int] = mapped_column(Integer, nullable=False, default=100)
    status: Mapped[JobStatus] = mapped_column(SQLAlchemyEnum(JobStatus), nullable=False, default=JobStatus.QUEUED)
    trigger_type: Mapped[str] = mapped_column(String(50), nullable=False)
    created_timestamp: Mapped[datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())
    completed_timestamp: Mapped[Optional[datetime]] = mapped_column(DateTime)

    # Relationships (Unchanged)
    scan_template: Mapped["ScanTemplate"] = relationship(
        primaryjoin=f"and_(ScanTemplate.id == foreign(Job.template_table_id), Job.template_type == '{JobType.SCANNING.value}')",
        back_populates="jobs",
        overlaps="policy_template"
    )
    policy_template: Mapped["PolicyTemplate"] = relationship(
        primaryjoin=f"and_(PolicyTemplate.id == foreign(Job.template_table_id), Job.template_type == '{JobType.POLICY.value}')",
        back_populates="jobs",
        overlaps="scan_template"
    )
    tasks: Mapped[List["Task"]] = relationship(cascade="all, delete-orphan")

# =================================================================
# Task Models (Unchanged)
# =================================================================
class Task(Base):
    __tablename__ = 'tasks'
    # ... (content of this class remains the same)
    __table_args__ = (
        Index('IX_Tasks_Status_JobId', 'Status', 'JobID'),
        Index('IX_Tasks_LeaseExpiry', 'LeaseExpiry'),
    )
    ID: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    JobID: Mapped[int] = mapped_column(ForeignKey('jobs.id', ondelete="CASCADE"), nullable=False)
    DatasourceID: Mapped[Optional[str]] = mapped_column(String(255))
    ParentTaskID: Mapped[Optional[int]] = mapped_column(Integer)
    TaskType: Mapped[str] = mapped_column(String(100), nullable=False)
    Status: Mapped[TaskStatus] = mapped_column(SQLAlchemyEnum(TaskStatus), nullable=False, default=TaskStatus.PENDING, index=True)
    WorkPacket: Mapped[Dict[str, Any]] = mapped_column(JSON, nullable=False)
    WorkerID: Mapped[Optional[str]] = mapped_column(String(255))
    LeaseExpiry: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    RetryCount: Mapped[int] = mapped_column(Integer, default=0)

class TaskOutputRecord(Base):
    __tablename__ = 'task_output_records'
    # ... (content of this class remains the same)
    __table_args__ = (
        Index('IX_TaskOutputRecords_Status_TaskID', 'Status', 'TaskID'),
    )
    ID: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    TaskID: Mapped[int] = mapped_column(Integer, nullable=False)
    Status: Mapped[str] = mapped_column(String(50), nullable=False, default='PENDING_PROCESSING')
    OutputType: Mapped[str] = mapped_column(String(100), nullable=False)
    OutputPayload: Mapped[Dict[str, Any]] = mapped_column(JSON, nullable=False)
    CreatedTimestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))