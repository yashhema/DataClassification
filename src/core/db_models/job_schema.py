# src/core/db_models/job_schema.py
"""
Defines the database schema for Master Jobs, Scan Templates, Policy Templates, and Jobs
using the SQLAlchemy ORM.
"""

import enum
from datetime import datetime, timezone
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
    __tablename__ = 'master_jobs'
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    master_job_id: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(String(1024))
    configuration: Mapped[Dict[str, Any]] = mapped_column(JSON, nullable=False)

# =================================================================
# Template Models
# =================================================================
class PolicyTemplate(Base):
    __tablename__ = 'policy_templates'
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    template_id: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(String(1024))
    is_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    configuration: Mapped[Dict[str, Any]] = mapped_column(JSON, nullable=False)
    
    jobs: Mapped[List["Job"]] = relationship(
        "Job",
        primaryjoin="and_(PolicyTemplate.id == Job.template_table_id, Job.template_type == 'POLICY')",
        back_populates="policy_template",
        overlaps="scan_template,jobs"
    )

class ScanTemplate(Base):
    __tablename__ = 'scan_templates'
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
        primaryjoin="and_(ScanTemplate.id == Job.template_table_id, Job.template_type == 'SCANNING')",
        back_populates="scan_template",
        overlaps="policy_template,jobs"
    )

# =================================================================
# Job Model
# =================================================================
class Job(Base):
    __tablename__ = 'jobs'
    __table_args__ = (
        Index('ix_jobs_status', 'status'),
        Index(
            'ix_jobs_lease_for_recovery', 
            'node_group', 'status', 'orchestrator_lease_expiry',
            postgresql_where=lambda: "status = 'RUNNING'"
        ),
    )
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    execution_id: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    master_job_id: Mapped[Optional[str]] = mapped_column(String(255), comment="The ID of the master job that created this child job.")
    master_pending_commands: Mapped[Optional[str]] = mapped_column(String(255), comment="Command issued by the master cli.")
    # Audit trail back to the original template
    template_table_id: Mapped[int] = mapped_column(Integer, nullable=False)
    template_type: Mapped[JobType] = mapped_column(SQLAlchemyEnum(JobType), nullable=False)

    # Self-contained, filtered configuration for this specific job
    configuration: Mapped[Dict[str, Any]] = mapped_column(JSON, nullable=False, comment="A self-contained copy of the filtered scan configuration.")
    
    # Columns for leasing, recovery, and regional assignment
    node_group: Mapped[Optional[str]] = mapped_column(String(255), comment="The datacenter/region this job is assigned to.")
    orchestrator_id: Mapped[Optional[str]] = mapped_column(String(255), comment="The unique ID of the orchestrator instance that owns this job.")
    orchestrator_lease_expiry: Mapped[Optional[datetime]] = mapped_column(DateTime, comment="The timestamp when the current lease expires.")
    lease_warning_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, comment="A counter for the failover grace period.")
    last_lease_warning_timestamp: Mapped[Optional[datetime]] = mapped_column(DateTime, comment="Timestamp of the last lease expiry warning.")
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1, comment="Version number for optimistic locking.")
    
    # Core job properties
    priority: Mapped[int] = mapped_column(Integer, nullable=False, default=100)
    status: Mapped[JobStatus] = mapped_column(SQLAlchemyEnum(JobStatus), nullable=False, default=JobStatus.QUEUED)
    trigger_type: Mapped[str] = mapped_column(String(50), nullable=False)
    created_timestamp: Mapped[datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())
    completed_timestamp: Mapped[Optional[datetime]] = mapped_column(DateTime)

    # Relationships
    scan_template: Mapped[Optional["ScanTemplate"]] = relationship(
        primaryjoin="and_(ScanTemplate.id == Job.template_table_id, Job.template_type == 'SCANNING')",
        back_populates="jobs",
        overlaps="policy_template"
    )
    policy_template: Mapped[Optional["PolicyTemplate"]] = relationship(
        primaryjoin="and_(PolicyTemplate.id == Job.template_table_id, Job.template_type == 'POLICY')",
        back_populates="jobs",
        overlaps="scan_template"
    )
    tasks: Mapped[List["Task"]] = relationship(cascade="all, delete-orphan")

# =================================================================
# Task Models
# =================================================================
class Task(Base):
    __tablename__ = 'tasks'
    __table_args__ = (
        Index('ix_tasks_status_job_id', 'status', 'job_id'),
        Index('ix_tasks_lease_expiry', 'lease_expiry'),
    )
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    job_id: Mapped[int] = mapped_column(ForeignKey('jobs.id', ondelete="CASCADE"), nullable=False)
    datasource_id: Mapped[Optional[str]] = mapped_column(String(255))
    parent_task_id: Mapped[Optional[int]] = mapped_column(Integer)
    task_type: Mapped[str] = mapped_column(String(100), nullable=False)
    status: Mapped[TaskStatus] = mapped_column(SQLAlchemyEnum(TaskStatus), nullable=False, default=TaskStatus.PENDING, index=True)
    work_packet: Mapped[Dict[str, Any]] = mapped_column(JSON, nullable=False)
    worker_id: Mapped[Optional[str]] = mapped_column(String(255))
    lease_expiry: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    retry_count: Mapped[int] = mapped_column(Integer, default=0)

class TaskOutputRecord(Base):
    __tablename__ = 'task_output_records'
    __table_args__ = (
        Index('ix_task_output_records_status_task_id', 'status', 'task_id'),
    )
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    task_id: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(String(50), nullable=False, default='PENDING_PROCESSING')
    output_type: Mapped[str] = mapped_column(String(100), nullable=False)
    output_payload: Mapped[Dict[str, Any]] = mapped_column(JSON, nullable=False)
    created_timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))